"""
Coordinates of the key-verse highlight inside a selected passage
(ClickUp 86cb8vw1h follow-up, architect/adr/0005-grounded-passage-rerank.md).

The grounded rerank answers with a span of VERSE MARKERS — 1-based indexes
into the verse list the server itself rendered into the prompt (never verse
numbers invented by the model). This module turns that validated span into
the two coordinate systems the public contract speaks:

- canonical (english-masoretic) coordinates, the same space as the chunk's
  canonical ID (ADR 0003);
- the numbering of the translation the passage is rendered in.

Outside the Psalms the two coincide and the conversion is the identity. In
the Psalms they do not: `syn`/`bti`/`npu` number chapters after the
Septuagint and count ~60 superscriptions as verse 1 (or 1-2), and `ubh`
keeps Masoretic chapters with Hebrew verse numbering. The mapping is the
stored, verified one — `cep_public.psalm_verse_mappings`, produced by
`app/versification_cli.py build` — loaded once with the corpus.

Any coordinate that cannot be mapped exactly (missing psalm mapping, a span
straddling two canonical chapters, a superscription that the canon does not
number, a mapped range falling outside the passage that is actually served,
a boundary number that passage does not number at all) yields None: the
passage is then served WITHOUT a highlight rather than with a guessed
reference.

One deliberate asymmetry: a highlight is at most 3 verses in the numbering
the model saw, but a canonical span may come out LONGER — a translation
verse that merges several canonical ones expands on conversion (syn 114:8
alone is canonical 116:8-9). The canonical range stays exact rather than
truncated, so the two coordinate systems keep describing the same text;
only the passage-side range is bound by the 3-verse product rule.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from versification import (
    PSALMS_BOOK,
    VERSIFICATION_VERSION,
    PsalmMap,
    VerseMapping,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VerseSpan:
    """A verse range inside one chapter of one coordinate system."""
    chapter: int
    verse_start: int
    verse_end: int


@dataclass(frozen=True)
class Highlight:
    """The key verses in both coordinate systems.

    `passage` is the 1-3 verses the model marked, in the numbering of the
    passage served. `canonical` is the same text in canonical coordinates
    and may be LONGER than 3 verses when a translation verse merges several
    canonical ones (see the module docstring).
    """
    canonical: VerseSpan
    passage: VerseSpan


def to_canonical_span(
    book_number: int, span: VerseSpan, psalm_map: PsalmMap | None
) -> VerseSpan | None:
    """Translation coordinates -> canonical ones (identity outside Psalms).

    A translation verse may cover several canonical verses (a merge), so the
    start of the span takes the mapped range's first verse and the end its
    last one — which is why the canonical span can come out longer than the
    3 verses the model was allowed to mark. That is accepted deliberately:
    the exact canonical range describes the same text, a truncated one would
    not. None when the psalm mapping is missing, does not know the verse, or
    the span would straddle two canonical chapters.
    """
    if book_number != PSALMS_BOOK:
        return span
    if psalm_map is None:
        return None
    try:
        start_chapter, start_verse, _ = psalm_map.to_canonical(
            span.chapter, span.verse_start
        )
        end_chapter, _, end_verse = psalm_map.to_canonical(
            span.chapter, span.verse_end
        )
    except KeyError:
        return None
    if start_chapter != end_chapter or end_verse < start_verse:
        return None
    return VerseSpan(start_chapter, start_verse, end_verse)


def from_canonical_span(
    book_number: int, span: VerseSpan, psalm_map: PsalmMap | None
) -> VerseSpan | None:
    """Canonical coordinates -> one translation's own numbering.

    Only needed when the passage is rendered in a translation OTHER than the
    one whose verses were shown to the model. Canonical verse 0 (a
    superscription, which the canon does not number) has no reverse entry by
    construction, so such a span yields None.
    """
    if book_number != PSALMS_BOOK:
        return span
    if psalm_map is None or span.verse_start < 1:
        return None
    start = psalm_map.from_canonical(span.chapter, span.verse_start)
    end = psalm_map.from_canonical(span.chapter, span.verse_end)
    if start is None or end is None or start[0] != end[0]:
        return None
    if end[1] < start[1]:
        return None
    return VerseSpan(start[0], start[1], end[1])


def _inside_passage(passage, span: VerseSpan) -> bool:
    """Is `span` a real sub-range of the passage that will be served?

    Needed only for a span that reached the target translation through the
    canon: its verse numbers come from a mapping table, not from the verse
    list of that passage, so nothing so far has compared them with the
    passage's own chunk boundaries. The two translations chunk the corpus
    independently, so the mapped range can legitimately land outside the
    window this translation returns.
    """
    return (
        span.chapter == passage.chapter_number
        and passage.verse_number_start <= span.verse_start
        and span.verse_end <= passage.verse_number_end
    )


def _numbered_in_passage(passage, span: VerseSpan) -> bool:
    """Do the span's boundary numbers actually occur in the passage's verses?

    `_inside_passage` compares the span with the FIRST and the LAST verse of
    the passage, and a range is not a set. A translation that carries two
    canonical verses in one leaves a HOLE in its own numbering — `bti` has no
    Genesis 35:10, its verse 9 says both — and a span converted from another
    translation's numbering can land exactly in such a hole. Outside the
    Psalms the conversion is the identity, so nothing before this point had
    any reason to notice.

    The public contract promises that a highlight served next to `verses`
    names numbers a client can find there, so a boundary the served passage
    does not number yields no highlight at all — the same fail-closed rule
    the rest of the ladder follows. Inside the Psalms the mapping table
    already answers per verse and this check agrees with it. A passage
    carrying no verse list promises nothing (the client only gets `text`),
    so there is nothing to check against.
    """
    if not passage.verses:
        return True
    numbers = {verse.verse_number for verse in passage.verses}
    return span.verse_start in numbers and span.verse_end in numbers


def resolve_highlight(
    book_number: int,
    prompt_passage,
    target_passage,
    indices: tuple[int, int],
    psalm_maps: dict[int, PsalmMap] | None = None,
) -> Highlight | None:
    """Marker span -> canonical + translation coordinates, or None.

    prompt_passage - the retrieval.PassageText whose verses were rendered
                     into the rerank prompt (the markers index into it);
    target_passage - the PassageText actually returned to the client.

    The bounds are re-checked here as well: this function is the last place
    before the public contract, and it must never build a range out of an
    index it was not given verses for, nor hand back a range that is not
    inside the passage actually served (`_inside_passage`), nor one whose
    ends that passage does not number at all (`_numbered_in_passage` — the
    check that makes the published `verses` guarantee true).
    """
    psalm_maps = psalm_maps or {}
    verses = getattr(prompt_passage, "verses", None) or []
    start_index, end_index = indices
    if not 1 <= start_index <= end_index <= len(verses):
        return None
    span = VerseSpan(
        chapter=prompt_passage.chapter_number,
        verse_start=verses[start_index - 1].verse_number,
        verse_end=verses[end_index - 1].verse_number,
    )
    canonical = to_canonical_span(
        book_number, span, psalm_maps.get(prompt_passage.translation)
    )
    if canonical is None:
        return None
    if target_passage.translation == prompt_passage.translation:
        target = span
    else:
        target = from_canonical_span(
            book_number, canonical, psalm_maps.get(target_passage.translation)
        )
        if target is None or not _inside_passage(target_passage, target):
            return None
    if not _numbered_in_passage(target_passage, target):
        return None
    return Highlight(canonical=canonical, passage=target)


def load_psalm_maps(cursor) -> dict[int, PsalmMap]:
    """Per-translation PsalmMap from `cep_public.psalm_verse_mappings`.

    Corpus-derived, prayer-independent data — loaded once with the vector
    index and cached with it. A translation whose stored rows do not form a
    consistent map is skipped (its Psalm highlights degrade to absent)
    rather than failing the whole corpus load.
    """
    cursor.execute(
        """
        SELECT translation, chapter_number, verse_number, canonical_chapter,
               canonical_verse_start, canonical_verse_end
        FROM psalm_verse_mappings
        WHERE mapping_version = %s AND book_number = %s
        ORDER BY translation, chapter_number, verse_number
        """,
        (VERSIFICATION_VERSION, PSALMS_BOOK),
    )
    rows_by_translation: dict[int, list[VerseMapping]] = {}
    for row in cursor.fetchall():
        rows_by_translation.setdefault(row["translation"], []).append(
            VerseMapping(
                chapter=row["chapter_number"],
                verse=row["verse_number"],
                canonical_chapter=row["canonical_chapter"],
                canonical_verse_start=row["canonical_verse_start"],
                canonical_verse_end=row["canonical_verse_end"],
            )
        )
    maps: dict[int, PsalmMap] = {}
    for translation, mappings in rows_by_translation.items():
        try:
            maps[translation] = PsalmMap(mappings)
        except ValueError as error:
            logger.warning(
                "Psalm mapping of translation %s is inconsistent: %s",
                translation, error,
            )
    return maps
