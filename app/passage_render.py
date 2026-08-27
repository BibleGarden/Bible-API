"""
Rendering a canonical passage window in a translation that is NOT indexed
(ADR 0007, ClickUp follow-up of 0006 open question 5).

The retrieval corpus is built from ONE indexed ("reference") translation per
language: chunk boundaries, embeddings and the BM25 index all live in that
translation's coordinate space, and the canonical chunk IDs (`v3:19.023.001-006`)
are shared across traditions by construction (ADR 0001/0003). Every other
active translation of the same language can therefore be SERVED without being
indexed: the canonical window chosen by the pipeline is converted into that
translation's own coordinates and its verses are read straight from
`translation_verses`.

Two things make that safe rather than approximate:

- `psalm_verse_mappings` (ADR 0003) converts the Psalter both ways, so a
  Septuagint-numbered target renders the same words the canonical window
  names, not the same numbers;
- a per-translation COVERAGE set is computed with the corpus: the canonical
  windows every verse of which really exists in that translation. Windows
  outside it are filtered out of the candidate list BEFORE the rerank, so the
  chosen passage is always fully renderable (a translation may be missing
  whole books — `npu` is New Testament + Psalms only — or single verses).

Text assembly mirrors `chunking.build_text` exactly (empty verses dropped,
paragraphs at `start_paragraph` and at section titles), and the section title
is chosen the way `chunking.apply_chapter_plan` chooses it: the most specific
title at or before the first verse of the window.

One deliberate difference from an indexed passage: the rendered range is the
window's OWN canonical range. The overlap verses an indexed chunk carries in
its text come from the pivot translation's chunking plan, which does not exist
for a translation that was never chunked; rendering the own range is exact and
reproducible instead of approximated.

That difference is bounded on purpose. A stored chunk may also reach PAST its
own range: the last plan slot of a chapter absorbs the chapter's trailing
verses (`chunking.apply_chapter_plan` gives it no upper bound), so a window
whose ID says 12-26 can be stored as 12-34. For such a window the reranker
judges text the reader would never see, and the difference is a tail, not the
documented overlap prefix. `reference_faithful_windows` therefore drops those
windows from the candidate universe of the non-primary translations, measured
against the stored ranges of the reference translation itself — never against
a heuristic (ADR 0007, review fix F2).
"""

from __future__ import annotations

import logging

from chunking import CHUNKING_VERSION, build_text
from retrieval import PassageText, VerseText
from versification import PSALMS_BOOK, PsalmMap

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Canonical window -> the target translation's own coordinates
# ---------------------------------------------------------------------------

def to_translation_range(
    book_number: int,
    chapter_number: int,
    verse_start: int,
    verse_end: int,
    psalm_map: PsalmMap | None,
) -> tuple[int, int, int] | None:
    """Canonical window -> (chapter, first verse, last verse) of a translation.

    Identity outside the Psalms. Inside them the stored versification map is
    used in the reverse direction, plus two rules the canon itself imposes:

    - canonical verse 0 is a superscription, which the canon does not number
      and the reverse map therefore cannot resolve. A window starting at 0
      starts at canonical verse 1 in the target and then grows BACK over the
      verses that the target counts as the superscription of that chapter
      (`syn` and `ubh` count 1 or 2 of them, `bsb` none), so the target
      renders the superscription exactly when it has one;
    - a window that would straddle two chapters of the target, or whose
      verses the map does not know, yields None — such a window is never a
      candidate for that translation anyway (it is outside its coverage set).
    """
    if book_number != PSALMS_BOOK:
        return (chapter_number, max(verse_start, 1), verse_end)
    if psalm_map is None:
        return None
    first_canonical = max(verse_start, 1)
    start = psalm_map.from_canonical(chapter_number, first_canonical)
    end = psalm_map.from_canonical(chapter_number, verse_end)
    if start is None or end is None or start[0] != end[0] or end[1] < start[1]:
        return None
    chapter, first = start
    if verse_start <= 0:
        first = _superscription_start(psalm_map, chapter, first, chapter_number)
    return (chapter, first, end[1])


def _superscription_start(
    psalm_map: PsalmMap, chapter: int, first: int, canonical_chapter: int
) -> int:
    """Extend a window's first verse back over the counted superscription.

    The verses a translation counts as a superscription are the head of the
    chapter and map to canonical verse 0, so walking back from `first` while
    that holds is exact; a translation that does not count one stops at once.
    """
    verse = first
    while verse > 1:
        try:
            mapped = psalm_map.to_canonical(chapter, verse - 1)
        except KeyError:
            break
        if mapped[0] != canonical_chapter or mapped[2] != 0:
            break
        verse -= 1
    return verse


# ---------------------------------------------------------------------------
# Coverage: canonical windows that fully exist in a translation
# ---------------------------------------------------------------------------

def canonical_presence(
    rows, psalm_map: PsalmMap | None
) -> dict[tuple[int, int], set[int]]:
    """(book, canonical chapter) -> canonical verse numbers present.

    `rows` are the translation's non-empty verses as
    (book_number, chapter_number, verse_number). Empty verses must already be
    excluded by the caller: `chunking.build_text` drops them, so a window
    whose only remaining verses are blank does not exist as text either.
    Psalm coordinates are converted with the stored map; a verse the map does
    not know is skipped (the windows needing it stay uncovered).
    """
    present: dict[tuple[int, int], set[int]] = {}
    for book, chapter, verse in rows:
        if book != PSALMS_BOOK:
            present.setdefault((book, chapter), set()).add(verse)
            continue
        if psalm_map is None:
            continue
        try:
            c_chapter, c_start, c_end = psalm_map.to_canonical(chapter, verse)
        except KeyError:
            continue
        target = present.setdefault((book, c_chapter), set())
        for c_verse in range(max(c_start, 1), c_end + 1):
            target.add(c_verse)
    return present


def covered_windows(
    windows, present: dict[tuple[int, int], set[int]]
) -> set[str]:
    """Canonical IDs whose every verse exists in the translation.

    `windows` are (canonical_id, book, chapter, verse_start, verse_end) in
    canonical coordinates. Verse 0 (a superscription) is not required: the
    canon does not number it, translations disagree on whether it exists at
    all, and `to_translation_range` renders it only where the target has one.

    A window with no positive verse at all (`end <= 0`) is NEVER covered: an
    empty verse range would make `all()` vacuously true while
    `to_translation_range` has nothing to resolve, i.e. a "covered" window
    that answers 503. The corpus contains no such window today; the rule
    keeps it that way (review fix F5).
    """
    covered: set[str] = set()
    for canonical_id, book, chapter, start, end in windows:
        if end < max(start, 1):
            continue
        verses = present.get((book, chapter))
        if not verses:
            continue
        if all(verse in verses for verse in range(max(start, 1), end + 1)):
            covered.add(canonical_id)
    return covered


def load_verse_coordinates(cursor, translation_code: int) -> list[tuple]:
    """Every non-empty verse coordinate of one translation (one query)."""
    cursor.execute(
        """
        SELECT book_number, chapter_number, verse_number
        FROM translation_verses
        WHERE translation = %s AND TRIM(text) <> ''
        """,
        (translation_code,),
    )
    return [
        (row["book_number"], row["chapter_number"], row["verse_number"])
        for row in cursor.fetchall()
    ]


def build_coverage(
    cursor,
    translation_code: int,
    windows,
    psalm_map: PsalmMap | None,
) -> set[str]:
    """Coverage set of one translation over the canonical windows given."""
    rows = load_verse_coordinates(cursor, translation_code)
    return covered_windows(windows, canonical_presence(rows, psalm_map))


# ---------------------------------------------------------------------------
# Windows whose reference chunk really is the window's own range
# ---------------------------------------------------------------------------

def load_chunk_ranges(
    cursor, translation_code: int, chunking_version: int = CHUNKING_VERSION
) -> dict[str, tuple[int, int]]:
    """canonical_id -> (chapter, last verse) actually STORED for a chunk.

    The chunk's own coordinates, in that translation's numbering — the range
    whose text `translation_chunks` holds and the reranker is shown.
    """
    cursor.execute(
        """
        SELECT canonical_id, chapter_number, verse_number_end
        FROM translation_chunks
        WHERE translation = %s AND chunking_version = %s
        """,
        (translation_code, chunking_version),
    )
    return {
        row["canonical_id"]: (row["chapter_number"], row["verse_number_end"])
        for row in cursor.fetchall()
    }


def reference_faithful_windows(
    windows,
    chunk_ranges: dict[str, tuple[int, int]],
    psalm_map: PsalmMap | None,
) -> list[tuple]:
    """Windows a non-indexed translation may be offered (ADR 0007, fix F2).

    The reranker judges the REFERENCE translation's stored chunk; a
    non-indexed translation is served the window's own canonical range. The
    two may legitimately differ by the overlap prefix the chunker copies from
    the previous chunk — that prefix is text the reader gets LESS of at the
    front, in exchange for the exact range the canonical ID names.

    They may also differ by a TAIL: the last plan slot of a chapter has no
    upper bound, so its stored chunk absorbs every trailing verse of the
    chapter (up to 8 verses / 55 % of the text in the current corpus). Such a
    window is judged on text the reader would never see, so it is dropped
    here instead — the candidate universe of every non-primary translation is
    restricted to the windows whose reference chunk does NOT end past the
    window's own range.

    The test is made against real data, not a rule of thumb: the stored
    (chapter, last verse) of `translation_chunks` versus the window's own
    canonical range converted into the same numbering. A window with no
    stored chunk, or one the reference cannot map, is dropped as well
    (fail-closed — it is a window nothing can be verified against).

    The opposite case is kept on purpose: a reference chunk that stops EARLY
    (the reference lacks a verse the served translation has — bsb has no
    Matthew 17:21, webus does) makes the reader see one verse more than the
    reranker did, still inside the range the canonical ID names. Five windows
    in the current corpus; nothing there is hidden from the reader.
    """
    kept: list[tuple] = []
    for window in windows:
        canonical_id, book, chapter, start, end = window
        stored = chunk_ranges.get(canonical_id)
        mapped = to_translation_range(book, chapter, start, end, psalm_map)
        if stored is None or mapped is None:
            continue
        reference_chapter, reference_end = stored
        own_chapter, _first, own_end = mapped
        if reference_chapter != own_chapter or reference_end > own_end:
            continue
        kept.append(window)
    dropped = len(windows) - len(kept)
    if dropped:
        logger.info(
            "ADR 0007 coverage: %s of %s canonical windows are excluded from "
            "the non-indexed translations (the reference chunk reaches past "
            "the window's own range)",
            dropped,
            len(windows),
        )
    return kept


# ---------------------------------------------------------------------------
# Passage rendering
# ---------------------------------------------------------------------------

def load_section_title(
    cursor, translation_code: int, book_number: int, chapter_number: int,
    verse_number: int,
) -> str | None:
    """Most specific section title at or before a verse (chunking's rule).

    Same source and ordering as `chunk_cli.load_translation_chapters`:
    non-subtitle rows of `translation_titles`, ordered by their own code so a
    later row wins for the same verse.
    """
    cursor.execute(
        """
        SELECT tv.verse_number, tt.text
        FROM translation_titles tt
        JOIN translation_verses tv ON tt.before_translation_verse = tv.code
        WHERE tv.translation = %s AND tt.subtitle = 0
          AND tv.book_number = %s AND tv.chapter_number = %s
          AND tv.verse_number <= %s
        ORDER BY tt.code
        """,
        (translation_code, book_number, chapter_number, verse_number),
    )
    titles: dict[int, str] = {}
    for row in cursor.fetchall():
        titles[row["verse_number"]] = row["text"]
    if not titles:
        return None
    return titles[max(titles)]


def load_range_verses(
    cursor, translation_code: int, book_number: int, chapter_number: int,
    first: int, last: int,
) -> list[VerseText]:
    """Non-empty verses of a range, in order (chunking's text semantics)."""
    cursor.execute(
        """
        SELECT verse_number, text, start_paragraph
        FROM translation_verses
        WHERE translation = %s AND book_number = %s AND chapter_number = %s
          AND verse_number BETWEEN %s AND %s
        ORDER BY verse_number
        """,
        (translation_code, book_number, chapter_number, first, last),
    )
    return [
        VerseText(
            verse_number=row["verse_number"],
            text=row["text"].strip(),
            start_paragraph=bool(row["start_paragraph"]),
        )
        for row in cursor.fetchall()
        if row["text"].strip()
    ]


def title_verses_of_range(
    cursor, translation_code: int, book_number: int, chapter_number: int,
    first: int, last: int,
) -> set[int]:
    """Verses inside the range that carry a section title (paragraph breaks)."""
    cursor.execute(
        """
        SELECT tv.verse_number
        FROM translation_titles tt
        JOIN translation_verses tv ON tt.before_translation_verse = tv.code
        WHERE tv.translation = %s AND tt.subtitle = 0
          AND tv.book_number = %s AND tv.chapter_number = %s
          AND tv.verse_number BETWEEN %s AND %s
        """,
        (translation_code, book_number, chapter_number, first, last),
    )
    return {row["verse_number"] for row in cursor.fetchall()}


def render_passage(
    cursor,
    translation_code: int,
    alias: str,
    book_number: int,
    chapter_number: int,
    verse_start: int,
    verse_end: int,
    psalm_map: PsalmMap | None,
) -> PassageText | None:
    """Canonical window -> the passage of one translation, straight from the DB.

    Returns None when the window cannot be mapped into the translation's
    coordinates or has no text there — the caller then answers 503 rather than
    silently serving another translation (grounding rule: the response always
    carries the translation that was asked for).
    """
    mapped = to_translation_range(
        book_number, chapter_number, verse_start, verse_end, psalm_map
    )
    if mapped is None:
        return None
    chapter, first, last = mapped
    verses = load_range_verses(
        cursor, translation_code, book_number, chapter, first, last
    )
    if not verses:
        return None
    title_verses = title_verses_of_range(
        cursor, translation_code, book_number, chapter, first, last
    )
    text = build_text(verses, title_verses)
    if not text:
        return None
    title = load_section_title(
        cursor, translation_code, book_number, chapter, verses[0].verse_number
    )
    return PassageText(
        translation=translation_code,
        alias=alias,
        book_number=book_number,
        chapter_number=chapter,
        verse_number_start=verses[0].verse_number,
        verse_number_end=verses[-1].verse_number,
        title=title,
        text=text,
        verses=verses,
    )
