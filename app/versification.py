"""
Psalm versification mapping to the project's canonical numbering.

Project canon (decided by Maria, ClickUp 86cb90j2f): Psalm coordinates follow
the english-masoretic numbering — Masoretic chapter numbers 1-150 (+151 for
the extra Septuagint psalm), superscriptions unnumbered. This matches
bsb/webus/webbe as stored in `cep_public`.

The other translations differ:

- `syn`, `bti`, `npu` — Septuagint chapter numbering (Psalms 9/10 and 114/115
  merged, 113 and 147 split relative to the Masoretic text) and, in ~60
  psalms, the superscription counted as verse 1 (or verses 1-2);
- `ubh` — Masoretic chapter numbers, but Hebrew-style verse numbering: the
  superscription is verse 1 (or 1-2) in ~60 psalms.

This module builds, purely and deterministically, a verse-level mapping
"translation coordinates -> canonical coordinates" for every translation:

- chapter correspondence follows the classical Masoretic/Septuagint rules
  (9+10=9, 114+115=113, 116=114+115, 147=146+147, identity elsewhere);
- verse offsets are data-driven: offset = max_verse(translation, chapter)
  minus the expected canonical verse count of the chapter's segment(s).
  The offset verses (0-2) at the head of the chapter are the counted
  superscription and map to canonical verse 0;
- irregular chapters are covered by the explicit ``EXCEPTIONS`` table; any
  chapter that fits neither the offset rule nor an exception fails the build
  loudly (never a silent guess).

Canonical verse 0 represents the superscription: the canon does not number
it, but translations that count it need a target. A translation verse that
covers several canonical verses (a merge, e.g. syn 114:8 = canonical
116:8-9) maps to the canonical range [canonical_verse_start ..
canonical_verse_end]; for regular verses start == end.

Every existing verse of every translation therefore has exactly one
canonical mapping, and every canonical verse of a chapter is covered by
exactly one translation verse (validated by ``build_psalm_map``).

Storage (`cep_public.psalm_verse_mappings`) and verification against the
live database live in `app/versification_cli.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from chunking import Verse

# Bump on any change of the mapping rules or the exceptions table.
VERSIFICATION_VERSION = 1

PSALMS_BOOK = 19

# The canonical numbering is defined by this translation's structure.
CANONICAL_ALIAS = "bsb"

# The extra Septuagint psalm; absent from the Masoretic canon, kept as
# canonical chapter 151 (LXX text, 7 numbered verses).
PSALM_151_VERSES = 7

SCHEME_MASORETIC = "masoretic"
SCHEME_SEPTUAGINT = "septuagint"

# Psalm numbering scheme per translation alias. New translations must be
# added here explicitly — build_psalm_map refuses unknown aliases.
TRANSLATION_SCHEMES: dict[str, str] = {
    "syn": SCHEME_SEPTUAGINT,
    "bti": SCHEME_SEPTUAGINT,
    "npu": SCHEME_SEPTUAGINT,
    "bsb": SCHEME_MASORETIC,
    "webus": SCHEME_MASORETIC,
    "webbe": SCHEME_MASORETIC,
    "ubh": SCHEME_MASORETIC,
}

# Explicit per-verse exceptions that the head-offset rule cannot express:
# (alias, translation chapter) -> {translation verse: (canonical chapter,
# canonical verse start, canonical verse end)}.
# syn 114:8 merges canonical 116:8 and 116:9 into one verse («Ты избавил
# душу мою от смерти … Буду ходить пред лицем Господним …»), verified
# against the stored texts.
EXCEPTIONS: dict[tuple[str, int], dict[int, tuple[int, int, int]]] = {
    ("syn", 114): {8: (116, 8, 9)},
}


@dataclass(frozen=True)
class VerseMapping:
    """One translation verse of the Psalms and its canonical coordinates."""
    chapter: int
    verse: int
    canonical_chapter: int
    canonical_verse_start: int  # 0 = superscription (unnumbered in the canon)
    canonical_verse_end: int


def septuagint_segments(
    chapter: int, canonical_counts: Mapping[int, int]
) -> list[tuple[int, int, int]]:
    """Canonical segments covered by one Septuagint-numbered chapter.

    Returns a list of (canonical_chapter, first_verse, last_verse) in
    canonical order. Most chapters map to one whole canonical chapter; the
    classical seams map to two chapters (9, 113) or to a part of one
    (114/115 -> halves of 116, 146/147 -> halves of 147).
    """
    def whole(m: int) -> tuple[int, int, int]:
        return (m, 1, canonical_counts[m])

    if chapter <= 8 or 148 <= chapter <= 150:
        return [whole(chapter)]
    if chapter == 9:
        return [whole(9), whole(10)]
    if 10 <= chapter <= 112:
        return [whole(chapter + 1)]
    if chapter == 113:
        return [whole(114), whole(115)]
    if chapter == 114:
        return [(116, 1, 9)]
    if chapter == 115:
        return [(116, 10, canonical_counts[116])]
    if 116 <= chapter <= 145:
        return [whole(chapter + 1)]
    if chapter == 146:
        return [(147, 1, 11)]
    if chapter == 147:
        return [(147, 12, canonical_counts[147])]
    if chapter == 151:
        return [(151, 1, canonical_counts[151])]
    raise ValueError(f"Septuagint Psalm chapter out of range: {chapter}")


def masoretic_segments(
    chapter: int, canonical_counts: Mapping[int, int]
) -> list[tuple[int, int, int]]:
    """Masoretic-numbered chapters map to the same canonical chapter."""
    if chapter not in canonical_counts:
        raise ValueError(f"Masoretic Psalm chapter out of range: {chapter}")
    return [(chapter, 1, canonical_counts[chapter])]


def canonical_counts_with_extras(counts: Mapping[int, int]) -> dict[int, int]:
    """Canonical verse counts (from the canonical translation) + Psalm 151."""
    result = dict(counts)
    result.setdefault(151, PSALM_151_VERSES)
    return result


def build_psalm_map(
    alias: str,
    max_verses: Mapping[int, int],
    canonical_counts: Mapping[int, int],
) -> list[VerseMapping]:
    """Build the full Psalm verse mapping of one translation.

    alias            - translation alias (must be in TRANSLATION_SCHEMES)
    max_verses       - translation chapter -> its maximal verse number
    canonical_counts - canonical chapter -> canonical verse count
                       (see canonical_counts_with_extras)

    The result is deterministic and validated: every translation verse
    1..max_verse of every chapter gets exactly one mapping, and the
    canonical segments of the chapter are consumed exactly once.
    """
    scheme = TRANSLATION_SCHEMES.get(alias)
    if scheme is None:
        raise ValueError(
            f"Unknown Psalm numbering scheme for translation '{alias}'; "
            f"add it to versification.TRANSLATION_SCHEMES"
        )
    canonical_counts = canonical_counts_with_extras(canonical_counts)
    segments_of = (
        septuagint_segments if scheme == SCHEME_SEPTUAGINT else masoretic_segments
    )

    mappings: list[VerseMapping] = []
    for chapter in sorted(max_verses):
        max_verse = max_verses[chapter]
        segments = segments_of(chapter, canonical_counts)
        exceptions = EXCEPTIONS.get((alias, chapter), {})

        expected = sum(end - start + 1 for _, start, end in segments)
        # A merge exception consumes several canonical verses with one
        # translation verse, shrinking the expected verse count.
        expected -= sum(end - start for _, start, end in exceptions.values())
        offset = max_verse - expected
        if not 0 <= offset <= 2:
            raise ValueError(
                f"{alias} Psalm {chapter}: {max_verse} verses do not fit the "
                f"expected {expected} canonical verses (offset {offset}); an "
                f"explicit EXCEPTIONS entry is required"
            )

        axis = [
            (c_chapter, verse)
            for c_chapter, start, end in segments
            for verse in range(start, end + 1)
        ]
        position = 0
        for verse in range(1, max_verse + 1):
            if verse <= offset:  # counted superscription
                mappings.append(
                    VerseMapping(chapter, verse, segments[0][0], 0, 0)
                )
                continue
            if verse in exceptions:
                c_chapter, c_start, c_end = exceptions[verse]
                if axis[position] != (c_chapter, c_start):
                    raise ValueError(
                        f"{alias} Psalm {chapter}:{verse}: exception "
                        f"{(c_chapter, c_start, c_end)} does not continue the "
                        f"canonical axis at {axis[position]}"
                    )
                position += c_end - c_start + 1
                mappings.append(
                    VerseMapping(chapter, verse, c_chapter, c_start, c_end)
                )
                continue
            c_chapter, c_verse = axis[position]
            position += 1
            mappings.append(
                VerseMapping(chapter, verse, c_chapter, c_verse, c_verse)
            )
        if position != len(axis):
            raise ValueError(
                f"{alias} Psalm {chapter}: canonical axis not fully consumed "
                f"({position} of {len(axis)})"
            )
    return mappings


class PsalmMap:
    """Bidirectional lookup over the mappings of one translation."""

    def __init__(self, mappings: Iterable[VerseMapping]):
        self._forward: dict[tuple[int, int], VerseMapping] = {}
        self._reverse: dict[tuple[int, int], tuple[int, int]] = {}
        for m in mappings:
            key = (m.chapter, m.verse)
            if key in self._forward:
                raise ValueError(f"Duplicate mapping for Psalm {key}")
            self._forward[key] = m
            for c_verse in range(m.canonical_verse_start, m.canonical_verse_end + 1):
                if c_verse == 0:
                    continue  # superscriptions may repeat (verses 1-2)
                c_key = (m.canonical_chapter, c_verse)
                if c_key in self._reverse:
                    raise ValueError(
                        f"Canonical verse {c_key} covered twice"
                    )
                self._reverse[c_key] = key

    def to_canonical(self, chapter: int, verse: int) -> tuple[int, int, int]:
        """(chapter, verse) -> (canonical chapter, verse range start, end)."""
        m = self._forward[(chapter, verse)]
        return (m.canonical_chapter, m.canonical_verse_start, m.canonical_verse_end)

    def from_canonical(self, chapter: int, verse: int) -> tuple[int, int] | None:
        """Canonical verse -> the translation verse containing it, if any."""
        return self._reverse.get((chapter, verse))


# Canonical chapters that the Septuagint tradition splits into two chapters
# (canonical chapter -> first verse of the second half). The chunking plan
# treats these verses as hard section boundaries, so no chunk ever spans
# two translation chapters of the Septuagint-numbered translations.
CANONICAL_SPLITS: dict[int, int] = {116: 10, 147: 12}


def canonicalize_psalm_chapters(
    verses_by_chapter: Mapping[int, list[Verse]],
    titles_by_chapter: Mapping[int, dict[int, str]],
    psalm_map: PsalmMap,
) -> tuple[
    dict[int, list[Verse]],
    dict[int, dict[int, str]],
    dict[tuple[int, int], tuple[int, int]],
]:
    """Convert one translation's Psalter into canonical coordinate space.

    Input: the translation's own chapters (chapter -> verses,
    chapter -> {verse: section title}). Output:

    - canonical chapter -> verses renumbered with canonical verse numbers
      (texts and paragraph flags untouched);
    - canonical chapter -> {canonical verse number: title};
    - back-map (canonical chapter, canonical verse number) ->
      (translation chapter, translation verse number).

    Chunking the canonical chapters directly makes Psalm chunk boundaries
    and plan-slot IDs identical across traditions (CHUNKING_VERSION 3): a
    Septuagint chapter covering two canonical psalms is split apart, the
    two Septuagint halves of canonical 116/147 are joined, and counted
    superscriptions become canonical verse 0. A two-verse superscription
    gets the placeholder numbers -1 and 0 to keep verse numbers unique and
    ordered; IDs clamp negatives to 000. A merged verse (syn 114:8 =
    canonical 116:8-9) keeps the start of its canonical range as its
    number.
    """
    out_verses: dict[int, list[Verse]] = {}
    out_titles: dict[int, dict[int, str]] = {}
    back: dict[tuple[int, int], tuple[int, int]] = {}
    for chapter in sorted(verses_by_chapter):
        ordered = sorted(
            verses_by_chapter[chapter], key=lambda verse: verse.verse_number
        )
        superscriptions = sum(
            1 for verse in ordered
            if psalm_map.to_canonical(chapter, verse.verse_number)[1] == 0
        )
        seen_superscriptions = 0
        number_of: dict[int, tuple[int, int]] = {}
        for verse in ordered:
            c_chapter, c_start, _c_end = psalm_map.to_canonical(
                chapter, verse.verse_number
            )
            if c_start == 0:
                number = seen_superscriptions - (superscriptions - 1)
                seen_superscriptions += 1
            else:
                number = c_start
            out_verses.setdefault(c_chapter, []).append(
                Verse(number, verse.text, verse.start_paragraph)
            )
            back[(c_chapter, number)] = (chapter, verse.verse_number)
            number_of[verse.verse_number] = (c_chapter, number)
        for verse_number, title in (titles_by_chapter.get(chapter) or {}).items():
            if verse_number in number_of:
                c_chapter, number = number_of[verse_number]
                out_titles.setdefault(c_chapter, {})[number] = title
    for c_chapter in out_verses:
        out_titles.setdefault(c_chapter, {})
    return out_verses, out_titles, back
