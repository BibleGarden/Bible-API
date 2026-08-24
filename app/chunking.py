"""
Structural chunking of Bible texts for RAG retrieval.

Builds reproducible, translation-independent chunks from the existing Bible
structure: section titles (translation_titles.before_translation_verse),
paragraphs (translation_verses.start_paragraph) and verse coordinates.

Design (see architect/adr/0001-structural-chunking.md):

- A "canonical boundary plan" is computed once from a pivot translation.
  Every chunk is identified by canonical coordinates that are independent of
  the translation: book, chapter and the owned verse range.
- The plan is then applied to any translation: verses are assigned to chunks
  by verse coordinates, so parallel translations share the same canonical IDs.
- Hard boundaries: chapters and section titles are never crossed.
- Long sections are split ONLY at natural boundaries (paragraph first,
  verse inside an oversized paragraph). Short paragraphs are merged together
  until the configured target size is reached.
- Overlap is added with whole natural units (paragraphs, falling back to the
  last verse of the previous chunk) and never crosses a section boundary.
- Determinism: the algorithm is pure; the same input rows always produce the
  same IDs, boundaries and texts. CHUNKING_VERSION is part of the canonical
  ID, so any change of the algorithm produces a new set of IDs.
- Canonical coordinates (v3): the caller chunks the Psalter in canonical
  english-masoretic coordinate SPACE — verses of every translation are
  converted with versification.canonicalize_psalm_chapters before planning
  and application, and the resulting chapter/verse numbers here ARE
  canonical. The chunker itself stays coordinate-agnostic; the plan-slot
  IDs are therefore shared across traditions (see ADR 0003).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# Bump on ANY change of the boundary/text rules or of the ID coordinate
# semantics. The version is part of the canonical chunk ID, so downstream
# embedding indexes can be rebuilt idempotently per version.
# v2: same boundaries/texts as v1; ID coordinates are canonical
# (english-masoretic Psalm numbering) instead of raw pivot coordinates.
# v3: the Psalter is chunked in canonical coordinate SPACE (verses of every
# translation are converted to canonical chapters/verse numbers before
# planning and application, see versification.canonicalize_psalm_chapters),
# so Psalm chunk boundaries align across traditions and the plan-slot IDs
# are shared again. Non-Psalm boundaries/texts are unchanged since v1.
CHUNKING_VERSION = 3


@dataclass(frozen=True)
class ChunkingConfig:
    """Target sizes are measured in characters of the pivot translation."""
    target_chars: int = 1200      # preferred chunk size
    min_chars: int = 400          # chunks below this are merged when possible
    max_chars: int = 2400         # never grow a chunk past this by merging units
    overlap_units: int = 1        # whole paragraphs/pieces taken from the previous chunk
    overlap_max_chars: int = 400  # skip overlap units bigger than this (verse fallback)


@dataclass(frozen=True)
class Verse:
    """Minimal verse projection used by the chunker."""
    verse_number: int
    text: str
    start_paragraph: bool = False


@dataclass(frozen=True)
class PlannedChunk:
    """Canonical boundaries of one chunk, computed from the pivot translation.

    own_start..own_end   - verse range this chunk "owns" (non-overlapping,
                           chunks of a chapter partition the verse axis).
    text_start           - first verse included in the chunk text; lower than
                           own_start when the chunk starts with overlap taken
                           from the previous chunk of the same section.
    """
    book_number: int
    chapter_number: int
    own_start: int
    own_end: int
    text_start: int


@dataclass(frozen=True)
class Chunk:
    """A materialized chunk of one translation."""
    canonical_id: str
    book_number: int
    chapter_number: int
    verse_number_start: int   # actual first verse present in the text
    verse_number_end: int     # actual last verse present in the text
    verse_count: int
    title: Optional[str]
    text: str


def make_canonical_id(
    book_number: int,
    chapter_number: int,
    own_start: int,
    own_end: int,
    version: int = CHUNKING_VERSION,
) -> str:
    """Translation-independent chunk ID, e.g. 'v3:01.001.001-005'.

    Negative verse numbers (canonical-space placeholders for the extra
    verses of a multi-verse superscription, which the canon does not
    number) are clamped to 000.
    """
    own_start = max(own_start, 0)
    own_end = max(own_end, 0)
    return (
        f"v{version}:{book_number:02d}.{chapter_number:03d}"
        f".{own_start:03d}-{own_end:03d}"
    )


def _clean_text(verse: Verse) -> str:
    return verse.text.strip()


def _verses_len(verses: list[Verse]) -> int:
    """Length of the verses joined with single spaces (empty verses skipped)."""
    texts = [t for t in (_clean_text(v) for v in verses) if t]
    if not texts:
        return 0
    return sum(len(t) for t in texts) + len(texts) - 1


def _split_paragraphs(verses: list[Verse], title_verses: set[int]) -> list[list[Verse]]:
    """Group verses into paragraphs.

    A new paragraph starts at the first verse, at start_paragraph=1 and at any
    verse that has a section title before it (a title always implies a break).
    """
    paragraphs: list[list[Verse]] = []
    for verse in verses:
        if not paragraphs or verse.start_paragraph or verse.verse_number in title_verses:
            paragraphs.append([verse])
        else:
            paragraphs[-1].append(verse)
    return paragraphs


def _split_sections(
    paragraphs: list[list[Verse]], title_verses: set[int]
) -> list[list[list[Verse]]]:
    """Group paragraphs into sections; a title before the first verse of a
    paragraph starts a new section. A chapter without titles is one section."""
    sections: list[list[list[Verse]]] = []
    for paragraph in paragraphs:
        if not sections or paragraph[0].verse_number in title_verses:
            sections.append([paragraph])
        else:
            sections[-1].append(paragraph)
    return sections


def _split_long_paragraph(
    paragraph: list[Verse], config: ChunkingConfig
) -> list[list[Verse]]:
    """Split an oversized paragraph into pieces at verse boundaries.

    Greedy up to target_chars; a trailing piece below min_chars is merged back
    into the previous piece.
    """
    pieces: list[list[Verse]] = []
    current: list[Verse] = []
    for verse in paragraph:
        if current and _verses_len(current + [verse]) > config.target_chars:
            pieces.append(current)
            current = [verse]
        else:
            current.append(verse)
    if current:
        if (
            pieces
            and _verses_len(current) < config.min_chars
            and _verses_len(pieces[-1] + current) <= config.max_chars
        ):
            pieces[-1].extend(current)
        else:
            pieces.append(current)
    return pieces


def _section_units(
    section: list[list[Verse]], config: ChunkingConfig
) -> list[list[Verse]]:
    """Natural units of a section: paragraphs, oversized ones split by verses."""
    units: list[list[Verse]] = []
    for paragraph in section:
        if len(paragraph) > 1 and _verses_len(paragraph) > config.max_chars:
            units.extend(_split_long_paragraph(paragraph, config))
        else:
            units.append(paragraph)
    return units


def _group_len(group: list[list[Verse]]) -> int:
    unit_lens = [_verses_len(unit) for unit in group]
    return sum(unit_lens) + 2 * (len(unit_lens) - 1)


def _group_units(
    units: list[list[Verse]], config: ChunkingConfig
) -> list[list[list[Verse]]]:
    """Greedy grouping of units into chunks of ~target_chars.

    Rules:
    - close the current group when adding the next unit would push it past
      target_chars (once min_chars is reached) or past max_chars in any case;
    - a trailing group below min_chars is merged back into the previous one
      when the result stays within max_chars (min_chars is a soft preference,
      max_chars wins at section ends).
    """
    groups: list[list[list[Verse]]] = []
    current: list[list[Verse]] = []
    for unit in units:
        if current:
            candidate_len = _group_len(current) + 2 + _verses_len(unit)
            over_target = (
                candidate_len > config.target_chars
                and _group_len(current) >= config.min_chars
            )
            if over_target or candidate_len > config.max_chars:
                groups.append(current)
                current = []
        current.append(unit)
    if current:
        groups.append(current)
    if len(groups) >= 2 and _group_len(groups[-1]) < config.min_chars:
        merged_len = _group_len(groups[-2]) + 2 + _group_len(groups[-1])
        if merged_len <= config.max_chars:
            last = groups.pop()
            groups[-1].extend(last)
    return groups


def _overlap_text_start(
    prev_group: list[list[Verse]], config: ChunkingConfig
) -> Optional[int]:
    """First verse of the overlap taken from the previous group.

    Overlap is config.overlap_units whole units from the tail of the previous
    group. When the previous group is too small to give away whole units, or
    the tail units are longer than overlap_max_chars, fall back to the last
    verse of the previous group. Returns None when no overlap is possible
    (the previous group is a single one-verse unit, or even its last verse is
    longer than overlap_max_chars).
    """
    if config.overlap_units <= 0 or config.overlap_max_chars <= 0:
        return None
    if len(prev_group) > config.overlap_units:
        tail = prev_group[-config.overlap_units:]
        if _group_len(tail) <= config.overlap_max_chars:
            return tail[0][0].verse_number
    last_unit = prev_group[-1]
    last_verse = last_unit[-1]
    is_partial = len(prev_group) > 1 or len(last_unit) > 1
    if is_partial and _verses_len([last_verse]) <= config.overlap_max_chars:
        return last_verse.verse_number
    return None


def build_chapter_plan(
    book_number: int,
    chapter_number: int,
    verses: list[Verse],
    title_verses: set[int],
    config: ChunkingConfig,
) -> list[PlannedChunk]:
    """Compute canonical chunk boundaries for one chapter of the pivot."""
    ordered = sorted(verses, key=lambda v: v.verse_number)
    if not ordered:
        return []
    paragraphs = _split_paragraphs(ordered, title_verses)
    sections = _split_sections(paragraphs, title_verses)

    planned: list[PlannedChunk] = []
    for section in sections:
        units = _section_units(section, config)
        groups = _group_units(units, config)
        for index, group in enumerate(groups):
            own_start = group[0][0].verse_number
            own_end = group[-1][-1].verse_number
            text_start = own_start
            if index > 0:
                overlap_start = _overlap_text_start(groups[index - 1], config)
                if overlap_start is not None:
                    text_start = overlap_start
            planned.append(
                PlannedChunk(
                    book_number=book_number,
                    chapter_number=chapter_number,
                    own_start=own_start,
                    own_end=own_end,
                    text_start=text_start,
                )
            )
    return planned


def _build_text(verses: list[Verse], title_verses: set[int]) -> str:
    """Chunk text: paragraphs separated by blank lines, verses by spaces."""
    non_empty = [v for v in verses if _clean_text(v)]
    paragraphs = _split_paragraphs(non_empty, title_verses)
    return "\n\n".join(
        " ".join(_clean_text(v) for v in paragraph) for paragraph in paragraphs
    )


def apply_chapter_plan(
    planned: list[PlannedChunk],
    book_number: int,
    chapter_number: int,
    verses: list[Verse],
    titles: dict[int, str],
    config: ChunkingConfig,
    version: int = CHUNKING_VERSION,
    _refine: bool = True,
) -> list[Chunk]:
    """Materialize chunks of one chapter of a translation from the plan.

    Ownership: chunk i owns every verse v with own_start_i <= v < own_start_{i+1}
    (the first chunk also absorbs earlier verses, the last one absorbs trailing
    verses missing from the pivot), so each verse of the translation lands in
    exactly one chunk. Overlap verses (text_start..own_start-1) are prepended
    to the text only.

    When the chapter is absent from the plan, a fallback plan is built from
    the translation's own structure.

    Refinement: when the verses owned by one planned chunk are longer than
    max_chars in THIS translation (versification offsets against the pivot,
    trailing verses absorbed by the last chunk, verbose translations), the
    range is re-chunked at the translation's own natural boundaries. The
    refined canonical IDs are still coordinate-based and deterministic, but
    may differ between translations for such ranges.

    titles: verse_number -> section title of THIS translation (metadata and
    paragraph breaks); boundaries come from the plan.
    """
    ordered = sorted(verses, key=lambda v: v.verse_number)
    if not ordered:
        return []
    if not planned:
        planned = build_chapter_plan(
            book_number, chapter_number, ordered, set(titles), config
        )
    title_verses = set(titles)

    chunks: list[Chunk] = []
    for index, plan in enumerate(planned):
        # The first chunk absorbs ALL earlier verses, including negative
        # canonical-space placeholders (the extra verses of a multi-verse
        # superscription are numbered -1, 0 — see versification).
        lower = plan.own_start if index > 0 else None
        upper = planned[index + 1].own_start if index + 1 < len(planned) else None
        owned = [
            v for v in ordered
            if (lower is None or v.verse_number >= lower)
            and (upper is None or v.verse_number < upper)
        ]
        if not owned:
            continue
        if _refine and len(owned) > 1 and _verses_len(owned) > config.max_chars:
            sub_plan = build_chapter_plan(
                book_number, chapter_number, owned, title_verses, config
            )
            if len(sub_plan) > 1:
                chunks.extend(
                    apply_chapter_plan(
                        sub_plan, book_number, chapter_number,
                        owned, titles, config, version, _refine=False,
                    )
                )
                continue
        overlap = [
            v for v in ordered
            if plan.text_start <= v.verse_number < plan.own_start
        ]
        included = overlap + owned
        text = _build_text(included, title_verses)
        if not text:
            continue
        title_candidates = [
            tv for tv in titles if tv <= owned[0].verse_number
        ]
        title = titles[max(title_candidates)] if title_candidates else None
        chunks.append(
            Chunk(
                canonical_id=make_canonical_id(
                    book_number, chapter_number,
                    plan.own_start, plan.own_end, version,
                ),
                book_number=book_number,
                chapter_number=chapter_number,
                verse_number_start=included[0].verse_number,
                verse_number_end=included[-1].verse_number,
                verse_count=len(included),
                title=title,
                text=text,
            )
        )
    return chunks


ChapterKey = tuple[int, int]  # (book_number, chapter_number)


def build_plan(
    pivot_chapters: dict[ChapterKey, tuple[list[Verse], set[int]]],
    config: ChunkingConfig,
) -> dict[ChapterKey, list[PlannedChunk]]:
    """Canonical boundary plan for a whole pivot translation.

    pivot_chapters: (book, chapter) -> (verses, verse numbers having a title).
    """
    plan: dict[ChapterKey, list[PlannedChunk]] = {}
    for (book_number, chapter_number) in sorted(pivot_chapters):
        verses, title_verses = pivot_chapters[(book_number, chapter_number)]
        plan[(book_number, chapter_number)] = build_chapter_plan(
            book_number, chapter_number, verses, title_verses, config
        )
    return plan


def chunk_translation(
    plan: dict[ChapterKey, list[PlannedChunk]],
    chapters: dict[ChapterKey, tuple[list[Verse], dict[int, str]]],
    config: ChunkingConfig,
    version: int = CHUNKING_VERSION,
) -> list[Chunk]:
    """Materialize all chunks of a translation from the canonical plan.

    chapters: (book, chapter) -> (verses, {verse_number: title_text}).
    """
    chunks: list[Chunk] = []
    for (book_number, chapter_number) in sorted(chapters):
        verses, titles = chapters[(book_number, chapter_number)]
        chunks.extend(
            apply_chapter_plan(
                plan.get((book_number, chapter_number), []),
                book_number, chapter_number,
                verses, titles, config, version,
            )
        )
    return chunks
