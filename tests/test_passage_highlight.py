"""
Coordinates of the key-verse highlight (app/passage_highlight.py).

The rerank stage answers with a span of VERSE MARKERS into the verse list
the server rendered; these tests pin how that span becomes canonical and
translation coordinates — the Psalter being the whole point, since the
translations there number chapters and verses differently (ADR 0003).

The Psalm maps are built from the same fixture the versification tests use
(tests/data/psalm_max_verses.json — a snapshot of MAX(verse_number) per
Psalm chapter of every translation in cep_public).
"""

import json
import os
from pathlib import Path

import pytest

os.environ.setdefault("API_KEY", "test-api-key")

from passage_highlight import (
    Highlight,
    VerseSpan,
    from_canonical_span,
    load_psalm_maps,
    resolve_highlight,
    to_canonical_span,
)
from retrieval import PassageText, VerseText
from versification import (
    PSALMS_BOOK,
    VERSIFICATION_VERSION,
    PsalmMap,
    build_psalm_map,
    canonical_counts_with_extras,
)

FIXTURE = Path(__file__).parent / "data" / "psalm_max_verses.json"
JOHN = 43  # a non-Psalm book: coordinates are canonical as they stand
GENESIS = 1

# translation codes of the indexed corpus (cep_public)
SYN, BSB, UBH = 1, 16, 20
BTI = 11  # rendered from its own verses (ADR 0007), not indexed


@pytest.fixture(scope="module")
def max_verses():
    return {
        alias: {int(chapter): mx for chapter, mx in chapters.items()}
        for alias, chapters in json.loads(FIXTURE.read_text()).items()
    }


@pytest.fixture(scope="module")
def psalm_maps(max_verses):
    counts = canonical_counts_with_extras(max_verses["bsb"])
    return {
        code: PsalmMap(build_psalm_map(alias, max_verses[alias], counts))
        for code, alias in ((SYN, "syn"), (BSB, "bsb"), (UBH, "ubh"))
    }


def passage(translation, chapter, verse_numbers, alias="syn"):
    return PassageText(
        translation=translation,
        alias=alias,
        book_number=PSALMS_BOOK,
        chapter_number=chapter,
        verse_number_start=verse_numbers[0],
        verse_number_end=verse_numbers[-1],
        title=None,
        text="…",
        verses=[VerseText(number, f"стих {number}") for number in verse_numbers],
    )


# ---------------------------------------------------------------------------
# Coordinate conversion
# ---------------------------------------------------------------------------

def test_outside_the_psalms_the_conversion_is_the_identity():
    span = VerseSpan(chapter=14, verse_start=27, verse_end=27)

    assert to_canonical_span(JOHN, span, None) == span
    assert from_canonical_span(JOHN, span, None) == span


def test_septuagint_chapter_shift_is_applied(psalm_maps):
    """syn 22 («Господь — Пастырь мой») is canonical 23."""
    span = VerseSpan(chapter=22, verse_start=1, verse_end=2)

    canonical = to_canonical_span(PSALMS_BOOK, span, psalm_maps[SYN])

    assert canonical == VerseSpan(23, 1, 2)
    assert from_canonical_span(PSALMS_BOOK, canonical, psalm_maps[SYN]) == span


def test_masoretic_translation_needs_no_shift(psalm_maps):
    span = VerseSpan(chapter=23, verse_start=4, verse_end=4)

    assert to_canonical_span(PSALMS_BOOK, span, psalm_maps[BSB]) == span


def test_counted_superscription_shifts_the_verse_numbers(psalm_maps):
    """syn 3:2-3 and ubh 3:2-3 are both canonical 3:1-2 — the counted
    superscription is verse 1 of the translation and unnumbered in the
    canon."""
    span = VerseSpan(chapter=3, verse_start=2, verse_end=3)

    assert to_canonical_span(PSALMS_BOOK, span, psalm_maps[SYN]) == VerseSpan(3, 1, 2)
    assert to_canonical_span(PSALMS_BOOK, span, psalm_maps[UBH]) == VerseSpan(3, 1, 2)


def test_a_highlighted_superscription_is_canonical_verse_zero(psalm_maps):
    span = VerseSpan(chapter=51, verse_start=1, verse_end=2)  # double title

    canonical = to_canonical_span(PSALMS_BOOK, span, psalm_maps[UBH])

    assert canonical == VerseSpan(51, 0, 0)
    # canonical verse 0 has no reverse entry: another translation cannot be
    # given a guessed verse for it
    assert from_canonical_span(PSALMS_BOOK, canonical, psalm_maps[SYN]) is None


def test_a_merged_verse_covers_a_canonical_range(psalm_maps):
    """syn 114:8 carries canonical 116:8-9 in one verse."""
    span = VerseSpan(chapter=114, verse_start=8, verse_end=8)

    assert to_canonical_span(PSALMS_BOOK, span, psalm_maps[SYN]) == VerseSpan(116, 8, 9)


def test_psalms_without_a_mapping_yield_no_highlight():
    span = VerseSpan(chapter=22, verse_start=1, verse_end=1)

    assert to_canonical_span(PSALMS_BOOK, span, None) is None
    assert from_canonical_span(PSALMS_BOOK, span, None) is None


def test_an_unknown_verse_yields_no_highlight(psalm_maps):
    span = VerseSpan(chapter=22, verse_start=1, verse_end=999)

    assert to_canonical_span(PSALMS_BOOK, span, psalm_maps[SYN]) is None


# ---------------------------------------------------------------------------
# resolve_highlight
# ---------------------------------------------------------------------------

def test_resolve_maps_marker_indexes_onto_both_coordinate_systems(psalm_maps):
    shown = passage(SYN, 22, [1, 2, 3, 4, 5, 6])

    resolved = resolve_highlight(
        PSALMS_BOOK, shown, shown, (4, 4), psalm_maps
    )

    assert resolved == Highlight(
        canonical=VerseSpan(23, 4, 4), passage=VerseSpan(22, 4, 4)
    )


def test_resolve_uses_verse_numbers_not_marker_numbers(psalm_maps):
    """The chunk starts at verse 4, so marker 1 is verse 4."""
    shown = passage(SYN, 22, [4, 5, 6])

    resolved = resolve_highlight(PSALMS_BOOK, shown, shown, (1, 2), psalm_maps)

    assert resolved.passage == VerseSpan(22, 4, 5)
    assert resolved.canonical == VerseSpan(23, 4, 5)


@pytest.mark.parametrize("indices", [(0, 1), (1, 7), (7, 7), (3, 2)])
def test_resolve_refuses_a_span_outside_the_shown_verses(psalm_maps, indices):
    shown = passage(SYN, 22, [1, 2, 3, 4, 5, 6])

    assert resolve_highlight(PSALMS_BOOK, shown, shown, indices, psalm_maps) is None


def test_resolve_refuses_when_no_verses_were_shown(psalm_maps):
    shown = passage(SYN, 22, [1, 2])
    shown.verses = []

    assert resolve_highlight(PSALMS_BOOK, shown, shown, (1, 1), psalm_maps) is None


def test_resolve_renders_another_translation_through_the_canon(psalm_maps):
    """A second translation of the language would be numbered its own way:
    the canonical span is the bridge, never the marker index."""
    shown = passage(SYN, 22, [1, 2, 3, 4, 5, 6])
    target = passage(BSB, 23, [1, 2, 3, 4, 5, 6], alias="bsb")

    resolved = resolve_highlight(PSALMS_BOOK, shown, target, (4, 4), psalm_maps)

    assert resolved == Highlight(
        canonical=VerseSpan(23, 4, 4), passage=VerseSpan(23, 4, 4)
    )


def test_a_merged_verse_may_stretch_the_canonical_span_past_three(psalm_maps):
    """m5: the 3-verse rule binds the numbering the model saw. syn 114:6-8
    is 3 verses there and canonical 116:6-9 — four. The exact canonical
    range is kept: both systems must point at the same words, and a
    truncated canonical reference would not (ADR 0005)."""
    shown = passage(SYN, 114, [6, 7, 8])

    resolved = resolve_highlight(PSALMS_BOOK, shown, shown, (1, 3), psalm_maps)

    assert resolved.passage == VerseSpan(114, 6, 8)
    assert resolved.canonical == VerseSpan(116, 6, 9)


def test_resolve_refuses_a_mapped_span_outside_the_served_passage(psalm_maps):
    """M2: the two translations chunk the corpus independently, so a span
    converted through the canon can land outside the window the other one
    returns. The contract says the highlight is a sub-range of the passage
    served, so such a span yields no highlight at all."""
    shown = passage(SYN, 22, [1, 2, 3, 4, 5, 6])
    # bsb 23 exists, but this chunk of it stops at verse 3
    target = passage(BSB, 23, [1, 2, 3], alias="bsb")

    assert resolve_highlight(PSALMS_BOOK, shown, target, (4, 4), psalm_maps) is None
    # a span the target passage does cover still resolves
    assert resolve_highlight(
        PSALMS_BOOK, shown, target, (2, 2), psalm_maps
    ) == Highlight(canonical=VerseSpan(23, 2, 2), passage=VerseSpan(23, 2, 2))


def test_resolve_refuses_a_mapped_span_in_another_chapter(psalm_maps):
    """The chapter is checked too: a Septuagint-shifted psalm can map onto a
    chapter the served passage does not belong to."""
    shown = passage(SYN, 22, [1, 2, 3, 4, 5, 6])
    target = passage(BSB, 22, [1, 2, 3, 4, 5, 6], alias="bsb")

    assert resolve_highlight(PSALMS_BOOK, shown, target, (4, 4), psalm_maps) is None


def test_resolve_refuses_a_number_the_served_translation_does_not_have():
    """F1: outside the Psalms the conversion between the two numberings is
    the identity, so a range check cannot notice that the served translation
    has no such verse. `bti` says Genesis 35:9 and 35:10 in one verse and
    numbers it 9, so its verses run 9, 11, 12… — a `syn` highlight of verse
    10 lies inside the bti window and is absent from its verse list. The
    contract promises a highlight is findable in `verses`; this one is not,
    so it is dropped rather than served."""
    def genesis(translation, alias, verse_numbers):
        return PassageText(
            translation=translation, alias=alias, book_number=GENESIS,
            chapter_number=35, verse_number_start=verse_numbers[0],
            verse_number_end=verse_numbers[-1], title=None, text="…",
            verses=[VerseText(n, f"стих {n}") for n in verse_numbers],
        )

    shown = genesis(SYN, "syn", [9, 10, 11, 12])
    target = genesis(BTI, "bti", [9, 11, 12])

    # marker 2 is syn 35:10 — inside the bti range, missing from its verses
    assert resolve_highlight(GENESIS, shown, target, (2, 2), {}) is None
    # a number bti does have still resolves
    assert resolve_highlight(GENESIS, shown, target, (3, 3), {}) == Highlight(
        canonical=VerseSpan(35, 11, 11), passage=VerseSpan(35, 11, 11)
    )
    # a span whose ENDS both exist is served even though it steps over the
    # hole: the client selects by number and simply finds nothing at 10
    assert resolve_highlight(GENESIS, shown, target, (1, 3), {}) == Highlight(
        canonical=VerseSpan(35, 9, 11), passage=VerseSpan(35, 9, 11)
    )


def test_a_passage_without_verses_is_not_checked_for_membership():
    """The guarantee is conditional on `verses` being served at all: a
    passage whose verse load failed publishes only `text`, and the highlight
    keeps degrading no further than it did before."""
    shown = PassageText(
        translation=SYN, alias="syn", book_number=GENESIS, chapter_number=35,
        verse_number_start=9, verse_number_end=12, title=None, text="…",
        verses=[VerseText(n, f"стих {n}") for n in (9, 10, 11, 12)],
    )
    target = PassageText(
        translation=BTI, alias="bti", book_number=GENESIS, chapter_number=35,
        verse_number_start=9, verse_number_end=12, title=None, text="…",
    )

    assert resolve_highlight(GENESIS, shown, target, (2, 2), {}) == Highlight(
        canonical=VerseSpan(35, 10, 10), passage=VerseSpan(35, 10, 10)
    )


def test_resolve_gives_up_when_the_other_translation_cannot_be_mapped():
    shown = passage(SYN, 22, [1, 2])
    target = passage(BSB, 23, [1, 2], alias="bsb")

    assert resolve_highlight(PSALMS_BOOK, shown, target, (1, 1), {}) is None


# ---------------------------------------------------------------------------
# Loading the stored mapping
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def execute(self, _sql, params=None):
        self.params = params

    def fetchall(self):
        return self.rows


def mapping_row(translation, chapter, verse, c_chapter, c_start, c_end):
    return {
        "translation": translation,
        "chapter_number": chapter,
        "verse_number": verse,
        "canonical_chapter": c_chapter,
        "canonical_verse_start": c_start,
        "canonical_verse_end": c_end,
    }


def test_load_psalm_maps_groups_rows_per_translation():
    cursor = FakeCursor([
        mapping_row(SYN, 22, 1, 23, 1, 1),
        mapping_row(SYN, 22, 2, 23, 2, 2),
        mapping_row(BSB, 23, 1, 23, 1, 1),
    ])

    maps = load_psalm_maps(cursor)

    assert set(maps) == {SYN, BSB}
    assert maps[SYN].to_canonical(22, 2) == (23, 2, 2)
    assert cursor.params == (VERSIFICATION_VERSION, PSALMS_BOOK)


def test_an_inconsistent_stored_mapping_skips_only_that_translation(caplog):
    cursor = FakeCursor([
        mapping_row(SYN, 22, 1, 23, 1, 1),
        mapping_row(SYN, 22, 2, 23, 1, 1),   # canonical verse covered twice
        mapping_row(BSB, 23, 1, 23, 1, 1),
    ])

    with caplog.at_level("WARNING"):
        maps = load_psalm_maps(cursor)

    assert set(maps) == {BSB}
    assert "inconsistent" in caplog.text
