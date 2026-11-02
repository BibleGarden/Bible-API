"""
Tests of the Psalm versification mapping (app/versification.py).

The fixture tests/data/psalm_max_verses.json is a snapshot of
MAX(verse_number) per Psalm chapter for every translation in cep_public —
the exact input the builder receives from the database. All seam and shift
expectations below were verified against the stored verse texts (see
architect/adr/0003-psalm-versification-canon.md).
"""

import json
from pathlib import Path

import numpy as np
import pytest

from chunking import Verse
from scripture_select import reference_windows
from vector_index import InMemoryVectorIndex
from versification import (
    PSALM_151_VERSES,
    TRANSLATION_VERSIFICATIONS,
    PsalmMap,
    build_psalm_map,
    canonical_counts_with_extras,
    canonicalize_psalm_chapters,
    check_chapters,
    septuagint_segments,
    to_reference_numbering,
)

FIXTURE = Path(__file__).parent / "data" / "psalm_max_verses.json"
ALL_ALIASES = ("syn", "bti", "bsb", "webus", "ubh", "npu", "webbe")


@pytest.fixture(scope="module")
def max_verses():
    return {
        alias: {int(chapter): mx for chapter, mx in chapters.items()}
        for alias, chapters in json.loads(FIXTURE.read_text()).items()
    }


@pytest.fixture(scope="module")
def canonical_counts(max_verses):
    return canonical_counts_with_extras(max_verses["bsb"])


@pytest.fixture(scope="module")
def maps(max_verses, canonical_counts):
    return {
        alias: PsalmMap(
            build_psalm_map(alias, max_verses[alias], canonical_counts)
        )
        for alias in ALL_ALIASES
    }


# ---------------------------------------------------------------------------
# Canonical translations are identity
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("alias", ["bsb", "webus", "webbe"])
def test_canonical_translations_are_identity(maps, max_verses, alias):
    psalm_map = maps[alias]
    for chapter, max_verse in max_verses[alias].items():
        for verse in range(1, max_verse + 1):
            assert psalm_map.to_canonical(chapter, verse) == (chapter, verse, verse)


# ---------------------------------------------------------------------------
# Septuagint chapter seams (verified against verse texts in cep_public)
# ---------------------------------------------------------------------------

def test_seam_psalm_9_10_merged_in_septuagint(maps):
    syn = maps["syn"]
    assert syn.to_canonical(9, 1) == (9, 0, 0)      # counted superscription
    assert syn.to_canonical(9, 2) == (9, 1, 1)      # «Буду славить Тебя...»
    assert syn.to_canonical(9, 21) == (9, 20, 20)
    assert syn.to_canonical(9, 22) == (10, 1, 1)    # «Для чего, Господи...»
    assert syn.to_canonical(9, 39) == (10, 18, 18)
    # regular +1 chapter shift after the seam
    assert syn.to_canonical(10, 1) == (11, 1, 1)


def test_seam_psalm_113_covers_masoretic_114_and_115(maps):
    syn = maps["syn"]
    assert syn.to_canonical(113, 1) == (114, 1, 1)  # «Когда вышел Израиль...»
    assert syn.to_canonical(113, 8) == (114, 8, 8)
    assert syn.to_canonical(113, 9) == (115, 1, 1)  # «Не нам, Господи...»
    assert syn.to_canonical(113, 26) == (115, 18, 18)


def test_split_masoretic_116_between_septuagint_114_and_115(maps):
    syn = maps["syn"]
    assert syn.to_canonical(114, 1) == (116, 1, 1)  # «Я радуюсь...»
    # the explicit exception: syn 114:8 merges canonical 116:8-9
    assert syn.to_canonical(114, 8) == (116, 8, 9)
    assert syn.to_canonical(115, 1) == (116, 10, 10)
    assert syn.to_canonical(115, 10) == (116, 19, 19)
    # bti/npu have no merge: their 114 has 9 verses
    assert maps["bti"].to_canonical(114, 8) == (116, 8, 8)
    assert maps["bti"].to_canonical(114, 9) == (116, 9, 9)
    assert maps["npu"].to_canonical(115, 1) == (116, 10, 10)


def test_split_masoretic_147_between_septuagint_146_and_147(maps):
    syn = maps["syn"]
    assert syn.to_canonical(146, 1) == (147, 1, 1)   # «Хвалите Господа...»
    assert syn.to_canonical(146, 11) == (147, 11, 11)
    assert syn.to_canonical(147, 1) == (147, 12, 12)  # «Хвали, Иерусалим...»
    assert syn.to_canonical(147, 9) == (147, 20, 20)
    assert syn.to_canonical(148, 1) == (148, 1, 1)    # identity resumes


def test_septuagint_psalm_151_is_canonical_151(maps):
    syn = maps["syn"]
    assert syn.to_canonical(151, 1) == (151, 1, 1)
    assert syn.to_canonical(151, PSALM_151_VERSES) == (151, 7, 7)


# ---------------------------------------------------------------------------
# Superscription (counted title) shifts
# ---------------------------------------------------------------------------

def test_syn_counted_superscriptions(maps):
    syn = maps["syn"]
    assert syn.to_canonical(3, 1) == (3, 0, 0)
    assert syn.to_canonical(3, 2) == (3, 1, 1)   # «Господи! как умножились...»
    # double superscription (masoretic 52): syn 51:1-2 are both the title
    assert syn.to_canonical(51, 1) == (52, 0, 0)
    assert syn.to_canonical(51, 2) == (52, 0, 0)
    assert syn.to_canonical(51, 3) == (52, 1, 1)  # «Что хвалишься злодейством...»


def test_ubh_hebrew_style_verse_shifts(maps):
    ubh = maps["ubh"]
    # masoretic chapters, superscription counted as verse 1
    assert ubh.to_canonical(3, 1) == (3, 0, 0)   # «Псалом Давида, коли він...»
    assert ubh.to_canonical(3, 2) == (3, 1, 1)
    # double superscription: ubh 51:1-2
    assert ubh.to_canonical(51, 1) == (51, 0, 0)
    assert ubh.to_canonical(51, 2) == (51, 0, 0)
    assert ubh.to_canonical(51, 3) == (51, 1, 1)  # «Помилуй м'я, Боже...»
    # superscription merged into verse 1 -> no shift (ubh 103 «Давида. Благослови...»)
    assert ubh.to_canonical(103, 1) == (103, 1, 1)
    # no chapter renumbering
    assert ubh.to_canonical(116, 1) == (116, 1, 1)
    assert ubh.to_canonical(147, 12) == (147, 12, 12)


def test_bti_numbering_gap_maps_by_verse_number(maps):
    # bti skips 67:14 (text merged into a neighbour); numbers, not
    # positions, drive the mapping, so verses after the gap stay aligned.
    bti = maps["bti"]
    assert bti.to_canonical(67, 13) == (68, 12, 12)
    assert bti.to_canonical(67, 15) == (68, 14, 14)


# ---------------------------------------------------------------------------
# Completeness, uniqueness, determinism
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("alias", ALL_ALIASES)
def test_every_verse_has_exactly_one_mapping(max_verses, canonical_counts, alias):
    mappings = build_psalm_map(alias, max_verses[alias], canonical_counts)
    keys = [(m.chapter, m.verse) for m in mappings]
    assert len(keys) == len(set(keys))
    expected = {
        (chapter, verse)
        for chapter, max_verse in max_verses[alias].items()
        for verse in range(1, max_verse + 1)
    }
    assert set(keys) == expected


@pytest.mark.parametrize("alias", ALL_ALIASES)
def test_canonical_axis_is_partitioned(max_verses, canonical_counts, alias):
    """Every canonical verse of every psalm is covered exactly once."""
    mappings = build_psalm_map(alias, max_verses[alias], canonical_counts)
    covered = []
    for m in mappings:
        for verse in range(max(m.canonical_verse_start, 1),
                           m.canonical_verse_end + 1):
            covered.append((m.canonical_chapter, verse))
    assert len(covered) == len(set(covered))
    chapters = set(m.canonical_chapter for m in mappings)
    expected = {
        (chapter, verse)
        for chapter in chapters
        for verse in range(1, canonical_counts[chapter] + 1)
    }
    assert set(covered) == expected


@pytest.mark.parametrize("alias", ALL_ALIASES)
def test_build_is_deterministic(max_verses, canonical_counts, alias):
    first = build_psalm_map(alias, max_verses[alias], canonical_counts)
    second = build_psalm_map(alias, max_verses[alias], canonical_counts)
    assert first == second


def test_reverse_lookup_roundtrip(maps, max_verses, canonical_counts):
    for alias in ALL_ALIASES:
        psalm_map = maps[alias]
        for chapter, max_verse in max_verses[alias].items():
            for verse in range(1, max_verse + 1):
                c_chapter, c_start, c_end = psalm_map.to_canonical(chapter, verse)
                for c_verse in range(max(c_start, 1), c_end + 1):
                    assert psalm_map.from_canonical(c_chapter, c_verse) == \
                        (chapter, verse)


def test_from_canonical_absent_verse_returns_none(maps):
    # The pure builder maps every verse NUMBER 1..max — bti 67:14 is mapped
    # even though the row is absent from the database (the CLI build layer
    # filters such rows out before storing).
    assert maps["bti"].from_canonical(68, 13) == (67, 14)
    assert maps["bsb"].from_canonical(151, 1) is None   # no Psalm 151 in bsb


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------

def test_unknown_translation_scheme_raises(canonical_counts):
    with pytest.raises(ValueError, match="TRANSLATION_SCHEMES"):
        build_psalm_map("unknown", {1: 6}, canonical_counts)


def test_unexplained_verse_count_raises(canonical_counts):
    # canonical Psalm 1 has 6 verses; 9 cannot be a superscription offset
    with pytest.raises(ValueError, match="EXCEPTIONS"):
        build_psalm_map("bsb", {1: 9}, canonical_counts)


def test_septuagint_segments_reject_out_of_range(canonical_counts):
    with pytest.raises(ValueError):
        septuagint_segments(152, canonical_counts)


# ---------------------------------------------------------------------------
# Canonical-space chunking (CHUNKING_VERSION 3)
# ---------------------------------------------------------------------------

def _psalm_text(chapter, verse):
    """Deterministic synthetic verse text, same for every translation."""
    return f"c{chapter:03d}v{verse:03d} " + "x" * (40 + (chapter * 7 + verse * 13) % 60)


def _bsb_like_verses(canonical_counts, chapters):
    """Masoretic-style Psalter: canonical numbering, no superscriptions."""
    return {
        chapter: [
            Verse(verse, _psalm_text(chapter, verse), verse % 4 == 1)
            for verse in range(1, canonical_counts[chapter] + 1)
        ]
        for chapter in chapters
    }


def test_canonicalize_seam_chapter_splits_and_numbers(maps):
    # syn 9 = superscription + canonical 9 (20 vv) + canonical 10 (18 vv)
    verses = {9: [Verse(1, "sup", False)] + [
        Verse(v, _psalm_text(*(9, v - 1) if v <= 21 else (10, v - 21)), v % 3 == 0)
        for v in range(2, 40)
    ]}
    c_verses, c_titles, back = canonicalize_psalm_chapters(verses, {9: {2: "T"}}, maps["syn"])
    assert sorted(c_verses) == [9, 10]
    assert [v.verse_number for v in c_verses[9]] == list(range(0, 21))
    assert [v.verse_number for v in c_verses[10]] == list(range(1, 19))
    assert back[(9, 0)] == (9, 1)       # superscription
    assert back[(9, 1)] == (9, 2)
    assert back[(10, 1)] == (9, 22)
    assert back[(10, 18)] == (9, 39)
    assert c_titles[9] == {1: "T"}      # title key renumbered
    # texts and paragraph flags travel unchanged
    assert c_verses[10][0].text == _psalm_text(10, 1)


def test_canonicalize_double_superscription_placeholders(maps):
    # ubh 51: verses 1-2 are the superscription -> placeholders -1 and 0
    verses = {51: [Verse(v, f"t{v}", False) for v in range(1, 22)]}
    c_verses, _titles, back = canonicalize_psalm_chapters(verses, {}, maps["ubh"])
    numbers = [v.verse_number for v in c_verses[51]]
    assert numbers == [-1, 0] + list(range(1, 20))
    assert back[(51, -1)] == (51, 1)
    assert back[(51, 0)] == (51, 2)
    assert back[(51, 19)] == (51, 21)


def test_canonicalize_joins_septuagint_halves_of_116(maps):
    # syn 114 (8 vv, merged 8=canonical 8-9) + syn 115 (10 vv) -> canonical 116
    verses = {
        114: [Verse(v, f"a{v}", False) for v in range(1, 9)],
        115: [Verse(v, f"b{v}", False) for v in range(1, 11)],
    }
    c_verses, _titles, back = canonicalize_psalm_chapters(verses, {}, maps["syn"])
    assert sorted(c_verses) == [116]
    numbers = [v.verse_number for v in c_verses[116]]
    assert numbers == [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    assert back[(116, 8)] == (114, 8)   # merged verse keeps its range start
    assert (116, 9) not in back         # covered by the merged verse
    assert back[(116, 10)] == (115, 1)


def test_shared_plan_slot_ids_across_traditions(maps, canonical_counts):
    # The same canonical content in Septuagint and Masoretic numbering must
    # produce IDENTICAL chunk IDs once chunked in canonical space.
    from chunking import ChunkingConfig, build_plan, chunk_translation
    from versification import CANONICAL_SPLITS

    config = ChunkingConfig(target_chars=300, min_chars=100, max_chars=600,
                            overlap_units=0)
    bsb_verses = _bsb_like_verses(canonical_counts, [9, 10, 116])
    syn_verses = {
        9: [Verse(1, "", False)]                       # empty superscription slot
           + [Verse(v, _psalm_text(9, v - 1), v % 4 == 2) for v in range(2, 22)]
           + [Verse(v, _psalm_text(10, v - 21), v % 4 == 2) for v in range(22, 40)],
        114: [Verse(v, _psalm_text(116, v), v % 4 == 1) for v in range(1, 8)]
             + [Verse(8, _psalm_text(116, 8) + " " + _psalm_text(116, 9), False)],
        115: [Verse(v, _psalm_text(116, v + 9), v % 4 == 1) for v in range(1, 11)],
    }

    def chunk(alias, verses_by_chapter, plan=None):
        c_verses, c_titles, back = canonicalize_psalm_chapters(
            verses_by_chapter, {}, maps[alias]
        )
        chapters = {
            (19, chapter): (verses, c_titles[chapter])
            for chapter, verses in c_verses.items()
        }
        if plan is None:
            plan_input = {}
            for key, (verses, titles) in chapters.items():
                title_verses = set(titles)
                if key[1] in CANONICAL_SPLITS:
                    title_verses.add(CANONICAL_SPLITS[key[1]])
                plan_input[key] = (verses, title_verses)
            plan = build_plan(plan_input, config)
        return chunk_translation(plan, chapters, config), back, plan

    syn_chunks, syn_back, plan = chunk("syn", syn_verses)          # syn = pivot
    bsb_chunks, _bsb_back, _ = chunk("bsb", bsb_verses, plan=plan)

    assert [c.canonical_id for c in syn_chunks] == \
        [c.canonical_id for c in bsb_chunks]
    assert len(syn_chunks) > 3
    # no chunk crosses a canonical chapter, and the forced split keeps every
    # canonical-116 chunk inside ONE Septuagint chapter of syn
    for chunk_ in syn_chunks:
        start = syn_back[(chunk_.chapter_number, chunk_.verse_number_start)]
        end = syn_back[(chunk_.chapter_number, chunk_.verse_number_end)]
        assert start[0] == end[0]
    split = CANONICAL_SPLITS[116]
    for chunk_ in bsb_chunks:
        if chunk_.chapter_number == 116:
            assert not (chunk_.verse_number_start < split <= chunk_.verse_number_end)


def test_canonicalize_is_deterministic(maps):
    verses = {9: [Verse(v, f"t{v}", v % 2 == 0) for v in range(1, 40)]}
    first = canonicalize_psalm_chapters(verses, {9: {3: "T"}}, maps["syn"])
    second = canonicalize_psalm_chapters(verses, {9: {3: "T"}}, maps["syn"])
    assert first == second


# ---------------------------------------------------------------------------
# Other books: translation numbering -> English reference numbering.
# tests/data/chapter_max_verses.json is MAX(verse_number) per non-Psalm
# chapter of the indexed translations in cep_public (2026-11-02); the pairs
# below were checked against the stored verse texts.
# ---------------------------------------------------------------------------

CHAPTERS = Path(__file__).parent / "data" / "chapter_max_verses.json"


@pytest.fixture(scope="module")
def stored_chapters():
    return {
        alias: {
            (int(book), int(chapter)): last
            for book, chapters in books.items()
            for chapter, last in chapters.items()
        }
        for alias, books in json.loads(CHAPTERS.read_text()).items()
    }


def test_versifications_match_the_stored_chapters(stored_chapters):
    assert set(stored_chapters) == set(TRANSLATION_VERSIFICATIONS)
    for alias, chapters in stored_chapters.items():
        check_chapters(alias, chapters)


def test_differing_stored_chapter_is_refused(stored_chapters):
    chapters = {**stored_chapters["syn"], (32, 2): 10}
    with pytest.raises(ValueError, match=r"\(\(32, 2\), 11, 10\)"):
        check_chapters("syn", chapters)
    with pytest.raises(ValueError, match="TRANSLATION_VERSIFICATIONS"):
        check_chapters("bti", chapters)


@pytest.mark.parametrize(
    ("alias", "stored", "english"),
    [
        # Jonah's fish and prayer: English 1:17 and 2:1-10.
        ("syn", (32, 2, 1, 1), [(32, 1, 17, 17)]),
        ("ubh", (32, 2, 2, 11), [(32, 2, 1, 10)]),
        # The doxology closes Synodal Romans 14.
        ("syn", (52, 14, 24, 26), [(52, 16, 25, 27)]),
        ("ubh", (39, 3, 13, 24), [(39, 3, 13, 18), (39, 4, 1, 6)]),
        ("ubh", (4, 17, 1, 15), [(4, 16, 36, 50)]),
        ("syn", (4, 16, 36, 50), [(4, 16, 36, 50)]),
        ("syn", (54, 13, 13, 13), [(54, 13, 14, 14)]),
        ("syn", (18, 40, 20, 27), [(18, 41, 1, 8)]),
        ("ubh", (18, 40, 25, 32), [(18, 41, 1, 8)]),
        ("ubh", (23, 8, 23, 23), [(23, 9, 1, 1)]),
        ("syn", (21, 4, 17, 17), [(21, 5, 1, 1)]),
        ("syn", (27, 3, 98, 100), [(27, 4, 1, 3)]),
        # ubh numbers Joel and Nehemiah 7 as English Bibles do.
        ("ubh", (29, 3, 1, 5), [(29, 3, 1, 5)]),
        ("ubh", (16, 7, 73, 73), [(16, 7, 73, 73)]),
        # Greek additions have no English counterpart.
        ("ubh", (27, 3, 24, 90), []),
        ("bsb", (50, 1, 13, 14), [(50, 1, 13, 14)]),
        ("bsb", (66, 12, 13, 17), [(66, 12, 13, 17)]),
    ],
)
def test_translation_ranges_in_english_numbering(alias, stored, english):
    assert to_reference_numbering(alias, *stored) == english


def test_reference_windows_use_english_numbering(stored_chapters):
    metas = [
        dict(
            canonical_id=f"v3:{book:02d}.{chapter:03d}.001-{last:03d}",
            language=language,
            translation=code,
            alias=alias,
            book_number=book,
            chapter_number=chapter,
            verse_number_start=1,
            verse_number_end=last,
        )
        for language, code, alias in (("ru", 1, "syn"), ("uk", 20, "ubh"))
        for (book, chapter), last in stored_chapters[alias].items()
    ]
    # A Psalm window keeps its canonical ID: Synodal Psalm 22 is Psalm 23.
    metas.append({**metas[0], "canonical_id": "v3:19.023.001-006",
                  "book_number": 19, "chapter_number": 22,
                  "verse_number_end": 7})
    index = InMemoryVectorIndex(np.zeros((len(metas), 2)), metas)
    windows = {}
    for language, rows in reference_windows(index).items():
        for cid, *verse_range in rows:
            windows.setdefault((language, cid), []).append(tuple(verse_range))
    assert windows["ru", "v3:32.002.001-011"] == [(32, 1, 17, 17), (32, 2, 1, 10)]
    assert windows["uk", "v3:39.003.001-024"] == [(39, 3, 1, 18), (39, 4, 1, 6)]
    assert windows["ru", "v3:19.023.001-006"] == [(19, 23, 1, 6)]
    metas[1] = {**metas[1], "verse_number_end": metas[1]["verse_number_end"] + 1}
    with pytest.raises(ValueError, match="syn chapters differ"):
        reference_windows(InMemoryVectorIndex(np.zeros((len(metas), 2)), metas))
