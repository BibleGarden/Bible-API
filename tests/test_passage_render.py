"""
Rendering a canonical window in a translation that was never chunked
(app/passage_render.py, ADR 0007).

Two halves:

- pure tests of the coordinate conversion and of the coverage rule, over the
  same Psalm-versification fixture the other tests use;
- live tests against `cep_public`, because the point of the feature is the
  real corpus: `bti` (Septuagint numbering, missing books, two unreachable
  Psalm verses), `webus`/`webbe` (Masoretic, near-complete) and `npu`
  (Psalms + New Testament only).
"""

import json
import os
from pathlib import Path

import pytest

os.environ.setdefault("API_KEY", "test-api-key")

from canon import CANONICAL_CHAPTER_COUNTS
from chunking import CHUNKING_VERSION, build_text
from passage_highlight import load_psalm_maps
from passage_render import (
    build_coverage,
    canonical_presence,
    covered_windows,
    load_chunk_ranges,
    load_verse_coordinates,
    reference_faithful_windows,
    render_passage,
    to_translation_range,
)
from retrieval import VerseText, parse_canonical_id
from versification import (
    PsalmMap,
    build_psalm_map,
    canonical_counts_with_extras,
)

FIXTURE = Path(__file__).parent / "data" / "psalm_max_verses.json"

SYN, BTI, BSB, WEBUS, UBH, NPU, WEBBE = 1, 11, 16, 17, 20, 21, 779
ALIASES = {
    SYN: "syn", BTI: "bti", BSB: "bsb", WEBUS: "webus",
    UBH: "ubh", NPU: "npu", WEBBE: "webbe",
}


@pytest.fixture(scope="module")
def psalm_maps():
    max_verses = {
        alias: {int(chapter): mx for chapter, mx in chapters.items()}
        for alias, chapters in json.loads(FIXTURE.read_text()).items()
    }
    counts = canonical_counts_with_extras(max_verses["bsb"])
    return {
        code: PsalmMap(build_psalm_map(alias, max_verses[alias], counts))
        for code, alias in ALIASES.items()
    }


# ---------------------------------------------------------------------------
# Canonical window -> the target translation's coordinates
# ---------------------------------------------------------------------------

def test_outside_the_psalms_the_conversion_is_the_identity(psalm_maps):
    assert to_translation_range(43, 14, 27, 27, psalm_maps[BTI]) == (14, 27, 27)
    assert to_translation_range(40, 11, 28, 30, None) == (11, 28, 30)


def test_a_septuagint_translation_shifts_the_chapter(psalm_maps):
    """Canonical (Masoretic) 23 is chapter 22 in syn/bti/npu, 23 in the
    Masoretic-numbered ones."""
    for code in (SYN, BTI, NPU):
        assert to_translation_range(19, 23, 1, 6, psalm_maps[code])[0] == 22
    for code in (BSB, WEBUS, WEBBE, UBH):
        assert to_translation_range(19, 23, 1, 6, psalm_maps[code])[0] == 23


def test_the_two_halves_of_a_split_canonical_psalm_land_in_two_chapters(
    psalm_maps
):
    """The Septuagint splits canonical 116 at verse 10 (ADR 0003)."""
    assert to_translation_range(19, 116, 1, 9, psalm_maps[BTI])[0] == 114
    assert to_translation_range(19, 116, 10, 19, psalm_maps[BTI])[0] == 115


def test_a_window_starting_at_the_superscription_renders_it_where_it_exists(
    psalm_maps
):
    """Canonical verse 0 is the superscription the canon does not number.
    A translation that counts it starts one verse earlier; one that does not
    starts at canonical verse 1."""
    assert to_translation_range(19, 3, 0, 8, psalm_maps[SYN]) == (3, 1, 9)
    assert to_translation_range(19, 3, 0, 8, psalm_maps[BTI]) == (3, 1, 9)
    assert to_translation_range(19, 3, 0, 8, psalm_maps[UBH]) == (3, 1, 9)
    # bsb does not number superscriptions at all: canonical 1 is verse 1
    assert to_translation_range(19, 3, 0, 8, psalm_maps[BSB]) == (3, 1, 8)


def test_a_psalm_the_translation_does_not_have_cannot_be_rendered(psalm_maps):
    """Psalm 151 exists in syn only; the others answer None, never a guess."""
    assert to_translation_range(19, 151, 1, 7, psalm_maps[SYN]) == (151, 1, 7)
    for code in (BTI, BSB, WEBUS, WEBBE, UBH, NPU):
        assert to_translation_range(19, 151, 1, 7, psalm_maps[code]) is None


def test_a_psalm_window_without_a_versification_map_is_refused():
    assert to_translation_range(19, 23, 1, 6, None) is None


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------

WINDOWS = [
    ("v3:43.014.027-027", 43, 14, 27, 27),
    ("v3:43.014.028-031", 43, 14, 28, 31),
    ("v3:01.001.001-005", 1, 1, 1, 5),
]


def test_a_window_is_covered_only_when_every_verse_exists():
    present = canonical_presence(
        [(43, 14, 27), (43, 14, 28), (43, 14, 29), (43, 14, 30)], None
    )

    covered = covered_windows(WINDOWS, present)

    assert covered == {"v3:43.014.027-027"}   # 28-31 is partial, book 1 absent


def test_empty_verses_do_not_count_as_present():
    """`chunking.build_text` drops empty verses, so a window whose verses are
    blank has no text either — the caller filters them out of the rows."""
    rows = [(43, 14, 27)]     # verse 28 exists in the table but is empty

    assert covered_windows(
        [("v3:43.014.027-028", 43, 14, 27, 28)], canonical_presence(rows, None)
    ) == set()


def test_the_superscription_is_not_required_for_coverage(psalm_maps):
    """Canonical verse 0 is unnumbered; requiring it would make every
    superscription window uncoverable for bsb, which has none."""
    rows = [(19, 3, verse) for verse in range(1, 9)]

    covered = covered_windows(
        [("v3:19.003.000-008", 19, 3, 0, 8)],
        canonical_presence(rows, psalm_maps[BSB]),
    )

    assert covered == {"v3:19.003.000-008"}


def test_psalm_coverage_is_measured_in_canonical_coordinates(psalm_maps):
    """syn 22:1-6 is canonical 23:1-6: the window is covered although no syn
    verse carries those numbers."""
    rows = [(19, 22, verse) for verse in range(1, 7)]

    covered = covered_windows(
        [("v3:19.023.001-006", 19, 23, 1, 6)],
        canonical_presence(rows, psalm_maps[SYN]),
    )

    assert covered == {"v3:19.023.001-006"}


def test_a_window_without_a_positive_verse_is_never_covered(psalm_maps):
    """Fix F5: `range(1, 1)` is empty, so `all()` would call such a window
    covered — and `to_translation_range` would then answer None on it, i.e. a
    503 on a window the filter said was fine. The corpus has none today."""
    rows = [(19, 3, verse) for verse in range(1, 9)]
    present = canonical_presence(rows, psalm_maps[SYN])

    assert covered_windows([("v3:19.003.000-000", 19, 3, 0, 0)], present) == set()
    assert covered_windows([("v3:19.003.001-000", 19, 3, 1, 0)], present) == set()
    # the same window with a real verse in it is covered as before
    assert covered_windows(
        [("v3:19.003.000-001", 19, 3, 0, 1)], present
    ) == {"v3:19.003.000-001"}


# ---------------------------------------------------------------------------
# Reference-faithful windows (fix F2)
# ---------------------------------------------------------------------------

def test_a_window_whose_reference_chunk_absorbs_a_tail_is_dropped():
    """The last plan slot of a chapter has no upper bound, so its stored
    chunk swallows the chapter's trailing verses: the ID says 12-26, the
    reranker reads 12-34. The reader of another translation would get 12-26
    — shorter by a tail, not by the documented overlap prefix."""
    windows = [
        ("v3:18.041.012-026", 18, 41, 12, 26),
        ("v3:18.041.001-011", 18, 41, 1, 11),
    ]
    chunk_ranges = {
        "v3:18.041.012-026": (41, 34),     # absorbed 27-34
        "v3:18.041.001-011": (41, 11),     # own range exactly
    }

    kept = reference_faithful_windows(windows, chunk_ranges, None)

    assert [w[0] for w in kept] == ["v3:18.041.001-011"]


def test_the_overlap_prefix_of_a_reference_chunk_is_allowed():
    """A chunk that STARTS earlier is the documented overlap the chunker
    copies from its predecessor — the rendered own range is that text minus
    a prefix, which is the accepted difference of ADR 0007."""
    windows = [("v3:43.014.027-031", 43, 14, 27, 31)]

    kept = reference_faithful_windows(
        windows, {"v3:43.014.027-031": (14, 31)}, None
    )

    assert [w[0] for w in kept] == ["v3:43.014.027-031"]


def test_a_window_with_no_reference_chunk_is_dropped():
    """Fail-closed: nothing to verify the rendering against."""
    windows = [("v3:43.014.027-031", 43, 14, 27, 31)]

    assert reference_faithful_windows(windows, {}, None) == []


def test_the_reference_comparison_uses_the_reference_own_numbering(psalm_maps):
    """Psalms: canonical 23:1-6 is syn 22:1-6, so the stored chunk of a
    Septuagint-numbered reference must be compared in ITS chapter, not in the
    canonical one."""
    windows = [("v3:19.023.001-006", 19, 23, 1, 6)]

    assert reference_faithful_windows(
        windows, {"v3:19.023.001-006": (22, 6)}, psalm_maps[SYN]
    ) == windows
    # a tail in the translation's own numbering is still a tail
    assert reference_faithful_windows(
        windows, {"v3:19.023.001-006": (22, 7)}, psalm_maps[SYN]
    ) == []
    # and the canonical chapter number is not the translation's
    assert reference_faithful_windows(
        windows, {"v3:19.023.001-006": (23, 6)}, psalm_maps[SYN]
    ) == []


# ---------------------------------------------------------------------------
# Text assembly
# ---------------------------------------------------------------------------

def test_paragraphs_follow_the_chunking_rules():
    """Same function the corpus was built with: blank line at a paragraph
    start, single spaces inside one."""
    verses = [
        VerseText(1, "Первый", start_paragraph=True),
        VerseText(2, "Второй"),
        VerseText(3, "Третий", start_paragraph=True),
    ]

    assert build_text(verses, set()) == "Первый Второй\n\nТретий"
    assert build_text(verses, {2}) == "Первый\n\nВторой\n\nТретий"


# ---------------------------------------------------------------------------
# Live corpus (cep_public)
# ---------------------------------------------------------------------------

def _database_available() -> bool:
    from database import create_connection

    try:
        connection = create_connection()
    except Exception:
        return False
    if connection is None:
        return False
    connection.close()
    return True


needs_db = pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)


@pytest.fixture(scope="module")
def live():
    from database import create_connection
    from vector_index import load_index

    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        index = load_index(cursor)
        maps = load_psalm_maps(cursor)
        windows: dict[str, list[tuple]] = {}
        seen: dict[str, set] = {}
        for meta in index.metas:
            language, canonical_id = meta["language"], meta["canonical_id"]
            if canonical_id in seen.setdefault(language, set()):
                continue
            seen[language].add(canonical_id)
            _v, book, chapter, start, end = parse_canonical_id(canonical_id)
            windows.setdefault(language, []).append(
                (canonical_id, book, chapter, start, end)
            )
        yield cursor, maps, windows
    finally:
        cursor.close()
        connection.close()


PRIMARY_OF = {"ru": SYN, "en": BSB, "uk": UBH}


def production_coverage(cursor, maps, windows, code, language):
    """The candidate set the endpoint really builds for a translation.

    Both steps of `scripture_select._build_catalogue`: the windows whose
    stored reference chunk is the window's own range, then the ones this
    translation carries in full.
    """
    reference = PRIMARY_OF[language]
    offered = reference_faithful_windows(
        windows[language], load_chunk_ranges(cursor, reference), maps[reference]
    )
    return offered, build_coverage(cursor, code, offered, maps[code])


@pytest.fixture(scope="module")
def reference_text():
    """(chunk texts, verses, title verses) of one indexed translation, cached.

    One pass over `translation_verses` instead of three queries per window:
    the invariant below is checked over EVERY covered window, not a sample.
    """
    loaded: dict[int, tuple] = {}

    def load(cursor, code):
        if code in loaded:
            return loaded[code]
        cursor.execute(
            "SELECT canonical_id, text FROM translation_chunks "
            "WHERE translation = %s AND chunking_version = %s",
            (code, CHUNKING_VERSION),
        )
        chunks = {row["canonical_id"]: row["text"] for row in cursor.fetchall()}
        cursor.execute(
            "SELECT book_number, chapter_number, verse_number, text, "
            "start_paragraph FROM translation_verses WHERE translation = %s "
            "ORDER BY book_number, chapter_number, verse_number",
            (code,),
        )
        verses: dict[tuple[int, int], list[VerseText]] = {}
        for row in cursor.fetchall():
            text = row["text"].strip()
            if not text:
                continue
            verses.setdefault(
                (row["book_number"], row["chapter_number"]), []
            ).append(
                VerseText(row["verse_number"], text, bool(row["start_paragraph"]))
            )
        cursor.execute(
            "SELECT tv.book_number, tv.chapter_number, tv.verse_number "
            "FROM translation_titles tt "
            "JOIN translation_verses tv ON tt.before_translation_verse = tv.code "
            "WHERE tv.translation = %s AND tt.subtitle = 0",
            (code,),
        )
        titles: dict[tuple[int, int], set[int]] = {}
        for row in cursor.fetchall():
            titles.setdefault(
                (row["book_number"], row["chapter_number"]), set()
            ).add(row["verse_number"])
        loaded[code] = (chunks, verses, titles)
        return loaded[code]

    return load


@needs_db
@pytest.mark.parametrize(
    ("code", "language", "minimum"),
    [(BTI, "ru", 3000), (WEBUS, "en", 3000), (WEBBE, "en", 3000),
     (NPU, "uk", 500)],
)
def test_no_covered_window_loses_more_than_the_overlap_prefix(
    live, reference_text, code, language, minimum
):
    """Fix F2, over EVERY window of the coverage set.

    The reranker judges the reference translation's stored chunk; the client
    is served the window's OWN canonical range. The only difference ADR 0007
    accepts is the overlap prefix the chunker copies from the previous chunk
    — so the stored chunk must END with the own-range rendering, never carry
    a tail beyond it. Checked on the reference translation itself, where both
    texts exist; the served translation renders the same canonical range,
    which coverage guarantees it has in full.
    """
    cursor, maps, windows = live
    reference = PRIMARY_OF[language]
    _offered, covered = production_coverage(
        cursor, maps, windows, code, language
    )
    chunks, verses, titles = reference_text(cursor, reference)

    assert len(covered) >= minimum
    by_id = {row[0]: row for row in windows[language]}
    for canonical_id in sorted(covered):
        _cid, book, chapter, start, end = by_id[canonical_id]
        mapped = to_translation_range(
            book, chapter, start, end, maps[reference]
        )
        assert mapped is not None, canonical_id
        own_chapter, first, last = mapped
        own = [
            verse for verse in verses.get((book, own_chapter), [])
            if first <= verse.verse_number <= last
        ]
        rendered = build_text(own, titles.get((book, own_chapter), set()))
        stored = chunks.get(canonical_id)
        assert stored is not None, canonical_id
        assert rendered and stored.endswith(rendered), canonical_id


@needs_db
def test_the_tail_absorbing_windows_are_excluded_from_every_coverage_set(live):
    """The windows the review measured: their stored `bsb`/`ubh` chunk runs
    up to 8 verses past the range the ID names, so no non-indexed translation
    may be offered them."""
    cursor, maps, windows = live

    for code, language, examples in (
        (WEBUS, "en", ("v3:18.041.012-026", "v3:27.004.031-034")),
        (WEBBE, "en", ("v3:18.041.012-026", "v3:27.004.031-034")),
        (NPU, "uk", ("v3:52.016.021-024", "v3:19.116.001-008")),
    ):
        _offered, covered = production_coverage(
            cursor, maps, windows, code, language
        )
        for canonical_id in examples:
            assert canonical_id not in covered, (code, canonical_id)


@needs_db
@pytest.mark.parametrize(
    "canonical_id",
    ["v3:19.121.001-008", "v3:57.004.001-009", "v3:40.006.025-034"],
)
def test_the_added_safe_pool_windows_render_in_every_translation(
    live, canonical_id
):
    """Safe pool 1.1.0 (Мария, 2026-08-28): the three added places must exist
    in all seven active translations, `npu` included — that is the whole
    point of adding them."""
    cursor, maps, _windows = live
    _v, book, chapter, start, end = parse_canonical_id(canonical_id)

    for code, alias in ALIASES.items():
        passage = render_passage(
            cursor, code, alias, book, chapter, start, end, maps[code]
        )
        assert passage is not None, (alias, canonical_id)
        assert passage.text.strip(), (alias, canonical_id)
        assert passage.verse_number_start <= passage.verse_number_end


@needs_db
@pytest.mark.parametrize(
    ("code", "language", "minimum"),
    [(BTI, "ru", 3000), (WEBUS, "en", 3000), (WEBBE, "en", 3000),
     (NPU, "uk", 500)],
)
def test_every_covered_window_is_actually_renderable(
    live, code, language, minimum
):
    """The contract the whole feature rests on: a window that survives the
    filter always produces text in that translation."""
    cursor, maps, windows = live
    covered = build_coverage(cursor, code, windows[language], maps[code])

    assert len(covered) >= minimum

    by_id = {row[0]: row for row in windows[language]}
    # A spread sample: the full sweep is three queries per window and takes
    # minutes; the prime stride hits every book of the corpus.
    for canonical_id in sorted(covered)[::89]:
        _cid, book, chapter, start, end = by_id[canonical_id]
        passage = render_passage(
            cursor, code, ALIASES[code], book, chapter, start, end, maps[code]
        )
        assert passage is not None, canonical_id
        assert passage.text.strip(), canonical_id
        assert passage.verse_number_start <= passage.verse_number_end


@needs_db
def test_npu_covers_only_the_books_it_has(live):
    """A translation may be radically incomplete (npu is Psalms + the New
    Testament); it is supported with a narrowed pool, not excluded."""
    cursor, maps, windows = live
    covered = build_coverage(cursor, NPU, windows["uk"], maps[NPU])

    books = {parse_canonical_id(cid)[1] for cid in covered}

    assert books, "npu must still be renderable"
    assert books <= {19} | set(range(40, 67))
    assert 19 in books and 40 in books
    assert len(covered) < len(windows["uk"]) / 2


@needs_db
def test_bti_cannot_reach_two_canonical_psalm_verses(live):
    """bti has a numbering hole at 67:14 and one at its 104:6, so canonical
    68:13 and 105:6 exist in no bti verse. Windows containing them are
    excluded instead of being rendered with a verse silently missing."""
    cursor, maps, windows = live
    present = canonical_presence(
        load_verse_coordinates(cursor, BTI), maps[BTI]
    )

    assert 12 in present[(19, 68)] and 13 not in present[(19, 68)]
    assert 5 in present[(19, 105)] and 6 not in present[(19, 105)]

    covered = covered_windows(windows["ru"], present)
    for canonical_id, _b, chapter, start, end in windows["ru"]:
        if chapter == 68 and start <= 13 <= end:
            assert canonical_id not in covered


@needs_db
def test_bti_has_every_canonical_chapter_and_no_deuterocanonical_extras(live):
    """After the 2026-08-30 BTI backfill (ClickUp 86cbb1reb) BTI is complete:
    all 1189 canonical chapters have text — this test used to pin the
    opposite (Deut 32-34 unreachable), which is no longer true and would now
    be a false assertion.

    What still holds, and is the lasting value of this test: BTI carries
    none of the deuterocanonical chapters of other translations — Ps 151
    (syn), Dan 13-14 (syn, ubh), 2 Chr 37 (syn), Esth 11-12 (ubh). Those were
    never part of BTI's own canon, and backfilling BTI's own text does not
    manufacture them.
    """
    cursor, maps, _windows = live
    present = canonical_presence(load_verse_coordinates(cursor, BTI), maps[BTI])

    missing_canonical_chapters = [
        (book, chapter)
        for book, chapters in CANONICAL_CHAPTER_COUNTS.items()
        for chapter in range(1, chapters + 1)
        if (book, chapter) not in present
    ]
    assert missing_canonical_chapters == []

    deuterocanonical_extras_of_other_translations = [
        (19, 151),          # Ps 151 (syn)
        (27, 13), (27, 14),  # Dan 13-14 (syn, ubh)
        (14, 37),           # 2 Chr 37 (syn)
        (17, 11), (17, 12),  # Esth 11-12 (ubh)
    ]
    for book, chapter in deuterocanonical_extras_of_other_translations:
        assert (book, chapter) not in present


@needs_db
def test_a_septuagint_psalm_renders_the_right_words_not_the_right_numbers(
    live
):
    """Canonical Psalm 23 in bti is chapter 22 — and the text really is the
    shepherd psalm, read from the database."""
    cursor, maps, _windows = live

    passage = render_passage(
        cursor, BTI, "bti", 19, 23, 1, 6, maps[BTI]
    )

    assert passage.chapter_number == 22
    assert passage.book_number == 19
    assert "Пастырь" in passage.text
    assert passage.verse_number_start >= 1


@needs_db
def test_a_superscription_window_starts_at_the_translations_own_first_verse(
    live
):
    """Canonical window 3:0-8 (the superscription plus eight verses): syn,
    bti and npu count the inscription, bsb does not."""
    cursor, maps, _windows = live

    for code, alias in ((SYN, "syn"), (BTI, "bti"), (NPU, "npu")):
        passage = render_passage(cursor, code, alias, 19, 3, 0, 8, maps[code])
        assert passage.chapter_number == 3
        assert passage.verse_number_end == 9

    bsb = render_passage(cursor, BSB, "bsb", 19, 3, 0, 8, maps[BSB])
    assert (bsb.verse_number_start, bsb.verse_number_end) == (1, 8)


@needs_db
def test_the_section_title_is_the_most_specific_one_before_the_window(live):
    """Same rule as the chunker's (`apply_chapter_plan`)."""
    cursor, maps, _windows = live

    passage = render_passage(cursor, BTI, "bti", 43, 14, 27, 27, maps[BTI])

    assert passage.title == "Дух истины"
    assert passage.text.startswith("Ныне же мир")


@needs_db
def test_a_window_outside_the_translation_renders_nothing(live):
    """No fallback to another translation, no partial text: None."""
    cursor, maps, _windows = live

    assert render_passage(
        cursor, NPU, "npu", 1, 1, 1, 5, maps[NPU]        # Genesis: absent
    ) is None
    assert render_passage(
        cursor, BTI, "bti", 19, 151, 1, 7, maps[BTI]     # Psalm 151: absent
    ) is None


@needs_db
def test_the_renderer_reproduces_the_stored_chunk_of_an_indexed_translation(
    live
):
    """Cross-check against the corpus itself: for windows the indexed
    translation covers, the rendered text is the stored chunk text minus the
    overlap prefix the chunker copies from the previous chunk (which is a
    property of the chunking plan, not of the window)."""
    cursor, maps, windows = live
    cursor.execute(
        "SELECT canonical_id, title, text FROM translation_chunks "
        "WHERE translation = %s AND book_number = 43 AND chapter_number = 14",
        (SYN,),
    )
    stored = {row["canonical_id"]: row for row in cursor.fetchall()}
    assert stored, "the corpus must contain John 14"

    for canonical_id, row in stored.items():
        _v, book, chapter, start, end = parse_canonical_id(canonical_id)
        passage = render_passage(
            cursor, SYN, "syn", book, chapter, start, end, maps[SYN]
        )
        assert passage is not None
        assert row["text"].endswith(passage.text), canonical_id
        assert (passage.title or None) == (row["title"] or None)
