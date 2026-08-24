from chunking import (
    CHUNKING_VERSION,
    ChunkingConfig,
    Verse,
    apply_chapter_plan,
    build_chapter_plan,
    build_plan,
    chunk_translation,
    make_canonical_id,
)

CONFIG = ChunkingConfig(target_chars=100, min_chars=40, max_chars=200, overlap_units=1)
NO_OVERLAP = ChunkingConfig(target_chars=100, min_chars=40, max_chars=200, overlap_units=0)


def make_verses(specs):
    """specs: list of (verse_number, char_len, start_paragraph)."""
    return [
        Verse(
            verse_number=number,
            text=f"v{number} ".ljust(length, "x"),
            start_paragraph=start_paragraph,
        )
        for number, length, start_paragraph in specs
    ]


def own_ranges(planned):
    return [(p.own_start, p.own_end) for p in planned]


def test_canonical_id_format_and_version():
    assert make_canonical_id(1, 1, 1, 5) == f"v{CHUNKING_VERSION}:01.001.001-005"
    assert make_canonical_id(66, 22, 1, 21, version=7) == "v7:66.022.001-021"


def test_short_paragraphs_are_merged_into_one_chunk():
    # Three tiny paragraphs, total below target -> a single chunk
    verses = make_verses([(1, 30, True), (2, 30, True), (3, 30, True)])
    planned = build_chapter_plan(1, 1, verses, set(), NO_OVERLAP)
    assert own_ranges(planned) == [(1, 3)]


def test_long_section_split_only_at_paragraph_boundaries():
    # Paragraphs start at verses 1, 3, 5; each ~90 chars
    verses = make_verses(
        [(1, 45, True), (2, 45, False),
         (3, 45, True), (4, 45, False),
         (5, 45, True), (6, 45, False)]
    )
    planned = build_chapter_plan(1, 1, verses, set(), NO_OVERLAP)
    starts = [p.own_start for p in planned]
    assert set(starts).issubset({1, 3, 5})
    assert len(planned) > 1
    # Full coverage without gaps
    assert planned[0].own_start == 1
    assert planned[-1].own_end == 6


def test_trailing_short_group_merged_into_previous():
    verses = make_verses([(1, 90, True), (2, 90, True), (3, 20, True)])
    planned = build_chapter_plan(1, 1, verses, set(), NO_OVERLAP)
    # verse 3 paragraph is too small to stand alone
    assert planned[-1].own_end == 3
    assert all(p.own_start != 3 for p in planned)


def test_oversized_paragraph_split_at_verse_boundaries():
    # One paragraph of 6 verses x 60 chars = ~360 chars > max_chars
    verses = make_verses([(n, 60, n == 1) for n in range(1, 7)])
    planned = build_chapter_plan(1, 1, verses, set(), NO_OVERLAP)
    assert len(planned) > 1
    # Boundaries fall on verse numbers, cover 1..6 contiguously
    assert planned[0].own_start == 1
    assert planned[-1].own_end == 6
    for prev, cur in zip(planned, planned[1:]):
        assert cur.own_start == prev.own_end + 1


def test_titles_are_hard_section_boundaries():
    # Title before verse 4; both sections are small enough to fit target,
    # but they must not be merged together.
    verses = make_verses([(n, 20, n in (1, 4)) for n in range(1, 7)])
    planned = build_chapter_plan(1, 1, verses, {4}, NO_OVERLAP)
    assert own_ranges(planned) == [(1, 3), (4, 6)]


def test_title_inside_paragraph_forces_break():
    # No start_paragraph on verse 3, but a title stands before it
    verses = make_verses([(1, 20, True), (2, 20, False), (3, 20, False)])
    planned = build_chapter_plan(1, 1, verses, {3}, NO_OVERLAP)
    assert own_ranges(planned) == [(1, 2), (3, 3)]


def test_chapter_without_titles_is_chunked():
    verses = make_verses([(n, 30, n % 2 == 1) for n in range(1, 9)])
    planned = build_chapter_plan(1, 1, verses, set(), NO_OVERLAP)
    assert planned
    assert planned[0].own_start == 1
    assert planned[-1].own_end == 8


def test_empty_chapter_produces_no_plan():
    assert build_chapter_plan(1, 1, [], set(), CONFIG) == []


def test_overlap_takes_previous_paragraph():
    # Two chunks in one section; second must start its text with the tail
    # paragraph of the first while owning only its own verses.
    verses = make_verses(
        [(1, 45, True), (2, 45, False), (3, 45, True), (4, 45, False)]
    )
    planned = build_chapter_plan(1, 1, verses, set(), CONFIG)
    assert len(planned) == 2
    first, second = planned
    assert first.text_start == first.own_start == 1
    assert second.own_start == 3
    assert second.text_start < second.own_start  # overlap present

    chunks = apply_chapter_plan(planned, 1, 1, verses, {}, CONFIG)
    assert chunks[1].verse_number_start == second.text_start
    assert chunks[1].verse_number_end == 4
    # Overlap verses are also part of the first chunk's text
    assert chunks[0].verse_number_end >= second.text_start


def test_overlap_does_not_cross_section_boundary():
    verses = make_verses([(n, 20, n in (1, 4)) for n in range(1, 7)])
    planned = build_chapter_plan(1, 1, verses, {4}, CONFIG)
    assert own_ranges(planned) == [(1, 3), (4, 6)]
    # New section -> no overlap from the previous section
    assert planned[1].text_start == 4


def test_apply_plan_reconstructs_exact_coordinates_and_text():
    verses = make_verses([(1, 30, True), (2, 30, False), (3, 30, True)])
    planned = build_chapter_plan(2, 5, verses, set(), NO_OVERLAP)
    chunks = apply_chapter_plan(planned, 2, 5, verses, {1: "Heading"}, NO_OVERLAP)
    assert len(chunks) == 1
    chunk = chunks[0]
    assert chunk.book_number == 2
    assert chunk.chapter_number == 5
    assert chunk.verse_number_start == 1
    assert chunk.verse_number_end == 3
    assert chunk.verse_count == 3
    assert chunk.title == "Heading"
    # Paragraph structure preserved: verse 3 starts a new paragraph
    par1, par2 = chunk.text.split("\n\n")
    assert par1.startswith("v1") and "v2" in par1
    assert par2.startswith("v3")


def test_title_attribution_nearest_preceding():
    verses = make_verses([(n, 60, True) for n in range(1, 7)])
    titles = {1: "First", 4: "Second"}
    planned = build_chapter_plan(1, 1, verses, set(titles), NO_OVERLAP)
    chunks = apply_chapter_plan(planned, 1, 1, verses, titles, NO_OVERLAP)
    by_start = {c.verse_number_start: c for c in chunks}
    assert by_start[1].title == "First"
    starts_in_second_section = [s for s in by_start if s >= 4]
    assert starts_in_second_section
    for start in starts_in_second_section:
        assert by_start[start].title == "Second"


def test_no_title_before_first_chunk_gives_none():
    verses = make_verses([(1, 30, True), (2, 30, False)])
    planned = build_chapter_plan(1, 1, verses, set(), NO_OVERLAP)
    chunks = apply_chapter_plan(planned, 1, 1, verses, {}, NO_OVERLAP)
    assert chunks[0].title is None


def test_parallel_translations_share_canonical_ids():
    # Pivot structure
    pivot = make_verses(
        [(1, 45, True), (2, 45, False), (3, 45, True), (4, 45, False)]
    )
    plan = build_plan({(1, 1): (pivot, set())}, NO_OVERLAP)

    # Another translation: same coordinates, different lengths and paragraphs
    other = make_verses(
        [(1, 70, True), (2, 10, False), (3, 20, False), (4, 80, True)]
    )
    pivot_chunks = chunk_translation(plan, {(1, 1): (pivot, {})}, NO_OVERLAP)
    other_chunks = chunk_translation(plan, {(1, 1): (other, {})}, NO_OVERLAP)
    assert [c.canonical_id for c in pivot_chunks] == [c.canonical_id for c in other_chunks]


def test_translation_with_missing_and_extra_verses():
    pivot = make_verses([(n, 60, n in (1, 4)) for n in range(1, 7)])
    plan = build_plan({(1, 1): (pivot, set())}, NO_OVERLAP)
    ranges = own_ranges(plan[(1, 1)])
    assert ranges[0][0] == 1 and ranges[-1][1] == 6

    # The translation lacks verse 2 and has extra trailing verses 7-8
    # (sizes small enough not to trigger the oversized-range refinement)
    other = make_verses(
        [(1, 30, True), (3, 30, False), (4, 30, True),
         (5, 30, False), (6, 30, False), (7, 30, False), (8, 30, True)]
    )
    chunks = chunk_translation(plan, {(1, 1): (other, {})}, NO_OVERLAP)
    covered = []
    for chunk in chunks:
        covered.extend(
            range(chunk.verse_number_start, chunk.verse_number_end + 1)
        )
    # Every existing verse is covered exactly once (no overlap configured)
    assert sorted(set(covered) & {1, 3, 4, 5, 6, 7, 8}) == [1, 3, 4, 5, 6, 7, 8]
    # Trailing verses 7-8 are absorbed by the last chunk
    assert chunks[-1].verse_number_end == 8
    # Canonical IDs still come from the plan
    assert chunks[-1].canonical_id == make_canonical_id(1, 1, ranges[-1][0], ranges[-1][1])


def test_chapter_absent_from_plan_falls_back_to_own_structure():
    verses = make_verses([(n, 40, n % 2 == 1) for n in range(1, 7)])
    chunks = chunk_translation({}, {(17, 11): (verses, {1: "Extra"})}, NO_OVERLAP)
    assert chunks
    assert all(c.book_number == 17 and c.chapter_number == 11 for c in chunks)
    assert chunks[0].verse_number_start == 1
    assert chunks[-1].verse_number_end == 6
    assert chunks[0].title == "Extra"


def test_chunks_never_cross_chapter_boundaries():
    chapters = {
        (1, 1): (make_verses([(n, 80, n == 1) for n in range(1, 5)]), set()),
        (1, 2): (make_verses([(n, 80, n == 1) for n in range(1, 5)]), set()),
    }
    plan = build_plan(chapters, NO_OVERLAP)
    data = {key: (verses, {}) for key, (verses, _t) in chapters.items()}
    chunks = chunk_translation(plan, data, NO_OVERLAP)
    for chunk in chunks:
        assert chunk.canonical_id.split(":")[1].split(".")[1] == f"{chunk.chapter_number:03d}"
    chapters_seen = {(c.book_number, c.chapter_number) for c in chunks}
    assert chapters_seen == {(1, 1), (1, 2)}


def test_empty_text_verses_are_skipped_in_text_but_kept_in_range():
    verses = [
        Verse(1, "In the beginning", True),
        Verse(2, "   ", False),
        Verse(3, "the earth was formless", False),
    ]
    planned = build_chapter_plan(1, 1, verses, set(), NO_OVERLAP)
    chunks = apply_chapter_plan(planned, 1, 1, verses, {}, NO_OVERLAP)
    assert len(chunks) == 1
    assert chunks[0].verse_number_start == 1
    assert chunks[0].verse_number_end == 3
    assert "  " not in chunks[0].text.replace("\n\n", "|")
    assert "formless" in chunks[0].text


def test_determinism_same_input_same_output():
    pivot = make_verses(
        [(n, 35 + (n * 7) % 40, n % 3 == 1) for n in range(1, 31)]
    )
    titles = {1: "Alpha", 11: "Beta", 21: "Gamma"}
    first_plan = build_chapter_plan(40, 3, pivot, set(titles), CONFIG)
    second_plan = build_chapter_plan(40, 3, list(reversed(pivot)), set(titles), CONFIG)
    assert first_plan == second_plan  # input order does not matter

    chunks_a = apply_chapter_plan(first_plan, 40, 3, pivot, titles, CONFIG)
    chunks_b = apply_chapter_plan(second_plan, 40, 3, list(reversed(pivot)), titles, CONFIG)
    assert chunks_a == chunks_b
    assert len({c.canonical_id for c in chunks_a}) == len(chunks_a)


def test_overlap_skipped_when_previous_unit_is_too_big():
    # Previous chunk is one huge paragraph of one huge verse: overlap would
    # double the next chunk, so it must be skipped entirely.
    config = ChunkingConfig(
        target_chars=100, min_chars=40, max_chars=200,
        overlap_units=1, overlap_max_chars=50,
    )
    verses = [
        Verse(1, "x" * 150, True),
        Verse(2, "y" * 90, True),
    ]
    planned = build_chapter_plan(1, 1, verses, set(), config)
    assert len(planned) == 2
    assert planned[1].text_start == planned[1].own_start == 2


def test_overlap_falls_back_to_last_verse_when_unit_too_big():
    config = ChunkingConfig(
        target_chars=100, min_chars=40, max_chars=200,
        overlap_units=1, overlap_max_chars=50,
    )
    # First chunk = one paragraph of two verses (91 chars > overlap cap),
    # so the overlap degrades to the last verse only (45 chars <= cap).
    verses = make_verses(
        [(1, 45, True), (2, 45, False), (3, 45, True), (4, 45, False)]
    )
    planned = build_chapter_plan(1, 1, verses, set(), config)
    assert len(planned) == 2
    assert planned[1].own_start == 3
    assert planned[1].text_start == 2


def test_oversized_owned_range_is_refined_by_own_structure():
    # The pivot chapter is tiny (like ru Psalm 119), the translation chapter
    # is huge (like en Psalm 119): trailing verses must not pile up into one
    # monster chunk but get re-chunked at the translation's own boundaries.
    pivot = make_verses([(1, 40, True), (2, 40, False)])
    plan = build_plan({(19, 119): (pivot, set())}, NO_OVERLAP)
    assert own_ranges(plan[(19, 119)]) == [(1, 2)]

    other = make_verses([(n, 60, n % 2 == 1) for n in range(1, 21)])
    chunks = chunk_translation(plan, {(19, 119): (other, {})}, NO_OVERLAP)
    assert len(chunks) > 1
    for chunk in chunks:
        assert len(chunk.text) <= NO_OVERLAP.max_chars
    # Coverage is preserved and contiguous
    assert chunks[0].verse_number_start == 1
    assert chunks[-1].verse_number_end == 20
    for prev, cur in zip(chunks, chunks[1:]):
        assert cur.verse_number_start == prev.verse_number_end + 1
    # Refined IDs are still deterministic coordinate-based IDs
    assert chunks[0].canonical_id.startswith(f"v{CHUNKING_VERSION}:19.119.")
    rerun = chunk_translation(plan, {(19, 119): (other, {})}, NO_OVERLAP)
    assert rerun == chunks


def test_giant_single_verse_stays_one_chunk():
    # A single verse longer than max_chars cannot be split at a natural
    # boundary and must stay intact (e.g. ru Esther 4:17 additions).
    verses = [Verse(1, "z" * 500, True)]
    planned = build_chapter_plan(1, 1, verses, set(), NO_OVERLAP)
    chunks = apply_chapter_plan(planned, 1, 1, verses, {}, NO_OVERLAP)
    assert len(chunks) == 1
    assert chunks[0].verse_number_start == chunks[0].verse_number_end == 1


def test_max_chars_is_respected_for_multi_unit_chunks():
    verses = make_verses([(n, 90, True) for n in range(1, 13)])
    planned = build_chapter_plan(1, 1, verses, set(), NO_OVERLAP)
    chunks = apply_chapter_plan(planned, 1, 1, verses, {}, NO_OVERLAP)
    for chunk in chunks:
        assert len(chunk.text) <= NO_OVERLAP.max_chars



# ---------------------------------------------------------------------------
# Canonical-space IDs (v3, see app/versification.py and ADR 0003)
# ---------------------------------------------------------------------------

def test_canonical_id_clamps_negative_superscription_placeholders():
    # canonical-space verse numbers -1/0 stand for the extra verses of a
    # multi-verse superscription; the canon does not number them
    assert make_canonical_id(19, 51, -1, 5, version=3) == "v3:19.051.000-005"
    assert make_canonical_id(19, 69, 0, 0, version=3) == "v3:19.069.000-000"


def test_canonical_space_verses_chunk_like_any_chapter():
    # a canonical-space Psalm chapter starting at verse 0 (superscription)
    # chunks normally and the plan-slot ID starts at 000
    verses = make_verses([(0, 30, True)] + [(n, 60, n % 3 == 1) for n in range(1, 9)])
    plan = build_plan({(19, 3): (verses, set())}, NO_OVERLAP)
    chunks = chunk_translation(plan, {(19, 3): (verses, {})}, NO_OVERLAP)
    assert chunks[0].canonical_id.split(".")[2].split("-")[0] == "000"
    assert chunks[0].verse_number_start == 0
    assert chunks[-1].verse_number_end == 8


def test_first_chunk_keeps_negative_superscription_verses():
    # Canonical space numbers a two-verse superscription -1 and 0 (e.g. ubh
    # Ps 51). Regression: verse -1 used to be dropped from every chunk,
    # losing real text from the corpus.
    verses = make_verses(
        [(-1, 20, True), (0, 20, False)]
        + [(n, 60, n % 2 == 1) for n in range(1, 6)]
    )
    plan = build_plan({(19, 51): (verses, set())}, NO_OVERLAP)
    chunks = chunk_translation(plan, {(19, 51): (verses, {})}, NO_OVERLAP)
    assert chunks[0].verse_number_start == -1
    assert "v-1" in chunks[0].text
    covered = set()
    for chunk in chunks:
        covered.update(range(chunk.verse_number_start, chunk.verse_number_end + 1))
    assert covered == set(range(-1, 6))


def test_first_chunk_absorbs_verses_below_plan_start():
    # The plan (from a pivot without counted superscriptions) starts at
    # verse 1; a translation with extra verses -1/0 must still keep them.
    pivot = make_verses([(n, 60, n % 2 == 1) for n in range(1, 6)])
    plan = build_plan({(19, 51): (pivot, set())}, NO_OVERLAP)
    other = make_verses(
        [(-1, 20, True), (0, 20, False)]
        + [(n, 60, n % 2 == 1) for n in range(1, 6)]
    )
    chunks = chunk_translation(plan, {(19, 51): (other, {})}, NO_OVERLAP)
    assert chunks[0].verse_number_start == -1
    assert "v-1" in chunks[0].text and "v0" in chunks[0].text
