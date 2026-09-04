"""De-fingerprinting and fidelity checks for the benchmark rewrite prompts.

`evaluation/rewrite_prompts.py` builds experimental rewrite prompts (8a/8b/8c)
for benchmarking small models. Two invariants must hold, and neither can be
left to a comment in a docstring:

1. **The few-shot examples must not leak the evaluation set.** The same rule
   the rerank prompt v6 established (README, "Редакторские решения"): a prompt
   that quotes the reference answers measures the prompt, not the model. So no
   example topic may equal a scenario topic and no example passage may share
   book+chapter with ANY reference of ANY grade in `scenarios.json`.
2. **Version 7 must be the production prompt, byte for byte.** It is imported
   from `app/query_rewrite.py`; if a refactor ever turned that import into a
   local copy, the baseline column of every comparison would silently stop
   being the baseline.

No database and no network: `scenarios.json` and the canon table are read from
disk.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
EVALUATION = REPO_ROOT / "evaluation"
SCENARIOS_FILE = EVALUATION / "scenarios.json"

# The benchmark lives outside the app package and imports `query_rewrite` the
# same flat way `retrieval_benchmark.py` does.
for path in (str(EVALUATION), str(REPO_ROOT / "app")):
    if path not in sys.path:
        sys.path.insert(0, path)

pytestmark = pytest.mark.skipif(
    not SCENARIOS_FILE.exists(),
    reason="evaluation/ is not present in this container copy",
)

rewrite_prompts = pytest.importorskip("rewrite_prompts")

from canon import CANONICAL_BOOKS  # noqa: E402
from query_rewrite import (  # noqa: E402
    REWRITE_VARIANTS,
    build_rewrite_instruction,
)

LANGUAGES = ("ru", "en", "uk")
BOOK_NUMBER_BY_CODE = {code: number for number, code, _ in CANONICAL_BOOKS}


def load_dataset() -> dict:
    return json.loads(SCENARIOS_FILE.read_text(encoding="utf-8"))


def example_items() -> list[tuple[str, str, int, str, str]]:
    """(language, book_code, chapter, display_ref, query) for every example."""
    return [
        (language, book_code, chapter, ref, query)
        for language, examples in rewrite_prompts._EXAMPLES.items()
        for example in examples
        for book_code, chapter, ref, query in example["items"]
    ]


def test_example_book_codes_are_canonical():
    """A typo in a book code would silently disable the collision check."""
    unknown = sorted(
        {code for _, code, _, _, _ in example_items()}
        - set(BOOK_NUMBER_BY_CODE)
    )
    assert not unknown, f"example book codes absent from app/canon.py: {unknown}"


def test_example_topics_do_not_appear_in_the_dataset():
    dataset = load_dataset()
    topics = {
        s["prayer_context"]["topic"].strip().casefold()
        for s in dataset["scenarios"]
    }
    collisions = [
        example["topic"]
        for examples in rewrite_prompts._EXAMPLES.values()
        for example in examples
        if example["topic"].strip().casefold() in topics
    ]
    assert not collisions, f"example topics leak dataset topics: {collisions}"


def test_example_passages_do_not_touch_any_reference():
    """Book+chapter of every example against every reference of every grade."""
    dataset = load_dataset()
    reference_coords = {
        (ref["book_number"], ref["chapter"])
        for scenario in dataset["scenarios"]
        for ref in scenario["references"]
    }
    assert reference_coords, "dataset carries no references — check is vacuous"

    collisions = [
        f"{ref} -> ({BOOK_NUMBER_BY_CODE[code]}, {chapter})"
        for _, code, chapter, ref, _ in example_items()
        if (BOOK_NUMBER_BY_CODE[code], chapter) in reference_coords
    ]
    assert not collisions, (
        "few-shot examples quote graded reference passages "
        f"(de-fingerprint rule): {collisions}"
    )


def test_example_queries_carry_no_digits_and_no_book_names():
    """A query is the passage's own words: no coordinates, no book names."""
    # Book-name tokens actually used in this file's display references, plus
    # the Russian/Ukrainian names of those same books.
    name_tokens = {
        token.casefold()
        for _, _, _, ref, _ in example_items()
        for token in re.findall(r"[A-Za-z]{3,}", ref)
    }
    name_tokens |= {
        "псалом", "псалтир", "притч", "приповіст", "ісая", "исаия",
        "єремі", "иереми", "буття", "бытие", "вихід", "исход",
        "матвія", "матфея", "луки", "івана", "иоанна", "римлян",
        "коринт", "ефесян", "колосян", "тимофія", "тимофею", "пісня",
    }
    problems: list[str] = []
    for language, _, _, ref, query in example_items():
        digits = re.findall(r"\d", query)
        if digits:
            problems.append(f"[{language}] digits in query for {ref}: {query!r}")
        if rewrite_prompts.find_reference_leaks(query):
            problems.append(f"[{language}] chapter:verse in query for {ref}")
        folded = query.casefold()
        hits = sorted(t for t in name_tokens if t in folded)
        if hits:
            problems.append(f"[{language}] book name {hits} in query for {ref}")
    assert not problems, problems


def test_version_7_is_the_production_prompt_byte_for_byte():
    for language in LANGUAGES:
        assert rewrite_prompts.build_instruction(
            "7", language, REWRITE_VARIANTS
        ) == build_rewrite_instruction(language, REWRITE_VARIANTS), language


def test_8x_prompts_build_and_keep_the_v7_body():
    """Each 8x variant extends v7 rather than replacing it."""
    for language in LANGUAGES:
        base = build_rewrite_instruction(language, REWRITE_VARIANTS)
        opening = base.split("\n", 1)[0]
        for version in ("8a", "8b", "8c"):
            text = rewrite_prompts.build_instruction(
                version, language, REWRITE_VARIANTS
            )
            assert text.startswith(opening), (version, language)
            assert len(text) > len(base) * 0.9, (version, language)


def test_8a_and_8c_ask_for_objects_and_8b_does_not():
    text_8a = rewrite_prompts.build_instruction("8a", "ru", REWRITE_VARIANTS)
    text_8b = rewrite_prompts.build_instruction("8b", "ru", REWRITE_VARIANTS)
    text_8c = rewrite_prompts.build_instruction("8c", "ru", REWRITE_VARIANTS)
    for text in (text_8a, text_8c):
        assert '"ref"' in text and '"query"' in text
    assert '"ref"' not in text_8b


def test_few_shot_prompts_forbid_copying_and_name_the_language():
    """Revision 2 of 8b/8c: the closing reminder after the examples."""
    for version in ("8b", "8c"):
        for language, expected_name in (
            ("ru", "Russian"), ("en", "English"), ("uk", "Ukrainian")
        ):
            text = rewrite_prompts.build_instruction(
                version, language, REWRITE_VARIANTS
            )
            assert "Never copy a sentence from an example" in text
            reminder_at = text.rindex("Never copy a sentence from an example")
            last_example_at = text.rindex("### Example")
            assert reminder_at > last_example_at, (
                f"{version}/{language}: the reminder must follow the examples"
            )
            assert f"Write your queries in {expected_name}" in text


def test_parse_response_splits_refs_from_queries():
    queries, refs = rewrite_prompts.parse_response(
        "8c",
        '{"queries": [{"ref": "Psalm 23:1", "query": "The LORD is my shepherd"},'
        ' {"ref": "John 3:16", "query": "For God so loved the world"}]}',
        6,
    )
    assert queries == ["The LORD is my shepherd", "For God so loved the world"]
    assert refs == ["Psalm 23:1", "John 3:16"]


def test_parse_response_v7_uses_the_production_parser():
    queries, refs = rewrite_prompts.parse_response(
        "7", '```json\n{"queries": ["alpha", "beta", "alpha"]}\n```', 6
    )
    assert queries == ["alpha", "beta"]  # deduplicated by the production parser
    assert refs == []


def test_parse_response_truncates_at_the_production_limit():
    from query_rewrite import _MAX_QUERY_CHARS

    long_query = "a" * (_MAX_QUERY_CHARS + 50)
    queries, _ = rewrite_prompts.parse_response(
        "8a", json.dumps({"queries": [{"ref": "R", "query": long_query}]}), 6
    )
    assert len(queries[0]) == _MAX_QUERY_CHARS


def test_find_reference_leaks_spots_a_coordinate():
    assert rewrite_prompts.find_reference_leaks("as it says in 23:1 today")
    assert not rewrite_prompts.find_reference_leaks("the LORD is my shepherd")


def test_unknown_prompt_version_is_rejected():
    with pytest.raises(ValueError):
        rewrite_prompts.build_instruction("9z", "ru", REWRITE_VARIANTS)


def test_every_declared_version_has_a_revision():
    assert set(rewrite_prompts.PROMPT_VERSIONS) == set(
        rewrite_prompts.PROMPT_REVISIONS
    )
    assert rewrite_prompts.PROMPT_REVISIONS["8b"] >= 2
    assert rewrite_prompts.PROMPT_REVISIONS["8c"] >= 2


def test_example_queries_helper_matches_the_examples():
    assert rewrite_prompts.example_queries() == {
        " ".join(query.split()) for _, _, _, _, query in example_items()
    }


# --------------------------------------------------------------------------
# gen_rewrites: a partial run must be recognisable from the artifact alone.
# A full 21-scenario run was once lost because the file was written only at
# the end and a partial file was indistinguishable from a full one.
# --------------------------------------------------------------------------

gen_rewrites = pytest.importorskip("gen_rewrites")


class _Args:
    model = "test-model"
    prompt_version = "8c"
    endpoint = "http://localhost:11434/v1"
    temperature = 0.0
    max_tokens = 1024
    variants = 6


def _dataset_and_eligible():
    dataset = load_dataset()
    eligible = [s for s in dataset["scenarios"] if s["category"] != "empty"]
    return dataset, eligible


def _records(ids):
    return [{"id": i, "language": i[:2], "variants": ["x"], "error": None}
            for i in ids]


def test_artifact_is_marked_partial_when_scenarios_are_missing():
    dataset, eligible = _dataset_and_eligible()
    artifact = gen_rewrites.build_artifact(
        _Args(), dataset, _records(["ru-002", "en-001"]), eligible
    )
    meta = artifact["meta"]
    assert meta["partial"] is True
    assert meta["scenarios_covered"] == ["ru-002", "en-001"]
    assert meta["scenarios_expected"] == len(eligible)
    assert "PARTIAL" in meta["note"]


def test_artifact_is_full_when_every_non_empty_scenario_is_covered():
    dataset, eligible = _dataset_and_eligible()
    artifact = gen_rewrites.build_artifact(
        _Args(), dataset, _records([s["id"] for s in eligible]), eligible
    )
    meta = artifact["meta"]
    assert meta["partial"] is False
    assert meta["scenarios_expected"] == len(eligible)
    # `empty` scenarios are deliberately not expected: production answers them
    # from the safe pool before the rewrite stage runs.
    assert len(eligible) < len(dataset["scenarios"])
    assert "note" not in meta


def test_artifact_records_the_sampling_actually_used():
    dataset, eligible = _dataset_and_eligible()
    meta = gen_rewrites.build_artifact(
        _Args(), dataset, _records(["ru-002"]), eligible
    )["meta"]
    assert meta["sampling"]["max_tokens"] == 1024
    assert meta["sampling"]["variants"] == 6
    assert meta["prompt_revision"] == rewrite_prompts.PROMPT_REVISIONS["8c"]
    # The endpoint is recorded without any path or query string (keys live
    # in those on some providers).
    assert meta["endpoint"] == "http://localhost:11434"


def test_write_artifact_is_atomic_and_leaves_no_temp_file(tmp_path):
    out = tmp_path / "artifact.json"
    gen_rewrites.write_artifact(out, {"meta": {"a": 1}, "scenarios": []})
    assert json.loads(out.read_text(encoding="utf-8"))["meta"]["a"] == 1
    gen_rewrites.write_artifact(out, {"meta": {"a": 2}, "scenarios": []})
    assert json.loads(out.read_text(encoding="utf-8"))["meta"]["a"] == 2
    assert list(tmp_path.iterdir()) == [out]


def test_relative_paths_resolve_against_the_evaluation_directory():
    assert gen_rewrites.resolve_path("bench_data/x.json") == (
        EVALUATION / "bench_data" / "x.json"
    )
    assert gen_rewrites.resolve_path("/tmp/x.json") == Path("/tmp/x.json")
