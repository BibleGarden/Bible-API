"""Tests for evaluation/retrieval_benchmark.py.

The benchmark is not part of the service, but two of its decisions cost
money or corrupt measurements:

* which Gemini key it bills — one of them is paid
  (AI_SCRIPTURE_REWRITE_API_KEY, ADR 0004);
* whether the embedding matrix it loads actually belongs to the corpus it is
  scoring against (86cbe4n7e).

The module is loaded by path because `evaluation/` is deliberately not on
`pythonpath` (pytest.ini exposes `app` only); its import is side-effect free
apart from putting `app/` on sys.path. Nothing here needs torch, a network
or the corpus files: the local embedder is a stand-in.
"""

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path
from unittest import mock

import numpy as np
import pytest

os.environ.setdefault("API_KEY", "test-api-key")

BENCHMARK = Path(__file__).resolve().parents[1] / "evaluation" / "retrieval_benchmark.py"


def _load_benchmark():
    spec = importlib.util.spec_from_file_location("retrieval_benchmark", BENCHMARK)
    module = importlib.util.module_from_spec(spec)
    # Registered before execution: the module defines dataclasses, and
    # `dataclasses` resolves annotations through sys.modules[cls.__module__].
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def benchmark():
    return _load_benchmark()


def test_key_comes_from_the_environment_first(benchmark, monkeypatch, tmp_path):
    monkeypatch.setenv("SOME_KEY", "from-env")
    env_file = tmp_path / ".env"
    env_file.write_text("SOME_KEY=from-file\n")
    assert benchmark._key_from_env("SOME_KEY", env_file) == "from-env"


def test_key_falls_back_to_the_env_file(benchmark, monkeypatch, tmp_path):
    monkeypatch.delenv("SOME_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("OTHER=x\nSOME_KEY=from-file\n")
    assert benchmark._key_from_env("SOME_KEY", env_file) == "from-file"


def test_missing_env_file_is_not_an_error(benchmark, monkeypatch, tmp_path):
    # Regression: `.env` is absent inside the bible-api image, so reading it
    # unconditionally crashed a documented benchmark run instead of falling
    # back to the shared key.
    monkeypatch.delenv("SOME_KEY", raising=False)
    assert benchmark._key_from_env("SOME_KEY", tmp_path / "nope.env") == ""


def test_rewrite_key_falls_back_to_the_shared_key(benchmark, monkeypatch, tmp_path):
    monkeypatch.setattr(benchmark, "ENV_FILE", tmp_path / "nope.env")
    monkeypatch.delenv("AI_SCRIPTURE_REWRITE_API_KEY", raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "shared-key")
    assert benchmark.require_rewrite_api_key() == "shared-key"


def test_rewrite_key_is_preferred_when_set(benchmark, monkeypatch, tmp_path):
    monkeypatch.setattr(benchmark, "ENV_FILE", tmp_path / "nope.env")
    monkeypatch.setenv("AI_SCRIPTURE_REWRITE_API_KEY", "paid-key")
    monkeypatch.setenv("GEMINI_API_KEY", "shared-key")
    assert benchmark.require_rewrite_api_key() == "paid-key"


@pytest.mark.parametrize("value", ["", "   "])
def test_blank_rewrite_key_falls_back_like_an_unset_one(
    benchmark, monkeypatch, tmp_path, value
):
    monkeypatch.setattr(benchmark, "ENV_FILE", tmp_path / "nope.env")
    monkeypatch.setenv("AI_SCRIPTURE_REWRITE_API_KEY", value)
    monkeypatch.setenv("GEMINI_API_KEY", "shared-key")
    assert benchmark.require_rewrite_api_key() == "shared-key"


def test_no_key_at_all_exits_with_a_message(benchmark, monkeypatch, tmp_path):
    monkeypatch.setattr(benchmark, "ENV_FILE", tmp_path / "nope.env")
    monkeypatch.delenv("AI_SCRIPTURE_REWRITE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(SystemExit) as exc:
        benchmark.require_rewrite_api_key()
    assert "GEMINI_API_KEY" in str(exc.value)


# ---------------------------------------------------------------------------
# The embedding matrix must belong to the corpus it is scored against
# (ClickUp 86cbe4n7e). `corpus[idx] @ qvec` with row indices from a NEWER
# chunks.jsonl returns cosines for the WRONG passages and numpy says nothing,
# so a stale .npy degrades metrics silently — which is how the ADR 0002
# matrices (11 987 rows) survived a corpus re-export to 11 960 unnoticed.
# ---------------------------------------------------------------------------

def _metas(benchmark, n, translation=1):
    return [
        benchmark.ChunkMeta(
            canonical_id=f"v3:19.{i:03d}.001-005", translation=translation,
            book=19, chapter=i, vstart=1, vend=5,
        )
        for i in range(n)
    ]


@pytest.fixture
def corpus_dir(benchmark, monkeypatch, tmp_path):
    """Point the module's bench_data at an empty tmp dir."""
    monkeypatch.setattr(benchmark, "DATA", tmp_path)
    monkeypatch.setattr(benchmark, "CHUNKS_FILE", tmp_path / "chunks.jsonl")
    return tmp_path


def test_fingerprint_tracks_identity_and_order(benchmark):
    metas = _metas(benchmark, 5)
    assert benchmark.corpus_fingerprint(metas) == \
        benchmark.corpus_fingerprint(_metas(benchmark, 5))
    # same count, different documents
    assert benchmark.corpus_fingerprint(metas) != \
        benchmark.corpus_fingerprint(_metas(benchmark, 5, translation=16))
    # same documents, different order
    assert benchmark.corpus_fingerprint(metas) != \
        benchmark.corpus_fingerprint(list(reversed(metas)))


def test_missing_matrix_names_the_embed_command(benchmark, corpus_dir):
    with pytest.raises(SystemExit) as exc:
        benchmark.load_corpus_matrix("e5-small", "title_text",
                                     _metas(benchmark, 3))
    assert "embed --model e5-small" in str(exc.value)


def test_row_count_mismatch_is_a_hard_error(benchmark, corpus_dir):
    benchmark.save_corpus_matrix(
        "e5-small", "title_text", np.zeros((11987, 4), dtype=np.float32),
        _metas(benchmark, 11987))
    with pytest.raises(SystemExit) as exc:
        benchmark.load_corpus_matrix("e5-small", "title_text",
                                     _metas(benchmark, 11960))
    message = str(exc.value)
    assert "11987" in message and "11960" in message
    assert "--force" in message


def test_same_row_count_different_corpus_is_a_hard_error(benchmark, corpus_dir):
    benchmark.save_corpus_matrix(
        "e5-small", "title_text", np.zeros((5, 4), dtype=np.float32),
        _metas(benchmark, 5))
    with pytest.raises(SystemExit) as exc:
        benchmark.load_corpus_matrix(
            "e5-small", "title_text", _metas(benchmark, 5, translation=16))
    assert "DIFFERENT corpus" in str(exc.value)


def test_matching_matrix_loads(benchmark, corpus_dir):
    metas = _metas(benchmark, 5)
    benchmark.save_corpus_matrix(
        "e5-small", "title_text", np.zeros((5, 4), dtype=np.float32), metas)
    assert benchmark.load_corpus_matrix(
        "e5-small", "title_text", metas).shape == (5, 4)


def test_matrix_without_a_sidecar_warns_but_loads(benchmark, corpus_dir, capsys):
    # Files written before the sidecar existed keep working; the row count is
    # still enforced, and the warning says the identity check was skipped.
    metas = _metas(benchmark, 5)
    np.save(benchmark.emb_path("e5-small", "title_text"),
            np.zeros((5, 4), dtype=np.float32))
    assert benchmark.load_corpus_matrix(
        "e5-small", "title_text", metas).shape == (5, 4)
    assert "no .sha1 sidecar" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# `pipeline` label: the `config` field of every saved artifact
# ---------------------------------------------------------------------------

def _pipeline_args(benchmark, **overrides):
    defaults = dict(
        no_rewrite=False, no_raw=True, no_blacklist=False, no_pool=False,
        fusion="interleave", no_lexical=False, lex_k=20, variants=6,
        fetch_k=50, max_per_book=4, max_per_chapter=1,
        coverage_translation=0, embedder=benchmark.PIPELINE_DEFAULT_EMBEDDER,
        doc_text=benchmark.PIPELINE_DEFAULT_DOC_TEXT, descriptions_file="",
        languages="", allow_partial_coverage=False,
    )
    return argparse.Namespace(**{**defaults, **overrides})


def test_default_embedder_leaves_the_label_unchanged(benchmark):
    # Runs recorded before --embedder existed must stay comparable byte for
    # byte, so the default embedder is not mentioned at all.
    label = benchmark.pipeline_label(
        _pipeline_args(benchmark), "gemini-3.7-flash")
    assert "embedder=" not in label
    assert label == (
        "pipeline rewrite=gemini-3.7-flash variants=6 raw=no fusion=interleave"
        " blacklist=on pool=on lexical=k20 fetch_k=50 max_per_book=4"
    )


def test_non_default_embedder_is_named_in_the_label(benchmark):
    label = benchmark.pipeline_label(
        _pipeline_args(benchmark, embedder="bge-m3"), "gemini-3.7-flash")
    assert label.endswith(" embedder=bge-m3")


def test_ablation_flags_reach_the_label(benchmark):
    label = benchmark.pipeline_label(
        _pipeline_args(benchmark, no_lexical=True, no_rewrite=True,
                       embedder="e5-small"), "gemini-3.7-flash")
    assert "lexical=off" in label and "rewrite=off" in label


# ---------------------------------------------------------------------------
# A local embedder must not reach for a Gemini key
# ---------------------------------------------------------------------------

def test_local_embedder_never_reads_the_gemini_key(
    benchmark, corpus_dir, monkeypatch
):
    """`--embedder <local>` embeds both sides itself: no key, no HTTP.

    (The rewrite stage is a separate matter and still bills the paid key
    unless it is taken offline with --rewrites-file/--no-rewrite — that is
    what the --embedder help text says.)
    """
    def explode(*_a, **_k):
        raise AssertionError("a Gemini key was requested")

    monkeypatch.setattr(benchmark, "require_api_key", explode)
    monkeypatch.setattr(benchmark, "_query_vector", explode)

    class FakeModel:
        max_seq_length = 8192

        def encode(self, texts, **_kwargs):
            return np.ones((len(texts), 4), dtype=np.float32)

    monkeypatch.setattr(benchmark, "load_st_model",
                        lambda model_id, max_seq_length=0: FakeModel())
    metas = _metas(benchmark, 5)
    benchmark.save_corpus_matrix(
        "e5-small", "title_text", np.zeros((5, 4), dtype=np.float32), metas)

    corpus, query_vector = benchmark._pipeline_embedder(
        "e5-small", metas, ["t"] * 5, {"query_embeddings": {}})
    assert corpus.shape == (5, 4)
    assert query_vector("тревога").shape == (4,)


def test_offline_rerank_is_named_in_the_label(benchmark):
    label = benchmark.pipeline_label(
        _pipeline_args(benchmark), "gemini-3.7-flash", "file:qwen3-4b")
    assert label.endswith(" rerank=file:qwen3-4b")


# ---------------------------------------------------------------------------
# Rerank input export and `--reranks-file` (ClickUp 86cbed851).
#
# The export exists so a local model can answer the SAME question the
# production reranker is asked; if the exported prompt drifted from
# `passage_rerank.build_rerank_*`, the measurement would be of another prompt.
# And an answer is an INDEX into a candidate list, so a file made against a
# different list must be refused rather than silently believed.
# ---------------------------------------------------------------------------

SCENARIO = {
    "id": "ru-001",
    "language": "ru",
    "category": "regular",
    "prayer_context": {"topic": "О детях", "user_replies": ["дочь родилась"]},
}


def _export_meta(benchmark, ids, translation=1):
    return {
        (translation, cid): benchmark.ChunkMeta(
            canonical_id=cid, translation=translation, book=19,
            chapter=number, vstart=1, vend=5,
        )
        for number, cid in enumerate(ids, start=1)
    }


def test_exported_prompt_is_the_production_prompt(benchmark):
    from passage_rerank import (
        build_rerank_instruction,
        build_rerank_response_schema,
        build_rerank_user_content,
    )

    ids = ["v3:19.127.001-005", "v3:19.128.001-006"]
    texts = ["Title\n[1] verse one [2] verse two", "[1] another verse"]
    record = benchmark.build_rerank_input_record(
        SCENARIO, 1, ids, texts, _export_meta(benchmark, ids), True)

    assert record["user_content"] == build_rerank_user_content(
        SCENARIO["prayer_context"]["topic"],
        SCENARIO["prayer_context"]["user_replies"],
        texts,
    )
    assert record["instruction"] == build_rerank_instruction(2, True)
    assert record["response_schema"] == build_rerank_response_schema(2, True)
    assert record["candidate_count"] == 2
    assert [c["canonical_id"] for c in record["candidates"]] == ids
    assert [c["number"] for c in record["candidates"]] == [1, 2]


def test_export_without_markers_drops_the_key_verse_contract(benchmark):
    from passage_rerank import build_rerank_instruction

    ids = ["v3:19.127.001-005"]
    record = benchmark.build_rerank_input_record(
        SCENARIO, 1, ids, ["plain stored text"],
        _export_meta(benchmark, ids), False)
    assert record["instruction"] == build_rerank_instruction(1, False)
    assert "key_verse_start" not in record["response_schema"]["properties"]


def test_exported_hash_is_the_rerank_cache_key(benchmark):
    """The export must key on exactly what the `reranks` cache keys on.

    Proved by a cache HIT: the fake reranker below explodes if called, so the
    lookup can only succeed when the two hashes agree.
    """
    from passage_rerank import RERANK_PROMPT_VERSION

    ids = ["v3:19.127.001-005", "v3:19.023.001-006"]
    record = benchmark.build_rerank_input_record(
        SCENARIO, 1, ids, ["a", "b"], _export_meta(benchmark, ids), True)
    key = (f"model-x|p{RERANK_PROMPT_VERSION}|ru-001|"
           f"{record['candidates_hash']}")
    cache = {"reranks": {key: {"index": 1, "reason": "r",
                               "key_verse_start": 2, "key_verse_end": 3}}}

    class Explode:
        def choose(self, **_kwargs):
            raise AssertionError("the cached answer was not found")

    got = benchmark._rerank_cached(
        SCENARIO, ids, ["a", "b"], Explode(), "model-x", cache,
        {"calls": 0, "failures": 0})
    assert got == (1, "r", 2, 3)


def test_candidate_hash_depends_on_order(benchmark):
    ids = ["a", "b"]
    assert benchmark.candidates_hash(ids) != \
        benchmark.candidates_hash(list(reversed(ids)))


def test_detail_candidate_ids_reads_both_shapes(benchmark):
    assert benchmark.detail_candidate_ids(
        {"top": [{"id": "a", "score": 1}, {"id": "b", "score": 2}]}
    ) == ["a", "b"]
    # safe_pool rows store bare ids
    assert benchmark.detail_candidate_ids({"top": ["a", "b"]}) == ["a", "b"]
    assert benchmark.detail_candidate_ids({"top": []}) == []


def _reranks_file(tmp_path, rows, model="qwen3-4b"):
    path = tmp_path / "reranks.json"
    path.write_text(json.dumps({"meta": {"model": model}, "scenarios": rows}))
    return str(path)


def test_external_reranks_are_loaded_by_id(benchmark, tmp_path):
    path = _reranks_file(tmp_path, [
        {"id": "ru-001", "chosen_index": 2, "candidates_hash": "h",
         "key_verse_span": [1, 2], "error": None},
    ])
    records, meta = benchmark._load_external_reranks(path)
    assert meta["model"] == "qwen3-4b"
    stats = {"calls": 0, "failures": 0, "missing": 0}
    with mock.patch.object(benchmark, "candidates_hash", lambda _ids: "h"):
        got = benchmark._external_rerank_choice(
            SCENARIO, ["a", "b", "c"], records, path, stats)
    assert got[0] == 2 and got[2] == 1 and got[3] == 2
    assert stats == {"calls": 0, "failures": 0, "missing": 0}


def test_an_answer_without_a_hash_is_a_hard_error(benchmark, tmp_path):
    # Otherwise dropping one field would be enough to walk past the guard:
    # an answer that cannot be checked is refused, not believed.
    path = _reranks_file(tmp_path, [{"id": "ru-001", "chosen_index": 0}])
    records, _meta = benchmark._load_external_reranks(path)
    with pytest.raises(SystemExit) as exc:
        benchmark._external_rerank_choice(
            SCENARIO, ["a", "b"], records, path,
            {"calls": 0, "failures": 0, "missing": 0})
    assert "candidates_hash" in str(exc.value)


def test_a_different_candidate_list_is_a_hard_error(benchmark, tmp_path):
    path = _reranks_file(tmp_path, [
        {"id": "ru-001", "chosen_index": 0, "candidates_hash": "made-for-another-run"},
    ])
    records, _meta = benchmark._load_external_reranks(path)
    with pytest.raises(SystemExit) as exc:
        benchmark._external_rerank_choice(
            SCENARIO, ["a", "b"], records, path,
            {"calls": 0, "failures": 0, "missing": 0})
    message = str(exc.value)
    assert "ru-001" in message and "made-for-another-run" in message
    assert "export-rerank-input" in message


@pytest.mark.parametrize("row, expected_missing", [
    ({"id": "ru-001", "chosen_index": None, "candidates_hash": "h",
      "error": "parse: rerank response is not valid JSON"}, 0),
    # a hand-edited index the production parser would have refused
    ({"id": "ru-001", "chosen_index": 7, "candidates_hash": "h"}, 0),
])
def test_failed_answers_degrade_like_a_live_rerank_error(
    benchmark, tmp_path, row, expected_missing
):
    path = _reranks_file(tmp_path, [row])
    records, _meta = benchmark._load_external_reranks(path)
    stats = {"calls": 0, "failures": 0, "missing": 0}
    with mock.patch.object(benchmark, "candidates_hash", lambda _ids: "h"):
        got = benchmark._external_rerank_choice(
            SCENARIO, ["a", "b"], records, path, stats)
    assert got is None
    assert stats["failures"] == 1 and stats["missing"] == expected_missing


@pytest.mark.parametrize("span", [
    [2], [], [1, 2, 3], "1-2", [1, None], ["1", "2"], [True, True], None,
])
def test_a_malformed_key_verse_span_costs_the_highlight_not_the_run(
    benchmark, tmp_path, span
):
    # ADR 0005: a broken highlight never invalidates the passage choice — and
    # it must not raise either (a one-element span used to be an IndexError).
    path = _reranks_file(tmp_path, [
        {"id": "ru-001", "chosen_index": 1, "candidates_hash": "h",
         "key_verse_span": span},
    ])
    records, _meta = benchmark._load_external_reranks(path)
    with mock.patch.object(benchmark, "candidates_hash", lambda _ids: "h"):
        got = benchmark._external_rerank_choice(
            SCENARIO, ["a", "b"], records, path,
            {"calls": 0, "failures": 0, "missing": 0})
    assert got[0] == 1
    assert got[2] is None and got[3] is None


def test_a_scenario_absent_from_the_file_degrades_and_is_counted(
    benchmark, tmp_path
):
    # A probe run (--only) covers a subset; the rest must fall back to
    # retrieval rank-1 rather than abort the whole run.
    path = _reranks_file(tmp_path, [])
    records, _meta = benchmark._load_external_reranks(path)
    stats = {"calls": 0, "failures": 0, "missing": 0}
    assert benchmark._external_rerank_choice(
        SCENARIO, ["a"], records, path, stats) is None
    assert stats["failures"] == 1 and stats["missing"] == 1


# --- end to end: the exported artifact, without a database ------------------

def _write_export_inputs(benchmark, monkeypatch, tmp_path):
    """A two-chunk corpus, one scenario and a recorded run over them."""
    chunks = tmp_path / "chunks.jsonl"
    with chunks.open("w") as fh:
        for chapter, title in ((127, "Песнь восхождения"), (23, "")):
            fh.write(json.dumps({
                "canonical_id": f"v3:19.{chapter:03d}.001-005",
                "translation": 1, "book_number": 19,
                "chapter_number": chapter, "verse_number_start": 1,
                "verse_number_end": 5, "title": title,
                "text": f"stored text of chapter {chapter}",
            }, ensure_ascii=False) + "\n")
    scenarios = tmp_path / "scenarios.json"
    scenarios.write_text(json.dumps(
        {"version": "0.7.0", "scenarios": [SCENARIO]}, ensure_ascii=False))
    results = tmp_path / "results.json"
    results.write_text(json.dumps({
        "config": "pipeline rewrite=gemini-3.7-flash",
        "details": [
            {"scenario_id": "ru-001", "source": "retrieval", "queries": [],
             "top": [{"id": "v3:19.127.001-005", "score": 0.9, "variant": 0},
                     {"id": "v3:19.023.001-005", "score": 0.8, "variant": 1}]},
            {"scenario_id": "ru-009", "source": "safe_pool", "queries": [],
             "top": ["v3:19.023.001-005"]},
        ],
    }))
    monkeypatch.setattr(benchmark, "CHUNKS_FILE", chunks)
    monkeypatch.setattr(benchmark, "SCENARIOS_FILE", scenarios)
    return argparse.Namespace(
        results=str(results), out=str(tmp_path / "input.json"), only="")


def test_export_end_to_end_without_a_database(benchmark, monkeypatch, tmp_path):
    from passage_rerank import build_rerank_user_content

    args = _write_export_inputs(benchmark, monkeypatch, tmp_path)
    # No database: candidates keep their stored text and carry no markers,
    # exactly as cmd_pipeline and production degrade.
    monkeypatch.setattr(benchmark, "_load_chunk_verses", lambda _metas: {})
    benchmark.cmd_export_rerank_input(args)

    artifact = json.loads(Path(args.out).read_text())
    assert artifact["meta"]["key_verses"] is False
    # the safe_pool scenario is not a rerank scenario, so it is not expected
    assert artifact["meta"]["partial"] is False
    assert artifact["meta"]["scenarios_expected"] == 1
    assert artifact["meta"]["source_config"].startswith("pipeline rewrite=")
    entry = artifact["scenarios"][0]
    assert entry["id"] == "ru-001"
    assert entry["user_content"] == build_rerank_user_content(
        "О детях", ["дочь родилась"],
        ["Песнь восхождения\nstored text of chapter 127",
         "stored text of chapter 23"],
    )


def test_export_numbers_the_verses_when_the_database_answers(
    benchmark, monkeypatch, tmp_path
):
    from passage_rerank import build_rerank_user_content
    from retrieval import VerseText

    args = _write_export_inputs(benchmark, monkeypatch, tmp_path)
    verses = {
        (1, "v3:19.127.001-005"): [
            VerseText(verse_number=1, text="Вот наследие", start_paragraph=True),
            VerseText(verse_number=2, text="от Господа", start_paragraph=False),
        ],
        (1, "v3:19.023.001-005"): [
            VerseText(verse_number=1, text="Господь Пастырь мой",
                      start_paragraph=True),
        ],
    }
    monkeypatch.setattr(benchmark, "_load_chunk_verses", lambda _metas: verses)
    benchmark.cmd_export_rerank_input(args)

    artifact = json.loads(Path(args.out).read_text())
    assert artifact["meta"]["key_verses"] is True
    entry = artifact["scenarios"][0]
    assert entry["user_content"] == build_rerank_user_content(
        "О детях", ["дочь родилась"],
        ["Песнь восхождения\n[1] Вот наследие [2] от Господа",
         "[1] Господь Пастырь мой"],
    )
    assert "[1]" in entry["instruction"]


# --- one renderer for both branches, and it is production's ----------------

def _production_prompt_text(title, verses, stored_text):
    """What `retrieval._candidate_prompt_text` produces for one chunk."""
    from retrieval import Candidate, PassageText, _candidate_prompt_text

    passage = PassageText(
        translation=1, alias="syn", book_number=19, chapter_number=127,
        verse_number_start=1, verse_number_end=5, title=title,
        text=stored_text, verses=verses,
    )
    return _candidate_prompt_text(Candidate(
        canonical_id="v3:19.127.001-005", book_number=19, chapter_number=127,
        verse_start=1, verse_end=5, score=1.0, best_variant=0,
        variant_scores={}, passages=[passage],
    ))


@pytest.mark.parametrize("title", ["Песнь восхождения", ""])
def test_the_benchmark_renders_candidates_exactly_like_production(
    benchmark, title
):
    """`pipeline --rerank` and `export-rerank-input` share this one function.

    They used to hold a copy each, and the export is only worth anything if
    it is byte-identical to what the live stage sends.
    """
    from retrieval import VerseText

    key = (1, "v3:19.127.001-005")
    verses = [
        VerseText(verse_number=1, text="Вот наследие", start_paragraph=True),
        VerseText(verse_number=2, text="от Господа", start_paragraph=False),
        VerseText(verse_number=3, text="награда от Него", start_paragraph=True),
    ]
    stored = "Вот наследие от Господа\n\nнаграда от Него"
    chunk_title, chunk_text = {key: title}, {key: stored}

    assert benchmark.candidate_prompt_text(
        key, {key: verses}, chunk_title, chunk_text
    ) == _production_prompt_text(title or None, verses, stored)
    # and the degraded path: no verses loaded -> the plain stored text
    assert benchmark.candidate_prompt_text(
        key, {}, chunk_title, chunk_text
    ) == _production_prompt_text(title or None, [], stored)


# --- `--reranks-file` never builds a reranker and never touches the cache ---

def _mini_pipeline(benchmark, monkeypatch, tmp_path, reranks_file):
    """`cmd_pipeline` over a two-chunk corpus and one scenario, no I/O."""
    chunks = tmp_path / "chunks.jsonl"
    ids = ["v3:19.127.001-005", "v3:19.023.001-005"]
    with chunks.open("w") as fh:
        for canonical_id, chapter in zip(ids, (127, 23)):
            fh.write(json.dumps({
                "canonical_id": canonical_id, "translation": 1,
                "book_number": 19, "chapter_number": chapter,
                "verse_number_start": 1, "verse_number_end": 5,
                "title": "", "text": f"text {chapter}",
            }) + "\n")
    scenarios = tmp_path / "scenarios.json"
    scenarios.write_text(json.dumps({
        "version": "0.7.0",
        "scenarios": [{**SCENARIO, "references": [
            {"book_number": 19, "chapter": 127, "verse_start": 1,
             "verse_end": 5, "grade": "relevant", "reason": "r"},
        ]}],
    }, ensure_ascii=False))
    monkeypatch.setattr(benchmark, "CHUNKS_FILE", chunks)
    monkeypatch.setattr(benchmark, "SCENARIOS_FILE", scenarios)
    monkeypatch.setattr(benchmark, "load_psalm_maps", lambda: {})
    # Psalm references would need the versification tables; the reference
    # above is graded through a stand-in that maps a Psalm to itself.
    monkeypatch.setattr(
        benchmark, "map_reference",
        lambda ref, _t, _m: [(ref["book_number"], ref["chapter"],
                              ref["verse_start"], ref["verse_end"])])
    monkeypatch.setattr(benchmark, "_load_chunk_verses", lambda _m: {})
    monkeypatch.setattr(benchmark, "_load_book_names", lambda: {})
    monkeypatch.setattr(
        benchmark, "_pipeline_embedder",
        lambda *_a, **_k: (np.ones((2, 4), dtype=np.float32),
                           lambda _t: np.ones(4, dtype=np.float32)))
    cache = {"rewrites": {}, "query_embeddings": {},
             "reranks": {"pre-existing": {"index": 0}}}
    monkeypatch.setattr(benchmark, "_load_pipeline_cache", lambda: cache)
    monkeypatch.setattr(benchmark, "_save_pipeline_cache", lambda _c: None)
    monkeypatch.setattr(
        benchmark, "require_api_key",
        lambda: (_ for _ in ()).throw(AssertionError("a key was requested")))

    args = _pipeline_args(
        benchmark, no_rewrite=True, no_lexical=True, rerank=False,
        rerank_model="", rewrite_model="", cache_tag="", rewrites_file="",
        reranks_file=reranks_file, json_out="")
    return args, cache, ids


def test_reranks_file_builds_no_reranker_and_leaves_the_cache_alone(
    benchmark, monkeypatch, tmp_path, capsys
):
    import passage_rerank

    path = tmp_path / "reranks.json"
    args, cache, ids = _mini_pipeline(benchmark, monkeypatch, tmp_path,
                                      str(path))
    # chosen_index 1 — the SECOND candidate, so the file's answer cannot be
    # mistaken for a plain rank-1 fallback. Both corpus rows carry the same
    # stand-in embedding, so the fused order is the corpus order.
    path.write_text(json.dumps({
        "meta": {"model": "qwen3-4b"},
        "scenarios": [{
            "id": "ru-001", "chosen_index": 1,
            "candidates_hash": benchmark.candidates_hash(ids),
            "key_verse_span": None, "error": None, "reason": "from the file",
        }],
    }))
    monkeypatch.setattr(
        passage_rerank.GeminiPassageReranker, "__init__",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("a rerank provider was constructed")))
    before = json.dumps(cache["reranks"], sort_keys=True)

    benchmark.cmd_pipeline(args)

    assert json.dumps(cache["reranks"], sort_keys=True) == before
    out = capsys.readouterr().out
    assert "rerank: model=file:qwen3-4b" in out
    assert "rerank=file:qwen3-4b" in out          # the run label
    assert "final top-1: rerank file:qwen3-4b" in out
    # the file's choice (candidate 2), not the retrieval rank-1 (candidate 1)
    rerank_section = out.split("final top-1: rerank file:qwen3-4b")[1] \
        .split("=== ")[0]
    assert ids[1] in rerank_section and ids[0] not in rerank_section


def test_gemini_embedder_still_requires_the_key(benchmark, corpus_dir, monkeypatch):
    metas = _metas(benchmark, 5)
    benchmark.save_corpus_matrix(
        "gemini", "title_text",
        np.zeros((5, benchmark.PIPELINE_DIMS), dtype=np.float32), metas)
    monkeypatch.setattr(
        benchmark, "require_api_key",
        lambda: (_ for _ in ()).throw(SystemExit("GEMINI_API_KEY not found")))
    with pytest.raises(SystemExit):
        benchmark._pipeline_embedder("gemini", metas, [], {})


# ---------------------------------------------------------------------------
# Doc-text variants: what gets EMBEDDED as the document (ClickUp 86cbeef7h).
#
# The experiment indexes a fragment by descriptions of the situations it can
# serve instead of by its own words. Three things must hold: the default must
# not move by a single byte (every recorded measurement is against it), a
# fragment's senses must collapse back to ONE hit before fusion, and a
# language the sense file does not cover must be refused rather than quietly
# scored against the old index.
# ---------------------------------------------------------------------------

def _senses_file(tmp_path, rows, meta=None):
    path = tmp_path / "senses.jsonl"
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )
    if meta is not None:
        (tmp_path / "senses.jsonl.meta.json").write_text(json.dumps(meta))
    return path


def test_the_default_doc_text_is_byte_identical_to_the_old_behaviour(benchmark):
    metas = _metas(benchmark, 3)
    texts = ["t0", "t1", "t2"]
    title_texts = ["T0\n\nt0", "T1\n\nt1", "T2\n\nt2"]
    docs, owners, stats = benchmark.build_doc_texts(
        "title_text", metas, texts, title_texts, {})
    assert docs == title_texts
    assert owners == [0, 1, 2]
    assert stats["missing"] == 0 and stats["senses"] == 0
    # and the matrix keeps the historical name, so every .npy and .sha1 on
    # disk stays valid
    assert benchmark.doc_matrix_variant("title_text", "") == "title_text"
    assert benchmark.doc_matrix_variant("text", "") == "text"


def test_a_default_run_says_nothing_about_doc_text_in_its_label(benchmark):
    label = benchmark.pipeline_label(
        _pipeline_args(benchmark), "gemini-3.7-flash")
    assert "doc=" not in label and "langs=" not in label
    assert label == (
        "pipeline rewrite=gemini-3.7-flash variants=6 raw=no fusion=interleave"
        " blacklist=on pool=on lexical=k20 fetch_k=50 max_per_book=4"
    )


def test_a_doc_text_variant_and_a_language_subset_reach_the_label(benchmark):
    args = _pipeline_args(benchmark, doc_text="description", languages=["en"])
    args.doc_coverage = 1.0
    assert benchmark.pipeline_label(args, "gemini-3.7-flash").endswith(
        " doc=description doc_cov=1.000 langs=en")


def test_partial_coverage_is_visible_in_the_label(benchmark):
    """A half-annotated corpus is a different measurement and must say so."""
    args = _pipeline_args(benchmark, doc_text="description")
    args.doc_coverage = 0.4213
    assert " doc=description doc_cov=0.421" in benchmark.pipeline_label(
        args, "gemini-3.7-flash")


def test_every_sense_becomes_its_own_row_under_the_same_fragment(benchmark):
    metas = _metas(benchmark, 3)
    descriptions = {
        (1, metas[0].canonical_id): ["для скорбящего", "для благодарящего"],
        (1, metas[1].canonical_id): ["для ждущего"],
        (1, metas[2].canonical_id): ["a", "b", "c"],
    }
    docs, owners, stats = benchmark.build_doc_texts(
        "description", metas, ["t"] * 3, ["T"] * 3, descriptions)
    assert docs == ["для скорбящего", "для благодарящего", "для ждущего",
                    "a", "b", "c"]
    assert owners == [0, 0, 1, 2, 2, 2]
    assert stats["senses"] == 6 and stats["rows"] == 6 and stats["missing"] == 0


def test_the_combined_variant_keeps_one_row_per_fragment(benchmark):
    metas = _metas(benchmark, 1)
    descriptions = {(1, metas[0].canonical_id): ["первый", "второй"]}
    docs, owners, _stats = benchmark.build_doc_texts(
        "title_text_description", metas, ["t"], ["T\n\nt"], descriptions)
    assert docs == ["T\n\nt\n\nпервый\nвторой"]
    assert owners == [0]


def test_a_fragment_without_senses_degrades_to_title_text_and_is_counted(
    benchmark
):
    metas = _metas(benchmark, 2)
    descriptions = {(1, metas[0].canonical_id): ["для ждущего"]}
    docs, owners, stats = benchmark.build_doc_texts(
        "description", metas, ["t0", "t1"], ["T0", "T1"], descriptions)
    assert docs == ["для ждущего", "T1"]
    assert owners == [0, 1]
    assert stats["missing"] == 1
    assert stats["missing_by_language"] == {"ru": 1}
    assert stats["fragments_by_language"] == {"ru": 2}


def test_the_matrix_name_carries_the_sense_file_and_the_languages(benchmark):
    assert benchmark.doc_matrix_variant("description", "abc123def456") == \
        "description_abc123def456"
    assert benchmark.doc_matrix_variant(
        "description", "abc123def456", ["en"]) == "description_abc123def456_en"
    # all three languages is the same thing as no restriction
    assert benchmark.doc_matrix_variant(
        "description", "abc123def456", ["ru", "en", "uk"]) == \
        "description_abc123def456"
    with pytest.raises(ValueError):
        benchmark.doc_matrix_variant("description", "")


def test_a_language_subset_drops_the_other_languages_rows(benchmark):
    ru = _metas(benchmark, 1, translation=1)
    en = _metas(benchmark, 1, translation=16)
    metas = ru + en
    descriptions = {
        (1, metas[0].canonical_id): ["ru sense"],
        (16, metas[1].canonical_id): ["en sense"],
    }
    docs, owners, _stats = benchmark.build_doc_texts(
        "description", metas, ["t", "t"], ["T", "T"], descriptions, ["en"])
    assert docs == ["en sense"] and owners == [1]


def test_languages_are_parsed_in_canonical_order_and_validated(benchmark):
    assert benchmark.parse_languages("") == ["ru", "en", "uk"]
    assert benchmark.parse_languages("uk,en") == ["en", "uk"]
    with pytest.raises(SystemExit) as exc:
        benchmark.parse_languages("de")
    assert "de" in str(exc.value)


def test_sense_files_are_read_last_row_wins_and_error_rows_ignored(
    benchmark, tmp_path
):
    path = _senses_file(tmp_path, [
        {"canonical_id": "a", "translation": 1, "senses": ["first"],
         "caution": False},
        {"canonical_id": "a", "translation": 1, "senses": ["retried"],
         "caution": True},
        {"canonical_id": "b", "translation": 1, "senses": [],
         "caution": False, "error": "incomplete"},
    ], meta={"model": "some-model"})
    by_key, meta, digest = benchmark.load_descriptions(str(path))
    # the resumed retry wins; the error row leaves the fragment un-annotated
    assert by_key == {(1, "a"): ["retried"]}
    assert meta["model"] == "some-model" and meta["caution_rows"] == 1
    assert len(digest) == 12


def test_a_sense_file_covering_no_language_of_the_run_is_refused(
    benchmark, monkeypatch, tmp_path
):
    """No silent degradation of a whole language (Maria, 2026-09-04)."""
    path = _senses_file(tmp_path, [
        {"canonical_id": "v3:19.000.001-005", "translation": 16,
         "senses": ["en sense"], "caution": False},
    ])
    args, _cache, _ids = _mini_pipeline(benchmark, monkeypatch, tmp_path, "")
    args.doc_text = "description"
    args.descriptions_file = str(path)
    with pytest.raises(SystemExit) as exc:
        benchmark.cmd_pipeline(args)
    message = str(exc.value)
    assert "annotates no fragment at all in ['ru']" in message
    assert "--languages" in message


def test_a_description_run_needs_its_file_and_refuses_an_unused_one(
    benchmark, monkeypatch, tmp_path
):
    args, _cache, _ids = _mini_pipeline(benchmark, monkeypatch, tmp_path, "")
    args.doc_text = "description"
    with pytest.raises(SystemExit) as exc:
        benchmark.cmd_pipeline(args)
    assert "requires --descriptions-file" in str(exc.value)

    args.doc_text = "title_text"
    args.descriptions_file = str(_senses_file(tmp_path, []))
    with pytest.raises(SystemExit) as exc:
        benchmark.cmd_pipeline(args)
    assert "is ignored by --doc-text title_text" in str(exc.value)


def test_the_senses_of_one_fragment_collapse_to_its_best_hit(
    benchmark, monkeypatch, tmp_path, capsys
):
    """One hit per fragment, best sense first, BEFORE fusion and diversity."""
    args, _cache, ids = _mini_pipeline(benchmark, monkeypatch, tmp_path, "")
    path = _senses_file(tmp_path, [
        {"canonical_id": ids[0], "translation": 1,
         "senses": ["weak sense", "strong sense"], "caution": False},
        {"canonical_id": ids[1], "translation": 1,
         "senses": ["middling sense"], "caution": True},
    ])
    args.doc_text = "description"
    args.descriptions_file = str(path)
    args.json_out = str(tmp_path / "out.json")

    seen = {}

    def fake_embedder(model_key, row_metas, docs, cache, variant, provider="local"):
        seen["docs"] = list(docs)
        seen["variant"] = variant
        seen["provider"] = provider
        seen["row_ids"] = [m.canonical_id for m in row_metas]
        # rows in order: chunk0/sense0, chunk0/sense1, chunk1/sense0
        corpus = np.array([[0.1], [0.9], [0.5]], dtype=np.float32)
        return corpus, (lambda _t: np.ones(1, dtype=np.float32))

    monkeypatch.setattr(benchmark, "_pipeline_embedder", fake_embedder)
    benchmark.cmd_pipeline(args)

    assert seen["docs"] == ["weak sense", "strong sense", "middling sense"]
    assert seen["row_ids"] == [ids[0], ids[0], ids[1]]
    assert seen["variant"].startswith("description_")
    payload = json.loads(Path(args.json_out).read_text())
    top = payload["details"][0]["top"]
    # two fragments, not three rows; the strong sense carried its fragment
    assert [hit["id"] for hit in top] == [ids[0], ids[1]]
    assert top[0]["score"] == pytest.approx(0.9, abs=1e-4)
    assert payload["doc_stage"]["rows"] == 3
    assert payload["doc_stage"]["senses"] == 3
    assert payload["doc_stage"]["descriptions_meta"]["caution_rows"] == 1
    assert "doc=description" in payload["config"]
    assert "doc-text: description" in capsys.readouterr().out


def test_gemini_never_embeds_a_doc_text_variant_by_itself(
    benchmark, corpus_dir, monkeypatch
):
    """12 000 paid calls must not happen as a side effect of a flag."""
    monkeypatch.setattr(
        benchmark, "require_api_key",
        lambda: (_ for _ in ()).throw(AssertionError("a key was requested")))
    with pytest.raises(SystemExit) as exc:
        benchmark._pipeline_embedder(
            "gemini", _metas(benchmark, 3), ["a", "b", "c"], {},
            "description_abc123def456")
    message = str(exc.value)
    assert "not cached" in message and "--embedder" in message


def test_thin_coverage_is_refused_without_the_explicit_flag(
    benchmark, monkeypatch, tmp_path
):
    """15 senses in a 4000-fragment language is the old index, relabelled."""
    args, _cache, ids = _mini_pipeline(benchmark, monkeypatch, tmp_path, "")
    path = _senses_file(tmp_path, [
        {"canonical_id": ids[0], "translation": 1, "senses": ["one sense"],
         "caution": False},
    ])
    args.doc_text = "description"
    args.descriptions_file = str(path)
    with pytest.raises(SystemExit) as exc:
        benchmark.cmd_pipeline(args)
    message = str(exc.value)
    assert "--allow-partial-coverage" in message
    assert "ru=0.500" in message

    # with the flag the run proceeds, and the share is in the label
    args.allow_partial_coverage = True
    args.json_out = str(tmp_path / "thin.json")
    benchmark.cmd_pipeline(args)
    payload = json.loads(Path(args.json_out).read_text())
    assert "doc_cov=0.500" in payload["config"]
    assert payload["doc_stage"]["coverage_by_language"]["ru"] == 0.5
    assert payload["doc_stage"]["allow_partial_coverage"] is True
