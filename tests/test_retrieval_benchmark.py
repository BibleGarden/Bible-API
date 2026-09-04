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
import os
import sys
from pathlib import Path

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
