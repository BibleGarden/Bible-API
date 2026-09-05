"""Tests for the local-models column of evaluation/trace_picker.py (86cbegcmm).

The stand is not part of the service, but three of its properties decide
whether the comparison it draws is honest:

* the three local adapters must be DUCKS of the production clients — same
  method, same arguments, same exception — or `ScriptureRetriever` would
  take a different path for them than for Gemini and the two columns would
  no longer be the same scheme;
* their answers must be understood by the PRODUCTION parsers, not by a
  private copy that could accept what the service rejects;
* the in-memory bge-m3 index must rank exactly the way the benchmark ranks
  over the same matrix, or the column would be measuring something the
  benchmark never measured.

Nothing here needs torch, a network or the corpus files: the embedder is a
stand-in, the HTTP client is an httpx.MockTransport and the matrix is
synthetic. The module is loaded by path because `evaluation/` is deliberately
not on `pythonpath` (pytest.ini exposes `app` only).
"""

import importlib.util
import inspect
import json
import sys
from pathlib import Path

import httpx
import numpy as np
import pytest

EVALUATION = Path(__file__).resolve().parents[1] / "evaluation"
TRACE_PICKER = EVALUATION / "trace_picker.py"


def _load_trace_picker():
    spec = importlib.util.spec_from_file_location("trace_picker", TRACE_PICKER)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


tp = _load_trace_picker()


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _answer(payload: str) -> httpx.Response:
    return httpx.Response(
        200, json={"choices": [{"message": {"content": payload}}]}
    )


# ---------------------------------------------------------------------------
# The adapters are ducks of the production clients
# ---------------------------------------------------------------------------

def _signature(callable_obj) -> list[str]:
    """Parameter names without `self`."""
    return [
        name for name in inspect.signature(callable_obj).parameters
        if name != "self"
    ]


def test_rewriter_matches_the_production_rewrite_signature():
    from query_rewrite import GeminiQueryRewriter

    assert _signature(tp.QwenQueryRewriter.rewrite) == _signature(
        GeminiQueryRewriter.rewrite
    )


def test_reranker_matches_the_production_choose_signature():
    from passage_rerank import GeminiPassageReranker

    assert _signature(tp.QwenPassageReranker.choose) == _signature(
        GeminiPassageReranker.choose
    )


def test_embedder_matches_the_production_embed_query_signature():
    from embeddings import GeminiEmbeddingClient

    assert _signature(tp.LocalEmbedder.embed_query) == _signature(
        GeminiEmbeddingClient.embed_query
    )


def test_embedder_raises_the_production_error_the_pipeline_catches():
    from embeddings import EmbeddingUnavailable

    class Broken:
        def encode(self, *args, **kwargs):
            raise RuntimeError("no weights")

    embedder = tp.LocalEmbedder(Broken(), "")
    with pytest.raises(EmbeddingUnavailable) as caught:
        embedder.embed_query("что-нибудь")
    # provider_down is what makes retrieval stop embedding the remaining
    # variants instead of failing six times over.
    assert caught.value.provider_down is True
    assert "no weights" not in str(caught.value)   # category only


def test_embedder_returns_a_unit_float32_vector_with_the_model_prefix():
    seen = {}

    class Model:
        def encode(self, texts, **kwargs):
            seen["texts"] = list(texts)
            seen["kwargs"] = kwargs
            return np.array([[0.6, 0.8]], dtype=np.float64)

    vector = tp.LocalEmbedder(Model(), "query: ").embed_query("тревога")
    assert seen["texts"] == ["query: тревога"]
    assert seen["kwargs"]["normalize_embeddings"] is True
    assert vector.dtype == np.float32


# ---------------------------------------------------------------------------
# Qwen answers go through the production parsers
# ---------------------------------------------------------------------------

@pytest.fixture
def local_endpoints(monkeypatch):
    monkeypatch.setattr(tp, "LOCAL_REWRITE_ENDPOINT", "https://llm.example/v1")
    monkeypatch.setattr(tp, "LOCAL_REWRITE_MODEL", "qwen3-30b-a3b-instruct-2507")
    monkeypatch.setattr(tp, "LOCAL_REWRITE_API_KEY", "secret")
    monkeypatch.setattr(tp, "LOCAL_RERANK_ENDPOINT", "https://llm.example/v1")
    monkeypatch.setattr(tp, "LOCAL_RERANK_MODEL", "qwen3-30b-a3b-instruct-2507")
    monkeypatch.setattr(tp, "LOCAL_RERANK_API_KEY", "secret")


def test_rewrite_parses_prompt_8c_objects_and_drops_the_references(
    local_endpoints,
):
    """Prompt 8c answers `{"ref": ..., "query": ...}`; only queries survive."""
    body = json.dumps({"queries": [
        {"ref": "Псалом 22:1", "query": "Господь Пастырь мой, я ни в чём не буду нуждаться"},
        {"ref": "Исаия 41:10", "query": "Не бойся, ибо Я с тобою, Я укреплю тебя"},
    ]}, ensure_ascii=False)
    sent = {}

    def handler(request):
        sent["url"] = str(request.url)
        sent["json"] = json.loads(request.content)
        sent["auth"] = request.headers.get("Authorization")
        return _answer(body)

    rewriter = tp.QwenQueryRewriter(_client(handler), variants=6)
    queries = rewriter.rewrite("ru", "тревога", [])

    assert queries == [
        "Господь Пастырь мой, я ни в чём не буду нуждаться",
        "Не бойся, ибо Я с тобою, Я укреплю тебя",
    ]
    assert sent["url"] == "https://llm.example/v1/chat/completions"
    assert sent["auth"] == "Bearer secret"
    assert sent["json"]["temperature"] == 0.0
    assert sent["json"]["response_format"] == {"type": "json_object"}
    # The instruction is prompt 8c built from the production v7 text, not a
    # copy living in this file.
    from rewrite_prompts import build_instruction

    assert sent["json"]["messages"][0]["content"] == build_instruction(
        "8c", "ru", 6
    )


def test_rewrite_strips_a_reasoning_block_before_parsing(local_endpoints):
    body = (
        "<think>Надо подобрать шесть.</think>"
        + json.dumps({"queries": [{"ref": "", "query": "Бог наш прибежище и сила"}]},
                     ensure_ascii=False)
    )
    rewriter = tp.QwenQueryRewriter(_client(lambda r: _answer(body)))
    assert rewriter.rewrite("ru", "страх", []) == ["Бог наш прибежище и сила"]


def test_rewrite_junk_raises_the_error_retrieval_degrades_on(local_endpoints):
    from query_rewrite import QueryRewriteError

    rewriter = tp.QwenQueryRewriter(_client(lambda r: _answer("совсем не JSON")))
    with pytest.raises(QueryRewriteError):
        rewriter.rewrite("ru", "страх", [])


def test_rewrite_without_configuration_names_the_variables(monkeypatch):
    from query_rewrite import QueryRewriteError

    monkeypatch.setattr(tp, "LOCAL_REWRITE_ENDPOINT", "")
    rewriter = tp.QwenQueryRewriter(_client(lambda r: _answer("{}")))
    with pytest.raises(QueryRewriteError, match="TRACE_LOCAL_REWRITE_ENDPOINT"):
        rewriter.rewrite("ru", "страх", [])


def test_rerank_parses_the_choice_with_the_production_parser(local_endpoints):
    body = json.dumps({
        "candidate": 3, "key_verse_start": 2, "key_verse_end": 3,
        "reason": "Speaks to exhaustion and fear of failing.",
    })
    sent = {}

    def handler(request):
        sent["json"] = json.loads(request.content)
        return _answer(body)

    reranker = tp.QwenPassageReranker(_client(handler))
    choice = reranker.choose("устала", [], ["a", "b", "c", "d"])

    assert (choice.index, choice.key_verse_start, choice.key_verse_end) == (2, 2, 3)
    assert choice.reason == "Speaks to exhaustion and fear of failing."
    # The instruction is the production v9 prompt, imported, for the real
    # candidate count.
    from passage_rerank import build_rerank_instruction

    assert sent["json"]["messages"][0]["content"] == build_rerank_instruction(
        4, True
    )


def test_rerank_out_of_range_answer_is_refused(local_endpoints):
    from passage_rerank import PassageRerankError

    body = json.dumps({"candidate": 9, "reason": "x"})
    reranker = tp.QwenPassageReranker(_client(lambda r: _answer(body)))
    with pytest.raises(PassageRerankError):
        reranker.choose("устала", [], ["a", "b"])


def test_rerank_empty_content_is_refused(local_endpoints):
    from passage_rerank import PassageRerankError

    reranker = tp.QwenPassageReranker(_client(lambda r: _answer("   ")))
    with pytest.raises(PassageRerankError):
        reranker.choose("устала", [], ["a", "b"])


# ---------------------------------------------------------------------------
# The in-memory index ranks the way the benchmark ranks
# ---------------------------------------------------------------------------

def _synthetic_corpus(rows: int = 60, dims: int = 8, seed: int = 20260905):
    """A matrix plus the two readings of its metadata the stand keeps aligned."""
    rng = np.random.default_rng(seed)
    matrix = rng.normal(size=(rows, dims)).astype(np.float32)
    matrix /= np.linalg.norm(matrix, axis=1, keepdims=True)
    metas = []
    for row in range(rows):
        # Two languages, so the language filter is actually exercised.
        russian = row % 3 != 0
        metas.append({
            "canonical_id": f"v3:{row // 10 + 1:02d}.{row:03d}.001-003",
            "translation": 1 if russian else 16,
            "alias": "syn" if russian else "bsb",
            "language": "ru" if russian else "en",
            "book_number": row // 10 + 1,
            "chapter_number": row,
            "verse_number_start": 1,
            "verse_number_end": 3,
            "title": None,
        })
    return matrix, metas


def _benchmark_semantic(matrix, metas, query, language, fetch_k):
    """`retrieval_benchmark.cmd_pipeline.search_variant`, semantic half.

    Re-stated here rather than imported because it is a closure inside the
    `pipeline` command. It is the reference ranking this stand must match:
    rows of the language, cosine, best score first, de-duplicated by
    fragment, cut at fetch_k.
    """
    rows = np.array(
        [r for r, m in enumerate(metas) if m["language"] == language], dtype=int
    )
    sims = matrix[rows] @ query
    hits, seen = [], set()
    for j in np.argsort(-sims):
        canonical_id = metas[rows[j]]["canonical_id"]
        if canonical_id in seen:
            continue
        seen.add(canonical_id)
        hits.append((canonical_id, float(sims[j])))
        if len(hits) >= fetch_k:
            break
    return hits


@pytest.mark.parametrize("fetch_k", [1, 10, 50])
def test_index_top_matches_the_benchmark_ranking(fetch_k):
    from vector_index import InMemoryVectorIndex

    matrix, metas = _synthetic_corpus()
    index = InMemoryVectorIndex(matrix, metas)
    rng = np.random.default_rng(7)
    for _ in range(20):
        query = rng.normal(size=matrix.shape[1]).astype(np.float32)
        query /= np.linalg.norm(query)
        ours = [
            (hit.canonical_id, hit.score)
            for hit in index.search(query, top_k=fetch_k, language="ru")
        ]
        theirs = _benchmark_semantic(matrix, metas, query, "ru", fetch_k)
        assert [cid for cid, _ in ours] == [cid for cid, _ in theirs]
        assert ours == pytest.approx(theirs, abs=1e-6)


def test_load_local_index_refuses_metadata_that_disagrees(tmp_path, monkeypatch):
    """The two readings of chunks.jsonl must describe the same rows.

    Guards the failure class of 86cbe4n7e: a matrix whose rows no longer
    belong to the metadata they are scored against returns cosine numbers for
    the WRONG passages, and numpy does not mind.
    """
    import retrieval_benchmark as rb

    chunks = tmp_path / "chunks.jsonl"
    rows = [
        {
            "canonical_id": f"v3:01.00{i}.001-002", "translation": 1,
            "alias": "syn", "language": "ru", "book_number": 1,
            "chapter_number": i, "verse_number_start": 1,
            "verse_number_end": 2, "title": None,
        }
        for i in (1, 2, 3)
    ]
    chunks.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(rb, "CHUNKS_FILE", chunks)
    # The benchmark's own reading claims a DIFFERENT second fragment.
    shifted = [
        rb.ChunkMeta(row["canonical_id"], row["translation"], 1, i, 1, 2)
        for i, row in enumerate(rows)
    ]
    shifted[1] = rb.ChunkMeta("v3:01.099.001-002", 1, 1, 99, 1, 2)
    monkeypatch.setattr(rb, "load_chunks", lambda: (shifted, [], []))
    monkeypatch.setattr(
        rb, "load_corpus_matrix",
        lambda *a, **k: np.eye(3, dtype=np.float32),
    )
    with pytest.raises(RuntimeError, match="disagreed at rows"):
        tp.load_local_index()


# ---------------------------------------------------------------------------
# Tripwire
# ---------------------------------------------------------------------------

def test_transport_refuses_a_host_outside_the_allowlist():
    transport = tp.LocalOnlyTransport({"llm.ai2.ru"})
    request = httpx.Request(
        "POST", "https://generativelanguage.googleapis.com/v1beta/x"
    )
    with pytest.raises(tp.TripwireError, match="refused"):
        transport.handle_request(request)


@pytest.mark.parametrize("host", ["llm.ai2.ru", "127.0.0.1", "localhost",
                                  "172.18.0.5", "::1"])
def test_allowed_hosts_of_the_local_stack(host):
    assert tp._host_allowed(host, {"llm.ai2.ru"}) is True


@pytest.mark.parametrize("host", ["generativelanguage.googleapis.com",
                                  "8.8.8.8", "example.com", ""])
def test_refused_hosts_of_the_local_stack(host):
    assert tp._host_allowed(host, {"llm.ai2.ru"}) is False


def test_a_gemini_client_in_the_local_column_is_refused():
    from embeddings import GeminiEmbeddingClient

    class Stand:
        stack = "local-prod"
        rewriter = object()
        embedder = GeminiEmbeddingClient()
        reranker = object()

    with pytest.raises(tp.TripwireError, match="Gemini embedder"):
        tp.assert_no_gemini_clients(Stand())


def test_the_local_adapters_pass_the_structural_tripwire():
    class Stand:
        stack = "local-prod"
        rewriter = tp.QwenQueryRewriter(_client(lambda r: _answer("{}")))
        embedder = tp.LocalEmbedder(object(), "")
        reranker = tp.QwenPassageReranker(_client(lambda r: _answer("{}")))

    tp.assert_no_gemini_clients(Stand())    # does not raise


# ---------------------------------------------------------------------------
# The right-column switch
# ---------------------------------------------------------------------------

def test_unknown_right_column_falls_back_loudly():
    column, note = tp.pick_right("нет-такой")
    assert column == tp.DEFAULT_RIGHT
    assert "replies" in note          # the renamed replies parameter is named


@pytest.mark.parametrize("value,expected", [
    ("", "prod-local"), ("prod-local", "prod-local"), ("senses", "senses"),
])
def test_known_right_columns(value, expected):
    assert tp.pick_right(value) == (expected, "")
