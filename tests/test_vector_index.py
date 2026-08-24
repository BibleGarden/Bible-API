"""Unit tests for the scripture-selection vector index and embedding client.

Pure logic only: no database and no network. The Gemini client is tested
through an httpx.MockTransport.
"""

import json
import math

import httpx
import numpy as np
import pytest

from embeddings import (
    EmbeddingConfig,
    EmbeddingUnavailable,
    GeminiEmbeddingClient,
    normalize,
)
from vector_index import (
    InMemoryVectorIndex,
    MissingChunksError,
    build_embedding_text,
    current_embedding_version,
    pack_vector,
    plan_reindex,
    reindex_translation,
    unpack_vector,
)


# ---------------------------------------------------------------------------
# Versioning / text building / packing
# ---------------------------------------------------------------------------

def test_embedding_version_contains_all_parts():
    version = current_embedding_version("gemini-embedding-001", 768, 1)
    assert version == "c1:gemini-embedding-001@768"


def test_embedding_version_changes_with_each_component():
    base = current_embedding_version("m", 768, 1)
    assert current_embedding_version("other", 768, 1) != base
    assert current_embedding_version("m", 512, 1) != base
    assert current_embedding_version("m", 768, 2) != base


def test_build_embedding_text_includes_title():
    assert build_embedding_text("Заголовок", "Текст.") == "Заголовок\n\nТекст."


def test_build_embedding_text_without_title():
    assert build_embedding_text(None, "Текст.") == "Текст."
    assert build_embedding_text("   ", "Текст.") == "Текст."


def test_pack_unpack_roundtrip():
    vector = [0.25, -1.5, 3.0, 0.0]
    unpacked = unpack_vector(pack_vector(vector))
    assert unpacked.dtype == np.float32
    assert np.allclose(unpacked, vector)


def test_normalize_returns_unit_vector():
    result = normalize([3.0, 4.0])
    assert math.isclose(sum(x * x for x in result), 1.0, rel_tol=1e-6)
    assert normalize([0.0, 0.0]) == [0.0, 0.0]


# ---------------------------------------------------------------------------
# Reindex planning (idempotency, duplicates, stale rows)
# ---------------------------------------------------------------------------

VERSION = "c1:model@768"


def test_plan_reindex_fresh_index_embeds_everything():
    to_embed, to_delete = plan_reindex({"a", "b"}, {}, VERSION)
    assert to_embed == {"a", "b"}
    assert to_delete == set()


def test_plan_reindex_second_run_is_noop():
    existing = {"a": VERSION, "b": VERSION}
    to_embed, to_delete = plan_reindex({"a", "b"}, existing, VERSION)
    assert to_embed == set()
    assert to_delete == set()


def test_plan_reindex_stale_version_replaced():
    existing = {"a": "c1:old-model@768", "b": VERSION}
    to_embed, to_delete = plan_reindex({"a", "b"}, existing, VERSION)
    assert to_embed == {"a"}
    assert to_delete == {"a"}


def test_plan_reindex_removed_chunk_deleted():
    existing = {"a": VERSION, "gone": VERSION}
    to_embed, to_delete = plan_reindex({"a"}, existing, VERSION)
    assert to_embed == set()
    assert to_delete == {"gone"}


def test_plan_reindex_force_reembeds_all_without_duplicates():
    existing = {"a": VERSION}
    to_embed, to_delete = plan_reindex({"a", "b"}, existing, VERSION, force=True)
    assert to_embed == {"a", "b"}
    assert to_delete == set()  # current-version rows are replaced by upsert


# ---------------------------------------------------------------------------
# In-memory cosine search with filters
# ---------------------------------------------------------------------------

def make_index():
    def meta(cid, translation, language):
        return {
            "canonical_id": cid,
            "translation": translation,
            "alias": {1: "syn", 16: "bsb"}[translation],
            "language": language,
            "book_number": 19,
            "chapter_number": 22,
            "verse_number_start": 1,
            "verse_number_end": 6,
            "title": None,
        }

    vectors = np.array(
        [
            [1.0, 0.0, 0.0],
            [0.8, 0.6, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
        ],
        dtype=np.float32,
    )
    metas = [
        meta("v1:19.022.001-006", 1, "ru"),
        meta("v1:19.023.001-010", 1, "ru"),
        meta("v1:19.022.001-011", 16, "en"),
        meta("v1:19.024.001-010", 16, "en"),
    ]
    return InMemoryVectorIndex(vectors, metas)


def test_search_orders_by_cosine_similarity():
    index = make_index()
    hits = index.search([1.0, 0.0, 0.0], top_k=4)
    assert [h.canonical_id for h in hits][:2] == [
        "v1:19.022.001-006",
        "v1:19.023.001-010",
    ]
    assert hits[0].score == pytest.approx(1.0)
    assert hits[0].score >= hits[1].score >= hits[2].score


def test_search_filters_by_translation():
    index = make_index()
    hits = index.search([1.0, 0.0, 0.0], top_k=10, translation=16)
    assert {h.translation for h in hits} == {16}
    assert len(hits) == 2


def test_search_filters_by_language():
    index = make_index()
    hits = index.search([0.0, 1.0, 0.0], top_k=10, language="en")
    assert {h.language for h in hits} == {"en"}
    assert hits[0].canonical_id == "v1:19.022.001-011"


def test_search_top_k_limits_results():
    index = make_index()
    assert len(index.search([1.0, 0.0, 0.0], top_k=2)) == 2


def test_search_empty_filter_returns_nothing():
    index = make_index()
    assert index.search([1.0, 0.0, 0.0], translation=999) == []


def test_search_empty_index():
    index = InMemoryVectorIndex(np.empty((0, 3), np.float32), [])
    assert index.search([1.0, 0.0, 0.0]) == []


def test_index_rejects_mismatched_lengths():
    with pytest.raises(ValueError):
        InMemoryVectorIndex(np.ones((2, 3), np.float32), [{}])


# ---------------------------------------------------------------------------
# Gemini embedding client (httpx.MockTransport, no network)
# ---------------------------------------------------------------------------

def make_client(handler, dims=4, sleeps=None):
    transport = httpx.MockTransport(handler)
    return GeminiEmbeddingClient(
        config=EmbeddingConfig(model="gemini-embedding-001", dimensions=dims,
                               api_key="test-key"),
        http_client=httpx.Client(transport=transport),
        sleep=(sleeps.append if sleeps is not None else (lambda _s: None)),
    )


def embedding_response(dims=4):
    return httpx.Response(
        200, json={"embedding": {"values": [2.0] + [0.0] * (dims - 1)}}
    )


def test_embed_documents_one_call_per_text_and_normalizes():
    captured = []

    def handler(request):
        captured.append(json.loads(request.content))
        return embedding_response()

    client = make_client(handler)
    vectors = client.embed_documents([f"text {i}" for i in range(7)])
    assert len(captured) == 7
    assert captured[0]["taskType"] == "RETRIEVAL_DOCUMENT"
    assert captured[0]["outputDimensionality"] == 4
    assert captured[3]["content"]["parts"][0]["text"] == "text 3"
    assert len(vectors) == 7
    assert vectors[0] == pytest.approx([1.0, 0.0, 0.0, 0.0])


def test_embed_query_uses_query_task_type():
    captured = []

    def handler(request):
        captured.append(json.loads(request.content))
        return embedding_response()

    vector = make_client(handler).embed_query("помощь в тревоге")
    assert captured[0]["taskType"] == "RETRIEVAL_QUERY"
    assert len(vector) == 4


def test_empty_text_is_replaced_with_space():
    captured = []

    def handler(request):
        captured.append(json.loads(request.content))
        return embedding_response()

    make_client(handler).embed_documents(["", "  "])
    texts = [c["content"]["parts"][0]["text"] for c in captured]
    assert texts == [" ", " "]


def test_retry_on_429_uses_server_retry_delay():
    calls = {"n": 0}
    sleeps = []

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(
                429,
                json={"error": {"details": [
                    {"@type": "type.googleapis.com/google.rpc.RetryInfo",
                     "retryDelay": "7s"}
                ]}},
            )
        return embedding_response()

    vector = make_client(handler, sleeps=sleeps).embed_query("q")
    assert calls["n"] == 2
    assert sleeps == [7.0]
    assert len(vector) == 4


def test_non_retryable_error_raises_immediately():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(400, json={"error": "bad request"})

    with pytest.raises(EmbeddingUnavailable):
        make_client(handler).embed_query("q")
    assert calls["n"] == 1


def test_persistent_failure_raises_after_retries():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(503, json={"error": "unavailable"})

    with pytest.raises(EmbeddingUnavailable):
        make_client(handler).embed_query("q")
    assert calls["n"] == 6  # _MAX_RETRIES


def test_missing_api_key_raises():
    client = GeminiEmbeddingClient(
        config=EmbeddingConfig(model="m", dimensions=4, api_key=""),
        http_client=httpx.Client(
            transport=httpx.MockTransport(lambda r: embedding_response())
        ),
    )
    with pytest.raises(EmbeddingUnavailable):
        client.embed_query("q")


def test_wrong_dimension_count_raises():
    def handler(request):
        return embedding_response(dims=8)  # config expects 4

    with pytest.raises(EmbeddingUnavailable):
        make_client(handler).embed_query("q")


# ---------------------------------------------------------------------------
# Rebuild guard (no current-version chunks must not wipe the index)
# ---------------------------------------------------------------------------

class ScriptedCursor:
    """Returns queued fetchall() results; records executed SQL."""

    def __init__(self, fetchall_results):
        self.results = list(fetchall_results)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def executemany(self, sql, rows):
        self.executed.append(sql)

    def fetchall(self):
        return self.results.pop(0)


class ScriptedConnection:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


def _guard_cursor():
    return ScriptedCursor([
        [],  # chunks of the current CHUNKING_VERSION
        [{"canonical_id": "v2:01.001.001-005",
          "embedding_version": "c2:model@4"}],  # stored embeddings
    ])


def test_rebuild_refused_without_current_version_chunks():
    cursor = _guard_cursor()
    with pytest.raises(MissingChunksError, match="rechunk"):
        reindex_translation(ScriptedConnection(), cursor,
                            lambda texts: [], translation_code=1)
    # nothing was deleted before the refusal
    assert not any("DELETE" in sql for sql in cursor.executed)


def test_rebuild_force_overrides_the_guard():
    cursor = _guard_cursor()
    connection = ScriptedConnection()
    stats = reindex_translation(connection, cursor, lambda texts: [],
                                translation_code=1, force=True)
    assert stats == {"embedded": 0, "kept": 0, "deleted": 1}
    assert any("DELETE" in sql for sql in cursor.executed)
