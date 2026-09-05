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
import vector_index
from vector_index import (
    IndexVersionUnavailable,
    InMemoryVectorIndex,
    MissingChunksError,
    build_embedding_text,
    current_embedding_version,
    load_index,
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


@pytest.mark.parametrize(
    ("model", "dims"),
    [("", 768), ("   ", 768), ("gemini-embedding-001", 0),
     ("gemini-embedding-001", -1)],
)
def test_embedding_version_refuses_unconfigured_model_or_dims(model, dims):
    """`c3:@0` is not a version — it addresses an index nobody ever wrote, so
    a read finds nothing and a rebuild would delete everything as stale."""
    with pytest.raises(IndexVersionUnavailable):
        current_embedding_version(model, dims, 3)


class RecordingCursor:
    """Minimal cursor double: records statements, returns no rows."""

    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append((" ".join(sql.split()), params))

    def fetchall(self):
        return []


def test_load_index_queries_a_fully_specified_version():
    """Regression guard for the keyless contract: the index version describes
    the STORED corpus, so it must not depend on GEMINI_API_KEY (config
    requires EMBEDDING_MODEL/DIMENSIONS with or without a key). When it did,
    a keyless deployment queried `c3:@0`, got an empty index and answered 503
    instead of the documented safe-pool 200 (`ai_unavailable`)."""
    cursor = RecordingCursor()

    index = load_index(cursor)

    assert len(index) == 0
    (_sql, params), = cursor.statements
    version, = params
    assert version == f"c{vector_index.CHUNKING_VERSION}:" \
        f"{vector_index.EMBEDDING_MODEL}@{vector_index.EMBEDDING_DIMENSIONS}"
    assert vector_index.EMBEDDING_MODEL.strip()
    assert vector_index.EMBEDDING_DIMENSIONS >= 1


class TwoVersionCursor:
    """A `chunk_embeddings` table holding two index versions at once.

    Answers the query the way MySQL would: only the rows whose
    `embedding_version` equals the parameter. Since ClickUp 86cbegg2r that
    table legitimately holds a 768-wide and a 1024-wide index side by side,
    so this is the read path's half of the migration invariant.
    """

    ROWS = {
        "c3:old@2": [("a", [1.0, 0.0]), ("b", [0.0, 1.0])],
        "c3:new@3": [("a", [1.0, 0.0, 0.0]), ("b", [0.0, 1.0, 0.0]),
                     ("c", [0.0, 0.0, 1.0])],
    }

    def __init__(self):
        self.rows: list[dict] = []

    def execute(self, sql, params=None):
        assert "e.embedding_version = %s" in sql, (
            "the read path must filter by index version: without it a table "
            "holding two versions returns vectors of two different widths"
        )
        version, = params
        self.rows = [
            {
                "canonical_id": cid, "translation": 1,
                "vector": pack_vector(vector),
                "alias": "syn", "language": "ru",
                "book_number": 19, "chapter_number": 23,
                "verse_number_start": 1, "verse_number_end": 6, "title": None,
            }
            for cid, vector in self.ROWS.get(version, [])
        ]

    def fetchall(self):
        return self.rows


@pytest.mark.parametrize(
    "version, expected_ids, width",
    [("c3:old@2", ["a", "b"], 2), ("c3:new@3", ["a", "b", "c"], 3)],
)
def test_load_index_reads_one_version_out_of_a_table_holding_two(
    version, expected_ids, width
):
    """Two index versions coexist during a model migration, and their
    vectors have different widths — so the loader must return exactly one
    version's rows. Mixing them would not even build a matrix (`np.vstack`
    raises), and the value that decides is the version, not the order."""
    cursor = TwoVersionCursor()

    index = load_index(cursor, version=version)

    assert [meta["canonical_id"] for meta in index.metas] == expected_ids
    assert index.vectors.shape == (len(expected_ids), width)


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


OTHER_VERSION = "c1:old-model@768"


def test_plan_reindex_fresh_index_embeds_everything():
    to_embed, to_delete = plan_reindex({"a", "b"}, set(), VERSION)
    assert to_embed == {"a", "b"}
    assert to_delete == set()


def test_plan_reindex_second_run_is_noop():
    existing = {("a", VERSION), ("b", VERSION)}
    to_embed, to_delete = plan_reindex({"a", "b"}, existing, VERSION)
    assert to_embed == set()
    assert to_delete == set()


def test_plan_reindex_missing_current_version_row_is_embedded():
    existing = {("a", OTHER_VERSION), ("b", VERSION)}
    to_embed, to_delete = plan_reindex({"a", "b"}, existing, VERSION)
    assert to_embed == {"a"}


def test_plan_reindex_keeps_other_versions_by_default():
    """The migration invariant (ClickUp 86cbegg2r): the running container is
    still reading the old index while the new one is being written, so a
    rebuild must not delete a single row of it."""
    existing = {("a", OTHER_VERSION), ("b", OTHER_VERSION)}
    to_embed, to_delete = plan_reindex({"a", "b"}, existing, VERSION)
    assert to_embed == {"a", "b"}
    assert to_delete == set()


def test_plan_reindex_drops_other_versions_only_when_asked():
    existing = {("a", OTHER_VERSION), ("a", VERSION)}
    to_embed, to_delete = plan_reindex(
        {"a"}, existing, VERSION, drop_other_versions=True
    )
    assert to_embed == set()
    # The row of the CURRENT version survives; only the other one goes, and
    # it is addressed by (id, version) — deleting by id alone would take the
    # live index with it.
    assert to_delete == {("a", OTHER_VERSION)}


def test_plan_reindex_removed_chunk_deleted():
    existing = {("a", VERSION), ("gone", VERSION)}
    to_embed, to_delete = plan_reindex({"a"}, existing, VERSION)
    assert to_embed == set()
    assert to_delete == {("gone", VERSION)}


def test_plan_reindex_removed_chunk_of_another_version_is_kept():
    """A chunk that vanished is only this index's problem: the other
    version's row belongs to a corpus that was chunked differently."""
    existing = {("gone", OTHER_VERSION)}
    _to_embed, to_delete = plan_reindex({"a"}, existing, VERSION)
    assert to_delete == set()


def test_plan_reindex_force_reembeds_all_without_duplicates():
    existing = {("a", VERSION)}
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


def test_rebuild_force_overrides_the_guard_without_touching_other_versions():
    """force says "re-embed everything of MY version", not "wipe the table":
    the stored row belongs to another index version and a container may be
    serving it right now."""
    cursor = _guard_cursor()
    connection = ScriptedConnection()
    stats = reindex_translation(connection, cursor, lambda texts: [],
                                translation_code=1, force=True)
    assert stats == {"embedded": 0, "kept": 0, "deleted": 0}
    assert not any("DELETE" in sql for sql in cursor.executed)


def test_drop_other_versions_deletes_them_naming_the_version():
    cursor = _guard_cursor()
    connection = ScriptedConnection()
    stats = reindex_translation(connection, cursor, lambda texts: [],
                                translation_code=1, force=True,
                                drop_other_versions=True)
    assert stats == {"embedded": 0, "kept": 0, "deleted": 1}
    deletes = [sql for sql in cursor.executed if "DELETE" in sql]
    assert len(deletes) == 1
    # The version is part of the predicate, so a row of the current version
    # with the same canonical id cannot be swept up with it.
    assert "embedding_version = %s" in deletes[0]
