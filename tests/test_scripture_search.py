"""Offline API and pipeline tests for general Bible search."""

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import numpy as np
import pytest
import httpx
from fastapi.testclient import TestClient

import passage_render
import scripture_search as search
import scripture_select
from deadline import Deadline
from config import StageProvider
from embeddings import EmbeddingUnavailable
from lexical_index import LexicalIndex
from main import app
from query_rewrite import QueryRewriteError, build_query_rewriter
from retrieval import VerseText, is_blacklisted, load_genre_blacklist
from scripture_select import CorpusResources
from vector_index import InMemoryVectorIndex
from versification import PsalmMap, build_psalm_map, canonical_counts_with_extras

client = TestClient(app)
GENEALOGY = "v3:13.001.001-004"
PSALM = "v3:19.023.001-003"
QUERY = "private search wording"
BODY = {"query": QUERY, "language": "ru"}


def post(body=None, key="test-api-key", **kwargs):
    headers = {"X-API-Key": key} if key else {}
    return client.post(
        "/api/ai/scripture/search",
        json=BODY if body is None else body,
        headers=headers,
        **kwargs,
    )


@pytest.fixture(autouse=True)
def environment(monkeypatch, request_log):
    maxima = {
        alias: {int(ch): n for ch, n in chapters.items()}
        for alias, chapters in json.loads(
            (Path(__file__).parent / "data/psalm_max_verses.json").read_text()
        ).items()
    }
    counts = canonical_counts_with_extras(maxima["bsb"])
    maps = {
        code: PsalmMap(build_psalm_map(alias, maxima[alias], counts))
        for code, alias in ((1, "syn"), (16, "bsb"), (20, "ubh"))
    }
    maps[2] = maps[1]
    metas = [
        dict(
            canonical_id=cid,
            language=language,
            translation=code,
            alias=alias,
            title=None,
            book_number=book,
            chapter_number=chapter,
            verse_number_start=1,
            verse_number_end=end,
        )
        for language, code, alias in (
            ("ru", 1, "syn"),
            ("en", 16, "bsb"),
            ("uk", 20, "ubh"),
        )
        for cid, book, chapter, end in ((GENEALOGY, 13, 1, 4), (PSALM, 19, 23, 3))
    ]
    resources = CorpusResources(
        index=InMemoryVectorIndex(np.array([[1.0, 0.0], [0.0, 1.0]] * 3), metas),
        lexical={
            lang: LexicalIndex([(GENEALOGY, "Adam Seth"), (PSALM, "shepherd")])
            for lang in ("ru", "en", "uk")
        },
        translations={
            "ru": [(1, "syn"), (2, "rst")],
            "en": [(16, "bsb")],
            "uk": [(20, "ubh")],
        },
        primary={"ru": 1, "en": 16, "uk": 20},
        coverage={2: frozenset({PSALM})},
        loaded_at=0,
        psalm_maps=maps,
    )
    rewrite, ranker, embedder = MagicMock(), MagicMock(), MagicMock()
    for provider in (rewrite, ranker, embedder):
        provider.__enter__.return_value = provider
    rewrite.complete.return_value = json.dumps(
        {"queries": ["Adam", "Seth", "genealogy"]}
    )
    ranker.complete.return_value = '{"indices": [1, 0]}'
    embedder.embed_query.return_value = [1.0, 0.0]
    monkeypatch.setattr(search, "get_resources", Mock(return_value=resources))
    factory = Mock(
        side_effect=lambda **kw: (
            rewrite if kw["stage"] is search.SCRIPTURE_REWRITE_PROVIDER else ranker
        )
    )
    monkeypatch.setattr(search, "build_query_rewriter", factory)
    monkeypatch.setattr(search, "build_embedding_client", Mock(return_value=embedder))
    connection = MagicMock()
    monkeypatch.setattr(search, "create_connection", Mock(return_value=connection))
    monkeypatch.setattr(
        search,
        "make_db_passage_loader",
        lambda cursor: (
            lambda code, ids: {cid: {"text": "verified indexed passage"} for cid in ids}
        ),
    )
    monkeypatch.setattr(passage_render, "title_verses_of_range", lambda *a: set())
    monkeypatch.setattr(passage_render, "load_section_title", lambda *a: None)
    verse_loader = Mock(
        side_effect=lambda cursor, code, book, chapter, first, last, titles: [
            VerseText(n, f"translation {code} verse {n}")
            for n in range(first, last + 1)
        ]
    )
    monkeypatch.setattr(passage_render, "load_range_verses", verse_loader)
    monkeypatch.setattr(search, "AI_ENABLED", True)
    monkeypatch.setattr(search, "AI_SCRIPTURE_REQUESTS_PER_MINUTE", 100)
    monkeypatch.setattr(search, "AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE", 100)
    search._limiter.reset()
    yield SimpleNamespace(
        resources=resources,
        rewrite=rewrite,
        ranker=ranker,
        embedder=embedder,
        factory=factory,
        connection=connection,
        verse_loader=verse_loader,
    )
    search._limiter.reset()


def test_returns_grounded_ranked_passages_and_preserves_genealogies(environment):
    assert is_blacklisted(load_genre_blacklist(), 13, 1, 1, 4)
    response = post()
    assert response.status_code == 200
    body = response.json()
    assert body["translation"] == 1
    assert [r["canonical"]["canonical_id"] for r in body["results"]] == [
        PSALM,
        GENEALOGY,
    ]
    psalm = body["results"][0]
    assert psalm["canonical"]["chapter_number"] == 23
    assert psalm["passage"]["chapter_number"] == 22
    assert (
        psalm["passage"]["text"]
        == "translation 1 verse 1 translation 1 verse 2 translation 1 verse 3"
    )
    assert [v["number"] for v in psalm["passage"]["verses"]] == [1, 2, 3]
    assert environment.embedder.embed_query.call_args_list[0].args == (QUERY,)
    environment.connection.close.assert_called_once()


def test_pipeline_uses_strict_resources(environment):
    result = search._run_search(search.SearchRequest(**BODY), Deadline(5))
    assert len(result.results) == 2
    search.get_resources.assert_called_once_with(allow_stale=False)


def test_limit_and_requested_nonindexed_translation(environment):
    environment.ranker.complete.return_value = '{"indices": [0]}'
    response = post({**BODY, "limit": 1, "translation": 2})
    assert response.status_code == 200
    results = response.json()["results"]
    assert len(results) == 1
    assert results[0]["canonical"]["canonical_id"] == PSALM
    assert results[0]["passage"]["translation"] == 2
    assert results[0]["passage"]["translation_alias"] == "rst"
    assert results[0]["passage"]["chapter_number"] == 22
    assert "translation 2" in results[0]["passage"]["text"]


@pytest.mark.parametrize(
    "language,translation,chapter,first", [("en", 16, 23, 1), ("uk", 20, 23, 1)]
)
def test_language_and_psalm_coordinates(
    environment, language, translation, chapter, first
):
    response = post({**BODY, "language": language})
    assert response.status_code == 200
    passage = response.json()["results"][0]["passage"]
    assert passage["translation"] == translation
    assert passage["chapter_number"] == chapter
    assert passage["verse_start"] == first
    assert passage["verse_end"] == first + 2
    assert [v["number"] for v in passage["verses"]] == list(range(first, first + 3))


@pytest.mark.parametrize(
    "payload",
    [
        {**BODY, "query": "  "},
        {**BODY, "query": "x" * 501},
        {**BODY, "language": "de"},
        {**BODY, "translation": 16},
        {**BODY, "translation": 999},
        {**BODY, "limit": 0},
        {**BODY, "limit": 11},
        {**BODY, "limit": True},
        {**BODY, QUERY: QUERY},
    ],
)
def test_invalid_requests_are_sanitized(payload):
    response = post(payload)
    assert response.status_code == 422
    assert QUERY not in response.text


@pytest.mark.parametrize(
    "key",
    [
        None,
        "wrong",
        "lampada-test-key-12345678901234567890",
        "ops-test-key-1234567890123456789012",
        "site-test-key-12345678901234567890123",
    ],
)
def test_requires_bible_garden_identity(environment, key):
    assert post(key=key).status_code == 403
    search.get_resources.assert_not_called()


def test_empty_ranking_is_a_success(environment):
    environment.ranker.complete.return_value = '{"indices": []}'
    assert post().json() == {"language": "ru", "translation": 1, "results": []}


def test_no_covered_candidates_does_not_call_ranker(environment):
    environment.resources.coverage[2] = frozenset()
    response = post({**BODY, "translation": 2})
    assert response.status_code == 200
    assert response.json()["results"] == []
    environment.ranker.complete.assert_not_called()


@pytest.mark.parametrize(
    "stage",
    [
        "resources",
        "rewrite",
        "embedding",
        "index",
        "lexical",
        "database",
        "render",
        "rank",
    ],
)
def test_stage_failures_are_explicit_and_private(
    environment, monkeypatch, caplog, stage
):
    error = RuntimeError(QUERY)
    if stage == "resources":
        search.get_resources.side_effect = error
    elif stage == "rewrite":
        environment.rewrite.complete.side_effect = QueryRewriteError(QUERY)
    elif stage == "embedding":
        # A partial failure must fail the entire search as well.
        environment.embedder.embed_query.side_effect = [
            [1.0, 0.0],
            EmbeddingUnavailable(QUERY),
        ]
    elif stage == "index":
        monkeypatch.setattr(
            environment.resources.index, "search", Mock(side_effect=error)
        )
    elif stage == "lexical":
        monkeypatch.setattr(
            environment.resources.lexical["ru"], "search", Mock(side_effect=error)
        )
    elif stage == "database":
        search.create_connection.return_value = None
    elif stage == "render":
        environment.verse_loader.side_effect = error
    elif stage == "rank":
        environment.ranker.complete.side_effect = QueryRewriteError(QUERY)
    response = post()
    assert response.status_code == 503
    assert QUERY not in response.text + caplog.text


@pytest.mark.parametrize(
    "answer",
    ["not JSON", '{"indices": [200]}', '{"indices": [0, 0]}', '{"indices": [true]}'],
)
def test_ranking_cannot_invent_or_duplicate_results(environment, answer):
    environment.ranker.complete.return_value = answer
    assert post().status_code == 503


def test_malformed_rewrites_are_not_replaced_by_raw_search(environment):
    environment.rewrite.complete.return_value = '{"queries": []}'
    assert post().status_code == 503
    environment.embedder.embed_query.assert_not_called()


def test_deduplicates_index_hits_across_variants(environment):
    environment.ranker.complete.return_value = '{"indices": [0, 1]}'
    result = post().json()["results"]
    assert [r["canonical"]["canonical_id"] for r in result] == [GENEALOGY, PSALM]


def test_rate_limit_has_retry_after_before_loading_resources(environment, monkeypatch):
    monkeypatch.setattr(search, "AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE", 0)
    response = post()
    assert response.status_code == 429
    assert int(response.headers["Retry-After"]) > 0
    search.get_resources.assert_not_called()


def test_limiter_failure_is_not_a_rate_limit(environment, monkeypatch):
    monkeypatch.setattr(
        search._limiter, "_pseudonymize", Mock(side_effect=RuntimeError(QUERY))
    )
    assert post().status_code == 503
    search.get_resources.assert_not_called()


def test_disabled_search_does_not_use_safe_pool(environment, monkeypatch):
    monkeypatch.setattr(search, "AI_ENABLED", False)
    assert post().status_code == 503
    search.get_resources.assert_not_called()


def test_total_timeout_cancels_waiting_for_worker(environment, monkeypatch):
    async def blocked(*args):
        await asyncio.Future()

    monkeypatch.setattr(search, "run_in_threadpool", blocked)
    monkeypatch.setattr(search, "AI_SCRIPTURE_TIMEOUT_SECONDS", 0.01)
    assert post().status_code == 504


def test_total_timeout_returns_before_blocking_work_finishes(monkeypatch):
    release = threading.Event()

    def blocked(*args):
        release.wait(timeout=2)

    monkeypatch.setattr(search, "_run_search", blocked)
    monkeypatch.setattr(search, "AI_SCRIPTURE_TIMEOUT_SECONDS", 0.01)
    with ThreadPoolExecutor(max_workers=1) as pool:
        response = pool.submit(post)
        try:
            assert response.result(timeout=1).status_code == 504
        finally:
            release.set()


def test_expired_deadline_never_loads_resources():
    with pytest.raises(TimeoutError):
        search._run_search(search.SearchRequest(**BODY), Deadline(0))
    search.get_resources.assert_not_called()


def test_search_refuses_stale_corpus_on_refresh_failure(environment, monkeypatch):
    monkeypatch.setattr(scripture_select, "_resources", environment.resources)
    monkeypatch.setattr(scripture_select, "AI_SCRIPTURE_INDEX_CACHE_SECONDS", 0)
    monkeypatch.setattr(
        scripture_select, "_load_resources", Mock(side_effect=RuntimeError("DB down"))
    )
    with pytest.raises(RuntimeError, match="DB down"):
        scripture_select.get_resources(allow_stale=False)


@pytest.mark.parametrize("warm_cache", [True, False])
def test_search_refuses_corpus_with_failed_metadata(
    environment, monkeypatch, warm_cache
):
    environment.resources.metadata_available = False
    environment.resources.loaded_at = scripture_select.time.monotonic()
    monkeypatch.setattr(
        scripture_select, "_resources", environment.resources if warm_cache else None
    )
    monkeypatch.setattr(
        scripture_select, "_load_resources", Mock(return_value=environment.resources)
    )
    with pytest.raises(scripture_select.ScriptureSelectUnavailable, match="metadata"):
        scripture_select.get_resources(allow_stale=False)


@pytest.mark.parametrize("stage", ["psalm_maps", "catalogue"])
def test_loader_records_metadata_failures(environment, monkeypatch, stage):
    monkeypatch.setattr(
        scripture_select, "create_connection", Mock(return_value=environment.connection)
    )
    monkeypatch.setattr(
        scripture_select, "load_index", Mock(return_value=environment.resources.index)
    )
    monkeypatch.setattr(
        scripture_select,
        "load_lexical_indexes",
        Mock(return_value=environment.resources.lexical),
    )
    monkeypatch.setattr(
        scripture_select,
        "load_psalm_maps",
        Mock(return_value=environment.resources.psalm_maps),
    )
    monkeypatch.setattr(
        scripture_select,
        "_build_catalogue",
        Mock(
            return_value=(
                environment.resources.translations,
                environment.resources.coverage,
            )
        ),
    )
    failed = (
        scripture_select.load_psalm_maps
        if stage == "psalm_maps"
        else scripture_select._build_catalogue
    )
    failed.side_effect = RuntimeError("metadata unavailable")
    resources = scripture_select._load_resources()
    assert resources.metadata_available is False


def test_unknown_identifier_fields_are_not_echoed():
    response = post({**BODY, "privateQuery": "private text"})
    assert response.status_code == 422
    assert "privateQuery" not in response.text


def test_empty_lexical_index_is_an_error(environment):
    environment.resources.lexical["ru"] = LexicalIndex([])
    assert post().status_code == 503


def test_request_stats_do_not_include_search_text_or_user_agent(request_log):
    response = post()
    assert response.status_code == 200
    args = request_log.call_args.args
    assert args[0] == "/api/ai/scripture/search"
    assert args[5] == ""
    assert args[6] == "bible-garden"
    assert QUERY not in repr(args)


def test_openapi_exposes_search_contract():
    schema = app.openapi()
    route = schema["paths"]["/api/ai/scripture/search"]["post"]
    assert route["operationId"] == "search_scripture"
    assert {"200", "403", "422", "429", "503", "504"} <= route["responses"].keys()
    assert route["security"] == [{"APIKeyHeader": []}]
    request = schema["components"]["schemas"]["SearchRequest"]
    assert request["required"] == ["query", "language"]
    assert request["properties"]["limit"]["maximum"] == 10


@pytest.mark.parametrize("provider", ["gemini", "openai_compat"])
def test_search_prompts_use_both_existing_transports(provider):
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        output = '{"queries": ["Adam", "Seth", "genealogy"]}'
        if provider == "gemini":
            return httpx.Response(
                200, json={"candidates": [{"content": {"parts": [{"text": output}]}}]}
            )
        return httpx.Response(200, json={"choices": [{"message": {"content": output}}]})

    stage = StageProvider(
        stage="scripture_rewrite",
        provider=provider,
        model="test-model",
        endpoint="https://test.invalid/v1" if provider == "openai_compat" else "",
        api_key="unit-test-secret",
        reasoning_effort="omit",
    )
    with httpx.Client(transport=httpx.MockTransport(respond)) as transport:
        with build_query_rewriter(
            stage=stage, http_client=transport, attempts=1
        ) as generator:
            assert search._rewrites(generator, QUERY, "en", Deadline(2)) == [
                QUERY,
                "Adam",
                "Seth",
                "genealogy",
            ]
    payload = requests[0]
    if provider == "gemini":
        instruction = payload["system_instruction"]["parts"][0]["text"]
        content = payload["contents"][0]["parts"][0]["text"]
    else:
        instruction = payload["messages"][0]["content"]
        content = payload["messages"][1]["content"]
    assert "general Bible search" in instruction
    assert json.loads(content) == {"query": QUERY}
