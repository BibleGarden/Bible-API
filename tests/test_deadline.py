"""
Unit tests for the per-request time budget (app/deadline.py) and its effect
on the three Gemini stages (ADR 0006, ClickUp 86cbbnaxn).

The contract: no stage starts a call once the budget is gone, every call it
does start is capped by what is left — across ALL of httpx's timeout phases,
not each of them — and no pause is slept unless the attempt after it still
fits. So the endpoint's worst case is the budget, not the sum of the stages'
retry ladders.

The ceiling tests below run on a fake clock that only advances where real
time would be spent (inside a provider call, and inside a backoff), so they
assert the wall-clock bound without waiting for it.
"""

import os

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")

from deadline import Deadline, request_timeout
from embeddings import EmbeddingUnavailable, GeminiEmbeddingClient
from passage_rerank import GeminiPassageReranker, PassageRerankError
from query_rewrite import GeminiQueryRewriter, QueryRewriteError
from test_gemini_retry import FakeClock, quota_body

BUDGET = 15.0            # AI_SCRIPTURE_TIMEOUT_SECONDS in production
PROVIDER_TIMEOUT = 8.0   # scripture_select._PROVIDER_TIMEOUT_SECONDS
PROVIDER_ATTEMPTS = 2    # scripture_select._PROVIDER_ATTEMPTS


def test_budget_shrinks_with_elapsed_time():
    clock = FakeClock()
    deadline = Deadline(10.0, clock=clock)

    assert deadline.remaining() == 10.0
    assert deadline.budget(8.0) == 8.0

    clock.advance(6.0)
    assert deadline.elapsed() == 6.0
    assert deadline.budget(8.0) == 4.0
    assert not deadline.expired()

    clock.advance(5.0)
    assert deadline.expired()
    assert deadline.budget(8.0) == 0.0


def test_helpers_pass_through_without_a_deadline():
    assert request_timeout(None, 8.0) == 8.0


# ---------------------------------------------------------------------------
# Fake providers
# ---------------------------------------------------------------------------

def burning_transport(clock, respond=None):
    """A provider that spends every second its timeout allows.

    The handler advances the clock by the SUM of the four httpx phases,
    which is exactly what httpx permits a hostile or hung endpoint to take
    (each phase is bounded on its own, never in total). A stage that hands
    httpx a bare number therefore burns four times the budget it meant to
    grant — the overrun this suite exists to catch.
    """
    calls: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        timeout = dict(request.extensions.get("timeout", {}))
        calls.append(timeout)
        clock.advance(sum(value for value in timeout.values() if value))
        if respond is None:
            raise httpx.ReadTimeout("timed out", request=request)
        return respond(request)

    return httpx.MockTransport(handler), calls


def instant_transport(clock, respond):
    """A provider that answers immediately (no time spent)."""
    calls: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(request.extensions.get("timeout", {})))
        return respond(request)

    return httpx.MockTransport(handler), calls


@pytest.fixture
def clock(monkeypatch):
    """A fake clock that every sleep in the retry ladders advances."""
    fake = FakeClock()
    import time as time_module

    monkeypatch.setattr(time_module, "sleep", fake.advance)
    return fake


def make_rewriter(transport, **kwargs):
    kwargs.setdefault("timeout", PROVIDER_TIMEOUT)
    kwargs.setdefault("attempts", PROVIDER_ATTEMPTS)
    return GeminiQueryRewriter(
        api_key="k", model="gemini-test",
        http_client=httpx.Client(transport=transport), **kwargs,
    )


def make_reranker(transport, **kwargs):
    kwargs.setdefault("timeout", PROVIDER_TIMEOUT)
    kwargs.setdefault("attempts", PROVIDER_ATTEMPTS)
    return GeminiPassageReranker(
        api_key="k", model="gemini-test",
        http_client=httpx.Client(transport=transport), **kwargs,
    )


def make_embedder(transport, clock, **kwargs):
    kwargs.setdefault("timeout", PROVIDER_TIMEOUT)
    kwargs.setdefault("max_retries", PROVIDER_ATTEMPTS)
    embedder = GeminiEmbeddingClient(
        http_client=httpx.Client(transport=transport),
        sleep=clock.advance, **kwargs,
    )
    embedder.config = type(embedder.config)(api_key="k")
    return embedder


# ---------------------------------------------------------------------------
# Each call is capped by what is left — in total, not per phase
# ---------------------------------------------------------------------------

def phase_total(timeout: dict) -> float:
    return sum(value for value in timeout.values() if value)


def test_rewrite_caps_each_call_by_the_remaining_budget(clock):
    transport, calls = instant_transport(
        clock, lambda r: httpx.Response(500)
    )
    rewriter = make_rewriter(transport)

    with pytest.raises(QueryRewriteError):
        rewriter.rewrite(
            "ru", "тема", [], deadline=Deadline(3.0, clock=clock)
        )

    assert calls and phase_total(calls[0]) == pytest.approx(3.0)


def test_rerank_caps_each_call_by_the_remaining_budget(clock):
    transport, calls = instant_transport(
        clock, lambda r: httpx.Response(500)
    )
    reranker = make_reranker(transport)

    with pytest.raises(PassageRerankError):
        reranker.choose(
            "тема", [], ["кандидат"], deadline=Deadline(3.0, clock=clock)
        )

    assert calls and phase_total(calls[0]) == pytest.approx(3.0)


def test_embedding_caps_each_call_by_the_remaining_budget(clock):
    transport, calls = instant_transport(
        clock, lambda r: httpx.Response(500)
    )
    embedder = make_embedder(transport, clock)

    with pytest.raises(EmbeddingUnavailable):
        embedder.embed_query("запрос", deadline=Deadline(3.0, clock=clock))

    assert calls and phase_total(calls[0]) == pytest.approx(3.0)


def test_rerank_does_not_start_a_call_without_budget(clock):
    transport, calls = burning_transport(clock)
    reranker = make_reranker(transport)

    with pytest.raises(PassageRerankError, match="budget exhausted"):
        reranker.choose(
            "тема", [], ["кандидат"], deadline=Deadline(0.0, clock=clock)
        )

    assert calls == []


def test_embedding_does_not_start_a_call_without_budget(clock):
    transport, calls = burning_transport(clock)
    embedder = make_embedder(transport, clock)

    with pytest.raises(EmbeddingUnavailable) as error:
        embedder.embed_query("запрос", deadline=Deadline(0.0, clock=clock))

    assert calls == []
    assert error.value.provider_down is True


# ---------------------------------------------------------------------------
# The wall-clock ceiling of a stage whose provider hangs
# ---------------------------------------------------------------------------

def test_a_hanging_provider_cannot_outlive_the_rewrite_budget(clock):
    transport, calls = burning_transport(clock)
    rewriter = make_rewriter(transport)
    started = clock.now

    with pytest.raises(QueryRewriteError):
        rewriter.rewrite(
            "ru", "тема", [], deadline=Deadline(BUDGET, clock=clock)
        )

    assert clock.now - started <= BUDGET + 1e-6
    assert len(calls) <= PROVIDER_ATTEMPTS


def test_a_hanging_provider_cannot_outlive_the_rerank_budget(clock):
    transport, calls = burning_transport(clock)
    reranker = make_reranker(transport)
    started = clock.now

    with pytest.raises(PassageRerankError):
        reranker.choose(
            "тема", [], ["кандидат"], deadline=Deadline(BUDGET, clock=clock)
        )

    assert clock.now - started <= BUDGET + 1e-6
    assert len(calls) <= PROVIDER_ATTEMPTS


def test_a_hanging_provider_cannot_outlive_the_embedding_budget(clock):
    transport, _calls = burning_transport(clock)
    embedder = make_embedder(transport, clock)
    started = clock.now

    with pytest.raises(EmbeddingUnavailable):
        embedder.embed_query("запрос", deadline=Deadline(BUDGET, clock=clock))

    assert clock.now - started <= BUDGET + 1e-6


def test_the_offline_indexing_ladder_still_has_no_ceiling(clock):
    """Without a deadline nothing above applies: the CLI keeps its patient
    six-attempt ladder (that path indexes the corpus, it does not serve a
    request)."""
    transport, calls = instant_transport(
        clock, lambda r: httpx.Response(503)
    )
    embedder = make_embedder(transport, clock, max_retries=6)

    with pytest.raises(EmbeddingUnavailable):
        embedder.embed_query("запрос")

    assert len(calls) == 6


# ---------------------------------------------------------------------------
# 429: which quota rejected us decides whether a retry is worth anything
# ---------------------------------------------------------------------------

DAILY = "GenerateRequestsPerDayPerProjectPerModel-FreeTier"
PER_MINUTE = "GenerateRequestsPerMinutePerProjectPerModel-FreeTier"


def test_the_offline_ladder_still_stops_on_an_exhausted_daily_quota(clock):
    """The one place the deadline-free CLI path DID change (and should).

    Its patience is for a busy provider, not for a closed window: a daily
    free-tier quota reopens tomorrow, so the six-attempt ladder would spend
    63 s per chunk to fail exactly as it failed at once. `index_cli rebuild`
    is resume-safe, so failing immediately is the cheap answer.
    """
    transport, calls = instant_transport(clock, quota_response(DAILY))
    embedder = make_embedder(transport, clock, max_retries=6)

    with pytest.raises(EmbeddingUnavailable):
        embedder.embed_query("запрос")

    assert len(calls) == 1
    assert clock.now == 1000.0, "and nothing was slept off waiting for it"


def quota_response(quota_id: str, retry_delay: str = "14s"):
    def respond(_request):
        return httpx.Response(429, json=quota_body(quota_id, retry_delay))

    return respond


def test_rewrite_stops_at_once_on_an_exhausted_daily_quota(clock):
    transport, calls = instant_transport(clock, quota_response(DAILY))
    rewriter = make_rewriter(transport)
    started = clock.now

    with pytest.raises(QueryRewriteError, match="after retries"):
        rewriter.rewrite(
            "ru", "тема", [], deadline=Deadline(BUDGET, clock=clock)
        )

    assert len(calls) == 1, "a daily quota cannot reopen inside one request"
    assert clock.now == started, "and nothing was slept off waiting for it"


def test_rerank_stops_at_once_on_an_exhausted_daily_quota(clock):
    transport, calls = instant_transport(clock, quota_response(DAILY))
    reranker = make_reranker(transport)
    started = clock.now

    with pytest.raises(PassageRerankError, match="after retries"):
        reranker.choose(
            "тема", [], ["кандидат"], deadline=Deadline(BUDGET, clock=clock)
        )

    assert len(calls) == 1
    assert clock.now == started


def test_embedding_stops_at_once_on_an_exhausted_daily_quota(clock):
    transport, calls = instant_transport(clock, quota_response(DAILY))
    embedder = make_embedder(transport, clock, max_retries=6)
    started = clock.now

    with pytest.raises(EmbeddingUnavailable) as error:
        embedder.embed_query("запрос", deadline=Deadline(BUDGET, clock=clock))

    assert len(calls) == 1
    assert clock.now == started
    assert error.value.provider_down is True


def test_a_minute_quota_is_retried_after_the_delay_the_provider_asked_for(clock):
    answers = {"n": 0}

    def respond(request):
        answers["n"] += 1
        if answers["n"] == 1:
            return httpx.Response(429, json=quota_body(PER_MINUTE, "3s"))
        return httpx.Response(200, json={
            "candidates": [{"content": {"parts": [
                {"text": '{"queries": ["вариант"]}'}
            ]}}]
        })

    transport, calls = instant_transport(clock, respond)
    rewriter = make_rewriter(transport)
    started = clock.now

    assert rewriter.rewrite(
        "ru", "тема", [], deadline=Deadline(BUDGET, clock=clock)
    ) == ["вариант"]

    assert len(calls) == 2
    assert clock.now - started == 3.0, "the provider's own retryDelay won"


def test_a_minute_quota_longer_than_the_budget_degrades_immediately(clock):
    transport, calls = instant_transport(
        clock, quota_response(PER_MINUTE, "55s")
    )
    reranker = make_reranker(transport)
    started = clock.now

    with pytest.raises(PassageRerankError, match="after retries"):
        reranker.choose(
            "тема", [], ["кандидат"], deadline=Deadline(BUDGET, clock=clock)
        )

    assert len(calls) == 1
    assert clock.now == started


def test_a_429_without_recognisable_details_keeps_the_ordinary_ladder(clock):
    transport, calls = instant_transport(
        clock, lambda r: httpx.Response(429, json={"error": {}})
    )
    reranker = make_reranker(transport, attempts=3)
    started = clock.now

    with pytest.raises(PassageRerankError, match="after retries"):
        reranker.choose(
            "тема", [], ["кандидат"], deadline=Deadline(BUDGET, clock=clock)
        )

    assert len(calls) == 3, "unknown details -> the documented backoff ladder"
    assert clock.now - started == 6.0, "2 s + 4 s"


# ---------------------------------------------------------------------------
# End to end: the whole pipeline against a dead provider
# ---------------------------------------------------------------------------

def test_the_whole_pipeline_degrades_inside_the_budget(clock):
    """The regression test of the incident: rewrite, embeddings and rerank
    all hanging must still leave a verified answer inside the budget.

    Before the fix each stage handed httpx a bare number, so the FIRST
    rewrite attempt alone was allowed 4x8 = 32 s — twice the whole budget —
    and the embedding ladder then slept off whatever was left before finding
    it had nothing to spend it on.
    """
    from retrieval import ScriptureRetriever, SelectionRequest
    # The synthetic corpus of the retrieval suite: a real in-memory vector
    # index and a passage loader, so the fallback path is the real one.
    from test_retrieval import fake_loader, make_index

    transport, _calls = burning_transport(clock)
    deadline = Deadline(BUDGET, clock=clock)
    retriever = ScriptureRetriever(
        index=make_index(),
        embedder=make_embedder(transport, clock),
        rewriter=make_rewriter(transport),
        reranker=make_reranker(transport),
        load_passages=fake_loader,
    )
    started = clock.now

    final = retriever.select_final(
        SelectionRequest(language="ru", topic="Тревога перед операцией"),
        deadline,
    )

    assert clock.now - started <= BUDGET + 1e-6
    # degraded, but with a real passage from the corpus
    assert final.candidate is not None
    assert final.selection.source == "safe_pool"
    assert final.selection.fallback_reason == "deadline"
