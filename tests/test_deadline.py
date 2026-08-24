"""
Unit tests for the per-request time budget (app/deadline.py) and its effect
on the three Gemini stages (ADR 0006).

The contract: no stage starts a call once the budget is gone, and every
call it does start is capped by what is left — so the endpoint's worst case
is the budget, not the sum of the stages' retry ladders.
"""

import os

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")

from deadline import Deadline, request_timeout, sleep_budget
from embeddings import EmbeddingUnavailable, GeminiEmbeddingClient
from passage_rerank import GeminiPassageReranker, PassageRerankError
from query_rewrite import GeminiQueryRewriter, QueryRewriteError


class FakeClock:
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


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
    assert sleep_budget(None, 2.0) == 2.0


def test_retry_backoff_never_outlives_the_budget():
    clock = FakeClock()
    deadline = Deadline(1.5, clock=clock)

    assert sleep_budget(deadline, 4.0) == 1.5


def _transport(recorder):
    def handler(request: httpx.Request) -> httpx.Response:
        recorder.append(request.extensions.get("timeout", {}).get("connect"))
        return httpx.Response(500)

    return httpx.MockTransport(handler)


def test_rewrite_caps_each_call_by_the_remaining_budget():
    timeouts: list[float] = []
    client = httpx.Client(transport=_transport(timeouts))
    rewriter = GeminiQueryRewriter(
        api_key="k", model="gemini-test", http_client=client,
        timeout=8.0, attempts=2,
    )
    clock = FakeClock()
    deadline = Deadline(3.0, clock=clock)

    with pytest.raises(QueryRewriteError):
        rewriter.rewrite("ru", "тема", [], deadline=deadline)

    assert timeouts and timeouts[0] == 3.0


def test_rerank_does_not_start_a_call_without_budget():
    calls: list[float] = []
    client = httpx.Client(transport=_transport(calls))
    reranker = GeminiPassageReranker(
        api_key="k", model="gemini-test", http_client=client, attempts=2
    )
    clock = FakeClock()
    deadline = Deadline(0.0, clock=clock)

    with pytest.raises(PassageRerankError, match="budget exhausted"):
        reranker.choose("тема", [], ["кандидат"], deadline=deadline)

    assert calls == []


def test_embedding_does_not_start_a_call_without_budget():
    calls: list[float] = []
    client = httpx.Client(transport=_transport(calls))
    embedder = GeminiEmbeddingClient(http_client=client, max_retries=2)
    embedder.config = type(embedder.config)(api_key="k")
    deadline = Deadline(0.0, clock=FakeClock())

    with pytest.raises(EmbeddingUnavailable) as error:
        embedder.embed_query("запрос", deadline=deadline)

    assert calls == []
    assert error.value.provider_down is True
