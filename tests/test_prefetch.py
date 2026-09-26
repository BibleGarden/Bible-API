"""Speculative traffic is declined before consuming demand quota or AI work."""

from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import middleware
import rate_limit
import scripture_select
import twinkler_ai
from main import app
from prefetch import PrefetchPolicy


@pytest.fixture
def environment(monkeypatch):
    monkeypatch.setattr(middleware, "_insert_request_log", Mock())
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 100.0)


@pytest.mark.parametrize(
    "module,policy_name,path,payload,work_name",
    [
        (twinkler_ai, "question_policy", "/api/ai/question",
         {"topic": "Family", "stage": "first", "messages": []}, "complete"),
        (scripture_select, "scripture_policy", "/api/ai/scripture",
         {"language": "en", "topic": "Family"}, "get_resources"),
    ],
)
@pytest.mark.parametrize("enabled", [False, True])
def test_denial_precedes_work_and_preserves_demand_quota(
    environment, monkeypatch, module, policy_name, path, payload, work_name, enabled
):
    policy = PrefetchPolicy(enabled, 1, 1)
    if enabled:
        policy.enforce("another-client")
    monkeypatch.setattr(module, policy_name, policy)
    work = Mock(side_effect=AssertionError("declined request started AI/corpus work"))
    monkeypatch.setattr(module, work_name, work)
    quota = AsyncMock()
    monkeypatch.setattr(module, "_enforce_rate_limit", quota)
    response = TestClient(app).post(
        path, headers={"X-API-Key": "test-api-key"}, json={**payload, "prefetch": True}
    )
    assert response.status_code == 429
    assert response.json() == {
        "detail": "prefetch_limit_exceeded" if enabled else "prefetch_disabled"
    }
    assert response.headers.get("Retry-After") == ("60" if enabled else None)
    work.assert_not_called()
    quota.assert_not_awaited()


def test_client_and_global_limits_expire_independently(environment, monkeypatch):
    policy = PrefetchPolicy(True, 2, 1)
    policy.enforce("client-a")
    with pytest.raises(HTTPException) as denied:
        policy.enforce("client-a")
    assert denied.value.detail == "prefetch_limit_exceeded"
    policy.enforce("client-b")
    with pytest.raises(HTTPException):
        policy.enforce("client-c")
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 160.0)
    policy.enforce("client-a")
    policy.enforce("client-c")


def test_independent_endpoint_budgets(environment):
    questions = PrefetchPolicy(True, 1, 1)
    scripture = PrefetchPolicy(True, 1, 1)
    questions.enforce("client")
    scripture.enforce("client")
    with pytest.raises(HTTPException):
        questions.enforce("client")


@pytest.mark.parametrize("request_model,payload", [
    (twinkler_ai.CompleteRequest, {"topic": "", "stage": "first", "messages": []}),
    (scripture_select.SelectRequest, {"language": "en"}),
])
@pytest.mark.parametrize("invalid", ["true", 1, None])
def test_prefetch_requires_a_json_boolean(request_model, payload, invalid):
    from pydantic import ValidationError

    assert request_model(**payload).prefetch is False
    with pytest.raises(ValidationError):
        request_model(**payload, prefetch=invalid)
