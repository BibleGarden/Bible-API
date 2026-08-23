import asyncio
import hashlib
import hmac
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("LAMPADA_SYSTEM_PROMPT", "Серверная система")
os.environ.setdefault("LAMPADA_CLIENT_HMAC_KEY", "test-hmac-key")

from fastapi.testclient import TestClient

import lampada_ai
import client_ip
import middleware
from main import app


client = TestClient(app)
real_reserve_rate_limit = lampada_ai._reserve_rate_limit


class FakeRateLimitCursor:
    def __init__(self, results):
        self.executions = []
        self.results = iter(results)
        self.unread_result = False
        self.closed = False

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        self.executions.append((normalized, params))
        self.unread_result = normalized.startswith("SELECT")

    def fetchone(self):
        result = next(self.results)
        self.unread_result = False
        return result

    def close(self):
        if self.unread_result:
            raise RuntimeError("Unread result found")
        self.closed = True


class FakeRateLimitConnection:
    def __init__(self, results):
        self.cursor_instance = FakeRateLimitCursor(results)
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def allow_ai_requests(monkeypatch):
    reservation = Mock()
    monkeypatch.setattr(lampada_ai, "_reserve_rate_limit", reservation)
    monkeypatch.setattr(middleware, "_insert_request_log", Mock())
    return reservation


def test_extracts_text_parts():
    data = {
        "candidates": [{
            "content": {"parts": [{"text": "Тихий "}, {"text": "ответ"}]},
        }],
    }
    assert lampada_ai._extract_text(data) == "Тихий ответ"


def test_requires_api_key():
    response = client.post(
        "/api/lampada/v1/complete",
        json={"user": "Запрос"},
    )
    assert response.status_code == 403


def test_returns_generated_text(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(lampada_ai, "complete", generated)

    response = client.post(
        "/api/lampada/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
    )

    assert response.status_code == 200
    assert response.json() == {"text": "Ответ"}
    generated.assert_awaited_once_with("Запрос")


def test_ignores_forwarded_for_from_untrusted_peer(monkeypatch, allow_ai_requests):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(lampada_ai, "complete", generated)

    response = client.post(
        "/api/lampada/v1/complete",
        headers={
            "X-API-Key": "test-api-key",
            "X-Forwarded-For": "203.0.113.7",
        },
        json={"user": "Запрос"},
    )

    assert response.status_code == 200
    allow_ai_requests.assert_called_once_with("testclient")


def test_uses_forwarded_for_from_trusted_peer(monkeypatch, allow_ai_requests):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(lampada_ai, "complete", generated)
    monkeypatch.setattr(client_ip, "TRUSTED_PROXY_IPS", frozenset({"testclient"}))

    response = client.post(
        "/api/lampada/v1/complete",
        headers={
            "X-API-Key": "test-api-key",
            "X-Forwarded-For": "203.0.113.7, 192.0.2.1",
        },
        json={"user": "Запрос"},
    )

    assert response.status_code == 200
    allow_ai_requests.assert_called_once_with("203.0.113.7")


@pytest.mark.parametrize(
    "payload",
    [
        {"user": ""},
        {"user": "x" * 16001},
        {"system": "Клиентская система", "user": "Запрос"},
    ],
)
def test_rejects_invalid_prompts(payload):
    response = client.post(
        "/api/lampada/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json=payload,
    )

    assert response.status_code == 422


def test_hides_provider_failure(monkeypatch):
    generated = AsyncMock(side_effect=lampada_ai.GeminiError("provider details"))
    monkeypatch.setattr(lampada_ai, "complete", generated)

    response = client.post(
        "/api/lampada/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
    )

    assert response.status_code == 502
    assert response.json() == {"detail": "AI service unavailable"}
    assert "provider details" not in response.text


def test_rate_limits_requests(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    limiter = AsyncMock()
    monkeypatch.setattr(lampada_ai, "complete", generated)
    monkeypatch.setattr(lampada_ai, "_enforce_rate_limit", limiter)
    limiter.side_effect = [
        None,
        lampada_ai.HTTPException(
            status_code=429,
            detail="AI request limit exceeded",
            headers={"Retry-After": "60"},
        ),
    ]

    first_response = client.post(
        "/api/lampada/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Первый запрос"},
    )
    second_response = client.post(
        "/api/lampada/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Второй запрос"},
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 429
    assert second_response.headers["Retry-After"] == "60"
    generated.assert_awaited_once()


def test_trailing_slash_is_recorded_without_request_body(monkeypatch):
    started_threads = []

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            started_threads.append((self.args, self.kwargs))

    monkeypatch.setattr(
        middleware,
        "threading",
        SimpleNamespace(Thread=FakeThread),
    )

    response = client.post(
        "/api/lampada/v1/complete/",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
        follow_redirects=False,
    )

    assert response.status_code == 307
    assert len(started_threads) == 1
    args, kwargs = started_threads[0]
    assert args == ()
    assert kwargs["args"][0:3] == (
        "/api/lampada/v1/complete/",
        "POST",
        307,
    )
    assert len(kwargs["args"]) == 6
    expected_client = hmac.new(
        b"test-hmac-key",
        b"testclient",
        hashlib.sha256,
    ).hexdigest()[:40]
    assert kwargs["args"][4:] == (expected_client, "")


def test_openapi_documents_public_errors():
    operation = app.openapi()["paths"]["/api/lampada/v1/complete"]["post"]

    assert {"200", "403", "422", "429", "502", "503"} <= set(
        operation["responses"]
    )
    assert "Retry-After" in operation["responses"]["429"]["headers"]


def test_sends_expected_gemini_request(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url == (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            "gemini-test:generateContent"
        )
        assert request.headers["x-goog-api-key"] == "secret-test-key"
        assert request.headers["content-type"] == "application/json"
        assert json.loads(request.read()) == {
            "system_instruction": {"parts": [{"text": "Серверная система"}]},
            "contents": [{"role": "user", "parts": [{"text": "Запрос"}]}],
            "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.7},
        }
        return httpx.Response(
            200,
            json={"candidates": [{"content": {"parts": [{"text": "Ответ"}]}}]},
        )

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient

    def async_client(*args, **kwargs):
        return real_async_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(lampada_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(lampada_ai, "GEMINI_MODEL", "gemini-test")
    monkeypatch.setattr(lampada_ai, "LAMPADA_SYSTEM_PROMPT", "Серверная система")
    monkeypatch.setattr(lampada_ai.httpx, "AsyncClient", async_client)

    assert asyncio.run(lampada_ai.complete("Запрос")) == "Ответ"


@pytest.mark.parametrize(
    ("response", "expected_message"),
    [
        (httpx.Response(429), "Gemini request failed"),
        (httpx.Response(200, json={"candidates": []}), "Gemini returned no text"),
    ],
)
def test_handles_gemini_failures(monkeypatch, response, expected_message):
    transport = httpx.MockTransport(lambda request: response)
    real_async_client = httpx.AsyncClient

    def async_client(*args, **kwargs):
        return real_async_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(lampada_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(lampada_ai, "LAMPADA_SYSTEM_PROMPT", "Серверная система")
    monkeypatch.setattr(lampada_ai.httpx, "AsyncClient", async_client)

    with pytest.raises(lampada_ai.GeminiError, match=expected_message):
        asyncio.run(lampada_ai.complete("Запрос"))


def test_rate_limit_reservation_is_shared_and_hashed(monkeypatch):
    connection = FakeRateLimitConnection([(1,), (0, None), (0, None), (1,)])
    monkeypatch.setattr(lampada_ai, "create_connection", lambda: connection)
    real_reserve_rate_limit("203.0.113.7")

    insert = next(
        params
        for sql, params in connection.cursor_instance.executions
        if sql.startswith("INSERT INTO lampada_rate_limit_events")
    )
    expected_hash = hmac.new(
        b"test-hmac-key",
        b"203.0.113.7",
        hashlib.sha256,
    ).digest()
    assert insert == (expected_hash,)
    assert connection.commits == 1
    assert connection.cursor_instance.closed is True
    assert connection.closed is True


def test_global_limit_is_shared_between_connections(monkeypatch):
    first = FakeRateLimitConnection([(1,), (0, None), (0, None), (1,)])
    second = FakeRateLimitConnection([(1,), (1, 10_000_000), (1,)])
    connections = iter([first, second])
    monkeypatch.setattr(lampada_ai, "create_connection", lambda: next(connections))
    monkeypatch.setattr(lampada_ai, "GEMINI_REQUESTS_PER_MINUTE", 1)

    real_reserve_rate_limit("203.0.113.7")
    with pytest.raises(lampada_ai.RateLimitError) as error:
        real_reserve_rate_limit("198.51.100.9")

    assert error.value.retry_after == 50
    assert first.commits == 1
    assert second.rollbacks == 1
    assert first.closed is True
    assert second.closed is True


def test_per_client_limit_and_lock_timeout(monkeypatch):
    limited = FakeRateLimitConnection(
        [(1,), (0, None), (3, 20_000_000), (1,)]
    )
    timed_out = FakeRateLimitConnection([(0,)])
    connections = iter([limited, timed_out])
    monkeypatch.setattr(lampada_ai, "create_connection", lambda: next(connections))
    monkeypatch.setattr(lampada_ai, "GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE", 3)

    with pytest.raises(lampada_ai.RateLimitError) as limited_error:
        real_reserve_rate_limit("203.0.113.7")
    with pytest.raises(lampada_ai.RateLimitError) as timeout_error:
        real_reserve_rate_limit("203.0.113.7")

    assert limited_error.value.retry_after == 40
    assert timeout_error.value.retry_after is None
    assert limited.rollbacks == 1
    assert timed_out.rollbacks == 1
    assert limited.cursor_instance.closed is True
    assert timed_out.cursor_instance.closed is True


def test_rate_limiter_fails_closed(monkeypatch):
    monkeypatch.setattr(
        lampada_ai,
        "_reserve_rate_limit",
        lambda client_key: (_ for _ in ()).throw(
            lampada_ai.RateLimitError("database unavailable")
        ),
    )

    with pytest.raises(lampada_ai.HTTPException) as error:
        asyncio.run(lampada_ai._enforce_rate_limit("203.0.113.7"))

    assert error.value.status_code == 503
