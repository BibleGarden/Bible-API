import asyncio
import hashlib
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("LAMPADA_SYSTEM_PROMPT", "Серверная система")

from fastapi.testclient import TestClient

import lampada_ai
import middleware
from main import app


client = TestClient(app)
real_reserve_rate_limit = lampada_ai._reserve_rate_limit


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
    class FakeCursor:
        def __init__(self):
            self.executions = []
            self.results = iter([(1,), (0, None), (0, None)])

        def execute(self, sql, params=()):
            self.executions.append((" ".join(sql.split()), params))

        def fetchone(self):
            return next(self.results)

        def close(self):
            pass

    class FakeConnection:
        def __init__(self):
            self.cursor_instance = FakeCursor()
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

    connection = FakeConnection()
    monkeypatch.setattr(lampada_ai, "create_connection", lambda: connection)
    monkeypatch.setattr(lampada_ai, "_rate_limit_table_ready", True)

    real_reserve_rate_limit("203.0.113.7")

    insert = next(
        params
        for sql, params in connection.cursor_instance.executions
        if sql.startswith("INSERT INTO lampada_rate_limit_events")
    )
    assert insert == (hashlib.sha256(b"203.0.113.7").digest(),)
    assert connection.commits == 1
    assert connection.closed is True


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
