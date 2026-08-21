import asyncio
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")

from fastapi.testclient import TestClient

import lampada_ai
import middleware
from main import app


client = TestClient(app)


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
        json={"system": "Система", "user": "Запрос"},
    )
    assert response.status_code == 403


def test_returns_generated_text(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(lampada_ai, "complete", generated)

    response = client.post(
        "/api/lampada/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"system": "Система", "user": "Запрос"},
    )

    assert response.status_code == 200
    assert response.json() == {"text": "Ответ"}
    generated.assert_awaited_once_with("Система", "Запрос")


@pytest.mark.parametrize(
    "payload",
    [
        {"system": "", "user": "Запрос"},
        {"system": "Система", "user": ""},
        {"system": "Система", "user": "x" * 16001},
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
        json={"system": "Система", "user": "Запрос"},
    )

    assert response.status_code == 502
    assert response.json() == {"detail": "AI service unavailable"}
    assert "provider details" not in response.text


def test_rate_limits_requests(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(lampada_ai, "complete", generated)
    monkeypatch.setattr(lampada_ai, "GEMINI_REQUESTS_PER_MINUTE", 1)
    lampada_ai._request_times.clear()

    first_response = client.post(
        "/api/lampada/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"system": "Система", "user": "Первый запрос"},
    )
    second_response = client.post(
        "/api/lampada/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"system": "Система", "user": "Второй запрос"},
    )
    lampada_ai._request_times.clear()

    assert first_response.status_code == 200
    assert second_response.status_code == 429
    assert second_response.headers["Retry-After"] == "60"
    generated.assert_awaited_once()


def test_trailing_slash_does_not_log_private_endpoint(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    started_threads = []

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            started_threads.append((self.args, self.kwargs))

    monkeypatch.setattr(lampada_ai, "complete", generated)
    monkeypatch.setattr(
        middleware,
        "threading",
        SimpleNamespace(Thread=FakeThread),
    )

    response = client.post(
        "/api/lampada/v1/complete/",
        headers={"X-API-Key": "test-api-key"},
        json={"system": "Система", "user": "Запрос"},
        follow_redirects=False,
    )

    assert response.status_code == 307
    assert started_threads == []


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
            "system_instruction": {"parts": [{"text": "Система"}]},
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
    monkeypatch.setattr(lampada_ai.httpx, "AsyncClient", async_client)

    assert asyncio.run(lampada_ai.complete("Система", "Запрос")) == "Ответ"


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
    monkeypatch.setattr(lampada_ai.httpx, "AsyncClient", async_client)

    with pytest.raises(lampada_ai.GeminiError, match=expected_message):
        asyncio.run(lampada_ai.complete("Система", "Запрос"))
