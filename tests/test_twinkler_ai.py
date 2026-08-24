import asyncio
import base64
from collections import deque
import hashlib
import hmac
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("TWINKLER_SYSTEM_PROMPT", "Серверная система")
os.environ.setdefault("TWINKLER_CLIENT_HMAC_KEY", "test-hmac-key")

from fastapi.testclient import TestClient

import twinkler_ai
import client_ip
import middleware
from main import app


client = TestClient(app)
real_reserve_rate_limit = twinkler_ai._reserve_rate_limit


@pytest.fixture(autouse=True)
def allow_ai_requests(monkeypatch):
    twinkler_ai._request_times.clear()
    twinkler_ai._client_request_times.clear()
    twinkler_ai._last_client_cleanup = 0.0
    reservation = Mock()
    monkeypatch.setattr(twinkler_ai, "_reserve_rate_limit", reservation)
    monkeypatch.setattr(middleware, "_insert_request_log", Mock())
    return reservation


def test_extracts_text_parts():
    data = {
        "candidates": [{
            "content": {"parts": [{"text": "Тихий "}, {"text": "ответ"}]},
        }],
    }
    assert twinkler_ai._extract_text(data) == "Тихий ответ"


def test_requires_api_key():
    response = client.post(
        "/api/twinkler/v1/complete",
        json={"user": "Запрос"},
    )
    assert response.status_code == 403


def test_returns_generated_text(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/twinkler/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
    )

    assert response.status_code == 200
    assert response.json() == {"text": "Ответ"}
    generated.assert_awaited_once_with("Запрос")


def test_ignores_forwarded_for_from_untrusted_peer(monkeypatch, allow_ai_requests):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/twinkler/v1/complete",
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
    monkeypatch.setattr(twinkler_ai, "complete", generated)
    monkeypatch.setattr(client_ip, "TRUSTED_PROXY_IPS", frozenset({"testclient"}))

    response = client.post(
        "/api/twinkler/v1/complete",
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
        "/api/twinkler/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json=payload,
    )

    assert response.status_code == 422


def test_hides_provider_failure(monkeypatch):
    generated = AsyncMock(side_effect=twinkler_ai.GeminiError("provider details"))
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/twinkler/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
    )

    assert response.status_code == 502
    assert response.json() == {"detail": "AI service unavailable"}
    assert "provider details" not in response.text


def test_rate_limits_requests(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    limiter = AsyncMock()
    monkeypatch.setattr(twinkler_ai, "complete", generated)
    monkeypatch.setattr(twinkler_ai, "_enforce_rate_limit", limiter)
    limiter.side_effect = [
        None,
        twinkler_ai.HTTPException(
            status_code=429,
            detail="AI request limit exceeded",
            headers={"Retry-After": "60"},
        ),
    ]

    first_response = client.post(
        "/api/twinkler/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Первый запрос"},
    )
    second_response = client.post(
        "/api/twinkler/v1/complete",
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
        "/api/twinkler/v1/complete/",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
        follow_redirects=False,
    )

    assert response.status_code == 307
    assert len(started_threads) == 1
    args, kwargs = started_threads[0]
    assert args == ()
    assert kwargs["args"][0:3] == (
        "/api/twinkler/v1/complete/",
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
    operation = app.openapi()["paths"]["/api/twinkler/v1/complete"]["post"]

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

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(twinkler_ai, "GEMINI_MODEL", "gemini-test")
    monkeypatch.setattr(twinkler_ai, "TWINKLER_SYSTEM_PROMPT", "Серверная система")
    monkeypatch.setattr(twinkler_ai.httpx, "AsyncClient", async_client)

    assert asyncio.run(twinkler_ai.complete("Запрос")) == "Ответ"


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

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(twinkler_ai, "TWINKLER_SYSTEM_PROMPT", "Серверная система")
    monkeypatch.setattr(twinkler_ai.httpx, "AsyncClient", async_client)

    with pytest.raises(twinkler_ai.GeminiError, match=expected_message):
        asyncio.run(twinkler_ai.complete("Запрос"))


def test_rate_limit_reservation_is_hashed_in_memory(monkeypatch):
    monkeypatch.setattr(twinkler_ai.time, "monotonic", lambda: 100.0)
    real_reserve_rate_limit("203.0.113.7")

    expected_hash = hmac.new(
        b"test-hmac-key",
        b"203.0.113.7",
        hashlib.sha256,
    ).hexdigest()
    assert twinkler_ai._request_times == deque([100.0])
    assert twinkler_ai._client_request_times == {expected_hash: deque([100.0])}
    assert "203.0.113.7" not in twinkler_ai._client_request_times


def test_global_in_memory_limit(monkeypatch):
    monkeypatch.setattr(twinkler_ai.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(twinkler_ai, "GEMINI_REQUESTS_PER_MINUTE", 1)

    real_reserve_rate_limit("203.0.113.7")
    with pytest.raises(twinkler_ai.RateLimitError) as error:
        real_reserve_rate_limit("198.51.100.9")

    assert error.value.retry_after == 60


def test_per_client_in_memory_limit(monkeypatch):
    monkeypatch.setattr(twinkler_ai.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(twinkler_ai, "GEMINI_REQUESTS_PER_MINUTE", 10)
    monkeypatch.setattr(twinkler_ai, "GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE", 1)

    real_reserve_rate_limit("203.0.113.7")
    with pytest.raises(twinkler_ai.RateLimitError) as limited_error:
        real_reserve_rate_limit("203.0.113.7")

    assert limited_error.value.retry_after == 60


def test_in_memory_limit_expires(monkeypatch):
    request_times = iter([100.0, 161.0])
    monkeypatch.setattr(twinkler_ai.time, "monotonic", lambda: next(request_times))
    monkeypatch.setattr(twinkler_ai, "GEMINI_REQUESTS_PER_MINUTE", 1)
    monkeypatch.setattr(twinkler_ai, "GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE", 1)

    real_reserve_rate_limit("203.0.113.7")
    real_reserve_rate_limit("203.0.113.7")

    assert twinkler_ai._request_times == deque([161.0])


def test_rate_limiter_fails_closed(monkeypatch):
    monkeypatch.setattr(
        twinkler_ai,
        "_reserve_rate_limit",
        lambda client_key: (_ for _ in ()).throw(
            twinkler_ai.RateLimitError("limiter unavailable")
        ),
    )

    with pytest.raises(twinkler_ai.HTTPException) as error:
        asyncio.run(twinkler_ai._enforce_rate_limit("203.0.113.7"))

    assert error.value.status_code == 503


def test_transcription_requires_api_key():
    response = client.post(
        "/api/twinkler/v1/transcribe",
        files={"file": ("recording.m4a", b"audio", "audio/mp4")},
    )

    assert response.status_code == 403


def test_returns_transcript_with_soft_locale_hint(monkeypatch):
    generated = AsyncMock(return_value="Господи, помоги мне.")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.m4a", b"m4a-bytes", "audio/mp4")},
        data={"locale": "ru-RU"},
    )

    assert response.status_code == 200
    assert response.json() == {"text": "Господи, помоги мне."}
    generated.assert_awaited_once_with(b"m4a-bytes", "audio/mp4", "ru-RU")


@pytest.mark.parametrize(
    "content_type",
    ["audio/mp4", "audio/x-m4a", "audio/m4a"],
)
def test_transcription_accepts_every_m4a_mime_spelling(monkeypatch, content_type):
    generated = AsyncMock(return_value="Transcript")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.m4a", b"m4a-bytes", content_type)},
    )

    assert response.status_code == 200
    generated.assert_awaited_once_with(b"m4a-bytes", content_type, None)


def test_transcription_locale_is_optional_and_m4a_has_safe_mime_fallback(monkeypatch):
    generated = AsyncMock(return_value="Original language")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={
            "file": (
                "recording.M4A",
                b"m4a-bytes",
                "application/octet-stream",
            )
        },
    )

    assert response.status_code == 200
    generated.assert_awaited_once_with(b"m4a-bytes", "audio/mp4", None)


@pytest.mark.parametrize("locale", ["r", "../../ru", "ru_RU"])
def test_transcription_rejects_invalid_locale(monkeypatch, locale):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.m4a", b"audio", "audio/mp4")},
        data={"locale": locale},
    )

    assert response.status_code == 422
    generated.assert_not_awaited()


def test_transcription_rejects_empty_audio(monkeypatch):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.m4a", b"", "audio/x-m4a")},
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "Audio file is empty"}
    generated.assert_not_awaited()


def test_transcription_rejects_oversized_audio(monkeypatch):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={
            "file": (
                "recording.m4a",
                b"x" * (twinkler_ai._MAX_AUDIO_BYTES + 1),
                "audio/mp4",
            )
        },
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "Audio file is too large"}
    generated.assert_not_awaited()


@pytest.mark.parametrize(
    "filename, content_type",
    [
        ("recording.wav", "audio/wav"),
        ("recording.m4a", "image/png"),
        ("recording.bin", "application/octet-stream"),
    ],
)
def test_transcription_rejects_unsupported_audio(monkeypatch, filename, content_type):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": (filename, b"audio", content_type)},
    )

    assert response.status_code == 415
    assert response.json() == {"detail": "Unsupported audio format"}
    generated.assert_not_awaited()


def test_invalid_audio_does_not_consume_rate_limit(monkeypatch, allow_ai_requests):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.wav", b"audio", "audio/wav")},
    )

    assert response.status_code == 415
    allow_ai_requests.assert_not_called()
    generated.assert_not_awaited()


def test_transcription_hides_provider_failure(monkeypatch, caplog):
    generated = AsyncMock(side_effect=twinkler_ai.GeminiError("private details"))
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("private-name.m4a", b"private audio", "audio/mp4")},
    )

    assert response.status_code == 502
    assert response.json() == {"detail": "AI service unavailable"}
    assert "private details" not in response.text
    assert "private-name" not in response.text
    assert "private audio" not in response.text
    assert "private details" not in caplog.text
    assert "private-name" not in caplog.text
    assert "private audio" not in caplog.text


def test_sends_expected_gemini_transcription_request(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url == (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            "gemini-test:generateContent"
        )
        assert request.headers["x-goog-api-key"] == "secret-test-key"
        payload = json.loads(request.read())
        assert payload["generationConfig"] == {
            "maxOutputTokens": 4096,
            "temperature": 0,
        }
        parts = payload["contents"][0]["parts"]
        assert "original language" in parts[0]["text"]
        assert "Do not translate" in parts[0]["text"]
        assert "app locale is uk-UA" in parts[0]["text"]
        assert "weak hint" in parts[0]["text"]
        assert parts[1] == {
            "inline_data": {
                "mime_type": "audio/mp4",
                "data": base64.b64encode(b"audio bytes").decode("ascii"),
            }
        }
        return httpx.Response(
            200,
            json={"candidates": [{"content": {"parts": [{"text": "Текст"}]}}]},
        )

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient

    def async_client(*args, **kwargs):
        assert kwargs["timeout"] == 60.0
        return real_async_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(twinkler_ai, "GEMINI_TRANSCRIPTION_MODEL", "gemini-test")
    monkeypatch.setattr(twinkler_ai.httpx, "AsyncClient", async_client)

    result = asyncio.run(
        twinkler_ai.transcribe(b"audio bytes", "audio/mp4", "uk-UA")
    )
    assert result == "Текст"


def test_transcription_stats_are_pseudonymized_without_user_agent(monkeypatch):
    started_threads = []

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            started_threads.append((self.args, self.kwargs))

    monkeypatch.setattr(middleware, "threading", SimpleNamespace(Thread=FakeThread))
    monkeypatch.setattr(twinkler_ai, "transcribe", AsyncMock(return_value="Текст"))

    response = client.post(
        "/api/twinkler/v1/transcribe",
        headers={
            "X-API-Key": "test-api-key",
            "User-Agent": "private-device-details",
        },
        files={"file": ("private-name.m4a", b"private audio", "audio/mp4")},
    )

    assert response.status_code == 200
    assert len(started_threads) == 1
    args, kwargs = started_threads[0]
    assert args == ()
    expected_client = hmac.new(
        b"test-hmac-key",
        b"testclient",
        hashlib.sha256,
    ).hexdigest()[:40]
    assert kwargs["args"][:3] == (
        "/api/twinkler/v1/transcribe",
        "POST",
        200,
    )
    assert kwargs["args"][4:] == (expected_client, "")
    assert "private-name" not in repr(kwargs)
    assert "private audio" not in repr(kwargs)


def test_openapi_documents_transcription_contract():
    operation = app.openapi()["paths"]["/api/twinkler/v1/transcribe"]["post"]

    assert operation["requestBody"]["content"].keys() == {"multipart/form-data"}
    assert {"200", "403", "413", "415", "422", "429", "502", "503"} <= set(
        operation["responses"]
    )
    assert "Retry-After" in operation["responses"]["429"]["headers"]
