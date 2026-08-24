"""Unit tests for the Gemini embedding client failure modes
(app/embeddings.py). No network: httpx.MockTransport only.

Focus: every failure surfaces as EmbeddingUnavailable (never a raw
json.JSONDecodeError — retrieval m3) and the provider_down flag lets
callers fail fast when the provider is down for everyone (retrieval m2).
"""

import os

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")

from embeddings import (
    EmbeddingConfig,
    EmbeddingUnavailable,
    GeminiEmbeddingClient,
    normalize,
)

DIMS = 4


def make_client(handler) -> GeminiEmbeddingClient:
    return GeminiEmbeddingClient(
        config=EmbeddingConfig(
            model="embed-test", dimensions=DIMS, api_key="test-key"
        ),
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _s: None,
    )


def test_embed_query_happy_path():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"embedding": {"values": [3.0, 0.0, 4.0, 0.0]}}
        )

    vector = make_client(handler).embed_query("текст")
    assert vector == pytest.approx(normalize([3.0, 0.0, 4.0, 0.0]))


def test_http_200_with_invalid_json_raises_embedding_unavailable():
    # m3: a 200 with a broken body used to escape as json.JSONDecodeError
    # and crash select() instead of triggering its fallback.
    client = make_client(
        lambda r: httpx.Response(200, content=b"<html>not json</html>")
    )
    with pytest.raises(EmbeddingUnavailable, match="invalid JSON") as exc_info:
        client.embed_query("текст")
    assert exc_info.value.provider_down is False


def test_http_200_with_non_object_json_raises_embedding_unavailable():
    for payload in ([1, 2, 3], {"embedding": "junk"}, {"embedding": {}}):
        client = make_client(lambda r, p=payload: httpx.Response(200, json=p))
        with pytest.raises(EmbeddingUnavailable):
            client.embed_query("текст")


def test_wrong_dimension_count_is_not_provider_down():
    client = make_client(
        lambda r: httpx.Response(200, json={"embedding": {"values": [1.0]}})
    )
    with pytest.raises(EmbeddingUnavailable, match="size") as exc_info:
        client.embed_query("текст")
    assert exc_info.value.provider_down is False


def test_missing_api_key_is_provider_down():
    client = GeminiEmbeddingClient(
        config=EmbeddingConfig(model="m", dimensions=DIMS, api_key=""),
        http_client=httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200))
        ),
    )
    with pytest.raises(EmbeddingUnavailable) as exc_info:
        client.embed_query("текст")
    assert exc_info.value.provider_down is True


def test_exhausted_retries_on_5xx_is_provider_down():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    with pytest.raises(EmbeddingUnavailable) as exc_info:
        make_client(handler).embed_query("текст")
    assert exc_info.value.provider_down is True
    assert calls["n"] == 6  # full retry budget


def test_transport_errors_are_provider_down():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(EmbeddingUnavailable) as exc_info:
        make_client(handler).embed_query("текст")
    assert exc_info.value.provider_down is True


def test_non_retryable_http_error_is_not_provider_down():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, json={"error": {"message": "bad"}})

    with pytest.raises(EmbeddingUnavailable) as exc_info:
        make_client(handler).embed_query("текст")
    assert exc_info.value.provider_down is False
    assert calls["n"] == 1  # no retries on a request-specific error


def test_retry_then_success_recovers():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(429, json={"error": {}})
        return httpx.Response(
            200, json={"embedding": {"values": [1.0, 0.0, 0.0, 0.0]}}
        )

    assert make_client(handler).embed_query("текст")[0] == pytest.approx(1.0)
    assert calls["n"] == 3
