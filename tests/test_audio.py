"""Browser audio authentication, CORS and byte-range contract."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import audio
import middleware

SITE_KEY = "site-test-key-12345678901234567890123"
AUDIO_PATH = "/api/audio/en/voice/1/1.mp3"
CONTENT = b"0123456789"


@pytest.fixture
def client(tmp_path, monkeypatch, request_log):
    chapter = tmp_path / "en" / "voice" / "mp3" / "1" / "1.mp3"
    chapter.parent.mkdir(parents=True)
    chapter.write_bytes(CONTENT)
    monkeypatch.setattr(audio, "AUDIO_FILES_PATH", str(tmp_path))
    app = FastAPI()
    app.add_middleware(middleware.RequestStatsMiddleware)
    app.include_router(audio.router, prefix="/api")
    return TestClient(app)


def assert_cors(response):
    assert response.headers["access-control-allow-origin"] == "*"
    exposed = {name.strip().lower() for name in
               response.headers["access-control-expose-headers"].split(",")}
    assert {"accept-ranges", "content-range", "content-length"} <= exposed
    assert response.headers["accept-ranges"] == "bytes"


@pytest.mark.parametrize("key", [SITE_KEY, "test-api-key",
    "lampada-test-key-12345678901234567890", "ops-test-key-1234567890123456789012"])
@pytest.mark.parametrize("range_header,expected,content_range", [
    ("bytes=2-5", b"2345", "bytes 2-5/10"),
    ("bytes=6-", b"6789", "bytes 6-9/10"),
    ("bytes=-3", b"789", "bytes 7-9/10"),
    ("bytes=-20", CONTENT, "bytes 0-9/10"),
    ("bytes=8-20", b"89", "bytes 8-9/10"),
])
def test_audio_range_and_cors(client, key, range_header, expected, content_range):
    response = client.get(AUDIO_PATH, params={"api_key": key},
                          headers={"Origin": "https://bible.garden", "Range": range_header})
    assert response.status_code == 206
    assert response.content == expected
    assert response.headers["content-range"] == content_range
    assert response.headers["content-length"] == str(len(expected))
    assert response.headers["content-type"] == "audio/mpeg"
    assert_cors(response)


@pytest.mark.parametrize("method", ["get", "head"])
def test_audio_full_response_and_cors(client, method):
    response = getattr(client, method)(AUDIO_PATH, params={"api_key": SITE_KEY},
                                     headers={"Origin": "https://bible.garden"})
    assert response.status_code == 200
    assert response.content == (CONTENT if method == "get" else b"")
    assert response.headers["content-length"] == "10"
    assert_cors(response)


@pytest.mark.parametrize("range_header", ["bytes=10-", "bytes=8-2", "bytes=-0",
    "bytes=-", "bytes=bad-5", "bytes=0-+5", "items=0-2"])
def test_invalid_range_has_cors_and_file_size(client, range_header):
    response = client.get(AUDIO_PATH, params={"api_key": SITE_KEY},
                          headers={"Origin": "https://bible.garden", "Range": range_header})
    assert response.status_code == 416
    assert response.headers["content-range"] == "bytes */10"
    assert_cors(response)


def test_audio_preflight_allows_range_without_key(client):
    response = client.options(AUDIO_PATH, headers={
        "Origin": "https://bible.garden", "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "Range, If-Range",
    })
    assert response.status_code == 200
    assert "GET" in response.headers["access-control-allow-methods"]
    assert "HEAD" in response.headers["access-control-allow-methods"]
    allowed = {name.strip().lower() for name in
               response.headers["access-control-allow-headers"].split(",")}
    assert {"range", "if-range"} <= allowed
    assert_cors(response)
