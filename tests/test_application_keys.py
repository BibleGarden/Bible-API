from unittest.mock import Mock

import pytest
from fastapi import FastAPI, Request, Response
from fastapi.testclient import TestClient

import audio
import auth
import middleware


@pytest.fixture
def app_and_insert(monkeypatch, request_log):
    app = FastAPI()
    app.add_middleware(middleware.RequestStatsMiddleware)
    app.include_router(audio.router, prefix="/api")

    @app.get("/api/probe")
    def probe(request: Request, authenticated: bool = auth.RequireAPIKey):
        return {"application": request.state.application}

    @app.get("/api/degraded-probe")
    def degraded_probe(request: Request, authenticated: bool = auth.RequireAPIKey):
        request.state.degraded_reason = "rerank_failed"
        return {"ok": True}

    @app.get("/api/public-probe")
    def public_probe():
        return {"ok": True}

    monkeypatch.setattr(audio, "validate_audio_path", lambda *args: "audio.mp3")
    monkeypatch.setattr(audio, "create_range_response", lambda *args: Response(status_code=200))
    return TestClient(app), request_log


@pytest.mark.parametrize(
    ("application", "key"),
    [
        ("bible-garden", "test-api-key"),
        ("lampada", "lampada-test-key-12345678901234567890"),
        ("ops", "ops-test-key-1234567890123456789012"),
    ],
)
def test_header_resolves_application_and_logs_it(app_and_insert, application, key):
    client, insert = app_and_insert
    response = client.get("/api/probe", headers={"X-API-Key": key})
    assert response.status_code == 200
    assert response.json() == {"application": application}
    assert insert.call_args.args[6] == application


def test_degraded_reason_is_logged_only_when_the_handler_sets_it(app_and_insert):
    client, insert = app_and_insert
    client.get("/api/probe", headers={"X-API-Key": "test-api-key"})
    assert insert.call_args.args[7] is None
    response = client.get("/api/degraded-probe", headers={"X-API-Key": "test-api-key"})
    assert response.status_code == 200
    assert insert.call_args.args[7] == "rerank_failed"


def test_invalid_key_is_forbidden_and_not_logged(app_and_insert):
    client, insert = app_and_insert
    assert client.get("/api/probe", headers={"X-API-Key": "invalid"}).status_code == 403
    assert client.get("/api/probe").status_code == 403
    insert.assert_not_called()


def test_all_keys_are_compared_even_after_a_match(monkeypatch):
    compare = auth.hmac.compare_digest
    calls = []

    def observed(left, right):
        calls.append((left, right))
        return compare(left, right)

    monkeypatch.setattr(auth.hmac, "compare_digest", observed)
    assert auth.resolve_application("test-api-key") == "bible-garden"
    assert len(calls) == 3
    assert all(len(left) == len(right) == 32 for left, right in calls)


def test_audio_query_takes_precedence_and_preflight_is_not_logged(app_and_insert):
    client, insert = app_and_insert
    path = "/api/audio/en/voice/gen/1.mp3"
    response = client.get(
        path, params={"api_key": "lampada-test-key-12345678901234567890"},
        headers={"X-API-Key": "test-api-key"},
    )
    assert response.status_code == 200
    assert insert.call_args.args[6] == "lampada"
    insert.reset_mock()
    assert client.get(path, params={"api_key": "invalid"},
                      headers={"X-API-Key": "test-api-key"}).status_code == 403
    assert client.options(path).status_code == 200
    insert.assert_not_called()


def test_success_without_authenticated_application_is_not_logged(
    app_and_insert, caplog
):
    client, insert = app_and_insert
    response = client.get("/api/public-probe")
    assert response.status_code == 200
    assert "Request statistics omitted: application missing for status 200" in caplog.text
    insert.assert_not_called()


def test_request_insert_persists_application_and_degraded_reason(monkeypatch):
    connection = Mock()
    cursor = connection.cursor.return_value
    monkeypatch.setattr(middleware, "create_connection", lambda: connection)
    middleware._insert_request_log(
        "/api/probe", "GET", 200, 12, "a" * 40, "test-agent", "lampada",
        "deadline",
    )
    query, values = cursor.execute.call_args.args
    assert "application, degraded_reason)" in query
    assert values[-2:] == ("lampada", "deadline")
    connection.commit.assert_called_once_with()


def test_cache_clear_is_ops_only(monkeypatch, request_log):
    import main

    monkeypatch.setattr(main, "_cache", {})
    monkeypatch.setattr(main, "_cache_timestamps", {})
    clear_corpus = Mock()
    monkeypatch.setattr(main, "clear_cached_resources", clear_corpus)
    insert = request_log
    client = TestClient(main.app)

    for key in ("test-api-key", "lampada-test-key-12345678901234567890"):
        response = client.post("/api/cache/clear", headers={"X-API-Key": key})
        assert response.status_code == 403
        assert response.json() == {"detail": "Invalid or missing API Key"}
    clear_corpus.assert_not_called()

    response = client.post(
        "/api/cache/clear",
        headers={"X-API-Key": "ops-test-key-1234567890123456789012"},
    )
    assert response.status_code == 200
    clear_corpus.assert_called_once_with()
    assert insert.call_args.args[6] == "ops"
