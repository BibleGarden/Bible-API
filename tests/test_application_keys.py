from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI, Request, Response
from fastapi.testclient import TestClient

import audio
import auth
import middleware


class ImmediateThread:
    def __init__(self, *, target, args, daemon):
        self.target = target
        self.args = args

    def start(self):
        self.target(*self.args)


@pytest.fixture
def app_and_insert(monkeypatch):
    app = FastAPI()
    app.add_middleware(middleware.RequestStatsMiddleware)
    app.include_router(audio.router, prefix="/api")

    @app.get("/api/probe")
    def probe(request: Request, authenticated: bool = auth.RequireAPIKey):
        return {"application": request.state.application}

    @app.get("/api/public-probe")
    def public_probe():
        return {"ok": True}

    insert = Mock()
    monkeypatch.setattr(middleware, "_insert_request_log", insert)
    monkeypatch.setattr(middleware, "threading", SimpleNamespace(Thread=ImmediateThread))
    monkeypatch.setattr(audio, "validate_audio_path", lambda *args: "audio.mp3")
    monkeypatch.setattr(audio, "create_range_response", lambda *args: Response(status_code=200))
    return TestClient(app), insert


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
    assert insert.call_args.args[-1] == application


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
    assert insert.call_args.args[-1] == "lampada"
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


def test_request_insert_persists_application(monkeypatch):
    connection = Mock()
    cursor = connection.cursor.return_value
    monkeypatch.setattr(middleware, "create_connection", lambda: connection)
    middleware._insert_request_log(
        "/api/probe", "GET", 200, 12, "a" * 40, "test-agent", "lampada"
    )
    query, values = cursor.execute.call_args.args
    assert "application" in query
    assert values[-1] == "lampada"
    connection.commit.assert_called_once_with()


def test_request_insert_rejects_missing_application(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(middleware, "create_connection", connect)
    with pytest.raises(RuntimeError, match="application is missing"):
        middleware._insert_request_log("/api/probe", "GET", 200, 12, "a" * 40, "", None)
    connect.assert_not_called()
