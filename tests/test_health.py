from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import health
import middleware
from health import router
from middleware import RequestStatsMiddleware


class ImmediateThread:
    def __init__(self, *, target, args, daemon):
        self.target = target
        self.args = args

    def start(self):
        self.target(*self.args)


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestStatsMiddleware)
    app.include_router(router, prefix="/api")

    @app.get("/api/normal")
    def normal():
        return {"status": "ok"}

    return app


client = TestClient(_app())
headers = {"X-API-Key": "test-api-key"}


def _connection(row=(1,)):
    cursor = Mock()
    cursor.fetchone.return_value = row
    connection = Mock()
    connection.cursor.return_value = cursor
    return connection, cursor


def test_health_returns_ok_for_a_nonempty_languages_table(monkeypatch):
    connection, cursor = _connection()
    monkeypatch.setattr(health, "create_connection", lambda: connection)

    response = client.get("/api/health", headers=headers)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    cursor.execute.assert_called_once_with("SELECT 1 FROM languages LIMIT 1")
    cursor.close.assert_called_once_with()
    connection.close.assert_called_once_with()


def test_health_requires_the_existing_api_key(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(health, "create_connection", connect)

    response = client.get("/api/health")

    assert response.status_code == 403
    connect.assert_not_called()


def test_health_returns_generic_503_on_database_failure(monkeypatch):
    connection, cursor = _connection()
    cursor.execute.side_effect = RuntimeError("password=secret-value")
    monkeypatch.setattr(health, "create_connection", lambda: connection)

    response = client.get("/api/health", headers=headers)

    assert response.status_code == 503
    assert response.json() == {"detail": "Service unavailable"}
    assert "secret-value" not in response.text
    cursor.close.assert_called_once_with()
    connection.close.assert_called_once_with()


def test_health_returns_503_for_an_empty_languages_table(monkeypatch):
    connection, _cursor = _connection(row=None)
    monkeypatch.setattr(health, "create_connection", lambda: connection)

    response = client.get("/api/health", headers=headers)

    assert response.status_code == 503
    assert response.json() == {"detail": "Service unavailable"}


def test_health_never_records_stats_but_normal_requests_still_do(monkeypatch):
    connection, cursor = _connection()
    monkeypatch.setattr(health, "create_connection", lambda: connection)
    insert = Mock()
    monkeypatch.setattr(middleware, "_insert_request_log", insert)
    monkeypatch.setattr(
        middleware, "threading", SimpleNamespace(Thread=ImmediateThread)
    )

    assert client.get("/api/health", headers=headers).status_code == 200
    cursor.execute.side_effect = RuntimeError("database unavailable")
    assert client.get("/api/health", headers=headers).status_code == 503
    assert client.get("/api/health").status_code == 403
    insert.assert_not_called()

    assert client.get("/api/normal").status_code == 200
    insert.assert_called_once()
    assert insert.call_args.args[:3] == ("/api/normal", "GET", 200)


def test_openapi_documents_readiness_scope_and_authentication():
    operation = _app().openapi()["paths"]["/api/health"]["get"]

    assert operation["security"] == [{"APIKeyHeader": []}]
    assert "languages table" in operation["description"]
    assert "excluded from request statistics" in operation["description"]
    assert "503" in operation["responses"]


def test_production_app_binds_the_health_router():
    from main import app

    operation = app.openapi()["paths"]["/api/health"]["get"]
    assert operation["operationId"] == "get_health"
