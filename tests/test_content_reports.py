from fastapi.testclient import TestClient

import content_reports
from content_reports import CONTENT_REPORT_PATH, MAX_BODY_BYTES
from main import app
from middleware import PRIVATE_PATHS


client = TestClient(app)


class FakeCursor:
    def __init__(self, *, lastrowid=17, error=None):
        self.lastrowid = lastrowid
        self.error = error
        self.sql = None
        self.params = None
        self.closed = False

    def execute(self, sql, params):
        if self.error is not None:
            raise self.error
        self.sql = sql
        self.params = params

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def report_body(**overrides):
    body = {
        "content_type": "question",
        "content_text": "What gives you hope today?",
        "user_comment": "This felt unrelated",
        "language": "en",
    }
    body.update(overrides)
    return body


def test_content_report_requires_api_key():
    response = client.post(CONTENT_REPORT_PATH, json=report_body())

    assert response.status_code == 403


def test_content_report_is_stored(monkeypatch):
    cursor = FakeCursor(lastrowid=23)
    connection = FakeConnection(cursor)
    monkeypatch.setattr(content_reports, "create_connection", lambda: connection)

    response = client.post(
        CONTENT_REPORT_PATH,
        headers={"X-API-Key": "test-api-key"},
        json=report_body(),
    )

    assert response.status_code == 201
    assert response.json() == {"status": "ok", "report_id": 23}
    assert "INSERT INTO ai_content_reports" in cursor.sql
    assert cursor.params == (
        "question",
        "What gives you hope today?",
        "This felt unrelated",
        "en",
    )
    assert connection.committed is True
    assert connection.rolled_back is False
    assert cursor.closed is True
    assert connection.closed is True


def test_blank_optional_comment_is_stored_as_null(monkeypatch):
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    monkeypatch.setattr(content_reports, "create_connection", lambda: connection)

    response = client.post(
        CONTENT_REPORT_PATH,
        headers={"X-API-Key": "test-api-key"},
        json=report_body(user_comment="   "),
    )

    assert response.status_code == 201
    assert cursor.params[2] is None


def test_content_report_rejects_unknown_fields_without_echoing_content():
    response = client.post(
        CONTENT_REPORT_PATH,
        headers={"X-API-Key": "test-api-key"},
        json=report_body(private_prayer="do not echo this"),
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "unknown field: private_prayer"}
    assert "do not echo this" not in response.text


def test_content_report_rejects_invalid_values():
    for field, value in (
        ("content_type", "answer"),
        ("content_text", "   "),
        ("language", "de"),
    ):
        response = client.post(
            CONTENT_REPORT_PATH,
            headers={"X-API-Key": "test-api-key"},
            json=report_body(**{field: value}),
        )

        assert response.status_code == 422


def test_content_report_rejects_oversized_body_before_validation():
    response = client.post(
        CONTENT_REPORT_PATH,
        headers={"X-API-Key": "test-api-key"},
        content=b"{" + b" " * MAX_BODY_BYTES + b"}",
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "request body is too large"}


def test_content_report_fails_loudly_when_storage_is_unavailable(monkeypatch):
    monkeypatch.setattr(content_reports, "create_connection", lambda: None)

    response = client.post(
        CONTENT_REPORT_PATH,
        headers={"X-API-Key": "test-api-key"},
        json=report_body(),
    )

    assert response.status_code == 500
    assert response.json() == {"detail": "Content report storage is unavailable"}


def test_content_report_rolls_back_database_errors(monkeypatch):
    cursor = FakeCursor(error=RuntimeError("database failed"))
    connection = FakeConnection(cursor)
    monkeypatch.setattr(content_reports, "create_connection", lambda: connection)

    response = client.post(
        CONTENT_REPORT_PATH,
        headers={"X-API-Key": "test-api-key"},
        json=report_body(),
    )

    assert response.status_code == 500
    assert response.json() == {"detail": "Failed to store content report"}
    assert connection.rolled_back is True
    assert cursor.closed is True
    assert connection.closed is True


def test_content_report_stats_do_not_store_raw_client_identity():
    assert CONTENT_REPORT_PATH in PRIVATE_PATHS
