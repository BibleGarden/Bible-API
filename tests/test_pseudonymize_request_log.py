import hashlib
import hmac

import pytest

import pseudonymize_request_log as migration


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.selected = []
        self.rowcount = 0
        self.closed = False

    def execute(self, sql, params=()):
        if sql.startswith("SELECT COALESCE(MAX"):
            self.selected = [(max(self.rows, default=0),)]
        elif sql.startswith("SELECT id, client_ip"):
            after, maximum, limit = params
            self.selected = [
                (row_id, value)
                for row_id, value in sorted(self.rows.items())
                if after < row_id <= maximum
            ][:limit]
        elif sql.startswith("UPDATE api_requests"):
            pseudonym, row_id, original = params
            self.rowcount = int(self.rows[row_id] == original)
            if self.rowcount:
                self.rows[row_id] = pseudonym
        else:
            raise AssertionError(sql)

    def fetchone(self):
        return self.selected[0]

    def fetchall(self):
        return self.selected

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.cursor_instance = FakeCursor(rows)
        self.commits = 0
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commits += 1

    def rollback(self):
        raise AssertionError("Unexpected rollback")

    def close(self):
        self.closed = True


def test_request_log_conversion_is_dry_run_safe_batched_and_idempotent(monkeypatch):
    already_hashed = "a" * 40
    rows = {1: "203.0.113.7", 2: already_hashed, 3: "2001:db8::7", 4: "unknown"}
    connection = FakeConnection(rows)
    monkeypatch.setattr(migration, "create_connection", lambda: connection)

    assert migration.convert_request_log(dry_run=True, batch_size=2) == (4, 2, 2)
    assert rows[1] == "203.0.113.7"
    assert connection.commits == 0

    assert migration.convert_request_log(dry_run=False, batch_size=2) == (4, 2, 2)
    assert (
        rows[1]
        == hmac.new(b"test-hmac-key", b"203.0.113.7", hashlib.sha256).hexdigest()[:40]
    )
    assert (
        rows[3]
        == hmac.new(b"test-hmac-key", b"2001:db8::7", hashlib.sha256).hexdigest()[:40]
    )
    assert rows[2] == already_hashed
    assert connection.commits == 2
    assert migration.convert_request_log(dry_run=False, batch_size=2) == (4, 0, 4)


def test_request_log_conversion_rejects_invalid_batch_size():
    with pytest.raises(ValueError, match="batch_size must be positive"):
        migration.convert_request_log(dry_run=True, batch_size=0)


def test_request_log_conversion_fails_when_database_is_unavailable(monkeypatch):
    monkeypatch.setattr(migration, "create_connection", lambda: None)
    with pytest.raises(RuntimeError, match="Cannot connect"):
        migration.convert_request_log(dry_run=True)
