"""Tests for the vector-index CLI guards (no database, no network).

The one thing `rebuild` must never do is destroy the stored index because of
a configuration gap: it deletes every embedding whose version differs from the
target one and only then calls the embedding API, so an unusable target
version (or a key that cannot embed) has to stop it before the first
statement.
"""

from types import SimpleNamespace

import index_cli
from vector_index import IndexVersionUnavailable


class RecordingCursor:
    """Cursor double that records every statement it is asked to run."""

    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append((" ".join(sql.split()), params))

    def executemany(self, sql, seq):
        self.statements.append((" ".join(sql.split()), list(seq)))

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class RecordingConnection:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


REBUILD_ARGS = SimpleNamespace(translations=None, force=False, batch_size=50)


def test_rebuild_without_api_key_touches_nothing(monkeypatch, capsys):
    monkeypatch.setattr(index_cli, "GEMINI_API_KEY", "")
    cursor, connection = RecordingCursor(), RecordingConnection()

    assert index_cli.cmd_rebuild(connection, cursor, REBUILD_ARGS) == 1

    assert cursor.statements == []
    assert connection.commits == 0
    err = capsys.readouterr().err
    assert "GEMINI_API_KEY is not configured" in err
    assert "Nothing was changed." in err


def test_rebuild_refuses_an_unusable_index_version(monkeypatch, capsys):
    """Even with a key: no version, no rebuild — and no rows touched."""
    monkeypatch.setattr(index_cli, "GEMINI_API_KEY", "some-key")
    monkeypatch.setattr(
        index_cli, "current_embedding_version",
        lambda: (_ for _ in ()).throw(IndexVersionUnavailable("unset dims")),
    )
    cursor, connection = RecordingCursor(), RecordingConnection()

    assert index_cli.cmd_rebuild(connection, cursor, REBUILD_ARGS) == 1

    assert cursor.statements == []
    assert connection.commits == 0
    assert "Nothing was changed." in capsys.readouterr().err


class FakeEmbeddingClient:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def embed_documents(self, texts):  # pragma: no cover - never reached
        raise AssertionError("the run aborts before embedding")


def test_rebuild_abort_message_does_not_promise_untouched_rows(
    monkeypatch, capsys
):
    """The old message claimed already-stored embeddings were untouched. That
    is false: `reindex_translation` deletes stale-version rows and commits
    before the first embedding call, so a provider outage can leave the index
    smaller than it was."""
    monkeypatch.setattr(index_cli, "GEMINI_API_KEY", "some-key")
    monkeypatch.setattr(
        index_cli, "resolve_translations",
        lambda cursor, spec: [{"code": 1, "alias": "syn"}],
    )
    monkeypatch.setattr(index_cli, "GeminiEmbeddingClient", FakeEmbeddingClient)

    def broken(*args, **kwargs):
        raise index_cli.EmbeddingUnavailable("provider down")

    monkeypatch.setattr(index_cli, "reindex_translation", broken)

    assert index_cli.cmd_rebuild(
        RecordingConnection(), RecordingCursor(), REBUILD_ARGS
    ) == 1

    err = capsys.readouterr().err
    assert "untouched" not in err
    assert "may already have been removed" in err
    assert "re-run to continue" in err
