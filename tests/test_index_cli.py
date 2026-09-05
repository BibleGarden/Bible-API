"""Tests for the vector-index CLI guards (no database, no network, no model).

The one thing `rebuild` must never do is destroy stored embeddings because of
a configuration gap: it can delete rows (a chunk that vanished, and every
other index version with `--drop-other-versions`) and only then call the
embedder, so an unusable target version — or an embedder that cannot be built
at all — has to stop it before the first statement.
"""

from types import SimpleNamespace

import pytest

import index_cli
from embeddings import EmbeddingUnavailable
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


REBUILD_ARGS = SimpleNamespace(
    translations=None, force=False, batch_size=50, drop_other_versions=False
)


def test_rebuild_without_api_key_touches_nothing(monkeypatch, capsys):
    monkeypatch.setattr(index_cli, "GEMINI_API_KEY", "")
    cursor, connection = RecordingCursor(), RecordingConnection()

    assert index_cli.cmd_rebuild(connection, cursor, REBUILD_ARGS) == 1

    assert cursor.statements == []
    assert connection.commits == 0
    err = capsys.readouterr().err
    assert "GEMINI_API_KEY is not configured" in err
    assert "Nothing was changed." in err


def test_rebuild_on_the_local_provider_needs_no_gemini_key(monkeypatch):
    """The migration's whole point: an index built with no Google
    credentials in the environment (ADR 0010)."""
    monkeypatch.setattr(index_cli, "GEMINI_API_KEY", "")
    monkeypatch.setattr(
        index_cli, "EMBEDDING_PROVIDER", index_cli.EMBEDDING_PROVIDER_LOCAL
    )
    monkeypatch.setattr(
        index_cli, "resolve_translations",
        lambda cursor, spec: [{"code": 1, "alias": "syn"}],
    )
    monkeypatch.setattr(
        index_cli, "build_embedding_client", lambda: FakeEmbeddingClient()
    )
    monkeypatch.setattr(
        index_cli, "reindex_translation",
        lambda *a, **kw: {"embedded": 3, "kept": 0, "deleted": 0},
    )

    assert index_cli.cmd_rebuild(
        RecordingConnection(), RecordingCursor(), REBUILD_ARGS
    ) == 0


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


def test_rebuild_refuses_when_the_embedder_cannot_be_built(monkeypatch, capsys):
    """A missing weights volume is the local provider's "no API key": it
    must stop the run before the first WRITE, not half way through it (the
    translation lookup that precedes it is read-only and stubbed here)."""
    monkeypatch.setattr(
        index_cli, "EMBEDDING_PROVIDER", index_cli.EMBEDDING_PROVIDER_LOCAL
    )
    monkeypatch.setattr(
        index_cli, "resolve_translations",
        lambda cursor, spec: [{"code": 1, "alias": "syn"}],
    )

    def no_model():
        raise EmbeddingUnavailable("cannot load the embedding model")

    monkeypatch.setattr(index_cli, "build_embedding_client", no_model)
    cursor, connection = RecordingCursor(), RecordingConnection()

    assert index_cli.cmd_rebuild(connection, cursor, REBUILD_ARGS) == 1

    assert cursor.statements == []
    assert connection.commits == 0
    err = capsys.readouterr().err
    assert "cannot load the embedding model" in err
    assert "Nothing was changed." in err


def test_rebuild_abort_message_does_not_promise_untouched_rows(
    monkeypatch, capsys
):
    """The old message claimed already-stored embeddings were untouched. That
    is false: `reindex_translation` can delete rows and commit before the
    first embedding call, so a provider outage can leave the index smaller
    than it was."""
    monkeypatch.setattr(index_cli, "GEMINI_API_KEY", "some-key")
    monkeypatch.setattr(
        index_cli, "resolve_translations",
        lambda cursor, spec: [{"code": 1, "alias": "syn"}],
    )
    monkeypatch.setattr(
        index_cli, "build_embedding_client", lambda: FakeEmbeddingClient()
    )

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


@pytest.mark.parametrize("flag", [False, True])
def test_drop_other_versions_reaches_the_reindexer(monkeypatch, capsys, flag):
    """The flag is the only thing standing between a migration and the loss
    of the index that is currently serving traffic — so it is asserted where
    it is used, not where it is parsed."""
    monkeypatch.setattr(index_cli, "GEMINI_API_KEY", "some-key")
    monkeypatch.setattr(
        index_cli, "resolve_translations",
        lambda cursor, spec: [{"code": 1, "alias": "syn"}],
    )
    monkeypatch.setattr(
        index_cli, "build_embedding_client", lambda: FakeEmbeddingClient()
    )
    seen = {}

    def record(connection, cursor, embed, code, **kwargs):
        seen.update(kwargs)
        return {"embedded": 0, "kept": 0, "deleted": 0}

    monkeypatch.setattr(index_cli, "reindex_translation", record)
    args = SimpleNamespace(
        translations=None, force=False, batch_size=50, drop_other_versions=flag
    )

    assert index_cli.cmd_rebuild(
        RecordingConnection(), RecordingCursor(), args
    ) == 0
    assert seen["drop_other_versions"] is flag
    # The destructive run says so before it starts.
    assert ("DELETED" in capsys.readouterr().out) is flag


def test_rebuild_keeps_other_versions_unless_the_flag_is_given():
    """Parsed by the CLI's own parser, so the default cannot drift."""
    parser = index_cli.build_parser()
    assert parser.parse_args(["rebuild"]).drop_other_versions is False
    assert parser.parse_args(
        ["rebuild", "--drop-other-versions"]
    ).drop_other_versions is True


def test_batch_size_default_follows_the_provider():
    """Not cosmetic: 50 vs 512 is a five-hour rebuild vs a two-hour one,
    because sentence-transformers sorts one call's texts by length."""
    parser = index_cli.build_parser()
    assert parser.parse_args(["rebuild"]).batch_size == (
        index_cli.LOCAL_DEFAULT_BATCH_SIZE
        if index_cli.EMBEDDING_PROVIDER == index_cli.EMBEDDING_PROVIDER_LOCAL
        else index_cli.DEFAULT_BATCH_SIZE
    )
    assert parser.parse_args(["rebuild", "--batch-size", "7"]).batch_size == 7
