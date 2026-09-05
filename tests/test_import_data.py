"""
`GET /api/import` — the streamed, per-translation full resync (ClickUp 86cbbq5zp).

Why these tests exist. On 2026-08-30 a full resync stopped answering in
production: `GET /api/data` returned the entire export as one 147 MB JSON
document, and the importer parsed it whole (measured peak RSS of the worker:
972 MB against 82 MB idle). Two things had to change, and both are invariants
rather than numbers, so both are asserted here:

1. the resync never holds the whole corpus — it walks translations one at a
   time, under a size cap that fails loudly BEFORE it writes anything;
2. the resync never empties the database first. The old order was
   `TRUNCATE` every table, then insert; `TRUNCATE` is DDL and commits
   implicitly, so an OOM kill in the insert phase left every table empty with
   nothing to roll back.

The review of that change added two more invariants, asserted here as well:

3. verification is per translation, not only in total — global counts pass on
   compensating errors, and a point import verifies every table it writes
   (it checked 3 of 7, leaving `voice_alignments` unverified);
4. a resync removes no translation without `?allow_removals=1`. A translation
   missing from the manifest is usually an accident on the admin side, and a
   routine resync must not be able to delete a live Bible because of one.

The RAG index joined the import on 2026-09-05 (ClickUp 86cbegwr9), and it
brought invariants of its own — all of them about *when* things happen:

5. a translation's index rows are written in the SAME transaction as its
   text, so production can never hold new text under the index of the old
   one. Proved by injecting a failure into the embedding insert and watching
   the text of that translation roll back with it;
6. the index version this deployment reads is checked against the manifest
   BEFORE the first write: a missing version is a 502 with nothing written,
   never a silently empty index;
7. every index page passes the size valve before the transaction opens, as
   the text export does;
8. rows of another embedding version survive a routine import and are deleted
   only with `?drop_other_index_versions=1` — the rollback of a model
   migration must not evaporate on a resync;
9. what was written is verified against the source (counts, the chunk digest,
   orphaned embeddings) and the in-process index cache is dropped afterwards.

The database is a recording stand-in rather than SQLite or MySQL: what is
under test is the *order* of statements and where the transaction boundaries
fall, which is precisely what a real database hides once the dust settles. The
stand-in also lets a failure be injected at an exact statement, which is how
"an abort at any point keeps the other translations" is proved rather than
asserted.
"""

import base64
import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import import_data
from chunking import CHUNKING_VERSION
from config import EMBEDDING_DIMENSIONS
from main import app
from vector_index import current_embedding_version

client = TestClient(app)
HEADERS = {"X-API-Key": "test-api-key"}

# The index version the test environment reads (conftest pins the model and
# the dimensions): built here rather than hardcoded, so the fixtures follow
# the deployment the same way the importer does.
VERSION = current_embedding_version()
OTHER_VERSION = "c3:some-other-model@512"


# --------------------------------------------------------------------------
# A recording stand-in for the MySQL connection.
# --------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self._rows = []

    def execute(self, sql, params=()):
        flat = ' '.join(sql.split())
        self.connection.log.append(('execute', flat, params))
        self.connection.maybe_fail(sql)
        self._rows = self.connection.answer(sql, params)
        self.rowcount = self.connection.rowcounts.get(_verb(sql), 0)
        # A rowcount for one specific statement, when the verb is too coarse:
        # `?drop_other_index_versions=1` reports what its DELETE removed, and
        # the orphan sweep's DELETEs must not answer that number too.
        for needle, count in self.connection.statement_rowcounts.items():
            if needle in flat:
                self.rowcount = count

    def executemany(self, sql, values):
        self.connection.log.append(
            ('executemany', ' '.join(sql.split()), len(values))
        )
        self.connection.maybe_fail(sql)
        self._rows = []
        self.rowcount = len(values)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


def _verb(sql: str) -> str:
    return sql.strip().split()[0].upper()


class FakeConnection:
    """Records every statement, commit and rollback, and can fail on cue."""

    def __init__(self, counts=None, translations=None, fail_on=None,
                 translation_counts=None, digests=None, orphans=None,
                 statement_rowcounts=None):
        self.log = []
        self.in_transaction = False
        # What `SELECT COUNT(*)` answers, per table.
        self.counts = counts or {}
        # What a translation-SCOPED `SELECT COUNT(*)` answers:
        # {translation_code: {table: n}}. Kept apart from `counts` on purpose —
        # totals that match while a single translation does not is exactly the
        # compensating error the per-translation verification exists to catch.
        self.translation_counts = translation_counts or {}
        # What `SELECT code, alias FROM translations` answers.
        self.translations = translations if translations is not None else []
        # What the chunk-set digest answers, {translation_code: uint64}. A
        # code absent from here has no chunks at all, which MySQL reports as
        # an empty result rather than a NULL row — as the fake does below.
        self.digests = digests or {}
        # Embeddings whose chunk is missing, {translation_code: n}.
        self.orphans = orphans or {}
        # (substring, occurrence) of the statement that must raise.
        self.fail_on = fail_on
        self.seen_fail_candidates = 0
        self.rowcounts = {}
        self.statement_rowcounts = statement_rowcounts or {}
        self.closed = False

    def cursor(self, dictionary=False):
        return FakeCursor(self)

    def maybe_fail(self, sql):
        if not self.fail_on:
            return
        needle, nth = self.fail_on
        if needle in ' '.join(sql.split()):
            self.seen_fail_candidates += 1
            if self.seen_fail_candidates == nth:
                raise RuntimeError("simulated crash mid-import")

    def answer(self, sql, params):
        flat = ' '.join(sql.split())
        if flat.startswith('SELECT translation AS t, BIT_XOR'):
            digest = self.digests.get(params[0])
            # No chunks -> no group -> no row. NULL would be a different
            # answer and the importer must survive both.
            return [] if digest is None else [{'t': params[0], 'digest': digest}]
        if 'LEFT JOIN translation_chunks c' in flat:
            return [{'n': self.orphans.get(params[0], 0)}]
        if flat.startswith('SELECT COUNT(*) AS n FROM `'):
            table = flat.split('`')[1]
            return [{'n': self.counts.get(table, 0)}]
        if flat.startswith('SELECT COUNT(*) AS n FROM'):
            # a translation-scoped check (point import, or the per-translation
            # verification of a resync); the table is the first name after FROM
            table = flat.split('FROM ')[1].split(' ')[0]
            if params:
                scoped = self.translation_counts.get(params[0])
                if scoped is not None:
                    return [{'n': scoped.get(table, 0)}]
            return [{'n': self.counts.get(table, 0)}]
        if flat.startswith('SELECT code, alias FROM translations'):
            return list(self.translations)
        return []

    def start_transaction(self):
        self.log.append(('begin', None, None))
        self.in_transaction = True

    def commit(self):
        self.log.append(('commit', None, None))
        self.in_transaction = False

    def rollback(self):
        self.log.append(('rollback', None, None))
        self.in_transaction = False

    def close(self):
        self.closed = True

    # -- helpers the assertions read ---------------------------------------

    def statements(self):
        return [entry[1] for entry in self.log if entry[0] in ('execute', 'executemany')]

    def commits(self):
        return [i for i, entry in enumerate(self.log) if entry[0] == 'commit']

    def deleted_translation_codes(self):
        return [
            params[0]
            for kind, sql, params in self.log
            if kind == 'execute'
            and sql.startswith('DELETE FROM translations WHERE code =')
        ]

    def inserted_tables(self):
        return [
            sql.split('`')[1]
            for kind, sql, _ in self.log
            if kind == 'executemany' and sql.startswith('INSERT INTO')
        ]


# --------------------------------------------------------------------------
# Fixture data: a miniature admin-api.
# --------------------------------------------------------------------------

LANGUAGES = [{'alias': 'ru', 'name_en': 'Russian', 'name_national': 'Русский'}]
BIBLE_BOOKS = [{'number': 1, 'alias': 'gen'}, {'number': 40, 'alias': 'mat'}]

TRANSLATIONS = [
    {'code': 1, 'alias': 'syn'},
    {'code': 11, 'alias': 'bti'},
    {'code': 21, 'alias': 'npu'},
]


def _payload(code, alias, verses=2):
    return {
        'languages': list(LANGUAGES),
        'bible_books': list(BIBLE_BOOKS),
        'translations': [{'code': code, 'alias': alias, 'language': 'ru'}],
        'translation_books': [{'code': code * 100, 'translation': code, 'book_number': 1}],
        'translation_verses': [
            {'code': code * 1000 + i, 'translation': code, 'text': 'x'}
            for i in range(verses)
        ],
        'translation_titles': [],
        'translation_notes': [],
        'voices': [{'code': code * 10, 'translation': code, 'alias': 'v'}],
        'voice_alignments': [
            {'code': code * 10000, 'voice': code * 10, 'begin': 0.0, 'end': 1.0}
        ],
    }


# The index fixture: two chunks, one Psalm mapping and two embeddings per
# translation. The digests are deliberately above 2^63 — the real ones are
# (syn = 18030424974330788968), and reading them into a signed 64-bit type is
# the mistake this fixture exists to catch.
INDEX_CHUNKS = 2
INDEX_MAPPINGS = 1
INDEX_EMBEDDINGS = 2
DIGESTS = {
    'syn': 18030424974330788968,
    'bti': 9223372036854775809,
    'npu': 42,
}


def _vector_b64(dims=EMBEDDING_DIMENSIONS):
    """A vector as it travels: base64 of dims * 4 bytes of float32."""
    return base64.b64encode(b'\x00' * (dims * 4)).decode('ascii')


def _index_rows(code, alias, chunks=INDEX_CHUNKS, mappings=INDEX_MAPPINGS,
                embeddings=INDEX_EMBEDDINGS, dims=EMBEDDING_DIMENSIONS,
                version=None):
    """What admin-api's index export ships for one translation."""
    version = version or VERSION
    return {
        'translation_chunks': [
            {
                'code': code * 100 + i, 'canonical_id': f'{alias}-{i}',
                'chunking_version': CHUNKING_VERSION, 'translation': code,
                'book_number': 19, 'chapter_number': i + 1,
                'verse_number_start': 1, 'verse_number_end': 2,
                'verse_count': 2, 'title': None, 'text': 'x',
                'char_count': 1, 'created_at': '2026-09-05T10:00:00',
            }
            for i in range(chunks)
        ],
        'psalm_verse_mappings': [
            {
                'code': code * 200 + i, 'mapping_version': 1,
                'translation': code, 'book_number': 19,
                'chapter_number': i + 1, 'verse_number': 1,
                'canonical_chapter': i + 1, 'canonical_verse_start': 1,
                'canonical_verse_end': 1,
                'created_at': '2026-09-05T10:00:00',
            }
            for i in range(mappings)
        ],
        'chunk_embeddings': [
            {
                'code': code * 300 + i, 'canonical_id': f'{alias}-{i}',
                'translation': code, 'embedding_version': version,
                'dims': dims, 'vector': _vector_b64(dims),
                'created_at': '2026-09-05T10:00:00',
            }
            for i in range(embeddings)
        ],
    }


def _index_block(translations=TRANSLATIONS, available=None,
                 chunking_versions=None, per_translation=None,
                 chunks_digest=None, error=None):
    """The `index` block of the manifest, as Dashboard-API builds it."""
    chunking_versions = (
        [CHUNKING_VERSION] if chunking_versions is None else chunking_versions
    )
    available = [OTHER_VERSION, VERSION] if available is None else available
    if per_translation is None:
        per_translation = {
            entry['alias']: {
                'translation_chunks': INDEX_CHUNKS,
                'psalm_verse_mappings': INDEX_MAPPINGS,
                'chunk_embeddings': {v: INDEX_EMBEDDINGS for v in available},
            }
            for entry in translations
        }
    if chunks_digest is None:
        chunks_digest = {
            entry['alias']: DIGESTS[entry['alias']] for entry in translations
        }
    return {
        'chunking_version': (
            chunking_versions[0] if len(chunking_versions) == 1 else None
        ),
        'chunking_versions': chunking_versions,
        'mapping_version': 1,
        'mapping_versions': [1],
        'available_versions': available,
        'counts': {'per_translation': per_translation},
        'chunks_digest': chunks_digest,
        'error': error,
    }


_NO_INDEX_BLOCK = object()


def _manifest(translations=TRANSLATIONS, totals=None, per_translation=None,
              index=None):
    computed = {
        'languages': len(LANGUAGES),
        'bible_books': len(BIBLE_BOOKS),
        'translations': len(translations),
        'translation_books': len(translations),
        'translation_verses': 2 * len(translations),
        'translation_titles': 0,
        'translation_notes': 0,
        'voices': len(translations),
        'voice_alignments': len(translations),
    }
    if per_translation is None:
        # What `_payload` ships for one translation.
        per_translation = {
            entry['alias']: {
                'translations': 1,
                'translation_books': 1,
                'translation_verses': 2,
                'translation_titles': 0,
                'translation_notes': 0,
                'voices': 1,
                'voice_alignments': 1,
            }
            for entry in translations
        }
    manifest = {
        'languages': list(LANGUAGES),
        'bible_books': list(BIBLE_BOOKS),
        'translations': list(translations),
        'counts': {
            'per_translation': per_translation,
            'totals': totals if totals is not None else computed,
        },
    }
    if index is not _NO_INDEX_BLOCK:
        manifest['index'] = index if index is not None else _index_block(translations)
    return manifest


def _matching_counts(manifest):
    """Row counts that make the post-import verification pass."""
    return dict(manifest['counts']['totals'])


def _matching_translation_counts(manifest):
    """Per-translation row counts that make the per-translation check pass.

    The index tables are in here as well: they are counted per translation
    exactly as the text tables are, and the version-scoped embedding count is
    keyed by the same table name (the fake answers `SELECT COUNT(*)` by table,
    and the importer only ever asks for the version it imported).
    """
    per = manifest['counts']['per_translation']
    index_per = ((manifest.get('index') or {}).get('counts') or {}).get(
        'per_translation'
    ) or {}
    counts = {}
    for entry in manifest['translations']:
        alias, code = entry['alias'], entry['code']
        row = dict(per[alias])
        index_row = index_per.get(alias) or {}
        row['translation_chunks'] = index_row.get('translation_chunks', 0)
        row['psalm_verse_mappings'] = index_row.get('psalm_verse_mappings', 0)
        row['chunk_embeddings'] = (
            index_row.get('chunk_embeddings') or {}
        ).get(VERSION, 0)
        counts[code] = row
    return counts


def _matching_digests(manifest):
    """Chunk digests that agree with the manifest, keyed by translation code."""
    declared = (manifest.get('index') or {}).get('chunks_digest') or {}
    return {
        entry['code']: declared.get(entry['alias'])
        for entry in manifest['translations']
    }


def _fake(manifest, translations=TRANSLATIONS, **kwargs):
    """A stand-in cep_public that agrees with the manifest in every table."""
    kwargs.setdefault('counts', _matching_counts(manifest))
    kwargs.setdefault('translation_counts', _matching_translation_counts(manifest))
    kwargs.setdefault('digests', _matching_digests(manifest))
    return FakeConnection(translations=translations, **kwargs)


class FakeAdmin:
    """Answers `_fetch_json` for the manifest, the exports and the index.

    The index is paged the way admin-api pages it — a small first page that
    also carries the whole chunk corpus, larger ones afterwards — so every
    test that imports anything walks a multi-page index rather than the happy
    single-page case.
    """

    #: Deliberately smaller than the fixture's embedding count, so the walk
    #: always spans more than one page.
    FIRST_PAGE = 1
    LATER_PAGES = 2

    def __init__(self, manifest=None, payloads=None, fail_alias=None,
                 index_rows=None):
        self.manifest = manifest if manifest is not None else _manifest()
        self.payloads = payloads
        self.fail_alias = fail_alias
        # {alias: the dict `_index_rows` returns} for translations that need
        # something other than the default fixture.
        self.index_rows = index_rows or {}
        self.requests = []

    def _code(self, alias):
        return next(
            t['code'] for t in self.manifest['translations'] if t['alias'] == alias
        )

    def _index(self, alias):
        if alias in self.index_rows:
            return self.index_rows[alias]
        return _index_rows(self._code(alias), alias)

    def index_page(self, alias, offset):
        rows = self._index(alias)
        embeddings = rows['chunk_embeddings']
        applied = self.FIRST_PAGE if offset == 0 else self.LATER_PAGES
        window = embeddings[offset:offset + applied]
        next_offset = offset + applied
        return {
            'translation': alias,
            'translation_code': self._code(alias),
            'embedding_version': VERSION,
            'limit_applied': applied,
            'offset': offset,
            'chunk_embeddings_total': len(embeddings),
            'translation_chunks': (
                rows['translation_chunks'] if offset == 0 else None
            ),
            'psalm_verse_mappings': (
                rows['psalm_verse_mappings'] if offset == 0 else None
            ),
            'chunk_embeddings': window,
            'next_offset': (
                next_offset if next_offset < len(embeddings) else None
            ),
        }

    def __call__(self, path, params=None):
        params = params or {}
        self.requests.append((path, params.get('translation')))
        if path == '/api/data/manifest':
            return self.manifest
        alias = params.get('translation')
        if alias is None:
            raise AssertionError(
                "the full resync must never request the whole export"
            )
        if path == '/api/data/index':
            return self.index_page(alias, int(params.get('offset') or 0))
        if self.payloads is not None and alias in self.payloads:
            return self.payloads[alias]
        return _payload(self._code(alias), alias)


def _run(connection, admin, url='/api/import'):
    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json', side_effect=admin):
        return client.get(url, headers=HEADERS)


# --------------------------------------------------------------------------
# The two properties the incident was about.
# --------------------------------------------------------------------------

def test_full_resync_never_truncates():
    """No TRUNCATE anywhere. It is DDL: it commits, and cannot be rolled back."""
    manifest = _manifest()
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 200, response.text
    assert not any(
        'TRUNCATE' in statement.upper() for statement in connection.statements()
    )


def test_full_resync_never_requests_the_whole_export():
    """The 147 MB document is never asked for — that was the OOM."""
    admin = FakeAdmin()
    manifest = admin.manifest
    connection = _fake(manifest)
    response = _run(connection, admin)

    assert response.status_code == 200, response.text
    assert admin.requests[0] == ('/api/data/manifest', None)
    exports = [
        alias for path, alias in admin.requests if path == '/api/data'
    ]
    assert exports == ['syn', 'bti', 'npu']
    assert all(alias is not None for _, alias in admin.requests[1:])


def test_each_translation_is_committed_on_its_own():
    """One transaction per translation, so an abort cannot span two."""
    manifest = _manifest()
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))
    assert response.status_code == 200, response.text

    # reference tables + 3 translations + the local-translations read
    # + the orphan sweep; every one of them closed by a commit.
    kinds = [entry[0] for entry in connection.log]
    assert kinds.count('commit') >= 6
    # No two `DELETE FROM translations WHERE code` in the same transaction.
    depth = 0
    deletes_in_transaction = 0
    for kind, sql, _ in connection.log:
        if kind == 'begin':
            depth, deletes_in_transaction = 1, 0
        elif kind == 'commit':
            depth = 0
        elif kind == 'execute' and sql.startswith('DELETE FROM translations WHERE code'):
            deletes_in_transaction += 1
            assert depth == 1, "destructive statement outside a transaction"
            assert deletes_in_transaction == 1


def test_crash_mid_import_keeps_the_translations_already_written():
    """The acceptance property: an abort never leaves the database empty.

    The crash is injected into the SECOND translation's insert phase. The
    first translation must be committed, the second rolled back, and the
    third never touched.
    """
    manifest = _manifest()
    connection = _fake(manifest, fail_on=('INSERT INTO `translation_verses`', 2))
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 500
    assert 'simulated crash' in response.json()['detail']

    deleted = connection.deleted_translation_codes()
    # syn was replaced, bti's replacement was rolled back, npu untouched.
    assert deleted == [1, 11]
    assert 21 not in deleted

    kinds = [entry[0] for entry in connection.log]
    # The reference phase and syn are committed before the crash...
    assert kinds.index('rollback') > kinds.index('commit')
    assert kinds.count('commit') == 2
    # ...and the failed translation is rolled back, not left open.
    assert 'rollback' in kinds


def test_crash_before_any_write_leaves_everything_alone():
    """A manifest that cannot be fetched must not start the destructive phase."""
    connection = FakeConnection()

    def broken(path, params=None):
        from fastapi import HTTPException
        raise HTTPException(status_code=502, detail="admin-api is down")

    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json', side_effect=broken):
        response = client.get('/api/import', headers=HEADERS)

    assert response.status_code == 502
    assert not any(
        statement.startswith(('DELETE', 'INSERT', 'REPLACE', 'TRUNCATE'))
        for statement in connection.statements()
    )


def test_empty_work_list_is_refused():
    """An empty source is a broken source, never "publish an empty Bible"."""
    manifest = _manifest(translations=[])
    connection = FakeConnection(translations=TRANSLATIONS)
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 502
    assert 'no active translations' in response.json()['detail']
    assert not any(
        statement.startswith(('DELETE', 'INSERT', 'REPLACE', 'TRUNCATE'))
        for statement in connection.statements()
    )


# --------------------------------------------------------------------------
# The size valve.
# --------------------------------------------------------------------------

class _FakeStream:
    def __init__(self, body, status=200, content_length=None, headers=None):
        self.body = body
        self.status_code = status
        self.headers = dict(headers or {})
        if content_length is not None:
            self.headers['content-length'] = str(content_length)
        self.text = body.decode('utf-8', 'replace')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self.body

    def iter_bytes(self, size):
        for i in range(0, len(self.body), size):
            yield self.body[i:i + size]


class _FakeHttpClient:
    def __init__(self, stream):
        self._stream = stream

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def stream(self, method, url, params=None, headers=None):
        return self._stream


def _with_body(body, **kwargs):
    stream = _FakeStream(body, **kwargs)
    return patch.object(
        import_data.httpx, 'Client', lambda *a, **kw: _FakeHttpClient(stream)
    )


def test_size_valve_trips_on_declared_content_length():
    huge = 200 * 1024 * 1024
    with _with_body(b'{}', content_length=huge):
        with pytest.raises(Exception) as excinfo:
            import_data._fetch_json('/api/data', {'translation': 'syn'})
    assert excinfo.value.status_code == 507
    assert 'IMPORT_MAX_PAYLOAD_MB' in excinfo.value.detail


def test_size_valve_trips_on_a_chunked_body_that_lies():
    """No Content-Length is not a way past the cap."""
    body = b'[' + b'0' * (import_data.IMPORT_MAX_PAYLOAD_MB * 1024 * 1024 + 10)
    with _with_body(body):
        with pytest.raises(Exception) as excinfo:
            import_data._fetch_json('/api/data', {'translation': 'syn'})
    assert excinfo.value.status_code == 507


def test_size_valve_lets_a_normal_payload_through():
    body = json.dumps({'translations': [{'code': 1, 'alias': 'syn'}]}).encode()
    with _with_body(body, content_length=len(body)):
        parsed = import_data._fetch_json('/api/data', {'translation': 'syn'})
    assert parsed['translations'][0]['alias'] == 'syn'


def test_oversized_translation_is_refused_before_it_is_deleted():
    """The valve is only worth anything if it trips before the DELETE."""
    manifest = _manifest()
    connection = _fake(manifest)

    def admin(path, params=None):
        params = params or {}
        if path == '/api/data/manifest':
            return manifest
        if params.get('translation') == 'bti':
            raise import_data._oversized('/api/data', 200 * 1048576, 1)
        code = next(
            t['code'] for t in manifest['translations']
            if t['alias'] == params.get('translation')
        )
        return _payload(code, params['translation'])

    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json', side_effect=admin):
        response = client.get('/api/import', headers=HEADERS)

    assert response.status_code == 507
    # syn was replaced; bti was never deleted; npu was never reached.
    assert connection.deleted_translation_codes() == [1]


# --------------------------------------------------------------------------
# Reporting.
# --------------------------------------------------------------------------

def test_report_counts_and_verification():
    manifest = _manifest()
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))
    body = response.json()

    assert body['status'] == 'ok'
    assert body['translation'] is None
    assert body['translations_imported'] == ['syn', 'bti', 'npu']
    assert body['translations_removed'] == []
    assert body['tables']['translation_verses'] == 6
    assert body['tables']['languages'] == 1
    assert body['verification']['translation_verses'] == {
        'expected': 6, 'actual': 6, 'ok': True
    }
    assert body['duration_seconds'] >= 0


def test_count_mismatch_is_reported_not_swallowed():
    manifest = _manifest()
    counts = _matching_counts(manifest)
    counts['translation_verses'] = 5  # one verse short of what admin declared
    connection = _fake(manifest, counts=counts)
    response = _run(connection, FakeAdmin(manifest))
    body = response.json()

    assert response.status_code == 200
    assert body['status'] == 'mismatch'
    assert body['verification']['translation_verses'] == {
        'expected': 6, 'actual': 5, 'ok': False
    }


def test_translations_admin_no_longer_publishes_are_dropped():
    """What TRUNCATE used to do implicitly is now an explicit, reported step."""
    manifest = _manifest(translations=TRANSLATIONS[:2])
    connection = _fake(
        manifest,
        # cep_public still holds npu, which admin-api no longer publishes.
        translations=[
            {'code': 1, 'alias': 'syn'},
            {'code': 11, 'alias': 'bti'},
            {'code': 21, 'alias': 'npu'},
        ],
    )
    # The removal gate: without the flag nothing would be dropped (asserted in
    # its own test below), so this one states the intent explicitly.
    response = _run(connection, FakeAdmin(manifest),
                    url='/api/import?allow_removals=1')
    body = response.json()

    assert body['translations_imported'] == ['syn', 'bti']
    assert body['translations_removed'] == ['npu']
    assert 21 in connection.deleted_translation_codes()


def test_a_removed_translation_takes_its_index_with_it():
    """The index rides with the text on the way OUT too (review 2026-09-05).

    A translation dropped from cep_public whose chunks, Psalm map and vectors
    stayed behind would leave rows nothing owns: no import writes them again
    (the translation is gone from the manifest), `index_cli` rebuilds only
    what `translations` lists, and the orphan sweep does not reach the index
    tables. Every embedding version goes — `drop_other_index_versions`
    protects the rollback of a model migration for a translation that stays,
    and this one has no text to roll back to.
    """
    manifest = _manifest(translations=TRANSLATIONS[:2])
    connection = _fake(
        manifest,
        translations=[
            {'code': 1, 'alias': 'syn'},
            {'code': 11, 'alias': 'bti'},
            {'code': 21, 'alias': 'npu'},
        ],
    )
    assert _run(connection, FakeAdmin(manifest),
                url='/api/import?allow_removals=1').status_code == 200

    # The removal transaction — identified by the only statement unique to it
    # (no import drops another version without the flag): the text delete and
    # all three index deletes are in it, together.
    block = _block_with(connection, 'embedding_version <> %s')
    for needle in (
        'DELETE FROM translations WHERE code = %s',
        'DELETE FROM translation_chunks WHERE translation = %s',
        'DELETE FROM psalm_verse_mappings WHERE translation = %s',
        'DELETE FROM chunk_embeddings WHERE translation = %s AND '
        'embedding_version = %s',
        'DELETE FROM chunk_embeddings WHERE translation = %s AND '
        'embedding_version <> %s',
    ):
        assert any(needle in s for s in block), needle
    # ...and it is npu's index, not somebody else's.
    removed_index_codes = {
        params[0]
        for kind, sql, params in connection.log
        if kind == 'execute'
        and sql.startswith('DELETE FROM chunk_embeddings WHERE translation = %s '
                           'AND embedding_version <> %s')
    }
    assert removed_index_codes == {21}


def test_a_compensating_error_between_translations_is_caught():
    """Totals are not a verification (review MINOR-2).

    cep_public here holds one verse too many in `syn` and one too few in
    `bti`: every global total matches the manifest exactly, and yet a
    translation lost a verse. Before the per-translation check this resync
    reported status "ok".
    """
    manifest = _manifest()
    scoped = _matching_translation_counts(manifest)
    scoped[1]['translation_verses'] = 3   # syn: one too many
    scoped[11]['translation_verses'] = 1  # bti: one too few
    connection = _fake(manifest, translation_counts=scoped)
    response = _run(connection, FakeAdmin(manifest))
    body = response.json()

    # The totals still add up to 6 — that is the point of the test.
    assert body['verification']['translation_verses'] == {
        'expected': 6, 'actual': 6, 'ok': True
    }
    assert body['status'] == 'mismatch'
    assert body['translation_mismatches'] == {
        'syn': {'translation_verses': {'expected': 2, 'actual': 3, 'ok': False}},
        'bti': {'translation_verses': {'expected': 2, 'actual': 1, 'ok': False}},
    }


def test_every_translation_is_verified_in_every_table_it_owns():
    """A matching resync reports no per-translation disagreement at all."""
    manifest = _manifest()
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))

    assert response.json()['translation_mismatches'] == {}
    # voice_alignments is the table the manual fixes ride in on; it must be
    # among the ones checked per translation, not only in the global total.
    scoped_counts = [
        sql for kind, sql, params in connection.log
        if kind == 'execute'
        and sql.startswith('SELECT COUNT(*) AS n FROM voice_alignments va')
    ]
    assert len(scoped_counts) == len(TRANSLATIONS)


def test_a_manifest_that_cannot_count_a_translation_is_refused():
    """An unverifiable translation is a broken source, caught before writing."""
    manifest = _manifest()
    connection = _fake(manifest)
    del manifest['counts']['per_translation']['bti']
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 502
    assert 'bti' in response.json()['detail']
    assert not any(
        statement.startswith(('DELETE', 'INSERT', 'REPLACE'))
        for statement in connection.statements()
    )


def test_manifest_without_per_translation_counts_names_it():
    connection = FakeConnection()
    stale = {
        'languages': [], 'bible_books': [], 'translations': [],
        'counts': {'totals': {}},
    }

    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json', side_effect=lambda p, q=None: stale):
        response = client.get('/api/import', headers=HEADERS)

    assert response.status_code == 502
    assert "'counts.per_translation'" in response.json()['detail']


def test_manifest_without_the_new_keys_names_the_missing_one():
    """Bible-API deployed ahead of Dashboard-API must say so, not guess."""
    connection = FakeConnection()
    stale = {'languages': [], 'bible_books': [], 'translations': []}

    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json', side_effect=lambda p, q=None: stale):
        response = client.get('/api/import', headers=HEADERS)

    assert response.status_code == 502
    assert "'counts'" in response.json()['detail']
    assert '86cbbq5zp' in response.json()['detail']


# --------------------------------------------------------------------------
# The removal gate: dropping a translation takes an explicit flag.
# --------------------------------------------------------------------------

def _resync_that_would_drop_npu():
    """A manifest of two translations against a cep_public that holds three."""
    manifest = _manifest(translations=TRANSLATIONS[:2])
    counts = _matching_counts(manifest)
    # cep_public still holds npu's rows, so the totals are over the manifest
    # by exactly one translation's worth of data.
    for table, extra in (
        ('translations', 1), ('translation_books', 1),
        ('translation_verses', 2), ('voices', 1), ('voice_alignments', 1),
    ):
        counts[table] += extra
    connection = _fake(
        manifest,
        counts=counts,
        translations=[
            {'code': 1, 'alias': 'syn'},
            {'code': 11, 'alias': 'bti'},
            {'code': 21, 'alias': 'npu'},
        ],
    )
    return manifest, connection


def test_resync_refuses_to_drop_a_translation_without_the_flag():
    """The gate. A live translation must not vanish on a routine resync.

    A translation missing from the manifest is far more often an accident on
    the admin side (`active = 0` by mistake, a bad filter) than an instruction
    to delete a published Bible, so the resync removes nothing and says what
    it would have removed.
    """
    manifest, connection = _resync_that_would_drop_npu()
    response = _run(connection, FakeAdmin(manifest))
    body = response.json()

    assert response.status_code == 200, response.text
    assert body['status'] == 'removals_rejected'
    assert body['removals_rejected'] == ['npu']
    assert body['translations_removed'] == []
    # Nothing of npu's was touched — the refusal precedes every delete.
    assert 21 not in connection.deleted_translation_codes()
    # ...while the import itself went through, which is why this is a report
    # and not an exception.
    assert body['translations_imported'] == ['syn', 'bti']
    assert body['tables']['translation_verses'] == 4
    # The operator is told which translations and how to proceed.
    assert 'npu' in body['detail']
    assert 'allow_removals=1' in body['detail']


def test_rejected_removals_outrank_the_count_mismatch_they_cause():
    """The leftover translation makes the totals disagree; say why."""
    manifest, connection = _resync_that_would_drop_npu()
    body = _run(connection, FakeAdmin(manifest)).json()

    assert body['verification']['translation_verses']['ok'] is False
    assert body['status'] == 'removals_rejected'


def test_a_resync_with_nothing_to_remove_is_ok_without_the_flag():
    """The gate is silent on the normal case: no removals, no flag needed."""
    manifest = _manifest()
    body = _run(_fake(manifest), FakeAdmin(manifest)).json()

    assert body['status'] == 'ok'
    assert body['removals_rejected'] == []
    assert body['detail'] is None


# --------------------------------------------------------------------------
# The RAG index travels with the text (ClickUp 86cbegwr9).
# --------------------------------------------------------------------------

def _transactions(connection):
    """The statements of each committed or rolled-back transaction, in order."""
    blocks, current = [], None
    for kind, sql, _ in connection.log:
        if kind == 'begin':
            current = []
        elif kind in ('commit', 'rollback'):
            if current is not None:
                blocks.append(current)
            current = None
        elif kind in ('execute', 'executemany') and current is not None:
            current.append(sql)
    return blocks


def _blocks_with(connection, needle):
    """Every transaction that contains this statement (one per translation)."""
    found = [b for b in _transactions(connection) if any(needle in s for s in b)]
    assert found, f"no transaction contains {needle!r}"
    return found


def _block_with(connection, needle):
    """The first transaction that contains this statement."""
    return _blocks_with(connection, needle)[0]


def test_the_index_is_written_in_the_translations_own_transaction():
    """The property the whole ticket is about.

    Production must never hold a translation's new text under the index of
    its old text. One transaction per translation, carrying both, is what
    makes that impossible — not an ordering convention, not a second request.
    """
    manifest = _manifest()
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))
    assert response.status_code == 200, response.text

    blocks = _blocks_with(connection, 'INSERT INTO `chunk_embeddings`')
    assert len(blocks) == len(TRANSLATIONS), "one transaction per translation"
    block = blocks[0]
    # Same transaction: this translation's text, its chunks, its Psalm map,
    # its embeddings, and the deletes that precede them.
    for needle in (
        'DELETE FROM translation_verses WHERE translation',
        'INSERT INTO `translation_verses`',
        'DELETE FROM translation_chunks WHERE translation',
        'INSERT INTO `translation_chunks`',
        'DELETE FROM psalm_verse_mappings WHERE translation',
        'INSERT INTO `psalm_verse_mappings`',
        'DELETE FROM chunk_embeddings WHERE translation = %s AND embedding_version',
    ):
        assert any(needle in s for s in block), needle


def test_a_failed_embedding_insert_rolls_back_the_text_of_that_translation():
    """Atomicity, from the other side: the text does not survive alone.

    The failure is injected into the FIRST translation's embedding insert.
    Its verses were already inserted in the same transaction, so they must go
    down with it — leaving syn exactly as it was, old text and old index.
    """
    manifest = _manifest()
    connection = _fake(manifest, fail_on=('INSERT INTO `chunk_embeddings`', 1))
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 500
    assert 'simulated crash' in response.json()['detail']

    kinds = [entry[0] for entry in connection.log]
    # Only the reference-table transaction was committed; syn's was not.
    assert kinds.count('commit') == 1
    assert kinds[-1] == 'rollback' or 'rollback' in kinds
    rolled_back = _block_with(connection, 'INSERT INTO `translation_verses`')
    assert any('INSERT INTO `translation_chunks`' in s for s in rolled_back)
    # bti and npu were never reached.
    assert connection.deleted_translation_codes() == [1]


def test_the_ddl_runs_outside_every_transaction():
    """`CREATE TABLE` commits implicitly — inside a transaction it would split
    a translation's write in half."""
    manifest = _manifest()
    connection = _fake(manifest)
    assert _run(connection, FakeAdmin(manifest)).status_code == 200

    depth = 0
    seen = 0
    for kind, sql, _ in connection.log:
        if kind == 'begin':
            depth = 1
        elif kind in ('commit', 'rollback'):
            depth = 0
        elif kind == 'execute' and sql.startswith('CREATE TABLE IF NOT EXISTS'):
            seen += 1
            assert depth == 0, "DDL inside a transaction"
    assert seen == len(import_data.INDEX_TABLES)


def test_an_unavailable_embedding_version_is_refused_before_any_write():
    """The 502 that names everything the operator needs to act.

    An index of another version is not "close enough": the vectors this
    service reads are addressed by version, so importing what happens to be
    there would produce an index that reads as empty — indistinguishable from
    a translation nobody has indexed.
    """
    manifest = _manifest(index=_index_block(available=[OTHER_VERSION]))
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 502
    detail = response.json()['detail']
    assert VERSION in detail and OTHER_VERSION in detail
    assert 'EMBEDDING_MODEL' in detail and 'EMBEDDING_DIMENSIONS' in detail
    assert 'Nothing was written' in detail
    assert not any(
        statement.startswith(('DELETE', 'INSERT', 'REPLACE'))
        for statement in connection.statements()
    )


def test_a_manifest_without_an_index_block_names_it():
    """Bible-API deployed ahead of Dashboard-API: say so, do not import blind."""
    manifest = _manifest(index=_NO_INDEX_BLOCK)
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 502
    detail = response.json()['detail']
    assert "'index'" in detail and '86cbegwqg' in detail
    assert not any(
        statement.startswith(('DELETE', 'INSERT', 'REPLACE'))
        for statement in connection.statements()
    )


def test_an_index_the_source_cannot_read_is_refused_with_its_own_words():
    manifest = _manifest(
        index=_index_block(error="RAG index database 'cep_public' is not readable")
    )
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 502
    assert 'not readable' in response.json()['detail']


def test_a_foreign_chunking_version_is_refused():
    """canonical_id carries the chunking version: another version is another
    corpus, not an older copy of this one."""
    manifest = _manifest(index=_index_block(chunking_versions=[CHUNKING_VERSION + 1]))
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 502
    detail = response.json()['detail']
    assert str(CHUNKING_VERSION) in detail
    assert 'Nothing was written' in detail


def test_several_chunking_versions_at_the_source_are_refused():
    """A half-finished rechunk at the source is a mixed corpus, and the
    manifest reports that as `chunking_version: null` rather than a number."""
    manifest = _manifest(
        index=_index_block(chunking_versions=[CHUNKING_VERSION, CHUNKING_VERSION + 1])
    )
    connection = _fake(manifest)
    response = _run(connection, FakeAdmin(manifest))

    assert response.status_code == 502
    assert 'single chunking version' in response.json()['detail']


def test_other_embedding_versions_survive_a_routine_import():
    """The rollback of a model migration must not evaporate on a resync."""
    manifest = _manifest()
    connection = _fake(manifest)
    body = _run(connection, FakeAdmin(manifest)).json()

    assert body['index']['drop_other_index_versions'] is False
    assert body['index']['other_versions_removed'] == {}
    # Every embedding DELETE names the version being replaced; none of them
    # touches another version's rows.
    deletes = [
        sql for sql in connection.statements()
        if sql.startswith('DELETE FROM chunk_embeddings')
    ]
    assert deletes
    assert all('embedding_version = %s' in sql for sql in deletes)
    assert not any('embedding_version <> %s' in sql for sql in deletes)


def test_drop_other_index_versions_removes_them_and_says_how_many():
    manifest = _manifest()
    connection = _fake(
        manifest,
        statement_rowcounts={'DELETE FROM chunk_embeddings WHERE translation = %s '
                             'AND embedding_version <> %s': 7},
    )
    body = _run(connection, FakeAdmin(manifest),
                url='/api/import?drop_other_index_versions=1').json()

    assert body['index']['drop_other_index_versions'] is True
    assert body['index']['other_versions_removed'] == {'syn': 7, 'bti': 7, 'npu': 7}
    assert any(
        'embedding_version <> %s' in sql for sql in connection.statements()
    )


def test_an_oversized_index_page_is_refused_before_the_transaction():
    """The valve covers the index too, and it trips before the DELETE."""
    manifest = _manifest()
    connection = _fake(manifest)

    def admin(path, params=None):
        params = params or {}
        if path == '/api/data/manifest':
            return manifest
        if path == '/api/data/index' and params.get('translation') == 'bti':
            raise import_data._oversized('/api/data/index', 200 * 1048576, 1)
        if path == '/api/data/index':
            return FakeAdmin(manifest).index_page(
                params['translation'], int(params.get('offset') or 0)
            )
        code = next(
            t['code'] for t in manifest['translations']
            if t['alias'] == params.get('translation')
        )
        return _payload(code, params['translation'])

    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json', side_effect=admin):
        response = client.get('/api/import', headers=HEADERS)

    assert response.status_code == 507
    # syn was replaced; bti kept both its text and its index.
    assert connection.deleted_translation_codes() == [1]


def test_the_index_walk_follows_next_offset_to_the_last_row():
    """The fake pages one embedding first and two afterwards, exactly as
    admin-api clips its first page — every row must still arrive."""
    manifest = _manifest()
    connection = _fake(manifest)
    admin = FakeAdmin(manifest)
    body = _run(connection, admin).json()

    assert body['status'] == 'ok'
    assert body['index']['tables'] == {
        'translation_chunks': INDEX_CHUNKS * len(TRANSLATIONS),
        'psalm_verse_mappings': INDEX_MAPPINGS * len(TRANSLATIONS),
        'chunk_embeddings': INDEX_EMBEDDINGS * len(TRANSLATIONS),
    }
    assert body['index']['translations_indexed'] == ['syn', 'bti', 'npu']
    assert body['index']['embedding_version'] == VERSION
    assert body['index']['chunking_version'] == CHUNKING_VERSION
    # More than one index page per translation.
    index_requests = [p for p, _ in admin.requests if p == '/api/data/index']
    assert len(index_requests) == 2 * len(TRANSLATIONS)


def test_an_incomplete_index_export_is_refused():
    """The source says how many embeddings the walk covers; delivering fewer
    would write a silently partial index."""
    manifest = _manifest()
    connection = _fake(manifest)
    admin = FakeAdmin(manifest)
    real_page = admin.index_page

    def short_page(alias, offset):
        page = real_page(alias, offset)
        page['chunk_embeddings_total'] = INDEX_EMBEDDINGS + 5
        return page

    admin.index_page = short_page
    response = _run(connection, admin)

    assert response.status_code == 502
    assert 'delivered' in response.json()['detail']
    # The reference tables are written before the first translation, as they
    # always were; what matters is that no translation was touched.
    assert connection.deleted_translation_codes() == []


def test_a_vector_of_the_wrong_width_is_refused_before_the_transaction():
    """`load_index` stacks every vector into one matrix: a single short one
    breaks the whole index at read time, far from here."""
    manifest = _manifest()
    connection = _fake(manifest)
    broken = _index_rows(1, 'syn')
    broken['chunk_embeddings'][0]['vector'] = base64.b64encode(b'\x00' * 8).decode()
    response = _run(connection, FakeAdmin(manifest, index_rows={'syn': broken}))

    assert response.status_code == 502
    assert 'bytes of vector' in response.json()['detail']
    # syn keeps everything it had: the check runs before its transaction.
    assert connection.deleted_translation_codes() == []


def test_a_vector_of_another_dimensionality_is_refused():
    manifest = _manifest()
    connection = _fake(manifest)
    broken = _index_rows(1, 'syn', dims=EMBEDDING_DIMENSIONS // 2)
    response = _run(connection, FakeAdmin(manifest, index_rows={'syn': broken}))

    assert response.status_code == 502
    assert 'EMBEDDING_DIMENSIONS' in response.json()['detail']


def test_an_undecodable_vector_is_refused():
    manifest = _manifest()
    connection = _fake(manifest)
    broken = _index_rows(1, 'syn')
    broken['chunk_embeddings'][0]['vector'] = 'not base64 at all !!'
    response = _run(connection, FakeAdmin(manifest, index_rows={'syn': broken}))

    assert response.status_code == 502
    assert 'undecodable vector' in response.json()['detail']


def test_index_counts_are_verified_per_translation():
    """One chunk short in one translation, against the manifest's own count."""
    manifest = _manifest()
    scoped = _matching_translation_counts(manifest)
    scoped[11]['translation_chunks'] = INDEX_CHUNKS - 1
    connection = _fake(manifest, translation_counts=scoped)
    body = _run(connection, FakeAdmin(manifest)).json()

    assert body['status'] == 'mismatch'
    assert body['translation_mismatches']['bti']['translation_chunks'] == {
        'expected': INDEX_CHUNKS, 'actual': INDEX_CHUNKS - 1, 'ok': False
    }


def test_a_disagreeing_chunk_digest_is_reported_and_never_signed():
    """The digest is an unsigned 64-bit number and routinely exceeds 2^63.

    Equal counts with different content is exactly what it catches: the same
    number of chunks, one of them holding different text.
    """
    manifest = _manifest()
    digests = _matching_digests(manifest)
    digests[1] = 18446744073709551615  # 2^64 - 1: still unsigned, still exact
    connection = _fake(manifest, digests=digests)
    body = _run(connection, FakeAdmin(manifest)).json()

    assert body['status'] == 'mismatch'
    check = body['translation_mismatches']['syn']['chunks_digest']
    assert check['expected'] == DIGESTS['syn'] > 2 ** 63
    assert check['actual'] == 18446744073709551615
    assert check['ok'] is False


def test_an_absent_digest_is_reported_as_null_not_as_zero():
    """"No chunks at all" and "a digest that happens to be 0" are different
    facts (review 2026-09-05).

    Here the source declares a chunk set for syn and this database holds none,
    so our side has no digest to report. `0` would say "a digest of zero",
    which a total XOR cancellation could genuinely produce; `null` says what
    is true.
    """
    manifest = _manifest()
    digests = _matching_digests(manifest)
    digests[1] = None  # syn: no chunks here at all
    scoped = _matching_translation_counts(manifest)
    scoped[1]['translation_chunks'] = 0
    connection = _fake(manifest, digests=digests, translation_counts=scoped)
    body = _run(connection, FakeAdmin(manifest)).json()

    assert body['status'] == 'mismatch'
    check = body['translation_mismatches']['syn']['chunks_digest']
    assert check == {'expected': DIGESTS['syn'], 'actual': None, 'ok': False}


def test_orphan_embeddings_are_reported():
    manifest = _manifest()
    connection = _fake(manifest, orphans={21: 3})
    body = _run(connection, FakeAdmin(manifest)).json()

    assert body['status'] == 'mismatch'
    assert body['translation_mismatches']['npu']['chunk_embeddings_orphans'] == {
        'expected': 0, 'actual': 3, 'ok': False
    }


def test_chunks_without_embeddings_of_this_version_are_a_named_mismatch():
    """A translation with chunks and no vectors of the version this service
    reads is unusable for retrieval — reported even though the source
    declared the same zero, which is the reason to say so, not to stay quiet.
    """
    per_translation = {
        entry['alias']: {
            'translation_chunks': INDEX_CHUNKS,
            'psalm_verse_mappings': INDEX_MAPPINGS,
            'chunk_embeddings': {VERSION: 0 if entry['alias'] == 'npu' else INDEX_EMBEDDINGS},
        }
        for entry in TRANSLATIONS
    }
    manifest = _manifest(index=_index_block(per_translation=per_translation))
    connection = _fake(manifest)
    admin = FakeAdmin(manifest, index_rows={'npu': _index_rows(21, 'npu', embeddings=0)})
    body = _run(connection, admin).json()

    assert body['status'] == 'mismatch'
    assert body['translation_mismatches']['npu']['chunk_embeddings'] == {
        'expected': INDEX_CHUNKS, 'actual': 0, 'ok': False
    }
    assert 'syn' not in body['translation_mismatches']


def test_a_translation_without_chunks_at_all_is_normal():
    """bti, npu, webbe and webus have no chunk corpus today; their Psalm map
    still travels, and none of it is a mismatch."""
    per_translation = {
        entry['alias']: {
            'translation_chunks': 0 if entry['alias'] == 'bti' else INDEX_CHUNKS,
            'psalm_verse_mappings': INDEX_MAPPINGS,
            'chunk_embeddings': {VERSION: 0 if entry['alias'] == 'bti' else INDEX_EMBEDDINGS},
        }
        for entry in TRANSLATIONS
    }
    digests = dict(DIGESTS, bti=None)
    manifest = _manifest(
        index=_index_block(per_translation=per_translation, chunks_digest=digests)
    )
    connection = _fake(manifest)
    admin = FakeAdmin(
        manifest,
        index_rows={'bti': _index_rows(11, 'bti', chunks=0, embeddings=0)},
    )
    body = _run(connection, admin).json()

    assert body['status'] == 'ok'
    assert body['translation_mismatches'] == {}
    assert 'bti' in body['index']['translations_indexed']
    assert body['index']['tables']['psalm_verse_mappings'] == (
        INDEX_MAPPINGS * len(TRANSLATIONS)
    )


def test_the_index_cache_is_dropped_after_the_import():
    """One worker, one cache: `POST /api/ai/scripture` must serve the new
    index without a restart."""
    manifest = _manifest()
    connection = _fake(manifest)
    with patch.object(import_data, 'clear_cached_resources') as clear:
        body = _run(connection, FakeAdmin(manifest)).json()

    assert clear.call_count >= 1
    assert body['index']['index_cache_cleared'] is True


def test_the_index_cache_is_dropped_on_a_mismatch_too():
    """The rows on disk changed either way; a cache older than them is the
    divergence this change exists to remove."""
    manifest = _manifest()
    scoped = _matching_translation_counts(manifest)
    scoped[1]['translation_chunks'] = 0
    connection = _fake(manifest, translation_counts=scoped)
    with patch.object(import_data, 'clear_cached_resources') as clear:
        body = _run(connection, FakeAdmin(manifest)).json()

    assert body['status'] == 'mismatch'
    assert clear.call_count >= 1
    assert body['index']['index_cache_cleared'] is True


def test_a_cache_that_cannot_be_dropped_is_reported_not_hidden():
    manifest = _manifest()
    connection = _fake(manifest)
    with patch.object(import_data, 'clear_cached_resources',
                      side_effect=RuntimeError("cache is wedged")):
        body = _run(connection, FakeAdmin(manifest)).json()

    assert body['status'] == 'ok'
    assert body['index']['index_cache_cleared'] is False


# --------------------------------------------------------------------------
# The point import must keep working exactly as it did.
# --------------------------------------------------------------------------

BTI_ROWS = {
    'translations': 1, 'translation_books': 1, 'translation_verses': 2,
    'translation_titles': 0, 'translation_notes': 0, 'voices': 1,
    'voice_alignments': 1,
    # The index this translation ends up with, checked against the manifest
    # exactly as the text tables are (ClickUp 86cbegwr9).
    'translation_chunks': INDEX_CHUNKS,
    'psalm_verse_mappings': INDEX_MAPPINGS,
    'chunk_embeddings': INDEX_EMBEDDINGS,
}


def _point_connection(scoped=None, **kwargs):
    """A stand-in cep_public holding exactly what a bti point import writes."""
    kwargs.setdefault('digests', {11: DIGESTS['bti']})
    return FakeConnection(
        translation_counts={11: dict(scoped or BTI_ROWS)}, **kwargs
    )


def _point_import(payload, connection, alias='bti', admin=None):
    admin = admin or FakeAdmin(payloads={alias: payload})
    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json', side_effect=admin):
        return client.get(f'/api/import?translation={alias}', headers=HEADERS)


def test_point_import_still_replaces_one_translation():
    payload = _payload(11, 'bti')
    connection = _point_connection()
    response = _point_import(payload, connection)

    body = response.json()
    assert response.status_code == 200, response.text
    assert body['status'] == 'ok'
    assert body['translation'] == 'bti'
    assert body['tables']['translation_verses'] == 2
    # Reference tables via REPLACE, translation data via INSERT — unchanged.
    replaced = [
        sql.split('`')[1] for kind, sql, _ in connection.log
        if kind == 'executemany' and sql.startswith('REPLACE INTO')
    ]
    assert replaced == ['languages', 'bible_books']
    assert connection.deleted_translation_codes() == [11]
    assert not any(
        'TRUNCATE' in statement.upper() for statement in connection.statements()
    )


def test_point_import_verifies_every_table_it_writes():
    """Review MINOR-3: it used to check 3 of the 7 tables it replaces."""
    connection = _point_connection()
    body = _point_import(_payload(11, 'bti'), connection).json()

    assert set(body['verification']) == set(import_data.TRANSLATION_TABLES)
    assert all(check['ok'] for check in body['verification'].values())
    assert body['status'] == 'ok'


def test_point_import_catches_a_short_voice_alignments_write():
    """The table the manual fixes ride in on, and the largest one by far.

    An import that wrote only part of a translation's alignments used to pass
    as "ok": nothing counted them.
    """
    payload = _payload(11, 'bti')
    payload['voice_alignments'] = [
        {'code': 110000 + i, 'voice': 110, 'begin': 0.0, 'end': 1.0}
        for i in range(3)
    ]
    scoped = dict(BTI_ROWS, voice_alignments=2)  # one alignment did not land
    connection = _point_connection(scoped)
    body = _point_import(payload, connection).json()

    assert body['status'] == 'mismatch'
    assert body['verification']['voice_alignments'] == {
        'expected': 3, 'actual': 2, 'ok': False
    }


def test_point_import_catches_missing_titles_and_notes():
    payload = _payload(11, 'bti')
    payload['translation_titles'] = [
        {'code': 1101, 'before_translation_verse': 11000, 'text': 'A Psalm'}
    ]
    payload['translation_notes'] = [
        {'code': 1102, 'translation_verse': 11000, 'text': 'note'}
    ]
    scoped = dict(BTI_ROWS, translation_titles=1, translation_notes=0)
    connection = _point_connection(scoped)
    body = _point_import(payload, connection).json()

    assert body['status'] == 'mismatch'
    assert body['verification']['translation_titles']['ok'] is True
    assert body['verification']['translation_notes'] == {
        'expected': 1, 'actual': 0, 'ok': False
    }


def test_point_import_of_an_unknown_translation_is_404():
    """Still a 404, and now one round trip earlier: the manifest already says
    which translations exist, so the alias is refused before the export and
    before the index plan (which would otherwise answer "no index counts")."""
    connection = FakeConnection()
    admin = FakeAdmin()

    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json', side_effect=admin):
        response = client.get('/api/import?translation=nope', headers=HEADERS)

    assert response.status_code == 404
    assert 'nope' in response.json()['detail']
    assert [path for path, _ in admin.requests] == ['/api/data/manifest']
    assert not any(
        statement.startswith(('DELETE', 'INSERT', 'REPLACE'))
        for statement in connection.statements()
    )


def test_point_import_carries_the_index_too():
    """`?translation=` is the path production uses most, so it gets the same
    guarantee: text and index in one transaction, verified against the
    manifest afterwards."""
    connection = _point_connection()
    body = _point_import(_payload(11, 'bti'), connection).json()

    assert body['status'] == 'ok'
    assert body['translation_mismatches'] == {}
    assert body['index']['embedding_version'] == VERSION
    assert body['index']['tables'] == {
        'translation_chunks': INDEX_CHUNKS,
        'psalm_verse_mappings': INDEX_MAPPINGS,
        'chunk_embeddings': INDEX_EMBEDDINGS,
    }
    assert body['index']['index_cache_cleared'] is True
    block = _block_with(connection, 'INSERT INTO `chunk_embeddings`')
    assert any('INSERT INTO `translation_verses`' in s for s in block)


def test_point_import_refuses_an_unavailable_index_version():
    connection = _point_connection()
    manifest = _manifest(index=_index_block(available=[OTHER_VERSION]))
    response = _point_import(
        _payload(11, 'bti'), connection, admin=FakeAdmin(manifest)
    )

    assert response.status_code == 502
    assert VERSION in response.json()['detail']
    assert not any(
        statement.startswith(('DELETE', 'INSERT', 'REPLACE'))
        for statement in connection.statements()
    )


def test_point_import_reports_an_index_disagreement():
    scoped = dict(BTI_ROWS, chunk_embeddings=INDEX_EMBEDDINGS - 1)
    connection = _point_connection(scoped)
    body = _point_import(_payload(11, 'bti'), connection).json()

    assert body['status'] == 'mismatch'
    assert body['translation_mismatches']['bti']['chunk_embeddings'] == {
        'expected': INDEX_EMBEDDINGS, 'actual': INDEX_EMBEDDINGS - 1, 'ok': False
    }


def test_point_import_is_not_refused_by_another_translations_missing_counts():
    """The emergency path stays usable: a manifest that cannot count `syn`
    must not block a point import of `bti`."""
    manifest = _manifest()
    del manifest['counts']['per_translation']['syn']
    connection = _point_connection()
    body = _point_import(
        _payload(11, 'bti'), connection, admin=FakeAdmin(manifest)
    ).json()

    assert body['status'] == 'ok'


def test_import_requires_the_api_key():
    response = client.get('/api/import')
    assert response.status_code in (401, 403)


def test_payload_cap_defaults_to_48_mb(monkeypatch):
    """NIT-2: the cap is only useful if the container survives reaching it.

    The largest translation is 29.3 MB; parsing costs several times the body
    in RSS, so on the 2-4 GB production VM a 96 MB body could OOM-kill the
    worker before it ever answered 507 — the exact failure the valve replaces.
    """
    import importlib

    import config

    monkeypatch.delenv('IMPORT_MAX_PAYLOAD_MB', raising=False)
    try:
        assert importlib.reload(config).IMPORT_MAX_PAYLOAD_MB == 48
    finally:
        monkeypatch.undo()
        importlib.reload(config)
