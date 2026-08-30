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

The database is a recording stand-in rather than SQLite or MySQL: what is
under test is the *order* of statements and where the transaction boundaries
fall, which is precisely what a real database hides once the dust settles. The
stand-in also lets a failure be injected at an exact statement, which is how
"an abort at any point keeps the other translations" is proved rather than
asserted.
"""

import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import import_data
from main import app

client = TestClient(app)
HEADERS = {"X-API-Key": "test-api-key"}


# --------------------------------------------------------------------------
# A recording stand-in for the MySQL connection.
# --------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self._rows = []

    def execute(self, sql, params=()):
        self.connection.log.append(('execute', ' '.join(sql.split()), params))
        self.connection.maybe_fail(sql)
        self._rows = self.connection.answer(sql, params)
        self.rowcount = self.connection.rowcounts.get(_verb(sql), 0)

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
                 translation_counts=None):
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
        # (substring, occurrence) of the statement that must raise.
        self.fail_on = fail_on
        self.seen_fail_candidates = 0
        self.rowcounts = {}
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


def _manifest(translations=TRANSLATIONS, totals=None, per_translation=None):
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
    return {
        'languages': list(LANGUAGES),
        'bible_books': list(BIBLE_BOOKS),
        'translations': list(translations),
        'counts': {
            'per_translation': per_translation,
            'totals': totals if totals is not None else computed,
        },
    }


def _matching_counts(manifest):
    """Row counts that make the post-import verification pass."""
    return dict(manifest['counts']['totals'])


def _matching_translation_counts(manifest):
    """Per-translation row counts that make the per-translation check pass."""
    per = manifest['counts']['per_translation']
    return {
        entry['code']: dict(per[entry['alias']])
        for entry in manifest['translations']
    }


def _fake(manifest, translations=TRANSLATIONS, **kwargs):
    """A stand-in cep_public that agrees with the manifest in every table."""
    kwargs.setdefault('counts', _matching_counts(manifest))
    kwargs.setdefault('translation_counts', _matching_translation_counts(manifest))
    return FakeConnection(translations=translations, **kwargs)


class FakeAdmin:
    """Answers `_fetch_json` for the manifest and per-translation exports."""

    def __init__(self, manifest=None, payloads=None, fail_alias=None):
        self.manifest = manifest if manifest is not None else _manifest()
        self.payloads = payloads
        self.fail_alias = fail_alias
        self.requests = []

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
        if self.payloads is not None and alias in self.payloads:
            return self.payloads[alias]
        code = next(
            t['code'] for t in self.manifest['translations'] if t['alias'] == alias
        )
        return _payload(code, alias)


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
    assert [alias for _, alias in admin.requests[1:]] == ['syn', 'bti', 'npu']
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
# The point import must keep working exactly as it did.
# --------------------------------------------------------------------------

BTI_ROWS = {
    'translations': 1, 'translation_books': 1, 'translation_verses': 2,
    'translation_titles': 0, 'translation_notes': 0, 'voices': 1,
    'voice_alignments': 1,
}


def _point_import(payload, connection, alias='bti'):
    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json',
                      side_effect=lambda p, q=None: payload):
        return client.get(f'/api/import?translation={alias}', headers=HEADERS)


def test_point_import_still_replaces_one_translation():
    payload = _payload(11, 'bti')
    connection = FakeConnection(translation_counts={11: dict(BTI_ROWS)})

    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json',
                      side_effect=lambda p, q=None: payload):
        response = client.get('/api/import?translation=bti', headers=HEADERS)

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
    connection = FakeConnection(translation_counts={11: dict(BTI_ROWS)})
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
    connection = FakeConnection(translation_counts={11: scoped})
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
    connection = FakeConnection(translation_counts={11: scoped})
    body = _point_import(payload, connection).json()

    assert body['status'] == 'mismatch'
    assert body['verification']['translation_titles']['ok'] is True
    assert body['verification']['translation_notes'] == {
        'expected': 1, 'actual': 0, 'ok': False
    }


def test_point_import_of_an_unknown_translation_is_404():
    connection = FakeConnection()
    empty = {'languages': [], 'bible_books': [], 'translations': []}

    with patch.object(import_data, 'create_connection', return_value=connection), \
         patch.object(import_data, '_fetch_json', side_effect=lambda p, q=None: empty):
        response = client.get('/api/import?translation=nope', headers=HEADERS)

    assert response.status_code == 404
    assert not any(
        statement.startswith(('DELETE', 'INSERT', 'REPLACE'))
        for statement in connection.statements()
    )


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
