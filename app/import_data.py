"""
Import data from admin-api into cep_public

GET /api/import[?translation=alias] — loads data from admin-api

Full resync is a SEQUENCE of per-translation imports (ClickUp 86cbbq5zp)
--------------------------------------------------------------------------
Until 2026-08-30 a full resync was one request: `GET /api/data` returned the
whole export as a single 147 MB JSON document, `response.json()` turned it
into Python objects, and the importer then built insert tuples for 197 614
verses and 259 663 alignments — in a one-worker container sharing a small VM
with MySQL. On 2026-08-30 that combination stopped answering: peak RSS of the
importing worker measured 972 MB against 82 MB idle. Point imports
(`?translation=`, 18-29 MB) always passed, which is the whole idea behind the
fix: the full resync now *is* a sequence of those point imports.

Three properties follow from that, and they are the reason this shape was
chosen over streaming the 147 MB document with an incremental JSON parser:

1. **Bounded memory, on both sides.** Only one translation is in flight at a
   time, so peak memory is set by the largest translation (29 MB of JSON),
   not by the corpus. The saving is not the importer's alone: the 147 MB
   document was also materialised inside admin-api, which runs on the same VM.
2. **No "empty everything, then refill".** `TRUNCATE` is DDL and commits
   implicitly, so the old order left every table empty the moment the process
   died. There is no TRUNCATE here at all: each translation is replaced by
   `DELETE`+`INSERT` inside its own transaction, so an abort at any point
   leaves every other translation exactly as it was, and the interrupted one
   at its previous contents.
3. **The proven path is the only path.** The point import was the code that
   kept working in production; a full resync now exercises it N times instead
   of a second, differently-shaped code path that only runs on resync day.

What a full resync still does that a loop over point imports does not:
reference tables first, translations that admin-api no longer publishes are
dropped (only with `?allow_removals=1`, see below), orphan rows are swept, and
the resulting row counts are verified against the manifest — per table AND per
translation — and reported.

Two guards were added on review of this change (2026-08-30):

- **Verification is per translation, not only in total.** Global totals hide
  compensating errors: a hundred verses too many in one translation and a
  hundred too few in another sum to a passing count. The manifest already
  carries `counts.per_translation`, so every translation is checked against
  its own expected row counts, and the disagreements are named in the report.
- **Dropping a translation needs `?allow_removals=1`.** Removing a translation
  from production is a rare, deliberate act; a bug in the admin-side
  `active = 1` filter, or one wrong click in the dashboard, would otherwise be
  enough for a routine resync to delete a live translation. A resync that is
  about to remove at least one translation now refuses to remove anything
  without the flag — after the import itself, which is why the report says
  `status="removals_rejected"` rather than failing: the translations that were
  imported are correct and stay.
"""

import json
import tempfile
import time
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

from auth import RequireAPIKey
from config import (
    ADMIN_API_KEY,
    ADMIN_API_URL,
    IMPORT_HTTP_TIMEOUT_SECONDS,
    IMPORT_MAX_PAYLOAD_MB,
)
from database import create_connection
from models import ImportCountCheckModel, ImportReportModel

router = APIRouter()

# Reference tables, written once per full resync before any translation.
REFERENCE_TABLES = ['languages', 'bible_books']

# Tables owned by a single translation, in insert order (parents first).
TRANSLATION_TABLES = [
    'translations',
    'translation_books',
    'translation_verses',
    'translation_titles',
    'translation_notes',
    'voices',
    'voice_alignments',
]

# Every table the import owns — the domain of the count verification.
ALL_TABLES = REFERENCE_TABLES + TRANSLATION_TABLES

BATCH_SIZE = 5000

# Size of the chunks the response body is streamed in. 1 MiB: large enough
# that the loop is not the bottleneck, small enough that the cap below is
# enforced long before an oversized body has been read.
DOWNLOAD_CHUNK_BYTES = 1024 * 1024


def _admin_headers() -> dict:
    headers = {}
    if ADMIN_API_KEY:
        headers['X-API-Key'] = ADMIN_API_KEY
    return headers


def _oversized(url: str, size_bytes: int, limit_bytes: int) -> HTTPException:
    """The loud failure of the size valve.

    507 rather than 502: the payload is not malformed, this deployment
    refuses to buffer it. The message names the variable to change, because
    the only correct reactions are "raise the cap" or "split the export".
    """
    return HTTPException(
        status_code=507,
        detail=(
            f"admin-api response for {url} is {size_bytes / 1048576:.1f} MB, "
            f"over the {IMPORT_MAX_PAYLOAD_MB} MB import limit. Nothing was "
            f"written. Raise IMPORT_MAX_PAYLOAD_MB only if the container has "
            f"the memory for it — parsing a payload of this size costs several "
            f"times its size in RSS."
        ),
    )


def _fetch_json(path: str, params: Optional[dict] = None) -> dict:
    """GET a JSON document from admin-api under a hard size cap.

    The body is streamed to a temporary file and parsed from it, so the raw
    bytes and the parsed objects are never both resident — `response.json()`
    holds both. The cap is checked against `Content-Length` when the server
    sends one and, either way, against the bytes actually received: a
    chunked response cannot slip past it.

    Callers must invoke this BEFORE the destructive phase it feeds, which is
    what makes the valve meaningful — see `_import_translation`.
    """
    limit_bytes = IMPORT_MAX_PAYLOAD_MB * 1024 * 1024
    url = f"{ADMIN_API_URL}{path}"

    with httpx.Client(timeout=IMPORT_HTTP_TIMEOUT_SECONDS) as client:
        with client.stream(
            'GET', url, params=params or {}, headers=_admin_headers()
        ) as response:
            if response.status_code != 200:
                response.read()
                raise HTTPException(
                    status_code=502,
                    detail=(
                        f"admin-api returned status {response.status_code} "
                        f"for {path}: {response.text[:500]}"
                    ),
                )

            declared = response.headers.get('content-length')
            if declared is not None:
                try:
                    if int(declared) > limit_bytes:
                        raise _oversized(path, int(declared), limit_bytes)
                except ValueError:
                    pass  # unparsable header; the byte counter below decides

            received = 0
            with tempfile.TemporaryFile() as buffer:
                for chunk in response.iter_bytes(DOWNLOAD_CHUNK_BYTES):
                    received += len(chunk)
                    if received > limit_bytes:
                        raise _oversized(path, received, limit_bytes)
                    buffer.write(chunk)
                buffer.seek(0)
                try:
                    return json.load(buffer)
                except ValueError as exc:
                    raise HTTPException(
                        status_code=502,
                        detail=f"admin-api returned invalid JSON for {path}: {exc}",
                    ) from None


def fetch_data_from_admin(translation: Optional[str] = None) -> dict:
    """Fetches the export of one translation (or the whole export) from admin-api.

    A full resync no longer calls this without `translation`; the argument is
    kept optional because the point import is the same call, and because a
    deliberate whole-export fetch stays available to anything that can afford
    it (the size valve decides, not the absence of the code path).
    """
    params = {'translation': translation} if translation else {}
    return _fetch_json('/api/data', params)


def fetch_manifest() -> dict:
    """Fetches the resync plan: reference tables, work list, expected counts."""
    manifest = _fetch_json('/api/data/manifest')
    for key in ('languages', 'bible_books', 'translations', 'counts'):
        if key not in manifest:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"admin-api manifest is missing '{key}'. Deploy the "
                    f"matching Dashboard-API version before running a full "
                    f"resync (ClickUp 86cbbq5zp)."
                ),
            )
    for key in ('totals', 'per_translation'):
        if key not in manifest['counts']:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"admin-api manifest is missing 'counts.{key}'. Deploy the "
                    f"matching Dashboard-API version before running a full "
                    f"resync (ClickUp 86cbbq5zp)."
                ),
            )
    # The per-translation expectations are the verification of this resync, so
    # a manifest that lists a translation it cannot count is a broken source,
    # not a translation to import unchecked. Caught here, before any write.
    missing = [
        entry['alias']
        for entry in manifest['translations']
        if entry['alias'] not in manifest['counts']['per_translation']
    ]
    if missing:
        raise HTTPException(
            status_code=502,
            detail=(
                f"admin-api manifest declares translations it has no expected "
                f"counts for: {', '.join(missing)}. Nothing was written."
            ),
        )
    return manifest


def insert_rows(cursor, table: str, rows: list[dict]):
    """Inserts rows into a table in batches"""
    if not rows:
        return 0

    columns = list(rows[0].keys())
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(f'`{c}`' for c in columns)

    sql = f"INSERT INTO `{table}` ({columns_str}) VALUES ({placeholders})"

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        values = [tuple(row[c] for c in columns) for row in batch]
        cursor.executemany(sql, values)

    return len(rows)


def replace_rows(cursor, table: str, rows: list[dict]):
    """REPLACE INTO for reference tables in batches"""
    if not rows:
        return 0

    columns = list(rows[0].keys())
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(f'`{c}`' for c in columns)

    sql = f"REPLACE INTO `{table}` ({columns_str}) VALUES ({placeholders})"

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        values = [tuple(row[c] for c in columns) for row in batch]
        cursor.executemany(sql, values)

    return len(rows)


def delete_translation_data(cursor, translation_code: int):
    """Deletes data for a specific translation from cep_public"""
    # Delete in dependency order (from dependent to parent tables)

    # voice_alignments for voices of this translation
    cursor.execute("""
        DELETE va FROM voice_alignments va
        INNER JOIN voices v ON va.voice = v.code
        WHERE v.translation = %s
    """, (translation_code,))

    # voices of this translation
    cursor.execute("DELETE FROM voices WHERE translation = %s", (translation_code,))

    # translation_notes for verses of this translation
    cursor.execute("""
        DELETE tn FROM translation_notes tn
        INNER JOIN translation_verses tv ON tn.translation_verse = tv.code
        WHERE tv.translation = %s
    """, (translation_code,))

    # translation_notes for titles of this translation
    cursor.execute("""
        DELETE tn FROM translation_notes tn
        INNER JOIN translation_titles tt ON tn.translation_title = tt.code
        INNER JOIN translation_verses tv ON tt.before_translation_verse = tv.code
        WHERE tv.translation = %s
    """, (translation_code,))

    # translation_titles for verses of this translation
    cursor.execute("""
        DELETE tt FROM translation_titles tt
        INNER JOIN translation_verses tv ON tt.before_translation_verse = tv.code
        WHERE tv.translation = %s
    """, (translation_code,))

    # translation_verses of this translation
    cursor.execute("DELETE FROM translation_verses WHERE translation = %s", (translation_code,))

    # translation_books of this translation
    cursor.execute("DELETE FROM translation_books WHERE translation = %s", (translation_code,))

    # the translation itself
    cursor.execute("DELETE FROM translations WHERE code = %s", (translation_code,))


# Rows whose parent no longer exists. A point import cleans up after itself
# through the joins above, but only while the parent row is still there: a
# note whose verse was already gone is invisible to them. `TRUNCATE` used to
# hide that by removing everything; a resync built out of point imports has
# to say it. Order matters — children before parents.
ORPHAN_SWEEP_SQL = [
    ('voice_alignments', """
        DELETE va FROM voice_alignments va
        LEFT JOIN voices v ON va.voice = v.code
        WHERE v.code IS NULL
    """),
    ('voices', """
        DELETE v FROM voices v
        LEFT JOIN translations t ON v.translation = t.code
        WHERE t.code IS NULL
    """),
    ('translation_notes', """
        DELETE tn FROM translation_notes tn
        LEFT JOIN translation_verses tv ON tn.translation_verse = tv.code
        LEFT JOIN translation_titles tt ON tn.translation_title = tt.code
        WHERE tv.code IS NULL AND tt.code IS NULL
    """),
    ('translation_titles', """
        DELETE tt FROM translation_titles tt
        LEFT JOIN translation_verses tv ON tt.before_translation_verse = tv.code
        WHERE tv.code IS NULL
    """),
    ('translation_verses', """
        DELETE tv FROM translation_verses tv
        LEFT JOIN translations t ON tv.translation = t.code
        WHERE t.code IS NULL
    """),
    ('translation_books', """
        DELETE tb FROM translation_books tb
        LEFT JOIN translations t ON tb.translation = t.code
        WHERE t.code IS NULL
    """),
]


# How many rows of each table belong to ONE translation in cep_public.
#
# The predicates mirror two things at once, and have to keep mirroring both:
# the joins of `delete_translation_data` above (what a point import replaces)
# and `MANIFEST_COUNT_SQL` in Dashboard-API `app/data.py` (what admin-api
# declares). The second entry of each pair is how many times the translation
# code appears in the statement — only the notes query names it twice.
TRANSLATION_COUNT_SQL = {
    'translations': (
        "SELECT COUNT(*) AS n FROM translations WHERE code = %s", 1),
    'translation_books': (
        "SELECT COUNT(*) AS n FROM translation_books WHERE translation = %s", 1),
    'translation_verses': (
        "SELECT COUNT(*) AS n FROM translation_verses WHERE translation = %s", 1),
    'translation_titles': ("""
        SELECT COUNT(*) AS n FROM translation_titles tt
        INNER JOIN translation_verses tv ON tt.before_translation_verse = tv.code
        WHERE tv.translation = %s
    """, 1),
    'translation_notes': ("""
        SELECT COUNT(*) AS n FROM translation_notes tn
        LEFT JOIN translation_verses tv ON tn.translation_verse = tv.code
        LEFT JOIN translation_titles tt ON tn.translation_title = tt.code
        LEFT JOIN translation_verses tv2 ON tt.before_translation_verse = tv2.code
        WHERE tv.translation = %s OR tv2.translation = %s
    """, 2),
    'voices': (
        "SELECT COUNT(*) AS n FROM voices WHERE translation = %s", 1),
    'voice_alignments': ("""
        SELECT COUNT(*) AS n FROM voice_alignments va
        INNER JOIN voices v ON va.voice = v.code
        WHERE v.translation = %s
    """, 1),
}


def _begin(connection):
    """Open a transaction, whatever the driver left behind."""
    if connection.in_transaction:
        connection.rollback()
    connection.start_transaction()


def _scalar(cursor, sql: str, params=()) -> int:
    """One number from one row.

    `fetchall`, not `fetchone`: the driver's cursors are unbuffered, and a
    result set left half-read makes the NEXT `execute` fail with "Unread
    result found" — in the middle of an import, several statements later.
    """
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    row = rows[0]
    return int(row['n'] if isinstance(row, dict) else row[0])


def _count_rows(cursor, table: str) -> int:
    return _scalar(cursor, f"SELECT COUNT(*) AS n FROM `{table}`")


def _count_translation_rows(cursor, table: str, translation_code: int) -> int:
    """Rows of one table that belong to one translation."""
    sql, arity = TRANSLATION_COUNT_SQL[table]
    return _scalar(cursor, sql, (translation_code,) * arity)


def _verify_counts(cursor, expected: dict) -> tuple[dict, bool]:
    """Compare what cep_public now holds against what admin-api declared."""
    checks = {}
    all_ok = True
    for table in ALL_TABLES:
        if table not in expected:
            continue
        want = int(expected[table])
        got = _count_rows(cursor, table)
        checks[table] = ImportCountCheckModel(
            expected=want, actual=got, ok=(want == got)
        )
        if want != got:
            all_ok = False
    return checks, all_ok


def _verify_translation_counts(
    cursor, wanted: list, per_translation: dict
) -> tuple[dict, bool]:
    """Check every translation against its own expected row counts.

    Totals alone are not a verification of a resync: an error that adds a
    hundred verses to one translation and loses a hundred from another sums to
    a passing global count, and the two halves of that would be a real data
    loss in a real translation. Only the disagreements are returned — seven
    translations times seven tables of matching numbers would bury them.
    """
    mismatches: dict = {}
    all_ok = True
    for entry in wanted:
        alias = entry['alias']
        code = entry['code']
        expected = per_translation.get(alias) or {}
        for table in TRANSLATION_TABLES:
            if table not in expected:
                continue
            want = int(expected[table])
            got = _count_translation_rows(cursor, table, code)
            if want != got:
                mismatches.setdefault(alias, {})[table] = ImportCountCheckModel(
                    expected=want, actual=got, ok=False
                )
                all_ok = False
    return mismatches, all_ok


def _import_translation(connection, cursor, alias: str, expected_code=None) -> dict:
    """Replace one translation: fetch, then DELETE+INSERT in one transaction.

    The fetch (and therefore the size valve) happens before the transaction
    opens, so a payload this deployment refuses to buffer costs nothing: the
    translation keeps the rows it had.
    """
    payload = fetch_data_from_admin(alias)

    translations_data = payload.get('translations') or []
    if not translations_data:
        raise HTTPException(
            status_code=404,
            detail=f"Translation '{alias}' not found in admin-api data",
        )
    translation_code = translations_data[0].get('code')
    if expected_code is not None and translation_code != expected_code:
        raise HTTPException(
            status_code=502,
            detail=(
                f"admin-api changed the code of '{alias}' between the manifest "
                f"({expected_code}) and the export ({translation_code}); "
                f"nothing was written for it"
            ),
        )

    counts = {}
    _begin(connection)
    delete_translation_data(cursor, translation_code)
    for table in TRANSLATION_TABLES:
        rows = payload.pop(table, None) or []
        counts[table] = insert_rows(cursor, table, rows)
        # Release the table before parsing the next one: a translation's
        # verses and alignments are the bulk of its footprint and are of no
        # further use once written.
        del rows
    connection.commit()
    return counts


def _import_full(connection, cursor, allow_removals: bool = False) -> dict:
    """Full resync: reference tables, then every translation, one at a time."""
    started = time.monotonic()
    manifest = fetch_manifest()

    wanted = manifest['translations']
    if not wanted:
        # The pre-2026-08-30 code would happily TRUNCATE everything and
        # insert nothing here. Refusing is the only safe answer: an empty
        # work list is a broken source, never an instruction to publish an
        # empty Bible.
        raise HTTPException(
            status_code=502,
            detail=(
                "admin-api declares no active translations — refusing a full "
                "resync that would leave cep_public empty. Nothing was written."
            ),
        )

    report = {table: 0 for table in ALL_TABLES}

    # 1. Reference tables. REPLACE INTO, one transaction, before anything
    #    references them.
    _begin(connection)
    for table in REFERENCE_TABLES:
        report[table] = replace_rows(cursor, table, manifest.get(table) or [])
    connection.commit()

    # 2. One translation at a time, one transaction each.
    imported = []
    for entry in wanted:
        alias = entry['alias']
        counts = _import_translation(
            connection, cursor, alias, expected_code=entry.get('code')
        )
        for table, n in counts.items():
            report[table] += n
        imported.append(alias)

    # 3. Translations cep_public still holds that admin-api no longer
    #    publishes (deactivated or deleted). TRUNCATE used to do this
    #    implicitly; now it is an explicit step, one transaction each — and
    #    it does not happen at all without `?allow_removals=1`.
    _begin(connection)
    cursor.execute("SELECT code, alias FROM translations")
    local = cursor.fetchall()
    connection.commit()
    wanted_codes = {entry['code'] for entry in wanted}
    stale = [row for row in local if row['code'] not in wanted_codes]

    removed = []
    removals_rejected = []
    detail = None
    if stale and not allow_removals:
        # The gate. Removing a translation from production is a rare,
        # deliberate act; a translation missing from the manifest is far more
        # often an accident on the admin side (a wrong `active = 0`, a bad
        # filter) than an instruction to delete a published Bible. Refuse
        # BEFORE any delete — the import of step 2 already happened and is
        # correct, so it is not undone; the report says so and says what was
        # refused.
        removals_rejected = [row['alias'] for row in stale]
        detail = (
            "Refused to remove translations missing from the admin-api "
            "manifest: " + ', '.join(removals_rejected) + ". The import "
            "itself completed and the imported translations are up to date. "
            "Check that this is intended (a translation deactivated by "
            "mistake looks exactly like this), then repeat the request with "
            "?allow_removals=1 to drop them."
        )
    else:
        for row in stale:
            _begin(connection)
            delete_translation_data(cursor, row['code'])
            connection.commit()
            removed.append(row['alias'])

    # 4. Rows whose parent is gone, and reference rows admin-api dropped.
    _begin(connection)
    orphans = {}
    for table, sql in ORPHAN_SWEEP_SQL:
        cursor.execute(sql)
        if cursor.rowcount:
            orphans[table] = cursor.rowcount
    # Stale reference rows, but only where nothing points at them: an
    # unexpected reference is a reason to report a mismatch below, not to
    # create a dangling one.
    aliases = [row['alias'] for row in manifest['languages']]
    if aliases:
        cursor.execute(
            "DELETE FROM languages WHERE alias NOT IN (%s) "
            "AND alias NOT IN (SELECT language FROM translations)"
            % ','.join(['%s'] * len(aliases)),
            aliases,
        )
        if cursor.rowcount:
            orphans['languages'] = cursor.rowcount
    numbers = [row['number'] for row in manifest['bible_books']]
    if numbers:
        # Same rule as `languages` above: a stale reference row that something
        # still points at is a mismatch to report, not a dangling row to
        # create. FOREIGN_KEY_CHECKS is 0 on this session, so nothing else
        # would stop the delete.
        cursor.execute(
            "DELETE FROM bible_books WHERE number NOT IN (%s) "
            "AND number NOT IN (SELECT book_number FROM translation_books)"
            % ','.join(['%s'] * len(numbers)),
            numbers,
        )
        if cursor.rowcount:
            orphans['bible_books'] = cursor.rowcount
    connection.commit()

    # 5. What cep_public actually holds now, against what admin-api declared —
    #    in total and per translation.
    checks, totals_ok = _verify_counts(cursor, manifest['counts']['totals'])
    translation_mismatches, per_translation_ok = _verify_translation_counts(
        cursor, wanted, manifest['counts']['per_translation']
    )

    if removals_rejected:
        # Not "ok" and not merely "mismatch": the operator has a decision to
        # make, and the totals will disagree anyway while the extra
        # translations are still there.
        status = 'removals_rejected'
    elif totals_ok and per_translation_ok:
        status = 'ok'
    else:
        status = 'mismatch'

    return {
        'status': status,
        'detail': detail,
        'tables': report,
        'translations_imported': imported,
        'translations_removed': removed,
        'removals_rejected': removals_rejected,
        'orphans_removed': orphans,
        'verification': checks,
        'translation_mismatches': translation_mismatches,
        'duration_seconds': round(time.monotonic() - started, 1),
    }


def _import_single(connection, cursor, translation: str) -> dict:
    """Point import of one translation — the pre-existing, proven path."""
    started = time.monotonic()

    # Reference tables come along with the export; keep updating them, as
    # before, so a point import can introduce a new language.
    payload = fetch_data_from_admin(translation)
    translations_data = payload.get('translations') or []
    if not translations_data:
        raise HTTPException(
            status_code=404,
            detail=f"Translation '{translation}' not found in admin-api data",
        )
    translation_code = translations_data[0].get('code')

    report = {}
    _begin(connection)
    for table in REFERENCE_TABLES:
        report[table] = replace_rows(
            cursor, table, payload.pop(table, None) or []
        )
    delete_translation_data(cursor, translation_code)
    for table in TRANSLATION_TABLES:
        rows = payload.pop(table, None) or []
        report[table] = insert_rows(cursor, table, rows)
        del rows
    connection.commit()

    # Verification scoped to this translation: the reference tables are
    # shared, so their global counts say nothing about this import. Every
    # table this import owns is checked — the three it used to check left the
    # bulk of the payload unverified, `voice_alignments` above all: that is
    # the table the manual fixes are delivered in, and the one a partial
    # write would be least visible in.
    checks = {}
    all_ok = True
    for table in TRANSLATION_TABLES:
        got = _count_translation_rows(cursor, table, translation_code)
        want = report.get(table, 0)
        checks[table] = ImportCountCheckModel(
            expected=want, actual=got, ok=(want == got)
        )
        if want != got:
            all_ok = False

    return {
        'status': 'ok' if all_ok else 'mismatch',
        'detail': None,
        'tables': report,
        'translations_imported': [translation],
        'translations_removed': [],
        'removals_rejected': [],
        'orphans_removed': {},
        'verification': checks,
        'translation_mismatches': {},
        'duration_seconds': round(time.monotonic() - started, 1),
    }


@router.get('/import', response_model=ImportReportModel, operation_id="importData", tags=["Import"])
def import_data(
    translation: Optional[str] = Query(None, description="Translation alias for partial import"),
    allow_removals: bool = Query(
        False,
        description=(
            "Full resync only: permit dropping translations that admin-api no "
            "longer publishes. Without it, a resync that would remove at least "
            "one translation removes none and answers "
            "status=\"removals_rejected\", naming them in removals_rejected."
        ),
    ),
    api_key: bool = RequireAPIKey
):
    """
    Import data from admin-api

    Without parameters: full resync — reference tables, then every active
    translation replaced one at a time, then (with `allow_removals=1`)
    translations admin-api no longer publishes, then an orphan sweep, then a
    count check against the manifest, per table and per translation. Each step
    is its own transaction: an abort leaves the untouched translations intact
    rather than emptying cep_public.

    With translation parameter: update a single translation. `allow_removals`
    has no meaning there — a point import removes no translation.
    """
    connection = create_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Import failed: no database connection")
    cursor = connection.cursor(dictionary=True)

    try:
        # Session-level, not transactional: the import writes children before
        # parents inside a translation and drops parents before children
        # across them. Unchanged from the pre-2026-08-30 importer.
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        if translation is None:
            result = _import_full(connection, cursor, allow_removals=allow_removals)
        else:
            result = _import_single(connection, cursor, translation)

        return ImportReportModel(translation=translation, **result)

    except HTTPException:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            connection.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")
    finally:
        # Deliberately no commit here: cleanup must never persist work the
        # body did not commit itself. `SET FOREIGN_KEY_CHECKS` is a session
        # variable, not transactional, so it needs none.
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        except Exception:
            pass
        try:
            cursor.close()
        except Exception:
            pass
        try:
            connection.close()
        except Exception:
            pass
