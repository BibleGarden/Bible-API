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

The RAG index travels with the text (ClickUp 86cbegwr9, 2026-09-05)
--------------------------------------------------------------------------
`translation_chunks`, `psalm_verse_mappings` and `chunk_embeddings` used to be
a separate pipeline: production got its text from this import and its index
from a hand-made MySQL dump over an SSH tunnel. That let production hold a new
text under an index built for the old one — a divergence nothing detected,
because the two halves arrived by different roads at different times.

They arrive together now, and the guarantee is stronger than "in the same
request": **a translation's index rows are written in the same transaction as
its text**. An interrupted import leaves that translation whole — old text
with old index — never new text with an index for text that is gone.

Four things follow, and each is a decision rather than an accident:

1. **Everything is fetched before the transaction opens.** The index of one
   translation is several pages of `GET /api/data/index`, each under the
   `IMPORT_MAX_PAYLOAD_MB` valve, and all of them are downloaded, decoded and
   validated *first*. A page this deployment refuses to buffer is a `507`
   before a single row is touched, exactly like an oversized text export.
2. **The version is checked against the manifest before the first write.**
   `current_embedding_version()` is built from *this* deployment's
   `EMBEDDING_MODEL`, `EMBEDDING_DIMENSIONS` and `CHUNKING_VERSION`. If
   admin-api holds no index of that version — or a different chunking version,
   or several at once — the import is a `502` naming both variables, the
   version asked for and the versions that exist. Importing "whatever is
   there" would fill the index with vectors this service cannot search, and
   an empty answer would be indistinguishable from a translation nobody has
   indexed yet.
3. **Rows of other embedding versions survive by default.** A model migration
   keeps the old index beside the new one (ADR 0010), so the rollback is an
   `.env` edit plus a restart. `?drop_other_index_versions=1` is the explicit
   cleanup afterwards, the same shape as `?allow_removals=1` and
   `index_cli rebuild --drop-other-versions`.
4. **The write is verified against the source, not assumed.** Per translation:
   the three row counts, the order-independent `chunks_digest` (a `BIT_XOR` of
   per-chunk MD5s, recomputed here with admin-api's own statement — it is an
   **unsigned** 64-bit number and routinely exceeds 2^63), and the count of
   embeddings whose chunk is missing, which must be zero. Disagreements land
   in `translation_mismatches` beside the text ones; `status="ok"` still means
   everything matched. A translation with no chunks at all (`bti`, `npu`,
   `webbe`, `webus` today) is normal and reported as such.

The DDL (`CREATE TABLE IF NOT EXISTS`, verbatim from the three CLIs that own
these tables) runs **outside** any transaction, because DDL commits
implicitly and would otherwise split a translation's transaction in half.

Finally, the in-process corpus cache is dropped at the end of the import —
the same `scripture_select.clear_cached_resources()` that `POST
/api/cache/clear` calls, in the same single worker — so `POST /api/ai/scripture`
answers from the new index without a restart. It happens on `status="mismatch"`
too: the data on disk changed either way.
"""

import base64
import binascii
import json
import logging
import tempfile
import time
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

from auth import RequireAPIKey
from chunk_cli import CREATE_TABLE_SQL as TRANSLATION_CHUNKS_DDL
from chunking import CHUNKING_VERSION
from config import (
    ADMIN_API_KEY,
    ADMIN_API_URL,
    EMBEDDING_DIMENSIONS,
    EMBEDDING_MODEL,
    IMPORT_HTTP_TIMEOUT_SECONDS,
    IMPORT_MAX_PAYLOAD_MB,
)
from database import create_connection
from models import ImportCountCheckModel, ImportReportModel
from scripture_select import clear_cached_resources
from vector_index import CREATE_TABLE_SQL as CHUNK_EMBEDDINGS_DDL
from vector_index import IndexVersionUnavailable, current_embedding_version
from versification import VERSIFICATION_VERSION
from versification_cli import CREATE_TABLE_SQL as PSALM_MAPPINGS_DDL

logger = logging.getLogger(__name__)

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

# Every TEXT table the import owns — the domain of the manifest count
# verification. The index tables below are verified against the manifest's
# `index` block instead, which counts them per translation and per embedding
# version, so they are deliberately not in this list.
ALL_TABLES = REFERENCE_TABLES + TRANSLATION_TABLES

# The RAG index of one translation, in insert order (chunks before the
# embeddings that point at them). Written inside the SAME transaction as that
# translation's text — see the module docstring.
INDEX_TABLES = ['translation_chunks', 'psalm_verse_mappings', 'chunk_embeddings']

# `CREATE TABLE IF NOT EXISTS`, taken verbatim from the three CLIs that own
# these tables (imported, not copied: a divergence between the importer's DDL
# and the CLI's would be invisible until a production deploy created the wrong
# schema). Executed outside every transaction — DDL commits implicitly.
INDEX_DDL = {
    'translation_chunks': TRANSLATION_CHUNKS_DDL,
    'psalm_verse_mappings': PSALM_MAPPINGS_DDL,
    'chunk_embeddings': CHUNK_EMBEDDINGS_DDL,
}

BATCH_SIZE = 5000

# Embedding rows are ~4 KB of BLOB each, so a 5000-row `executemany` would be
# a ~20 MB packet against MySQL's default `max_allowed_packet`. 500 rows is
# ~2 MB per statement and still one round trip per 500 vectors.
INDEX_BATCH_SIZE = 500

# What one page of `GET /api/data/index` asks for. admin-api clips it to its
# own byte budget (600 on the first page, which also carries the whole chunk
# corpus, 2000 afterwards) and reports the clip as `limit_applied`; the walk
# below follows `next_offset`, never its own arithmetic.
INDEX_PAGE_LIMIT = 2000

# A source that always answers `next_offset` would otherwise loop forever.
# 1000 pages is ~2 000 000 embeddings, two orders of magnitude above the
# largest translation (3963).
INDEX_MAX_PAGES = 1000

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


def fetch_manifest(for_translation: Optional[str] = None) -> dict:
    """Fetches the resync plan: reference tables, work list, expected counts.

    `for_translation` narrows the "every translation must be countable" rule
    to the one a point import is about. A point import is the emergency path;
    refusing it because an unrelated translation is uncountable on the admin
    side would take away the tool used to fix such things.
    """
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
    entries = manifest['translations']
    if for_translation is not None:
        entries = [e for e in entries if e.get('alias') == for_translation]
    missing = [
        entry['alias']
        for entry in entries
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


def _index_source_error(detail: str) -> HTTPException:
    """A broken index source: 502, and nothing was written.

    Every refusal below is raised before the transaction of the translation it
    concerns opens, so the sentence is literally true and the operator can act
    on it without checking what state the database is in.
    """
    return HTTPException(status_code=502, detail=detail)


def index_plan_from_manifest(manifest: dict, aliases: list[str]) -> dict:
    """What index this deployment wants, checked against what admin-api has.

    Everything here happens BEFORE the first write, and every disagreement is
    a `502` rather than an empty index: an index of the wrong version reads
    exactly like a translation nobody has indexed yet, and that silence is the
    failure this whole ticket exists to remove.
    """
    block = manifest.get('index')
    if not isinstance(block, dict):
        raise _index_source_error(
            "admin-api's manifest has no 'index' block: it cannot ship the "
            "RAG index. Deploy the Dashboard-API version that serves "
            "GET /api/data/index (ClickUp 86cbegwqg) before importing. "
            "Nothing was written."
        )
    if block.get('error'):
        raise _index_source_error(
            f"admin-api cannot read its RAG index: {block['error']} "
            f"Nothing was written."
        )

    try:
        want = current_embedding_version()
    except IndexVersionUnavailable as exc:
        raise _index_source_error(
            f"this deployment has no index version to import: {exc}"
        ) from None

    available = list(block.get('available_versions') or [])
    if want not in available:
        raise _index_source_error(
            f"admin-api has no RAG index of version '{want}' — the version "
            f"this deployment reads, built from EMBEDDING_MODEL="
            f"'{EMBEDDING_MODEL}' and EMBEDDING_DIMENSIONS={EMBEDDING_DIMENSIONS} "
            f"(plus CHUNKING_VERSION {CHUNKING_VERSION}). Available there: "
            f"{', '.join(available) if available else 'none'}. Point "
            f"EMBEDDING_MODEL / EMBEDDING_DIMENSIONS at a version that exists, "
            f"or build that version at the source first. Nothing was written."
        )

    source_chunking = block.get('chunking_version')
    if source_chunking is None:
        raise _index_source_error(
            f"admin-api does not report a single chunking version for its "
            f"chunk corpus (chunking_versions="
            f"{block.get('chunking_versions') or []}); this deployment reads "
            f"CHUNKING_VERSION {CHUNKING_VERSION}. A corpus holding several "
            f"chunking versions at once cannot be imported: canonical_id "
            f"carries the version, so the two sets are different corpora that "
            f"would be mixed in one table. Finish the rechunk at the source "
            f"(python app/versification_cli.py rechunk) first. Nothing was "
            f"written."
        )
    if int(source_chunking) != CHUNKING_VERSION:
        raise _index_source_error(
            f"admin-api's chunk corpus is chunking version {source_chunking}, "
            f"this deployment reads CHUNKING_VERSION {CHUNKING_VERSION}. "
            f"canonical_id carries the chunking version, so this is a "
            f"different corpus, not an older copy of the same one. Nothing "
            f"was written."
        )

    per_translation = (block.get('counts') or {}).get('per_translation') or {}
    uncounted = [alias for alias in aliases if alias not in per_translation]
    if uncounted:
        raise _index_source_error(
            f"admin-api's manifest declares no index counts for: "
            f"{', '.join(uncounted)}. An unverifiable index is a broken "
            f"source, exactly as for the text. Nothing was written."
        )

    mapping_version = block.get('mapping_version')
    if mapping_version is not None and int(mapping_version) != VERSIFICATION_VERSION:
        # Not a refusal: the Psalm map is imported as stored, version column
        # included, and a foreign mapping version simply is not read by
        # `passage_highlight` (which selects by VERSIFICATION_VERSION). Worth
        # saying out loud, worth reporting, not worth refusing the text.
        logger.warning(
            "admin-api ships Psalm mappings of version %s while this "
            "deployment reads VERSIFICATION_VERSION %s: the imported map will "
            "not be used until the versions agree",
            mapping_version, VERSIFICATION_VERSION,
        )

    return {
        'embedding_version': want,
        'chunking_version': CHUNKING_VERSION,
        'mapping_version': mapping_version,
        'per_translation': per_translation,
        'chunks_digest': block.get('chunks_digest') or {},
    }


def _prepare_index_rows(rows: list[dict]) -> list[dict]:
    """Rows as stored at the source, minus the AUTO_INCREMENT `code`.

    `code` is dropped rather than carried: it is renumbered by every rebuild,
    nothing references it across tables, and reusing it here would collide
    with the rows of another embedding version this import deliberately keeps.
    The natural keys (`canonical_id`, the verse coordinates) are what the
    unique keys and the digest are built from, and those travel unchanged.
    """
    prepared = []
    for row in rows:
        row = {k: v for k, v in row.items() if k != 'code'}
        created = row.get('created_at')
        if isinstance(created, str):
            # JSON carries a datetime as ISO 8601 with a 'T'. MySQL accepts
            # that form, but only in its relaxed parsing; a space is the
            # documented one and costs nothing.
            row['created_at'] = created.replace('T', ' ')
        prepared.append(row)
    return prepared


def _prepare_embedding_rows(rows: list[dict], alias: str) -> list[dict]:
    """base64 -> BLOB, with the two checks that make the bytes meaningful.

    A vector of the wrong width is not a row to fix later: `load_index` stacks
    every vector into one matrix, so a single short one breaks the whole
    index at read time, far from here. Both checks therefore run before the
    transaction opens — a broken source must not become a partial write.
    """
    prepared = _prepare_index_rows(rows)
    for row in prepared:
        canonical_id = row.get('canonical_id')
        try:
            dims = int(row.get('dims'))
        except (TypeError, ValueError):
            raise _index_source_error(
                f"admin-api sent an embedding for {alias}/{canonical_id} with "
                f"a non-numeric 'dims' ({row.get('dims')!r}). Nothing was "
                f"written."
            ) from None
        if dims != EMBEDDING_DIMENSIONS:
            raise _index_source_error(
                f"admin-api sent a {dims}-dimensional embedding for "
                f"{alias}/{canonical_id} while this deployment reads "
                f"EMBEDDING_DIMENSIONS={EMBEDDING_DIMENSIONS}. Nothing was "
                f"written."
            )
        try:
            blob = base64.b64decode(row['vector'], validate=True)
        except (KeyError, TypeError, ValueError, binascii.Error) as exc:
            raise _index_source_error(
                f"admin-api sent an undecodable vector for "
                f"{alias}/{canonical_id}: {exc}. Nothing was written."
            ) from None
        if len(blob) != dims * 4:
            raise _index_source_error(
                f"admin-api sent {len(blob)} bytes of vector for "
                f"{alias}/{canonical_id}, expected {dims * 4} "
                f"(dims * 4, float32). Nothing was written."
            )
        row['vector'] = blob
    return prepared


def fetch_index_from_admin(
    alias: str,
    embedding_version: str,
    chunking_version: int = CHUNKING_VERSION,
) -> dict:
    """The whole RAG index of one translation, page by page.

    Called BEFORE the translation's transaction opens, like the text export
    and for the same reason: every page passes the `IMPORT_MAX_PAYLOAD_MB`
    valve, and a page this deployment refuses to buffer costs the translation
    nothing (`507`, its rows untouched).

    The corpus tables come back in full on the first page and as `null` on
    later ones, so only the embeddings are accumulated after that. The walk
    follows the source's `next_offset` rather than computing its own: the
    source clips `limit` to its byte budget, and following the clip is what
    keeps the walk from skipping rows.
    """
    chunks: list[dict] = []
    mappings: list[dict] = []
    embeddings: list[dict] = []
    offset = 0
    pages = 0
    total = 0

    while True:
        page = _fetch_json('/api/data/index', {
            'translation': alias,
            'embedding_version': embedding_version,
            'chunking_version': chunking_version,
            'limit': INDEX_PAGE_LIMIT,
            'offset': offset,
        })
        if offset == 0:
            chunks = _prepare_index_rows(page.get('translation_chunks') or [])
            mappings = _prepare_index_rows(page.get('psalm_verse_mappings') or [])
            total = int(page.get('chunk_embeddings_total') or 0)
        embeddings.extend(
            _prepare_embedding_rows(page.get('chunk_embeddings') or [], alias)
        )

        next_offset = page.get('next_offset')
        if next_offset is None:
            break
        next_offset = int(next_offset)
        if next_offset <= offset:
            raise _index_source_error(
                f"admin-api's index export for {alias} made no progress "
                f"(offset {offset} -> next_offset {next_offset}). Nothing was "
                f"written."
            )
        offset = next_offset
        pages += 1
        if pages > INDEX_MAX_PAGES:
            raise _index_source_error(
                f"admin-api's index export for {alias} did not end after "
                f"{INDEX_MAX_PAGES} pages. Nothing was written."
            )

    if len(embeddings) != total:
        # The source told us how many rows the walk covers; a different number
        # here means pages were lost or repeated, and importing that would
        # write a silently incomplete index.
        raise _index_source_error(
            f"admin-api's index export for {alias} declared "
            f"{total} embeddings of '{embedding_version}' but delivered "
            f"{len(embeddings)}. Nothing was written."
        )

    return {
        'translation_chunks': chunks,
        'psalm_verse_mappings': mappings,
        'chunk_embeddings': embeddings,
    }


def insert_rows(cursor, table: str, rows: list[dict], batch_size: int = BATCH_SIZE):
    """Inserts rows into a table in batches"""
    if not rows:
        return 0

    columns = list(rows[0].keys())
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(f'`{c}`' for c in columns)

    sql = f"INSERT INTO `{table}` ({columns_str}) VALUES ({placeholders})"

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
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


# How many index rows belong to ONE translation. The first two mirror
# `INDEX_COUNT_SQL` in Dashboard-API `app/data.py` (which does not narrow by
# chunking/mapping version — the manifest speaks for every version at once,
# and `index_plan_from_manifest` has already refused a source holding more
# than one chunking version). The third narrows to the version this import
# wrote, because the rows of another version are deliberately still there.
INDEX_COUNT_SQL = {
    'translation_chunks':
        "SELECT COUNT(*) AS n FROM translation_chunks WHERE translation = %s",
    'psalm_verse_mappings':
        "SELECT COUNT(*) AS n FROM psalm_verse_mappings WHERE translation = %s",
    'chunk_embeddings':
        "SELECT COUNT(*) AS n FROM chunk_embeddings "
        "WHERE translation = %s AND embedding_version = %s",
}

# admin-api's own digest statement, verbatim (Dashboard-API `app/data.py`,
# `INDEX_CHUNKS_DIGEST_SQL`), so the two sides cannot drift into computing
# different numbers from the same rows. XOR is commutative, so row order and
# the renumbered AUTO_INCREMENT `code` cannot affect it.
#
# The result is an UNSIGNED 64-bit number and routinely exceeds 2^63 (syn =
# 18030424974330788968). Python ints are unbounded, which is why it is
# compared as an int here and must never be handed to a signed 64-bit type.
INDEX_CHUNKS_DIGEST_SQL = """
    SELECT translation AS t,
           BIT_XOR(CONV(SUBSTRING(MD5(CONCAT_WS('\\n', canonical_id, char_count,
                        COALESCE(title,''), text)),1,16),16,10)) AS digest
    FROM translation_chunks
    WHERE translation IN (%s)
    GROUP BY translation
"""

# Embeddings pointing at a chunk that is not there. Zero, always: the two
# tables are written in one transaction from one export, so anything else
# means the export was inconsistent with itself.
INDEX_ORPHAN_SQL = """
    SELECT COUNT(*) AS n FROM chunk_embeddings e
    LEFT JOIN translation_chunks c
      ON c.translation = e.translation AND c.canonical_id = e.canonical_id
    WHERE e.translation = %s AND e.embedding_version = %s
      AND c.canonical_id IS NULL
"""


def ensure_index_tables(cursor):
    """`CREATE TABLE IF NOT EXISTS` for the three index tables.

    Outside every transaction on purpose: DDL commits implicitly in MySQL, so
    running it inside a translation's transaction would silently commit half
    of it. Production has none of these tables before the first import of this
    version, which is the whole reason this runs at all.
    """
    for table in INDEX_TABLES:
        cursor.execute(INDEX_DDL[table])


def delete_translation_index(cursor, translation_code: int, embedding_version: str,
                             drop_other_versions: bool = False) -> int:
    """Remove one translation's index, keeping other embedding versions.

    Chunks and Psalm mappings are replaced whole (`WHERE translation = %s`):
    they are what the source ships for this translation, in one version.
    Embeddings are removed for the imported version only — the rows of another
    version are the rollback of a model migration (ADR 0010) and a routine
    resync must not delete them. `drop_other_versions` is the deliberate
    cleanup, and it reports how many rows it removed.

    One consequence of keeping them: the chunks they join on are replaced by
    this import too. Identical chunk ids (the normal case — same corpus, same
    chunking version) leave them valid; a rechunk at the source would leave
    them pointing at chunks that no longer exist. They are outside the orphan
    check below on purpose — it verifies the index this deployment reads —
    which is another reason the old version is meant to be dropped once the
    new one is trusted, rather than kept forever.
    """
    cursor.execute(
        "DELETE FROM translation_chunks WHERE translation = %s",
        (translation_code,),
    )
    cursor.execute(
        "DELETE FROM psalm_verse_mappings WHERE translation = %s",
        (translation_code,),
    )
    cursor.execute(
        "DELETE FROM chunk_embeddings "
        "WHERE translation = %s AND embedding_version = %s",
        (translation_code, embedding_version),
    )
    if not drop_other_versions:
        return 0
    cursor.execute(
        "DELETE FROM chunk_embeddings "
        "WHERE translation = %s AND embedding_version <> %s",
        (translation_code, embedding_version),
    )
    return cursor.rowcount or 0


def _chunks_digest(cursor, translation_code: int) -> Optional[int]:
    """The digest of what this database now holds for one translation."""
    cursor.execute(INDEX_CHUNKS_DIGEST_SQL, (translation_code,))
    rows = cursor.fetchall()
    if not rows:
        return None
    digest = rows[0]['digest'] if isinstance(rows[0], dict) else rows[0][1]
    return None if digest is None else int(digest)


def _verify_translation_index(cursor, alias: str, code: int, plan: dict) -> dict:
    """Index disagreements for one translation — only the disagreements.

    Four questions, all answered from the database rather than from what the
    importer believes it wrote: the three row counts against the manifest, the
    chunk digest against the manifest's, and whether any embedding has lost
    its chunk.
    """
    expected = plan['per_translation'].get(alias) or {}
    version = plan['embedding_version']
    mismatches: dict = {}

    def check(table, want, got):
        if want != got:
            mismatches[table] = ImportCountCheckModel(
                expected=want, actual=got, ok=False
            )

    chunks = _scalar(cursor, INDEX_COUNT_SQL['translation_chunks'], (code,))
    check('translation_chunks', int(expected.get('translation_chunks', 0)), chunks)
    check(
        'psalm_verse_mappings',
        int(expected.get('psalm_verse_mappings', 0)),
        _scalar(cursor, INDEX_COUNT_SQL['psalm_verse_mappings'], (code,)),
    )

    per_version = expected.get('chunk_embeddings') or {}
    embeddings = _scalar(
        cursor, INDEX_COUNT_SQL['chunk_embeddings'], (code, version)
    )
    check('chunk_embeddings', int(per_version.get(version, 0)), embeddings)

    # A translation with chunks but no embeddings of the version this service
    # reads is unusable for retrieval, and it is reported even when the source
    # declared the same zero — "the source is missing it too" is the reason to
    # say so, not a reason to call the import clean. A translation with no
    # chunks at all (bti, npu, webbe, webus today) is normal and silent.
    if chunks > 0 and embeddings == 0 and 'chunk_embeddings' not in mismatches:
        mismatches['chunk_embeddings'] = ImportCountCheckModel(
            expected=chunks, actual=0, ok=False
        )

    want_digest = (plan['chunks_digest'] or {}).get(alias)
    got_digest = _chunks_digest(cursor, code)
    if want_digest != got_digest:
        # Both sides are unsigned 64-bit or None (an empty chunk set), and both
        # are reported as they are: `null` says "no chunks at all", which 0
        # could not — a real digest of exactly 0 is possible in theory (a total
        # XOR cancellation) and would then read as "no chunks".
        mismatches['chunks_digest'] = ImportCountCheckModel(
            expected=None if want_digest is None else int(want_digest),
            actual=None if got_digest is None else int(got_digest),
            ok=False,
        )

    orphans = _scalar(cursor, INDEX_ORPHAN_SQL, (code, version))
    if orphans:
        mismatches['chunk_embeddings_orphans'] = ImportCountCheckModel(
            expected=0, actual=orphans, ok=False
        )

    return mismatches


def _clear_index_cache() -> bool:
    """Drop the in-process corpus cache — the same call as POST /api/cache/clear.

    Production runs a single worker (the rate limiters are process-local), so
    this one call is the whole cache. Done after the import finished, mismatch
    or not: the rows on disk changed either way, and an index cache older than
    the rows it describes is exactly the divergence this ticket removes.
    """
    try:
        clear_cached_resources()
        return True
    except Exception:
        logger.exception(
            "import finished but the in-process index cache could not be "
            "cleared; POST /api/cache/clear or a restart is needed before "
            "the new index is served"
        )
        return False


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
    cursor, wanted: list, per_translation: dict, index_plan: Optional[dict] = None
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
        if index_plan is not None:
            index_mismatches = _verify_translation_index(
                cursor, alias, code, index_plan
            )
            if index_mismatches:
                mismatches.setdefault(alias, {}).update(index_mismatches)
                all_ok = False
    return mismatches, all_ok


def _import_translation(
    connection, cursor, alias: str, expected_code=None,
    index_plan: Optional[dict] = None, drop_other_index_versions: bool = False,
) -> dict:
    """Replace one translation: fetch, then DELETE+INSERT in one transaction.

    The fetch (and therefore the size valve) happens before the transaction
    opens, so a payload this deployment refuses to buffer costs nothing: the
    translation keeps the rows it had. That is true of the RAG index too — all
    of its pages are downloaded and validated first, and the text and the
    index are then written together, so this translation can never end up with
    new text under the old index.
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

    index_payload = None
    if index_plan is not None:
        index_payload = fetch_index_from_admin(
            alias, index_plan['embedding_version'], index_plan['chunking_version']
        )

    counts = {}
    index_counts = {}
    dropped = 0
    _begin(connection)
    delete_translation_data(cursor, translation_code)
    for table in TRANSLATION_TABLES:
        rows = payload.pop(table, None) or []
        counts[table] = insert_rows(cursor, table, rows)
        # Release the table before parsing the next one: a translation's
        # verses and alignments are the bulk of its footprint and are of no
        # further use once written.
        del rows
    if index_payload is not None:
        dropped = delete_translation_index(
            cursor, translation_code, index_plan['embedding_version'],
            drop_other_versions=drop_other_index_versions,
        )
        for table in INDEX_TABLES:
            rows = index_payload.pop(table, None) or []
            index_counts[table] = insert_rows(
                cursor, table, rows, batch_size=INDEX_BATCH_SIZE
            )
            del rows
    connection.commit()
    return {
        'code': translation_code,
        'tables': counts,
        'index': index_counts,
        'other_versions_removed': dropped,
    }


def _import_full(
    connection, cursor, allow_removals: bool = False,
    drop_other_index_versions: bool = False,
) -> dict:
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

    # The index version this deployment reads, checked against what admin-api
    # actually holds — BEFORE the first write, so a wrong or missing version
    # costs nothing. See `index_plan_from_manifest`.
    index_plan = index_plan_from_manifest(
        manifest, [entry['alias'] for entry in wanted]
    )
    index_report = _new_index_report(index_plan, drop_other_index_versions)

    report = {table: 0 for table in ALL_TABLES}

    # 0. The index tables may not exist yet (production before this feature).
    #    DDL commits implicitly, so it runs outside every transaction below.
    ensure_index_tables(cursor)

    # 1. Reference tables. REPLACE INTO, one transaction, before anything
    #    references them.
    _begin(connection)
    for table in REFERENCE_TABLES:
        report[table] = replace_rows(cursor, table, manifest.get(table) or [])
    connection.commit()

    # 2. One translation at a time, one transaction each — text and index
    #    together, so neither can be newer than the other.
    imported = []
    for entry in wanted:
        alias = entry['alias']
        written = _import_translation(
            connection, cursor, alias, expected_code=entry.get('code'),
            index_plan=index_plan,
            drop_other_index_versions=drop_other_index_versions,
        )
        for table, n in written['tables'].items():
            report[table] += n
        _record_index_write(index_report, alias, written)
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
            # The index goes with the text, on the way out as much as on the
            # way in — and every version of it: `drop_other_index_versions`
            # protects the ROLLBACK of a model migration for a translation
            # that stays, and a translation whose text is gone has nothing to
            # roll back to. Rows left here would be dead forever: no import
            # owns them any more, `index_cli` rebuilds only what
            # `translations` lists, and the orphan sweep below does not reach
            # the index tables. Same transaction as the text, for the same
            # reason as everywhere else.
            delete_translation_index(
                cursor, row['code'], index_plan['embedding_version'],
                drop_other_versions=True,
            )
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
        cursor, wanted, manifest['counts']['per_translation'],
        index_plan=index_plan,
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
        'index': index_report,
        'duration_seconds': round(time.monotonic() - started, 1),
    }


def _new_index_report(index_plan: dict, drop_other_index_versions: bool) -> dict:
    """The `index` block of the report, before anything has been written."""
    return {
        'embedding_version': index_plan['embedding_version'],
        'chunking_version': index_plan['chunking_version'],
        'mapping_version': index_plan.get('mapping_version'),
        'translations_indexed': [],
        'tables': {table: 0 for table in INDEX_TABLES},
        'other_versions_removed': {},
        'drop_other_index_versions': drop_other_index_versions,
        # Set by the endpoint once the whole import is over: one cache drop
        # per request, not one per translation.
        'index_cache_cleared': False,
    }


def _record_index_write(index_report: dict, alias: str, written: dict) -> None:
    """Add what one translation's index write did to the report."""
    for table, n in written['index'].items():
        index_report['tables'][table] += n
    index_report['translations_indexed'].append(alias)
    if written['other_versions_removed']:
        index_report['other_versions_removed'][alias] = (
            written['other_versions_removed']
        )


def _import_single(
    connection, cursor, translation: str,
    drop_other_index_versions: bool = False,
) -> dict:
    """Point import of one translation — the pre-existing, proven path."""
    started = time.monotonic()

    # The manifest is fetched here too, and only for the index: it is the one
    # place that says which index versions exist at the source and what the
    # counts and the chunk digest should be. Its per-translation count check
    # is narrowed to this alias — a point import must not be refused because
    # some other translation is uncountable on the admin side.
    manifest = fetch_manifest(for_translation=translation)
    # Answered from the manifest rather than from the export below, so an
    # unknown alias is still the 404 it always was — and one round trip
    # earlier — instead of "the manifest has no index counts for it".
    if translation not in {
        entry['alias'] for entry in manifest.get('translations') or []
    }:
        raise HTTPException(
            status_code=404,
            detail=f"Translation '{translation}' not found in admin-api data",
        )
    index_plan = index_plan_from_manifest(manifest, [translation])
    index_report = _new_index_report(index_plan, drop_other_index_versions)

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

    # Before the transaction, like the text export: the valve must be able to
    # refuse a page without this translation losing the index it has.
    index_payload = fetch_index_from_admin(
        translation, index_plan['embedding_version'],
        index_plan['chunking_version'],
    )

    ensure_index_tables(cursor)

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
    dropped = delete_translation_index(
        cursor, translation_code, index_plan['embedding_version'],
        drop_other_versions=drop_other_index_versions,
    )
    index_counts = {}
    for table in INDEX_TABLES:
        rows = index_payload.pop(table, None) or []
        index_counts[table] = insert_rows(
            cursor, table, rows, batch_size=INDEX_BATCH_SIZE
        )
        del rows
    connection.commit()
    _record_index_write(index_report, translation, {
        'index': index_counts, 'other_versions_removed': dropped,
    })

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

    # The index is verified against the manifest — the source's own counts and
    # digest — not against what this request believes it wrote, which is the
    # same rule the full resync follows.
    index_mismatches = _verify_translation_index(
        cursor, translation, translation_code, index_plan
    )
    translation_mismatches = {translation: index_mismatches} if index_mismatches else {}

    return {
        'status': 'ok' if all_ok and not index_mismatches else 'mismatch',
        'detail': None,
        'tables': report,
        'translations_imported': [translation],
        'translations_removed': [],
        'removals_rejected': [],
        'orphans_removed': {},
        'verification': checks,
        'translation_mismatches': translation_mismatches,
        'index': index_report,
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
    drop_other_index_versions: bool = Query(
        False,
        description=(
            "Delete the chunk_embeddings rows of every embedding version other "
            "than the one this deployment reads. Off by default: the old "
            "version is the rollback of a model migration (an .env edit plus a "
            "restart), so it is removed by a deliberate run after the new "
            "index has been checked, never by a routine resync. Rows removed "
            "are reported per translation in index.other_versions_removed."
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

    Both paths carry the RAG index of every translation they write
    (`translation_chunks`, `psalm_verse_mappings`, `chunk_embeddings`), in the
    **same transaction as its text**, and refuse with `502` before any write
    when admin-api holds no index of the version this deployment reads. The
    in-process index cache is dropped at the end, so the new index is served
    without a restart. See the module docstring (ClickUp 86cbegwr9).
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
            result = _import_full(
                connection, cursor, allow_removals=allow_removals,
                drop_other_index_versions=drop_other_index_versions,
            )
        else:
            result = _import_single(
                connection, cursor, translation,
                drop_other_index_versions=drop_other_index_versions,
            )

        # The rows changed, so the cached corpus is stale — on "mismatch" as
        # much as on "ok". One drop per request, after everything is written.
        result['index']['index_cache_cleared'] = _clear_index_cache()

        return ImportReportModel(translation=translation, **result)

    except HTTPException:
        try:
            connection.rollback()
        except Exception:
            pass
        # A refusal raised before the first write leaves the cache correct,
        # but a failure part-way through a resync does not: translations
        # committed before it are already new. Dropping a cache that did not
        # need dropping costs one lazy reload, so it is done either way.
        _clear_index_cache()
        raise
    except Exception as e:
        try:
            connection.rollback()
        except Exception:
            pass
        _clear_index_cache()
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
