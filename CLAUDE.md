# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Public API — a read-only FastAPI service for the Bible Garden iOS app. Works with the `cep_public` database, which contains only finalized data. Data is imported from Dashboard-API via `GET /api/import`.

## Common Commands

### Run / Build
```bash
docker compose up -d --build
docker logs bible-api -f
docker compose down
```

### OpenAPI Spec
```bash
docker exec bible-api bash -c "cd /code && PYTHONPATH=app python3 extract-openapi.py app.main:app"
```

### Tests
Only `app/` is bind-mounted, so `tests/` and `evaluation/` must be copied into
the container (trailing `/.` — plain `docker cp tests` nests it into
`/code/tests/tests`; recreating the container resets both directories to the
image contents):
```bash
docker cp tests/. bible-api:/code/tests
docker cp evaluation/. bible-api:/code/evaluation
docker exec -e API_KEY=test-api-key -e AI_CLIENT_HMAC_KEY=test-hmac-key \
  bible-api pytest -q
```
`tests/conftest.py` supplies the model and DB variables the fail-fast config
now requires (via `setdefault`, so real values still win), which is why no
extra `-e` overrides are needed. The two that remain are *not* redundant:
`conftest` uses `setdefault`, so the container's real `.env` values for
`API_KEY` and `AI_CLIENT_HMAC_KEY` would win and the auth/pseudonym
assertions (which expect `test-api-key` / `test-hmac-key`) would fail with
403. The `TWINKLER_SYSTEM_PROMPT` override was dropped on 2026-08-30: the
system prompt is a code constant now, not an environment value.

## Architecture

### Application Structure (`app/`)

- **`main.py`** — FastAPI app entry point, languages/translations/books endpoints, `timed_cache` decorator
- **`excerpt.py`** — Core content endpoint: `excerpt_with_alignment`. No COALESCE, no voice_manual_fixes (manual fixes already applied during import). Also owns `prev_excerpt`/`next_excerpt` navigation, which walks the books of *this* translation and steps over the ones it ships no text for (see "Navigation across books without text" below)
- **`canon.py`** — chapter structure of the 66-book canon (`CANONICAL_BOOKS`, 1189 chapters) plus the per-translation exceptions; the single source of "how many chapters a book is expected to have" for `/translations/{code}/books` and for excerpt navigation (see "Chapter coverage" below)
- **`audio.py`** — MP3 file serving with HTTP Range request support
- **`about.py`** — About page content
- **`version_check.py`** — App version check
- **`import_data.py`** — Import data from Dashboard-API
- **`twinkler_ai.py`** — Server-prompted Gemini integration with in-memory rate limiting: `POST /api/ai/question` and `POST /api/ai/transcribe` (see `architect/twinkler-ai.md`)
- **`question_prompt.py`** — the system prompt of `POST /api/ai/question` as a versioned constant (`QUESTION_PROMPT`, `QUESTION_PROMPT_VERSION`), the way `query_rewrite`/`passage_rerank` version theirs; moved out of `TWINKLER_SYSTEM_PROMPT` on 2026-08-30
- **`scripture_select.py`** — Public scripture-selection endpoint `POST /api/ai/scripture` over `retrieval.select_final`; owns the process-local corpus cache: vector + BM25 indexes, Psalm maps, catalogue, coverage sets (see `architect/scripture-select.md`, `architect/adr/0006-scripture-select-api.md`, `architect/adr/0007-reference-translation-rendering.md`)
- **`passage_render.py`** — renders a canonical passage window in a translation that has no chunk corpus (coordinates through `psalm_verse_mappings`, text from `translation_verses` with `chunking.build_text` semantics) and builds the per-translation coverage sets used to filter candidates before the rerank (ADR 0007)
- **`rate_limit.py`** — Shared in-memory rolling-window limiter (Twinkler + scripture selection)
- **`deadline.py`** — Per-request time budget threaded through the AI stages
- **`prompt_safety.py`** — Neutralizes forged prompt data-block delimiters (invisible characters and angle-bracket look-alikes) in user text
- **`trusted_proxies.py`** — which peers may speak for their clients through `X-Forwarded-For`: container names resolved through docker DNS with a TTL (plus literal IPs and CIDR networks), and the loud diagnostics around them (see "Trusted proxies survive a reboot" below)
- **`client_ip.py`** — the client address of a request (`resolve_client_ip`, through `trusted_proxies`; `X-Forwarded-For` is read **right to left**, see "Which element of `X-Forwarded-For`" below) and its HMAC pseudonym for the stats table and the per-client limiter
- **`auth.py`** — Only API Key authentication (no JWT)
- **`models.py`** — Pydantic response models (no admin models)
- **`chunking.py`** — Pure structural chunking algorithm for RAG (see `architect/adr/0001-structural-chunking.md`)
- **`chunk_cli.py`** — CLI that materializes chunks into `translation_chunks`
- **`versification.py`** — Pure Psalm versification mapping to the canonical english-masoretic numbering (see `architect/adr/0003-psalm-versification-canon.md`)
- **`versification_cli.py`** — CLI: builds/verifies `psalm_verse_mappings`, migrates the chunk corpus to the current CHUNKING_VERSION carrying embeddings over by text (`build`/`verify`/`rechunk`)
- **`embeddings.py`** — Gemini embedding client for RAG retrieval (see `architect/adr/0002-embedding-model-and-vector-store.md`)
- **`vector_index.py`** — `chunk_embeddings` storage + in-process cosine search with language/translation filters
- **`index_cli.py`** — CLI that (re)builds the vector index idempotently (`rebuild`/`status`/`search`)
- **`query_rewrite.py`** — Gemini rewrite of prayer context into scripture-register query variants (see `architect/adr/0004-retrieval-pipeline.md`)
- **`lexical_index.py`** — in-process BM25 over chunks, the hybrid lexical signal
- **`retrieval.py`** — retrieval pipeline: interleave fusion, global genre blacklist (`data/genre_blacklist.json`), safe pool (`data/safe_pool.json`), diversity, `ScriptureRetriever` service; `select_final` adds the grounded rerank with top-1 fallback; both entry points accept an optional per-request `Deadline` and can embed query variants concurrently
- **`passage_rerank.py`** — grounded AI choice of the final passage among retrieval candidates plus its 1-3 key verses: validated index-only answer (candidate number + verse-marker span), JSON schema, injection-hardened prompt (see `architect/adr/0005-grounded-passage-rerank.md`)
- **`passage_highlight.py`** — turns the validated key-verse marker span into canonical and translation coordinates through `psalm_verse_mappings`; the optional `highlight` field of the selection response
- **`retrieval_cli.py`** — end-to-end retrieval smoke CLI (`select`)
- **`database.py`** — MySQL connection factory
- **`config.py`** — Environment variable loading

### Key Differences from Dashboard-API

- No JWT authentication, no admin endpoints
- No `voice_manual_fixes` table — fixes are baked into `voice_alignments` during import
- No `voice_anomalies`, `voice_chapters`, `bible_stat` tables
- No `chapter_with_alignment` endpoint
- `get_translations` always returns only active translations/voices
- `clear_cache` uses API Key instead of JWT

### Database: `cep_public`

Content tables: `languages`, `bible_books`, `translations`, `translation_books`,
`translation_verses`, `translation_titles`, `translation_notes`, `voices`, and
`voice_alignments`. Operational tables include request statistics,
`translation_chunks` (RAG chunks, produced by `app/chunk_cli.py`, not part of
the admin-api import), `chunk_embeddings` (chunk vectors, produced by
`app/index_cli.py rebuild`; versioned by chunking version + embedding model)
and `psalm_verse_mappings` (Psalm versification: translation verse →
canonical english-masoretic coordinates, produced by
`app/versification_cli.py build`).

### Environment

**No silent defaults.** `app/config.py` fails fast: a missing required
variable, or a non-numeric value in any numeric variable, aborts startup with
a single `ConfigError` listing *every* problem at once (not one per restart).
An unset *operational* parameter still falls back to its documented default —
those are tuning knobs. Model names never do.

**Enforced always** (startup fails when unset or blank): `API_KEY`,
`DB_HOST`, `DB_USER`, `DB_NAME`, `EMBEDDING_MODEL`, `EMBEDDING_DIMENSIONS`
(must be ≥ 1). `DB_PASSWORD` must be *present* but may be empty (`DB_PASSWORD=`
is an explicit statement; MySQL accepts a passwordless user — both the local
and the production `.env` set a real password today). `DB_PORT` keeps its
default 3306; a non-numeric value is an error.

**Enforced when `GEMINI_API_KEY` is set** (no defaults in code): `AI_QUESTION_MODEL`,
`AI_TRANSCRIBE_MODEL`, `AI_SCRIPTURE_REWRITE_MODEL`,
`AI_SCRIPTURE_RERANK_MODEL`. (Reason: on 2026-08-29 a default
`AI_SCRIPTURE_REWRITE_MODEL=gemini-3.7-flash` sent the rewrite stage to a model
the key could not reach, while `.env` said flash-lite everywhere.)

`GEMINI_API_KEY` itself stays optional — without it the AI surface is "not
configured", the AI endpoints answer with their own error and the rest of the
API works as before, including `POST /api/ai/scripture`, which
degrades to the safe pool with `fallback_reason=ai_unavailable`. That answer
still reads the vector index, which is why `EMBEDDING_MODEL` /
`EMBEDDING_DIMENSIONS` are required with or without a key: they name the
index version (`c{chunking}:{model}@{dims}`), not a provider call.

**Operational, keep their defaults** (a malformed value is still an error):
`AUDIO_FILES_PATH` (`audio`), `AUDIO_BASE_URL` (`http://localhost:8000`),
`ADMIN_API_URL`, `ADMIN_API_KEY` (for import), `IMPORT_MAX_PAYLOAD_MB` (48),
`IMPORT_HTTP_TIMEOUT_SECONDS` (300), `AI_REQUESTS_PER_MINUTE`,
`AI_REQUESTS_PER_CLIENT_PER_MINUTE`, `AI_CLIENT_HMAC_KEY`,
`TRUSTED_PROXY_HOSTS` / `TRUSTED_PROXY_IPS` /
`TRUSTED_PROXY_DNS_TTL_SECONDS` (30) and the `AI_SCRIPTURE_*` knobs below.
`AUDIO_DIR` is read by docker-compose, not by the application.

Exactly two variables decide whether the AI surface works:
`GEMINI_API_KEY` (unset → `/api/ai/question` and `/api/ai/transcribe` answer
`502`) and `AI_CLIENT_HMAC_KEY` (unset → `503`, the per-client limiter fails
closed rather than dropping the limit). `AI_SCRIPTURE_REWRITE_API_KEY` is
optional and affects the rewrite stage only (see below). The limiters are
process-local, so production uses a single API worker.

RAG / scripture selection:

- `EMBEDDING_MODEL` (`gemini-embedding-001`) and `EMBEDDING_DIMENSIONS`
  (768) configure the vector index — required as a pair in every deployment;
  changing them changes the index version and requires
  `python app/index_cli.py rebuild`. `index_cli rebuild` also refuses to run
  without `GEMINI_API_KEY` (it would delete every stale-version row before
  discovering it cannot embed anything).
- `AI_SCRIPTURE_REWRITE_MODEL` (`gemini-3.7-flash`) — LLM query
  reformulation, the dominant quality lever; value pinned by the benchmark,
  deliberately independent of `AI_QUESTION_MODEL`, and required (ADR 0004).
- `AI_SCRIPTURE_REWRITE_API_KEY` (optional, no default) — API key for the
  **rewrite stage only**. Set: rewrites bill this key; unset or blank:
  rewrites bill `GEMINI_API_KEY` (one shared key — the previous behaviour and
  an operational default, not a hidden fallback: the *configured* behaviour
  is identical either way — same model, same prompt, same request — what
  differs is the quota and the bill, and therefore how often the stage is
  available rather than rejected with 429). Reason for the split: rewrite is
  pinned to `gemini-3.7-flash`, whose free daily quota the retrieval traffic
  exhausts, while embeddings and the rerank run on free lite-model quotas —
  so only this stage needs a paid key. Embeddings, `passage_rerank` and
  Twinkler keep reading `GEMINI_API_KEY`. Setting it while `GEMINI_API_KEY`
  is empty is a startup error (it would pay for one stage of a pipeline
  whose other stages have no key at all). Resolved once in
  `config.resolve_rewrite_api_key` → `config.REWRITE_API_KEY`, which is the
  default argument of `GeminiQueryRewriter`.
- `AI_SCRIPTURE_RERANK_MODEL` (`gemini-3.5-flash-lite`) — grounded choice
  of the final passage among candidates; value pinned by the benchmark,
  required (ADR 0005).
- `AI_SCRIPTURE_REQUESTS_PER_MINUTE` (10),
  `AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE` (3) — the selection
  endpoint's own rate-limit budget (separate from the Twinkler one; it reuses
  `AI_CLIENT_HMAC_KEY` for pseudonyms).
- `AI_SCRIPTURE_TIMEOUT_SECONDS` (15) — total budget of one selection;
  `AI_SCRIPTURE_INDEX_CACHE_SECONDS` (3600) — TTL of the in-process corpus cache,
  also dropped by `POST /api/cache/clear` (ADR 0006).
Trusted reverse proxies — **name the proxy, do not pin its address**
(`app/trusted_proxies.py`, ClickUp 86cbbq6vz):

- `TRUSTED_PROXY_HOSTS` (empty) — comma-separated **container / DNS names**
  of the reverse proxies whose `X-Forwarded-For` is believed, e.g.
  `bible-web`. Resolved through docker's embedded DNS at startup and
  re-resolved on a TTL, so an address change (reboot, resize, recreation) is
  picked up on its own. **This is the production setting.**
- `TRUSTED_PROXY_IPS` (empty) — literal addresses **and/or CIDR networks**
  (`127.0.0.1`, `172.18.0.0/16`). Still supported, still additive with the
  above. Networks are parsed strictly: `172.18.0.5/16` is a startup error,
  not a silent widening to the whole subnet.
- `TRUSTED_PROXY_DNS_TTL_SECONDS` (30) — how stale a resolved proxy address
  may get. One DNS call per TTL, never per request, and never **in** a
  request: the lookup runs on a background thread while the request path
  keeps answering from the previous snapshot. Floored at 1 s.
- Everything unset means "no proxy in front of this API": the peer address is
  the client and `X-Forwarded-For` is ignored. That is the local machine's
  configuration and a supported deployment, not a missing setting.
- Malformed entries abort startup naming the variable and the token, through
  the same aggregated `ConfigError` as everything else (a host name in
  `TRUSTED_PROXY_IPS` and an IP in `TRUSTED_PROXY_HOSTS` each point at the
  other variable).

- `AI_SCRIPTURE_PRIMARY_TRANSLATIONS` (empty) — per-language default translation
  of the selection endpoint, e.g. `ru=syn,en=bsb,uk=ubh` (`language=alias` or
  `language=code`, comma separated). Must name an INDEXED translation;
  entries that do not are ignored with a warning. Empty means the indexed
  translation with the lowest code (ADR 0007).

### Chapter coverage of a translation (ClickUp 86cbb2xxp, 2026-08-30)

`GET /api/translations/{code}/books` reports `chapters_count`,
`chapters_without_text` and (new) `has_text` per book. The expected structure
comes from **`app/canon.py`**, never from `translation_verses`:

- the base is the literal 66-book canon table (1189 chapters), verified
  against the three structurally complete translations in `cep_public`
  (`bsb`, `webus`, `webbe`);
- a translation that carries **more** than the canon keeps its own chapters:
  `chapters_count = max(canonical, this translation's last chapter)`, so
  `syn` still exposes Ps 151, Dan 13-14, 2 Chr 37 and `ubh` its Esth 11-12;
- a translation that divides the same text into **fewer** chapters needs an
  explicit entry in `TRANSLATION_CHAPTER_COUNTS` — today `("ubh", 39) = 3`
  (its Mal 3 holds canonical 3:1-18 + 4:1-6, so Mal 4 is not a hole);
- a book the translation declares in `translation_books` but ships **no text**
  for is still returned, with every canonical chapter in
  `chapters_without_text` and `has_text=false`. This is normal data, not an
  import failure: `npu` declares all 66 books and publishes the Psalms and
  the New Testament only (38 books without text, 779 chapters).

Reason for the table: the previous code took `SELECT max(chapter_number) FROM
translation_verses WHERE book_number = tb.book_number` — a subquery naming no
translation, so the maximum came from any translation in the shared table.
`bti` was reported to lack 26 chapters instead of 20 (the extras were
deuterocanonical chapters of `syn` and `ubh`), `syn` was reported to lack
Esth 11-12 and `ubh` to lack 2 Chr 37 and Ps 151. The same subquery fed
`chapters_count` in `app/excerpt.py`, which drives `prev_excerpt`/
`next_excerpt`: `bti` Ps 150 pointed at a non-existent Ps 151, `ubh` Mal 3 at
a non-existent Mal 4. `cep_public` cannot answer the question itself —
`bible_books` stores only `verse_count` — hence a reviewed constant in code.

`has_text` is the one additive field of the fix (default `true`); everything
else in `TranslationBookModel` is unchanged. Tests: `tests/test_translation_books.py`
(SQLite stand-in for `cep_public` holding several canons at once).

### Navigation across books without text (ClickUp 86cbbpc6v, 2026-08-30)

`prev_excerpt`/`next_excerpt` used to step into the neighbouring **book
number** whenever a chapter boundary was crossed, checking only its
`chapters_count`. For a translation that publishes part of the canon that
meant dead ends: `npu mat 1` offered `mal 4` and `npu psa 150` offered
`pro 1`, both answered 422 "No verses found".

Navigation now walks to the nearest book **that has text in this
translation** (`app/excerpt.py`, `get_adjacent_book_with_text`):
`npu psa 150` → `mat 1`, `npu mat 1` → `psa 150`. When no published book
remains on that side the field is empty, exactly as at the ends of the Bible
(`gen 1` prev, `rev 22` next) — so `npu psa 1` prev is `''`.

- "Has text" is not a second definition: `get_books_info` marks each book
  `has_text` from the `translation_max_chapter` it already selects, the same
  predicate `GET /api/translations/{code}/books` reports.
- **One query per boundary crossing**, whatever the distance: the book list
  is read once and searched in memory, so skipping npu's 38 text-less Old
  Testament books costs what stepping into the next book costs (fewer queries
  than before, in fact: the old path spent 2-3). No walk of one query per
  book. Guarded by a query-counting test.
- Translations that publish the whole canon are unaffected — verified by
  replaying every book boundary of every active translation against the
  pre-fix code on live `cep_public`: `syn`, `bti`, `bsb`, `webus`, `ubh`,
  `webbe` answer identically; only `npu` changes.
- `get_book_number`/`get_book_alias` are gone: the alias and number of both
  the current and the target book come from `get_books_info`, which the
  navigation already needs.

### Import: a full resync is a sequence of point imports (ClickUp 86cbbq5zp, 2026-08-30)

`GET /api/import` without parameters used to be one request: `GET /api/data`
returned the whole export as a single **147 MB** JSON document, `response.json()`
turned it into Python objects, and the importer then built insert tuples for
197 614 verses and 259 663 alignments — in the one-worker container that shares
the production VM with MySQL. On 2026-08-30 the VM stopped answering during a
full resync. Measured on the local machine before the fix: peak RSS of the
importing worker **972 MB** against 82 MB idle. Point imports
(`?translation=`, 18-29 MB) always passed.

So the full resync now *is* a sequence of point imports:

1. `GET /api/data/manifest` (Dashboard-API, new) — a few kilobytes: the
   reference tables in full, `code`+`alias` of every active translation, and
   the expected row count per table. This is the work list.
2. `languages` / `bible_books` — `REPLACE INTO`, one transaction.
3. every translation, **one at a time**: fetch its export, then
   `delete_translation_data` + `INSERT` inside **its own transaction**. Peak
   memory is set by the largest translation (`syn`, 29 MB), not by the corpus
   — on both sides, because the 147 MB document was materialised inside
   admin-api too, on the same VM.
4. translations `cep_public` still holds that admin-api no longer publishes —
   dropped, one transaction each, and named in the report. **Only with
   `?allow_removals=1`** (see below).
5. an orphan sweep (rows whose parent is gone) and stale reference rows.
   `TRUNCATE` used to remove these implicitly; a resync built out of point
   imports has to say it.
6. `SELECT COUNT(*)` against the manifest — per table **and per translation**,
   reported in the response.

**There is no `TRUNCATE` anywhere any more.** That was the second half of the
incident: `TRUNCATE` is DDL, it commits implicitly, so "truncate everything,
then insert" left every table empty the moment the process died, with nothing
to roll back. Now an abort at any point leaves every other translation exactly
as it was and the interrupted one at its previous contents. Proved two ways:
`tests/test_import_data.py` injects a failure at an exact statement against a
recording stand-in connection, and a live `kill -9` of the worker mid-resync
left `cep_public` complete (7 translations, 197 614 verses, 256 492
alignments).

Measured after the fix, same machine, same data: peak RSS **215 MB** (idle
87 MB), wall time 149 s (was 172 s — the per-translation queries are cheaper
on the admin side than one giant `IN`-list plus a 147 MB serialization).

Safety valve: `IMPORT_MAX_PAYLOAD_MB` (**48**) caps the body of **one**
`GET /api/data` response, enforced while streaming (`Content-Length` when the
server sends one, and always against the bytes received). A trip is a loud
`507` naming the variable, raised **before** that translation's rows are
touched — the fetch happens outside the transaction on purpose. 48 and not 96
(review of this ticket): the largest real payload is `syn` at 29.3 MB, parsing
costs several times the body in RSS, and on the 2-4 GB production VM that also
runs MySQL a 96 MB body could OOM-kill the worker *before* it answered 507 —
the very failure the valve replaces. And a manifest declaring **zero** active
translations is refused with `502`: an empty source is a broken source, never
an instruction to publish an empty Bible (the old code would have truncated
everything and inserted nothing).

**Verification is per translation, not only in total.** Global totals pass on
compensating errors — a hundred verses too many in one translation and a
hundred too few in another sum to the expected number, while a real
translation lost real text. The manifest carries `counts.per_translation`, so
each translation is counted in each of its seven tables and the
*disagreements* are reported in `translation_mismatches`
(`{alias: {table: {expected, actual, ok}}}`, empty when everything matched).
A manifest that lists a translation it has no counts for is a `502` before any
write: an unverifiable translation is a broken source.

**Removing a translation takes `?allow_removals=1`.** Step 4 above deletes
from `cep_public` every translation the manifest does not list — and a
translation goes missing from the manifest far more often by accident
(`active = 0` clicked in the dashboard, a filter bug on the admin side) than
by decision. A resync that is about to remove **at least one** translation
therefore removes **none** without the flag: it answers `200` with
`status="removals_rejected"`, the aliases in `removals_rejected`, and `detail`
naming them and the parameter. The import itself (step 2) is *not* undone —
the translations that were imported are correct and current, which is why this
is an honest report rather than an exception. Re-run with `?allow_removals=1`
to actually drop them; then it is the previous behaviour, with the aliases in
`translations_removed`. **Operationally:** a production resync answering
`removals_rejected` is the signal to check `cep_admin` before doing anything
else — never to add the flag reflexively.

Response contract of `/api/import` — additive, `status`/`translation`/`tables`
unchanged: `translations_imported`, `translations_removed`,
`removals_rejected`, `orphans_removed`, `verification`
(`{table: {expected, actual, ok}}`), `translation_mismatches`, `detail` and
`duration_seconds`. `status` is `"ok"` only when every count matches, per
table and per translation; `"removals_rejected"` when the gate above stopped a
removal; otherwise `"mismatch"`, with the data written and `verification` /
`translation_mismatches` saying what disagrees. Callers that tested
`status == "ok"` keep working and now catch more.

`?translation=` is unchanged in behaviour — same delete+insert, same
`REPLACE INTO` for the reference tables — and gains the size valve, one
explicit transaction, and a translation-scoped count check of **all seven**
tables it writes (it checked three until the review of this ticket, leaving
`voice_alignments` — the largest table, and the one the manual fixes are
delivered in — unverified). `allow_removals` is meaningless for a point import
and ignored there.

### Trusted proxies survive a reboot, and stop failing silently (ClickUp 86cbbq6vz, 2026-08-30)

`TRUSTED_PROXY_IPS` used to be one hardcoded container address. Docker hands
addresses out in start order and they do **not** survive a VM reboot or
resize: after the 2026-08-30 hard reboot the production containers came back
in a different order, `172.18.0.5` became MySQL instead of nginx, and
Bible-API silently stopped believing `X-Forwarded-For` from the only proxy in
front of it. Measured consequences: `api_requests` recorded every caller under
the nginx container's address (one client, statistically), and
`AI_REQUESTS_PER_CLIENT_PER_MINUTE` became a **global** limit, because every
caller hashed to the same pseudonym. **No errors in the logs** — the failure
was completely silent, and the fix was a manual one-line `.env` edit after
every reboot.

**Trust a name, not an address.** `app/trusted_proxies.py` owns a
`TrustedProxies` object built from the three variables above and answers one
question: `is_trusted(peer)`. Names in `TRUSTED_PROXY_HOSTS` are resolved
through docker's embedded DNS at startup and re-resolved whenever the cached
answer is older than `TRUSTED_PROXY_DNS_TTL_SECONDS`, so the address the proxy
holds *right now* is the address that is trusted — a reboot costs at most one
TTL of misattributed statistics and **no `.env` edit at all**.

**Resolution is stale-while-revalidate, and that is correctness, not speed.**
`is_trusted` is called in the request path of an async application and
`socket.getaddrinfo` is a blocking call: a name that falls outside docker's
DNS and reaches an unresponsive upstream would park the **whole event loop**
— every request in flight, not just this one — for the length of the lookup,
once per TTL. So the request path never resolves. It reads the current
snapshot and returns; when that snapshot is older than the TTL it hands the
work to a single background daemon thread (at most one in flight, whatever
the request rate) whose result atomically replaces the snapshot. Trust
follows a moved proxy within one TTL plus one lookup, and a hung resolver
costs staleness only. The one synchronous resolution is the cold one, at
startup, in `log_startup_state()` — there is no previous snapshot to serve
then, and answering "trust nobody" while a lookup is pending is the failure
this module exists to prevent.

Why not the alternatives (both were considered, both remain possible):

- **Static IPs in the prod compose (`ipam`)** — makes the address a declared
  fact rather than an accident, and would also fix nginx's cached upstreams.
  But it is a prod-compose change that has to be kept in sync by hand, it
  pins the subnet layout, and above all it keeps the failure *silent*: the
  day the declaration and reality disagree, nothing says so. Not chosen as
  the primary mechanism; it composes with this one if it is ever wanted.
- **Trusting the whole docker subnet** (`TRUSTED_PROXY_IPS=172.18.0.0/16`) —
  supported by the CIDR parsing, and it does survive reboots. Honest
  downside: it trusts *every* container on that network (MySQL, admin-api,
  anything added later), so a single compromised container could forge
  `X-Forwarded-For` and both poison the statistics and evade the per-client
  AI rate limit by minting a new client address per request. A name resolves
  to the proxy alone and costs nothing extra, so the subnet is documented as
  an escape hatch, not the recommendation.

**And the failure is loud now.** Four places, none of them per-request:

1. **Startup banner**, always: `Trusted proxies: hosts=[bible-web->172.18.0.4]`
   or `Trusted proxies: none configured — ...`. It is emitted through a
   handler this module installs when nothing else has configured logging:
   uvicorn leaves the root logger without handlers, so an application `INFO`
   record would otherwise be swallowed and only `WARNING`+ would reach
   `docker logs` (which is why the banner is worth grepping for after a
   deploy: `docker logs bible-api | grep 'Trusted prox'`).
2. **Address change**, `WARNING`:
   `Trusted proxy host 'bible-web' changed address: 172.18.0.5 -> 172.18.0.4`.
   The reboot, narrated.
3. **A host that stops resolving**, `ERROR`, once per state change (not once
   per TTL — it keeps retrying quietly and shouts again only when the state
   changes).
4. **`X-Forwarded-For` from an untrusted peer**, `ERROR`, once per peer per
   5 minutes (`client_ip.FORWARDING_LOG_INTERVAL_SECONDS`, table capped at 64
   peers, oldest evicted). That header from an unexpected peer *is* the
   signature of the incident. With no proxy configured at all the same event
   is a `WARNING` instead — locally it means someone sent a forged header to a
   directly exposed API, which is information, not a misconfiguration.
   Because the trigger is a request header, the *volume* of this log would
   otherwise be chosen by whoever sends the requests: a second, global ceiling
   of **10 lines per 5 minutes** (`client_ip._MAX_REPORTS_PER_INTERVAL`)
   caps it however many distinct peers appear. Peers silenced by that ceiling
   are not recorded as "already reported", so the real misconfigured proxy is
   reported again as soon as there is budget.

**Which element of `X-Forwarded-For` is the client: the RIGHTMOST one.**
Production nginx sets `X-Forwarded-For $proxy_add_x_forwarded_for` in every
`location`, and that variable **keeps the header the client sent** and appends
`$remote_addr` to it. Reading the leftmost element (as the first cut of this
ticket did) therefore let any caller simply *declare* its address: `curl -H
'X-Forwarded-For: 1.2.3.4'` was recorded as 1.2.3.4 in `api_requests`, and a
fresh value per request walked straight past
`AI_REQUESTS_PER_CLIENT_PER_MINUTE`, whose bucket is keyed by that address.
Only the part *our own* proxies appended can be believed.

`client_ip.client_from_forwarded` implements the standard rule: walk the list
right to left, skip addresses that are themselves trusted proxies, and the
first address that is not one is the client. With today's single hop that is
just the rightmost element, but written as a walk it stays correct if a second
trusted hop is ever added. A malformed or empty element (nginx never produces
one) means the header cannot be read as a hop chain, so it is not read at all
and the direct peer is the client — the same conservative answer as an
untrusted peer.

**Pseudonyms change.** Requests whose forged left element used to be recorded
now hash from the address nginx actually saw, so `api_requests.client` values
differ before and after this change for any caller that sent its own
`X-Forwarded-For`. That is the point of the fix; a report crossing the deploy
should not treat the two as the same client identity.

Startup is deliberately **not** aborted when a configured host does not
resolve: the proxy may simply be slower to come up, and taking the whole
public API down over statistics accuracy is the wrong trade. The error is
logged and every TTL retries.

Tests: `tests/test_trusted_proxies.py` (64) — parsing and its startup errors,
matching by address/CIDR/name, the reboot itself (a fake resolver whose answer
changes, trust follows within one TTL), each log line firing exactly once, the
spoofing cases (`X-Forwarded-For` from an untrusted peer is ignored and the
peer is the client; a forged left element is ignored and the right one wins),
the request path answering instantly while a deliberately hung resolver is
blocked on a background thread, and the two bounds on the untrusted-peer log
(oldest peer evicted, global ceiling per interval).

### All API routes are under `/api` prefix

### AI routes renamed on 2026-08-30 (ClickUp 86cbbmwjk)

`POST /api/twinkler/v1/complete` → `POST /api/ai/question`,
`POST /api/twinkler/v1/transcribe` → `POST /api/ai/transcribe`,
`POST /api/scripture/v1/select` → `POST /api/ai/scripture`. Paths only:
bodies, responses, headers, authentication and rate limits are unchanged.
The old paths return 404 — there are no aliases, because the only client is
one unpublished app (Twinkler-Mobile, renamed in a paired ticket).
`GET /api/scripture/v1/translations` was deleted outright: the app chooses
its translation once and the selection renders any accepted code (ADR 0007),
so the catalogue had no consumer.

Everything the client sees in `/docs` was renamed with the paths: the three
methods share one tag **`AI`** (the `Twinkler` and `Scripture` tags are
gone), and their `operationId`s are `ai_question`, `ai_transcribe` and
`ai_scripture`. The word "twinkler" no longer appears anywhere in the
generated spec. No generated clients depended on the old IDs — the mobile
client is handwritten.

Deliberately NOT renamed (out of scope): the modules `twinkler_ai.py` and
`scripture_select.py` and the handler functions — these are internal and
invisible to API consumers. The environment variables were renamed the
following day (below).

### Env variables renamed on 2026-08-30 (ClickUp 86cbbmy8d)

Names now mirror the method they configure: `AI_*` for the whole AI surface,
`AI_SCRIPTURE_*` for the selection pipeline only.

| Old | New |
| --- | --- |
| `GEMINI_MODEL` | `AI_QUESTION_MODEL` |
| `GEMINI_TRANSCRIPTION_MODEL` | `AI_TRANSCRIBE_MODEL` |
| `GEMINI_REQUESTS_PER_MINUTE` | `AI_REQUESTS_PER_MINUTE` |
| `GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE` | `AI_REQUESTS_PER_CLIENT_PER_MINUTE` |
| `TWINKLER_CLIENT_HMAC_KEY` | `AI_CLIENT_HMAC_KEY` |
| `RETRIEVAL_REWRITE_MODEL` | `AI_SCRIPTURE_REWRITE_MODEL` |
| `RETRIEVAL_RERANK_MODEL` | `AI_SCRIPTURE_RERANK_MODEL` |
| `RETRIEVAL_REWRITE_API_KEY` | `AI_SCRIPTURE_REWRITE_API_KEY` |
| `SCRIPTURE_SELECT_REQUESTS_PER_MINUTE` | `AI_SCRIPTURE_REQUESTS_PER_MINUTE` |
| `SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE` | `AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE` |
| `SCRIPTURE_SELECT_TIMEOUT_SECONDS` | `AI_SCRIPTURE_TIMEOUT_SECONDS` |
| `SCRIPTURE_INDEX_CACHE_SECONDS` | `AI_SCRIPTURE_INDEX_CACHE_SECONDS` |
| `SCRIPTURE_PRIMARY_TRANSLATIONS` | `AI_SCRIPTURE_PRIMARY_TRANSLATIONS` |
| `MP3_FILES_PATH` | `AUDIO_FILES_PATH` |
| `TWINKLER_SYSTEM_PROMPT` | **deleted** — now `app/question_prompt.py` |

Unchanged: `API_KEY`, `DB_*`, `ADMIN_API_URL/KEY`, `GEMINI_API_KEY`,
`EMBEDDING_MODEL`, `EMBEDDING_DIMENSIONS`, `TRUSTED_PROXY_IPS`,
`AUDIO_BASE_URL`, `AUDIO_DIR` (docker-compose host path), `OPENROUTER_API_KEY`
(benchmark only).

Values were **not** touched, only keys — in particular the HMAC key, so
client pseudonyms in `api_requests` stay stable across the rename (the digest
is keyed by the value; the variable name never enters it). There are no
aliases for the old names on purpose: a place the rename missed aborts
startup naming the variable it wants, which is ADR 0008's aggregated error
doing its job. Production `.env` is rebuilt with the new names at deploy time
(checklist 86cb8wdp4), not by this change.

**`TWINKLER_SYSTEM_PROMPT` is gone.** The system prompt of
`/api/ai/question` is the versioned constant `QUESTION_PROMPT` in
`app/question_prompt.py` (`QUESTION_PROMPT_VERSION = 1`), carried over from
the local `.env` byte for byte. A prompt is product behaviour, not a
deployment knob: as a variable it could silently differ between local and
production and every test run had to inject a stand-in. It is public from
this date (public repository, owner-approved: never a secret, no key
material). The old runtime guards "prompt is not configured" / "too long"
were removed from `complete()` — a reviewed literal cannot violate them —
and their invariants are asserted in `tests/test_twinkler_ai.py` instead.

The request statistics store the path verbatim, so `api_requests` and
`api_request_daily_stats` carry the old names before the rename and the new
ones after it; the rows were not rewritten. A report crossing 2026-08-30
must union both names (see `architect/scripture-select.md`).
