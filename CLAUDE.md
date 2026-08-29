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
docker exec -e API_KEY=test-api-key \
  -e TWINKLER_SYSTEM_PROMPT="Серверная система" \
  -e TWINKLER_CLIENT_HMAC_KEY=test-hmac-key \
  bible-api pytest -q
```
`tests/conftest.py` supplies the model and DB variables the fail-fast config
now requires (via `setdefault`, so real values still win), which is why no
extra `-e` overrides are needed.

## Architecture

### Application Structure (`app/`)

- **`main.py`** — FastAPI app entry point, languages/translations/books endpoints, `timed_cache` decorator
- **`excerpt.py`** — Core content endpoint: `excerpt_with_alignment`. No COALESCE, no voice_manual_fixes (manual fixes already applied during import)
- **`audio.py`** — MP3 file serving with HTTP Range request support
- **`about.py`** — About page content
- **`version_check.py`** — App version check
- **`import_data.py`** — Import data from Dashboard-API
- **`twinkler_ai.py`** — Server-prompted Gemini integration with in-memory rate limiting
- **`scripture_select.py`** — Public scripture-selection endpoints `POST /api/scripture/v1/select` over `retrieval.select_final` and `GET /api/scripture/v1/translations` (renderable-translation catalogue); owns the process-local corpus cache: vector + BM25 indexes, Psalm maps, catalogue, coverage sets (see `architect/scripture-select.md`, `architect/adr/0006-scripture-select-api.md`, `architect/adr/0007-reference-translation-rendering.md`)
- **`passage_render.py`** — renders a canonical passage window in a translation that has no chunk corpus (coordinates through `psalm_verse_mappings`, text from `translation_verses` with `chunking.build_text` semantics) and builds the per-translation coverage sets used to filter candidates before the rerank (ADR 0007)
- **`rate_limit.py`** — Shared in-memory rolling-window limiter (Twinkler + scripture selection)
- **`deadline.py`** — Per-request time budget threaded through the AI stages
- **`prompt_safety.py`** — Neutralizes forged prompt data-block delimiters (invisible characters and angle-bracket look-alikes) in user text
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

**Enforced when `GEMINI_API_KEY` is set** (no defaults in code): `GEMINI_MODEL`,
`GEMINI_TRANSCRIPTION_MODEL`, `RETRIEVAL_REWRITE_MODEL`,
`RETRIEVAL_RERANK_MODEL`. (Reason: on 2026-08-29 a default
`RETRIEVAL_REWRITE_MODEL=gemini-3.7-flash` sent the rewrite stage to a model
the key could not reach, while `.env` said flash-lite everywhere.)

`GEMINI_API_KEY` itself stays optional — without it the AI surface is "not
configured", the AI endpoints answer with their own error and the rest of the
API works as before, including `POST /api/scripture/v1/select`, which
degrades to the safe pool with `fallback_reason=ai_unavailable`. That answer
still reads the vector index, which is why `EMBEDDING_MODEL` /
`EMBEDDING_DIMENSIONS` are required with or without a key: they name the
index version (`c{chunking}:{model}@{dims}`), not a provider call.

**Operational, keep their defaults** (a malformed value is still an error):
`MP3_FILES_PATH` (`audio`), `AUDIO_BASE_URL` (`http://localhost:8000`),
`ADMIN_API_URL`, `ADMIN_API_KEY` (for import), `GEMINI_REQUESTS_PER_MINUTE`,
`GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE`, `TWINKLER_SYSTEM_PROMPT`,
`TWINKLER_CLIENT_HMAC_KEY`, `TRUSTED_PROXY_IPS` and the `SCRIPTURE_*` knobs
below. `AUDIO_DIR` is read by docker-compose, not by the application.
`GEMINI_API_KEY`, `TWINKLER_SYSTEM_PROMPT`, and `TWINKLER_CLIENT_HMAC_KEY`
must be set for Twinkler AI calls. `RETRIEVAL_REWRITE_API_KEY` is optional
and affects the rewrite stage only (see below). The limiters are process-local, so
production uses a single API worker.

RAG / scripture selection:

- `EMBEDDING_MODEL` (`gemini-embedding-001`) and `EMBEDDING_DIMENSIONS`
  (768) configure the vector index — required as a pair in every deployment;
  changing them changes the index version and requires
  `python app/index_cli.py rebuild`. `index_cli rebuild` also refuses to run
  without `GEMINI_API_KEY` (it would delete every stale-version row before
  discovering it cannot embed anything).
- `RETRIEVAL_REWRITE_MODEL` (`gemini-3.7-flash`) — LLM query
  reformulation, the dominant quality lever; value pinned by the benchmark,
  deliberately independent of `GEMINI_MODEL`, and required (ADR 0004).
- `RETRIEVAL_REWRITE_API_KEY` (optional, no default) — API key for the
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
- `RETRIEVAL_RERANK_MODEL` (`gemini-3.5-flash-lite`) — grounded choice
  of the final passage among candidates; value pinned by the benchmark,
  required (ADR 0005).
- `SCRIPTURE_SELECT_REQUESTS_PER_MINUTE` (10),
  `SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE` (3) — the selection
  endpoint's own rate-limit budget (separate from the Twinkler one; it reuses
  `TWINKLER_CLIENT_HMAC_KEY` for pseudonyms).
- `SCRIPTURE_SELECT_TIMEOUT_SECONDS` (15) — total budget of one selection;
  `SCRIPTURE_INDEX_CACHE_SECONDS` (3600) — TTL of the in-process corpus cache,
  also dropped by `POST /api/cache/clear` (ADR 0006).
- `SCRIPTURE_PRIMARY_TRANSLATIONS` (empty) — per-language default translation
  of the selection endpoint, e.g. `ru=syn,en=bsb,uk=ubh` (`language=alias` or
  `language=code`, comma separated). Must name an INDEXED translation;
  entries that do not are ignored with a warning. Empty means the indexed
  translation with the lowest code (ADR 0007).

### All API routes are under `/api` prefix
