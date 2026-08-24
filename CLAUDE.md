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

## Architecture

### Application Structure (`app/`)

- **`main.py`** — FastAPI app entry point, languages/translations/books endpoints, `timed_cache` decorator
- **`excerpt.py`** — Core content endpoint: `excerpt_with_alignment`. No COALESCE, no voice_manual_fixes (manual fixes already applied during import)
- **`audio.py`** — MP3 file serving with HTTP Range request support
- **`about.py`** — About page content
- **`version_check.py`** — App version check
- **`import_data.py`** — Import data from Dashboard-API
- **`twinkler_ai.py`** — Server-prompted Gemini integration with in-memory rate limiting
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
- **`retrieval.py`** — retrieval pipeline: interleave fusion, global genre blacklist (`data/genre_blacklist.json`), safe pool (`data/safe_pool.json`), diversity, `ScriptureRetriever` service; `select_final` adds the grounded rerank with top-1 fallback
- **`passage_rerank.py`** — grounded AI choice of the final passage among retrieval candidates: validated index-only answer, JSON schema, injection-hardened prompt (see `architect/adr/0005-grounded-passage-rerank.md`)
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

Required env vars: `API_KEY`, `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `AUDIO_DIR` (host path), `MP3_FILES_PATH` (container path).
Optional: `ADMIN_API_URL`, `ADMIN_API_KEY` (for import), `GEMINI_API_KEY`,
`GEMINI_MODEL`, `GEMINI_TRANSCRIPTION_MODEL`, `GEMINI_REQUESTS_PER_MINUTE`,
`GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE`, `TWINKLER_SYSTEM_PROMPT`,
`TWINKLER_CLIENT_HMAC_KEY`, and `TRUSTED_PROXY_IPS`. `GEMINI_API_KEY`,
`TWINKLER_SYSTEM_PROMPT`, and `TWINKLER_CLIENT_HMAC_KEY` must be set for Twinkler
AI calls. The limiter is process-local, so production uses a single API worker.
`EMBEDDING_MODEL` (default `gemini-embedding-001`) and `EMBEDDING_DIMENSIONS`
(default 768) configure the RAG vector index; changing them changes the index
version and requires `python app/index_cli.py rebuild`.

### All API routes are under `/api` prefix
