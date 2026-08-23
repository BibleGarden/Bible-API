# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Public API — a read-only FastAPI service for the Bible Garden iOS app. Works with the `cep_public` database, which contains only finalized data. Data is imported from admin-api via `GET /api/import`.

## Common Commands

### Run / Build
```bash
docker compose up -d --build
docker logs public-api -f
docker compose down
```

### OpenAPI Spec
```bash
docker exec public-api bash -c "cd /code && PYTHONPATH=app python3 extract-openapi.py app.main:app"
```

## Architecture

### Application Structure (`app/`)

- **`main.py`** — FastAPI app entry point, languages/translations/books endpoints, `timed_cache` decorator
- **`excerpt.py`** — Core content endpoint: `excerpt_with_alignment`. No COALESCE, no voice_manual_fixes (manual fixes already applied during import)
- **`audio.py`** — MP3 file serving with HTTP Range request support
- **`about.py`** — About page content
- **`version_check.py`** — App version check
- **`import_data.py`** — Import data from admin-api
- **`lampada_ai.py`** — Server-prompted Gemini integration with distributed rate limiting
- **`auth.py`** — Only API Key authentication (no JWT)
- **`models.py`** — Pydantic response models (no admin models)
- **`database.py`** — MySQL connection factory
- **`config.py`** — Environment variable loading

### Key Differences from admin-api

- No JWT authentication, no admin endpoints
- No `voice_manual_fixes` table — fixes are baked into `voice_alignments` during import
- No `voice_anomalies`, `voice_chapters`, `bible_stat` tables
- No `chapter_with_alignment` endpoint
- `get_translations` always returns only active translations/voices
- `clear_cache` uses API Key instead of JWT

### Database: `cep_public`

Content tables: `languages`, `bible_books`, `translations`, `translation_books`,
`translation_verses`, `translation_titles`, `translation_notes`, `voices`, and
`voice_alignments`. Operational tables include request statistics and
`lampada_rate_limit_events`.

### Environment

Required env vars: `API_KEY`, `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `AUDIO_DIR` (host path), `MP3_FILES_PATH` (container path).
Optional: `ADMIN_API_URL`, `ADMIN_API_KEY` (for import), `GEMINI_API_KEY`,
`GEMINI_MODEL`, `GEMINI_REQUESTS_PER_MINUTE`,
`GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE`, `LAMPADA_SYSTEM_PROMPT`,
`LAMPADA_CLIENT_HMAC_KEY`, and `TRUSTED_PROXY_IPS`. `GEMINI_API_KEY`,
`LAMPADA_SYSTEM_PROMPT`, and `LAMPADA_CLIENT_HMAC_KEY` must be set for Lampada
AI calls. Apply `sql/001_create_lampada_rate_limit_events.sql` before deploy.

### All API routes are under `/api` prefix
