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
- **`auth.py`** — Only API Key authentication (no JWT)
- **`models.py`** — Pydantic response models (no admin models)
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

9 tables: `languages`, `bible_books`, `translations`, `translation_books`, `translation_verses`, `translation_titles`, `translation_notes`, `voices`, `voice_alignments`

### Environment

Required env vars: `API_KEY`, `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `AUDIO_DIR` (host path), `MP3_FILES_PATH` (container path).
Optional: `ADMIN_API_URL`, `ADMIN_API_KEY` (for import).

### All API routes are under `/api` prefix
