from typing import Union, Optional
from datetime import timedelta, datetime
from functools import wraps
import hashlib
import json
import logging

from fastapi import FastAPI, HTTPException, status, APIRouter
from canon import chapter_coverage
from database import create_connection
from models import *

from fastapi.routing import APIRoute

from excerpt import router as excerpt_router
from excerpt import check_audio_file_exists
from audio import router as audio_router
from about import router as about_router
from version_check import router as version_check_router
from import_data import router as import_data_router
from twinkler_ai import router as twinkler_ai_router
from scripture_select import router as scripture_select_router
from scripture_select import clear_cached_resources, validation_exception_handler
from fastapi.exceptions import RequestValidationError
from auth import RequireAPIKey
from middleware import RequestStatsMiddleware
from trusted_proxies import TRUSTED_PROXIES, ensure_visible_handler
from config import (
    EMBEDDING_DIMENSIONS,
    EMBEDDING_MODEL,
    EMBEDDING_MODEL_PATH,
    EMBEDDING_PROVIDER,
    EMBEDDING_PROVIDER_LOCAL,
    AI_TRANSCRIBE_MODEL_PATH,
    QUESTION_PROVIDER,
    SCRIPTURE_REWRITE_PROVIDER,
    SCRIPTURE_RERANK_PROVIDER,
    TRANSCRIBE_PROVIDER,
)
from embeddings import load_embedding_model
from llm_client import endpoint_host
from transcription import load_transcription_model

logger = logging.getLogger(__name__)

# Simple in-memory cache with TTL
_cache = {}
_cache_timestamps = {}

def timed_cache(seconds: int = 3600):
    """Decorator for caching function results with TTL"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            cache_key = f"{func.__name__}:{hashlib.md5(json.dumps([args, kwargs], sort_keys=True, default=str).encode()).hexdigest()}"

            # Check if cached value exists and is not expired
            if cache_key in _cache:
                timestamp = _cache_timestamps.get(cache_key)
                if timestamp and (datetime.now() - timestamp).total_seconds() < seconds:
                    return _cache[cache_key]

            # Call function and cache result
            result = func(*args, **kwargs)
            _cache[cache_key] = result
            _cache_timestamps[cache_key] = datetime.now()

            return result
        return wrapper
    return decorator

# Tags metadata for controlling order in Swagger UI
tags_metadata = [
    {
        "name": "Languages",
        "description": "",
    },
    {
        "name": "Translations",
        "description": "",
    },
    {
        "name": "Excerpts",
        "description": "",
    },
    {
        "name": "Audio",
        "description": "Streaming & Download mp3",
    },
    {
        "name": "About",
        "description": "Project information",
    },
    {
        "name": "Version",
        "description": "App version check",
    },
    {
        "name": "Import",
        "description": "Data import from admin-api",
    },
    {
        "name": "Cache",
        "description": "Cache management",
    },
    {
        "name": "AI",
        "description": (
            "Server-side AI: companion replies, voice transcription and "
            "contextual Bible passage selection for a prayer context"
        ),
    },
]

app = FastAPI(
    openapi_tags=tags_metadata,
    title="Public API",
    description="Public API for the Bible Garden iOS app",
    version="0.1.0",
    swagger_ui_parameters={
        "deepLinking": True,
        "displayRequestDuration": True,
        "defaultModelsExpandDepth": 0,
        "tryItOutEnabled": True,
    }
)

app.add_middleware(RequestStatsMiddleware)

# Say out loud, once, whose X-Forwarded-For this process believes. Silence
# here is what made the 2026-08-30 proxy-address reshuffle invisible: a
# mismatched trust setting produces no errors at all, only wrong statistics
# and a per-client rate limit that acts globally (ClickUp 86cbbq6vz).
TRUSTED_PROXIES.log_startup_state()


def log_ai_providers() -> None:
    """One line per chat stage saying who answers it (ADR 0009).

    Which model served a request is the question the 2026-08-29 incident was
    debugged blind for, and a provider is one level above a model — so it is
    said out loud, once, next to the trusted-proxy banner. Never the key
    itself: only the host and whether a key is configured.
    `grep 'AI stage'` after a deploy.

    The handler is ensured for THIS logger: `trusted_proxies` installs one on
    its own logger, not on the root, so an INFO record from `main` would
    otherwise be dropped by logging's last-resort handler under uvicorn — an
    invisible banner, which is the same failure the proxy banner exists to
    prevent.
    """
    ensure_visible_handler(logger)
    for stage in (
        QUESTION_PROVIDER,
        SCRIPTURE_REWRITE_PROVIDER,
        SCRIPTURE_RERANK_PROVIDER,
        TRANSCRIBE_PROVIDER,
    ):
        if stage.is_openai_compat:
            where = f" at {endpoint_host(stage.endpoint) or '<no endpoint>'}"
        elif stage.is_local:
            where = f" from {AI_TRANSCRIBE_MODEL_PATH or '<no path>'}"
        else:
            where = ""
        logger.info(
            "AI stage %s: provider=%s model=%s%s key=%s",
            stage.stage,
            stage.provider or "<none: AI not configured>",
            stage.model or "<none>",
            where,
            "set" if stage.api_key else "none",
        )


log_ai_providers()


def load_local_transcription_model() -> None:
    """Pay for the Whisper weights here, or fail the start (ADR 0012).

    The same trade as the embedding model above and for the same reason: a
    weights volume that is missing, unreadable or holding something else is a
    deployment error, and loading it lazily would answer the first voice
    message with a 502 that looks like a provider being briefly down.

    Nothing happens on the other two providers — a deployment on `gemini` or
    on the remote audio server never imports faster-whisper at all.
    """
    if not TRANSCRIBE_PROVIDER.is_local:
        return
    load_transcription_model()


load_local_transcription_model()

# The transcription timings are `INFO` records from `app/transcription.py`,
# and uvicorn leaves the root logger bare — so without a handler of their own
# they never reach `docker logs`, and the documented
# `docker logs bible-api | grep 'transcription:'` (how the audio-to-CPU ratio
# is checked on a new machine, ADR 0012) would find nothing at all. Same call,
# same reason, as the two banners above.
ensure_visible_handler(logging.getLogger("transcription"))


def log_and_load_embedding_provider() -> None:
    """Say which vector space this process searches, and load it if local.

    Pays for the 2.3 GB of bge-m3 weights here, not in the first request.

    Deliberately at import time and deliberately fatal (ADR 0010): a weights
    volume that is missing, unreadable or holding another model is a
    deployment error of exactly the kind ADR 0008 wants loud. Loading it
    lazily would answer the first prayer with
    `fallback_reason=ai_unavailable` and look, from outside, like a provider
    being briefly down.

    Nothing happens on `EMBEDDING_PROVIDER=gemini`, which is why the test
    suite — and every deployment that has not migrated — never imports torch
    at all.
    """
    ensure_visible_handler(logger)
    # Said out loud whichever provider it is, next to the AI-stage banner:
    # which vector space this process searches in is the first question of
    # any retrieval-quality report. `grep 'Embeddings:'` after a deploy.
    logger.info(
        "Embeddings: provider=%s model=%s dims=%s%s",
        EMBEDDING_PROVIDER or "<none: unconfigured>",
        EMBEDDING_MODEL or "<none>",
        EMBEDDING_DIMENSIONS,
        f" from {EMBEDDING_MODEL_PATH}"
        if EMBEDDING_PROVIDER == EMBEDDING_PROVIDER_LOCAL else "",
    )
    if EMBEDDING_PROVIDER != EMBEDDING_PROVIDER_LOCAL:
        return
    load_embedding_model()


log_and_load_embedding_provider()

# Scripture selection answers validation failures with a flat, sanitised
# {"detail": "..."} body (the default one echoes the prayer text); every
# other route keeps FastAPI's default handler. See app/scripture_select.py.
app.add_exception_handler(RequestValidationError, validation_exception_handler)

# Create main router with /api prefix
api_router = APIRouter(prefix="/api")

# Include routers to main router
api_router.include_router(excerpt_router)
api_router.include_router(audio_router)
api_router.include_router(about_router)
api_router.include_router(version_check_router)
api_router.include_router(import_data_router)
api_router.include_router(twinkler_ai_router)
api_router.include_router(scripture_select_router)


@api_router.get('/languages', response_model=list[LanguageModel], operation_id="get_languages", tags=["Languages"])
def get_languages(api_key: bool = RequireAPIKey):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute('''
            SELECT alias, name_en, name_national
            FROM languages
        ''')
        result = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        connection.close()
    return result


@api_router.get('/translations', response_model=list[TranslationModel], operation_id="get_translations", tags=["Translations"])
def get_translations(language: Optional[str] = None, api_key: bool = RequireAPIKey):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        params = []
        sql = '''
            SELECT
                t.code        AS translation_code,
                t.alias       AS translation_alias,
                t.name        AS translation_name,
                t.description AS translation_description,
                t.language    AS translation_language,
                t.active      AS translation_active,

                a.code        AS audio_code,
                a.alias       AS audio_alias,
                a.name        AS audio_name,
                a.description AS audio_description,
                a.is_music    AS audio_is_music,
                a.active      AS audio_active
            FROM translations AS t
              LEFT JOIN voices AS a ON a.translation = t.code
            WHERE t.active=1 AND (a.active=1 OR a.active IS NULL)'''

        if language:
            sql += " AND t.language = %s "
            params.append(language)
        cursor.execute(sql, params)
        rows = cursor.fetchall()

        translations = {}
        for row in rows:
            translation_code = row['translation_code']
            if translation_code not in translations:
                translations[translation_code] = {
                    'code'        : translation_code,
                    'alias'       : row['translation_alias'],
                    'name'        : row['translation_name'],
                    'description' : row['translation_description'],
                    'language'    : row['translation_language'],
                    'active'      : row['translation_active'],
                    'voices'      : [],
                }
            if row['audio_code']:
                translations[translation_code]['voices'].append({
                    'code'        : row['audio_code'],
                    'alias'       : row['audio_alias'],
                    'name'        : row['audio_name'],
                    'description' : row['audio_description'],
                    'is_music'    : row['audio_is_music'],
                    'active'      : row['audio_active'],
                })

        result = list(translations.values())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        connection.close()
    return result


@timed_cache(seconds=3600)  # Cache for 1 hour
def get_chapters_by_book(translation_code: int) -> dict:
    """Get all chapters for all books in a translation (cached)"""
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        # Get all book numbers for this translation
        cursor.execute('''
            SELECT book_number FROM translation_books WHERE translation = %s
        ''', (translation_code,))
        book_numbers = [row['book_number'] for row in cursor.fetchall()]

        if not book_numbers:
            return {}

        # Get all chapters in one query (filtered by translation)
        placeholders = ','.join(['%s'] * len(book_numbers))
        cursor.execute(f'''
            SELECT book_number, chapter_number
            FROM translation_verses
            WHERE translation = %s AND book_number IN ({placeholders})
            GROUP BY book_number, chapter_number
        ''', [translation_code] + book_numbers)

        # Build map
        chapters_by_book = {}
        for row in cursor.fetchall():
            book_number = row['book_number']
            if book_number not in chapters_by_book:
                chapters_by_book[book_number] = set()
            chapters_by_book[book_number].add(row['chapter_number'])

        return chapters_by_book
    finally:
        cursor.close()
        connection.close()


@api_router.get('/translations/{translation_code}/books', response_model=list[TranslationBookModel], operation_id="get_translation_books", tags=["Translations"])
def get_translation_books(translation_code: int, voice_code: Optional[int] = None, api_key: bool = RequireAPIKey):
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        # Check if translation exists and get alias
        cursor.execute("SELECT code, alias FROM translations WHERE code = %s AND active = 1", (translation_code,))
        translation = cursor.fetchone()
        if not translation:
            raise HTTPException(status_code=404, detail=f"Translation {translation_code} not found")

        translation_alias = translation['alias']
        voice_alias = None

        # If voice_code is provided, check if voice exists and get alias
        if voice_code:
            cursor.execute("SELECT code, alias FROM voices WHERE code = %s", (voice_code,))
            voice = cursor.fetchone()
            if not voice:
                raise HTTPException(status_code=404, detail=f"Voice {voice_code} not found")
            voice_alias = voice['alias']

        # Get books — without anomalies (no voice_anomalies table in cep_public)
        # chapters_count is NOT read from translation_verses here: that table
        # holds every translation, and an unscoped max(chapter_number) mixed
        # canons (see app/canon.py). It is computed below from the canon table
        # plus this translation's own chapters.
        cursor.execute('''
            SELECT
                tb.code, tb.book_number, tb.name, bb.code1 AS alias
            FROM translation_books AS tb
            LEFT JOIN bible_books AS bb ON bb.number = tb.book_number
            WHERE tb.translation = %s
            ORDER BY tb.book_number
        ''', (translation_code,))

        books = cursor.fetchall()

        # Get all existing chapters from cache
        chapters_by_book = get_chapters_by_book(translation_code)

        # Check for chapters without text and audio
        for book in books:
            book_number = book['book_number']

            # Get existing chapters from pre-loaded data
            existing_chapters = chapters_by_book.get(book_number, set())

            # Expected structure of the book and the chapters this translation
            # has no text for. A book the publisher declares but ships no text
            # for (npu: 38 Old Testament books) is reported honestly — every
            # canonical chapter missing and has_text false.
            chapters_count, chapters_without_text = chapter_coverage(
                book_number, existing_chapters, translation_alias
            )
            book['chapters_count'] = chapters_count
            book['chapters_without_text'] = chapters_without_text
            book['has_text'] = bool(existing_chapters)

            # If voice_code is provided, check for chapters without audio
            if voice_code and voice_alias and translation_alias:
                chapters_without_audio = []

                # Check each existing chapter for audio file existence
                for chapter_number in existing_chapters:
                    if not check_audio_file_exists(translation_alias, voice_alias, book_number, chapter_number):
                        chapters_without_audio.append(chapter_number)

                book['chapters_without_audio'] = sorted(chapters_without_audio)

        return books

    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        connection.close()


@api_router.post('/cache/clear', operation_id="clear_cache", tags=["Cache"])
def clear_cache(api_key: bool = RequireAPIKey):
    """Clear all cached data (requires API Key)"""
    from excerpt import get_all_existing_audio_chapters, get_existing_audio_chapters, check_audio_file_exists

    global _cache, _cache_timestamps
    cache_size = len(_cache)
    _cache.clear()
    _cache_timestamps.clear()

    # Clear LRU caches from excerpt.py
    get_all_existing_audio_chapters.cache_clear()
    get_existing_audio_chapters.cache_clear()
    check_audio_file_exists.cache_clear()

    # Drop the cached scripture-selection corpus (vector + BM25 indexes)
    clear_cached_resources()

    return {
        "message": f"All caches cleared successfully",
        "items_cleared": cache_size,
        "lru_caches_cleared": ["get_all_existing_audio_chapters", "get_existing_audio_chapters", "check_audio_file_exists", "scripture_corpus"]
    }


# Include main router to application
app.include_router(api_router)
