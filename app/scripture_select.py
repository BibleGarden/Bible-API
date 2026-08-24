"""
Public scripture-selection API for the mobile app (ClickUp 86cb8vw1m).

`POST /api/scripture/v1/select` turns a prayer context (topic + the replies
the person picked in the Twinkler dialog) into ONE Bible passage: canonical
coordinates, the exact text of the chosen translation from `cep_public`,
and a stable canonical ID the client stores to avoid repeats.

Everything behind it already exists and is benchmarked: the retrieval
pipeline (ADR 0004) and the grounded rerank (ADR 0005). This module is the
public contract on top of `ScriptureRetriever.select_final`: validation,
API key, rate limiting, the request time budget, the process-local corpus
cache, and the privacy rules. See
architect/adr/0006-scripture-select-api.md and
architect/scripture-select.md.

Privacy rules enforced here:

- the model's `reason` is server-side diagnostics and is NEVER returned to
  the client (ADR 0005; showing an explanation is a separate product
  decision);
- topic, replies and the chosen passage never reach the logs or the
  request statistics. The passage alone is not private, but together with
  the client identity it reveals what the person prayed about, so
  `app/middleware.py` stores for this endpoint only the endpoint name,
  method, status, latency and an HMAC client pseudonym;
- only prayer-independent data is cached (the vector and BM25 indexes);
  nothing derived from a request is ever cached.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Annotated

from fastapi import APIRouter, HTTPException, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from starlette.concurrency import run_in_threadpool

from auth import RequireAPIKey
from chunking import CHUNKING_VERSION
from client_ip import resolve_client_ip
from config import (
    SCRIPTURE_INDEX_CACHE_SECONDS,
    SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE,
    SCRIPTURE_SELECT_REQUESTS_PER_MINUTE,
    SCRIPTURE_SELECT_TIMEOUT_SECONDS,
)
from database import create_connection
from deadline import Deadline
from lexical_index import load_lexical_indexes
from passage_rerank import GeminiPassageReranker
from query_rewrite import REWRITE_VARIANTS, GeminiQueryRewriter
from embeddings import GeminiEmbeddingClient
from rate_limit import RateLimiter, RateLimitError
from retrieval import (
    FinalSelection,
    ScriptureRetriever,
    SelectionRequest,
    make_db_passage_loader,
    split_exclusions,
)
from vector_index import load_index

router = APIRouter()
logger = logging.getLogger(__name__)

# --- request limits (documented in architect/scripture-select.md) ---------
# The Twinkler dialog produces a short theme line and short picked replies
# (the evaluation corpus tops out at 47 and 62 characters); these caps keep
# roughly an order of magnitude of headroom while bounding the rewrite
# prompt and the number of already-shown IDs the server has to parse.
MAX_TOPIC_CHARS = 500
MAX_REPLY_CHARS = 1000
MAX_REPLIES = 10
MAX_REPLIES_CHARS = 4000
MAX_EXCLUSIONS = 200
CANONICAL_ID_PATTERN = r"^v\d{1,3}:\d{2}\.\d{3}\.\d{3}-\d{3}$"

Reply = Annotated[str, StringConstraints(max_length=MAX_REPLY_CHARS)]
# Shape-checked here; the chunking-version part is interpreted (not
# rejected) by retrieval.split_exclusions.
CanonicalId = Annotated[
    str, StringConstraints(pattern=CANONICAL_ID_PATTERN, max_length=40)
]

# Candidate list handed to the reranker — the benchmarked configuration.
TOP_K = 10

# --- serve-time provider budgets -----------------------------------------
# Deliberately tighter than the CLI/benchmark defaults (20-60 s, 3-6
# attempts): here the total deadline, not the retry ladder, must bound the
# request. Every call is additionally capped by whatever is left of the
# deadline.
_PROVIDER_TIMEOUT_SECONDS = 8.0
_PROVIDER_ATTEMPTS = 2


class ScriptureSelectUnavailable(RuntimeError):
    """No verified passage can be produced (corpus, index or DB missing)."""


class Language(str, Enum):
    ru = "ru"
    en = "en"
    uk = "uk"


class SelectionSource(str, Enum):
    """Which stage produced the returned passage."""

    rerank = "rerank"
    retrieval_fallback = "retrieval_fallback"
    safe_pool = "safe_pool"


class FallbackReason(str, Enum):
    """Why the AI choice did not decide (null when `source` is rerank)."""

    empty_topic = "empty_topic"
    ai_unavailable = "ai_unavailable"
    rerank_failed = "rerank_failed"
    no_reranker = "no_reranker"
    deadline = "deadline"


class SelectRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "language": "ru",
                "topic": "Тревога перед операцией",
                "user_replies": ["Боюсь за исход", "Прошу мира в сердце"],
                "exclude_canonical_ids": ["v3:19.023.001-006"],
            }
        },
    )

    language: Language = Field(
        description=(
            "Corpus language. Determines both the indexed translations and "
            "the language of the internal query rewrite."
        ),
    )
    topic: str = Field(
        default="",
        max_length=MAX_TOPIC_CHARS,
        description=(
            "Prayer topic. An empty topic is valid and deliberately served "
            "from the curated safe pool without any AI call."
        ),
    )
    user_replies: list[Reply] = Field(
        default_factory=list,
        max_length=MAX_REPLIES,
        description=(
            "Replies the person picked in the dialog, in order. "
            f"At most {MAX_REPLIES} items, {MAX_REPLY_CHARS} characters each "
            f"and {MAX_REPLIES_CHARS} characters in total."
        ),
    )
    exclude_canonical_ids: list[CanonicalId] = Field(
        default_factory=list,
        max_length=MAX_EXCLUSIONS,
        description=(
            "Canonical IDs already shown to this person; they are excluded "
            "from the result. IDs of another chunking version are ignored "
            "and reported back through `history_reset`."
        ),
    )
    translation: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Translation code to render the passage in. Defaults to the "
            "primary indexed translation of the language; must belong to it."
        ),
    )

    def normalized_replies(self) -> list[str]:
        return [reply.strip() for reply in self.user_replies if reply.strip()]


class CanonicalRange(BaseModel):
    """Canonical (english-masoretic) coordinates of the chosen passage."""

    canonical_id: str = Field(
        description=(
            "Stable ID of the passage window; store it and send it back in "
            "`exclude_canonical_ids` to avoid repeats."
        ),
        examples=["v3:19.023.001-006"],
    )
    book_number: int = Field(description="Bible book number", examples=[19])
    chapter_number: int = Field(description="Canonical chapter", examples=[23])
    verse_start: int = Field(description="First canonical verse", examples=[1])
    verse_end: int = Field(description="Last canonical verse", examples=[6])


class PassageModel(BaseModel):
    """The passage as it exists in the requested translation."""

    translation: int = Field(description="Translation code", examples=[1])
    translation_alias: str = Field(description="Translation alias", examples=["syn"])
    book_number: int = Field(description="Bible book number", examples=[19])
    chapter_number: int = Field(
        description="Chapter in the translation's own numbering", examples=[22]
    )
    verse_start: int = Field(description="First verse in the translation")
    verse_end: int = Field(description="Last verse in the translation")
    title: str | None = Field(
        default=None, description="Section title of the passage, when the "
        "translation has one"
    )
    text: str = Field(description="Exact passage text from the database")


class SelectResponse(BaseModel):
    language: Language
    canonical: CanonicalRange
    passage: PassageModel
    source: SelectionSource = Field(
        description=(
            "`rerank` — the AI chose among verified candidates; "
            "`retrieval_fallback` — retrieval's top candidate was served "
            "because the AI choice was unavailable; `safe_pool` — the "
            "curated no-AI list was served."
        )
    )
    fallback_reason: FallbackReason | None = Field(
        default=None,
        description="Category of the fallback; null when `source` is `rerank`.",
    )
    history_reset: bool = Field(
        description=(
            "True when some sent `exclude_canonical_ids` belong to another "
            "chunking version and were ignored. The client should drop its "
            "stored history and keep only IDs returned from now on."
        )
    )


class ErrorResponse(BaseModel):
    detail: str = Field(description="Public error message")


# ---------------------------------------------------------------------------
# Validation errors without the prayer text
# ---------------------------------------------------------------------------
# FastAPI's default 422 body is an array of HTTPValidationError items, each
# carrying the offending `input` verbatim — for this endpoint that means
# echoing the prayer topic and replies back to the caller (and into any
# crash reporter or proxy log on the way). This endpoint therefore answers
# validation failures with the same flat `{"detail": "..."}` shape as its
# other errors, describing the CATEGORY and the field NAME only. The
# Twinkler routes keep FastAPI's default body — their contract is already
# published and their field is a single free-form message.

SANITIZED_VALIDATION_PATHS = frozenset({"/api/scripture/v1/select"})

_SAFE_FIELD_RE = re.compile(r"^[A-Za-z0-9_.-]{1,40}$")
_MAX_REPORTED_ERRORS = 3


def _field_name(location) -> str:
    """Dotted field path from a pydantic error location.

    Names come from the schema — except for `extra_forbidden`, where the
    last element is the client's own field name; anything that does not
    look like an identifier is replaced, so a prayer sentence smuggled in
    as a field name cannot be echoed either.
    """
    text = ""
    for item in location:
        if item == "body":
            continue
        if isinstance(item, int):
            text += f"[{item}]"
            continue
        name = str(item)
        if not _SAFE_FIELD_RE.match(name):
            name = "field"
        text = f"{text}.{name}" if text else name
    return text or "body"


def _describe_validation_error(error: dict) -> str:
    kind = str(error.get("type", ""))
    field = _field_name(error.get("loc", ()))
    if kind == "extra_forbidden":
        return f"unknown field: {field}"
    if kind == "missing":
        return f"{field} is required"
    if kind == "string_too_long":
        return f"{field} is too long"
    if kind == "string_too_short":
        return f"{field} is too short"
    if kind == "too_long":
        return f"{field} has too many items"
    if kind == "too_short":
        return f"{field} has too few items"
    if kind == "string_pattern_mismatch":
        return f"{field} has an invalid format"
    if kind == "enum":
        return f"{field} has an unsupported value"
    if kind in {
        "greater_than", "greater_than_equal", "less_than", "less_than_equal",
    }:
        return f"{field} is out of range"
    if kind in {"json_invalid", "value_error.jsondecode"}:
        return "request body is not valid JSON"
    if kind.endswith("_type") or kind.endswith("_parsing"):
        return f"{field} has a wrong type"
    return f"{field} is invalid"


def summarize_validation_error(exc: RequestValidationError) -> str:
    """Public 422 message: categories and field names, never any value."""
    messages: list[str] = []
    for error in exc.errors():
        described = _describe_validation_error(error)
        if described not in messages:
            messages.append(described)
        if len(messages) == _MAX_REPORTED_ERRORS:
            break
    return "; ".join(messages) or "request is invalid"


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """App-level handler: sanitised body for this endpoint, default elsewhere."""
    if request.url.path.rstrip("/") in SANITIZED_VALIDATION_PATHS:
        return JSONResponse(
            status_code=422, content={"detail": summarize_validation_error(exc)}
        )
    return await request_validation_exception_handler(request, exc)


# ---------------------------------------------------------------------------
# Rate limiting (own budget, shared pseudonymisation key)
# ---------------------------------------------------------------------------

_limiter = RateLimiter(name="scripture selection")


def _reserve_rate_limit(client_key: str) -> None:
    _limiter.reserve(
        client_key,
        SCRIPTURE_SELECT_REQUESTS_PER_MINUTE,
        SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE,
    )


async def _enforce_rate_limit(client_key: str) -> None:
    try:
        _reserve_rate_limit(client_key)
    except RateLimitError as error:
        if error.retry_after is not None:
            raise HTTPException(
                status_code=429,
                detail="Scripture selection request limit exceeded",
                headers={"Retry-After": str(error.retry_after)},
            ) from error
        logger.warning("Scripture selection rate limiter unavailable: %s", error)
        raise HTTPException(
            status_code=503,
            detail="Scripture selection temporarily unavailable",
        ) from error


# ---------------------------------------------------------------------------
# Process-local corpus cache (prayer-independent data only)
# ---------------------------------------------------------------------------

@dataclass
class CorpusResources:
    index: object                      # vector_index.InMemoryVectorIndex
    lexical: dict                      # language -> lexical_index.LexicalIndex
    translations: dict                 # language -> [(code, alias), ...]
    loaded_at: float


_resources_lock = threading.Lock()
_resources: CorpusResources | None = None


def _load_resources() -> CorpusResources:
    connection = create_connection()
    if connection is None:
        raise ScriptureSelectUnavailable("database is not available")
    cursor = connection.cursor(dictionary=True)
    try:
        index = load_index(cursor)
        if not len(index):
            raise ScriptureSelectUnavailable(
                "vector index is empty (run app/index_cli.py rebuild)"
            )
        lexical = load_lexical_indexes(cursor, CHUNKING_VERSION)
    finally:
        cursor.close()
        connection.close()

    translations: dict[str, list[tuple[int, str]]] = {}
    for meta in index.metas:
        entries = translations.setdefault(meta["language"], [])
        entry = (meta["translation"], meta["alias"])
        if entry not in entries:
            entries.append(entry)
    return CorpusResources(
        index=index,
        lexical=lexical,
        translations=translations,
        loaded_at=time.monotonic(),
    )


def get_resources() -> CorpusResources:
    """Cached vector + BM25 indexes; reloaded after the configured TTL.

    Loading them costs ~1 s and ~45 MB, and the data is identical for every
    request (it depends only on the corpus), so it is the one thing worth
    caching here. Nothing derived from a prayer context is ever cached.

    A failed refresh serves the stale copy instead of failing the request:
    the cached corpus is still a valid corpus, and a DB blip during a
    refresh must not take the endpoint down. Only a cold cache propagates
    the failure (503).
    """
    global _resources
    with _resources_lock:
        cached = _resources
        if cached is not None and (
            time.monotonic() - cached.loaded_at < SCRIPTURE_INDEX_CACHE_SECONDS
        ):
            return cached
        try:
            _resources = _load_resources()
        except Exception as error:
            if cached is None:
                raise
            # Category only, and keep serving the previous corpus. Reset
            # the timer so the next refresh is attempted after a full TTL
            # rather than on every request.
            logger.warning(
                "Scripture corpus refresh failed, serving the cached copy: %s",
                type(error).__name__,
            )
            cached.loaded_at = time.monotonic()
            return cached
        return _resources


def clear_cached_resources() -> None:
    """Drop the corpus cache (called by POST /api/cache/clear)."""
    global _resources
    with _resources_lock:
        _resources = None


# ---------------------------------------------------------------------------
# Gemini clients (shared, serve-time budgets)
# ---------------------------------------------------------------------------

_clients_lock = threading.Lock()
_clients: tuple | None = None


def _gemini_clients() -> tuple:
    """Lazily built (rewriter, embedder, reranker) with serve-time budgets.

    The HTTP clients are shared across requests (httpx clients are
    thread-safe) so a selection does not pay three TLS handshakes.
    """
    global _clients
    with _clients_lock:
        if _clients is None:
            _clients = (
                GeminiQueryRewriter(
                    timeout=_PROVIDER_TIMEOUT_SECONDS, attempts=_PROVIDER_ATTEMPTS
                ),
                GeminiEmbeddingClient(
                    timeout=_PROVIDER_TIMEOUT_SECONDS, max_retries=_PROVIDER_ATTEMPTS
                ),
                GeminiPassageReranker(
                    timeout=_PROVIDER_TIMEOUT_SECONDS, attempts=_PROVIDER_ATTEMPTS
                ),
            )
        return _clients


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

def resolve_translation(
    resources: CorpusResources, language: str, requested: int | None
) -> int:
    """Validate the requested translation or pick the language's primary."""
    available = resources.translations.get(language, [])
    if not available:
        raise ScriptureSelectUnavailable(f"no indexed translation for {language}")
    if requested is None:
        return available[0][0]
    if requested not in {code for code, _alias in available}:
        raise HTTPException(
            status_code=422,
            detail=f"Translation {requested} is not available for {language}",
        )
    return requested


def _run_selection(
    resources: CorpusResources,
    selection_request: SelectionRequest,
    deadline: Deadline,
) -> FinalSelection:
    """Blocking part of a selection: DB texts + the AI pipeline."""
    connection = create_connection()
    if connection is None:
        raise ScriptureSelectUnavailable("database is not available")
    cursor = connection.cursor(dictionary=True)
    try:
        rewriter, embedder, reranker = _gemini_clients()
        retriever = ScriptureRetriever(
            index=resources.index,
            embedder=embedder,
            rewriter=rewriter,
            reranker=reranker,
            load_passages=make_db_passage_loader(cursor),
            lexical_indexes=resources.lexical,
            # Independent round trips: embed the variants concurrently.
            embed_workers=REWRITE_VARIANTS,
        )
        return retriever.select_final(selection_request, deadline)
    finally:
        cursor.close()
        connection.close()


def _fallback_reason(value: str | None) -> FallbackReason | None:
    """Map an internal fallback category onto the public enum.

    A category the public contract does not know about must degrade to
    `null`, not to a 500: the passage itself is still valid.
    """
    if not value:
        return None
    try:
        return FallbackReason(value)
    except ValueError:
        logger.warning("Unmapped scripture selection fallback category: %s", value)
        return None


def build_response(
    final: FinalSelection,
    language: str,
    translation: int,
    history_reset: bool,
) -> SelectResponse:
    """Map the internal selection onto the public contract.

    `FinalSelection.reason` (the model's diagnostic sentence) is
    deliberately dropped here — it is server-side only.
    """
    candidate = final.candidate
    if candidate is None:
        raise ScriptureSelectUnavailable("no candidate passage")
    passage = next(
        (p for p in candidate.passages if p.translation == translation), None
    )
    if passage is None:
        raise ScriptureSelectUnavailable(
            f"chosen passage has no text in translation {translation}"
        )

    if final.selection.source == "safe_pool":
        source = SelectionSource.safe_pool
        reason = final.selection.fallback_reason
    elif final.method == "rerank":
        source = SelectionSource.rerank
        reason = None
    else:
        source = SelectionSource.retrieval_fallback
        reason = final.fallback_reason

    return SelectResponse(
        language=Language(language),
        canonical=CanonicalRange(
            canonical_id=candidate.canonical_id,
            book_number=candidate.book_number,
            chapter_number=candidate.chapter_number,
            verse_start=candidate.verse_start,
            verse_end=candidate.verse_end,
        ),
        passage=PassageModel(
            translation=passage.translation,
            translation_alias=passage.alias,
            book_number=passage.book_number,
            chapter_number=passage.chapter_number,
            verse_start=passage.verse_number_start,
            verse_end=passage.verse_number_end,
            title=passage.title,
            text=passage.text,
        ),
        source=source,
        fallback_reason=_fallback_reason(reason),
        history_reset=history_reset,
    )


@router.post(
    "/scripture/v1/select",
    response_model=SelectResponse,
    operation_id="scripture_select",
    tags=["Scripture"],
    summary="Select one Bible passage for a prayer context",
    description=(
        "Returns a single passage chosen for the prayer context: canonical "
        "coordinates, the exact text of the requested translation and a "
        "stable canonical ID.\n\n"
        "The passage is always a real verse range of the requested "
        "translation taken from the database — the AI stage only picks "
        "among candidates the server retrieved, it never produces "
        "scripture text or references itself.\n\n"
        "Degradation is part of the contract, not an error: when the AI "
        "choice is unavailable the retrieval top candidate is served "
        "(`source=retrieval_fallback`), and when retrieval itself cannot "
        "run — empty topic, provider outage, exhausted time budget — a "
        "curated safe pool is served (`source=safe_pool`). `fallback_reason` "
        "carries the category.\n\n"
        "Prayer topic, replies and the returned passage are never logged or "
        "stored in request statistics."
    ),
    responses={
        403: {"model": ErrorResponse, "description": "Invalid or missing API key"},
        422: {
            "model": ErrorResponse,
            "description": (
                "Request validation failed: unknown field, oversized topic, "
                "replies or exclusion list, malformed canonical ID, "
                "unsupported language, or a translation that does not belong "
                "to the language"
            ),
        },
        429: {
            "model": ErrorResponse,
            "description": "Global or per-client request limit exceeded",
            "headers": {
                "Retry-After": {
                    "description": "Seconds until another request can be attempted",
                    "schema": {"type": "integer", "minimum": 1},
                }
            },
        },
        503: {
            "model": ErrorResponse,
            "description": (
                "No verified passage could be produced (database, corpus or "
                "rate limiter unavailable)"
            ),
        },
    },
)
async def scripture_select(
    request: SelectRequest,
    http_request: Request,
    api_key: bool = RequireAPIKey,
) -> SelectResponse:
    replies = request.normalized_replies()
    if sum(len(reply) for reply in replies) > MAX_REPLIES_CHARS:
        raise HTTPException(status_code=422, detail="Replies are too long")

    try:
        resources = await run_in_threadpool(get_resources)
        translation = resolve_translation(
            resources, request.language.value, request.translation
        )
    except ScriptureSelectUnavailable as error:
        # Category only: never the prayer context.
        logger.warning("Scripture selection unavailable: %s", error)
        raise HTTPException(
            status_code=503, detail="Scripture selection temporarily unavailable"
        ) from error

    client_key = resolve_client_ip(http_request)
    await _enforce_rate_limit(client_key)

    exclusions, stale = split_exclusions(
        request.exclude_canonical_ids, CHUNKING_VERSION
    )
    selection_request = SelectionRequest(
        language=request.language.value,
        topic=request.topic.strip(),
        user_replies=tuple(replies),
        exclude_canonical_ids=exclusions,
        top_k=TOP_K,
    )
    deadline = Deadline(SCRIPTURE_SELECT_TIMEOUT_SECONDS)
    try:
        final = await run_in_threadpool(
            _run_selection, resources, selection_request, deadline
        )
        return build_response(
            final,
            request.language.value,
            translation,
            history_reset=bool(stale),
        )
    except ScriptureSelectUnavailable as error:
        logger.warning("Scripture selection unavailable: %s", error)
        raise HTTPException(
            status_code=503, detail="Scripture selection temporarily unavailable"
        ) from error
