"""
Public scripture-selection API for the mobile app (ClickUp 86cb8vw1m).

`POST /api/ai/scripture` turns a prayer context (topic + the replies
the person picked in the Twinkler dialog) into ONE Bible passage: canonical
coordinates, the exact text of the chosen translation from `cep_public`,
a stable canonical ID the client stores to avoid repeats, the passage's own
verse boundaries (`passage.verses`) so the client can place a highlight
deterministically, and optionally the key verses to emphasise inside it
(`highlight`, ADR 0005 prompt v9 — numbers only, resolved against the
versification table).

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
from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated

from fastapi import APIRouter, HTTPException, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_serializer,
)
from pydantic.json_schema import SkipJsonSchema
from starlette.concurrency import run_in_threadpool

from auth import RequireAPIKey
from chunking import CHUNKING_VERSION
from client_ip import resolve_client_ip
from config import (
    AI_SCRIPTURE_INDEX_CACHE_SECONDS,
    AI_SCRIPTURE_PRIMARY_TRANSLATIONS,
    AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE,
    AI_SCRIPTURE_REQUESTS_PER_MINUTE,
    AI_SCRIPTURE_TIMEOUT_SECONDS,
)
from database import create_connection
from deadline import Deadline
from lexical_index import load_lexical_indexes
from passage_highlight import load_psalm_maps, resolve_highlight
from passage_rerank import GeminiPassageReranker
from passage_render import (
    build_coverage,
    load_chunk_ranges,
    reference_faithful_windows,
    render_passage,
)
from query_rewrite import REWRITE_VARIANTS, GeminiQueryRewriter
from embeddings import GeminiEmbeddingClient
from rate_limit import RateLimiter, RateLimitError
from retrieval import (
    FinalSelection,
    ScriptureRetriever,
    SelectionRequest,
    make_db_passage_loader,
    make_db_verse_loader,
    parse_canonical_id,
    prompt_passage,
    split_exclusions,
)
from vector_index import IndexVersionUnavailable, load_index

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
    # ADR 0007: retrieval ran, but nothing it found exists in the requested
    # (non-primary) translation, so the coverage-filtered safe pool answered.
    coverage_empty = "coverage_empty"
    # ADR 0007 fix F1 on the primary path: retrieval ran on a fully covered
    # translation and the caller's exclusions (or the genre blacklist) left
    # its ranking empty, so the safe pool answered.
    ranking_empty = "ranking_empty"


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
            "language's primary translation. Must be a renderable "
            "translation of the requested language — any active translation "
            "of a language whose corpus is indexed can be served, not only "
            "the indexed one."
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


class VerseModel(BaseModel):
    """One verse of `passage.text`, in the returned translation's numbering."""

    number: int = Field(
        description=(
            "Verse number in the numbering of the translation this passage is "
            "returned in — the SAME numbering as `highlight.passage`, so a "
            "highlight is applied by selecting the verses whose `number` lies "
            "between `highlight.passage.verse_start` and `verse_end` "
            "inclusive. It is not a position in the list: a passage may start "
            "mid-chapter, a Psalm superscription is verse 1 wherever the "
            "translation counts it (`syn`, `bti`, `ubh`) and absent where it "
            "does not (`bsb`), and the numbering may have HOLES where the "
            "translation carries several canonical verses in one (`bti` has "
            "no Genesis 35:10 — its verse 9 says both). When a `highlight` is "
            "served together with these verses, its boundary numbers are "
            "guaranteed to occur among them; a highlight whose ends fall into "
            "such a hole is omitted instead."
        ),
        examples=[6],
    )
    text: str = Field(
        description="Exact verse text from the database, whitespace-trimmed.",
    )
    paragraph_start: bool = Field(
        description=(
            "True when this verse opens a paragraph of `passage.text` (its "
            "own paragraph flag, a section title standing before it, or its "
            "being the first verse of the passage). Joining the verses of a "
            "paragraph with single spaces and the paragraphs with a blank "
            "line reproduces `passage.text` exactly."
        ),
    )


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
    # SkipJsonSchema for the same reason as `SelectResponse.highlight`: the
    # serializer below drops the key when the verse breakdown is unavailable,
    # so `null` is not a value this endpoint can return and the published
    # schema must not offer it.
    verses: list[VerseModel] | SkipJsonSchema[None] = Field(
        default=None,
        description=(
            "The same text split into its verses, in order — the passage's "
            "verse boundaries, so the client can place `highlight` (or any "
            "other per-verse decoration) deterministically instead of "
            "guessing where a verse begins.\n\n"
            "`text` and every other field are unchanged by this: the verses "
            "are an additional view of the SAME passage, read from the same "
            "database rows the text was assembled from. Their `number`s are "
            "in the numbering of the returned translation and therefore "
            "directly comparable with `highlight.passage`.\n\n"
            "The KEY IS ABSENT ENTIRELY (never present with a null value, "
            "never an empty list) in the degraded case where the server has "
            "the passage text but not its verse breakdown — the verse load "
            "failed, or the passage is a chunk of an indexed translation "
            "other than the one the candidates were rendered in. Clients "
            "must fall back to `text` then.\n\n"
            "This field and `highlight` are independent: a response may "
            "carry a `highlight` and no `verses` (the case above, reachable "
            "once a language has a second indexed translation), `verses` and "
            "no `highlight`, both, or neither. Do not make the presence of "
            "one imply the other — locate the highlight in `text` when the "
            "verses are absent."
        ),
    )

    @model_serializer(mode="wrap")
    def _drop_absent_verses(self, handler):
        """Omit `verses` entirely when the breakdown is unavailable.

        Additivity: a client written against the previous contract must
        receive the exact same bytes it did before, so an unavailable verse
        list is an absent KEY, not a `null` and not an empty array.
        """
        payload = handler(self)
        if payload.get("verses") is None:
            payload.pop("verses", None)
        return payload


class HighlightCanonical(BaseModel):
    """Canonical (english-masoretic) coordinates of the key verses."""

    book_number: int = Field(description="Bible book number", examples=[19])
    chapter_number: int = Field(description="Canonical chapter", examples=[23])
    verse_start: int = Field(
        description=(
            "First canonical verse of the highlight (0 is a Psalm "
            "superscription, which the canon does not number)"
        ),
        examples=[4],
    )
    verse_end: int = Field(description="Last canonical verse", examples=[4])


class HighlightPassage(BaseModel):
    """The same key verses in the returned translation's own numbering."""

    chapter_number: int = Field(
        description="Chapter in the translation's own numbering", examples=[22]
    )
    verse_start: int = Field(description="First verse in the translation")
    verse_end: int = Field(description="Last verse in the translation")


class HighlightModel(BaseModel):
    """Key verses inside `passage` — 1 to 3 verses carrying its central
    thought for this prayer, chosen by the same grounded AI call that chose
    the passage. `passage` coordinates are always a real sub-range of the
    returned passage, and whenever `passage.verses` is served as well, both
    boundary numbers of that range occur among those verses (a translation
    that merges canonical verses leaves holes in its numbering; a highlight
    that would end in one is omitted rather than served unfindable).

    `canonical` describes the same text in the canonical numbering and MAY
    span more than 3 verses: a translation verse that merges several
    canonical ones expands when converted (syn 114:8 alone is canonical
    116:8-9). The exact canonical range is kept rather than truncated, so
    both coordinate systems point at the same words (ADR 0005)."""

    canonical: HighlightCanonical
    passage: HighlightPassage


class SelectResponse(BaseModel):
    language: Language
    canonical: CanonicalRange
    passage: PassageModel
    # SkipJsonSchema keeps the None out of the PUBLISHED schema while the
    # runtime type stays optional: the serializer below omits the key
    # entirely when there is no highlight, so `null` is not a value this
    # endpoint can ever return and OpenAPI should not advertise it.
    highlight: HighlightModel | SkipJsonSchema[None] = Field(
        default=None,
        description=(
            "Key verses to emphasise inside the passage, in canonical and "
            "in translation coordinates. The KEY IS ABSENT ENTIRELY (never "
            "present with a null value) whenever no highlight was produced "
            "— the AI stage did not decide, answered a span the server "
            "refused, or the passage came from a fallback. Clients must "
            "render the passage unchanged in that case."
        ),
    )
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
        description=(
            "Category of the fallback; null when `source` is `rerank`. "
            "`coverage_empty` means that once the requested translation's "
            "coverage, the caller's exclusions and the genre blacklist had "
            "all narrowed the candidate pool, nothing was left in it, so the "
            "safe pool answered — it can only appear for a translation "
            "other than the language's primary one. `ranking_empty` is the "
            "same outcome without a coverage filter: the caller's exclusions "
            "or the genre blacklist emptied the ranking of a fully covered "
            "translation, and the safe pool answered. A long "
            "`exclude_canonical_ids` history on a narrow topic is the "
            "expected cause; the client may keep sending the history."
        ),
    )
    history_reset: bool = Field(
        description=(
            "True when some sent `exclude_canonical_ids` belong to another "
            "chunking version and were ignored. The client should drop its "
            "stored history and keep only IDs returned from now on."
        )
    )

    @model_serializer(mode="wrap")
    def _drop_absent_highlight(self, handler):
        """Omit `highlight` entirely when there is none.

        Backward compatibility: a client written against the previous
        contract must receive the exact same bytes it did before, so an
        absent highlight is an absent KEY, not a `null` (unlike
        `fallback_reason`, whose null is part of the published contract).
        """
        payload = handler(self)
        if payload.get("highlight") is None:
            payload.pop("highlight", None)
        return payload


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

SANITIZED_VALIDATION_PATHS = frozenset({"/api/ai/scripture"})

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
        AI_SCRIPTURE_REQUESTS_PER_MINUTE,
        AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE,
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
    # RENDERABLE catalogue: language -> [(code, alias), ...], primary first.
    # A translation is renderable when its language has an index (the corpus
    # is retrieved in the indexed translation's space), it has a Psalm
    # versification map and it fully covers at least one canonical window —
    # see `_build_catalogue`.
    translations: dict
    loaded_at: float
    psalm_maps: dict = field(default_factory=dict)  # translation -> PsalmMap
    # INDEXED (reference) translations: language -> [(code, alias), ...].
    # Their chunks and embeddings ARE the corpus; every candidate window and
    # every rerank prompt passage comes from one of them.
    indexed: dict = field(default_factory=dict)
    # language -> the translation served when the request names none, and the
    # one whose path stays byte-for-byte the pre-catalogue behaviour.
    primary: dict = field(default_factory=dict)
    # translation code -> frozenset of canonical IDs fully present in it.
    # Built only for non-primary renderable translations (the primary needs
    # no filter: the corpus is its own chunk set).
    coverage: dict = field(default_factory=dict)


_resources_lock = threading.Lock()
_resources: CorpusResources | None = None


def parse_primary_config(raw: str) -> dict[str, str]:
    """Parse AI_SCRIPTURE_PRIMARY_TRANSLATIONS ("ru=syn,en=bsb,uk=16").

    Entries are `language=alias` or `language=code`, comma separated,
    whitespace tolerated. A malformed item is skipped with a warning (the
    language then falls back to the deterministic default) instead of taking
    the endpoint down over a typo in an optional variable.
    """
    config: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        language, separator, value = item.partition("=")
        language, value = language.strip(), value.strip()
        if not separator or not language or not value:
            logger.warning(
                "AI_SCRIPTURE_PRIMARY_TRANSLATIONS: ignoring a malformed entry"
            )
            continue
        config[language] = value
    return config


def resolve_primary_translations(
    indexed: dict[str, list[tuple[int, str]]], raw_config: str
) -> dict[str, int]:
    """language -> primary (default) translation code.

    Closes ADR 0006 open question 5. The primary must be an INDEXED
    translation of its language: it is the one the retrieval corpus is built
    from and the one the rerank prompt is rendered in, so serving it needs no
    coverage filter and no re-rendering.

    Without configuration the primary is the indexed translation with the
    lowest code — deterministic, and identical to the previous "first in
    index insertion order" while a language has a single indexed translation
    (which is the case for the whole current corpus).

    ADR 0007 OQ2 guard (review fix F7): the primary is NOT necessarily the
    translation the rerank prompt is rendered in — that one is
    `candidate.passages[0]`, i.e. index insertion order (`reference_
    translations`). While every language has a single indexed translation the
    two are the same object; a second indexed translation could separate
    them, and then the own-range rendering of a non-indexed translation would
    be verified against a chunk nobody read. The disagreement is therefore
    logged here, the reference (not the primary) is what the coverage filter
    is built from, and `_render_target_passage` refuses a candidate whose
    prompt passage is not the reference at all.
    """
    config = parse_primary_config(raw_config)
    primary: dict[str, int] = {}
    for language, entries in indexed.items():
        wanted = config.get(language)
        chosen: int | None = None
        if wanted is not None:
            for code, alias in entries:
                if alias == wanted or str(code) == wanted:
                    chosen = code
                    break
            if chosen is None:
                # Category only: never echo an unindexed value into the log
                # of a language it does not belong to.
                logger.warning(
                    "AI_SCRIPTURE_PRIMARY_TRANSLATIONS: %s names a translation "
                    "that is not indexed for that language; using the default",
                    language,
                )
        if chosen is None:
            chosen = min(code for code, _alias in entries)
        reference_code = entries[0][0]
        if chosen != reference_code:
            logger.warning(
                "Primary translation %s of %s is not the one the rerank "
                "prompt is rendered in (%s, index order): the candidate "
                "texts the AI reads and the default served translation are "
                "different books — see ADR 0007 open question 2",
                chosen,
                language,
                reference_code,
            )
        primary[language] = chosen
    for language in config:
        if language not in indexed:
            logger.warning(
                "AI_SCRIPTURE_PRIMARY_TRANSLATIONS: language %s has no index",
                language,
            )
    return primary


def _indexed_translations(index) -> dict[str, list[tuple[int, str]]]:
    """language -> [(code, alias), ...] in index insertion order."""
    indexed: dict[str, list[tuple[int, str]]] = {}
    for meta in index.metas:
        entries = indexed.setdefault(meta["language"], [])
        entry = (meta["translation"], meta["alias"])
        if entry not in entries:
            entries.append(entry)
    return indexed


def reference_translation(
    indexed: dict[str, list[tuple[int, str]]], language: str
) -> int | None:
    """The translation the rerank prompt (and every candidate text) is in.

    `retrieval.prompt_passage` shows the reranker `candidate.passages[0]`,
    and passages follow index insertion order — so the FIRST indexed entry of
    a language is the text every AI decision is made on, and the chunk the
    own-range rendering of another translation has to agree with (ADR 0007
    fix F2/F7). Identical to the primary for the whole current corpus.
    """
    entries = indexed.get(language) or []
    return entries[0][0] if entries else None


def _canonical_windows(index) -> dict[str, list[tuple]]:
    """language -> [(canonical_id, book, chapter, start, end), ...].

    The candidate universe of a language: every canonical window its indexed
    translations contribute, in canonical coordinates.
    """
    windows: dict[str, list[tuple]] = {}
    seen: dict[str, set[str]] = {}
    for meta in index.metas:
        language = meta["language"]
        canonical_id = meta["canonical_id"]
        known = seen.setdefault(language, set())
        if canonical_id in known:
            continue
        known.add(canonical_id)
        _version, book, chapter, start, end = parse_canonical_id(canonical_id)
        windows.setdefault(language, []).append(
            (canonical_id, book, chapter, start, end)
        )
    return windows


def _load_active_translations(cursor) -> dict[str, list[tuple[int, str]]]:
    """language -> [(code, alias), ...] of every ACTIVE translation."""
    cursor.execute(
        "SELECT code, alias, language FROM translations "
        "WHERE active = 1 ORDER BY code"
    )
    active: dict[str, list[tuple[int, str]]] = {}
    for row in cursor.fetchall():
        active.setdefault(row["language"], []).append(
            (row["code"], row["alias"])
        )
    return active


def _build_catalogue(
    cursor, index, psalm_maps: dict, indexed: dict, primary: dict
) -> tuple[dict, dict]:
    """Renderable catalogue + per-translation coverage sets.

    A translation is renderable when

    - its language has an index (there is nothing to retrieve otherwise);
    - it is active in `translations`;
    - it has a Psalm versification map, so a Psalm window can be converted
      into its numbering at all (ADR 0003). A translation without one is
      dropped WHOLE, not only for its Psalm windows: see ADR 0007;
    - at least one canonical window of its language exists in it fully.

    The window universe offered to a non-primary translation is additionally
    narrowed to the windows whose stored REFERENCE chunk really is the
    window's own range (`reference_faithful_windows`) — otherwise the reader
    would get a passage the reranker judged with a tail of extra verses
    attached (ADR 0007, fix F2).

    The primary of each language skips both steps entirely: it IS the corpus,
    its request path must stay byte-for-byte what it was, and the filter
    would be a no-op that costs a full verse scan.
    """
    windows = _canonical_windows(index)
    active = _load_active_translations(cursor)
    renderable: dict[str, list[tuple[int, str]]] = {}
    coverage: dict[int, frozenset[str]] = {}
    for language, entries in indexed.items():
        primary_code = primary.get(language, min(c for c, _a in entries))
        primary_alias = next(
            (alias for code, alias in entries if code == primary_code), ""
        )
        catalogue = [(primary_code, primary_alias)]
        others = [
            entry for entry in active.get(language, [])
            if entry[0] != primary_code
        ]
        reference_code = reference_translation(indexed, language) or primary_code
        offered = (
            reference_faithful_windows(
                windows.get(language, []),
                load_chunk_ranges(cursor, reference_code),
                psalm_maps.get(reference_code),
            )
            if others else []
        )
        for code, alias in others:
            if code not in psalm_maps:
                logger.warning(
                    "Translation %s has no Psalm versification map; "
                    "it cannot be served by scripture selection",
                    code,
                )
                continue
            covered = frozenset(
                build_coverage(cursor, code, offered, psalm_maps[code])
            )
            if not covered:
                logger.warning(
                    "Translation %s covers no canonical window of the corpus; "
                    "it cannot be served by scripture selection",
                    code,
                )
                continue
            coverage[code] = covered
            catalogue.append((code, alias))
        renderable[language] = catalogue
    return renderable, coverage


def _load_resources() -> CorpusResources:
    connection = create_connection()
    if connection is None:
        raise ScriptureSelectUnavailable("database is not available")
    cursor = connection.cursor(dictionary=True)
    try:
        try:
            index = load_index(cursor)
        except IndexVersionUnavailable as error:
            # Not "the index is empty, rebuild it": nothing is wrong with the
            # stored corpus, this deployment simply does not name which index
            # version it reads. Pointing at `rebuild` here would send an
            # operator to a command that re-embeds the whole corpus.
            raise ScriptureSelectUnavailable(str(error)) from error
        if not len(index):
            raise ScriptureSelectUnavailable(
                "vector index is empty (run app/index_cli.py rebuild)"
            )
        lexical = load_lexical_indexes(cursor, CHUNKING_VERSION)
        try:
            psalm_maps = load_psalm_maps(cursor)
        except Exception as error:
            # The highlight of a Psalm needs the versification table; the
            # rest of the endpoint does not. Category only.
            logger.warning(
                "Psalm versification mapping unavailable: %s",
                type(error).__name__,
            )
            psalm_maps = {}
        indexed = _indexed_translations(index)
        primary = resolve_primary_translations(
            indexed, AI_SCRIPTURE_PRIMARY_TRANSLATIONS
        )
        try:
            translations, coverage = _build_catalogue(
                cursor, index, psalm_maps, indexed, primary
            )
        except Exception as error:
            # The catalogue only ADDS translations; without it the endpoint
            # still serves every indexed one exactly as before. Category only.
            logger.warning(
                "Renderable translation catalogue unavailable: %s",
                type(error).__name__,
            )
            translations, coverage = dict(indexed), {}
    finally:
        cursor.close()
        connection.close()

    return CorpusResources(
        index=index,
        lexical=lexical,
        translations=translations,
        loaded_at=time.monotonic(),
        psalm_maps=psalm_maps,
        indexed=indexed,
        primary=primary,
        coverage=coverage,
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
            time.monotonic() - cached.loaded_at < AI_SCRIPTURE_INDEX_CACHE_SECONDS
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

    No key is passed here on purpose: each client's constructor already
    defaults to the key its stage bills — the rewriter to
    `config.REWRITE_API_KEY` (`AI_SCRIPTURE_REWRITE_API_KEY` when the deployment
    splits billing, `GEMINI_API_KEY` otherwise), the embedder and the
    reranker to `GEMINI_API_KEY`.
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

def primary_translation(resources: CorpusResources, language: str) -> int:
    """The language's default translation (see resolve_primary_translations).

    Falls back to the first entry of the catalogue when no primary was
    resolved — the pre-catalogue behaviour, kept so a partially built
    CorpusResources still answers.
    """
    code = resources.primary.get(language)
    if code is not None:
        return code
    return resources.translations[language][0][0]


def resolve_translation(
    resources: CorpusResources, language: str, requested: int | None
) -> int:
    """Validate the requested translation or pick the language's primary.

    422 means one thing only: the code is not in the language's RENDERABLE
    catalogue — it belongs to another language, is inactive, is unknown, or
    cannot be resolved against the canonical corpus. The message never
    repeats anything but the number the caller already sent.
    """
    available = resources.translations.get(language, [])
    if not available:
        raise ScriptureSelectUnavailable(f"no indexed translation for {language}")
    if requested is None:
        return primary_translation(resources, language)
    if requested not in {code for code, _alias in available}:
        raise HTTPException(
            status_code=422,
            detail=f"Translation {requested} is not available for {language}",
        )
    return requested


def translation_alias(
    resources: CorpusResources, language: str, code: int
) -> str:
    """Alias of a catalogue entry (empty string when unknown)."""
    for entry_code, alias in resources.translations.get(language, []):
        if entry_code == code:
            return alias
    return ""


def coverage_filter(
    resources: CorpusResources, language: str, translation: int
) -> frozenset[str] | None:
    """Canonical windows a selection may choose from, or None for "all".

    The primary translation is never filtered: it is the corpus itself, and
    its request path must stay byte-for-byte the one measured in ADR 0006.
    Any other translation is restricted to the windows that fully exist in
    it, BEFORE the rerank — so the AI never chooses a passage the server
    would then be unable to render (the rerank prompt is untouched).

    Fail-CLOSED for a non-primary translation with no coverage set (review
    fix F4): a missing set means the catalogue and the coverage map have
    drifted apart, and "None" would read as "no restriction at all" — every
    window allowed for exactly the translation whose renderability was never
    established. The empty set allows nothing instead; the selection then
    degrades to the (equally empty) safe pool and answers 503, which is the
    honest outcome for a translation the server cannot vouch for.
    """
    if translation == primary_translation(resources, language):
        return None
    covered = resources.coverage.get(translation)
    if covered is None:
        logger.warning(
            "No coverage set for translation %s; refusing every window",
            translation,
        )
        return frozenset()
    return covered


def _run_selection(
    resources: CorpusResources,
    selection_request: SelectionRequest,
    deadline: Deadline,
    allowed_canonical_ids: frozenset[str] | None = None,
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
            load_verses=make_db_verse_loader(cursor),
            lexical_indexes=resources.lexical,
            # Independent round trips: embed the variants concurrently.
            embed_workers=REWRITE_VARIANTS,
            allowed_canonical_ids=allowed_canonical_ids,
        )
        return retriever.select_final(selection_request, deadline)
    finally:
        cursor.close()
        connection.close()


def _render_target_passage(
    candidate,
    translation: int,
    alias: str,
    psalm_maps: dict | None,
    reference: int | None = None,
    deadline: Deadline | None = None,
):
    """Passage of a translation that has no chunk of the chosen window.

    Reads `translation_verses` directly (app/passage_render.py) for the
    window's canonical range, converted into the translation's own
    coordinates. Grounded exactly like the indexed path: the text comes from
    MySQL, and a window that cannot be resolved returns None (the request
    then fails with 503 — silently serving another translation is forbidden).

    Two guards around that one round trip:

    - the request's time budget (review fix F6). This runs AFTER the pipeline
      returned, so an exhausted deadline means every stage already degraded;
      spending another DB round trip on top of an over-budget request buys
      nothing the caller is still waiting for. There is no cheaper answer to
      fall back to — the passage of THIS translation is the response — so the
      request ends in the documented 503;
    - the window was verified against the REFERENCE translation's stored
      chunk (`reference_faithful_windows`), which is only meaningful if the
      reranker actually read that translation. A candidate whose prompt
      passage is another one is refused rather than rendered on a hope
      (review fix F7, ADR 0007 OQ2); a candidate with no prompt passage at
      all (`prompt_passage` returns None) is refused the same way — fail
      CLOSED, not "no reference to check against". Unreachable with today's
      single indexed translation per language.

    Any MySQL failure of the rendering is reported as the same
    ScriptureSelectUnavailable every other DB failure of this endpoint
    raises: a documented 503, not a bare 500.
    """
    if deadline is not None and deadline.expired():
        raise ScriptureSelectUnavailable(
            "time budget exhausted before the passage could be rendered"
        )
    shown = prompt_passage(candidate)
    if reference is not None and (shown is None or shown.translation != reference):
        logger.warning(
            "Candidate was judged in translation %s, not the language's "
            "reference %s; refusing to render translation %s from its own "
            "range (ADR 0007 open question 2)",
            shown.translation if shown is not None else None,
            reference,
            translation,
        )
        raise ScriptureSelectUnavailable(
            "chosen candidate was not judged in the reference translation"
        )
    connection = create_connection()
    if connection is None:
        raise ScriptureSelectUnavailable("database is not available")
    cursor = connection.cursor(dictionary=True)
    try:
        return render_passage(
            cursor,
            translation,
            alias,
            candidate.book_number,
            candidate.chapter_number,
            candidate.verse_start,
            candidate.verse_end,
            (psalm_maps or {}).get(translation),
        )
    except ScriptureSelectUnavailable:
        raise
    except Exception as error:
        # Category only — never the passage or the prayer context.
        logger.warning(
            "Rendering the chosen passage failed: %s", type(error).__name__
        )
        raise ScriptureSelectUnavailable(
            "the chosen passage could not be read from the database"
        ) from error
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


def build_highlight(
    final: FinalSelection,
    passage,
    psalm_maps: dict | None,
) -> HighlightModel | None:
    """Public highlight of the chosen passage, or None.

    `final.highlight` is a validated span of verse markers inside the
    passage the reranker was shown; `passage_highlight.resolve_highlight`
    turns it into canonical coordinates and into the numbering of the
    translation actually returned (they differ for Psalms). Anything that
    cannot be mapped exactly yields None — an absent highlight is always a
    valid answer, a guessed one never is.
    """
    if final.candidate is None or final.highlight is None:
        return None
    shown = prompt_passage(final.candidate)
    if shown is None:
        return None
    resolved = resolve_highlight(
        book_number=final.candidate.book_number,
        prompt_passage=shown,
        target_passage=passage,
        indices=final.highlight,
        psalm_maps=psalm_maps,
    )
    if resolved is None:
        return None
    return HighlightModel(
        canonical=HighlightCanonical(
            book_number=final.candidate.book_number,
            chapter_number=resolved.canonical.chapter,
            verse_start=resolved.canonical.verse_start,
            verse_end=resolved.canonical.verse_end,
        ),
        passage=HighlightPassage(
            chapter_number=resolved.passage.chapter,
            verse_start=resolved.passage.verse_start,
            verse_end=resolved.passage.verse_end,
        ),
    )


def build_passage_verses(passage) -> list[VerseModel] | None:
    """Verse boundaries of the passage actually served, or None.

    The verses come from the SAME object the text does — the chunk of an
    indexed translation whose verses the pipeline loaded
    (`retrieval.make_db_verse_loader`), or the own-range rendering of a
    non-indexed one (`passage_render.render_passage`) — so the numbering is
    the returned translation's own, exactly like `highlight.passage`, and no
    second interpretation of the passage can drift from the first.

    `paragraph_start` reports the paragraph structure of `passage.text`, which
    `chunking.build_text` derives from exactly two rules: the verse's own
    `start_paragraph` flag and a section title standing before it
    (`VerseText.title_break`). The first verse always opens a paragraph. So
    `build_text` over these verses reproduces `passage.text` byte for byte.

    None when the passage carries no verses: the caller then omits the field
    rather than serving an empty list. That happens when the verse loader
    failed (best effort by design — it also powers the highlight), when the
    pipeline ran without one at all, or when the served chunk is not the one
    the candidates were rendered in (only reachable with a second indexed
    translation per language, which the corpus does not have today).
    """
    if not passage.verses:
        return None
    return [
        VerseModel(
            number=verse.verse_number,
            text=verse.text,
            paragraph_start=bool(
                index == 0 or verse.start_paragraph or verse.title_break
            ),
        )
        for index, verse in enumerate(passage.verses)
    ]


def indexed_passage(candidate, translation: int):
    """The candidate's own chunk in a translation, when it has one."""
    if candidate is None:
        return None
    return next(
        (p for p in candidate.passages if p.translation == translation), None
    )


def build_response(
    final: FinalSelection,
    language: str,
    translation: int,
    history_reset: bool,
    psalm_maps: dict | None = None,
    passage=None,
) -> SelectResponse:
    """Map the internal selection onto the public contract.

    `passage` is the already-resolved rendering of the chosen window in the
    requested translation; when it is None the candidate's own chunk is used
    (the indexed path, unchanged).

    `FinalSelection.reason` (the model's diagnostic sentence) is
    deliberately dropped here — it is server-side only.
    """
    candidate = final.candidate
    if candidate is None:
        raise ScriptureSelectUnavailable("no candidate passage")
    if passage is None:
        passage = indexed_passage(candidate, translation)
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
            verses=build_passage_verses(passage),
        ),
        highlight=build_highlight(final, passage, psalm_maps),
        source=source,
        fallback_reason=_fallback_reason(reason),
        history_reset=history_reset,
    )


@router.post(
    "/ai/scripture",
    response_model=SelectResponse,
    operation_id="ai_scripture",
    tags=["AI"],
    summary="Select one Bible passage for a prayer context",
    description=(
        "Returns a single passage chosen for the prayer context: canonical "
        "coordinates, the exact text of the requested translation and a "
        "stable canonical ID.\n\n"
        "The passage is always a real verse range of the requested "
        "translation taken from the database — the AI stage only picks "
        "among candidates the server retrieved, it never produces "
        "scripture text or references itself.\n\n"
        "`passage.verses` carries the same text split into its verses, in "
        "the returned translation's own numbering — the same numbering "
        "`highlight.passage` speaks, so the key verses are placed by "
        "matching `number`, never by counting characters or guessing verse "
        "boundaries inside `text`. It is omitted (never null, never empty) "
        "in the rare case where the server has the text but not the "
        "breakdown.\n\n"
        "`highlight` is optional: when the AI stage also marked the key "
        "verses of the passage (1-3 verses), it carries their canonical "
        "and translation coordinates — the translation ones always inside "
        "the returned passage, while the canonical range can be wider "
        "where the translation merges canonical verses. The field is "
        "omitted entirely when there is no highlight; the passage is then "
        "rendered as before.\n\n"
        "Degradation is part of the contract, not an error: when the AI "
        "choice is unavailable the retrieval top candidate is served "
        "(`source=retrieval_fallback`), and when retrieval itself cannot "
        "run — empty topic, provider outage, exhausted time budget — or its "
        "candidates, once narrowed by the requested translation's coverage "
        "together with the caller's exclusions and the genre blacklist, "
        "leave nothing to choose from (`coverage_empty` for a non-primary "
        "translation of an incomplete Bible, `ranking_empty` when there is "
        "no coverage filter and the exclusions or the blacklist emptied the "
        "ranking), a curated safe pool is served (`source=safe_pool`). "
        "`fallback_reason` carries the category.\n\n"
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
                "unsupported language, or a translation that is not among "
                "those available for the language"
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
                "rate limiter unavailable, the chosen passage cannot be read "
                "in the requested translation, or the time budget ran out "
                "before it could be)"
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
    language = request.language.value
    allowed = coverage_filter(resources, language, translation)
    deadline = Deadline(AI_SCRIPTURE_TIMEOUT_SECONDS)
    try:
        final = await run_in_threadpool(
            _run_selection, resources, selection_request, deadline, allowed
        )
        passage = indexed_passage(final.candidate, translation)
        if passage is None and final.candidate is not None:
            # A renderable translation that was never chunked: build the
            # passage from its own verses for the same canonical window.
            passage = await run_in_threadpool(
                _render_target_passage,
                final.candidate,
                translation,
                translation_alias(resources, language, translation),
                resources.psalm_maps,
                reference_translation(resources.indexed, language),
                deadline,
            )
        return build_response(
            final,
            language,
            translation,
            history_reset=bool(stale),
            psalm_maps=resources.psalm_maps,
            passage=passage,
        )
    except ScriptureSelectUnavailable as error:
        logger.warning("Scripture selection unavailable: %s", error)
        raise HTTPException(
            status_code=503, detail="Scripture selection temporarily unavailable"
        ) from error
