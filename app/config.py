import os
from collections.abc import Mapping


class ConfigError(RuntimeError):
    """Configuration is unusable; raised once with every problem found.

    A RuntimeError subclass so existing callers that expected the old
    `_require` failure keep catching it.
    """


# ---------------------------------------------------------------------------
# Rule (2026-08-29): no silent fallbacks for settings that change behaviour.
#
# An unset *operational* parameter (limits, TTLs, timeouts) still falls back
# to the documented default: those are tuning knobs, and their default is the
# behaviour we intend. A *malformed* value never falls back — a typo in
# `AI_SCRIPTURE_TIMEOUT_SECONDS=15s` used to be swallowed and the service
# silently ran on 15.0 anyway.
#
# Model variables have no defaults at all. The scripture-selection incident of
# 2026-08-29 was invisible precisely because `AI_SCRIPTURE_REWRITE_MODEL`
# defaulted to a model the owner had not configured and which the key could
# not reach.
#
# Deliberate limitation of the requirement: the models of the *live provider
# calls* are required only when `GEMINI_API_KEY` is set. Without the key that
# whole surface is "not configured" — those endpoints already answer with
# their own error and the rest of the API must keep working, so demanding
# their model names would turn a supported deployment (Bible API without AI)
# into a startup failure.
#
# EMBEDDING_MODEL / EMBEDDING_DIMENSIONS are the exception and are required
# ALWAYS, key or no key: they do not name a provider call, they name the
# vector index this service READS (`c{chunking}:{model}@{dims}` — ADR 0002).
# The documented no-AI contract of `POST /api/ai/scripture` is a 200
# from the safe pool with `fallback_reason=ai_unavailable`, and even that
# answer is resolved through the loaded corpus. Making the pair conditional on
# the key turned that 200 into a 503 ("vector index is empty"), because an
# unset pair silently addressed the non-existent index version `c3:@0` — the
# very class of bug this rule exists to prevent. Naming the index one reads is
# not optional; there is no correct value to guess.
#
# Naming (2026-08-30, ClickUp 86cbbmy8d): the AI variables mirror the method
# they configure — `AI_*` for the whole AI surface, `AI_SCRIPTURE_*` for the
# selection pipeline only. The names in this file are the new ones throughout;
# the 2026-08-29 incident above was reported against the former
# `RETRIEVAL_REWRITE_MODEL`. No old name is accepted as an alias on purpose:
# a place this rename forgot fails the start with the variable it wants, which
# is exactly the fail-fast behaviour this module exists for.
# ---------------------------------------------------------------------------

# Required in every deployment, whatever is configured. Blank counts as unset.
ALWAYS_REQUIRED_VARS = (
    "API_KEY",
    "DB_HOST",
    "DB_USER",
    "DB_NAME",
    "EMBEDDING_MODEL",
    "EMBEDDING_DIMENSIONS",
)
# Must be PRESENT in the environment, but may be empty: MySQL accepts an empty
# password (a local server with a passwordless user is a legitimate setup), so
# `DB_PASSWORD=` is an explicit statement, while a missing variable is the
# silence this rule forbids. Both the local and the production .env set a real
# password today.
PRESENCE_REQUIRED_VARS = ("DB_PASSWORD",)
# Models of the live Gemini calls: required once AI is configured.
AI_REQUIRED_VARS = (
    "AI_QUESTION_MODEL",
    "AI_TRANSCRIBE_MODEL",
    "AI_SCRIPTURE_REWRITE_MODEL",
    "AI_SCRIPTURE_RERANK_MODEL",
)


def parse_int(name: str, raw: str | None, default: int) -> int:
    """Unset/empty -> default; a non-numeric value -> ConfigError naming it."""
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        raise ConfigError(
            f"{name}: expected an integer, got {raw!r}"
        ) from None


def parse_float(name: str, raw: str | None, default: float) -> float:
    """Unset/empty -> default; a non-numeric value -> ConfigError naming it."""
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw.strip())
    except ValueError:
        raise ConfigError(
            f"{name}: expected a number, got {raw!r}"
        ) from None


def missing_required_vars(env: Mapping[str, str]) -> list[str]:
    """Names of variables that must be set for this environment, but are not.

    Pure function over an environment mapping, so it is testable without
    reimporting the module. `ALWAYS_REQUIRED_VARS` must be non-blank,
    `PRESENCE_REQUIRED_VARS` must merely exist, and `AI_REQUIRED_VARS` are
    added when `GEMINI_API_KEY` is set (see the limitation note above).
    """
    missing = [
        name for name in ALWAYS_REQUIRED_VARS if not env.get(name, "").strip()
    ]
    missing.extend(name for name in PRESENCE_REQUIRED_VARS if name not in env)
    if env.get("GEMINI_API_KEY", "").strip():
        missing.extend(
            name for name in AI_REQUIRED_VARS if not env.get(name, "").strip()
        )
    return missing


def resolve_rewrite_api_key(env: Mapping[str, str]) -> str:
    """The key the retrieval *rewrite* stage calls Gemini with.

    `AI_SCRIPTURE_REWRITE_API_KEY` when it is set and non-blank, otherwise
    `GEMINI_API_KEY`. Blank counts as unset, so `AI_SCRIPTURE_REWRITE_API_KEY=`
    is the same statement as omitting it: "this deployment runs on one key".
    Both sides are stripped: a whitespace-only value is "unset" for the
    validation below, and returning it raw would put whitespace into the
    `x-goog-api-key` header instead of raising the "not configured" error.

    Why this fallback does not violate ADR 0008. The rule forbids a default
    that silently substitutes an *unreviewed behaviour* for a value that was
    supposed to be set. Here the absent value has exactly one intended
    meaning, and it is the meaning every deployment had before the variable
    existed: every Gemini stage bills the same key. The variable exists only
    to split *billing*, not behaviour — the rewrite model, prompt and
    request are identical either way (ADR 0004). Requiring it would turn the
    supported single-key deployment into a startup failure for no gain,
    which is the same trade already made for `GEMINI_API_KEY` itself.

    The asymmetric case IS an error and is reported by
    `invalid_required_values()`: a rewrite key set while `GEMINI_API_KEY` is
    empty pays for one stage of a pipeline whose remaining stages
    (embeddings, rerank) cannot run at all.
    """
    dedicated = env.get("AI_SCRIPTURE_REWRITE_API_KEY", "").strip()
    if dedicated:
        return dedicated
    return env.get("GEMINI_API_KEY", "").strip()


def invalid_required_values(env: Mapping[str, str]) -> list[str]:
    """Problems with values that ARE set but cannot be used, as messages.

    Non-numeric values are not reported here — `parse_int`/`parse_float`
    already name them while reading. This is the range check the type alone
    does not give: `EMBEDDING_DIMENSIONS=0` parses fine and then produces the
    index version `...@0` and an `outputDimensionality: 0` request.
    """
    problems = []
    raw = env.get("EMBEDDING_DIMENSIONS", "").strip()
    if raw:
        try:
            dims = int(raw)
        except ValueError:
            dims = None  # reported by parse_int, not twice
        if dims is not None and dims < 1:
            problems.append(
                f"EMBEDDING_DIMENSIONS: expected a positive integer, got {dims}"
            )
    if (
        env.get("AI_SCRIPTURE_REWRITE_API_KEY", "").strip()
        and not env.get("GEMINI_API_KEY", "").strip()
    ):
        problems.append(
            "AI_SCRIPTURE_REWRITE_API_KEY: set while GEMINI_API_KEY is empty — "
            "the rewrite key pays for one stage of a pipeline whose other "
            "stages (embeddings, rerank) have no key at all"
        )
    return problems


# Problems collected while parsing, reported together by _validate() below.
_problems: list[str] = []


def _get_int(name: str, default: int) -> int:
    try:
        return parse_int(name, os.getenv(name), default)
    except ConfigError as exc:
        _problems.append(str(exc))
        return default


def _get_float(name: str, default: float) -> float:
    try:
        return parse_float(name, os.getenv(name), default)
    except ConfigError as exc:
        _problems.append(str(exc))
        return default


def _validate(env: Mapping[str, str], problems: list[str]) -> None:
    """Raise one ConfigError listing every problem, or return silently."""
    reasons = [
        f"{name} is required"
        + (
            " when GEMINI_API_KEY is set (no default: the model must be named"
            " explicitly)"
            if name in AI_REQUIRED_VARS
            else ""
        )
        for name in missing_required_vars(env)
    ]
    reasons.extend(invalid_required_values(env))
    reasons.extend(problems)
    if reasons:
        raise ConfigError(
            "Invalid configuration ("
            f"{len(reasons)} problem{'s' if len(reasons) > 1 else ''}):\n"
            + "\n".join(f"  - {reason}" for reason in reasons)
        )


# Database. Host, user, name and password carry no defaults: "localhost /
# root / cep_public" silently pointed a misconfigured deployment at whatever
# database happened to answer. DB_PORT keeps its default (3306 is the port of
# the protocol, not a choice about which data is served).
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = _get_int("DB_PORT", 3306)
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "")

# Path to the audio (mp3) file storage inside the container. Renamed from
# MP3_FILES_PATH on 2026-08-30: the host-side bind mount is AUDIO_DIR and the
# public URL prefix is AUDIO_BASE_URL, so all three now read as one family.
AUDIO_FILES_PATH = os.getenv("AUDIO_FILES_PATH", "audio")

# Base URL for audio files
AUDIO_BASE_URL = os.getenv("AUDIO_BASE_URL", "http://localhost:8000")

# API Authorization settings (required)
API_KEY = os.getenv("API_KEY", "")

# Admin API connection settings (for import)
ADMIN_API_URL = os.getenv("ADMIN_API_URL", "http://dashboard-api:8000")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

# Import safety valve (ClickUp 86cbbq5zp). Operational parameters: their
# defaults ARE the intended working configuration, not a stand-in for a
# setting somebody forgot — a malformed value is still a startup error.
#
# IMPORT_MAX_PAYLOAD_MB caps the body of ONE `GET /api/data` response the
# importer is willing to buffer. The importer walks translations one at a
# time, so the cap is per translation, not per resync: the largest today is
# `syn` at 29.3 MB, and the whole export (the 147 MB that OOM-killed
# production on 2026-08-30) is never requested at all. 48 MB leaves room for
# the largest translation to grow by ~60% before the valve trips, and a trip
# is a loud 507 raised BEFORE that translation's rows are touched.
#
# Why not more (review of 86cbbq5zp, NIT-2): the cap is only useful if the
# container can survive reaching it. Parsing a payload costs several times its
# size in RSS, so on the 2-4 GB production VM — which also runs MySQL — a 96 MB
# body could OOM-kill the worker before it ever got to answer 507, which is
# precisely the failure this valve exists to replace.
IMPORT_MAX_PAYLOAD_MB = _get_int("IMPORT_MAX_PAYLOAD_MB", 48)

# Per-request timeout of the calls to admin-api. A full resync is now many
# requests instead of one, and each is bounded by this rather than by the
# total; the largest single translation takes ~50 s to export locally.
IMPORT_HTTP_TIMEOUT_SECONDS = _get_float("IMPORT_HTTP_TIMEOUT_SECONDS", 300.0)

# Gemini API for the prayer companion. Optional at startup so the rest of
# Bible API remains available when AI is not configured. When it IS set, the
# provider-call models below must be named explicitly — empty strings there
# mean "AI not configured" and are only reachable without the key.
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
# Models of the two chat-shaped endpoints, named after the method each one
# serves: POST /api/ai/question and POST /api/ai/transcribe.
AI_QUESTION_MODEL = os.getenv("AI_QUESTION_MODEL", "")
AI_TRANSCRIBE_MODEL = os.getenv("AI_TRANSCRIBE_MODEL", "")
AI_REQUESTS_PER_MINUTE = max(1, _get_int("AI_REQUESTS_PER_MINUTE", 10))
AI_REQUESTS_PER_CLIENT_PER_MINUTE = min(
    AI_REQUESTS_PER_MINUTE,
    max(1, _get_int("AI_REQUESTS_PER_CLIENT_PER_MINUTE", 3)),
)
# Embedding model for the scripture-selection RAG index (see
# architect/adr/0002-embedding-model-and-vector-store.md). Uses the same
# GEMINI_API_KEY as the Twinkler endpoints. Model and dimensions are a pair:
# together they version the stored vectors, so neither may be guessed — and
# both are required even without a key, because the read path (including the
# no-AI safe-pool answer) has to name the index it loads. The 0 default is
# unreachable: _validate() rejects a missing or non-positive value.
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "")
EMBEDDING_DIMENSIONS = _get_int("EMBEDDING_DIMENSIONS", 0)
# Model for LLM query reformulation in the retrieval pipeline (see
# architect/adr/0004-retrieval-pipeline.md). Deliberately NOT following
# AI_QUESTION_MODEL: the benchmark passes the retrieval thresholds only with
# gemini-3.7-flash rewrites (gemini-3.5-flash-lite fails recall@10 and MRR).
# The value is pinned by that benchmark but must be spelled out in the
# environment — a default here hid a broken model behind a working config.
AI_SCRIPTURE_REWRITE_MODEL = os.getenv("AI_SCRIPTURE_REWRITE_MODEL", "")
# Key the rewrite stage bills. Deliberately NOT a variable-shaped constant:
# it is the *resolved* value of `AI_SCRIPTURE_REWRITE_API_KEY or GEMINI_API_KEY`
# (see resolve_rewrite_api_key for why that default is operational, not a
# hidden fallback). Rewrite is the only stage pinned to gemini-3.7-flash,
# whose free daily quota the retrieval traffic exhausts, while embeddings and
# the rerank live comfortably on the free lite-model quotas — so this one
# stage can be moved to a paid key without paying for the whole pipeline.
# Every other Gemini client (embeddings, passage_rerank, twinkler_ai) keeps
# reading GEMINI_API_KEY directly and must stay that way.
REWRITE_API_KEY = resolve_rewrite_api_key(os.environ)
# Model for the grounded passage rerank (final choice among retrieval
# candidates, see architect/adr/0005-grounded-passage-rerank.md). Pinned by
# the final_top1 benchmark: gemini-3.5-flash-lite passes every threshold on
# both rerank prompt versions (gemini-3.7-flash ties on v2 only) and is the
# cheaper/faster stage. Independent from AI_QUESTION_MODEL and
# AI_SCRIPTURE_REWRITE_MODEL so each stage can be tuned separately.
AI_SCRIPTURE_RERANK_MODEL = os.getenv("AI_SCRIPTURE_RERANK_MODEL", "")

# Public scripture-selection endpoint (see
# architect/adr/0006-scripture-select-api.md). Its own rate-limit budget:
# one selection costs ~8 Gemini calls (1 rewrite + 6 embeddings + 1
# rerank), so it must not share a counter with the chat-shaped Twinkler
# endpoints — either feature could otherwise starve the other.
AI_SCRIPTURE_REQUESTS_PER_MINUTE = max(
    1, _get_int("AI_SCRIPTURE_REQUESTS_PER_MINUTE", 10)
)
AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE = min(
    AI_SCRIPTURE_REQUESTS_PER_MINUTE,
    max(1, _get_int("AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE", 3)),
)
# Total time budget of one selection (rewrite + embeddings + rerank).
# Measured p50 ~5-6 s with concurrent variant embeddings; 15 s leaves room
# for a slow provider before the endpoint degrades to a verified fallback.
AI_SCRIPTURE_TIMEOUT_SECONDS = max(
    1.0, _get_float("AI_SCRIPTURE_TIMEOUT_SECONDS", 15.0)
)
# TTL of the process-local corpus cache (vector index + BM25 index). The
# cached data is prayer-independent; nothing derived from a request is
# cached. `POST /api/cache/clear` drops it immediately. Floored at 1
# second: a 0 would rebuild ~45 MB of indexes under a global lock on every
# request (self-inflicted denial of service), and "no caching" is not a
# configuration this endpoint can serve traffic with.
AI_SCRIPTURE_INDEX_CACHE_SECONDS = max(
    1, _get_int("AI_SCRIPTURE_INDEX_CACHE_SECONDS", 3600)
)
# Which translation each language is served in when a selection request does
# not name one (ADR 0006 open question 5, closed by ADR 0007). Format:
# "ru=syn,en=bsb,uk=ubh" — comma separated `language=alias` or
# `language=code` pairs; whitespace is ignored and an entry naming a
# translation that is not INDEXED for that language is ignored with a
# warning. Empty (the default) means: the indexed translation with the
# lowest code, which is deterministic and — while every language has exactly
# one indexed translation — identical to the previous "first in index order".
AI_SCRIPTURE_PRIMARY_TRANSLATIONS = os.getenv(
    "AI_SCRIPTURE_PRIMARY_TRANSLATIONS", ""
)

# Salt of the in-memory client pseudonyms (rate limiter + request stats).
# Renamed from TWINKLER_CLIENT_HMAC_KEY on 2026-08-30 without touching the
# VALUE, so every existing pseudonym stays stable across the rename: the HMAC
# is keyed by the value alone — the variable name is never mixed into the
# digest (see client_ip.pseudonymize_twinkler_client).
#
# The system prompt of POST /api/ai/question used to be read here as
# TWINKLER_SYSTEM_PROMPT. It is product behaviour, not deployment
# configuration, and now lives in question_prompt.QUESTION_PROMPT.
AI_CLIENT_HMAC_KEY = os.getenv("AI_CLIENT_HMAC_KEY", "").strip()
TRUSTED_PROXY_IPS = frozenset(
    value.strip()
    for value in os.getenv("TRUSTED_PROXY_IPS", "").split(",")
    if value.strip()
)

# Fail fast: one aggregated error with everything that is wrong, so a broken
# deployment is fixed in a single pass instead of one variable per restart.
_validate(os.environ, _problems)
