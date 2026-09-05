import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from ipaddress import IPv4Network, IPv6Network, ip_address, ip_network
from urllib.parse import urlsplit


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
# into a startup failure. Since ADR 0009 there is one more way for a stage to
# be "configured" than a Gemini key: a chat stage whose provider is
# `openai_compat` must name its model whatever `GEMINI_API_KEY` says, because
# naming that provider IS the statement that the stage runs (see the provider
# section below).
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
    "EMBEDDING_PROVIDER",
)
# Must be PRESENT in the environment, but may be empty: MySQL accepts an empty
# password (a local server with a passwordless user is a legitimate setup), so
# `DB_PASSWORD=` is an explicit statement, while a missing variable is the
# silence this rule forbids. Both the local and the production .env set a real
# password today.
PRESENCE_REQUIRED_VARS = ("DB_PASSWORD",)
# Models of the live provider calls: required once AI is configured.
AI_REQUIRED_VARS = (
    "AI_QUESTION_MODEL",
    "AI_TRANSCRIBE_MODEL",
    "AI_SCRIPTURE_REWRITE_MODEL",
    "AI_SCRIPTURE_RERANK_MODEL",
)

# ---------------------------------------------------------------------------
# Which provider serves which stage (ClickUp 86cbegg2f, ADR 0009)
#
# Gemini used to be wired into four modules. The three CHAT-shaped stages now
# name their transport in the environment, so moving a stage to a local model
# is an `.env` edit plus a benchmark rather than a code change:
#
#   AI_QUESTION_PROVIDER           POST /api/ai/question
#   AI_SCRIPTURE_REWRITE_PROVIDER  the retrieval rewrite stage (ADR 0004)
#   AI_SCRIPTURE_RERANK_PROVIDER   the grounded rerank stage (ADR 0005)
#
# each `gemini` or `openai_compat`. The MODEL variable of a stage is the same
# one as before — the provider decides how it is interpreted (a Gemini model
# id, or the `model` field of a chat-completions request).
#
# Two stages are deliberately NOT in this table:
#
# * transcription (`AI_TRANSCRIBE_MODEL`) is Gemini-only and stays that way
#   until step 6 replaces it with Whisper (ClickUp 86cbe4mtq). There is no
#   `AI_TRANSCRIBE_PROVIDER`; setting one is a startup error rather than a
#   variable that quietly does nothing. Without `GEMINI_API_KEY` the endpoint
#   answers its documented 502 — an explicit refusal, not a crash — even when
#   every chat stage runs on another provider.
# * embeddings (`EMBEDDING_MODEL`) are step 3 (bge-m3, ClickUp 86cbegg2r) and
#   are untouched here: they name the stored index, not just a call.
#
# "AI is configured" is now: `GEMINI_API_KEY` is set OR a provider is named.
# Once configured, all three provider variables must be named — an `.env`
# that predates this change (a Gemini key and no providers) does NOT start,
# and says which three variables it wants. That is the point: which provider
# answers a request is exactly the class of decision ADR 0008 forbids
# defaulting in code, and a transitional "assume gemini" default would have
# been invisible in `.env` for as long as it lasted.
#
# `GEMINI_API_KEY` itself stays optional, as ADR 0008 promised: a stage on
# `gemini` without a key is the supported "deploy without AI" state (502 /
# safe pool), not a startup failure.
# ---------------------------------------------------------------------------

PROVIDER_GEMINI = "gemini"
PROVIDER_OPENAI_COMPAT = "openai_compat"
AI_PROVIDERS = (PROVIDER_GEMINI, PROVIDER_OPENAI_COMPAT)

# Shared endpoint/key of every openai_compat stage; a stage may override both.
OPENAI_COMPAT_ENDPOINT_VAR = "AI_OPENAI_COMPAT_ENDPOINT"
OPENAI_COMPAT_API_KEY_VAR = "AI_OPENAI_COMPAT_API_KEY"
# Gemini-only, and named here so the error can say so.
TRANSCRIBE_PROVIDER_VAR = "AI_TRANSCRIBE_PROVIDER"


@dataclass(frozen=True)
class StageVars:
    """The four environment variables that configure one chat stage."""

    stage: str
    provider_var: str
    model_var: str
    endpoint_var: str
    api_key_var: str


QUESTION_STAGE_VARS = StageVars(
    "question",
    "AI_QUESTION_PROVIDER",
    "AI_QUESTION_MODEL",
    "AI_QUESTION_ENDPOINT",
    "AI_QUESTION_API_KEY",
)
SCRIPTURE_REWRITE_STAGE_VARS = StageVars(
    "scripture_rewrite",
    "AI_SCRIPTURE_REWRITE_PROVIDER",
    "AI_SCRIPTURE_REWRITE_MODEL",
    "AI_SCRIPTURE_REWRITE_ENDPOINT",
    # Pre-dates the provider switch: it was the paid-key override of the
    # rewrite stage (ADR 0004/0008). Its meaning is unchanged and merely
    # generalised — "the key THIS stage bills", whoever serves it.
    "AI_SCRIPTURE_REWRITE_API_KEY",
)
SCRIPTURE_RERANK_STAGE_VARS = StageVars(
    "scripture_rerank",
    "AI_SCRIPTURE_RERANK_PROVIDER",
    "AI_SCRIPTURE_RERANK_MODEL",
    "AI_SCRIPTURE_RERANK_ENDPOINT",
    "AI_SCRIPTURE_RERANK_API_KEY",
)
AI_STAGE_VARS = (
    QUESTION_STAGE_VARS,
    SCRIPTURE_REWRITE_STAGE_VARS,
    SCRIPTURE_RERANK_STAGE_VARS,
)
AI_PROVIDER_VARS = tuple(stage.provider_var for stage in AI_STAGE_VARS)

# ---------------------------------------------------------------------------
# Who computes the embeddings (ClickUp 86cbegg2r, ADR 0010)
#
# `EMBEDDING_PROVIDER` is `gemini` (the API of ADR 0002) or `local` (bge-m3
# in this process through sentence-transformers). Unlike the three chat
# providers it is required ALWAYS, with or without any AI key — for the same
# reason `EMBEDDING_MODEL` / `EMBEDDING_DIMENSIONS` are: those three name the
# index this service READS, and the read path runs even in the documented
# no-AI deployment (`fallback_reason=ai_unavailable` is still resolved
# through the loaded corpus). There is no correct value to guess: the two
# providers produce different vector spaces, and picking one silently would
# search a 1024-dimension index with a 768-dimension query — or, worse,
# rebuild the index in the other space.
#
# This breaks every `.env` written before this change, exactly as the three
# `AI_*_PROVIDER` variables did in step 2, and for the same reason: the
# alternative is a default that is invisible in `.env` for as long as it
# lasts. The startup error names the variable.
#
# `EMBEDDING_MODEL_PATH` is required when, and only when, the provider is
# `local`: the weights are a read-only volume in the container, and nothing
# is ever resolved through the Hugging Face hub (the image also sets
# HF_HUB_OFFLINE=1, so a missing directory is a loud startup failure rather
# than a 2.3 GB download from a machine that may have no route to it).
# `EMBEDDING_MODEL` keeps naming the model *identity* — it is half of the
# stored index version — while the path says where its bytes are on THIS
# machine; conflating the two would put a filesystem path into
# `chunk_embeddings.embedding_version`.
# ---------------------------------------------------------------------------

EMBEDDING_PROVIDER_GEMINI = "gemini"
EMBEDDING_PROVIDER_LOCAL = "local"
EMBEDDING_PROVIDERS = (EMBEDDING_PROVIDER_GEMINI, EMBEDDING_PROVIDER_LOCAL)
EMBEDDING_PROVIDER_VAR = "EMBEDDING_PROVIDER"
EMBEDDING_MODEL_PATH_VAR = "EMBEDDING_MODEL_PATH"


@dataclass(frozen=True)
class StageProvider:
    """The resolved transport of one chat stage.

    `provider` is "" only when nothing named one (AI not configured, or a
    configuration `_validate` has already refused). `endpoint` is empty for
    Gemini, whose URL is a constant of the stage module.
    """

    stage: str
    provider: str
    model: str
    endpoint: str
    api_key: str

    @property
    def is_gemini(self) -> bool:
        return self.provider == PROVIDER_GEMINI

    @property
    def is_openai_compat(self) -> bool:
        return self.provider == PROVIDER_OPENAI_COMPAT


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


# A DNS label per RFC 1123: letters, digits and inner dashes, up to 63 chars
# per label. Deliberately conservative — anything else in TRUSTED_PROXY_HOSTS
# is a typo, and a typo in a *trust* setting must not be resolved silently.
_HOSTNAME_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
_HOSTNAME_RE = re.compile(rf"^{_HOSTNAME_LABEL}(?:\.{_HOSTNAME_LABEL})*\.?$")


def parse_trusted_proxy_ips(
    name: str, raw: str | None
) -> tuple[frozenset[str], tuple[IPv4Network | IPv6Network, ...]]:
    """Comma-separated IP addresses and/or CIDR networks -> (addresses, nets).

    Unset/empty is a valid, meaningful configuration ("no proxy in front of
    this deployment"), which is how the local machine runs. Anything that is
    neither an IP address nor a CIDR network is a ConfigError naming the
    variable and the offending token: this setting decides whose
    `X-Forwarded-For` is believed, so a value nobody can parse must never be
    quietly skipped.

    Networks are parsed strictly: `172.18.0.5/16` is rejected rather than
    silently widened to `172.18.0.0/16`, because "one address" and "the whole
    subnet" are very different amounts of trust and the difference is exactly
    the typo a strict parse catches.
    """
    addresses: set[str] = set()
    networks: list[IPv4Network | IPv6Network] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        if "/" in token:
            try:
                networks.append(ip_network(token))
            except ValueError as error:
                raise ConfigError(
                    f"{name}: {token!r} is not a valid CIDR network ({error})"
                ) from None
        else:
            try:
                addresses.add(str(ip_address(token)))
            except ValueError:
                raise ConfigError(
                    f"{name}: {token!r} is not a valid IP address or CIDR "
                    f"network (a host NAME belongs in TRUSTED_PROXY_HOSTS)"
                ) from None
    return frozenset(addresses), tuple(networks)


def parse_trusted_proxy_hosts(name: str, raw: str | None) -> tuple[str, ...]:
    """Comma-separated DNS names of the reverse proxies, in order, deduped.

    An IP literal here is an error: addresses belong in `TRUSTED_PROXY_IPS`,
    and accepting them in both places would make "which variable is stale"
    ambiguous during exactly the incident this exists to prevent.
    """
    hosts: list[str] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            ip_address(token)
        except ValueError:
            pass
        else:
            raise ConfigError(
                f"{name}: {token!r} is an IP address, not a host name — put "
                f"it in TRUSTED_PROXY_IPS"
            )
        if len(token) > 253 or not _HOSTNAME_RE.match(token):
            raise ConfigError(f"{name}: {token!r} is not a valid host name")
        if token not in hosts:
            hosts.append(token)
    return tuple(hosts)


def ai_configured(env: Mapping[str, str]) -> bool:
    """Is any AI transport declared in this environment?

    Either the historical switch (`GEMINI_API_KEY`) or a named provider. A
    deployment that says neither is "Bible API without AI": the AI endpoints
    answer their documented errors and nothing about them is required.
    """
    if env.get("GEMINI_API_KEY", "").strip():
        return True
    return any(env.get(name, "").strip() for name in AI_PROVIDER_VARS)


def resolve_stage(env: Mapping[str, str], stage: StageVars) -> StageProvider:
    """The transport of one chat stage, as configured.

    Key resolution keeps the shape `AI_SCRIPTURE_REWRITE_API_KEY` has had
    since ADR 0004 and generalises it to every stage and both providers: the
    stage's own key when it is set, otherwise the provider's shared key
    (`GEMINI_API_KEY` / `AI_OPENAI_COMPAT_API_KEY`). Blank counts as unset on
    the stage side, so `AI_QUESTION_API_KEY=` is the same statement as
    omitting it. On the shared openai_compat side a blank value is NOT unset:
    it is the explicit "this endpoint needs no Authorization header", which is
    why the presence of that variable is what `missing_required_vars` checks.

    An unset provider resolves like Gemini for the key, so that a deployment
    predating this change keeps `REWRITE_API_KEY` identical while `_validate`
    tells its operator to name the providers.
    """
    provider = env.get(stage.provider_var, "").strip()
    model = env.get(stage.model_var, "").strip()
    own_key = env.get(stage.api_key_var, "").strip()
    if provider == PROVIDER_OPENAI_COMPAT:
        endpoint = (
            env.get(stage.endpoint_var, "").strip()
            or env.get(OPENAI_COMPAT_ENDPOINT_VAR, "").strip()
        )
        api_key = own_key or env.get(OPENAI_COMPAT_API_KEY_VAR, "").strip()
        return StageProvider(stage.stage, provider, model, endpoint, api_key)
    api_key = own_key or env.get("GEMINI_API_KEY", "").strip()
    return StageProvider(stage.stage, provider, model, "", api_key)


def stage_model_required(env: Mapping[str, str], stage: StageVars) -> bool:
    """Must this stage's model be named?

    With `GEMINI_API_KEY` set: yes, exactly as before this change. On
    openai_compat: yes regardless of any Gemini key — naming that provider IS
    the statement that the stage runs, and there is no key to gate it on.
    """
    if env.get("GEMINI_API_KEY", "").strip():
        return True
    return env.get(stage.provider_var, "").strip() == PROVIDER_OPENAI_COMPAT


def validate_endpoint(name: str, value: str) -> str | None:
    """Problem with an openai_compat endpoint, or None.

    Never echoes the value: an endpoint is exactly the place an operator may
    have pasted a key into, and this string is printed by the startup error.
    Credentials and query strings are refused for the same reason — the key
    belongs in `AI_*_API_KEY`, where nothing logs it.
    """
    parts = urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return (
            f"{name}: expected an absolute http(s) URL of the chat API "
            f"(for example https://host:8443/v1)"
        )
    if parts.username or parts.password:
        return (
            f"{name}: must not carry credentials — put the key in the "
            f"stage's API key variable, never in the URL"
        )
    if parts.query:
        return (
            f"{name}: must not carry a query string — a key in a URL leaks "
            f"into logs and error messages"
        )
    return None


def _openai_compat_missing(
    env: Mapping[str, str], stage: StageVars
) -> list[str]:
    """Endpoint/key variables an openai_compat stage still needs."""
    missing = []
    if not (
        env.get(stage.endpoint_var, "").strip()
        or env.get(OPENAI_COMPAT_ENDPOINT_VAR, "").strip()
    ):
        missing.append(OPENAI_COMPAT_ENDPOINT_VAR)
    if not (
        env.get(stage.api_key_var, "").strip()
        or OPENAI_COMPAT_API_KEY_VAR in env
    ):
        missing.append(OPENAI_COMPAT_API_KEY_VAR)
    return missing


def missing_required_vars(env: Mapping[str, str]) -> list[str]:
    """Names of variables that must be set for this environment, but are not.

    Pure function over an environment mapping, so it is testable without
    reimporting the module. `ALWAYS_REQUIRED_VARS` must be non-blank,
    `PRESENCE_REQUIRED_VARS` must merely exist, and — once the AI surface is
    configured at all (`ai_configured`) — the three provider variables, the
    model of every stage that runs, and the endpoint/key of every stage on
    openai_compat. `AI_TRANSCRIBE_MODEL` keeps its old gate exactly: it is
    the Gemini-only stage, so it is required when, and only when, there is a
    Gemini key.
    """
    missing = [
        name for name in ALWAYS_REQUIRED_VARS if not env.get(name, "").strip()
    ]
    missing.extend(name for name in PRESENCE_REQUIRED_VARS if name not in env)
    if (
        env.get(EMBEDDING_PROVIDER_VAR, "").strip() == EMBEDDING_PROVIDER_LOCAL
        and not env.get(EMBEDDING_MODEL_PATH_VAR, "").strip()
    ):
        missing.append(EMBEDDING_MODEL_PATH_VAR)
    if not ai_configured(env):
        return missing
    missing.extend(
        name for name in AI_PROVIDER_VARS if not env.get(name, "").strip()
    )
    required_models = {
        stage.model_var
        for stage in AI_STAGE_VARS
        if stage_model_required(env, stage)
    }
    if env.get("GEMINI_API_KEY", "").strip():
        required_models.add("AI_TRANSCRIBE_MODEL")
    # AI_REQUIRED_VARS drives the ORDER so the aggregated error keeps reading
    # the way it did before the provider switch.
    missing.extend(
        name
        for name in AI_REQUIRED_VARS
        if name in required_models and not env.get(name, "").strip()
    )
    for stage in AI_STAGE_VARS:
        if env.get(stage.provider_var, "").strip() != PROVIDER_OPENAI_COMPAT:
            continue
        for name in _openai_compat_missing(env, stage):
            if name not in missing:
                missing.append(name)
    return missing


def resolve_rewrite_api_key(env: Mapping[str, str]) -> str:
    """The key the retrieval *rewrite* stage calls its provider with.

    Thin wrapper over `resolve_stage`, kept because the rule below is what
    ADR 0004/0008 documented and what every reader of this module looks for.

    `AI_SCRIPTURE_REWRITE_API_KEY` when it is set and non-blank, otherwise the
    shared key of the stage's provider (`GEMINI_API_KEY`, or
    `AI_OPENAI_COMPAT_API_KEY` when the stage runs on openai_compat). Blank
    counts as unset on the stage side, so `AI_SCRIPTURE_REWRITE_API_KEY=`
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
    return resolve_stage(env, SCRIPTURE_REWRITE_STAGE_VARS).api_key


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
    embedding_provider = env.get(EMBEDDING_PROVIDER_VAR, "").strip()
    if embedding_provider and embedding_provider not in EMBEDDING_PROVIDERS:
        problems.append(
            f"{EMBEDDING_PROVIDER_VAR}: unknown provider "
            f"{embedding_provider!r}, expected one of "
            f"{', '.join(EMBEDDING_PROVIDERS)}"
        )
    # A path set for the API provider is not a harmless leftover: it reads as
    # "this deployment serves embeddings locally" while every vector still
    # comes from Gemini, which is precisely the gap between .env and reality
    # ADR 0008 exists to close.
    if (
        embedding_provider == EMBEDDING_PROVIDER_GEMINI
        and env.get(EMBEDDING_MODEL_PATH_VAR, "").strip()
    ):
        problems.append(
            f"{EMBEDDING_MODEL_PATH_VAR}: set while {EMBEDDING_PROVIDER_VAR}"
            f"={EMBEDDING_PROVIDER_GEMINI} — the weights would never be "
            f"loaded; remove it or switch the provider to "
            f"{EMBEDDING_PROVIDER_LOCAL}"
        )
    for stage in AI_STAGE_VARS:
        provider = env.get(stage.provider_var, "").strip()
        if provider and provider not in AI_PROVIDERS:
            problems.append(
                f"{stage.provider_var}: unknown provider {provider!r}, "
                f"expected one of {', '.join(AI_PROVIDERS)}"
            )
        # A stage key that only the Gemini path could spend, on a deployment
        # that has no Gemini key: the 2026-08-29 asymmetry, generalised from
        # the rewrite stage to all three. On openai_compat the stage key is
        # the whole story and needs no companion.
        if (
            provider in ("", PROVIDER_GEMINI)
            and env.get(stage.api_key_var, "").strip()
            and not env.get("GEMINI_API_KEY", "").strip()
        ):
            problems.append(
                f"{stage.api_key_var}: set while GEMINI_API_KEY is empty — "
                f"the key pays for one stage of a pipeline whose other "
                f"stages (embeddings, and every stage on gemini) have no key "
                f"at all"
            )
        if provider != PROVIDER_OPENAI_COMPAT:
            continue
        for name in (stage.endpoint_var, OPENAI_COMPAT_ENDPOINT_VAR):
            value = env.get(name, "").strip()
            if value:
                problem = validate_endpoint(name, value)
                if problem and problem not in problems:
                    problems.append(problem)
    if env.get(TRANSCRIBE_PROVIDER_VAR, "").strip():
        problems.append(
            f"{TRANSCRIBE_PROVIDER_VAR}: transcription is Gemini-only until "
            f"it moves to a local speech model — remove the variable "
            f"(AI_TRANSCRIBE_MODEL plus GEMINI_API_KEY configure it)"
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


def _required_reason(env: Mapping[str, str], name: str) -> str:
    """"<NAME> is required" plus the rule that made it required."""
    if name == EMBEDDING_PROVIDER_VAR:
        return (
            f"{name} is required in every deployment (it decides who computes "
            f"the vectors of the index this service reads): one of "
            f"{', '.join(EMBEDDING_PROVIDERS)}"
        )
    if name == EMBEDDING_MODEL_PATH_VAR:
        return (
            f"{name} is required when {EMBEDDING_PROVIDER_VAR}="
            f"{EMBEDDING_PROVIDER_LOCAL}: the directory the model weights are "
            f"mounted at (nothing is downloaded — the image runs offline)"
        )
    if name in AI_PROVIDER_VARS:
        return (
            f"{name} is required when the AI surface is configured "
            f"(GEMINI_API_KEY is set or a provider is named): one of "
            f"{', '.join(AI_PROVIDERS)}"
        )
    if name == OPENAI_COMPAT_ENDPOINT_VAR:
        return (
            f"{name} is required when a stage runs on "
            f"{PROVIDER_OPENAI_COMPAT} (or name the endpoint of that stage "
            f"alone in AI_<STAGE>_ENDPOINT)"
        )
    if name == OPENAI_COMPAT_API_KEY_VAR:
        return (
            f"{name} must be present when a stage runs on "
            f"{PROVIDER_OPENAI_COMPAT} — it may be empty, which states that "
            f"the endpoint needs no Authorization header (or set "
            f"AI_<STAGE>_API_KEY for that stage alone)"
        )
    if name in AI_REQUIRED_VARS:
        on_openai_compat = any(
            stage.model_var == name
            and env.get(stage.provider_var, "").strip() == PROVIDER_OPENAI_COMPAT
            for stage in AI_STAGE_VARS
        )
        if on_openai_compat and not env.get("GEMINI_API_KEY", "").strip():
            return (
                f"{name} is required when its stage runs on "
                f"{PROVIDER_OPENAI_COMPAT} (no default: the model must be "
                f"named explicitly)"
            )
        return (
            f"{name} is required when GEMINI_API_KEY is set (no default: the "
            f"model must be named explicitly)"
        )
    return f"{name} is required"


def _validate(env: Mapping[str, str], problems: list[str]) -> None:
    """Raise one ConfigError listing every problem, or return silently."""
    reasons = [
        _required_reason(env, name) for name in missing_required_vars(env)
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
# Ceiling of the ONE provider call `/api/ai/question` makes. 20 s is the
# value the endpoint ran with while it was Gemini-only, kept as the default;
# it is an operational knob because a self-hosted model generating the same
# answer is slower than a hosted one, and the stage has no other budget.
AI_QUESTION_TIMEOUT_SECONDS = max(
    1.0, _get_float("AI_QUESTION_TIMEOUT_SECONDS", 20.0)
)
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
# Who computes them (ADR 0010): `gemini` (the API — ADR 0002) or `local`
# (bge-m3 in this process). Required always; `_validate` below has already
# refused an unknown value, so `build_embedding_client` can dispatch on it
# without a fallback branch. EMBEDDING_MODEL_PATH is the read-only directory
# the local weights are mounted at, required only for `local` — the model
# IDENTITY stays EMBEDDING_MODEL, which is what the index version carries.
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "").strip()
EMBEDDING_MODEL_PATH = os.getenv("EMBEDDING_MODEL_PATH", "").strip()
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

# The resolved transport of each chat stage (ADR 0009). One object per stage
# instead of a variable per field, so a stage is passed around whole and no
# caller can pair a model with another stage's endpoint. `_validate` below has
# already refused an incomplete one, so these are usable as they stand.
QUESTION_PROVIDER = resolve_stage(os.environ, QUESTION_STAGE_VARS)
SCRIPTURE_REWRITE_PROVIDER = resolve_stage(
    os.environ, SCRIPTURE_REWRITE_STAGE_VARS
)
SCRIPTURE_RERANK_PROVIDER = resolve_stage(os.environ, SCRIPTURE_RERANK_STAGE_VARS)

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
# Ceiling of ONE provider call inside that budget (rewrite, one embedding,
# rerank). 8 s is the value measured against Gemini (ADR 0006) and stays the
# default; it became a knob with ADR 0009 because a stage moved to a local
# model needs a different one, and a per-call ceiling hard-wired in code
# cannot be raised by raising the total budget — `provider_timeout` takes the
# MINIMUM of the two. Operational: the default is the reviewed operating
# point, a malformed value is still a startup error.
AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS = max(
    1.0, _get_float("AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS", 8.0)
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
# configuration, and now lives in question_prompt.py.
AI_CLIENT_HMAC_KEY = os.getenv("AI_CLIENT_HMAC_KEY", "").strip()

# Whose X-Forwarded-For is believed (ClickUp 86cbbq6vz). Three ways to say it,
# all optional and additive; unset everything means "no proxy in front of this
# API", which is how the local machine runs and is a supported deployment, not
# a missing setting.
#
#   TRUSTED_PROXY_HOSTS  container/DNS names, re-resolved at runtime — the
#                        production setting, because docker addresses do not
#                        survive a reboot or resize (see trusted_proxies.py)
#   TRUSTED_PROXY_IPS    literal addresses AND/OR CIDR networks
#   TRUSTED_PROXY_DNS_TTL_SECONDS  how stale a resolved address may get
#
# Malformed entries abort startup naming the variable and the token: this is a
# trust boundary, and skipping an unparsable entry would silently shrink it.
def _get_trusted_ips() -> tuple[
    frozenset[str], tuple[IPv4Network | IPv6Network, ...]
]:
    try:
        return parse_trusted_proxy_ips(
            "TRUSTED_PROXY_IPS", os.getenv("TRUSTED_PROXY_IPS")
        )
    except ConfigError as exc:
        _problems.append(str(exc))
        return frozenset(), ()


def _get_trusted_hosts() -> tuple[str, ...]:
    try:
        return parse_trusted_proxy_hosts(
            "TRUSTED_PROXY_HOSTS", os.getenv("TRUSTED_PROXY_HOSTS")
        )
    except ConfigError as exc:
        _problems.append(str(exc))
        return ()


TRUSTED_PROXY_IPS, TRUSTED_PROXY_NETWORKS = _get_trusted_ips()
TRUSTED_PROXY_HOSTS = _get_trusted_hosts()
# Operational knob: an address change is picked up within one TTL. 30 s is
# short enough that a reboot costs at most half a minute of misattributed
# statistics, and long enough that the DNS lookup is invisible (one call per
# 30 s, never per request). Floored at 1 s — a 0 would resolve on every
# request, on the event loop.
TRUSTED_PROXY_DNS_TTL_SECONDS = max(
    1, _get_int("TRUSTED_PROXY_DNS_TTL_SECONDS", 30)
)

# Fail fast: one aggregated error with everything that is wrong, so a broken
# deployment is fixed in a single pass instead of one variable per restart.
_validate(os.environ, _problems)
