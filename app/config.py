import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from ipaddress import IPv4Network, IPv6Network, ip_address, ip_network
from typing import Literal
from urllib.parse import urlsplit


class ConfigError(RuntimeError):
    """Configuration is unusable; raised once with every problem found.

    A RuntimeError subclass so existing callers that expected the old
    `_require` failure keep catching it.
    """


# ---------------------------------------------------------------------------
# Critical configuration is explicit and fail-fast (ADR 0008, ADR 0019).
#
# Operational limits, TTLs and timeouts retain documented defaults, but a
# malformed supplied value is always an error. Providers, models, endpoints,
# credentials and index identity have no guessed defaults or aliases.
#
# AI_ENABLED is required and is the sole chat/audio switch. When true, every
# stage is complete and self-contained. When false, stage fields are rejected
# as unused. Embeddings stay required independently because they identify and
# query the vector index even when chat/audio is disabled.
# ---------------------------------------------------------------------------

# Required in every deployment, whatever is configured. Blank counts as unset.
ALWAYS_REQUIRED_VARS = (
    "API_KEY",
    "DB_HOST",
    "DB_USER",
    "DB_NAME",
    "AI_ENABLED",
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
# Provider per stage (ADR 0009, ADR 0012, amended by ADR 0019).
#
# Question, rewrite and rerank accept gemini or openai_compat. Transcription
# additionally accepts local. With AI_ENABLED=true every stage names its own
# provider, model and remote API key; openai_compat also names its endpoint
# and, for chat, its reasoning effort. No value is inherited from another
# stage.
#
# Embeddings are configured below as an independent fifth stage.
# ---------------------------------------------------------------------------
PROVIDER_GEMINI = "gemini"
PROVIDER_OPENAI_COMPAT = "openai_compat"
AI_PROVIDERS = (PROVIDER_GEMINI, PROVIDER_OPENAI_COMPAT)

ReasoningEffort = Literal["omit", "none", "low", "medium", "high"]
REASONING_EFFORTS: tuple[ReasoningEffort, ...] = (
    "omit",
    "none",
    "low",
    "medium",
    "high",
)

# These names are intentionally unreadable. Chat reasoning belongs to one
# specific stage; transcription and embeddings use different protocols with
# no reasoning-effort field.
FORBIDDEN_REASONING_VARS = (
    "AI_REASONING_EFFORT",
    "AI_TRANSCRIBE_REASONING_EFFORT",
    "EMBEDDING_REASONING_EFFORT",
)

# These shared variables were removed by ADR 0019. Presence is rejected even
# when the value is blank: otherwise an old deployment could appear to start
# while none of the stage-specific clients reads its key.
REMOVED_AI_VARS = (
    "AI_OPENAI_COMPAT_ENDPOINT",
    "AI_OPENAI_COMPAT_API_KEY",
    "GEMINI_API_KEY",
)

# docker-compose.yml uses this value only to preserve the difference between
# an unexported shell key and an explicitly empty one. It is an implementation
# detail, never a valid credential.
COMPOSE_UNSET_SENTINEL = "__BIBLE_API_UNSET__"

# Transcription has its own value SET (ADR 0012): `openai_compat` here means
# the OpenAI **audio** API (`POST {endpoint}/audio/transcriptions`, multipart)
# and not `/chat/completions`, and `local` — Whisper in this process — is
# meaningless for the three chat stages.
TRANSCRIBE_PROVIDER_GEMINI = PROVIDER_GEMINI
TRANSCRIBE_PROVIDER_LOCAL = "local"
TRANSCRIBE_PROVIDER_OPENAI_COMPAT = PROVIDER_OPENAI_COMPAT
TRANSCRIBE_PROVIDERS = (
    TRANSCRIBE_PROVIDER_GEMINI,
    TRANSCRIBE_PROVIDER_LOCAL,
    TRANSCRIBE_PROVIDER_OPENAI_COMPAT,
)
TRANSCRIBE_PROVIDER_VAR = "AI_TRANSCRIBE_PROVIDER"
TRANSCRIBE_MODEL_PATH_VAR = "AI_TRANSCRIBE_MODEL_PATH"
# CTranslate2 quantisations that make sense on a CPU host, as a reviewed
# subset rather than a free-form string: a typo in a quantisation name is a
# startup failure inside the model load, and this way it is one aggregated
# ConfigError with the valid values instead.
TRANSCRIBE_COMPUTE_TYPES = (
    "int8",
    "int8_float32",
    "int8_bfloat16",
    "int16",
    "bfloat16",
    "float32",
    "auto",
    "default",
)


@dataclass(frozen=True)
class StageVars:
    """Environment variables that configure one AI stage."""

    stage: str
    provider_var: str
    model_var: str
    endpoint_var: str
    api_key_var: str
    reasoning_effort_var: str | None


QUESTION_STAGE_VARS = StageVars(
    "question",
    "AI_QUESTION_PROVIDER",
    "AI_QUESTION_MODEL",
    "AI_QUESTION_ENDPOINT",
    "AI_QUESTION_API_KEY",
    "AI_QUESTION_REASONING_EFFORT",
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
    "AI_SCRIPTURE_REWRITE_REASONING_EFFORT",
)
SCRIPTURE_RERANK_STAGE_VARS = StageVars(
    "scripture_rerank",
    "AI_SCRIPTURE_RERANK_PROVIDER",
    "AI_SCRIPTURE_RERANK_MODEL",
    "AI_SCRIPTURE_RERANK_ENDPOINT",
    "AI_SCRIPTURE_RERANK_API_KEY",
    "AI_SCRIPTURE_RERANK_REASONING_EFFORT",
)
AI_STAGE_VARS = (
    QUESTION_STAGE_VARS,
    SCRIPTURE_REWRITE_STAGE_VARS,
    SCRIPTURE_RERANK_STAGE_VARS,
)
TRANSCRIBE_STAGE_VARS = StageVars(
    "transcribe",
    TRANSCRIBE_PROVIDER_VAR,
    "AI_TRANSCRIBE_MODEL",
    "AI_TRANSCRIBE_ENDPOINT",
    "AI_TRANSCRIBE_API_KEY",
    None,
)
CHAT_PROVIDER_VARS = tuple(stage.provider_var for stage in AI_STAGE_VARS)
# Every provider variable of the AI surface: the three chat stages plus
# transcription. `AI_ENABLED=true` requires all four; provider presence by
# itself never enables the surface.
AI_PROVIDER_VARS = CHAT_PROVIDER_VARS + (TRANSCRIBE_PROVIDER_VAR,)

# ---------------------------------------------------------------------------
# Who computes the embeddings (ClickUp 86cbegg2r/86cbehd6h, ADR 0010/0014)
#
# `EMBEDDING_PROVIDER` is `gemini` (the API of ADR 0002), `local` (bge-m3 in
# this process through sentence-transformers) or `openai_compat` (the same
# bge-m3, on the company's CPU, through `POST {endpoint}/embeddings` — ADR
# 0014, the production provider since 2026-09-05). Unlike the four chat/audio
# providers it is required ALWAYS, with or without any AI key — for the same
# reason `EMBEDDING_MODEL` / `EMBEDDING_DIMENSIONS` are: those three name the
# index this service READS, and the read path runs even in the documented
# no-AI deployment (`fallback_reason=ai_unavailable` is still resolved
# through the loaded corpus). There is no correct value to guess: the
# providers produce different vector spaces, and picking one silently would
# search a 1024-dimension index with a 768-dimension query — or, worse,
# rebuild the index in the other space.
#
# `local` and `openai_compat` name the SAME vectors, which is why the index
# version does not change between them (`c3:BAAI/bge-m3@1024`): one runs the
# weights here, the other asks a server that runs the same model. The
# difference is 2.1 GB of RSS, not a vector space.
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
#
# `EMBEDDING_ENDPOINT` / `EMBEDDING_API_KEY` belong only to embeddings. The
# endpoint is valid only for openai_compat; a local provider uses neither.
# ---------------------------------------------------------------------------

EMBEDDING_PROVIDER_GEMINI = "gemini"
EMBEDDING_PROVIDER_LOCAL = "local"
EMBEDDING_PROVIDER_OPENAI_COMPAT = PROVIDER_OPENAI_COMPAT
EMBEDDING_PROVIDERS = (
    EMBEDDING_PROVIDER_GEMINI,
    EMBEDDING_PROVIDER_LOCAL,
    EMBEDDING_PROVIDER_OPENAI_COMPAT,
)
EMBEDDING_PROVIDER_VAR = "EMBEDDING_PROVIDER"
EMBEDDING_MODEL_PATH_VAR = "EMBEDDING_MODEL_PATH"
# The embedding "stage", in the shape every other one has. `EMBEDDING_MODEL`
# is its model variable because it already is exactly that — the model whose
# vectors this deployment reads and writes.
EMBEDDING_STAGE_VARS = StageVars(
    "embeddings",
    EMBEDDING_PROVIDER_VAR,
    "EMBEDDING_MODEL",
    "EMBEDDING_ENDPOINT",
    "EMBEDDING_API_KEY",
    None,
)


@dataclass(frozen=True)
class StageProvider:
    """The resolved transport of one chat stage.

    `provider` is "" only when nothing named one (AI not configured, or a
    configuration `_validate` has already refused). `endpoint` is empty for
    Gemini, whose URL is a constant of the stage module.
    `reasoning_effort` is set only for an OpenAI-compatible chat stage.
    """

    stage: str
    provider: str
    model: str
    endpoint: str
    api_key: str
    reasoning_effort: ReasoningEffort | None

    @property
    def is_gemini(self) -> bool:
        return self.provider == PROVIDER_GEMINI

    @property
    def is_openai_compat(self) -> bool:
        return self.provider == PROVIDER_OPENAI_COMPAT

    @property
    def is_local(self) -> bool:
        """Only the transcription stage can answer yes (ADR 0012)."""
        return self.provider == TRANSCRIBE_PROVIDER_LOCAL


def env_var_present(env: Mapping[str, str], name: str) -> bool:
    """Whether an environment variable was explicitly supplied.

    Compose cannot otherwise preserve unset versus explicitly empty values.
    Empty is therefore present, while its private transport sentinel is not.
    """
    return name in env and env[name] != COMPOSE_UNSET_SENTINEL


def parse_bool(name: str, raw: str | None) -> bool:
    """Parse the only accepted explicit boolean spellings."""
    if raw == "true":
        return True
    if raw == "false":
        return False
    raise ConfigError(f"{name}: expected exactly 'true' or 'false'")


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
    """Whether the AI chat/audio surface is explicitly enabled."""
    return env.get("AI_ENABLED") == "true"


def resolve_stage(env: Mapping[str, str], stage: StageVars) -> StageProvider:
    """Resolve one stage without aliases, inheritance or shared credentials."""
    provider = env.get(stage.provider_var, "").strip()
    model = env.get(stage.model_var, "").strip()
    api_key = (
        env.get(stage.api_key_var, "").strip()
        if env_var_present(env, stage.api_key_var)
        else ""
    )
    reasoning_effort = (
        env.get(stage.reasoning_effort_var, "").strip()
        if stage.reasoning_effort_var is not None
        else None
    )
    if provider == TRANSCRIBE_PROVIDER_LOCAL:
        return StageProvider(stage.stage, provider, model, "", "", None)
    if provider == PROVIDER_OPENAI_COMPAT:
        return StageProvider(
            stage.stage,
            provider,
            model,
            env.get(stage.endpoint_var, "").strip(),
            api_key,
            reasoning_effort,
        )
    return StageProvider(stage.stage, provider, model, "", api_key, None)


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
            f"{name}: expected an absolute http(s) URL of the provider API "
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


def _remote_missing(env: Mapping[str, str], stage: StageVars) -> list[str]:
    """Variables a remote stage must state explicitly."""
    missing = []
    provider = env.get(stage.provider_var, "").strip()
    if provider == PROVIDER_OPENAI_COMPAT:
        if not env.get(stage.endpoint_var, "").strip():
            missing.append(stage.endpoint_var)
        if stage.reasoning_effort_var is not None and not env.get(
            stage.reasoning_effort_var, ""
        ).strip():
            missing.append(stage.reasoning_effort_var)
    if provider == PROVIDER_GEMINI:
        if not env_var_present(env, stage.api_key_var) or not env.get(
            stage.api_key_var, ""
        ).strip():
            missing.append(stage.api_key_var)
    elif provider == PROVIDER_OPENAI_COMPAT and not env_var_present(
        env, stage.api_key_var
    ):
        missing.append(stage.api_key_var)
    return missing


def missing_required_vars(env: Mapping[str, str]) -> list[str]:
    """Names of variables that must be set for this environment, but are not.

    Pure function over an environment mapping, so it is testable without
    reimporting the module. `ALWAYS_REQUIRED_VARS` must be non-blank,
    `PRESENCE_REQUIRED_VARS` must merely exist. Embeddings are always complete.
    With `AI_ENABLED=true`, all four chat/audio stages and the limiter HMAC are
    complete as well.
    """
    missing = [
        name for name in ALWAYS_REQUIRED_VARS if not env.get(name, "").strip()
    ]
    missing.extend(name for name in PRESENCE_REQUIRED_VARS if name not in env)
    embedding_provider = env.get(EMBEDDING_PROVIDER_VAR, "").strip()
    if (
        embedding_provider == EMBEDDING_PROVIDER_LOCAL
        and not env.get(EMBEDDING_MODEL_PATH_VAR, "").strip()
    ):
        missing.append(EMBEDDING_MODEL_PATH_VAR)
    if embedding_provider in (
        EMBEDDING_PROVIDER_GEMINI,
        EMBEDDING_PROVIDER_OPENAI_COMPAT,
    ):
        missing.extend(_remote_missing(env, EMBEDDING_STAGE_VARS))
    if not ai_configured(env):
        return missing
    if not env.get("AI_CLIENT_HMAC_KEY", "").strip():
        missing.append("AI_CLIENT_HMAC_KEY")
    missing.extend(
        name for name in AI_PROVIDER_VARS if not env.get(name, "").strip()
    )
    missing.extend(
        name for name in AI_REQUIRED_VARS if not env.get(name, "").strip()
    )
    transcribe_provider = env.get(TRANSCRIBE_PROVIDER_VAR, "").strip()
    if (
        transcribe_provider == TRANSCRIBE_PROVIDER_LOCAL
        and not env.get(TRANSCRIBE_MODEL_PATH_VAR, "").strip()
    ):
        missing.append(TRANSCRIBE_MODEL_PATH_VAR)
    for stage in (*AI_STAGE_VARS, TRANSCRIBE_STAGE_VARS):
        provider = env.get(stage.provider_var, "").strip()
        if provider in (PROVIDER_GEMINI, PROVIDER_OPENAI_COMPAT):
            missing.extend(
                name for name in _remote_missing(env, stage) if name not in missing
            )
    return missing


def invalid_required_values(env: Mapping[str, str]) -> list[str]:
    """Problems with values that ARE set but cannot be used, as messages.

    Non-numeric values are not reported here — `parse_int`/`parse_float`
    already name them while reading. This is the range check the type alone
    does not give: `EMBEDDING_DIMENSIONS=0` parses fine and then produces the
    index version `...@0` and an `outputDimensionality: 0` request.
    """
    problems = []
    raw_ai_enabled = env.get("AI_ENABLED")
    if raw_ai_enabled and raw_ai_enabled not in ("true", "false"):
        problems.append("AI_ENABLED: expected exactly 'true' or 'false'")
    for name in REMOVED_AI_VARS:
        if name in env:
            problems.append(
                f"{name}: removed; configure every stage with its own "
                "PROVIDER, MODEL, ENDPOINT and API_KEY variables"
            )
    for name in FORBIDDEN_REASONING_VARS:
        if name in env:
            problems.append(
                f"{name}: unsupported; reasoning effort exists only as an "
                "explicit per-stage variable for question, scripture rewrite "
                "and scripture rerank"
            )
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
    if (
        embedding_provider
        in (EMBEDDING_PROVIDER_GEMINI, EMBEDDING_PROVIDER_OPENAI_COMPAT)
        and env_var_present(env, EMBEDDING_MODEL_PATH_VAR)
    ):
        problems.append(
            f"{EMBEDDING_MODEL_PATH_VAR}: set while {EMBEDDING_PROVIDER_VAR}"
            f"={embedding_provider} — the weights would never be "
            f"loaded; remove it or switch the provider to "
            f"{EMBEDDING_PROVIDER_LOCAL}"
        )
    if embedding_provider != EMBEDDING_PROVIDER_OPENAI_COMPAT and env_var_present(
        env, EMBEDDING_STAGE_VARS.endpoint_var
    ):
        problems.append(
            f"{EMBEDDING_STAGE_VARS.endpoint_var}: set while "
            f"{EMBEDDING_PROVIDER_VAR}={embedding_provider or '<unset>'} — "
            "only openai_compat uses an endpoint"
        )
    if embedding_provider == EMBEDDING_PROVIDER_LOCAL and env_var_present(
        env, EMBEDDING_STAGE_VARS.api_key_var
    ):
        problems.append(
            f"{EMBEDDING_STAGE_VARS.api_key_var}: set while "
            f"{EMBEDDING_PROVIDER_VAR}=local — a local model uses no API key"
        )
    if embedding_provider == EMBEDDING_PROVIDER_OPENAI_COMPAT:
        name = EMBEDDING_STAGE_VARS.endpoint_var
        value = env.get(name, "").strip()
        if value:
            problem = validate_endpoint(name, value)
            if problem:
                problems.append(problem)

    stage_config_names = {"AI_CLIENT_HMAC_KEY", TRANSCRIBE_MODEL_PATH_VAR}
    for stage in (*AI_STAGE_VARS, TRANSCRIBE_STAGE_VARS):
        stage_config_names.update(
            (
                stage.provider_var,
                stage.model_var,
                stage.endpoint_var,
                stage.api_key_var,
            )
        )
        if stage.reasoning_effort_var is not None:
            stage_config_names.add(stage.reasoning_effort_var)
    compute_type = env.get("AI_TRANSCRIBE_COMPUTE_TYPE", "").strip()
    if compute_type and compute_type not in TRANSCRIBE_COMPUTE_TYPES:
        problems.append(
            f"AI_TRANSCRIBE_COMPUTE_TYPE: unknown quantisation "
            f"{compute_type!r}, expected one of "
            f"{', '.join(TRANSCRIBE_COMPUTE_TYPES)}"
        )
    if raw_ai_enabled == "false":
        for name in sorted(stage_config_names):
            if env_var_present(env, name):
                problems.append(f"{name}: set while AI_ENABLED=false — remove it")
        return problems

    for stage in AI_STAGE_VARS:
        provider = env.get(stage.provider_var, "").strip()
        if provider and provider not in AI_PROVIDERS:
            problems.append(
                f"{stage.provider_var}: unknown provider {provider!r}, "
                f"expected one of {', '.join(AI_PROVIDERS)}"
            )
        if provider == PROVIDER_OPENAI_COMPAT:
            value = env.get(stage.endpoint_var, "").strip()
            if value:
                problem = validate_endpoint(stage.endpoint_var, value)
                if problem:
                    problems.append(problem)
            if stage.reasoning_effort_var is not None:
                reasoning_effort = env.get(stage.reasoning_effort_var, "")
                if reasoning_effort and reasoning_effort not in REASONING_EFFORTS:
                    problems.append(
                        f"{stage.reasoning_effort_var}: unknown reasoning effort "
                        f"{reasoning_effort!r}, expected one of "
                        f"{', '.join(REASONING_EFFORTS)}"
                    )
        elif env_var_present(env, stage.endpoint_var):
            problems.append(
                f"{stage.endpoint_var}: set while {stage.provider_var}="
                f"{provider or '<unset>'} — only openai_compat uses an endpoint"
            )
        if provider != PROVIDER_OPENAI_COMPAT and (
            stage.reasoning_effort_var is not None
            and env_var_present(env, stage.reasoning_effort_var)
        ):
            problems.append(
                f"{stage.reasoning_effort_var}: set while {stage.provider_var}="
                f"{provider or '<unset>'} — only openai_compat chat uses "
                "reasoning effort"
            )
    transcribe_provider = env.get(TRANSCRIBE_PROVIDER_VAR, "").strip()
    if transcribe_provider and transcribe_provider not in TRANSCRIBE_PROVIDERS:
        problems.append(
            f"{TRANSCRIBE_PROVIDER_VAR}: unknown provider "
            f"{transcribe_provider!r}, expected one of "
            f"{', '.join(TRANSCRIBE_PROVIDERS)}"
        )
    if (
        transcribe_provider
        in (TRANSCRIBE_PROVIDER_GEMINI, TRANSCRIBE_PROVIDER_OPENAI_COMPAT)
        and env_var_present(env, TRANSCRIBE_MODEL_PATH_VAR)
    ):
        problems.append(
            f"{TRANSCRIBE_MODEL_PATH_VAR}: set while {TRANSCRIBE_PROVIDER_VAR}"
            f"={transcribe_provider} — the weights would never be loaded; "
            f"remove it or switch the provider to "
            f"{TRANSCRIBE_PROVIDER_LOCAL}"
        )
    if transcribe_provider == TRANSCRIBE_PROVIDER_LOCAL and env_var_present(
        env, TRANSCRIBE_STAGE_VARS.api_key_var
    ):
        problems.append(
            f"{TRANSCRIBE_STAGE_VARS.api_key_var}: set while "
            f"{TRANSCRIBE_PROVIDER_VAR}=local — a local model uses no API key"
        )
    if transcribe_provider == TRANSCRIBE_PROVIDER_OPENAI_COMPAT:
        name = TRANSCRIBE_STAGE_VARS.endpoint_var
        value = env.get(name, "").strip()
        if value:
            problem = validate_endpoint(name, value)
            if problem:
                problems.append(problem)
    elif env_var_present(env, TRANSCRIBE_STAGE_VARS.endpoint_var):
        problems.append(
            f"{TRANSCRIBE_STAGE_VARS.endpoint_var}: set while "
            f"{TRANSCRIBE_PROVIDER_VAR}={transcribe_provider or '<unset>'} — "
            "only openai_compat uses an endpoint"
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
    if name == "AI_ENABLED":
        return (
            "AI_ENABLED is required in every deployment: exactly true or false"
        )
    if name == "AI_CLIENT_HMAC_KEY":
        return (
            "AI_CLIENT_HMAC_KEY is required when AI_ENABLED=true: the "
            "per-client limiter must not start in a degraded state"
        )
    if name == EMBEDDING_MODEL_PATH_VAR:
        return (
            f"{name} is required when {EMBEDDING_PROVIDER_VAR}="
            f"{EMBEDDING_PROVIDER_LOCAL}: the directory the model weights are "
            f"mounted at (nothing is downloaded — the image runs offline)"
        )
    if name == TRANSCRIBE_MODEL_PATH_VAR:
        return (
            f"{name} is required when {TRANSCRIBE_PROVIDER_VAR}="
            f"{TRANSCRIBE_PROVIDER_LOCAL}: the directory the converted "
            f"Whisper weights are mounted at (nothing is downloaded — the "
            f"image runs offline)"
        )
    if name in AI_PROVIDER_VARS:
        values = (
            TRANSCRIBE_PROVIDERS
            if name == TRANSCRIBE_PROVIDER_VAR
            else AI_PROVIDERS
        )
        return (
            f"{name} is required when AI_ENABLED=true: one of "
            f"{', '.join(values)}"
        )
    if name in AI_REQUIRED_VARS:
        return (
            f"{name} is required when AI_ENABLED=true (no default: the model "
            "must be named explicitly)"
        )
    for stage in (*AI_STAGE_VARS, TRANSCRIBE_STAGE_VARS, EMBEDDING_STAGE_VARS):
        if name == stage.endpoint_var:
            return (
                f"{name} is required when {stage.provider_var}="
                f"{PROVIDER_OPENAI_COMPAT}"
            )
        if name == stage.api_key_var:
            provider = env.get(stage.provider_var, "").strip()
            if provider == PROVIDER_GEMINI:
                return f"{name} must be non-empty when {stage.provider_var}=gemini"
            return (
                f"{name} must be present for remote provider "
                f"{provider or '<unset>'}; it may "
                "be empty to state that no Authorization header is required"
            )
        if name == stage.reasoning_effort_var:
            return (
                f"{name} is required when {stage.provider_var}="
                f"{PROVIDER_OPENAI_COMPAT}: one of "
                f"{', '.join(REASONING_EFFORTS)}; use omit to explicitly not "
                "send the field"
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

# Explicit switch for the chat/audio surface. False keeps the read-only API and
# the independently configured embedding index available, while stage config
# is rejected as unused. True requires four complete stage declarations.
AI_ENABLED = os.getenv("AI_ENABLED") == "true"
# Models of the two chat-shaped endpoints, named after the method each one
# serves: POST /api/ai/question and POST /api/ai/transcribe.
AI_QUESTION_MODEL = os.getenv("AI_QUESTION_MODEL", "")
AI_TRANSCRIBE_MODEL = os.getenv("AI_TRANSCRIBE_MODEL", "")

# The read-only directory the converted (CTranslate2) Whisper weights are
# mounted at, required for, and only for, `AI_TRANSCRIBE_PROVIDER=local`. The
# model IDENTITY stays AI_TRANSCRIBE_MODEL, exactly as EMBEDDING_MODEL /
# EMBEDDING_MODEL_PATH split the two: what the model is belongs in a report,
# where its bytes happen to live does not.
AI_TRANSCRIBE_MODEL_PATH = os.getenv("AI_TRANSCRIBE_MODEL_PATH", "").strip()
# Operational knobs of the LOCAL transcriber (the remote providers have their
# own budgets). Their defaults ARE the reviewed operating point (ADR 0012,
# measured in AI-Evaluation/evaluation/README.md), not a stand-in for a setting somebody
# forgot; a malformed value is still a startup error.
#
# COMPUTE_TYPE: CTranslate2 quantisation, applied while loading the float16
# weights the volume holds. `int8` is what the measurement ran on (peak RSS
# 849 MB for `small`, 2.1 GB for `medium`); the accepted values are above.
AI_TRANSCRIBE_COMPUTE_TYPE = (
    os.getenv("AI_TRANSCRIBE_COMPUTE_TYPE", "").strip() or "int8"
)
# THREADS: CPU threads one transcription may use. 0 is CTranslate2's own "all
# cores", which is the right default on a VM dedicated to this service and
# the wrong one only when the box has other work — hence the knob.
AI_TRANSCRIBE_THREADS = max(0, _get_int("AI_TRANSCRIBE_THREADS", 0))
# BEAM_SIZE: 1 (greedy) or 5. Measured on 15 ru/uk/en excerpts (ClickUp
# 86cbegg3m): beam 5 buys 0.7 WER points on `small` (0.095 -> 0.088) and 0.5
# on `medium` (0.066 -> 0.061) for ~1.5x the time and ~8% more memory. It does
# not pay for itself on either model, so greedy is the default.
AI_TRANSCRIBE_BEAM_SIZE = max(1, _get_int("AI_TRANSCRIBE_BEAM_SIZE", 1))
# The ceiling of the provider call. On `gemini` it is the httpx timeout the
# endpoint always had (60 s, per phase, unchanged) and on `openai_compat` it
# is the same ceiling, carved across httpx's four phases. On `local` there is no
# call to time out — CTranslate2 offers no cancellation and anyio's thread
# pool waits for its thread even when the awaiting task is cancelled, so a
# `wait_for` here would free nobody. It therefore bounds what this deployment
# ACCEPTS instead (see AI_TRANSCRIBE_MAX_AUDIO_SECONDS) and a run that
# outlives it is logged as too slow for this machine.
AI_TRANSCRIBE_TIMEOUT_SECONDS = max(
    1.0, _get_float("AI_TRANSCRIBE_TIMEOUT_SECONDS", 60.0)
)
# The longest recording the LOCAL provider will start work on. The 14 MiB
# upload cap is a Gemini request-size limit and lets ~30 minutes of 64 kbps
# AAC through; locally that is 30 minutes of CPU nobody can interrupt, taken
# from every other request on the box. 600 s is far above anything the app
# records (a spoken reply in a prayer) and far below that worst case.
AI_TRANSCRIBE_MAX_AUDIO_SECONDS = max(
    1.0, _get_float("AI_TRANSCRIBE_MAX_AUDIO_SECONDS", 600.0)
)
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
# architect/adr/0002-embedding-model-and-vector-store.md). Model and
# dimensions are a pair:
# together they version the stored vectors, so neither may be guessed — and
# both are required even without a key, because the read path (including the
# no-AI safe-pool answer) has to name the index it loads. The 0 default is
# unreachable: _validate() rejects a missing or non-positive value.
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "")
EMBEDDING_DIMENSIONS = _get_int("EMBEDDING_DIMENSIONS", 0)
# Who computes them (ADR 0010/0014): `gemini` (the API — ADR 0002), `local`
# (bge-m3 in this process) or `openai_compat` (the same bge-m3 on the
# company's server, over `POST {endpoint}/embeddings`). Required always;
# `_validate` below has already refused an unknown value, so
# `build_embedding_client` can dispatch on it without a fallback branch.
# EMBEDDING_MODEL_PATH is the read-only directory the local weights are
# mounted at, required only for `local` — the model IDENTITY stays
# EMBEDDING_MODEL, which is what the index version carries.
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "").strip()
EMBEDDING_MODEL_PATH = os.getenv("EMBEDDING_MODEL_PATH", "").strip()
# The resolved transport of the embedding stage. Every remote provider reads
# EMBEDDING_API_KEY directly; openai_compat also reads EMBEDDING_ENDPOINT.
EMBEDDING_STAGE = resolve_stage(os.environ, EMBEDDING_STAGE_VARS)
# Model for LLM query reformulation in the retrieval pipeline (see
# architect/adr/0004-retrieval-pipeline.md). Deliberately NOT following
# AI_QUESTION_MODEL: the benchmark passes the retrieval thresholds only with
# gemini-3.7-flash rewrites (gemini-3.5-flash-lite fails recall@10 and MRR).
# The value is pinned by that benchmark but must be spelled out in the
# environment — a default here hid a broken model behind a working config.
AI_SCRIPTURE_REWRITE_MODEL = os.getenv("AI_SCRIPTURE_REWRITE_MODEL", "")
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
# Transcription resolves through the same function (ADR 0012/0019) and reads
# only `AI_TRANSCRIBE_ENDPOINT` / `AI_TRANSCRIBE_API_KEY` when remote.
# `.provider` is one of `gemini` | `local` | `openai_compat`.
TRANSCRIBE_PROVIDER = resolve_stage(os.environ, TRANSCRIBE_STAGE_VARS)
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
