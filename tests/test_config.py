"""Tests for app/config.py — the no-silent-defaults rules.

`config` runs its checks at import time, so most of the logic lives in pure
functions (`parse_int`, `parse_float`, `missing_required_vars`, `_validate`)
that are tested directly. The import-time behaviour itself is covered by a
couple of `importlib.reload` tests with a patched environment.
"""

import importlib
import os

import pytest

os.environ.setdefault("API_KEY", "test-api-key")

import config  # noqa: E402


# Names spelled out on purpose: asserting against config.ALWAYS_REQUIRED_VARS
# / AI_REQUIRED_VARS would be self-referential and would keep passing if a
# variable were dropped from the tuple and given a default again.
ALWAYS_REQUIRED = [
    "API_KEY",
    "DB_HOST",
    "DB_USER",
    "DB_NAME",
    "EMBEDDING_MODEL",
    "EMBEDDING_DIMENSIONS",
    "EMBEDDING_PROVIDER",
]
PRESENCE_REQUIRED = ["DB_PASSWORD"]
AI_REQUIRED = [
    "AI_QUESTION_MODEL",
    "AI_TRANSCRIBE_MODEL",
    "AI_SCRIPTURE_REWRITE_MODEL",
    "AI_SCRIPTURE_RERANK_MODEL",
]
# Which transport serves each chat stage (ADR 0009). Spelled out for the same
# reason as the lists above: asserting against config.AI_PROVIDER_VARS would
# keep passing if a stage silently stopped requiring one.
PROVIDER_VARS = [
    "AI_QUESTION_PROVIDER",
    "AI_SCRIPTURE_REWRITE_PROVIDER",
    "AI_SCRIPTURE_RERANK_PROVIDER",
]
# Transcription got its own provider on 2026-09-05 (ADR 0012) with a third
# value the chat stages do not have (`local`), which is why it is a separate
# name here and not a fourth element of the list above.
TRANSCRIBE_PROVIDER_VAR = "AI_TRANSCRIBE_PROVIDER"
ALL_PROVIDER_VARS = PROVIDER_VARS + [TRANSCRIBE_PROVIDER_VAR]
ALL_GEMINI = {name: "gemini" for name in ALL_PROVIDER_VARS}
# Chat only: `openai_compat` means `/chat/completions` for these three and the
# audio API for transcription, so a test that switches "everything" must say
# which everything it means.
ALL_OPENAI_COMPAT = {name: "openai_compat" for name in PROVIDER_VARS}

# The minimum a deployment without AI must set.
BASE_ENV = {
    "API_KEY": "k",
    "DB_HOST": "cep-mysql",
    "DB_USER": "cep",
    "DB_PASSWORD": "secret",
    "DB_NAME": "cep_public",
    "EMBEDDING_MODEL": "gemini-embedding-001",
    "EMBEDDING_DIMENSIONS": "768",
    "EMBEDDING_PROVIDER": "gemini",
}

# The same deployment with the vectors computed in-process (ADR 0010). The
# model id still names the index version; the path says where the weights
# are mounted on this machine.
LOCAL_EMBEDDING_ENV = dict(
    BASE_ENV,
    EMBEDDING_PROVIDER="local",
    EMBEDDING_MODEL="BAAI/bge-m3",
    EMBEDDING_DIMENSIONS="1024",
    EMBEDDING_MODEL_PATH="/models/bge-m3",
)

AI_ENV = dict(
    BASE_ENV,
    **ALL_GEMINI,
    GEMINI_API_KEY="gemini-key",
    AI_QUESTION_MODEL="gemini-3.5-flash-lite",
    AI_TRANSCRIBE_MODEL="gemini-3.5-flash-lite",
    AI_SCRIPTURE_REWRITE_MODEL="gemini-3.7-flash",
    AI_SCRIPTURE_RERANK_MODEL="gemini-3.5-flash-lite",
)

# The same deployment with every CHAT stage on a local OpenAI-compatible
# endpoint while transcription is still Gemini's — the mixed state the switch
# has to support, and the one every deployment was in between ADR 0009 and
# ADR 0012.
OPENAI_COMPAT_ENV = dict(
    AI_ENV,
    **ALL_OPENAI_COMPAT,
    AI_OPENAI_COMPAT_ENDPOINT="https://llm.example:8443/v1",
    AI_OPENAI_COMPAT_API_KEY="local-key",
    AI_QUESTION_MODEL="qwen3-30b",
    AI_SCRIPTURE_REWRITE_MODEL="qwen3-30b",
    AI_SCRIPTURE_RERANK_MODEL="qwen3-30b",
)

# Every provider named, none of them keyed: the "deploy without AI" state
# that ADR 0008 keeps supported, and the base of the two below.
NAMED_PROVIDERS_ENV = dict(BASE_ENV, **ALL_GEMINI)

# Transcription in this process (ADR 0012): no key anywhere, the model
# identity and the weights path both named.
LOCAL_TRANSCRIBE_ENV = dict(
    NAMED_PROVIDERS_ENV,
    AI_TRANSCRIBE_PROVIDER="local",
    AI_TRANSCRIBE_MODEL="small",
    AI_TRANSCRIBE_MODEL_PATH="/models/whisper/small",
)

# Transcription on the company's audio server — the production provider.
REMOTE_TRANSCRIBE_ENV = dict(
    NAMED_PROVIDERS_ENV,
    AI_TRANSCRIBE_PROVIDER="openai_compat",
    AI_TRANSCRIBE_MODEL="Systran/faster-whisper-large-v3",
    AI_TRANSCRIBE_ENDPOINT="https://whisper.example:8000/v1",
    AI_TRANSCRIBE_API_KEY="audio-key",
)


# --- numeric parsing -------------------------------------------------------


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_parse_int_unset_falls_back_to_default(raw):
    assert config.parse_int("SOME_LIMIT", raw, 7) == 7


def test_parse_int_reads_value():
    assert config.parse_int("SOME_LIMIT", " 42 ", 7) == 42


def test_parse_int_rejects_garbage_naming_the_variable():
    with pytest.raises(config.ConfigError) as exc:
        config.parse_int("AI_SCRIPTURE_INDEX_CACHE_SECONDS", "3600s", 3600)
    assert "AI_SCRIPTURE_INDEX_CACHE_SECONDS" in str(exc.value)
    assert "3600s" in str(exc.value)


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_parse_float_unset_falls_back_to_default(raw):
    assert config.parse_float("SOME_TIMEOUT", raw, 15.0) == 15.0


def test_parse_float_reads_value():
    assert config.parse_float("SOME_TIMEOUT", "2.5", 15.0) == 2.5


def test_parse_float_rejects_garbage_naming_the_variable():
    with pytest.raises(config.ConfigError) as exc:
        config.parse_float("AI_SCRIPTURE_TIMEOUT_SECONDS", "fast", 15.0)
    assert "AI_SCRIPTURE_TIMEOUT_SECONDS" in str(exc.value)


def test_config_error_is_runtime_error():
    assert issubclass(config.ConfigError, RuntimeError)


# --- required variables ----------------------------------------------------


@pytest.mark.parametrize("name", ALWAYS_REQUIRED)
def test_every_always_required_var_is_reported_when_blank(name):
    env = dict(BASE_ENV)
    env[name] = "   "
    assert config.missing_required_vars(env) == [name]


@pytest.mark.parametrize("name", ALWAYS_REQUIRED)
def test_every_always_required_var_is_reported_when_absent(name):
    env = dict(BASE_ENV)
    del env[name]
    assert config.missing_required_vars(env) == [name]


def test_empty_environment_reports_every_always_required_var():
    assert config.missing_required_vars({}) == (
        ALWAYS_REQUIRED + PRESENCE_REQUIRED
    )


def test_db_password_may_be_empty_but_must_be_present():
    # An empty MySQL password is a legitimate setup; a missing variable is the
    # silence the rule forbids.
    assert config.missing_required_vars(dict(BASE_ENV, DB_PASSWORD="")) == []
    env = dict(BASE_ENV)
    del env["DB_PASSWORD"]
    assert config.missing_required_vars(env) == ["DB_PASSWORD"]


def test_without_gemini_key_only_the_provider_models_are_optional():
    assert config.missing_required_vars(BASE_ENV) == []


@pytest.mark.parametrize("name", AI_REQUIRED)
def test_every_provider_model_is_required_with_a_key(name):
    env = dict(AI_ENV)
    del env[name]
    assert config.missing_required_vars(env) == [name]


def test_gemini_key_makes_the_providers_and_every_model_required():
    """An `.env` that predates ADR 0009: a key, and nothing saying who serves
    which stage. It does not start, and it names the four variables."""
    env = dict(BASE_ENV, GEMINI_API_KEY="gemini-key")
    assert config.missing_required_vars(env) == ALL_PROVIDER_VARS + AI_REQUIRED


def test_embedding_pair_is_required_even_without_a_key():
    # Regression guard: the pair names the index the read path loads, so the
    # documented no-AI safe-pool answer needs it too.
    env = dict(BASE_ENV)
    del env["EMBEDDING_MODEL"]
    del env["EMBEDDING_DIMENSIONS"]
    assert "GEMINI_API_KEY" not in env
    assert config.missing_required_vars(env) == [
        "EMBEDDING_MODEL", "EMBEDDING_DIMENSIONS",
    ]


def test_fully_configured_ai_environment_has_no_missing_vars():
    assert config.missing_required_vars(AI_ENV) == []


def test_blank_model_var_counts_as_missing():
    env = dict(AI_ENV, AI_SCRIPTURE_REWRITE_MODEL="  ")
    assert config.missing_required_vars(env) == ["AI_SCRIPTURE_REWRITE_MODEL"]


# --- value ranges ----------------------------------------------------------


@pytest.mark.parametrize("raw", ["0", "-1", "-768"])
def test_non_positive_embedding_dimensions_are_rejected(raw):
    problems = config.invalid_required_values(dict(BASE_ENV, EMBEDDING_DIMENSIONS=raw))
    assert len(problems) == 1
    assert "EMBEDDING_DIMENSIONS" in problems[0]


def test_positive_embedding_dimensions_pass():
    assert config.invalid_required_values(BASE_ENV) == []


def test_non_numeric_dimensions_are_left_to_the_parser():
    # parse_int already names it; reporting it twice would be noise.
    assert config.invalid_required_values(
        dict(BASE_ENV, EMBEDDING_DIMENSIONS="many")
    ) == []


# --- who computes the embeddings (ADR 0010) --------------------------------


def test_embedding_provider_is_required_even_without_a_key():
    """Same rule as the model/dimensions pair: it describes the index this
    deployment reads, and the read path runs with no AI configured at all."""
    env = dict(BASE_ENV)
    del env["EMBEDDING_PROVIDER"]
    assert "GEMINI_API_KEY" not in env
    assert config.missing_required_vars(env) == ["EMBEDDING_PROVIDER"]


def test_unknown_embedding_provider_is_rejected_by_name():
    problems = config.invalid_required_values(
        dict(BASE_ENV, EMBEDDING_PROVIDER="bge")
    )
    assert len(problems) == 1
    assert "EMBEDDING_PROVIDER" in problems[0]
    assert "gemini" in problems[0] and "local" in problems[0]


def test_local_provider_requires_the_weights_path():
    env = dict(LOCAL_EMBEDDING_ENV)
    del env["EMBEDDING_MODEL_PATH"]
    assert config.missing_required_vars(env) == ["EMBEDDING_MODEL_PATH"]


def test_local_embedding_environment_is_complete():
    assert config.missing_required_vars(LOCAL_EMBEDDING_ENV) == []
    assert config.invalid_required_values(LOCAL_EMBEDDING_ENV) == []


def test_gemini_provider_does_not_want_a_weights_path():
    """A leftover path with the API provider says one thing while the
    deployment does another — the gap ADR 0008 exists to close."""
    problems = config.invalid_required_values(
        dict(BASE_ENV, EMBEDDING_MODEL_PATH="/models/bge-m3")
    )
    assert len(problems) == 1
    assert "EMBEDDING_MODEL_PATH" in problems[0]


def test_gemini_provider_needs_no_weights_path():
    assert config.missing_required_vars(BASE_ENV) == []


def test_local_embeddings_need_no_gemini_key_at_all():
    """The whole point of the migration: an index built and searched with no
    Google credentials anywhere in the environment."""
    env = dict(LOCAL_EMBEDDING_ENV)
    assert "GEMINI_API_KEY" not in env
    config._validate(env, [])


# --- rewrite API key -------------------------------------------------------


def test_rewrite_key_is_used_when_set():
    env = dict(AI_ENV, AI_SCRIPTURE_REWRITE_API_KEY="paid-key")
    assert config.resolve_rewrite_api_key(env) == "paid-key"
    # ...and it does not leak into the shared key.
    assert env["GEMINI_API_KEY"] == "gemini-key"


def test_rewrite_key_is_stripped():
    env = dict(AI_ENV, AI_SCRIPTURE_REWRITE_API_KEY="  paid-key\n")
    assert config.resolve_rewrite_api_key(env) == "paid-key"


@pytest.mark.parametrize("value", [None, "", "   "])
def test_unset_or_blank_rewrite_key_falls_back_to_the_shared_key(value):
    # The documented operational default: one key pays for every stage.
    env = dict(AI_ENV)
    if value is not None:
        env["AI_SCRIPTURE_REWRITE_API_KEY"] = value
    assert config.resolve_rewrite_api_key(env) == "gemini-key"


def test_without_any_key_the_rewrite_stage_has_none():
    assert config.resolve_rewrite_api_key(BASE_ENV) == ""


def test_a_whitespace_only_shared_key_resolves_to_nothing():
    # Symmetry with the validation, which also treats blank as unset: a raw
    # "   " would reach the x-goog-api-key header instead of raising the
    # rewriter's "not configured" error.
    env = dict(BASE_ENV, GEMINI_API_KEY="   ")
    assert config.resolve_rewrite_api_key(env) == ""


def test_the_shared_key_is_stripped_too():
    env = dict(BASE_ENV, GEMINI_API_KEY=" gemini-key\n")
    assert config.resolve_rewrite_api_key(env) == "gemini-key"


def test_rewrite_key_without_a_shared_key_is_a_configuration_error():
    # Paying for the rewrite of a pipeline whose embeddings and rerank have
    # no key at all is never what the deployer meant.
    env = dict(BASE_ENV, AI_SCRIPTURE_REWRITE_API_KEY="paid-key")
    problems = config.invalid_required_values(env)
    assert len(problems) == 1
    assert "AI_SCRIPTURE_REWRITE_API_KEY" in problems[0]
    assert "GEMINI_API_KEY" in problems[0]


@pytest.mark.parametrize("value", ["", "   "])
def test_blank_rewrite_key_without_a_shared_key_is_not_an_error(value):
    # Blank means unset, and "no AI configured" stays a supported deployment.
    env = dict(BASE_ENV, AI_SCRIPTURE_REWRITE_API_KEY=value)
    assert config.invalid_required_values(env) == []


def test_rewrite_key_problem_joins_the_aggregated_error():
    env = dict(BASE_ENV, AI_SCRIPTURE_REWRITE_API_KEY="paid-key")
    del env["DB_NAME"]
    with pytest.raises(config.ConfigError) as exc:
        config._validate(env, [])
    message = str(exc.value)
    assert "AI_SCRIPTURE_REWRITE_API_KEY" in message
    assert "DB_NAME" in message
    assert "2 problems" in message


# --- provider per stage (ADR 0009) -----------------------------------------


def test_no_key_and_no_provider_is_ai_switched_off():
    assert not config.ai_configured(BASE_ENV)
    assert config.missing_required_vars(BASE_ENV) == []


@pytest.mark.parametrize("name", ALL_PROVIDER_VARS)
def test_naming_one_provider_switches_the_whole_ai_surface_on(name):
    """No half-configured state: naming one stage's transport means the other
    three must be named too, key or no key."""
    env = dict(BASE_ENV, **{name: "gemini"})
    assert config.ai_configured(env)
    assert config.missing_required_vars(env) == [
        other for other in ALL_PROVIDER_VARS if other != name
    ]


def test_a_fully_named_gemini_deployment_without_a_key_still_starts():
    # ADR 0008 keeps "deploy without AI" supported: a stage on gemini with no
    # GEMINI_API_KEY is the documented 502 / safe-pool state, not a refusal
    # to start.
    env = dict(NAMED_PROVIDERS_ENV)
    assert config.missing_required_vars(env) == []
    assert config.invalid_required_values(env) == []


@pytest.mark.parametrize("name", PROVIDER_VARS)
def test_unknown_provider_value_is_reported_with_the_valid_ones(name):
    env = dict(AI_ENV, **{name: "vertex"})
    problems = config.invalid_required_values(env)
    assert len(problems) == 1
    assert name in problems[0]
    assert "vertex" in problems[0]
    assert "gemini" in problems[0] and "openai_compat" in problems[0]


def test_openai_compat_needs_an_endpoint_and_a_key_statement():
    env = dict(AI_ENV, **ALL_OPENAI_COMPAT)
    assert config.missing_required_vars(env) == [
        "AI_OPENAI_COMPAT_ENDPOINT", "AI_OPENAI_COMPAT_API_KEY",
    ]


def test_a_complete_openai_compat_deployment_has_no_missing_vars():
    assert config.missing_required_vars(OPENAI_COMPAT_ENV) == []
    assert config.invalid_required_values(OPENAI_COMPAT_ENV) == []


def test_an_empty_shared_key_is_a_statement_not_an_omission():
    # "This endpoint needs no Authorization header" — the DB_PASSWORD rule
    # applied to a local model server.
    env = dict(OPENAI_COMPAT_ENV, AI_OPENAI_COMPAT_API_KEY="")
    assert config.missing_required_vars(env) == []
    assert config.resolve_stage(env, config.QUESTION_STAGE_VARS).api_key == ""


def test_per_stage_endpoint_and_key_satisfy_the_requirement_alone():
    env = dict(AI_ENV, **ALL_OPENAI_COMPAT)
    env.pop("AI_OPENAI_COMPAT_ENDPOINT", None)
    for stage in ("AI_QUESTION", "AI_SCRIPTURE_REWRITE", "AI_SCRIPTURE_RERANK"):
        env[f"{stage}_ENDPOINT"] = "https://one.example/v1"
        env[f"{stage}_API_KEY"] = "one-key"
    assert config.missing_required_vars(env) == []


def test_the_chat_models_are_required_on_openai_compat_without_any_key():
    env = dict(BASE_ENV, **ALL_OPENAI_COMPAT, AI_TRANSCRIBE_PROVIDER="gemini")
    env["AI_OPENAI_COMPAT_ENDPOINT"] = "https://llm.example/v1"
    env["AI_OPENAI_COMPAT_API_KEY"] = "k"
    # Every chat model, and NOT the transcription one: that stage is on
    # gemini and has no key here, so demanding its model would refuse a
    # deployment that simply does not transcribe.
    assert config.missing_required_vars(env) == [
        "AI_QUESTION_MODEL",
        "AI_SCRIPTURE_REWRITE_MODEL",
        "AI_SCRIPTURE_RERANK_MODEL",
    ]


def test_the_transcription_model_is_required_by_the_gemini_key_alone():
    env = dict(OPENAI_COMPAT_ENV)
    del env["AI_TRANSCRIBE_MODEL"]
    assert config.missing_required_vars(env) == ["AI_TRANSCRIBE_MODEL"]


# --- the transcription provider (ADR 0012) ---------------------------------


def test_unknown_transcription_provider_is_reported_with_all_three():
    problems = config.invalid_required_values(
        dict(AI_ENV, AI_TRANSCRIBE_PROVIDER="whisper")
    )
    assert len(problems) == 1
    assert TRANSCRIBE_PROVIDER_VAR in problems[0]
    assert "whisper" in problems[0]
    for value in ("gemini", "local", "openai_compat"):
        assert value in problems[0]


def test_local_transcription_environment_is_complete_without_any_key():
    """The point of the fallback provider: voice messages transcribed with no
    Google credentials and no model server anywhere in the environment."""
    assert "GEMINI_API_KEY" not in LOCAL_TRANSCRIBE_ENV
    assert config.missing_required_vars(LOCAL_TRANSCRIBE_ENV) == []
    assert config.invalid_required_values(LOCAL_TRANSCRIBE_ENV) == []


def test_local_transcription_requires_the_weights_path():
    env = dict(LOCAL_TRANSCRIBE_ENV)
    del env["AI_TRANSCRIBE_MODEL_PATH"]
    assert config.missing_required_vars(env) == ["AI_TRANSCRIBE_MODEL_PATH"]


def test_local_transcription_requires_the_model_identity():
    """The path says where the bytes are; only the model name says WHICH
    Whisper this deployment runs, and every report needs that."""
    env = dict(LOCAL_TRANSCRIBE_ENV)
    del env["AI_TRANSCRIBE_MODEL"]
    assert config.missing_required_vars(env) == ["AI_TRANSCRIBE_MODEL"]


@pytest.mark.parametrize("provider", ["gemini", "openai_compat"])
def test_a_remote_transcription_provider_wants_no_weights_path(provider):
    env = dict(
        REMOTE_TRANSCRIBE_ENV,
        AI_TRANSCRIBE_PROVIDER=provider,
        AI_TRANSCRIBE_MODEL_PATH="/models/whisper/small",
    )
    problems = [
        problem
        for problem in config.invalid_required_values(env)
        if "AI_TRANSCRIBE_MODEL_PATH" in problem
    ]
    assert len(problems) == 1
    assert provider in problems[0]


def test_the_missing_model_error_names_the_provider_that_is_configured():
    """The production provider must not be told about the local weights path —
    on `openai_compat` that variable is itself a startup error."""
    env = dict(REMOTE_TRANSCRIBE_ENV)
    del env["AI_TRANSCRIBE_MODEL"]
    with pytest.raises(config.ConfigError) as exc:
        config._validate(env, [])
    message = str(exc.value)
    assert "AI_TRANSCRIBE_MODEL is required" in message
    assert "openai_compat" in message
    assert "AI_TRANSCRIBE_MODEL_PATH" not in message

    local = dict(LOCAL_TRANSCRIBE_ENV)
    del local["AI_TRANSCRIBE_MODEL"]
    with pytest.raises(config.ConfigError) as exc:
        config._validate(local, [])
    assert "AI_TRANSCRIBE_MODEL_PATH" in str(exc.value)


def test_remote_transcription_environment_is_complete():
    assert config.missing_required_vars(REMOTE_TRANSCRIBE_ENV) == []
    assert config.invalid_required_values(REMOTE_TRANSCRIBE_ENV) == []


def test_remote_transcription_needs_an_endpoint_and_a_key_statement():
    env = dict(NAMED_PROVIDERS_ENV, AI_TRANSCRIBE_PROVIDER="openai_compat")
    assert config.missing_required_vars(env) == [
        "AI_TRANSCRIBE_MODEL",
        "AI_OPENAI_COMPAT_ENDPOINT",
        "AI_OPENAI_COMPAT_API_KEY",
    ]


def test_the_transcription_stage_may_use_the_shared_openai_compat_pair():
    """The audio server is usually its own process, but nothing forces it to
    be: a deployment that serves both from one endpoint says so once."""
    env = dict(
        NAMED_PROVIDERS_ENV,
        AI_TRANSCRIBE_PROVIDER="openai_compat",
        AI_TRANSCRIBE_MODEL="whisper-large-v3",
        AI_OPENAI_COMPAT_ENDPOINT="https://llm.example/v1",
        AI_OPENAI_COMPAT_API_KEY="",
    )
    assert config.missing_required_vars(env) == []
    stage = config.resolve_stage(env, config.TRANSCRIBE_STAGE_VARS)
    assert stage.endpoint == "https://llm.example/v1"
    assert stage.api_key == ""


def test_the_transcription_endpoint_may_not_carry_a_secret():
    problems = config.invalid_required_values(
        dict(
            REMOTE_TRANSCRIBE_ENV,
            AI_TRANSCRIBE_ENDPOINT="https://user:pass@whisper.example/v1",
        )
    )
    assert len(problems) == 1
    assert "AI_TRANSCRIBE_ENDPOINT" in problems[0]
    assert "pass" not in problems[0]


def test_a_transcription_key_without_a_shared_key_is_an_error():
    env = dict(NAMED_PROVIDERS_ENV, AI_TRANSCRIBE_API_KEY="paid-key")
    problems = config.invalid_required_values(env)
    assert len(problems) == 1
    assert "AI_TRANSCRIBE_API_KEY" in problems[0]
    assert "GEMINI_API_KEY" in problems[0]


def test_resolve_stage_gives_the_local_provider_no_endpoint_and_no_key():
    """`local` runs in this process: an inherited GEMINI_API_KEY here would
    state, in the object the code reads, that a local model bills Google."""
    stage = config.resolve_stage(
        dict(LOCAL_TRANSCRIBE_ENV, GEMINI_API_KEY="gemini-key"),
        config.TRANSCRIBE_STAGE_VARS,
    )
    assert stage.is_local and not stage.is_gemini and not stage.is_openai_compat
    assert stage.model == "small"
    assert stage.endpoint == "" and stage.api_key == ""


def test_resolve_stage_reads_the_transcription_override_pair():
    stage = config.resolve_stage(
        dict(
            REMOTE_TRANSCRIBE_ENV,
            AI_OPENAI_COMPAT_ENDPOINT="https://llm.example/v1",
            AI_OPENAI_COMPAT_API_KEY="chat-key",
        ),
        config.TRANSCRIBE_STAGE_VARS,
    )
    # The audio server is not the chat server: the override wins over both
    # shared values, which is the whole reason it exists.
    assert stage.is_openai_compat
    assert stage.endpoint == "https://whisper.example:8000/v1"
    assert stage.api_key == "audio-key"
    assert stage.model == "Systran/faster-whisper-large-v3"


@pytest.mark.parametrize("value", ["int4", "float64", "INT8"])
def test_an_unknown_compute_type_is_refused_with_the_valid_ones(value):
    problems = config.invalid_required_values(
        dict(LOCAL_TRANSCRIBE_ENV, AI_TRANSCRIBE_COMPUTE_TYPE=value)
    )
    assert len(problems) == 1
    assert "AI_TRANSCRIBE_COMPUTE_TYPE" in problems[0]
    assert "int8" in problems[0]


def test_the_measured_compute_type_is_accepted():
    assert (
        config.invalid_required_values(
            dict(LOCAL_TRANSCRIBE_ENV, AI_TRANSCRIBE_COMPUTE_TYPE="int8")
        )
        == []
    )


@pytest.mark.parametrize(
    "endpoint",
    ["llm.example/v1", "ftp://llm.example/v1", "/v1", "https:///v1"],
)
def test_an_endpoint_that_is_not_an_http_url_is_rejected(endpoint):
    problem = config.validate_endpoint("AI_OPENAI_COMPAT_ENDPOINT", endpoint)
    assert problem is not None and "AI_OPENAI_COMPAT_ENDPOINT" in problem


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://user:s3cret@llm.example/v1",
        "https://llm.example/v1?key=s3cret",
    ],
)
def test_an_endpoint_carrying_a_secret_is_rejected_without_echoing_it(endpoint):
    problem = config.validate_endpoint("AI_OPENAI_COMPAT_ENDPOINT", endpoint)
    assert problem is not None
    assert "s3cret" not in problem
    # ...and it reaches the aggregated startup error the same way.
    env = dict(OPENAI_COMPAT_ENV, AI_OPENAI_COMPAT_ENDPOINT=endpoint)
    problems = config.invalid_required_values(env)
    assert len(problems) == 1
    assert "s3cret" not in problems[0]


def test_resolve_stage_reads_gemini_model_and_shared_key():
    stage = config.resolve_stage(AI_ENV, config.SCRIPTURE_RERANK_STAGE_VARS)
    assert stage.is_gemini and not stage.is_openai_compat
    assert stage.model == "gemini-3.5-flash-lite"
    assert stage.api_key == "gemini-key"
    assert stage.endpoint == ""


def test_resolve_stage_reads_the_openai_compat_triple():
    stage = config.resolve_stage(
        OPENAI_COMPAT_ENV, config.SCRIPTURE_REWRITE_STAGE_VARS
    )
    assert stage.is_openai_compat
    assert stage.model == "qwen3-30b"
    assert stage.endpoint == "https://llm.example:8443/v1"
    assert stage.api_key == "local-key"


def test_a_stage_override_wins_over_the_shared_endpoint_and_key():
    env = dict(
        OPENAI_COMPAT_ENV,
        AI_SCRIPTURE_RERANK_ENDPOINT="https://other.example/v1",
        AI_SCRIPTURE_RERANK_API_KEY="other-key",
    )
    rerank = config.resolve_stage(env, config.SCRIPTURE_RERANK_STAGE_VARS)
    question = config.resolve_stage(env, config.QUESTION_STAGE_VARS)
    assert (rerank.endpoint, rerank.api_key) == (
        "https://other.example/v1", "other-key",
    )
    # ...and only that stage moves.
    assert (question.endpoint, question.api_key) == (
        "https://llm.example:8443/v1", "local-key",
    )


def test_a_gemini_stage_key_still_bills_the_stage_not_the_endpoint():
    # AI_SCRIPTURE_REWRITE_API_KEY keeps the meaning it had before the
    # provider switch, and the two other stages gained the same option.
    env = dict(AI_ENV, AI_QUESTION_API_KEY="question-key")
    question = config.resolve_stage(env, config.QUESTION_STAGE_VARS)
    rerank = config.resolve_stage(env, config.SCRIPTURE_RERANK_STAGE_VARS)
    assert question.api_key == "question-key"
    assert rerank.api_key == "gemini-key"


@pytest.mark.parametrize(
    "name",
    ["AI_QUESTION_API_KEY", "AI_SCRIPTURE_RERANK_API_KEY"],
)
def test_any_stage_key_without_a_shared_gemini_key_is_an_error(name):
    # The 2026-08-29 asymmetry, generalised: a key that only the Gemini path
    # could spend, on a deployment that has no Gemini key.
    env = dict(BASE_ENV, **{name: "paid-key"})
    problems = config.invalid_required_values(env)
    assert len(problems) == 1
    assert name in problems[0] and "GEMINI_API_KEY" in problems[0]


def test_an_openai_compat_stage_key_needs_no_gemini_key():
    env = dict(BASE_ENV, **ALL_OPENAI_COMPAT)
    env.update(
        AI_OPENAI_COMPAT_ENDPOINT="https://llm.example/v1",
        AI_OPENAI_COMPAT_API_KEY="",
        AI_QUESTION_MODEL="m",
        AI_SCRIPTURE_REWRITE_MODEL="m",
        AI_SCRIPTURE_RERANK_MODEL="m",
        AI_SCRIPTURE_REWRITE_API_KEY="paid-local-key",
    )
    assert config.invalid_required_values(env) == []
    assert config.resolve_rewrite_api_key(env) == "paid-local-key"


# --- aggregated error ------------------------------------------------------


def test_validate_lists_every_problem_at_once():
    env = dict(AI_ENV)
    del env["AI_SCRIPTURE_REWRITE_MODEL"]
    del env["EMBEDDING_DIMENSIONS"]
    with pytest.raises(config.ConfigError) as exc:
        config._validate(env, ["AI_REQUESTS_PER_MINUTE: expected an integer, got 'many'"])
    message = str(exc.value)
    assert "AI_SCRIPTURE_REWRITE_MODEL" in message
    assert "EMBEDDING_DIMENSIONS" in message
    assert "AI_REQUESTS_PER_MINUTE" in message
    assert "3 problems" in message


def test_validate_passes_on_a_complete_environment():
    config._validate(AI_ENV, [])


# --- import-time behaviour -------------------------------------------------


def _reload_config(monkeypatch, env):
    for name in (*ALWAYS_REQUIRED, *PRESENCE_REQUIRED, *AI_REQUIRED,
                 *ALL_PROVIDER_VARS, "GEMINI_API_KEY", "DB_PORT",
                 "AI_OPENAI_COMPAT_ENDPOINT", "AI_OPENAI_COMPAT_API_KEY",
                 "AI_QUESTION_ENDPOINT", "AI_QUESTION_API_KEY",
                 "AI_SCRIPTURE_REWRITE_ENDPOINT", "AI_SCRIPTURE_REWRITE_API_KEY",
                 "AI_SCRIPTURE_RERANK_ENDPOINT", "AI_SCRIPTURE_RERANK_API_KEY",
                 "AI_TRANSCRIBE_ENDPOINT", "AI_TRANSCRIBE_API_KEY",
                 "AI_TRANSCRIBE_MODEL_PATH", "AI_TRANSCRIBE_COMPUTE_TYPE",
                 "AI_TRANSCRIBE_THREADS", "AI_TRANSCRIBE_BEAM_SIZE",
                 "AI_TRANSCRIBE_TIMEOUT_SECONDS",
                 "AI_TRANSCRIBE_MAX_AUDIO_SECONDS",
                 "AI_QUESTION_TIMEOUT_SECONDS",
                 "AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS",
                 "EMBEDDING_MODEL_PATH"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return importlib.reload(config)


@pytest.fixture(autouse=True)
def _restore_config():
    """Reimport the module with the real environment after each reload test."""
    yield
    importlib.reload(config)


def test_import_succeeds_without_gemini_key(monkeypatch):
    module = _reload_config(monkeypatch, BASE_ENV)
    assert module.GEMINI_API_KEY == ""
    # "AI not configured": empty provider models, never a guessed one.
    assert module.AI_QUESTION_MODEL == ""
    assert module.AI_SCRIPTURE_REWRITE_MODEL == ""
    # But the index this deployment reads is still named — that is what keeps
    # the documented keyless safe-pool answer working.
    assert module.EMBEDDING_MODEL == "gemini-embedding-001"
    assert module.EMBEDDING_DIMENSIONS == 768
    # The rest of the API keeps its configuration.
    assert module.API_KEY == "k"
    assert module.DB_NAME == "cep_public"


def test_import_reads_the_local_embedding_provider(monkeypatch):
    module = _reload_config(monkeypatch, LOCAL_EMBEDDING_ENV)
    assert module.EMBEDDING_PROVIDER == module.EMBEDDING_PROVIDER_LOCAL
    assert module.EMBEDDING_MODEL_PATH == "/models/bge-m3"
    # The identity that versions the index is the model id, never the path.
    assert module.EMBEDDING_MODEL == "BAAI/bge-m3"
    assert module.EMBEDDING_DIMENSIONS == 1024


def test_import_fails_without_an_embedding_provider(monkeypatch):
    env = dict(BASE_ENV)
    del env["EMBEDDING_PROVIDER"]
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, env)
    assert "EMBEDDING_PROVIDER" in str(exc.value)


def test_import_fails_with_gemini_key_and_no_models(monkeypatch):
    # RuntimeError, not config.ConfigError: importlib.reload rebuilds the
    # class object, so the pre-reload reference would not match.
    with pytest.raises(RuntimeError) as exc:
        _reload_config(
            monkeypatch, dict(BASE_ENV, GEMINI_API_KEY="gemini-key")
        )
    message = str(exc.value)
    for name in AI_REQUIRED:
        assert name in message
    assert "GEMINI_API_KEY is set" in message


def test_import_fails_without_the_embedding_pair(monkeypatch):
    env = dict(BASE_ENV)
    del env["EMBEDDING_MODEL"]
    del env["EMBEDDING_DIMENSIONS"]
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, env)
    message = str(exc.value)
    assert "EMBEDDING_MODEL" in message
    assert "EMBEDDING_DIMENSIONS" in message


@pytest.mark.parametrize("raw", ["0", "-1"])
def test_import_fails_on_non_positive_dimensions(monkeypatch, raw):
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, dict(BASE_ENV, EMBEDDING_DIMENSIONS=raw))
    assert "EMBEDDING_DIMENSIONS" in str(exc.value)


def test_import_fails_on_a_garbage_db_port(monkeypatch):
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, dict(BASE_ENV, DB_PORT="3306;DROP"))
    assert "DB_PORT" in str(exc.value)


def test_import_fails_on_missing_db_variables(monkeypatch):
    env = dict(BASE_ENV)
    del env["DB_NAME"]
    del env["DB_PASSWORD"]
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, env)
    message = str(exc.value)
    assert "DB_NAME" in message
    assert "DB_PASSWORD" in message


def test_import_fails_on_non_numeric_value(monkeypatch):
    env = dict(AI_ENV, AI_SCRIPTURE_INDEX_CACHE_SECONDS="1h")
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, env)
    assert "AI_SCRIPTURE_INDEX_CACHE_SECONDS" in str(exc.value)


def test_import_reports_missing_models_and_bad_numbers_together(monkeypatch):
    env = dict(AI_ENV, AI_SCRIPTURE_TIMEOUT_SECONDS="fifteen")
    del env["AI_SCRIPTURE_RERANK_MODEL"]
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, env)
    message = str(exc.value)
    assert "AI_SCRIPTURE_RERANK_MODEL" in message
    assert "AI_SCRIPTURE_TIMEOUT_SECONDS" in message


def test_import_resolves_the_rewrite_key_to_the_dedicated_one(monkeypatch):
    module = _reload_config(
        monkeypatch, dict(AI_ENV, AI_SCRIPTURE_REWRITE_API_KEY="paid-key")
    )
    assert module.REWRITE_API_KEY == "paid-key"
    # Every other stage keeps billing the shared key.
    assert module.GEMINI_API_KEY == "gemini-key"


def test_import_resolves_the_rewrite_key_to_the_shared_one(monkeypatch):
    module = _reload_config(monkeypatch, AI_ENV)
    assert module.REWRITE_API_KEY == module.GEMINI_API_KEY == "gemini-key"


def test_import_fails_on_a_rewrite_key_without_a_shared_key(monkeypatch):
    with pytest.raises(RuntimeError) as exc:
        _reload_config(
            monkeypatch, dict(BASE_ENV, AI_SCRIPTURE_REWRITE_API_KEY="paid-key")
        )
    assert "AI_SCRIPTURE_REWRITE_API_KEY" in str(exc.value)


def test_import_succeeds_on_a_fully_configured_environment(monkeypatch):
    module = _reload_config(monkeypatch, AI_ENV)
    assert module.AI_SCRIPTURE_REWRITE_MODEL == "gemini-3.7-flash"
    assert module.AI_SCRIPTURE_RERANK_MODEL == "gemini-3.5-flash-lite"
    assert module.EMBEDDING_DIMENSIONS == 768


def test_import_fails_on_a_key_without_the_providers(monkeypatch):
    """The `.env` every deployment had before ADR 0009 does not start — and
    since ADR 0012 the transcription provider is named in the same error."""
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, dict(BASE_ENV, GEMINI_API_KEY="gemini-key"))
    message = str(exc.value)
    for name in ALL_PROVIDER_VARS:
        assert name in message
    assert "openai_compat" in message
    assert "local" in message


def test_import_reads_the_local_transcription_provider(monkeypatch):
    module = _reload_config(monkeypatch, LOCAL_TRANSCRIBE_ENV)
    assert module.TRANSCRIBE_PROVIDER.is_local
    assert module.AI_TRANSCRIBE_MODEL == "small"
    assert module.AI_TRANSCRIBE_MODEL_PATH == "/models/whisper/small"
    # The measured operating point, as defaults rather than as required
    # variables (ADR 0012).
    assert module.AI_TRANSCRIBE_COMPUTE_TYPE == "int8"
    assert module.AI_TRANSCRIBE_THREADS == 0
    assert module.AI_TRANSCRIBE_BEAM_SIZE == 1
    assert module.AI_TRANSCRIBE_TIMEOUT_SECONDS == 60.0
    assert module.AI_TRANSCRIBE_MAX_AUDIO_SECONDS == 600.0


def test_import_reads_the_remote_transcription_provider(monkeypatch):
    module = _reload_config(monkeypatch, REMOTE_TRANSCRIBE_ENV)
    assert module.TRANSCRIBE_PROVIDER.is_openai_compat
    assert module.TRANSCRIBE_PROVIDER.endpoint == "https://whisper.example:8000/v1"
    assert module.TRANSCRIBE_PROVIDER.api_key == "audio-key"


def test_import_fails_on_a_local_transcriber_without_a_path(monkeypatch):
    env = dict(LOCAL_TRANSCRIBE_ENV)
    del env["AI_TRANSCRIBE_MODEL_PATH"]
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, env)
    assert "AI_TRANSCRIBE_MODEL_PATH" in str(exc.value)


def test_import_fails_on_a_garbage_transcription_number(monkeypatch):
    with pytest.raises(RuntimeError) as exc:
        _reload_config(
            monkeypatch, dict(LOCAL_TRANSCRIBE_ENV, AI_TRANSCRIBE_BEAM_SIZE="five")
        )
    assert "AI_TRANSCRIBE_BEAM_SIZE" in str(exc.value)


def test_import_resolves_every_stage_to_openai_compat(monkeypatch):
    module = _reload_config(monkeypatch, OPENAI_COMPAT_ENV)
    for stage in (
        module.QUESTION_PROVIDER,
        module.SCRIPTURE_REWRITE_PROVIDER,
        module.SCRIPTURE_RERANK_PROVIDER,
    ):
        assert stage.is_openai_compat
        assert stage.endpoint == "https://llm.example:8443/v1"
        assert stage.api_key == "local-key"
        assert stage.model == "qwen3-30b"
    # Transcription is untouched by THIS switch and still Gemini's: the two
    # providers are named separately on purpose (ADR 0012).
    assert module.TRANSCRIBE_PROVIDER.is_gemini
    assert module.GEMINI_API_KEY == "gemini-key"
    assert module.AI_TRANSCRIBE_MODEL == "gemini-3.5-flash-lite"


def test_import_fails_on_an_openai_compat_stage_without_an_endpoint(monkeypatch):
    env = dict(OPENAI_COMPAT_ENV)
    del env["AI_OPENAI_COMPAT_ENDPOINT"]
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, env)
    assert "AI_OPENAI_COMPAT_ENDPOINT" in str(exc.value)


def test_import_fails_on_an_unknown_provider(monkeypatch):
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, dict(AI_ENV, AI_QUESTION_PROVIDER="qwen"))
    assert "AI_QUESTION_PROVIDER" in str(exc.value)


def test_the_provider_timeout_knobs_keep_their_measured_defaults(monkeypatch):
    module = _reload_config(monkeypatch, AI_ENV)
    assert module.AI_QUESTION_TIMEOUT_SECONDS == 20.0
    assert module.AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS == 8.0
