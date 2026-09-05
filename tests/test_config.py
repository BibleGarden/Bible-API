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
ALL_GEMINI = {name: "gemini" for name in PROVIDER_VARS}
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
}

AI_ENV = dict(
    BASE_ENV,
    **ALL_GEMINI,
    GEMINI_API_KEY="gemini-key",
    AI_QUESTION_MODEL="gemini-3.5-flash-lite",
    AI_TRANSCRIBE_MODEL="gemini-3.5-flash-lite",
    AI_SCRIPTURE_REWRITE_MODEL="gemini-3.7-flash",
    AI_SCRIPTURE_RERANK_MODEL="gemini-3.5-flash-lite",
)

# The same deployment with every chat stage on a local OpenAI-compatible
# endpoint. Gemini is still configured — transcription has no other provider
# (ClickUp 86cbegg2f, step 6 replaces it) — which is exactly the mixed state
# the switch has to support.
OPENAI_COMPAT_ENV = dict(
    AI_ENV,
    **ALL_OPENAI_COMPAT,
    AI_OPENAI_COMPAT_ENDPOINT="https://llm.example:8443/v1",
    AI_OPENAI_COMPAT_API_KEY="local-key",
    AI_QUESTION_MODEL="qwen3-30b",
    AI_SCRIPTURE_REWRITE_MODEL="qwen3-30b",
    AI_SCRIPTURE_RERANK_MODEL="qwen3-30b",
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
    which stage. It does not start, and it names the three variables."""
    env = dict(BASE_ENV, GEMINI_API_KEY="gemini-key")
    assert config.missing_required_vars(env) == PROVIDER_VARS + AI_REQUIRED


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


@pytest.mark.parametrize("name", PROVIDER_VARS)
def test_naming_one_provider_switches_the_whole_ai_surface_on(name):
    """No half-configured state: naming one stage's transport means the other
    two must be named too, key or no key."""
    env = dict(BASE_ENV, **{name: "gemini"})
    assert config.ai_configured(env)
    assert config.missing_required_vars(env) == [
        other for other in PROVIDER_VARS if other != name
    ]


def test_a_fully_named_gemini_deployment_without_a_key_still_starts():
    # ADR 0008 keeps "deploy without AI" supported: a stage on gemini with no
    # GEMINI_API_KEY is the documented 502 / safe-pool state, not a refusal
    # to start.
    env = dict(BASE_ENV, **ALL_GEMINI)
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
    env = dict(BASE_ENV, **ALL_OPENAI_COMPAT)
    env["AI_OPENAI_COMPAT_ENDPOINT"] = "https://llm.example/v1"
    env["AI_OPENAI_COMPAT_API_KEY"] = "k"
    # Every chat model, and NOT the transcription one: that stage is
    # Gemini-only and has no key here, so demanding its model would refuse a
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


def test_transcription_has_no_provider_variable():
    problems = config.invalid_required_values(
        dict(AI_ENV, AI_TRANSCRIBE_PROVIDER="openai_compat")
    )
    assert len(problems) == 1
    assert "AI_TRANSCRIBE_PROVIDER" in problems[0]


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
                 *PROVIDER_VARS, "GEMINI_API_KEY", "DB_PORT",
                 "AI_OPENAI_COMPAT_ENDPOINT", "AI_OPENAI_COMPAT_API_KEY",
                 "AI_QUESTION_ENDPOINT", "AI_QUESTION_API_KEY",
                 "AI_SCRIPTURE_REWRITE_ENDPOINT", "AI_SCRIPTURE_REWRITE_API_KEY",
                 "AI_SCRIPTURE_RERANK_ENDPOINT", "AI_SCRIPTURE_RERANK_API_KEY",
                 "AI_TRANSCRIBE_PROVIDER", "AI_QUESTION_TIMEOUT_SECONDS",
                 "AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS"):
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
    """The `.env` every deployment had before ADR 0009 does not start."""
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, dict(BASE_ENV, GEMINI_API_KEY="gemini-key"))
    message = str(exc.value)
    for name in PROVIDER_VARS:
        assert name in message
    assert "openai_compat" in message


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
    # Transcription is untouched by the switch and still Gemini's.
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
