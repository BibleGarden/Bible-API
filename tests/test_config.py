"""Focused tests for the explicit AI configuration contract (ADR 0019)."""

import importlib

import pytest

import config

# Literal contract lists: these must not be derived from production tuples,
# or deleting a required variable from both validation and its test would pass.
ALWAYS_REQUIRED = (
    "API_KEY",
    "DB_HOST",
    "DB_USER",
    "DB_NAME",
    "AI_ENABLED",
    "EMBEDDING_MODEL",
    "EMBEDDING_DIMENSIONS",
    "EMBEDDING_PROVIDER",
)
PRESENCE_REQUIRED = ("DB_PASSWORD",)
AI_PROVIDERS_REQUIRED = (
    "AI_QUESTION_PROVIDER",
    "AI_SCRIPTURE_REWRITE_PROVIDER",
    "AI_SCRIPTURE_RERANK_PROVIDER",
    "AI_TRANSCRIBE_PROVIDER",
)
AI_MODELS_REQUIRED = (
    "AI_QUESTION_MODEL",
    "AI_TRANSCRIBE_MODEL",
    "AI_SCRIPTURE_REWRITE_MODEL",
    "AI_SCRIPTURE_RERANK_MODEL",
)
REMOVED_AI_VARS = (
    "AI_OPENAI_COMPAT_ENDPOINT",
    "AI_OPENAI_COMPAT_API_KEY",
    "GEMINI_API_KEY",
)
FORBIDDEN_REASONING_VARS = (
    "AI_REASONING_EFFORT",
    "AI_TRANSCRIBE_REASONING_EFFORT",
    "EMBEDDING_REASONING_EFFORT",
)
REASONING_EFFORTS = ("omit", "none", "low", "medium", "high")
INTEGER_OPERATIONAL_VARS = (
    "DB_PORT",
    "IMPORT_MAX_PAYLOAD_MB",
    "AI_TRANSCRIBE_THREADS",
    "AI_TRANSCRIBE_BEAM_SIZE",
    "AI_REQUESTS_PER_MINUTE",
    "AI_REQUESTS_PER_CLIENT_PER_MINUTE",
    "AI_SCRIPTURE_REQUESTS_PER_MINUTE",
    "AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE",
    "AI_SCRIPTURE_INDEX_CACHE_SECONDS",
    "TRUSTED_PROXY_DNS_TTL_SECONDS",
)
FLOAT_OPERATIONAL_VARS = (
    "IMPORT_HTTP_TIMEOUT_SECONDS",
    "AI_TRANSCRIBE_TIMEOUT_SECONDS",
    "AI_TRANSCRIBE_MAX_AUDIO_SECONDS",
    "AI_QUESTION_TIMEOUT_SECONDS",
    "AI_SCRIPTURE_TIMEOUT_SECONDS",
    "AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS",
)
STAGE_FIELDS = (
    (
        "question",
        "AI_QUESTION_PROVIDER",
        "AI_QUESTION_MODEL",
        "AI_QUESTION_ENDPOINT",
        "AI_QUESTION_API_KEY",
        "AI_QUESTION_REASONING_EFFORT",
    ),
    (
        "scripture_rewrite",
        "AI_SCRIPTURE_REWRITE_PROVIDER",
        "AI_SCRIPTURE_REWRITE_MODEL",
        "AI_SCRIPTURE_REWRITE_ENDPOINT",
        "AI_SCRIPTURE_REWRITE_API_KEY",
        "AI_SCRIPTURE_REWRITE_REASONING_EFFORT",
    ),
    (
        "scripture_rerank",
        "AI_SCRIPTURE_RERANK_PROVIDER",
        "AI_SCRIPTURE_RERANK_MODEL",
        "AI_SCRIPTURE_RERANK_ENDPOINT",
        "AI_SCRIPTURE_RERANK_API_KEY",
        "AI_SCRIPTURE_RERANK_REASONING_EFFORT",
    ),
    (
        "transcribe",
        "AI_TRANSCRIBE_PROVIDER",
        "AI_TRANSCRIBE_MODEL",
        "AI_TRANSCRIBE_ENDPOINT",
        "AI_TRANSCRIBE_API_KEY",
        None,
    ),
    (
        "embeddings",
        "EMBEDDING_PROVIDER",
        "EMBEDDING_MODEL",
        "EMBEDDING_ENDPOINT",
        "EMBEDDING_API_KEY",
        None,
    ),
)
AI_DISABLED_FORBIDDEN = (
    "AI_CLIENT_HMAC_KEY",
    "AI_QUESTION_PROVIDER",
    "AI_QUESTION_MODEL",
    "AI_QUESTION_ENDPOINT",
    "AI_QUESTION_API_KEY",
    "AI_QUESTION_REASONING_EFFORT",
    "AI_SCRIPTURE_REWRITE_PROVIDER",
    "AI_SCRIPTURE_REWRITE_MODEL",
    "AI_SCRIPTURE_REWRITE_ENDPOINT",
    "AI_SCRIPTURE_REWRITE_API_KEY",
    "AI_SCRIPTURE_REWRITE_REASONING_EFFORT",
    "AI_SCRIPTURE_RERANK_PROVIDER",
    "AI_SCRIPTURE_RERANK_MODEL",
    "AI_SCRIPTURE_RERANK_ENDPOINT",
    "AI_SCRIPTURE_RERANK_API_KEY",
    "AI_SCRIPTURE_RERANK_REASONING_EFFORT",
    "AI_TRANSCRIBE_PROVIDER",
    "AI_TRANSCRIBE_MODEL",
    "AI_TRANSCRIBE_ENDPOINT",
    "AI_TRANSCRIBE_API_KEY",
    "AI_TRANSCRIBE_MODEL_PATH",
)


BASE_ENV = {
    "API_KEY": "k",
    "DB_HOST": "db",
    "DB_USER": "cep",
    "DB_PASSWORD": "secret",
    "DB_NAME": "cep_public",
    "AI_ENABLED": "false",
    "EMBEDDING_PROVIDER": "openai_compat",
    "EMBEDDING_MODEL": "BAAI/bge-m3",
    "EMBEDDING_DIMENSIONS": "1024",
    "EMBEDDING_ENDPOINT": "https://embeddings.example/v1",
    "EMBEDDING_API_KEY": "embed-key",
}

AI_ENV = {
    **BASE_ENV,
    "AI_ENABLED": "true",
    "AI_CLIENT_HMAC_KEY": "hmac-key",
    "AI_QUESTION_PROVIDER": "openai_compat",
    "AI_QUESTION_MODEL": "cerebras-model",
    "AI_QUESTION_ENDPOINT": "https://cerebras.example/v1",
    "AI_QUESTION_API_KEY": "question-key",
    "AI_QUESTION_REASONING_EFFORT": "none",
    "AI_SCRIPTURE_REWRITE_PROVIDER": "openai_compat",
    "AI_SCRIPTURE_REWRITE_MODEL": "qwen3-30b",
    "AI_SCRIPTURE_REWRITE_ENDPOINT": "http://qwen:8000/v1",
    "AI_SCRIPTURE_REWRITE_API_KEY": "",
    "AI_SCRIPTURE_REWRITE_REASONING_EFFORT": "omit",
    "AI_SCRIPTURE_RERANK_PROVIDER": "openai_compat",
    "AI_SCRIPTURE_RERANK_MODEL": "qwen3-30b",
    "AI_SCRIPTURE_RERANK_ENDPOINT": "http://qwen:8000/v1",
    "AI_SCRIPTURE_RERANK_API_KEY": "rerank-key",
    "AI_SCRIPTURE_RERANK_REASONING_EFFORT": "high",
    "AI_TRANSCRIBE_PROVIDER": "local",
    "AI_TRANSCRIBE_MODEL": "small",
    "AI_TRANSCRIBE_MODEL_PATH": "/models/whisper/small",
}

GEMINI_AI_ENV = {
    **BASE_ENV,
    "AI_ENABLED": "true",
    "AI_CLIENT_HMAC_KEY": "hmac-key",
    "AI_QUESTION_PROVIDER": "gemini",
    "AI_QUESTION_MODEL": "gemini-question",
    "AI_QUESTION_API_KEY": "question-key",
    "AI_SCRIPTURE_REWRITE_PROVIDER": "gemini",
    "AI_SCRIPTURE_REWRITE_MODEL": "gemini-rewrite",
    "AI_SCRIPTURE_REWRITE_API_KEY": "rewrite-key",
    "AI_SCRIPTURE_RERANK_PROVIDER": "gemini",
    "AI_SCRIPTURE_RERANK_MODEL": "gemini-rerank",
    "AI_SCRIPTURE_RERANK_API_KEY": "rerank-key",
    "AI_TRANSCRIBE_PROVIDER": "gemini",
    "AI_TRANSCRIBE_MODEL": "gemini-transcribe",
    "AI_TRANSCRIBE_API_KEY": "transcribe-key",
}
GEMINI_AI_ENV.pop("EMBEDDING_ENDPOINT")
GEMINI_AI_ENV.update(
    EMBEDDING_PROVIDER="gemini",
    EMBEDDING_MODEL="gemini-embedding-001",
    EMBEDDING_DIMENSIONS="768",
    EMBEDDING_API_KEY="embedding-key",
)

AI_FIELDS = {
    "DEBUG",
    "AI_ENABLED",
    "AI_QUESTION_LOG_PROVIDER_BODIES",
    "AI_CLIENT_HMAC_KEY",
    "AI_QUESTION_PROVIDER",
    "AI_QUESTION_MODEL",
    "AI_QUESTION_ENDPOINT",
    "AI_QUESTION_API_KEY",
    "AI_QUESTION_REASONING_EFFORT",
    "AI_SCRIPTURE_REWRITE_PROVIDER",
    "AI_SCRIPTURE_REWRITE_MODEL",
    "AI_SCRIPTURE_REWRITE_ENDPOINT",
    "AI_SCRIPTURE_REWRITE_API_KEY",
    "AI_SCRIPTURE_REWRITE_REASONING_EFFORT",
    "AI_SCRIPTURE_RERANK_PROVIDER",
    "AI_SCRIPTURE_RERANK_MODEL",
    "AI_SCRIPTURE_RERANK_ENDPOINT",
    "AI_SCRIPTURE_RERANK_API_KEY",
    "AI_SCRIPTURE_RERANK_REASONING_EFFORT",
    "AI_TRANSCRIBE_PROVIDER",
    "AI_TRANSCRIBE_MODEL",
    "AI_TRANSCRIBE_ENDPOINT",
    "AI_TRANSCRIBE_API_KEY",
    "AI_TRANSCRIBE_MODEL_PATH",
    "AI_TRANSCRIBE_COMPUTE_TYPE",
    "GEMINI_API_KEY",
    "AI_OPENAI_COMPAT_ENDPOINT",
    "AI_OPENAI_COMPAT_API_KEY",
    "AI_REASONING_EFFORT",
    "AI_TRANSCRIBE_REASONING_EFFORT",
    "EMBEDDING_REASONING_EFFORT",
}
EMBEDDING_FIELDS = {
    "EMBEDDING_PROVIDER",
    "EMBEDDING_MODEL",
    "EMBEDDING_DIMENSIONS",
    "EMBEDDING_ENDPOINT",
    "EMBEDDING_API_KEY",
    "EMBEDDING_MODEL_PATH",
}


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_parse_int_unset_uses_operational_default(raw):
    assert config.parse_int("LIMIT", raw, 7) == 7


def test_parse_int_rejects_malformed_value():
    with pytest.raises(config.ConfigError, match="LIMIT"):
        config.parse_int("LIMIT", "seven", 7)


def test_parse_int_reads_and_strips_value():
    assert config.parse_int("LIMIT", " 42 ", 7) == 42


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_parse_float_unset_uses_operational_default(raw):
    assert config.parse_float("TIMEOUT", raw, 2.5) == 2.5


def test_parse_float_reads_and_strips_value():
    assert config.parse_float("TIMEOUT", " 3.5 ", 2.5) == 3.5


def test_parse_float_rejects_malformed_value():
    with pytest.raises(config.ConfigError, match="TIMEOUT"):
        config.parse_float("TIMEOUT", "soon", 2.5)


def test_config_error_remains_a_runtime_error():
    assert issubclass(config.ConfigError, RuntimeError)


@pytest.mark.parametrize("raw, expected", [("true", True), ("false", False)])
def test_parse_bool_accepts_only_explicit_values(raw, expected):
    assert config.parse_bool("AI_ENABLED", raw) is expected


@pytest.mark.parametrize("raw", [None, "", "True", "1", "yes", " false "])
def test_parse_bool_rejects_aliases_and_guesses(raw):
    with pytest.raises(config.ConfigError, match="AI_ENABLED"):
        config.parse_bool("AI_ENABLED", raw)


def test_literal_contract_matches_the_production_lists():
    assert config.ALWAYS_REQUIRED_VARS == ALWAYS_REQUIRED
    assert config.PRESENCE_REQUIRED_VARS == PRESENCE_REQUIRED
    assert config.AI_PROVIDER_VARS == AI_PROVIDERS_REQUIRED
    assert config.AI_REQUIRED_VARS == AI_MODELS_REQUIRED
    assert config.REMOVED_AI_VARS == REMOVED_AI_VARS
    assert config.FORBIDDEN_REASONING_VARS == FORBIDDEN_REASONING_VARS
    assert config.REASONING_EFFORTS == REASONING_EFFORTS
    configured_stages = (
        *config.AI_STAGE_VARS,
        config.TRANSCRIBE_STAGE_VARS,
        config.EMBEDDING_STAGE_VARS,
    )
    assert (
        tuple(
            (
                stage.stage,
                stage.provider_var,
                stage.model_var,
                stage.endpoint_var,
                stage.api_key_var,
                stage.reasoning_effort_var,
            )
            for stage in configured_stages
        )
        == STAGE_FIELDS
    )


@pytest.mark.parametrize("name", ALWAYS_REQUIRED)
@pytest.mark.parametrize("missing_kind", ["absent", "blank"])
def test_every_always_required_variable_is_reported(name, missing_kind):
    env = dict(BASE_ENV)
    if missing_kind == "absent":
        del env[name]
    else:
        env[name] = "   "
    assert name in config.missing_required_vars(env)


def test_empty_environment_reports_the_literal_base_contract():
    assert config.missing_required_vars({}) == [
        *ALWAYS_REQUIRED,
        *PRESENCE_REQUIRED,
    ]


def test_db_password_may_be_empty_but_must_be_present():
    assert config.missing_required_vars({**BASE_ENV, "DB_PASSWORD": ""}) == []
    env = dict(BASE_ENV)
    del env["DB_PASSWORD"]
    assert config.missing_required_vars(env) == ["DB_PASSWORD"]


def test_disabled_ai_with_remote_embeddings_is_complete():
    assert config.missing_required_vars(BASE_ENV) == []
    assert config.invalid_required_values(BASE_ENV) == []
    assert not config.ai_configured(BASE_ENV)


@pytest.mark.parametrize("name", AI_DISABLED_FORBIDDEN)
def test_disabled_ai_rejects_every_unused_stage_variable(name):
    value = "" if name.endswith(("ENDPOINT", "API_KEY")) else "configured"
    problems = config.invalid_required_values({**BASE_ENV, name: value})
    assert any(
        name in problem and "AI_ENABLED=false" in problem for problem in problems
    )


def test_compose_sentinel_preserves_unset_versus_empty():
    assert not config.env_var_present({"KEY": config.COMPOSE_UNSET_SENTINEL}, "KEY")
    assert config.env_var_present({"KEY": ""}, "KEY")
    assert not config.env_var_present({}, "KEY")


def test_disabled_ai_accepts_compose_key_sentinels():
    env = dict(BASE_ENV)
    for key_var in (
        "AI_QUESTION_API_KEY",
        "AI_SCRIPTURE_REWRITE_API_KEY",
        "AI_SCRIPTURE_RERANK_API_KEY",
        "AI_TRANSCRIBE_API_KEY",
    ):
        env[key_var] = config.COMPOSE_UNSET_SENTINEL
    assert config.invalid_required_values(env) == []


def test_enabled_ai_requires_hmac_and_all_four_stages():
    env = dict(BASE_ENV, AI_ENABLED="true")
    assert config.missing_required_vars(env) == [
        "AI_CLIENT_HMAC_KEY",
        *AI_PROVIDERS_REQUIRED,
        *AI_MODELS_REQUIRED,
    ]


def test_complete_mixed_environment_has_no_problems():
    assert config.missing_required_vars(AI_ENV) == []
    assert config.invalid_required_values(AI_ENV) == []


@pytest.mark.parametrize(
    "reasoning_var",
    [
        "AI_QUESTION_REASONING_EFFORT",
        "AI_SCRIPTURE_REWRITE_REASONING_EFFORT",
        "AI_SCRIPTURE_RERANK_REASONING_EFFORT",
    ],
)
def test_every_openai_compat_chat_stage_requires_reasoning_effort(reasoning_var):
    env = dict(AI_ENV)
    del env[reasoning_var]
    assert reasoning_var in config.missing_required_vars(env)
    with pytest.raises(config.ConfigError, match=reasoning_var):
        config._validate(env, [])


@pytest.mark.parametrize("value", REASONING_EFFORTS)
def test_every_reasoning_effort_literal_is_accepted(value):
    env = dict(AI_ENV, AI_QUESTION_REASONING_EFFORT=value)
    assert config.missing_required_vars(env) == []
    assert config.invalid_required_values(env) == []
    stage = config.resolve_stage(env, config.QUESTION_STAGE_VARS)
    assert stage.reasoning_effort == value


@pytest.mark.parametrize("value", ["", "minimal", "max", "NONE", " low "])
def test_reasoning_effort_rejects_missing_aliases_and_untrimmed_values(value):
    env = dict(AI_ENV, AI_QUESTION_REASONING_EFFORT=value)
    with pytest.raises(config.ConfigError, match="AI_QUESTION_REASONING_EFFORT"):
        config._validate(env, [])


@pytest.mark.parametrize(
    "reasoning_var",
    [
        "AI_QUESTION_REASONING_EFFORT",
        "AI_SCRIPTURE_REWRITE_REASONING_EFFORT",
        "AI_SCRIPTURE_RERANK_REASONING_EFFORT",
    ],
)
@pytest.mark.parametrize("value", ["", "omit", "none"])
def test_gemini_chat_rejects_reasoning_effort_even_when_blank(reasoning_var, value):
    env = {**GEMINI_AI_ENV, reasoning_var: value}
    problems = config.invalid_required_values(env)
    assert any(
        reasoning_var in problem and "openai_compat" in problem
        for problem in problems
    )


@pytest.mark.parametrize("name", FORBIDDEN_REASONING_VARS)
@pytest.mark.parametrize("value", ["", "none"])
def test_shared_audio_and_embedding_reasoning_names_are_unreadable(name, value):
    problems = config.invalid_required_values({**BASE_ENV, name: value})
    assert any(name in problem and "unsupported" in problem for problem in problems)


@pytest.mark.parametrize(
    "provider_var,key_var",
    [
        ("AI_QUESTION_PROVIDER", "AI_QUESTION_API_KEY"),
        ("AI_SCRIPTURE_REWRITE_PROVIDER", "AI_SCRIPTURE_REWRITE_API_KEY"),
        ("AI_SCRIPTURE_RERANK_PROVIDER", "AI_SCRIPTURE_RERANK_API_KEY"),
        ("AI_TRANSCRIBE_PROVIDER", "AI_TRANSCRIBE_API_KEY"),
        ("EMBEDDING_PROVIDER", "EMBEDDING_API_KEY"),
    ],
)
def test_every_openai_compat_stage_requires_its_own_key_presence(provider_var, key_var):
    env = dict(AI_ENV)
    if provider_var == "AI_TRANSCRIBE_PROVIDER":
        env.pop("AI_TRANSCRIBE_MODEL_PATH")
        env.update(
            AI_TRANSCRIBE_PROVIDER="openai_compat",
            AI_TRANSCRIBE_ENDPOINT="https://whisper.example/v1",
            AI_TRANSCRIBE_API_KEY="audio-key",
        )
    env[provider_var] = "openai_compat"
    env.pop(key_var, None)
    assert key_var in config.missing_required_vars(env)


def test_explicit_empty_remote_key_means_no_authorization():
    env = dict(AI_ENV, AI_QUESTION_API_KEY="")
    assert "AI_QUESTION_API_KEY" not in config.missing_required_vars(env)
    assert config.resolve_stage(env, config.QUESTION_STAGE_VARS).api_key == ""


def test_compose_sentinel_does_not_satisfy_remote_key_requirement():
    env = dict(AI_ENV, AI_QUESTION_API_KEY=config.COMPOSE_UNSET_SENTINEL)
    assert "AI_QUESTION_API_KEY" in config.missing_required_vars(env)


def test_openai_compat_requires_its_own_endpoint():
    env = dict(AI_ENV)
    del env["AI_SCRIPTURE_REWRITE_ENDPOINT"]
    assert "AI_SCRIPTURE_REWRITE_ENDPOINT" in config.missing_required_vars(env)


def test_no_shared_endpoint_or_key_inheritance():
    env = dict(AI_ENV)
    del env["AI_QUESTION_ENDPOINT"]
    del env["AI_QUESTION_API_KEY"]
    env["AI_OPENAI_COMPAT_ENDPOINT"] = "https://shared.example/v1"
    env["AI_OPENAI_COMPAT_API_KEY"] = "shared-key"
    missing = config.missing_required_vars(env)
    assert "AI_QUESTION_ENDPOINT" in missing
    assert "AI_QUESTION_API_KEY" in missing
    problems = config.invalid_required_values(env)
    assert any("AI_OPENAI_COMPAT_ENDPOINT" in problem for problem in problems)
    assert any("AI_OPENAI_COMPAT_API_KEY" in problem for problem in problems)


@pytest.mark.parametrize("name", REMOVED_AI_VARS)
@pytest.mark.parametrize("value", ["", "legacy-value"])
def test_removed_variables_fail_fast_even_blank(name, value):
    problems = config.invalid_required_values({**BASE_ENV, name: value})
    assert any(name in problem and "removed" in problem for problem in problems)


def test_gemini_stage_uses_only_its_stage_specific_key():
    stage = config.resolve_stage(GEMINI_AI_ENV, config.SCRIPTURE_RERANK_STAGE_VARS)
    assert stage.is_gemini
    assert stage.api_key == "rerank-key"
    assert stage.endpoint == ""
    assert stage.reasoning_effort is None


@pytest.mark.parametrize(
    "key_var",
    [
        "AI_QUESTION_API_KEY",
        "AI_SCRIPTURE_REWRITE_API_KEY",
        "AI_SCRIPTURE_RERANK_API_KEY",
        "AI_TRANSCRIBE_API_KEY",
        "EMBEDDING_API_KEY",
    ],
)
@pytest.mark.parametrize("value", ["", "   "])
def test_every_gemini_stage_requires_a_non_empty_key(key_var, value):
    env = {**GEMINI_AI_ENV, key_var: value}
    assert key_var in config.missing_required_vars(env)
    with pytest.raises(config.ConfigError) as exc:
        config._validate(env, [])
    assert key_var in str(exc.value)
    assert "non-empty" in str(exc.value)


@pytest.mark.parametrize("model_var", AI_MODELS_REQUIRED)
def test_every_enabled_ai_model_is_required(model_var):
    env = dict(AI_ENV)
    del env[model_var]
    assert model_var in config.missing_required_vars(env)


def test_gemini_rejects_even_a_blank_endpoint_variable():
    problems = config.invalid_required_values(
        {**GEMINI_AI_ENV, "AI_SCRIPTURE_RERANK_ENDPOINT": ""}
    )
    assert any("AI_SCRIPTURE_RERANK_ENDPOINT" in problem for problem in problems)


def test_local_transcription_requires_path_and_rejects_remote_fields():
    missing_path = dict(AI_ENV)
    del missing_path["AI_TRANSCRIBE_MODEL_PATH"]
    assert "AI_TRANSCRIBE_MODEL_PATH" in config.missing_required_vars(missing_path)
    for name in ("AI_TRANSCRIBE_ENDPOINT", "AI_TRANSCRIBE_API_KEY"):
        problems = config.invalid_required_values({**AI_ENV, name: ""})
        assert any(name in problem for problem in problems)


def test_remote_transcription_uses_its_own_endpoint_and_key():
    env = dict(AI_ENV)
    env.pop("AI_TRANSCRIBE_MODEL_PATH")
    env.update(
        AI_TRANSCRIBE_PROVIDER="openai_compat",
        AI_TRANSCRIBE_ENDPOINT="https://whisper.example/v1",
        AI_TRANSCRIBE_API_KEY="audio-key",
    )
    assert config.missing_required_vars(env) == []
    assert config.invalid_required_values(env) == []
    stage = config.resolve_stage(env, config.TRANSCRIBE_STAGE_VARS)
    assert (stage.endpoint, stage.api_key) == (
        "https://whisper.example/v1",
        "audio-key",
    )


def test_local_embeddings_require_path_and_no_key():
    env = {**BASE_ENV, "EMBEDDING_PROVIDER": "local"}
    env.pop("EMBEDDING_ENDPOINT")
    env.pop("EMBEDDING_API_KEY")
    assert "EMBEDDING_MODEL_PATH" in config.missing_required_vars(env)
    env["EMBEDDING_MODEL_PATH"] = "/models/bge-m3"
    assert config.missing_required_vars(env) == []
    assert config.invalid_required_values(env) == []


def test_gemini_embeddings_require_stage_key_and_no_endpoint():
    env = {
        **BASE_ENV,
        "EMBEDDING_PROVIDER": "gemini",
        "EMBEDDING_MODEL": "gemini-embedding-001",
        "EMBEDDING_DIMENSIONS": "768",
    }
    env.pop("EMBEDDING_ENDPOINT")
    env.pop("EMBEDDING_API_KEY")
    assert config.missing_required_vars(env) == ["EMBEDDING_API_KEY"]
    env["EMBEDDING_API_KEY"] = ""
    assert config.missing_required_vars(env) == ["EMBEDDING_API_KEY"]
    env["EMBEDDING_API_KEY"] = "embedding-key"
    assert config.missing_required_vars(env) == []
    assert config.invalid_required_values(env) == []


@pytest.mark.parametrize("value", ["0", "-1", "-768"])
def test_non_positive_embedding_dimensions_are_rejected(value):
    problems = config.invalid_required_values(
        {**BASE_ENV, "EMBEDDING_DIMENSIONS": value}
    )
    assert len(problems) == 1
    assert "EMBEDDING_DIMENSIONS" in problems[0]
    assert "positive" in problems[0]


def test_positive_embedding_dimensions_are_valid():
    assert config.invalid_required_values(BASE_ENV) == []


def test_non_numeric_embedding_dimensions_are_reported_once_by_import_parser(
    monkeypatch,
):
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, {**BASE_ENV, "EMBEDDING_DIMENSIONS": "many"})
    message = str(exc.value)
    assert "EMBEDDING_DIMENSIONS" in message
    assert "expected an integer" in message


@pytest.mark.parametrize(
    "provider_var,invalid_value",
    [
        ("AI_QUESTION_PROVIDER", "qwen"),
        ("AI_SCRIPTURE_REWRITE_PROVIDER", "qwen"),
        ("AI_SCRIPTURE_RERANK_PROVIDER", "qwen"),
        ("AI_TRANSCRIBE_PROVIDER", "whisper"),
        ("EMBEDDING_PROVIDER", "bge"),
    ],
)
def test_unknown_providers_are_rejected_with_the_variable_name(
    provider_var, invalid_value
):
    env = {**AI_ENV, provider_var: invalid_value}
    problems = config.invalid_required_values(env)
    assert any(
        provider_var in problem and invalid_value in problem for problem in problems
    )


@pytest.mark.parametrize("provider", ["gemini", "openai_compat"])
def test_remote_embeddings_reject_a_local_model_path(provider):
    env = {
        **BASE_ENV,
        "EMBEDDING_PROVIDER": provider,
        "EMBEDDING_MODEL_PATH": "/models/bge-m3",
    }
    if provider == "gemini":
        env.pop("EMBEDDING_ENDPOINT")
        env["EMBEDDING_API_KEY"] = "gemini-key"
    problems = config.invalid_required_values(env)
    assert any(
        "EMBEDDING_MODEL_PATH" in problem and provider in problem
        for problem in problems
    )


@pytest.mark.parametrize("name", ["EMBEDDING_ENDPOINT", "EMBEDDING_API_KEY"])
@pytest.mark.parametrize("value", ["", "configured"])
def test_local_embeddings_reject_remote_fields_even_when_blank(name, value):
    env = {
        **BASE_ENV,
        "EMBEDDING_PROVIDER": "local",
        "EMBEDDING_MODEL_PATH": "/models/bge-m3",
        name: value,
    }
    if name != "EMBEDDING_ENDPOINT":
        env.pop("EMBEDDING_ENDPOINT")
    if name != "EMBEDDING_API_KEY":
        env.pop("EMBEDDING_API_KEY")
    problems = config.invalid_required_values(env)
    assert any(name in problem and "local" in problem for problem in problems)


@pytest.mark.parametrize("provider", ["gemini", "openai_compat"])
def test_remote_transcription_rejects_a_local_model_path(provider):
    env = dict(GEMINI_AI_ENV)
    env["AI_TRANSCRIBE_PROVIDER"] = provider
    env["AI_TRANSCRIBE_MODEL_PATH"] = "/models/whisper/small"
    if provider == "openai_compat":
        env["AI_TRANSCRIBE_ENDPOINT"] = "https://whisper.example/v1"
    problems = config.invalid_required_values(env)
    assert any(
        "AI_TRANSCRIBE_MODEL_PATH" in problem and provider in problem
        for problem in problems
    )


@pytest.mark.parametrize("name", ["AI_TRANSCRIBE_ENDPOINT", "AI_TRANSCRIBE_API_KEY"])
@pytest.mark.parametrize("value", ["", "configured"])
def test_local_transcription_rejects_remote_fields_even_when_blank(name, value):
    env = {**AI_ENV, name: value}
    problems = config.invalid_required_values(env)
    assert any(name in problem and "local" in problem for problem in problems)


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://user:s3cret@llm.example/v1",
        "https://llm.example/v1?key=s3cret",
    ],
)
def test_endpoint_credentials_and_query_are_rejected_without_echoing(endpoint):
    problem = config.validate_endpoint("AI_QUESTION_ENDPOINT", endpoint)
    assert problem is not None
    assert "AI_QUESTION_ENDPOINT" in problem
    assert "s3cret" not in problem
    problems = config.invalid_required_values(
        {**AI_ENV, "AI_QUESTION_ENDPOINT": endpoint}
    )
    assert any("AI_QUESTION_ENDPOINT" in item for item in problems)
    assert "s3cret" not in repr(problems)


@pytest.mark.parametrize(
    "provider_var,endpoint_var",
    [
        ("AI_QUESTION_PROVIDER", "AI_QUESTION_ENDPOINT"),
        ("AI_SCRIPTURE_REWRITE_PROVIDER", "AI_SCRIPTURE_REWRITE_ENDPOINT"),
        ("AI_SCRIPTURE_RERANK_PROVIDER", "AI_SCRIPTURE_RERANK_ENDPOINT"),
        ("AI_TRANSCRIBE_PROVIDER", "AI_TRANSCRIBE_ENDPOINT"),
        ("EMBEDDING_PROVIDER", "EMBEDDING_ENDPOINT"),
    ],
)
def test_every_openai_compat_stage_requires_its_own_endpoint(
    provider_var, endpoint_var
):
    env = dict(AI_ENV)
    env[provider_var] = "openai_compat"
    env.pop(endpoint_var, None)
    if provider_var == "AI_TRANSCRIBE_PROVIDER":
        env.pop("AI_TRANSCRIBE_MODEL_PATH")
        env["AI_TRANSCRIBE_API_KEY"] = "audio-key"
    assert endpoint_var in config.missing_required_vars(env)


def test_local_transcription_requires_model_identity_and_path():
    without_model = dict(AI_ENV)
    del without_model["AI_TRANSCRIBE_MODEL"]
    assert "AI_TRANSCRIBE_MODEL" in config.missing_required_vars(without_model)

    without_path = dict(AI_ENV)
    del without_path["AI_TRANSCRIBE_MODEL_PATH"]
    assert "AI_TRANSCRIBE_MODEL_PATH" in config.missing_required_vars(without_path)


@pytest.mark.parametrize("value", ["int4", "float64", "INT8"])
def test_unknown_local_transcription_compute_type_is_rejected(value):
    problems = config.invalid_required_values(
        {**AI_ENV, "AI_TRANSCRIBE_COMPUTE_TYPE": value}
    )
    assert len(problems) == 1
    assert "AI_TRANSCRIBE_COMPUTE_TYPE" in problems[0]
    assert "int8" in problems[0]


def test_supported_local_transcription_compute_type_is_accepted():
    assert (
        config.invalid_required_values({**AI_ENV, "AI_TRANSCRIBE_COMPUTE_TYPE": "int8"})
        == []
    )


@pytest.mark.parametrize(
    "stage_name,stage_vars,expected",
    [
        (
            "question",
            config.QUESTION_STAGE_VARS,
            (
                "https://cerebras.example/v1",
                "question-key",
                "cerebras-model",
                "none",
            ),
        ),
        (
            "scripture_rewrite",
            config.SCRIPTURE_REWRITE_STAGE_VARS,
            ("http://qwen:8000/v1", "", "qwen3-30b", "omit"),
        ),
    ],
)
def test_openai_stage_routing_uses_only_its_own_fields(
    stage_name, stage_vars, expected
):
    stage = config.resolve_stage(AI_ENV, stage_vars)
    assert stage.stage == stage_name
    assert stage.is_openai_compat
    assert (
        stage.endpoint,
        stage.api_key,
        stage.model,
        stage.reasoning_effort,
    ) == expected


def test_local_transcription_routing_has_no_endpoint_or_key():
    stage = config.resolve_stage(AI_ENV, config.TRANSCRIBE_STAGE_VARS)
    assert stage.is_local
    assert stage.model == "small"
    assert stage.endpoint == ""
    assert stage.api_key == ""
    assert stage.reasoning_effort is None


@pytest.mark.parametrize(
    "endpoint",
    [
        "llm.example/v1",
        "ftp://llm.example/v1",
        "/v1",
        "https:///v1",
    ],
)
def test_invalid_endpoint_is_rejected(endpoint):
    problem = config.validate_endpoint("AI_QUESTION_ENDPOINT", endpoint)
    assert problem and "AI_QUESTION_ENDPOINT" in problem


def test_endpoint_credentials_are_not_echoed():
    problem = config.validate_endpoint(
        "AI_QUESTION_ENDPOINT", "https://user:s3cret@example.test/v1"
    )
    assert problem and "s3cret" not in problem


def test_validate_aggregates_missing_legacy_and_malformed_values():
    env = dict(BASE_ENV, AI_ENABLED="yes", GEMINI_API_KEY="")
    del env["EMBEDDING_DIMENSIONS"]
    with pytest.raises(config.ConfigError) as exc:
        config._validate(env, ["DB_PORT: expected an integer, got 'many'"])
    message = str(exc.value)
    for name in ("AI_ENABLED", "GEMINI_API_KEY", "EMBEDDING_DIMENSIONS", "DB_PORT"):
        assert name in message


def _reload_config(monkeypatch, env):
    for name in (
        AI_FIELDS
        | EMBEDDING_FIELDS
        | set(INTEGER_OPERATIONAL_VARS)
        | set(FLOAT_OPERATIONAL_VARS)
        | {
            "API_KEY",
            "DB_HOST",
            "DB_USER",
            "DB_PASSWORD",
            "DB_NAME",
            "DB_PORT",
        }
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return importlib.reload(config)


@pytest.fixture(autouse=True)
def _restore_config():
    yield
    importlib.reload(config)


def test_import_reads_explicit_disabled_ai(monkeypatch):
    module = _reload_config(monkeypatch, BASE_ENV)
    assert module.AI_ENABLED is False
    assert module.DEBUG is False
    assert module.AI_QUESTION_LOG_PROVIDER_BODIES is False
    assert module.QUESTION_PROVIDER.provider == ""
    assert module.EMBEDDING_STAGE.api_key == "embed-key"


def test_import_reads_explicit_question_provider_body_logging(monkeypatch):
    module = _reload_config(
        monkeypatch,
        {**AI_ENV, "AI_QUESTION_LOG_PROVIDER_BODIES": "true"},
    )
    assert module.AI_QUESTION_LOG_PROVIDER_BODIES is True


def test_import_reads_explicit_debug(monkeypatch):
    module = _reload_config(monkeypatch, {**BASE_ENV, "DEBUG": "true"})
    assert module.DEBUG is True


@pytest.mark.parametrize("raw", ["", "True", "1", "yes", " false "])
def test_import_rejects_invalid_debug(monkeypatch, raw):
    with pytest.raises(RuntimeError, match="DEBUG"):
        _reload_config(monkeypatch, {**BASE_ENV, "DEBUG": raw})


@pytest.mark.parametrize("raw", ["", "True", "1", "yes", " false "])
def test_import_rejects_invalid_question_provider_body_logging(monkeypatch, raw):
    with pytest.raises(
        RuntimeError, match="AI_QUESTION_LOG_PROVIDER_BODIES"
    ):
        _reload_config(
            monkeypatch,
            {**AI_ENV, "AI_QUESTION_LOG_PROVIDER_BODIES": raw},
        )


def test_import_rejects_question_body_logging_without_ai(monkeypatch):
    with pytest.raises(
        RuntimeError, match="there is no question-provider call"
    ):
        _reload_config(
            monkeypatch,
            {**BASE_ENV, "AI_QUESTION_LOG_PROVIDER_BODIES": "true"},
        )


def test_import_reads_each_stage_without_inheritance(monkeypatch):
    module = _reload_config(monkeypatch, AI_ENV)
    assert module.AI_ENABLED is True
    assert module.QUESTION_PROVIDER.endpoint == "https://cerebras.example/v1"
    assert module.QUESTION_PROVIDER.api_key == "question-key"
    assert module.QUESTION_PROVIDER.reasoning_effort == "none"
    assert module.SCRIPTURE_REWRITE_PROVIDER.endpoint == "http://qwen:8000/v1"
    assert module.SCRIPTURE_REWRITE_PROVIDER.api_key == ""
    assert module.SCRIPTURE_REWRITE_PROVIDER.reasoning_effort == "omit"
    assert module.SCRIPTURE_RERANK_PROVIDER.api_key == "rerank-key"
    assert module.SCRIPTURE_RERANK_PROVIDER.reasoning_effort == "high"
    assert module.TRANSCRIBE_PROVIDER.is_local


def test_import_fails_on_removed_variable_even_when_blank(monkeypatch):
    with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
        _reload_config(monkeypatch, {**BASE_ENV, "GEMINI_API_KEY": ""})


@pytest.mark.parametrize("name", INTEGER_OPERATIONAL_VARS)
def test_import_rejects_every_malformed_integer_operational_value(monkeypatch, name):
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, {**BASE_ENV, name: "many"})
    message = str(exc.value)
    assert name in message
    assert "expected an integer" in message


@pytest.mark.parametrize("name", FLOAT_OPERATIONAL_VARS)
def test_import_rejects_every_malformed_float_operational_value(monkeypatch, name):
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, {**BASE_ENV, name: "soon"})
    message = str(exc.value)
    assert name in message
    assert "expected a number" in message


def test_import_aggregates_missing_db_index_and_bad_operational_values(monkeypatch):
    env = {**BASE_ENV, "DB_PORT": "many", "AI_ENABLED": "yes"}
    del env["DB_NAME"]
    del env["DB_PASSWORD"]
    del env["EMBEDDING_MODEL"]
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, env)
    message = str(exc.value)
    for name in (
        "DB_NAME",
        "DB_PASSWORD",
        "EMBEDDING_MODEL",
        "AI_ENABLED",
        "DB_PORT",
    ):
        assert name in message
    assert "5 problems" in message


def test_import_rejects_unknown_provider_and_unused_endpoint_together(monkeypatch):
    env = {
        **AI_ENV,
        "AI_QUESTION_PROVIDER": "qwen",
        "AI_QUESTION_ENDPOINT": "https://qwen.example/v1",
    }
    with pytest.raises(RuntimeError) as exc:
        _reload_config(monkeypatch, env)
    message = str(exc.value)
    assert "AI_QUESTION_PROVIDER" in message
    assert "AI_QUESTION_ENDPOINT" in message


def test_operational_defaults_are_unchanged(monkeypatch):
    module = _reload_config(monkeypatch, AI_ENV)
    assert module.AI_QUESTION_TIMEOUT_SECONDS == 20.0
    assert module.AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS == 8.0
