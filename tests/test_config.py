"""Focused tests for the explicit AI configuration contract (ADR 0019)."""

import importlib

import pytest

import config


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
    "AI_SCRIPTURE_REWRITE_PROVIDER": "openai_compat",
    "AI_SCRIPTURE_REWRITE_MODEL": "qwen3-30b",
    "AI_SCRIPTURE_REWRITE_ENDPOINT": "http://qwen:8000/v1",
    "AI_SCRIPTURE_REWRITE_API_KEY": "",
    "AI_SCRIPTURE_RERANK_PROVIDER": "gemini",
    "AI_SCRIPTURE_RERANK_MODEL": "gemini-3.5-flash-lite",
    "AI_SCRIPTURE_RERANK_API_KEY": "rerank-key",
    "AI_TRANSCRIBE_PROVIDER": "local",
    "AI_TRANSCRIBE_MODEL": "small",
    "AI_TRANSCRIBE_MODEL_PATH": "/models/whisper/small",
}

AI_FIELDS = {
    "AI_ENABLED", "AI_CLIENT_HMAC_KEY", "AI_QUESTION_PROVIDER",
    "AI_QUESTION_MODEL", "AI_QUESTION_ENDPOINT", "AI_QUESTION_API_KEY",
    "AI_SCRIPTURE_REWRITE_PROVIDER", "AI_SCRIPTURE_REWRITE_MODEL",
    "AI_SCRIPTURE_REWRITE_ENDPOINT", "AI_SCRIPTURE_REWRITE_API_KEY",
    "AI_SCRIPTURE_RERANK_PROVIDER", "AI_SCRIPTURE_RERANK_MODEL",
    "AI_SCRIPTURE_RERANK_ENDPOINT", "AI_SCRIPTURE_RERANK_API_KEY",
    "AI_TRANSCRIBE_PROVIDER", "AI_TRANSCRIBE_MODEL", "AI_TRANSCRIBE_ENDPOINT",
    "AI_TRANSCRIBE_API_KEY", "AI_TRANSCRIBE_MODEL_PATH", "GEMINI_API_KEY",
    "AI_OPENAI_COMPAT_ENDPOINT", "AI_OPENAI_COMPAT_API_KEY",
}
EMBEDDING_FIELDS = {
    "EMBEDDING_PROVIDER", "EMBEDDING_MODEL", "EMBEDDING_DIMENSIONS",
    "EMBEDDING_ENDPOINT", "EMBEDDING_API_KEY", "EMBEDDING_MODEL_PATH",
}


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_parse_int_unset_uses_operational_default(raw):
    assert config.parse_int("LIMIT", raw, 7) == 7


def test_parse_int_rejects_malformed_value():
    with pytest.raises(config.ConfigError, match="LIMIT"):
        config.parse_int("LIMIT", "seven", 7)


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_parse_float_unset_uses_operational_default(raw):
    assert config.parse_float("TIMEOUT", raw, 2.5) == 2.5


@pytest.mark.parametrize("raw, expected", [("true", True), ("false", False)])
def test_parse_bool_accepts_only_explicit_values(raw, expected):
    assert config.parse_bool("AI_ENABLED", raw) is expected


@pytest.mark.parametrize("raw", [None, "", "True", "1", "yes", " false "])
def test_parse_bool_rejects_aliases_and_guesses(raw):
    with pytest.raises(config.ConfigError, match="AI_ENABLED"):
        config.parse_bool("AI_ENABLED", raw)


def test_ai_enabled_is_always_required():
    env = dict(BASE_ENV)
    del env["AI_ENABLED"]
    assert "AI_ENABLED" in config.missing_required_vars(env)


def test_disabled_ai_with_remote_embeddings_is_complete():
    assert config.missing_required_vars(BASE_ENV) == []
    assert config.invalid_required_values(BASE_ENV) == []
    assert not config.ai_configured(BASE_ENV)


@pytest.mark.parametrize("name,value", [
    ("AI_QUESTION_PROVIDER", "gemini"), ("AI_QUESTION_MODEL", "m"),
    ("AI_QUESTION_ENDPOINT", ""), ("AI_QUESTION_API_KEY", ""),
    ("AI_CLIENT_HMAC_KEY", "hmac"),
])
def test_disabled_ai_rejects_unused_stage_configuration(name, value):
    problems = config.invalid_required_values({**BASE_ENV, name: value})
    assert any(name in problem and "AI_ENABLED=false" in problem for problem in problems)


def test_compose_sentinel_preserves_unset_versus_empty():
    assert not config.env_var_present(
        {"KEY": config.COMPOSE_UNSET_SENTINEL}, "KEY"
    )
    assert config.env_var_present({"KEY": ""}, "KEY")
    assert not config.env_var_present({}, "KEY")


def test_disabled_ai_accepts_compose_key_sentinels():
    env = dict(BASE_ENV)
    for stage in (*config.AI_STAGE_VARS, config.TRANSCRIBE_STAGE_VARS):
        env[stage.api_key_var] = config.COMPOSE_UNSET_SENTINEL
    assert config.invalid_required_values(env) == []


def test_enabled_ai_requires_hmac_and_all_four_stages():
    env = dict(BASE_ENV, AI_ENABLED="true")
    missing = config.missing_required_vars(env)
    assert "AI_CLIENT_HMAC_KEY" in missing
    assert set(config.AI_PROVIDER_VARS).issubset(missing)
    assert set(config.AI_REQUIRED_VARS).issubset(missing)


def test_complete_mixed_environment_has_no_problems():
    assert config.missing_required_vars(AI_ENV) == []
    assert config.invalid_required_values(AI_ENV) == []


def test_every_remote_stage_requires_its_own_key_presence():
    for stage in (*config.AI_STAGE_VARS, config.EMBEDDING_STAGE_VARS):
        env = dict(AI_ENV)
        env.pop(stage.api_key_var, None)
        assert stage.api_key_var in config.missing_required_vars(env)


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


@pytest.mark.parametrize("name", config.REMOVED_AI_VARS)
@pytest.mark.parametrize("value", ["", "legacy-value"])
def test_removed_variables_fail_fast_even_blank(name, value):
    problems = config.invalid_required_values({**BASE_ENV, name: value})
    assert any(name in problem and "removed" in problem for problem in problems)


def test_gemini_stage_uses_only_its_stage_specific_key():
    stage = config.resolve_stage(AI_ENV, config.SCRIPTURE_RERANK_STAGE_VARS)
    assert stage.is_gemini
    assert stage.api_key == "rerank-key"
    assert stage.endpoint == ""


def test_gemini_rejects_even_a_blank_endpoint_variable():
    problems = config.invalid_required_values(
        {**AI_ENV, "AI_SCRIPTURE_RERANK_ENDPOINT": ""}
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
        "https://whisper.example/v1", "audio-key",
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
    assert config.missing_required_vars(env) == []
    assert config.invalid_required_values(env) == []


@pytest.mark.parametrize("endpoint", [
    "llm.example/v1", "ftp://llm.example/v1", "/v1", "https:///v1",
])
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
    for name in AI_FIELDS | EMBEDDING_FIELDS | {
        "API_KEY", "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "DB_PORT",
        "AI_QUESTION_TIMEOUT_SECONDS", "AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS",
    }:
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
    assert module.QUESTION_PROVIDER.provider == ""
    assert module.EMBEDDING_STAGE.api_key == "embed-key"


def test_import_reads_each_stage_without_inheritance(monkeypatch):
    module = _reload_config(monkeypatch, AI_ENV)
    assert module.AI_ENABLED is True
    assert module.QUESTION_PROVIDER.endpoint == "https://cerebras.example/v1"
    assert module.QUESTION_PROVIDER.api_key == "question-key"
    assert module.SCRIPTURE_REWRITE_PROVIDER.endpoint == "http://qwen:8000/v1"
    assert module.SCRIPTURE_REWRITE_PROVIDER.api_key == ""
    assert module.SCRIPTURE_RERANK_PROVIDER.api_key == "rerank-key"
    assert module.TRANSCRIBE_PROVIDER.is_local


def test_import_fails_on_removed_variable_even_when_blank(monkeypatch):
    with pytest.raises(RuntimeError, match="GEMINI_API_KEY"):
        _reload_config(monkeypatch, {**BASE_ENV, "GEMINI_API_KEY": ""})


def test_operational_defaults_are_unchanged(monkeypatch):
    module = _reload_config(monkeypatch, AI_ENV)
    assert module.AI_QUESTION_TIMEOUT_SECONDS == 20.0
    assert module.AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS == 8.0
