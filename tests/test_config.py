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
    GEMINI_API_KEY="gemini-key",
    AI_QUESTION_MODEL="gemini-3.5-flash-lite",
    AI_TRANSCRIBE_MODEL="gemini-3.5-flash-lite",
    AI_SCRIPTURE_REWRITE_MODEL="gemini-3.7-flash",
    AI_SCRIPTURE_RERANK_MODEL="gemini-3.5-flash-lite",
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


def test_gemini_key_makes_every_provider_model_required():
    env = dict(BASE_ENV, GEMINI_API_KEY="gemini-key")
    assert config.missing_required_vars(env) == AI_REQUIRED


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
                 "GEMINI_API_KEY", "AI_SCRIPTURE_REWRITE_API_KEY", "DB_PORT"):
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
