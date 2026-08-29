"""Tests for the key resolution of evaluation/retrieval_benchmark.py.

The benchmark is not part of the service, but it decides which Gemini key it
bills — and one of those keys is paid (RETRIEVAL_REWRITE_API_KEY, ADR 0004).
The module is loaded by path because `evaluation/` is deliberately not on
`pythonpath` (pytest.ini exposes `app` only); its import is side-effect free
apart from putting `app/` on sys.path.
"""

import importlib.util
import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("API_KEY", "test-api-key")

BENCHMARK = Path(__file__).resolve().parents[1] / "evaluation" / "retrieval_benchmark.py"


def _load_benchmark():
    spec = importlib.util.spec_from_file_location("retrieval_benchmark", BENCHMARK)
    module = importlib.util.module_from_spec(spec)
    # Registered before execution: the module defines dataclasses, and
    # `dataclasses` resolves annotations through sys.modules[cls.__module__].
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def benchmark():
    return _load_benchmark()


def test_key_comes_from_the_environment_first(benchmark, monkeypatch, tmp_path):
    monkeypatch.setenv("SOME_KEY", "from-env")
    env_file = tmp_path / ".env"
    env_file.write_text("SOME_KEY=from-file\n")
    assert benchmark._key_from_env("SOME_KEY", env_file) == "from-env"


def test_key_falls_back_to_the_env_file(benchmark, monkeypatch, tmp_path):
    monkeypatch.delenv("SOME_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("OTHER=x\nSOME_KEY=from-file\n")
    assert benchmark._key_from_env("SOME_KEY", env_file) == "from-file"


def test_missing_env_file_is_not_an_error(benchmark, monkeypatch, tmp_path):
    # Regression: `.env` is absent inside the bible-api image, so reading it
    # unconditionally crashed a documented benchmark run instead of falling
    # back to the shared key.
    monkeypatch.delenv("SOME_KEY", raising=False)
    assert benchmark._key_from_env("SOME_KEY", tmp_path / "nope.env") == ""


def test_rewrite_key_falls_back_to_the_shared_key(benchmark, monkeypatch, tmp_path):
    monkeypatch.setattr(benchmark, "ENV_FILE", tmp_path / "nope.env")
    monkeypatch.delenv("RETRIEVAL_REWRITE_API_KEY", raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "shared-key")
    assert benchmark.require_rewrite_api_key() == "shared-key"


def test_rewrite_key_is_preferred_when_set(benchmark, monkeypatch, tmp_path):
    monkeypatch.setattr(benchmark, "ENV_FILE", tmp_path / "nope.env")
    monkeypatch.setenv("RETRIEVAL_REWRITE_API_KEY", "paid-key")
    monkeypatch.setenv("GEMINI_API_KEY", "shared-key")
    assert benchmark.require_rewrite_api_key() == "paid-key"


@pytest.mark.parametrize("value", ["", "   "])
def test_blank_rewrite_key_falls_back_like_an_unset_one(
    benchmark, monkeypatch, tmp_path, value
):
    monkeypatch.setattr(benchmark, "ENV_FILE", tmp_path / "nope.env")
    monkeypatch.setenv("RETRIEVAL_REWRITE_API_KEY", value)
    monkeypatch.setenv("GEMINI_API_KEY", "shared-key")
    assert benchmark.require_rewrite_api_key() == "shared-key"


def test_no_key_at_all_exits_with_a_message(benchmark, monkeypatch, tmp_path):
    monkeypatch.setattr(benchmark, "ENV_FILE", tmp_path / "nope.env")
    monkeypatch.delenv("RETRIEVAL_REWRITE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(SystemExit) as exc:
        benchmark.require_rewrite_api_key()
    assert "GEMINI_API_KEY" in str(exc.value)
