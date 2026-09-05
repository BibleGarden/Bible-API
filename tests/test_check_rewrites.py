"""Tests for evaluation/check_rewrites.py (ClickUp 86cbegg36).

Narrow on purpose, like the gen_rewrites tests: what is guarded here are the
two judgements that would rot silently and quietly change a verdict in a
report — that the language check is taken PER SCENARIO (a single
scripture-register variant is often undecidable between ru and uk, and
counting those as violations would fail the approved production
configuration), and that a wrong language IS a failure while an undecidable
one is not.

No network, no database, no model: the artifact is a dict written to a temp
file. The module is loaded by path because `evaluation/` is deliberately not
on pytest's pythonpath.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

EVALUATION = Path(__file__).resolve().parents[1] / "evaluation"
TOOL = EVALUATION / "check_rewrites.py"

pytestmark = pytest.mark.skipif(
    not TOOL.exists(), reason="evaluation/ is not present in this container copy"
)


@pytest.fixture(scope="module")
def tool():
    if str(EVALUATION) not in sys.path:
        sys.path.insert(0, str(EVALUATION))
    spec = importlib.util.spec_from_file_location("check_rewrites", TOOL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _artifact(tmp_path: Path, records: list[dict]) -> Path:
    path = tmp_path / "artifact.json"
    path.write_text(
        json.dumps({"meta": {"model": "m"}, "scenarios": records},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def test_undecidable_cyrillic_is_reported_but_is_not_a_violation(tool, tmp_path):
    # Real Synodal-register wording with none of ы/э/ъ/ё and no everyday
    # function word — the detector cannot tell ru from uk on one such line.
    from check_questions import detect_language

    variant = "Я благодарю Тебя, Господи, всем сердцем моим, пред лицом богов пою"
    assert detect_language(variant) == "cyr?", "the premise of this test"

    path = _artifact(tmp_path, [
        {"id": "ru-001", "language": "ru", "error": None, "variants": [variant]},
    ])
    result = tool.check(path)
    assert result["wrong_language"] == []
    assert result["scenarios_language_bad"] == []
    assert result["undetermined_language"] == ["ru-001#1 (cyr?)"]
    # One variant is not enough to decide the scenario either — and an
    # undecided scenario is reported, never counted as a violation.
    assert result["scenarios_language_undetermined"] == ["ru-001 (cyr?)"]
    assert tool.report(result)


def test_a_positively_wrong_language_fails(tool, tmp_path):
    path = _artifact(tmp_path, [{
        "id": "ru-001", "language": "ru", "error": None,
        "variants": ["Нехай Бог помилує нас і поблагословить нас"],
    }])
    result = tool.check(path)
    assert result["scenarios_language_bad"]
    assert not tool.report(result)


def test_a_failed_record_is_counted_and_leaves_the_denominator(tool, tmp_path):
    path = _artifact(tmp_path, [
        {"id": "ru-001", "language": "ru", "error": None,
         "variants": ["Я благодарю Тебя, Господи, всем сердцем моим, пред лицом богов пою"]},
        {"id": "uk-001", "language": "uk", "error": "app: rewrite failed",
         "variants": []},
    ])
    result = tool.check(path)
    assert result["json_failures"] == ["uk-001"]
    assert result["scenarios_answered"] == 1
    assert not tool.report(result)


def test_the_tool_runs_with_no_deployment_environment(tmp_path):
    """The documented invocation is `python check_rewrites.py <artifact>`.

    Reaching the few-shot examples imports `app/query_rewrite.py` and with it
    `app/config.py`, which fails fast on a missing deployment variable
    (ADR 0008). A file-reading checker must not require a configured
    deployment to run, so the module stubs what `config` needs — the same
    block, and the same reason, as `gen_rewrites.py`. Asserted in a
    subprocess with a scrubbed environment, because this process is already
    configured by `conftest`.
    """
    path = _artifact(tmp_path, [{
        "id": "ru-001", "language": "ru", "error": None,
        "variants": ["Я благодарю Тебя, Господи, всем сердцем моим"],
    }])
    scrubbed = {
        key: value for key, value in os.environ.items()
        if key in ("PATH", "HOME", "LANG", "LC_ALL", "PYTHONHASHSEED")
    }
    done = subprocess.run(
        [sys.executable, str(TOOL), str(path)],
        capture_output=True, text=True, env=scrubbed, timeout=120,
    )
    assert done.returncode == 0, done.stderr
    assert "=> OK" in done.stdout


def test_copy_classes_are_exact_prefix_and_near(tool):
    examples = {"Господь даёт мудрость, из уст Его — знание и разум"}
    assert tool.copy_class(
        "Господь  даёт мудрость, из уст Его — знание и разум", examples
    ) == "exact"
    assert tool.copy_class(
        "Господь даёт мудрость, из уст Его — знание, и ещё много слов сверху",
        examples,
    ) == "prefix"
    assert tool.copy_class(
        "Господь дает мудрость, из уст Его знание и разум!", examples
    ) == "near"
    assert tool.copy_class("Не убойся, ибо Я с тобою", examples) == ""
