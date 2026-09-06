"""The arithmetic of the sampling report (ClickUp 86cbejvra).

`evaluation/question_v6a_report.py` is the only place the four configurations
are compared, so its two loop measures are pinned on a synthetic directory
where the right answer is obvious, and the refusal to read a partial run is
pinned too — a half-finished run is a biased subset, not a smaller one.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

EVALUATION = Path(__file__).resolve().parents[1] / "evaluation"
sys.path.insert(0, str(EVALUATION))
_spec = importlib.util.spec_from_file_location(
    "v6a_report", EVALUATION / "question_v6a_report.py"
)
report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report)


def _row(identifier, sample, step, text, language="ru", person=("Я рада",)):
    return {
        "id": identifier, "sample": sample, "step": step, "text": text,
        "language": language, "latency_ms": 100, "error": None,
        "automatic_violations": [], "person_words": list(person),
        "input": {"topic": "Тема", "stage": "next", "messages": []},
    }


def _directory(tmp_path, rows, complete=True, meta_extra=None):
    out = tmp_path / "config"
    out.mkdir()
    spec = {
        "temperature": 1.0, "sampling_overrides": {"temperature": 1.0},
        "inputs": {"inputs": [{"id": "series-a"}]},
    }
    (out / "protocol.json").write_text(json.dumps(spec), encoding="utf-8")
    (out / "qwen.meta.json").write_text(json.dumps({
        "complete": complete, "model_config": {"model": "m"},
        "protocol_hash": report.digest(spec), "written": len(rows),
        **(meta_extra or {}),
    }), encoding="utf-8")
    (out / "qwen.jsonl").write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )
    return tmp_path


def test_verbatim_repeats_and_step_one_collisions_are_counted_per_series():
    rows = [
        # One series repeating its own step 1 twice: two verbatim repeats.
        _row("series-a", 1, 1, "Что для тебя важно?"),
        _row("series-a", 1, 2, "Что для тебя важно?"),
        _row("series-a", 1, 3, "Совсем другое?"),
        _row("series-a", 1, 4, "Что для тебя важно?"),
        # Another sample of the same input opening identically: one collision.
        # Its own steps never repeat, so it adds no verbatim repeat.
        _row("series-a", 2, 1, "Что для тебя важно?"),
        _row("series-a", 2, 2, "Ещё одно?"),
    ]
    data = report.measure(rows)
    assert data["verbatim"] == 2 and data["verbatim_by_input"] == {"series-a": 2}
    assert data["collisions"] == 1 and data["collision_inputs"] == 1
    assert data["distinct"] == 3 and data["rows"] == 6


def test_gender_menus_and_tails_come_from_the_production_filters():
    rows = [
        # «Я рада» in the person's words, «должен» in the question.
        _row("x", 1, 1, "Что ты должен сделать завтра?"),
        _row("x", 2, 1, "Что сейчас важнее для тебя, работа или время с семьёй?"),
        _row("x", 3, 1, "Что для тебя главное — как ты это узнаешь?"),
        # English rows carry no gender at all.
        _row("y", 1, 1, "What matters most to you now?", language="en",
             person=("I keep putting it off",)),
    ]
    data = report.measure(rows)
    assert data["mismatch"] == 1 and data["imposed"] == 0
    assert data["menus"] == 1 and data["tails"] == 1


def test_a_partial_run_is_refused_rather_than_compared(tmp_path):
    root = _directory(tmp_path, [_row("series-a", 1, 1, "Вопрос?")], complete=False)
    with pytest.raises(ValueError, match="not complete"):
        report.load_config(root / "config")


def test_a_run_of_another_protocol_is_refused(tmp_path):
    root = _directory(tmp_path, [_row("series-a", 1, 1, "Вопрос?")],
                      meta_extra={"protocol_hash": "not the hash of this spec"})
    with pytest.raises(ValueError, match="another protocol"):
        report.load_config(root / "config")


def test_a_truncated_artifact_is_refused(tmp_path):
    """The rows on disk must be the rows the run says it wrote."""
    root = _directory(tmp_path, [_row("series-a", 1, 1, "Вопрос?")],
                      meta_extra={"written": 99})
    with pytest.raises(ValueError, match="against 99"):
        report.load_config(root / "config")


def test_the_two_digest_implementations_are_the_same_function():
    """This reader re-types `compare_question_models.digest`; pin them together.

    On the real protocol files, so a drift would be caught on the data the
    report is actually built from and not only on a constructed dict.
    """
    spec = importlib.util.spec_from_file_location(
        "comparison_for_digest", EVALUATION / "compare_question_models.py"
    )
    comparison = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(comparison)
    protocols = sorted(
        (EVALUATION / "bench_data" / "question_v6a_sampling").glob("*/protocol.json")
    )
    assert protocols, "the measured configurations are part of the branch"
    for path in protocols:
        value = json.loads(path.read_text(encoding="utf-8"))
        assert report.digest(value) == comparison.digest(value)


def test_the_measured_configurations_all_load():
    """Every directory of 86cbejvra passes the three refusals above."""
    root = EVALUATION / "bench_data" / "question_v6a_sampling"
    directories = sorted(
        path for path in root.iterdir()
        if path.is_dir() and (path / "protocol.json").exists()
    )
    assert len(directories) == 6
    for path in directories:
        config = report.load_config(path)
        assert config["runs"]["qwen"][0]["written"] == 75


def test_the_report_names_the_sampling_it_measured(tmp_path):
    root = _directory(tmp_path, [
        _row("series-a", 1, 1, "Вопрос?"), _row("series-a", 1, 2, "Вопрос?"),
    ])
    assert report.main(["--root", str(root), "--reference", ""]) == 0
    text = (root / "REPORT.md").read_text(encoding="utf-8")
    assert "temperature=1.0" in text
    assert "--temperature 1.0" in text  # the reproduction command
    assert "| `config` |" in text
