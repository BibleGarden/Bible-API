import importlib.util
import json
from pathlib import Path

import pytest


def load_tool():
    path = Path(__file__).parents[1] / "evaluation" / "prepare_runpod_judging.py"
    spec = importlib.util.spec_from_file_location("prepare_runpod_judging", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def synthetic_rows(cases, label):
    rows = []
    for case in cases:
        for sample in (1, 2, 3):
            for step in range(1, int(case.get("replacements", 1)) + 1):
                rows.append(
                    {
                        "id": case["id"],
                        "sample": sample,
                        "step": step,
                        "series_steps": int(case.get("replacements", 1)),
                        "language": case["language"],
                        "category": case["category"],
                        "text": f"{label} answer {case['id']} {sample}.{step}",
                        "error": None,
                        "skipped_questions": (
                            [f"{label} skipped {number}" for number in range(1, step)]
                            if case.get("kind") == "series"
                            else []
                        ),
                    }
                )
    return rows


def test_prepare_builds_complete_blind_legacy_manifest(tmp_path):
    tool = load_tool()
    repo = Path(__file__).parents[1]
    cases_source = (
        repo / "evaluation" / "bench_data" / "runpod_86cbh0p8t" / "quality_cases.json"
    )
    cases_payload = json.loads(cases_source.read_text())
    cases = cases_payload["inputs"]
    qwen = tmp_path / "qwen.jsonl"
    gemma = tmp_path / "gemma.jsonl"
    fixture = tmp_path / "cases.json"
    out = tmp_path / "out"
    qwen_rows = synthetic_rows(cases, "alpha")
    gemma_rows = synthetic_rows(cases, "beta")
    write_jsonl(qwen, qwen_rows)
    write_jsonl(gemma, gemma_rows)
    fixture.write_text(json.dumps(cases_payload))

    tool.prepare(qwen, gemma, fixture, out)

    canonical = tool.read_jsonl(out / "canonical_pairs.jsonl")
    batches = [tool.read_jsonl(out / f"judge_batch_{index}.jsonl") for index in (1, 2)]
    protocol = json.loads((out / "protocol.json").read_text())
    assert len(canonical) == 114
    assert sum(row["control"] for row in canonical) == 10
    assert {row["orientation"] for row in canonical} == {"ab", "ba"}
    assert [len(batch) for batch in batches] == [57, 57]
    assert all(
        set(row) == {"judgement_id", "prompt", "response_schema"}
        for batch in batches
        for row in batch
    )
    assert "qwen" not in "".join(map(str, batches)).casefold()
    assert "gemma" not in "".join(map(str, batches)).casefold()
    assert protocol["astra"]["required_model"] == "gpt-6-astra"

    write_jsonl(gemma, gemma_rows[:-1])
    with pytest.raises(SystemExit, match="refusing intersection"):
        tool.prepare(qwen, gemma, fixture, tmp_path / "mismatch")
