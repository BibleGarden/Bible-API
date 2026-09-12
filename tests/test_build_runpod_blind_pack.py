import importlib.util
import json
from pathlib import Path

import pytest


def load_tool():
    path = Path(__file__).parents[1] / "evaluation" / "build_runpod_blind_pack.py"
    spec = importlib.util.spec_from_file_location("build_runpod_blind_pack", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def test_pack_excludes_warmup_and_hides_model_identity(tmp_path):
    tool = load_tool()
    cases = {
        "inputs": [
            {
                "id": f"case-{index:02d}",
                "language": "en",
                "stage": "first",
                "topic": f"Topic {index}",
                "messages": [],
            }
            for index in range(20)
        ]
    }
    rows = [
        {
            "id": case["id"],
            "sample": sample,
            "text": f"answer {sample}",
            "error": None,
        }
        for case in cases["inputs"]
        for sample in (1, 2, 3)
    ]
    qwen_rows = list(rows)
    gemma_rows = list(rows)
    qwen = tmp_path / "qwen.jsonl"
    gemma = tmp_path / "gemma.jsonl"
    fixture = tmp_path / "cases.json"
    output = tmp_path / "review.html"
    mapping = tmp_path / "mapping.json"
    write_jsonl(qwen, qwen_rows)
    write_jsonl(gemma, gemma_rows)
    fixture.write_text(json.dumps(cases))

    tool.build_pack(qwen, gemma, fixture, output, mapping, "test seed")

    review = output.read_text()
    key = json.loads(mapping.read_text())
    assert review.count("<section data-review-id=") == 40
    assert "answer 1" not in review
    assert "qwen" not in review.casefold()
    assert "gemma" not in review.casefold()
    assert len(key["rows"]) == 40
    assert {row["sample"] for row in key["rows"]} == {2, 3}
    assert {row["A"] for row in key["rows"]} == {"qwen", "gemma"}

    second_output = tmp_path / "review-second.html"
    second_mapping = tmp_path / "mapping-second.json"
    tool.build_pack(qwen, gemma, fixture, second_output, second_mapping, "test seed")
    assert second_output.read_bytes() == output.read_bytes()
    assert second_mapping.read_bytes() == mapping.read_bytes()

    gemma_rows[-1] = {**gemma_rows[-1], "text": "I am Gemma"}
    write_jsonl(gemma, gemma_rows)
    with pytest.raises(SystemExit, match="model identity leaked"):
        tool.build_pack(qwen, gemma, fixture, output, mapping, "test seed")
