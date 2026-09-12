from __future__ import annotations

import importlib.util
import json
from argparse import Namespace
from pathlib import Path

import pytest


def load_tool():
    path = Path(__file__).parents[1] / "evaluation" / "judge_fable.py"
    spec = importlib.util.spec_from_file_location("judge_fable", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tool = load_tool()


def visible(judgement_id):
    return {
        "judgement_id": judgement_id,
        "prompt": "Choose A or B",
        "response_schema": {},
    }


def canonical(judgement_id, pair_id, orientation):
    return {
        "judgement_id": judgement_id,
        "pair_id": pair_id,
        "orientation": orientation,
    }


def response(ids, model=tool.MODEL):
    return {
        "modelUsage": {model: {"inputTokens": 10, "outputTokens": 2}},
        "usage": {
            "input_tokens": 10,
            "output_tokens": 2,
            "cache_creation_input_tokens": None,
            "cache_read_input_tokens": None,
            "output_tokens_details": {"thinking_tokens": 0},
        },
        "total_cost_usd": 0.25,
        "structured_output": {
            "verdicts": [
                {"judgement_id": judgement_id, "verdict": "A", "reason": "grounded"}
                for judgement_id in ids
            ]
        },
    }


def test_batches_keep_opposite_orientations_in_fresh_sessions():
    batches = [[visible("J1")], [visible("J2")]]
    rows = [canonical("J1", "P1", "ab"), canonical("J2", "P1", "ba")]
    tool.validate_batches(batches, rows)
    with pytest.raises(ValueError, match="different batches"):
        tool.validate_batches([[visible("J1"), visible("J2")]], rows)


def test_response_requires_complete_unique_ids_and_cost():
    verdicts, cost = tool.validate_response(response(["J1", "J2"]), ["J1", "J2"])
    assert [row["judgement_id"] for row in verdicts] == ["J1", "J2"]
    assert cost == 0.25
    duplicate = response(["J1", "J1"])
    with pytest.raises(ValueError, match="duplicated"):
        tool.validate_response(duplicate, ["J1", "J2"])
    free = response(["J1"])
    free["total_cost_usd"] = 0
    assert tool.validate_response(free, ["J1"])[1] == 0
    missing_cost = response(["J1"])
    del missing_cost["total_cost_usd"]
    with pytest.raises(ValueError, match="total_cost_usd"):
        tool.validate_response(missing_cost, ["J1"])


def test_call_uses_restricted_fresh_session_and_preserves_raw_output(
    tmp_path, monkeypatch
):
    payload = response(["J1"])
    payload["type"] = "result"
    stream = "\n".join(
        [
            json.dumps(
                {
                    "type": "assistant",
                    "message": {
                        "model": tool.MODEL,
                        "content": [{"type": "text", "text": "done"}],
                    },
                }
            ),
            json.dumps(payload),
        ]
    )

    def fake_run(command, **kwargs):
        assert command[:4] == ["claude", "-p", "--model", tool.MODEL]
        assert command[-1] == "none"
        assert "J1" in kwargs["input"]
        assert Path(kwargs["cwd"]).parent == Path("/tmp")
        assert not (Path(kwargs["cwd"]) / "CLAUDE.md").exists()
        assert "--no-session-persistence" in command
        assert "--restricted" in command
        assert command[command.index("--tools") + 1] == ""
        return type(
            "Completed",
            (),
            {"returncode": 0, "stdout": stream, "stderr": "trace"},
        )()

    monkeypatch.setattr(tool.subprocess, "run", fake_run)
    result, cost = tool.call_fable([visible("J1")], "claude", 10, 1, tmp_path, "proof")
    assert result[0]["verdict"] == "A"
    assert cost == 0.25
    assert (
        json.loads((tmp_path / "proof.stdout.jsonl").read_text().splitlines()[-1])
        == payload
    )
    assert (tmp_path / "proof.stderr.log").read_text() == "trace"


def test_timeout_preserves_diagnostics(tmp_path, monkeypatch):
    def timeout(*args, **kwargs):
        raise tool.subprocess.TimeoutExpired(
            args[0], 10, output="partial", stderr="slow"
        )

    monkeypatch.setattr(tool.subprocess, "run", timeout)
    with pytest.raises(RuntimeError, match="timed out"):
        tool.call_fable([visible("J1")], "claude", 10, 1, tmp_path, "proof")
    assert (tmp_path / "proof.stdout.jsonl").read_text() == "partial"
    assert (tmp_path / "proof.stderr.log").read_text() == "slow"
    assert (tmp_path / "proof.exit.txt").read_text() == "timeout\n"


def test_call_rejects_non_fable_assistant_message(tmp_path, monkeypatch):
    payload = response(["J1"])
    payload["type"] = "result"
    stream = "\n".join(
        [
            json.dumps(
                {
                    "type": "assistant",
                    "message": {
                        "model": "claude-opus-5",
                        "content": [{"type": "text", "text": "done"}],
                    },
                }
            ),
            json.dumps(payload),
        ]
    )

    def fake_run(*args, **kwargs):
        return type(
            "Completed", (), {"returncode": 0, "stdout": stream, "stderr": ""}
        )()

    monkeypatch.setattr(tool.subprocess, "run", fake_run)
    with pytest.raises(ValueError, match="assistant model mismatch"):
        tool.call_fable([visible("J1")], "claude", 10, 1, tmp_path, "proof")


def test_run_subtracts_each_call_from_one_total_budget(tmp_path, monkeypatch):
    batch_one = [visible("J1"), visible("J3")]
    batch_two = [visible("J2"), visible("J4")]
    canonical_rows = [
        canonical("J1", "P1", "ab"),
        canonical("J2", "P1", "ba"),
        canonical("J3", "P2", "ab"),
        canonical("J4", "P2", "ba"),
    ]
    for row in canonical_rows:
        row.update(
            {
                "id": row["pair_id"],
                "sample": 2,
                "step": 1,
                "control": False,
                "left_source": "a#2",
                "right_source": "b#2",
            }
        )
    paths = [tmp_path / "batch1.jsonl", tmp_path / "batch2.jsonl"]
    canonical_path = tmp_path / "canonical.jsonl"
    for path, rows in zip(paths, (batch_one, batch_two), strict=True):
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    canonical_path.write_text("".join(json.dumps(row) + "\n" for row in canonical_rows))
    budgets = []

    def fake_call(rows, _binary, _timeout, budget, _raw_dir, _tag):
        budgets.append(budget)
        return (
            [
                {"judgement_id": row["judgement_id"], "verdict": "A", "reason": "ok"}
                for row in rows
            ],
            2.0,
        )

    monkeypatch.setattr(tool, "call_fable", fake_call)
    args = Namespace(
        batch=paths,
        canonical=canonical_path,
        output=tmp_path / "verdicts.jsonl",
        meta=tmp_path / "meta.json",
        claude_binary="claude",
        timeout=10,
        max_budget_usd=5.0,
        unattributed_route_proof_response=None,
    )
    with pytest.raises(RuntimeError, match="exceeded"):
        tool.run(args)
    assert budgets == [5.0, 3.0, 1.0]
    meta = json.loads(args.meta.read_text())
    assert meta["complete"] is False
    assert meta["estimated_cost_usd"] == 6.0
