#!/usr/bin/env python3
"""Prepare blind Astra/Fable manifests for Runpod experiment 86cbh0p8t."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import random
from pathlib import Path

HERE = Path(__file__).resolve().parent
JUDGE_PATH = HERE / "judge_questions.py"
SEED = 86042026
EXPECTED_MAIN_PAIRS = 52
EXPECTED_CONTROL_PAIRS = 5


def load_judge_module():
    spec = importlib.util.spec_from_file_location("question_judge", JUDGE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def raw_key(row: dict) -> tuple[str, int, int]:
    return row["id"], int(row["sample"]), int(row.get("step", 1))


def expected_keys(
    cases: list[dict], samples: tuple[int, ...]
) -> set[tuple[str, int, int]]:
    keys = set()
    for case in cases:
        steps = int(case.get("replacements", 1))
        for sample in samples:
            for step in range(1, steps + 1):
                keys.add((case["id"], sample, step))
    return keys


def adapt_rows(raw_rows: list[dict], cases: dict[str, dict]) -> list[dict]:
    adapted = []
    for row in raw_rows:
        if int(row["sample"]) == 1:
            continue
        case = cases[row["id"]]
        adapted.append(
            {
                "id": row["id"],
                "sample": int(row["sample"]),
                "step": int(row.get("step", 1)),
                "series_steps": int(row.get("series_steps", 1)),
                "language": row["language"],
                "category": row["category"],
                "text": row["text"],
                "error": row.get("error"),
                "skipped_questions": list(row.get("skipped_questions") or []),
                "input": {
                    "topic": case["topic"],
                    "stage": case["stage"],
                    "messages": case.get("messages", []),
                },
            }
        )
    return adapted


def opaque_id(pair_id: str, orientation: str) -> str:
    digest = hashlib.sha256(f"86cbh0p8t:{pair_id}:{orientation}".encode()).hexdigest()
    return f"J{digest[:16]}"


def validate_judge_rows(rows: list[dict]) -> None:
    expected_fields = {"judgement_id", "prompt", "response_schema"}
    if any(set(row) != expected_fields for row in rows):
        raise SystemExit("judge-visible row contains metadata fields")
    rendered = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows).casefold()
    leaked = [token for token in ("qwen", "gemma") if token in rendered]
    if leaked:
        raise SystemExit(f"model identity leaked into judge-visible rows: {leaked}")


def prepare(qwen_path: Path, gemma_path: Path, cases_path: Path, out: Path) -> None:
    judge = load_judge_module()
    qwen_raw = read_jsonl(qwen_path)
    gemma_raw = read_jsonl(gemma_path)
    qwen_all = {raw_key(row) for row in qwen_raw}
    gemma_all = {raw_key(row) for row in gemma_raw}
    if qwen_all != gemma_all:
        raise SystemExit(
            "raw run keys differ; refusing intersection: "
            f"Qwen-only={sorted(qwen_all - gemma_all)}, "
            f"Gemma-only={sorted(gemma_all - qwen_all)}"
        )
    cases_payload = json.loads(cases_path.read_text())
    case_list = cases_payload["inputs"]
    cases = {case["id"]: case for case in case_list}
    if len(cases) != len(case_list):
        raise SystemExit("duplicate case id")
    expected_all = expected_keys(case_list, (1, 2, 3))
    if qwen_all != expected_all:
        raise SystemExit(
            "raw keys do not match the fixture: "
            f"extra={sorted(qwen_all - expected_all)}, "
            f"missing={sorted(expected_all - qwen_all)}"
        )
    wanted = expected_keys(case_list, (2, 3))
    selected = {key for key in qwen_all if key[1] in (2, 3)}
    if selected != wanted:
        raise SystemExit(
            "filtered keys do not match the fixture: "
            f"extra={sorted(selected - wanted)}, missing={sorted(wanted - selected)}"
        )
    if len(selected) != EXPECTED_MAIN_PAIRS:
        raise SystemExit(
            f"expected {EXPECTED_MAIN_PAIRS} main pairs, got {len(selected)}"
        )

    source_dir = out / "sources"
    qwen_filtered = source_dir / "run_a.jsonl"
    gemma_filtered = source_dir / "run_b.jsonl"
    write_jsonl(qwen_filtered, adapt_rows(qwen_raw, cases))
    write_jsonl(gemma_filtered, adapt_rows(gemma_raw, cases))
    protocol = {
        "ticket": "86cbh0p8t",
        "purpose": "pairwise question judging through evaluation/judge_questions.py",
        "sample_1_excluded_as_warmup": True,
        "inputs": cases_payload,
        "source_identity": {
            "run_a": "qwen3-30b-a3b-instruct-2507",
            "run_b": "gemma-4-31b-it-fp8",
        },
        "raw_artifacts": {
            "run_a": {"path": str(qwen_path), "sha256": sha256(qwen_path)},
            "run_b": {"path": str(gemma_path), "sha256": sha256(gemma_path)},
            "cases": {"path": str(cases_path), "sha256": sha256(cases_path)},
        },
        "series_note": (
            "At replacement steps after step 1, each run carries its own generated "
            "skipped_questions. The legacy judge prompt discloses this and compares "
            "both answers against the same human-authored conversation history."
        ),
    }
    write_json(source_dir / "protocol.json", protocol)

    run_a = judge.load_run(qwen_filtered, "unused")
    run_b = judge.load_run(gemma_filtered, "unused")
    if set(run_a["rows"]) != set(run_b["rows"]):
        raise SystemExit("adapted run keys differ; refusing intersection")
    pairs = judge.build_pairs(run_a, run_b, seed=SEED, control_fraction=0.1)
    controls = sum(bool(pair["control"]) for pair in pairs)
    if len(pairs) != EXPECTED_MAIN_PAIRS + EXPECTED_CONTROL_PAIRS:
        raise SystemExit(f"expected 57 total pairs, got {len(pairs)}")
    if controls != EXPECTED_CONTROL_PAIRS:
        raise SystemExit(f"expected 5 control pairs, got {controls}")

    canonical = []
    judge_rows = {"ab": [], "ba": []}
    for pair in pairs:
        for orientation in judge.ORIENTATIONS:
            judgement_id = opaque_id(pair["pair_id"], orientation)
            manifested = judge.manifest_rows([pair])
            row = next(
                item for item in manifested if item["orientation"] == orientation
            )
            canonical.append({"judgement_id": judgement_id, **row})
            judge_rows[orientation].append(
                {
                    "judgement_id": judgement_id,
                    "prompt": row["prompt"],
                    "response_schema": judge.VERDICT_SCHEMA,
                }
            )

    for index, orientation in enumerate(judge.ORIENTATIONS, start=1):
        random.Random(SEED + index).shuffle(judge_rows[orientation])
        validate_judge_rows(judge_rows[orientation])
        write_jsonl(out / f"judge_batch_{index}.jsonl", judge_rows[orientation])
    judgement_ids = [row["judgement_id"] for row in canonical]
    if len(set(judgement_ids)) != len(judgement_ids):
        raise SystemExit("duplicate opaque judgement id")
    write_jsonl(out / "canonical_pairs.jsonl", canonical)
    write_json(
        out / "protocol.json",
        {
            "ticket": "86cbh0p8t",
            "legacy_protocol": "evaluation/judge_questions.py",
            "seed": SEED,
            "main_pairs": EXPECTED_MAIN_PAIRS,
            "control_pairs": EXPECTED_CONTROL_PAIRS,
            "judgements": len(canonical),
            "judge_batches": [
                {"path": "judge_batch_1.jsonl", "judgements": len(judge_rows["ab"])},
                {"path": "judge_batch_2.jsonl", "judgements": len(judge_rows["ba"])},
            ],
            "judge_visible_fields": ["judgement_id", "prompt", "response_schema"],
            "astra": {
                "required_model": "gpt-6-astra",
                "invocation": (
                    "One codex exec call per judgement with --model gpt-6-astra, "
                    "--ephemeral, --sandbox read-only, and --output-schema. Record "
                    "the actual model reported by codex and reject a mismatch."
                ),
            },
            "fable": {
                "invocation": (
                    "Judge each blind batch independently with the same response "
                    "schema; do not expose canonical_pairs.jsonl to the judge."
                )
            },
            "aggregation": (
                "Join verdicts by judgement_id to canonical_pairs.jsonl, then use "
                "judge_questions.report semantics: both orientations must choose "
                "the same source, otherwise the pair is a tie."
            ),
            "interpretation": {
                "main_pairs": (
                    "The 52 elementary pairs come from 20 synthetic inputs and "
                    "two retained samples, with series steps correlated inside an "
                    "input. They are diagnostic and not a statistical population."
                ),
                "controls": (
                    "The five controls compare different samples from the same run, "
                    "not identical answers with a ground-truth tie. Decisive control "
                    "verdicts alone do not invalidate a judge; inspect orientation "
                    "agreement, position balance, and reasons. The control sample is "
                    "too small to prove absence of bias."
                ),
            },
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--qwen", type=Path, required=True)
    parser.add_argument("--gemma", type=Path, required=True)
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.qwen, args.gemma, args.cases, args.out)


if __name__ == "__main__":
    main()
