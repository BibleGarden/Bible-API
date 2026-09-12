#!/usr/bin/env python3
"""Build a self-contained blind A/B review pack for experiment 86cbh0p8t."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import html
import json
import os
from pathlib import Path


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def record_key(record: dict) -> tuple[str, int, int]:
    return record["id"], int(record["sample"]), int(record.get("step", 1))


def index_records(records: list[dict], *, minimum_sample: int) -> dict[tuple, dict]:
    selected = {}
    for record in records:
        if int(record["sample"]) < minimum_sample:
            continue
        key = record_key(record)
        if key in selected:
            raise SystemExit(f"duplicate record {key}")
        selected[key] = record
    return selected


def orientation(seed: str, case_id: str, sample: int) -> bool:
    digest = hmac.new(
        seed.encode(), f"{case_id}:{sample}".encode(), hashlib.sha256
    ).digest()
    return bool(digest[0] & 1)


def context_html(case: dict) -> str:
    messages = "".join(
        f"<li><b>{html.escape(message['role'])}:</b> "
        f"{html.escape(message['text'])}</li>"
        for message in case.get("messages", [])
    )
    history = f"<ul>{messages}</ul>" if messages else "<p><i>No history</i></p>"
    return (
        f"<p><b>Language:</b> {html.escape(case['language'])}; "
        f"<b>stage:</b> {html.escape(case['stage'])}</p>"
        f"<p><b>Topic:</b> {html.escape(case['topic'])}</p>{history}"
    )


def answers_html(records: list[dict]) -> str:
    parts = []
    for record in sorted(records, key=lambda row: int(row.get("step", 1))):
        prefix = (
            f"<b>Replacement {int(record['step'])}:</b> " if "step" in record else ""
        )
        text = record.get("text") or f"[Failure: {record.get('error') or 'empty'}]"
        parts.append(f"<p>{prefix}{html.escape(text)}</p>")
    return "".join(parts)


def build_pack(
    qwen_path: Path,
    gemma_path: Path,
    cases_path: Path,
    output_path: Path,
    mapping_path: Path,
    seed: str,
) -> None:
    qwen_rows = read_jsonl(qwen_path)
    gemma_rows = read_jsonl(gemma_path)
    qwen = index_records(qwen_rows, minimum_sample=2)
    gemma = index_records(gemma_rows, minimum_sample=2)
    if set(qwen) != set(gemma):
        missing_qwen = sorted(set(gemma) - set(qwen))
        missing_gemma = sorted(set(qwen) - set(gemma))
        raise SystemExit(
            f"record keys differ; missing Qwen={missing_qwen}, "
            f"missing Gemma={missing_gemma}"
        )

    cases_payload = json.loads(cases_path.read_text())
    cases = {case["id"]: case for case in cases_payload["inputs"]}
    groups: dict[tuple[str, int], list[tuple[str, int, int]]] = {}
    for key in sorted(qwen):
        groups.setdefault((key[0], key[1]), []).append(key)
    if len(groups) != 40:
        raise SystemExit(f"expected 40 review rows, got {len(groups)}")

    review_rows = []
    mapping_rows = []
    for ordinal, ((case_id, sample), keys) in enumerate(groups.items(), start=1):
        if case_id not in cases:
            raise SystemExit(f"missing fixture case {case_id}")
        swap = orientation(seed, case_id, sample)
        left_source, right_source = (gemma, qwen) if swap else (qwen, gemma)
        left_name, right_name = ("gemma", "qwen") if swap else ("qwen", "gemma")
        review_id = f"R{ordinal:02d}"
        left = [left_source[key] for key in keys]
        right = [right_source[key] for key in keys]
        review_rows.append(
            f'<section data-review-id="{review_id}"><h2>{review_id}</h2>'
            f"{context_html(cases[case_id])}"
            f'<div class="answers"><article><h3>A</h3>{answers_html(left)}</article>'
            f"<article><h3>B</h3>{answers_html(right)}</article></div>"
            f"<fieldset><legend>Decision</legend>"
            + "".join(
                f'<label><input type="radio" name="{review_id}" '
                f'value="{value}"> {label}</label>'
                for value, label in (
                    ("a", "A better"),
                    ("b", "B better"),
                    ("tie", "Tie"),
                    ("both_unacceptable", "Both unacceptable"),
                )
            )
            + f'<label class="reason">Optional reason '
            f'<textarea data-reason="{review_id}"></textarea></label>'
            f"</fieldset></section>"
        )
        mapping_rows.append(
            {
                "review_id": review_id,
                "case_id": case_id,
                "sample": sample,
                "A": left_name,
                "B": right_name,
            }
        )

    document = (
        """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Blind question review</title><style>
body{font:16px/1.45 system-ui,sans-serif;max-width:1050px;margin:auto;padding:24px;color:#202124}
section{border-top:2px solid #bbb;padding:20px 0}.answers{display:grid;grid-template-columns:1fr 1fr;gap:16px}
article{background:#f5f6f7;border-radius:8px;padding:8px 16px}label{margin-right:18px}.reason{display:block;margin-top:12px}
textarea{display:block;width:100%;min-height:52px;margin-top:4px}button{position:sticky;bottom:12px;padding:10px 18px}
@media(max-width:700px){.answers{grid-template-columns:1fr}}
</style></head><body><h1>Blind question review</h1>
<p>Compare depth, grounding, language, gentleness, hidden advice, and novelty. Model identities are not present in this file.</p>
<button id="export">Download decisions</button>"""
        + "".join(review_rows)
        + """
<script>
document.querySelector('#export').addEventListener('click',()=>{const decisions=[];
document.querySelectorAll('section[data-review-id]').forEach(s=>{const id=s.dataset.reviewId;
const picked=s.querySelector('input[type=radio]:checked');const reason=s.querySelector('textarea').value;
decisions.push({review_id:id,decision:picked?picked.value:null,reason});});
const blob=new Blob([JSON.stringify({reviewer:'Maria',decisions},null,2)+'\\n'],{type:'application/json'});
const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='blind_review_decisions.json';a.click();
URL.revokeObjectURL(a.href);});
</script></body></html>"""
    )
    identities = {"qwen", "gemma"} | {
        str(row.get(field, "")).casefold()
        for row in qwen_rows + gemma_rows
        for field in ("provider", "model")
        if row.get(field)
    }
    leaked = sorted(
        identity for identity in identities if identity in document.casefold()
    )
    if leaked:
        raise SystemExit(f"model identity leaked into blind pack: {leaked}")
    output_path.write_text(document)
    mapping_path.write_text(
        json.dumps(
            {
                "ticket": "86cbh0p8t",
                "seed_sha256": hashlib.sha256(seed.encode()).hexdigest(),
                "sample_1_excluded_as_warmup": True,
                "rows": mapping_rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--qwen", type=Path, required=True)
    parser.add_argument("--gemma", type=Path, required=True)
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mapping", type=Path, required=True)
    args = parser.parse_args()
    seed = os.environ.get("BLIND_PACK_SEED")
    if not seed:
        raise SystemExit("missing BLIND_PACK_SEED")
    build_pack(args.qwen, args.gemma, args.cases, args.output, args.mapping, seed)


if __name__ == "__main__":
    main()
