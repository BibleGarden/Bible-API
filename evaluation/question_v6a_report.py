#!/usr/bin/env python3
"""Read the sampling runs of ClickUp 86cbejvra and report what they measured.

One directory per sampling configuration, each produced by
`compare_question_models.py run` (which writes `protocol.json` beside one
`<alias>.jsonl` + `<alias>.meta.json` per model). This script reads them all
and writes a single Markdown table, so the four configurations are compared on
numbers rather than on impressions.

The columns are the defects the independent assessment of 86cbejtt2 counted by
hand (`bench_data/question_comparison_prompt_v5_before_after/FABLE_ASSESSMENT.md`)
plus the two loop measures that motivated the ticket:

* **verbatim repeats in a series** — a step ≥ 2 whose text equals an earlier
  step of the same series. That is the journal bug: «заменить вопрос» returned
  the same sentence.
* **step-1 collisions** — two of the three independent samples of an input
  producing the same first question. Invisible to the person, but it says the
  model's question space is narrow, which is the hypothesis this ticket tests.
* **gender** — `app/question_filters.gender_mismatch` against the gender
  `detect_gender` reads from the person's own words; split into an address in
  the *other* gender and a gender *imposed* where the request carried none.
* **menus**, **tails** — `is_menu` / `has_tail` of the same module, heuristics
  with documented false positives (see its docstring).
* **length**, **> 160**, **`automatic_violations`** — the formal checks
  `check_questions.py` already runs, carried in every row by the comparison
  runner.

Nothing here judges depth or usefulness; that is a human reading of
`report.md`, which the runner writes per directory.

    python3 question_v6a_report.py                       # the default root
    python3 question_v6a_report.py --root bench_data/xyz --out /tmp/report.md
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "app"))

# The production filters themselves, never a copy: a number in this report and
# a verdict in `app/` must be the same verdict (the rule `question_novelty.py`
# states for the repeat metric).
from question_filters import (  # noqa: E402
    detect_gender,
    gender_mismatch,
    has_tail,
    is_menu,
)

DEFAULT_ROOT = HERE / "bench_data" / "question_v6a_sampling"
LONG_ANSWER = 160


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def person_words(row: dict) -> list[str]:
    """The person's own words of this request — topic plus their `user` turns."""
    if row.get("person_words"):
        return list(row["person_words"])
    request = row["input"]
    return [request["topic"]] + [
        message["text"] for message in request["messages"] if message["role"] == "user"
    ]


def digest(value) -> str:
    """`compare_question_models.digest`, byte for byte.

    Re-typed rather than imported: that module builds an argparse parser and
    imports `gen_questions` (and through it the production prompt), which this
    reader has no business doing. `tests/test_question_v6a_report.py` pins the
    two functions to each other on the real protocol files, so they cannot
    drift apart into two different notions of "the same protocol".
    """
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def load_config(directory: Path) -> dict:
    """One sampling configuration: its protocol and every model that ran it.

    Three refusals, the same ones `compare_question_models.load_runs` makes
    before ranking anything, because a table built from a broken run is worse
    than no table: the run must be `complete`, it must hold as many rows as its
    own metadata claims, and its `protocol_hash` must be the hash of the
    `protocol.json` lying beside it — otherwise the answers were produced by a
    protocol that is no longer in the directory.
    """
    spec = read_json(directory / "protocol.json")
    identity = digest(spec)
    runs = {}
    for path in sorted(directory.glob("*.jsonl")):
        meta = read_json(directory / f"{path.stem}.meta.json")
        rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
        ]
        where = f"{directory.name}/{path.stem}"
        if not meta.get("complete"):
            # A partial run cannot be compared with a whole one: the surviving
            # subset is biased towards whatever failed last.
            raise ValueError(f"{where}: run is not complete")
        if meta.get("protocol_hash") != identity:
            raise ValueError(
                f"{where}: answers were produced by another protocol than the "
                "protocol.json beside them"
            )
        if len(rows) != meta.get("written"):
            raise ValueError(
                f"{where}: {len(rows)} answers against {meta.get('written')} "
                "recorded by the run"
            )
        runs[path.stem] = (meta, rows)
    if not runs:
        raise ValueError(f"{directory}: no model artifact")
    return {"name": directory.name, "spec": spec, "runs": runs}


def sort_key(config: dict) -> tuple:
    """Order by what was sent, not by directory name: baseline first."""
    overrides = config["spec"].get("sampling_overrides") or {}
    return (
        config["spec"]["temperature"],
        overrides.get("presence_penalty", 0.0),
        overrides.get("min_p", 0.0),
        config["name"],
    )


def sampling_label(config: dict) -> str:
    overrides = dict(config["spec"].get("sampling_overrides") or {})
    overrides.setdefault("temperature", config["spec"]["temperature"])
    return ", ".join(f"{key}={value}" for key, value in sorted(overrides.items()))


def series_of(rows: list[dict]) -> dict[tuple[str, int], list[dict]]:
    """The rows grouped into one series per (input, sample), in step order."""
    grouped: dict[tuple[str, int], list[dict]] = {}
    for row in rows:
        grouped.setdefault((row["id"], row["sample"]), []).append(row)
    return {
        key: sorted(group, key=lambda row: row["step"])
        for key, group in grouped.items()
    }


def measure(rows: list[dict]) -> dict:
    """Every number this report prints for one model in one configuration."""
    verbatim: Counter[str] = Counter()
    for (identifier, _sample), group in series_of(rows).items():
        seen: list[str] = []
        for row in group:
            text = row["text"].strip()
            if row["step"] >= 2 and text in seen:
                verbatim[identifier] += 1
            seen.append(text)

    # Two of three independent samples opening an input with the same text.
    collisions: Counter[str] = Counter()
    first_step: dict[str, list[str]] = {}
    for row in rows:
        if row["step"] == 1:
            first_step.setdefault(row["id"], []).append(row["text"].strip())
    for identifier, texts in first_step.items():
        repeated = sum(count - 1 for count in Counter(texts).values() if count > 1)
        if repeated:
            collisions[identifier] = repeated

    mismatch = imposed = menus = tails = 0
    for row in rows:
        text = row["text"]
        if row["language"] in ("ru", "uk"):
            gender = detect_gender(person_words(row))
            if gender_mismatch(text, gender):
                if gender is None:
                    imposed += 1
                else:
                    mismatch += 1
        menus += is_menu(text)
        tails += has_tail(text)

    lengths = [len(row["text"]) for row in rows]
    return {
        "rows": len(rows),
        "verbatim": sum(verbatim.values()),
        "verbatim_by_input": dict(verbatim.most_common()),
        "collisions": sum(collisions.values()),
        "collisions_by_input": dict(collisions.most_common()),
        "collision_inputs": len(collisions),
        "inputs": len(first_step),
        "mismatch": mismatch,
        "imposed": imposed,
        "menus": menus,
        "tails": tails,
        "mean_length": statistics.mean(lengths) if lengths else 0.0,
        "over_long": sum(1 for length in lengths if length > LONG_ANSWER),
        "violations": sum(1 for row in rows if row.get("automatic_violations")),
        "violation_kinds": dict(
            Counter(
                kind
                for row in rows
                for kind in (row.get("automatic_violations") or [])
            ).most_common()
        ),
        "distinct": len({row["text"].strip() for row in rows if row["text"].strip()}),
        "median_latency_ms": statistics.median(row["latency_ms"] for row in rows),
    }


def reference_rows(path: Path, identifiers: set[str]) -> list[dict]:
    """An earlier artifact restricted to the same inputs, as a variance row.

    `question_comparison_prompt_v5_final/qwen.jsonl` is the SAME configuration
    as `t07` — same prompt, same model, same temperature, three samples — run
    on a different day over sixteen inputs. Restricted to these eight it says
    how much of a difference between two configurations is simply the spread
    between two runs, which a table of single runs cannot say on its own.
    """
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    return [row for row in rows if row["id"] in identifiers]


def render(configs: list[dict], reference: tuple[str, list[dict]] | None = None) -> str:
    lines = [
        "# Sampling of the leading question on Qwen3-30B (ClickUp 86cbejvra)",
        "",
        "Prompt **v5** (`--prompt-variant production`), model "
        f"`{next(iter(configs[0]['runs'].values()))[0]['model_config']['model']}`, "
        "the eight inputs of `question_quality_inputs_v6a.json`, 3 samples, raw "
        "generation — no novelty retry and no safety replacement, exactly as in "
        "`question_comparison_prompt_v5_final`. One warm-up call per run is made "
        "and recorded separately in `<model>.meta.json` (ADR 0004), so no "
        "measured row is a first pass over a new prompt text.",
        "",
        "Gender, menus and tails are the verdicts of `app/question_filters.py`; "
        "the two loop columns count what the person would see when pressing "
        "«заменить вопрос». Sample of 75 answers per configuration — read the "
        "differences as directions, not as significance.",
        "",
        "| config | sampling | verbatim repeats in series | step-1 collisions "
        "(inputs) | wrong gender | gender imposed | menus | tails | mean length "
        "| > 160 | distinct texts | `automatic_violations` | median ms |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    def row(name: str, sampling: str, rows: list[dict]) -> str:
        data = measure(rows)
        return (
            f"| {name} | {sampling} | {data['verbatim']} | "
            f"{data['collisions']} ({data['collision_inputs']}/{data['inputs']}) | "
            f"{data['mismatch']} | {data['imposed']} | {data['menus']} | "
            f"{data['tails']} | {data['mean_length']:.0f} | {data['over_long']} | "
            f"{data['distinct']}/{data['rows']} | {data['violations']} | "
            f"{data['median_latency_ms']:.0f} |"
        )

    for config in configs:
        for alias, (_meta, rows) in sorted(config["runs"].items()):
            name = config["name"] if len(config["runs"]) == 1 else f"{config['name']} / {alias}"
            lines.append(row(f"`{name}`", sampling_label(config), rows))
    if reference is not None:
        label, rows = reference
        lines.append(row(f"_{label}_", "temperature=0.7 (another run)", rows))
    lines.extend(["", "## Where the repeats sit", ""])
    detail = [
        (config["name"], rows)
        for config in configs
        for _alias, (_meta, rows) in sorted(config["runs"].items())
    ]
    if reference is not None:
        detail.append(reference)
    for name, rows in detail:
        data = measure(rows)
        lines.append(
            f"* **{name}** — verbatim: {data['verbatim_by_input'] or 'none'}; "
            f"step-1 collisions: {data['collisions_by_input'] or 'none'}; "
            f"violation kinds: {data['violation_kinds'] or 'none'}"
        )
    lines.extend([
        "",
        "Reproduce (the key is read into the shell and never written to a file):",
        "",
        "```bash",
        "cd /root/cep/Bible-API/evaluation",
        'export AI_OPENAI_COMPAT_API_KEY="$(docker exec bible-api sh -c '
        "'printf %s \"$AI_OPENAI_COMPAT_API_KEY\"')\"",
    ])
    for config in configs:
        overrides = dict(config["spec"].get("sampling_overrides") or {})
        flags = "".join(
            f" --{key.replace('_', '-')} {value}" for key, value in sorted(overrides.items())
        )
        lines.append(
            "python3 compare_question_models.py run --models qwen "
            "--inputs question_quality_inputs_v6a.json --samples 3 --timeout 60"
            f"{flags} \\\n  --out bench_data/question_v6a_sampling/{config['name']}"
        )
    lines.extend(["python3 question_v6a_report.py", "```", ""])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--out", type=Path, default=None,
                        help="default: REPORT.md inside --root")
    parser.add_argument(
        "--reference",
        default=str(
            HERE / "bench_data" / "question_comparison_prompt_v5_final" / "qwen.jsonl"
        ),
        help="an artifact of the SAME configuration from another run, "
             "restricted to these inputs and printed as a variance row; "
             "'' switches it off",
    )
    args = parser.parse_args(argv)
    root = args.root if args.root.is_absolute() else HERE / args.root
    configs = sorted(
        (load_config(path) for path in root.iterdir()
         if path.is_dir() and (path / "protocol.json").exists()),
        key=sort_key,
    )
    if not configs:
        raise SystemExit(f"{root}: no configuration directory")
    reference = None
    if args.reference.strip():
        path = Path(args.reference)
        identifiers = {
            item["id"] for item in configs[0]["spec"]["inputs"]["inputs"]
        }
        reference = (
            f"{path.parent.name} (same config, another run)",
            reference_rows(path, identifiers),
        )
    out = args.out or root / "REPORT.md"
    out.write_text(render(configs, reference), encoding="utf-8")
    print(f"wrote {out} — {len(configs)} configurations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
