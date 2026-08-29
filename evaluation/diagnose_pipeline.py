"""Diagnostics for the pipeline benchmark: per-variant ranks of every
relevant reference for missed scenarios (uses the pipeline cache — free).

    python diagnose_pipeline.py [--scenarios ru-001,en-003] [--variants 4]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import numpy as np

os.environ.setdefault("API_KEY", "benchmark")
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "app"))

import retrieval_benchmark as rb  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenarios", default="")
    parser.add_argument("--variants", type=int, default=4)
    parser.add_argument("--no-raw", action="store_true")
    args = parser.parse_args()

    scenarios = json.loads(rb.SCENARIOS_FILE.read_text())["scenarios"]
    wanted = set(args.scenarios.split(",")) if args.scenarios else None
    metas, _t, _tt = rb.load_chunks()
    psalm_maps = rb.load_psalm_maps()
    corpus = rb.truncate_mrl(
        np.load(rb.emb_path("gemini", "title_text")), rb.PIPELINE_DIMS)
    cache = rb._load_pipeline_cache()
    api_key = rb.require_api_key()
    # No default: a diagnosis that silently runs on another model than the
    # deployment does is worse than no diagnosis (the 2026-08-29 incident).
    model = rb._dotenv_value("GEMINI_MODEL")
    if not model:
        raise SystemExit(
            "GEMINI_MODEL is not set (environment or .env) — set it to the "
            "model this diagnosis should reproduce."
        )

    from query_rewrite import REWRITE_PROMPT_VERSION

    trans_idx = {
        code: np.array([i for i, m in enumerate(metas) if m.translation == code])
        for code, _a in rb.LANGUAGE_CORPUS.values()
    }

    for scenario in scenarios:
        if wanted and scenario["id"] not in wanted:
            continue
        if scenario["category"] == "empty":
            continue
        code, _alias = rb.LANGUAGE_CORPUS[scenario["language"]]
        ctx = scenario["prayer_context"]
        raw = "\n".join(p for p in [ctx["topic"].strip()]
                        + [r.strip() for r in ctx["user_replies"]] if p)
        key = f"{model}|p{REWRITE_PROMPT_VERSION}|n{args.variants}|{scenario['id']}"
        queries = list(cache["rewrites"].get(key, []))
        if not args.no_raw:
            queries.append(raw)
        idx = trans_idx[code]
        sub_metas = [metas[i] for i in idx]

        relevant = [r for r in scenario["references"] if r["grade"] == "relevant"]
        rel_targets = [rb.map_reference(r, code, psalm_maps) for r in relevant]

        print(f"\n=== {scenario['id']} [{scenario['category']}] topic: {ctx['topic'][:60]}")
        rank_rows = []
        for qi, q in enumerate(queries):
            h = hashlib.sha1(f"q768:{q}".encode()).hexdigest()
            vec = cache["query_embeddings"].get(h)
            if vec is None:
                print(f"  v{qi} (not embedded yet): {q[:70]}")
                continue
            sims = corpus[idx] @ np.asarray(vec, dtype=np.float32)
            order = np.argsort(-sims)
            ranks = []
            for ref, targets in zip(relevant, rel_targets):
                best = None
                for rank, j in enumerate(order[:400], start=1):
                    if rb.chunk_matches(sub_metas[j], targets):
                        best = rank
                        break
                ranks.append(best)
            rank_rows.append(ranks)
            label = "raw" if qi == len(queries) - 1 and not args.no_raw else f"v{qi}"
            print(f"  {label}: {q[:78]}")
            print(f"       ranks {ranks}  top1={sub_metas[order[0]].canonical_id}"
                  f" ({sims[order[0]]:.3f})")
        if rank_rows:
            best_per_ref = [
                min((row[i] for row in rank_rows if row[i]), default=None)
                for i in range(len(relevant))
            ]
            refs = [f"{r['book_number']} {r['chapter']}:{r['verse_start']}-{r['verse_end']}"
                    for r in relevant]
            print(f"  refs: {refs}")
            print(f"  best rank over variants per ref: {best_per_ref}")


if __name__ == "__main__":
    main()
