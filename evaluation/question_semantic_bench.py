#!/usr/bin/env python3
"""Can bge-m3 see the thought where trigrams only see the frame? (86cbehyg8)

`app/question_novelty.py` is lexical: Jaccard over character 3-grams plus a
shared-opening rule. On prompt v3 that separated repeats from different
questions cleanly; on prompt v4 it does not, because v4 tells the model to
unfold the person's own last reply and every answer of a replacement series
then sits on one frame built from the same words. The trigram score measures
the frame. ClickUp 86cbehyg0 measured the damage: of 155 by-eye reworded
repeats it catches 80, and no threshold recovers the rest without buying false
positives.

This tool asks whether the embedding model the service already runs
(BAAI/bge-m3, `EMBEDDING_PROVIDER=openai_compat`) measures the thought
instead, and what one such check would cost inside a request.

    score     embed every pair of `question_pairs_labelled.json` and write
              `bench_data/question_pairs_scored.jsonl` (+ a meta sidecar)
    report    the separation tables — per signal, per threshold, per language
    latency   what the check would cost in a request: one batched embedding
              call for the candidate plus the questions already shown

The key is read from the environment and goes NOWHERE else: not into the
artifact, not into the meta sidecar, not into a log line, not into argv, not
into the URL. Read it straight out of the running container and export it for
this process only:

    export EMBEDDING_API_KEY="$(docker exec bible-api sh -c \\
        'printf %s "$EMBEDDING_API_KEY"')"
    python3 evaluation/question_semantic_bench.py score
    python3 evaluation/question_semantic_bench.py report
    python3 evaluation/question_semantic_bench.py latency --calls 25

`score` drives `app/embeddings.RemoteEmbeddingClient` — the production client,
not a copy of its request — so the vectors compared here are the vectors the
endpoint would compare, down to the batch size and the unit-length check.

The labels in `question_pairs_labelled.json` are a PROPOSAL. Pairs whose
reading is genuinely arguable carry `"ambiguous": true` and are listed for
Maria rather than resolved here; every table below is reported twice, with
them and without them.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
APP_DIR = HERE.parent / "app"
sys.path.insert(0, str(APP_DIR))

# The production filter itself, exactly as `check_questions.py` imports it:
# `question_novelty` is standard-library only, so the lexical column of this
# benchmark is the endpoint's own verdict rather than a re-implementation.
from question_novelty import (  # noqa: E402
    NEAR_REPEAT_THRESHOLD,
    is_repeat,
    similarity,
)

PAIRS_PATH = HERE / "question_pairs_labelled.json"
SCORED_PATH = HERE / "bench_data" / "question_pairs_scored.jsonl"

# The production endpoint and model. Overridable on the command line so that a
# re-measurement after the admins move the server does not need a code change;
# defaulted so that the common case cannot silently measure something else.
DEFAULT_ENDPOINT = "https://llm.ai2.ru/v1"
DEFAULT_MODEL = "BAAI/bge-m3"
DEFAULT_DIMENSIONS = 1024

# `app/config.py` fails fast on missing deployment variables (ADR 0008) and
# `embeddings` imports it at module level. This tool builds its client from
# the command line and reads none of these values — they exist only so the
# import succeeds outside the container, the way `transcribe_bench.py` and
# `tests/conftest.py` do it. `setdefault`, so a real environment still wins.
_CONFIG_STUBS = (
    ("API_KEY", "semantic-bench-unused"),
    ("DB_HOST", "semantic-bench-unused"),
    ("DB_USER", "semantic-bench-unused"),
    ("DB_PASSWORD", "semantic-bench-unused"),
    ("DB_NAME", "semantic-bench-unused"),
    ("EMBEDDING_PROVIDER", "gemini"),
    ("EMBEDDING_MODEL", "gemini-embedding-001"),
    ("EMBEDDING_DIMENSIONS", "768"),
    ("AI_QUESTION_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_REWRITE_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_RERANK_PROVIDER", "gemini"),
    ("AI_TRANSCRIBE_PROVIDER", "gemini"),
    ("AI_QUESTION_MODEL", "gemini-3.5-flash-lite"),
    ("AI_TRANSCRIBE_MODEL", "gemini-3.5-flash-lite"),
    ("AI_SCRIPTURE_REWRITE_MODEL", "gemini-3.7-flash"),
    ("AI_SCRIPTURE_RERANK_MODEL", "gemini-3.5-flash-lite"),
)

REPEAT = "repeat"
DIFFERENT = "different"


# ---------------------------------------------------------------------------
# The pure parts: loading, scoring arithmetic, the decision rules
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Pair:
    """One labelled pair. `label` is a proposal, `ambiguous` says how firm."""

    id: str
    language: str
    a: str
    b: str
    label: str
    source: str = "artifact"
    ambiguous: bool = False
    extra: dict | None = None

    @property
    def is_repeat(self) -> bool:
        return self.label == REPEAT


def load_pairs(path: Path | str) -> list[Pair]:
    """Read the labelled set, refusing anything this benchmark cannot score.

    A missing field or an unknown label is an error rather than a skipped
    row: a pair silently dropped would move every precision number below it
    without saying so.
    """
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("the pair file must hold a list of pairs")
    pairs: list[Pair] = []
    seen: set[str] = set()
    for index, row in enumerate(raw):
        if not isinstance(row, dict):
            raise ValueError(f"pair {index}: not an object")
        missing = [k for k in ("id", "language", "a", "b", "label") if not row.get(k)]
        if missing:
            raise ValueError(f"pair {index}: missing {', '.join(missing)}")
        if row["label"] not in (REPEAT, DIFFERENT):
            raise ValueError(f"pair {row['id']}: unknown label {row['label']!r}")
        if row["id"] in seen:
            raise ValueError(f"duplicate pair id {row['id']!r}")
        seen.add(row["id"])
        pairs.append(
            Pair(
                id=row["id"],
                language=row["language"],
                a=row["a"],
                b=row["b"],
                label=row["label"],
                source=row.get("source", "artifact"),
                ambiguous=bool(row.get("ambiguous", False)),
                extra={
                    k: v
                    for k, v in row.items()
                    if k
                    not in ("id", "language", "a", "b", "label", "source", "ambiguous")
                },
            )
        )
    return pairs


def cosine(left: list[float], right: list[float]) -> float:
    """Cosine of two vectors, normalising defensively.

    `RemoteEmbeddingClient` already returns unit vectors and verifies it, so
    the norms are 1.0 here; dividing anyway costs nothing and keeps this
    function usable on a stand-in encoder in the tests.
    """
    if len(left) != len(right):
        raise ValueError("vectors of different width")
    dot = sum(x * y for x, y in zip(left, right))
    norm = math.sqrt(sum(x * x for x in left)) * math.sqrt(sum(y * y for y in right))
    if norm == 0.0:
        return 0.0
    return dot / norm


def lexical_flag(row: dict) -> bool:
    """The production verdict for this pair: `is_repeat(a, [b])`."""
    return bool(row["lexical_repeat"])


def cosine_flag(row: dict, threshold: float) -> bool:
    return row["cosine"] >= threshold


def combined_flag(row: dict, threshold: float) -> bool:
    """The rule this experiment is really about: the lexical filter as it
    stands, OR a cosine at least `threshold`. Never AND — the lexical filter's
    value on v4 is that it has no false positives, and an AND would throw that
    away to no purpose."""
    return lexical_flag(row) or cosine_flag(row, threshold)


@dataclass(frozen=True)
class Metrics:
    tp: int
    fp: int
    fn: int
    tn: int

    @property
    def precision(self) -> float:
        return self.tp / (self.tp + self.fp) if self.tp + self.fp else 1.0

    @property
    def recall(self) -> float:
        return self.tp / (self.tp + self.fn) if self.tp + self.fn else 1.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if p + r else 0.0


def evaluate(rows: list[dict], predicate) -> Metrics:
    """Confusion counts of `predicate` against the `label` of each row."""
    tp = fp = fn = tn = 0
    for row in rows:
        flagged = predicate(row)
        positive = row["label"] == REPEAT
        if flagged and positive:
            tp += 1
        elif flagged:
            fp += 1
        elif positive:
            fn += 1
        else:
            tn += 1
    return Metrics(tp, fp, fn, tn)


def separation(rows: list[dict], key: str) -> dict:
    """Do the two classes separate on this score at all?

    `gap` is the highest score among the different pairs subtracted from the
    lowest among the repeats: positive means a threshold exists that is right
    about every pair, negative means the classes interleave and by how much.
    """
    positives = [row[key] for row in rows if row["label"] == REPEAT]
    negatives = [row[key] for row in rows if row["label"] == DIFFERENT]
    if not positives or not negatives:
        return {"gap": None, "positives": len(positives), "negatives": len(negatives)}
    return {
        "positives": len(positives),
        "negatives": len(negatives),
        "positive_min": min(positives),
        "positive_median": statistics.median(positives),
        "positive_max": max(positives),
        "negative_min": min(negatives),
        "negative_median": statistics.median(negatives),
        "negative_max": max(negatives),
        "gap": min(positives) - max(negatives),
    }


def best_threshold(rows: list[dict], predicate_factory, thresholds) -> tuple:
    """The threshold with the highest F1, then the highest precision.

    F1 and not accuracy: the label set is repeat-heavy (that is what a
    replacement series is), so accuracy would reward flagging everything.

    A remaining tie — the same confusion matrix over a range of thresholds,
    which is what a gap between the classes looks like — is broken towards the
    HIGHER threshold: same answer, more distance from the different pairs
    below it. `thresholds` is therefore expected in ascending order.
    """
    best = None
    for threshold in thresholds:
        metrics = evaluate(rows, lambda row, t=threshold: predicate_factory(row, t))
        key = (metrics.f1, metrics.precision)
        if best is None or key >= best[0]:
            best = (key, threshold, metrics)
    return (best[1], best[2]) if best else (None, None)


def threshold_grid(start: float = 0.50, stop: float = 0.96, step: float = 0.01):
    count = int(round((stop - start) / step)) + 1
    return [round(start + index * step, 4) for index in range(count)]


# ---------------------------------------------------------------------------
# The embedding client (network)
# ---------------------------------------------------------------------------


def build_client(endpoint: str, model: str, dimensions: int, batch_size: int = 64):
    """The PRODUCTION embedding client, driven from a CLI.

    The key comes from `EMBEDDING_API_KEY` in this process's environment and
    is removed from it before the import that validates deployment config —
    `config` would rightly refuse a key beside the stubbed `gemini` provider.
    It then lives only in the client object.
    """
    api_key = os.environ.get("EMBEDDING_API_KEY", "").strip()
    if not api_key:
        raise SystemExit(
            "EMBEDDING_API_KEY is not exported — read it from the running "
            "container, never from a file:\n"
            "  export EMBEDDING_API_KEY=\"$(docker exec bible-api sh -c "
            "'printf %s \"$EMBEDDING_API_KEY\"')\""
        )
    for name, value in _CONFIG_STUBS:
        os.environ.setdefault(name, value)
    os.environ.pop("EMBEDDING_API_KEY", None)
    from embeddings import RemoteEmbeddingClient

    return RemoteEmbeddingClient(
        endpoint=endpoint,
        api_key=api_key,
        model=model,
        dimensions=dimensions,
        batch_size=batch_size,
    )


def embed_all(client, texts: list[str]) -> dict[str, list[float]]:
    """One vector per DISTINCT text, batched by the client itself (<= 64).

    Deduplicated because the artifacts repeat questions across pairs — the
    same text embedded twice is the same vector and a wasted call.
    """
    unique = list(dict.fromkeys(texts))
    vectors = client.embed_documents(unique)
    return dict(zip(unique, vectors))


# ---------------------------------------------------------------------------
# score
# ---------------------------------------------------------------------------


def run_score(args) -> int:
    pairs = load_pairs(args.pairs)
    client = build_client(args.endpoint, args.model, args.dimensions)
    started = time.time()
    try:
        cache = embed_all(client, [t for p in pairs for t in (p.a, p.b)])
    finally:
        client.close()
    elapsed = time.time() - started

    rows = []
    for pair in pairs:
        verdict = is_repeat(pair.a, [pair.b])
        rows.append(
            {
                "id": pair.id,
                "language": pair.language,
                "source": pair.source,
                "label": pair.label,
                "ambiguous": pair.ambiguous,
                "trigram": round(similarity(pair.a, pair.b), 6),
                "lexical_repeat": verdict.repeat,
                "lexical_kind": verdict.kind,
                "cosine": round(cosine(cache[pair.a], cache[pair.b]), 6),
                **(pair.extra or {}),
            }
        )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    # The sidecar names the host and the model, never the key and never a URL
    # carrying credentials — the endpoint is a plain https base URL by
    # construction (`config` refuses one with a userinfo or a query string).
    meta = {
        "ticket": "86cbehyg8",
        "measured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "pairs": len(rows),
        "distinct_texts": len(cache),
        "embedding_host": args.endpoint.split("//")[-1].split("/")[0],
        "embedding_model": args.model,
        "embedding_dimensions": args.dimensions,
        "batch_size": 64,
        "embed_seconds": round(elapsed, 2),
        "lexical_constants": {
            "near_repeat_threshold": NEAR_REPEAT_THRESHOLD,
        },
        "pairs_file": str(Path(args.pairs).name),
    }
    Path(str(out) + ".meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
    )
    print(f"{len(rows)} pairs, {len(cache)} distinct texts, {elapsed:.1f} s -> {out}")
    return 0


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


def load_scored(path: Path | str) -> list[dict]:
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


def _cells(metrics: Metrics) -> str:
    """The confusion columns of one row, without the leading pipe."""
    return (
        f" {metrics.tp} | {metrics.fp} | {metrics.fn} | {metrics.tn} | "
        f"{metrics.precision:.2f} | {metrics.recall:.2f} | {metrics.f1:.2f} |"
    )


def render_report(rows: list[dict], drop_ambiguous: bool = False) -> list[str]:
    if drop_ambiguous:
        rows = [row for row in rows if not row.get("ambiguous")]
    grid = threshold_grid()
    lines: list[str] = []
    subsets = [("всего", rows)]
    for language in sorted({row["language"] for row in rows}):
        subsets.append((language, [row for row in rows if row["language"] == language]))

    lines.append("### Разделимость по каждому сигналу")
    lines.append("")
    lines.append(
        "| набор | пар | повторов | разных | триграммы: разрыв | косинус: разрыв |"
    )
    lines.append("|---|---|---|---|---|---|")
    for name, subset in subsets:
        tri = separation(subset, "trigram")
        cos = separation(subset, "cosine")
        lines.append(
            f"| {name} | {len(subset)} | {tri['positives']} | {tri['negatives']} | "
            + (f"{tri['gap']:+.3f} | " if tri["gap"] is not None else "— | ")
            + (f"{cos['gap']:+.3f} |" if cos["gap"] is not None else "— |")
        )
    lines.append("")

    lines.append("### Лучшее, что даёт каждый сигнал (порог по F1)")
    lines.append("")
    lines.append("| набор | сигнал | порог | TP | FP | FN | TN | точность | полнота | F1 |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for name, subset in subsets:
        lex = evaluate(subset, lexical_flag)
        lines.append(f"| {name} | фильтр `is_repeat` | — |" + _cells(lex))
        threshold, metrics = best_threshold(subset, cosine_flag, grid)
        lines.append(
            f"| {name} | косинус bge-m3 | {threshold:.2f} |" + _cells(metrics)
        )
        threshold, metrics = best_threshold(subset, combined_flag, grid)
        lines.append(
            f"| {name} | `is_repeat` ИЛИ косинус | {threshold:.2f} |" + _cells(metrics)
        )
    lines.append("")

    lines.append("### Косинус по порогам (весь набор)")
    lines.append("")
    lines.append(
        "| порог | косинус: TP/FP/FN | точность | полнота | "
        "ИЛИ с фильтром: TP/FP/FN | точность | полнота |"
    )
    lines.append("|---|---|---|---|---|---|---|")
    for threshold in (0.70, 0.75, 0.80, 0.82, 0.85, 0.87, 0.90, 0.92, 0.95):
        cos = evaluate(rows, lambda row, t=threshold: cosine_flag(row, t))
        both = evaluate(rows, lambda row, t=threshold: combined_flag(row, t))
        lines.append(
            f"| {threshold:.2f} | {cos.tp}/{cos.fp}/{cos.fn} | {cos.precision:.2f} | "
            f"{cos.recall:.2f} | {both.tp}/{both.fp}/{both.fn} | "
            f"{both.precision:.2f} | {both.recall:.2f} |"
        )
    lines.append("")
    return lines


def render_failures(rows: list[dict], threshold: float, limit: int = 12) -> list[str]:
    """Where each signal is wrong at `threshold` — the material for the README."""
    lines = ["### Где косинус ошибается", ""]
    fps = sorted(
        (r for r in rows if r["label"] == DIFFERENT and r["cosine"] >= threshold),
        key=lambda r: -r["cosine"],
    )
    fns = sorted(
        (r for r in rows if r["label"] == REPEAT and r["cosine"] < threshold),
        key=lambda r: r["cosine"],
    )
    saves = sorted(
        (
            r
            for r in rows
            if r["label"] == REPEAT and not r["lexical_repeat"] and r["cosine"] >= threshold
        ),
        key=lambda r: r["trigram"],
    )
    for title, group in (
        (f"ложные срабатывания косинуса при {threshold:.2f}", fps),
        (f"пропуски косинуса при {threshold:.2f}", fns),
        ("повторы, которые ловит ТОЛЬКО косинус (фильтр их не видит)", saves),
    ):
        lines.append(f"* **{title}**: {len(group)}")
        for row in group[:limit]:
            lines.append(
                f"  * `{row['id']}` ({row['language']}) "
                f"триграммы {row['trigram']:.3f}, косинус {row['cosine']:.3f}"
                + (" — спорная" if row.get("ambiguous") else "")
            )
    lines.append("")
    return lines


def run_report(args) -> int:
    rows = load_scored(args.scored)
    print("\n".join(render_report(rows)))
    print("\n".join(render_failures(rows, args.threshold)))
    ambiguous = [row["id"] for row in rows if row.get("ambiguous")]
    print(f"### Спорные пары ({len(ambiguous)}) — Марии\n")
    print("\n".join(f"* `{pid}`" for pid in ambiguous))
    print("\n### Те же таблицы без спорных пар\n")
    print("\n".join(render_report(rows, drop_ambiguous=True)))
    return 0


# ---------------------------------------------------------------------------
# latency
# ---------------------------------------------------------------------------


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile — no interpolation, so a p90 is a value that
    was actually measured."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1))
    return ordered[index]


def run_latency(args) -> int:
    """What one novelty check would add to `POST /api/ai/question`.

    The shape measured is the one the handler would use: ONE call carrying the
    candidate plus every question already shown — `twinkler_ai`'s own `shown`
    list, i.e. the `assistant` turns of `messages` plus `skipped_questions`.

    `--batch 12` is the TYPICAL prayer, not the ceiling (review of 86cbehyg8):
    `MAX_SKIPPED_QUESTIONS` is 10 and `MAX_MESSAGES` is 40, so a long prayer
    reaches ~30 texts — still one batch of the client's 64, but three times
    the work. Measure that case with `--batch 30` before quoting a cost.
    """
    pairs = load_pairs(args.pairs)
    texts = [pair.a for pair in pairs][: args.batch]
    while len(texts) < args.batch:
        texts.append(pairs[len(texts) % len(pairs)].b)
    client = build_client(args.endpoint, args.model, args.dimensions)
    timings: list[float] = []
    try:
        started = time.perf_counter()
        client.embed_documents(texts)
        cold = time.perf_counter() - started
        for index in range(args.calls):
            # `--gap 0` measures back-to-back calls, which on a CPU server is
            # a queue this process built itself; a gap measures what one
            # request in isolation costs. Both are real numbers about a
            # different load, so the gap is an argument rather than a choice
            # made here.
            if index and args.gap:
                time.sleep(args.gap)
            started = time.perf_counter()
            client.embed_documents(texts)
            timings.append(time.perf_counter() - started)

        print(f"batch of {len(texts)} texts, {args.calls} warm calls "
              f"{args.gap:g} s apart, host "
              f"{args.endpoint.split('//')[-1].split('/')[0]}")
        print(f"  cold (first call after the client is built): {cold * 1000:.0f} ms")
        print(f"  warm median: {statistics.median(timings) * 1000:.0f} ms")
        print(f"  warm p90:    {percentile(timings, 0.90) * 1000:.0f} ms")
        print(f"  warm min/max: {min(timings) * 1000:.0f} / "
              f"{max(timings) * 1000:.0f} ms")

        if args.with_question:
            # The same client: `build_client` deliberately empties
            # EMBEDDING_API_KEY out of the environment, so there is exactly
            # one chance to build one per process.
            question_and_check(args, client, client_texts=texts)
    finally:
        client.close()
    return 0


def question_and_check(args, client, client_texts: list[str]) -> None:
    """The whole path, sequentially: generate a question, then check it.

    Not wired into the app — this is the measurement that says whether wiring
    it in would fit the budget. It calls the LOCAL API's `/api/ai/question`
    (the production Qwen behind it) and then does the embedding call the
    handler would do with the answer in hand.
    """
    import httpx

    api_key = os.environ.get("API_KEY", "").strip()
    if not api_key:
        print("  (--with-question needs API_KEY exported; skipped)")
        return
    body = {
        "topic": "Понять масштаб целей на завтра",
        "stage": "next",
        "messages": [
            {"role": "assistant", "text": "Что сейчас внутри тебя?"},
            {
                "role": "user",
                "text": "Я рада тому, что сегодня немало сделано, но устала.",
            },
        ],
    }
    totals: list[float] = []
    questions: list[float] = []
    checks: list[float] = []
    with httpx.Client(timeout=60.0) as http:
        for index in range(args.question_calls):
            # `AI_REQUESTS_PER_CLIENT_PER_MINUTE` is 10 by default and this
            # loop is one client: without a pace the run measures the 429
            # ladder rather than the endpoint.
            if index:
                time.sleep(args.pace)
            started = time.perf_counter()
            response = http.post(
                f"{args.api}/api/ai/question",
                json=body,
                headers={"X-API-Key": api_key},
            )
            after_question = time.perf_counter()
            if response.status_code != 200:
                print(f"  question call failed: HTTP {response.status_code}")
                return
            text = response.json().get("text", "")
            client.embed_documents([text] + client_texts[: args.batch - 1])
            end = time.perf_counter()
            questions.append(after_question - started)
            checks.append(end - after_question)
            totals.append(end - started)
    print(f"  full path, {args.question_calls} runs (question then check):")
    print(f"    question median {statistics.median(questions) * 1000:.0f} ms, "
          f"p90 {percentile(questions, 0.90) * 1000:.0f} ms")
    print(f"    check    median {statistics.median(checks) * 1000:.0f} ms, "
          f"p90 {percentile(checks, 0.90) * 1000:.0f} ms")
    print(f"    total    median {statistics.median(totals) * 1000:.0f} ms, "
          f"p90 {percentile(totals, 0.90) * 1000:.0f} ms")


# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--dimensions", type=int, default=DEFAULT_DIMENSIONS)
    parser.add_argument("--pairs", default=str(PAIRS_PATH))
    sub = parser.add_subparsers(dest="command", required=True)

    score = sub.add_parser("score", help="embed every pair and write the artifact")
    score.add_argument("--out", default=str(SCORED_PATH))
    score.set_defaults(func=run_score)

    report = sub.add_parser("report", help="the separation tables")
    report.add_argument("--scored", default=str(SCORED_PATH))
    report.add_argument("--threshold", type=float, default=0.85)
    report.set_defaults(func=run_report)

    latency = sub.add_parser("latency", help="what the check costs in a request")
    latency.add_argument("--calls", type=int, default=20)
    latency.add_argument("--batch", type=int, default=12)
    latency.add_argument(
        "--gap", type=float, default=0.0,
        help="seconds between the warm embedding calls (0 = back to back, "
             "which measures a queue this process built itself)",
    )
    latency.add_argument("--with-question", action="store_true")
    latency.add_argument("--question-calls", type=int, default=10)
    latency.add_argument(
        "--pace", type=float, default=7.0,
        help="seconds between question calls (the endpoint's own per-client "
             "rate limit is 10 per minute)",
    )
    latency.add_argument("--api", default="http://127.0.0.1:9084")
    latency.set_defaults(func=run_latency)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
