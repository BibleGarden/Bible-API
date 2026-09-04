#!/usr/bin/env python3
"""
Answer the final-choice (rerank) stage of the evaluation set with ANY
OpenAI-compatible chat-completions endpoint (ClickUp 86cbed851, umbrella
86cbe4mtq).

Companion of `gen_rewrites.py`, one stage later. Input is an artifact of
`retrieval_benchmark.py export-rerank-input`: for every scenario it holds
exactly what the production reranker is handed — the v9 system instruction,
the user content with the numbered candidates and their `[n]` verse markers,
the JSON response schema and the candidate list. Output is the
`--reranks-file` artifact that `retrieval_benchmark.py pipeline` consumes, so
the local model's choices are scored by the same `final_top1` report as
Gemini's.

Umbrella hypothesis being measured: picking the best of ready texts should be
easier for a local model than recalling passages from memory, so the rerank
stage — not rewrite — is where a local model could reach production quality
(86cbe4n7e left retrieval MRR below the threshold).

Protocol, identical to `gen_rewrites.py` so the two artifacts read alike:
system = the exported instruction, user = the exported user content,
temperature 0, `response_format=json_object`; an unusable answer is retried
once, transport failures up to three attempts; wall time of each scenario is
recorded. The answer is validated by the PRODUCTION parser
(`app/passage_rerank.parse_rerank_response`) against the exported
`candidate_count`, so a file can never carry an index the server would have
refused.

The response schema is exported for reference and for endpoints that support
structured output; it is NOT sent — `response_format={"type":"json_object"}`
is what every local server understands, and the server-side validation is the
real guard either way.

Examples:

    # local Ollama (OpenAI-compatible surface)
    python gen_reranks.py --endpoint http://localhost:11434/v1 \\
        --model qwen3:4b-instruct-2507-q4_K_M \\
        --input bench_data/rerank_input_flash37.json \\
        --out bench_data/qwen3-4b_reranks_flash37.json

    # Maria's 30B over the network
    python gen_reranks.py --endpoint http://<host>/v1 \\
        --model qwen3-30b-a3b-instruct-2507 \\
        --input bench_data/rerank_input_flash37.json \\
        --out bench_data/qwen30b_reranks_flash37.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

import httpx

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "app"))

# `app/config.py` fails fast on missing deployment variables (ADR 0008) and
# `passage_rerank` imports it at module level. This tool talks to its own
# endpoint and reads none of these values — they exist only so the import
# succeeds outside the container, exactly as tests/conftest.py and
# gen_rewrites.py do it. `setdefault`, so a real environment still wins.
for _name, _value in (
    ("API_KEY", "gen-reranks-unused"),
    ("DB_HOST", "gen-reranks-unused"),
    ("DB_USER", "gen-reranks-unused"),
    ("DB_PASSWORD", "gen-reranks-unused"),
    ("DB_NAME", "gen-reranks-unused"),
    ("EMBEDDING_MODEL", "gemini-embedding-001"),
    ("EMBEDDING_DIMENSIONS", "768"),
    ("AI_SCRIPTURE_REWRITE_MODEL", "gemini-3.7-flash"),
    ("AI_SCRIPTURE_RERANK_MODEL", "gemini-3.5-flash-lite"),
    ("AI_QUESTION_MODEL", "gemini-3.5-flash-lite"),
    ("AI_TRANSCRIBE_MODEL", "gemini-3.5-flash-lite"),
):
    os.environ.setdefault(_name, _value)

from passage_rerank import (  # noqa: E402
    RERANK_PROMPT_VERSION,
    PassageRerankError,
    parse_rerank_response,
)

# A rerank answer is three small numbers and one short sentence, so the
# ceiling is generous even for a model that thinks out loud — and, unlike the
# rewrite stage, an unreachable ceiling here cannot feed a degenerate list.
DEFAULT_MAX_TOKENS = 2048
# One attempt may legitimately take minutes on a CPU-only local model, and
# this prompt is much longer than a rewrite one (10 candidate passages).
DEFAULT_TIMEOUT_SECONDS = 600.0
TRANSPORT_ATTEMPTS = 3
# A syntactically broken or out-of-range answer is retried once — the same
# allowance gen_rewrites.py gives a broken rewrite.
PARSE_ATTEMPTS = 2
# What of the model's raw answer is kept in the artifact: enough to see what
# it said, never enough to bloat the file with a repeated passage.
_MAX_RAW_CHARS = 2000

# Some small models emit a reasoning block before the answer; the JSON
# extraction in `parse_rerank_response` is greedy, so strip it rather than let
# a brace inside the reasoning swallow the real object.
_THINK_BLOCK = re.compile(r"<think>.*?</think>", flags=re.DOTALL | re.IGNORECASE)

PARTIAL_NOTE = (
    "PARTIAL run: fewer scenarios than the input artifact holds (--only, or "
    "the run was interrupted). Aggregates are NOT comparable with full-set "
    "artifacts, and `pipeline --reranks-file` will degrade every missing "
    "scenario to retrieval rank-1, exactly like a live rerank failure."
)


def endpoint_host(endpoint: str) -> str:
    """Host of the endpoint, never its query string (keys live there)."""
    parts = urlsplit(endpoint)
    return f"{parts.scheme}://{parts.netloc}" if parts.netloc else endpoint


def completions_url(endpoint: str) -> str:
    base = endpoint.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    return f"{base}/chat/completions"


def transport_error(exc: Exception) -> str:
    """Failure category of a transport error — never the provider's message.

    httpx renders an HTTPStatusError as "... for url '<full url>'", and that
    URL carries whatever the endpoint puts in its query string, `?key=…`
    included. These artifacts are committed to a PUBLIC repository, so only
    the exception type and — for an HTTP error — the status code are
    recorded. The status is the part anyone debugging actually needs.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return f"transport: HTTPStatusError (HTTP {exc.response.status_code})"
    return f"transport: {type(exc).__name__}"


def resolve_path(value: str) -> Path:
    """Relative paths are relative to evaluation/, not to the caller's cwd."""
    path = Path(value)
    return path if path.is_absolute() else HERE / path


def build_artifact(
    args: argparse.Namespace,
    source_meta: dict,
    records: list[dict],
    expected: int,
) -> dict:
    """The artifact document, including whether this run is partial.

    `partial` is derived, never passed in by hand: a run is full only when it
    produced a record for every scenario of the input artifact.
    """
    covered = [r["id"] for r in records]
    partial = len(covered) < expected
    meta = {
        "model": args.model,
        "date": date.today().isoformat(),
        "endpoint": endpoint_host(args.endpoint),
        # The prompt version of THIS code; `source` carries the one the input
        # was exported with. They differ only if the input predates a prompt
        # change, which is exactly what makes the run incomparable.
        "rerank_prompt_version": RERANK_PROMPT_VERSION,
        "source": {
            "input": args.input,
            "results": source_meta.get("source_results", ""),
            "config": source_meta.get("source_config", ""),
            "rerank_prompt_version": source_meta.get("rerank_prompt_version"),
            "key_verses": source_meta.get("key_verses"),
            "scenarios_version": source_meta.get("scenarios_version", ""),
            "partial": source_meta.get("partial"),
        },
        "sampling": {
            "temperature": args.temperature,
            "max_tokens": args.max_tokens,
            "response_format": "json_object",
        },
        "partial": partial,
        "scenarios_covered": covered,
        "scenarios_expected": expected,
    }
    if partial:
        meta["note"] = PARTIAL_NOTE
    return {"meta": meta, "scenarios": records}


def write_artifact(out: Path, artifact: dict) -> None:
    """Write atomically: a crash mid-write must not truncate a good file."""
    tmp = out.with_name(out.name + ".tmp")
    tmp.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    tmp.replace(out)


def call_model(
    client: httpx.Client,
    url: str,
    api_key: str,
    model: str,
    instruction: str,
    user_content: str,
    temperature: float,
    max_tokens: int,
) -> str:
    """One chat completion; raises httpx errors for the transport ladder."""
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": instruction},
            {"role": "user", "content": user_content},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    response = client.post(url, json=payload, headers=headers)
    response.raise_for_status()
    data = response.json()
    try:
        message = data["choices"][0]["message"]
    except (KeyError, IndexError, TypeError) as exc:
        raise PassageRerankError("response has no choices") from exc
    text = message.get("content") or ""
    if not isinstance(text, str) or not text.strip():
        # Some providers put the answer in a reasoning field when the visible
        # content came back empty; that is a failure, not something to guess.
        raise PassageRerankError("response content is empty")
    return _THINK_BLOCK.sub("", text)


def generate_one(
    client: httpx.Client,
    url: str,
    args: argparse.Namespace,
    entry: dict,
) -> dict:
    """The choice for one scenario, with the retry ladder and wall time."""
    started = time.monotonic()
    attempts = 0
    parse_failures = 0
    transport_failures = 0
    last_error = ""
    raw = ""
    choice = None

    while attempts < TRANSPORT_ATTEMPTS + PARSE_ATTEMPTS:
        attempts += 1
        try:
            raw = call_model(
                client, url, args.api_key, args.model,
                entry["instruction"], entry["user_content"],
                args.temperature, args.max_tokens,
            )
        except (httpx.HTTPError, ValueError) as exc:
            transport_failures += 1
            last_error = transport_error(exc)
            if transport_failures >= TRANSPORT_ATTEMPTS:
                break
            time.sleep(2.0 * transport_failures)
            continue
        except PassageRerankError as exc:
            parse_failures += 1
            last_error = f"empty: {exc}"
            if parse_failures >= PARSE_ATTEMPTS:
                break
            continue
        try:
            # The production validator, against the exported candidate count:
            # an out-of-range or malformed answer fails here exactly as it
            # would in the service.
            choice = parse_rerank_response(raw, entry["candidate_count"])
            break
        except PassageRerankError as exc:
            parse_failures += 1
            last_error = f"parse: {exc}"
            if parse_failures >= PARSE_ATTEMPTS:
                break

    latency_ms = int((time.monotonic() - started) * 1000)
    span = None
    if choice is not None and choice.key_verse_start is not None:
        span = [choice.key_verse_start, choice.key_verse_end]
    record: dict = {
        "id": entry["id"],
        "language": entry.get("language", ""),
        "category": entry.get("category", ""),
        "candidate_count": entry["candidate_count"],
        # Copied through, not recomputed: `pipeline --reranks-file` refuses
        # an answer whose candidate list is not the one it built.
        "candidates_hash": entry["candidates_hash"],
        "chosen_index": None if choice is None else choice.index,
        "chosen_canonical_id": None,
        "key_verse_span": span,
        "reason": "" if choice is None else choice.reason,
        "raw": raw[:_MAX_RAW_CHARS],
        "latency_ms": latency_ms,
        "attempts": attempts,
        "error": None if choice is not None else (last_error or "no answer"),
    }
    if choice is not None:
        candidates = entry.get("candidates") or []
        if 0 <= choice.index < len(candidates):
            record["chosen_canonical_id"] = \
                candidates[choice.index]["canonical_id"]
    if choice is not None and span is None and entry.get("key_verses"):
        # The passage choice stands; only the highlight is gone (ADR 0005).
        # A warning, not an error — `--reranks-file` must still use the row.
        record["warning"] = "no usable key-verse span in the answer"
    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Answer the exported rerank inputs with an OpenAI-compatible "
            "chat-completions endpoint."
        )
    )
    parser.add_argument(
        "--endpoint", required=True,
        help="base URL of the OpenAI-compatible API, e.g. "
             "http://localhost:11434/v1 (…/chat/completions is appended)",
    )
    parser.add_argument("--model", required=True, help="model id at that endpoint")
    parser.add_argument(
        "--api-key", default="",
        help="bearer token; falls back to $RERANK_BENCH_API_KEY, then "
             "$REWRITE_BENCH_API_KEY, then $OPENROUTER_API_KEY. Local "
             "endpoints need none.",
    )
    parser.add_argument(
        "--input", required=True,
        help="artifact of `retrieval_benchmark.py export-rerank-input`. A "
             "relative path is resolved against the evaluation/ directory, "
             "like --out.",
    )
    parser.add_argument(
        "--out", required=True,
        help="artifact path (JSON) for `pipeline --reranks-file`. A relative "
             "path is resolved against the evaluation/ directory.",
    )
    parser.add_argument(
        "--only", default="",
        help="comma-separated scenario ids — a probe run over a subset",
    )
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument(
        "--max-tokens", type=int, default=DEFAULT_MAX_TOKENS,
        help="output ceiling per call (the answer itself is a few dozen "
             "tokens; the room is for models that think out loud)",
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    args = parser.parse_args(argv)

    if args.max_tokens < 1:
        parser.error("--max-tokens must be >= 1")
    if not args.api_key:
        args.api_key = (
            os.environ.get("RERANK_BENCH_API_KEY")
            or os.environ.get("REWRITE_BENCH_API_KEY")
            or os.environ.get("OPENROUTER_API_KEY")
            or ""
        )

    payload = json.loads(resolve_path(args.input).read_text(encoding="utf-8"))
    source_meta = payload.get("meta", {})
    entries = payload.get("scenarios", [])
    if not entries:
        parser.error(f"{args.input} holds no scenarios")
    source_version = source_meta.get("rerank_prompt_version")
    if source_version is not None and source_version != RERANK_PROMPT_VERSION:
        print(
            f"  [warn] input was exported with rerank prompt v{source_version}, "
            f"this checkout is v{RERANK_PROMPT_VERSION} — the answers will not "
            f"be comparable with runs of the current prompt",
            flush=True,
        )
    wanted = {s.strip() for s in args.only.split(",") if s.strip()}
    selected = [e for e in entries if not wanted or e["id"] in wanted]
    if wanted:
        missing = wanted - {e["id"] for e in selected}
        if missing:
            parser.error(f"--only names ids absent from {args.input}: "
                         f"{sorted(missing)}")
    if not selected:
        parser.error("no scenarios selected")

    url = completions_url(args.endpoint)
    out = resolve_path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    records: list[dict] = []
    print(
        f"{args.model} @ {endpoint_host(args.endpoint)} — rerank prompt "
        f"v{RERANK_PROMPT_VERSION}, {len(selected)} of {len(entries)} "
        f"exported scenarios",
        flush=True,
    )

    def snapshot() -> None:
        write_artifact(
            out, build_artifact(args, source_meta, records, len(entries))
        )

    # The artifact is rewritten after EVERY scenario, and once more in
    # `finally` — the same lesson gen_rewrites.py learned on 2026-09-04: a
    # partial file that says it is partial beats no file at all.
    try:
        with httpx.Client(timeout=httpx.Timeout(args.timeout)) as client:
            for entry in selected:
                record = generate_one(client, url, args, entry)
                records.append(record)
                mark = "ok " if record["error"] is None else "ERR"
                note = record.get("error") or record.get("warning")
                chosen = (
                    "-" if record["chosen_index"] is None
                    else f"#{record['chosen_index'] + 1}"
                    f"/{record['candidate_count']}"
                    f" {record['chosen_canonical_id'] or ''}"
                )
                print(
                    f"  {mark} {record['id']:<7} {chosen:<28} "
                    f"{record['latency_ms'] / 1000:6.1f}s "
                    f"attempts={record['attempts']}"
                    + (f" — {note}" if note else ""),
                    flush=True,
                )
                snapshot()
    finally:
        snapshot()

    artifact = build_artifact(args, source_meta, records, len(entries))
    failures = sum(1 for r in records if r["error"])
    warnings = sum(1 for r in records if r.get("warning"))
    state = "PARTIAL" if artifact["meta"]["partial"] else "full"
    print(
        f"\nwrote {out} — {state}: {len(records)} of {len(entries)} scenarios, "
        f"{failures} failed, {warnings} without a key-verse span"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
