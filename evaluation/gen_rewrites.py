#!/usr/bin/env python3
"""
Generate rewrite variants for the evaluation set with ANY OpenAI-compatible
chat-completions endpoint (ClickUp 86cbe4nd3).

Produces the `--rewrites-file` artifact that `retrieval_benchmark.py pipeline`
consumes, in the format of `bench_data/qwen_rewrites_v070.json`. Until now this
file was made by one-off scripts on other machines (86cbbm70n via a local Qwen,
86cbbmhk8 via OpenRouter); this is the same protocol, in the repository.

Protocol (identical to those packages, so the artifacts stay comparable):
system = the rewrite instruction, user = the prayer context, temperature 0,
max_tokens 8192 (see DEFAULT_MAX_TOKENS — the one value local runs lower),
response_format json_object; invalid JSON is retried once, transport failures
up to three attempts; wall time of each scenario recorded.
The prompt is not re-typed here — the production one (v8, registered as "8c")
comes from `app/query_rewrite.py`, and the frozen historical 7/8a/8b are built
in `rewrite_prompts.py`.

`--via-app` swaps this tool's own chat request for the PRODUCTION rewriter
(`OpenAICompatQueryRewriter`): same prompt, same parser — including its
bounded JSON repair — and the same retry ladder the endpoint runs with. Use it
when the question is "what would the deployed service do with this model"; the
default `raw` path stays for the historical prompts, which the application
cannot produce.

`empty` scenarios are skipped: production answers them from the safe pool
before the rewrite stage ever runs (README, resolved question 5).

Examples (local endpoints want `--max-tokens` lowered — see DEFAULT_MAX_TOKENS):

    # local Ollama (OpenAI-compatible surface)
    python gen_rewrites.py --endpoint http://localhost:11434/v1 \\
        --model qwen3:4b-instruct-2507-q4_K_M --prompt-version 8c \\
        --max-tokens 1024 \\
        --out bench_data/qwen3-4b_rewrites_v070_p8c.json

    # Maria's 30B over the network
    python gen_rewrites.py --endpoint http://<host>/v1 \\
        --model qwen3-30b-a3b-instruct-2507 --prompt-version 8c \\
        --max-tokens 1024 \\
        --out bench_data/qwen30b_rewrites_v070_p8c.json

    # exactly what the deployed service would produce with that model
    python gen_rewrites.py --endpoint https://<host>/v1 --via-app \\
        --model qwen3-30b-a3b-instruct-2507 --prompt-version 8c \\
        --out bench_data/qwen30b_rewrites_v070_p8app.json

    # the same for the Gemini transport (no endpoint: it is a constant)
    python gen_rewrites.py --via-app --provider gemini \\
        --model gemini-3.7-flash --prompt-version 8c \\
        --out bench_data/flash37_rewrites_v070_p8app.json
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
# `query_rewrite` imports it at module level. This tool talks to its own
# endpoint and never reads a single one of these values — they exist only so
# the import succeeds outside the container, exactly as tests/conftest.py does
# it. `setdefault`, so a real environment still wins.
for _name, _value in (
    ("API_KEY", "gen-rewrites-unused"),
    ("DB_HOST", "gen-rewrites-unused"),
    ("DB_USER", "gen-rewrites-unused"),
    ("DB_PASSWORD", "gen-rewrites-unused"),
    ("DB_NAME", "gen-rewrites-unused"),
    ("EMBEDDING_MODEL", "gemini-embedding-001"),
    ("EMBEDDING_DIMENSIONS", "768"),
    # Required in every deployment since ADR 0010, and this tool computes no
    # vectors at all — the stub only has to let `config` import.
    ("EMBEDDING_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_REWRITE_MODEL", "gemini-3.7-flash"),
    ("AI_SCRIPTURE_RERANK_MODEL", "gemini-3.5-flash-lite"),
    ("AI_QUESTION_MODEL", "gemini-3.5-flash-lite"),
    ("AI_TRANSCRIBE_MODEL", "gemini-3.5-flash-lite"),
    # Required together once the AI surface is configured at all (ADR 0009),
    # which a `GEMINI_API_KEY` in the caller's environment does. This tool
    # never reads them: `--via-app` builds its `StageProvider` from the
    # command line, precisely so a benchmark run cannot depend on which
    # provider the machine's `.env` happens to name today.
    ("AI_QUESTION_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_REWRITE_PROVIDER", "gemini"),
    ("AI_SCRIPTURE_RERANK_PROVIDER", "gemini"),
):
    os.environ.setdefault(_name, _value)

from query_rewrite import (  # noqa: E402
    REWRITE_PROMPT_VERSION,
    REWRITE_VARIANTS,
    GeminiQueryRewriter,
    OpenAICompatQueryRewriter,
    QueryRewriteError,
    build_rewrite_user_content,
)
from config import StageProvider  # noqa: E402

# The prompt version this tool asks for when it runs through the application's
# own rewriter (`--via-app`): the name under which the production instruction
# is registered in `rewrite_prompts`. Named once, so the check below cannot
# drift from what `build_instruction` returns.
PRODUCTION_PROMPT_NAME = "8c"

import rewrite_prompts  # noqa: E402

SCENARIOS_FILE = HERE / "scenarios.json"
# Protocol default, carried over from `GeminiQueryRewriter.rewrite`
# (`maxOutputTokens`). It is generous because a *thinking* Gemini model spends
# most of it on hidden reasoning. A non-thinking local model needs ~250 tokens
# for six queries — and on a llama.cpp server with context shifting an
# unreachable ceiling turns a degenerate repetition into an endless one, so
# local runs are expected to lower it (`--max-tokens`); the artifact records
# the value actually used.
DEFAULT_MAX_TOKENS = 8192
# One attempt may legitimately take minutes on a CPU-only local model.
DEFAULT_TIMEOUT_SECONDS = 600.0
TRANSPORT_ATTEMPTS = 3
# A syntactically broken answer is retried once — the same allowance the
# earlier packages gave (README, "Как сгенерированы переформулировки").
PARSE_ATTEMPTS = 2

# Some small models emit a reasoning block before the answer; the JSON
# extraction is greedy, so strip it rather than let a brace inside the
# reasoning swallow the real object.
_THINK_BLOCK = re.compile(r"<think>.*?</think>", flags=re.DOTALL | re.IGNORECASE)


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
    """Relative paths are relative to evaluation/, not to the caller's cwd.

    Both `--out` and `--scenarios` go through this, so the same string means
    the same file whichever directory the script is invoked from.
    """
    path = Path(value)
    return path if path.is_absolute() else HERE / path


PARTIAL_NOTE = (
    "PARTIAL run: fewer scenarios than the evaluation set has non-empty ones "
    "(--only, or the run was interrupted). Aggregates are NOT comparable with "
    "full-set artifacts, and `pipeline --rewrites-file` will treat every "
    "missing scenario as a rewrite failure."
)


def build_artifact(
    args: argparse.Namespace,
    dataset: dict,
    records: list[dict],
    eligible: list[dict],
) -> dict:
    """The artifact document, including whether this run is partial.

    `partial` is derived, never passed in by hand: a run is full only when it
    produced a record for every non-`empty` scenario of the set.
    """
    covered = [r["id"] for r in records]
    partial = len(covered) < len(eligible)
    meta = {
        "model": args.model,
        "date": date.today().isoformat(),
        "prompt_version": args.prompt_version,
        "prompt_revision": rewrite_prompts.PROMPT_REVISIONS[args.prompt_version],
        # The production prompt these variants are derived from.
        "rewrite_prompt_version": REWRITE_PROMPT_VERSION,
        "scenarios_version": dataset["version"],
        # Gemini has no configurable endpoint: the URL is a constant of
        # `app/query_rewrite.py`, so the provider name is what identifies it.
        "endpoint": endpoint_host(args.endpoint) or "provider:gemini",
        "provider": args.provider if args.via_app else "openai_compat",
        "sampling": {
            "temperature": args.temperature,
            "max_tokens": args.max_tokens,
            "response_format": "json_object",
            "variants": args.variants,
        },
        # Which code path produced the answers: "app" means the production
        # rewriter (class, prompt, parser and retry ladder of
        # `app/query_rewrite.py`), "raw" the tool's own chat call. The two ask
        # the same model the same question; only "app" also measures the
        # production parser, so a run that claims the production path has to
        # say so in the artifact.
        "transport": "app" if args.via_app else "raw",
        "partial": partial,
        "scenarios_covered": covered,
        "scenarios_expected": len(eligible),
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
        raise QueryRewriteError("response has no choices") from exc
    text = message.get("content") or ""
    if not isinstance(text, str) or not text.strip():
        # Some providers put the answer in a reasoning field when the visible
        # content came back empty; that is a failure, not something to guess.
        raise QueryRewriteError("response content is empty")
    return _THINK_BLOCK.sub("", text)


def build_app_rewriter(args: argparse.Namespace):
    """The PRODUCTION rewriter of the chosen provider (`--via-app`).

    Not a re-implementation of the stage and not a copy of its settings: the
    class, the instruction, the parser and the retry ladder are the ones
    `POST /api/ai/scripture` runs with for that
    `AI_SCRIPTURE_REWRITE_PROVIDER`. Only the stage configuration is built
    here instead of read from the environment, so one machine can benchmark
    several endpoints and providers without rewriting its `.env`.

    The two classes are the same duck (ADR 0009) and send the same prompt, so
    the caller does not branch — which is the property this option exists to
    measure in the first place.
    """
    if args.provider == "gemini":
        return GeminiQueryRewriter(
            api_key=args.api_key,
            model=args.model,
            variants=args.variants,
            timeout=args.timeout,
        )
    return OpenAICompatQueryRewriter(
        StageProvider(
            stage="scripture_rewrite",
            provider="openai_compat",
            model=args.model,
            endpoint=args.endpoint,
            api_key=args.api_key,
        ),
        variants=args.variants,
        timeout=args.timeout,
    )


def generate_via_app(
    rewriter: OpenAICompatQueryRewriter,
    args: argparse.Namespace,
    scenario: dict,
) -> dict:
    """One scenario through the application's own rewrite stage.

    The record has the same shape as `generate_one`'s, minus `refs`: the
    production parser reads the reference field and drops it, and inventing a
    second parser here to recover it would defeat the purpose of measuring the
    production path. `attempts` is 1 because the ladder lives inside the
    stage — `QueryRewriteError` is what a production caller would see, and it
    is recorded as the failure category it is.
    """
    context = scenario["prayer_context"]
    started = time.monotonic()
    variants: list[str] = []
    error = ""
    try:
        variants = rewriter.rewrite(
            scenario["language"],
            context["topic"],
            list(context.get("user_replies") or []),
        )
    except QueryRewriteError as exc:
        # The stage's own message is already a category, never the prayer
        # text and never a URL (app/llm_client.transport_error).
        error = f"app: {exc}"
    latency_ms = int((time.monotonic() - started) * 1000)
    record: dict = {
        "id": scenario["id"],
        "language": scenario["language"],
        "variants": variants,
        "latency_ms": latency_ms,
        "attempts": 1,
        "error": None if variants else (error or "no variants"),
    }
    if variants and len(variants) < args.variants:
        record["warning"] = (
            f"short answer: {len(variants)} of {args.variants} variants"
        )
    leaks = sorted({
        leak
        for variant in variants
        for leak in rewrite_prompts.find_reference_leaks(variant)
    })
    if leaks:
        record["reference_leaks"] = leaks
    return record


def generate_one(
    client: httpx.Client,
    url: str,
    args: argparse.Namespace,
    scenario: dict,
) -> dict:
    """Variants for one scenario, with the retry ladder and wall time."""
    language = scenario["language"]
    context = scenario["prayer_context"]
    instruction = rewrite_prompts.build_instruction(
        args.prompt_version, language, args.variants
    )
    user_content = build_rewrite_user_content(
        context["topic"], list(context.get("user_replies") or [])
    )

    started = time.monotonic()
    attempts = 0
    parse_failures = 0
    transport_failures = 0
    last_error = ""
    variants: list[str] = []
    refs: list[str] = []

    while attempts < TRANSPORT_ATTEMPTS + PARSE_ATTEMPTS:
        attempts += 1
        try:
            text = call_model(
                client, url, args.api_key, args.model,
                instruction, user_content, args.temperature, args.max_tokens,
            )
        except (httpx.HTTPError, ValueError) as exc:
            transport_failures += 1
            last_error = transport_error(exc)
            if transport_failures >= TRANSPORT_ATTEMPTS:
                break
            time.sleep(2.0 * transport_failures)
            continue
        except QueryRewriteError as exc:
            parse_failures += 1
            last_error = f"empty: {exc}"
            if parse_failures >= PARSE_ATTEMPTS:
                break
            continue
        try:
            variants, refs = rewrite_prompts.parse_response(
                args.prompt_version, text, args.variants
            )
            break
        except QueryRewriteError as exc:
            parse_failures += 1
            last_error = f"parse: {exc}"
            if parse_failures >= PARSE_ATTEMPTS:
                break

    latency_ms = int((time.monotonic() - started) * 1000)
    record: dict = {
        "id": scenario["id"],
        "language": language,
        "variants": variants,
        "latency_ms": latency_ms,
        "attempts": attempts,
        "error": None if variants else (last_error or "no variants"),
    }
    if variants and len(variants) < args.variants:
        # Fewer variants than asked for is not a clean success — the fused
        # top-10 is built from all of them, so five of six quietly changes what
        # is measured. It goes in `warning`, NOT in `error`, on purpose:
        # `retrieval_benchmark._load_external_rewrites` drops every row whose
        # `error` is set, so writing it there would silently throw away usable
        # variants and turn the scenario into a rewrite failure.
        record["warning"] = (
            f"short answer: {len(variants)} of {args.variants} variants"
            + (f"; last provider error: {last_error}" if last_error else "")
        )
    if args.prompt_version in ("8a", "8c"):
        record["refs"] = refs
    leaks = sorted({
        leak
        for variant in variants
        for leak in rewrite_prompts.find_reference_leaks(variant)
    })
    if leaks:
        record["reference_leaks"] = leaks
    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate rewrite variants for evaluation/scenarios.json with an "
            "OpenAI-compatible chat-completions endpoint."
        )
    )
    parser.add_argument(
        "--endpoint", default="",
        help="base URL of the OpenAI-compatible API, e.g. "
             "http://localhost:11434/v1 (…/chat/completions is appended). "
             "Required except with --via-app --provider gemini, whose URL is "
             "a constant of the stage module.",
    )
    parser.add_argument(
        "--provider", default="openai_compat", choices=("openai_compat", "gemini"),
        help="with --via-app: which production rewriter to build. Ignored "
             "otherwise — the raw path speaks OpenAI's protocol only.",
    )
    parser.add_argument("--model", required=True, help="model id at that endpoint")
    parser.add_argument(
        "--api-key", default="",
        help="bearer token; falls back to $REWRITE_BENCH_API_KEY, then "
             "$OPENROUTER_API_KEY. Local endpoints need none.",
    )
    parser.add_argument(
        "--prompt-version", default=PRODUCTION_PROMPT_NAME,
        choices=rewrite_prompts.PROMPT_VERSIONS,
        help="8c = the production prompt (v8, app/query_rewrite.py); 7/8a/8b "
             "= the frozen historical texts of the prompt matrix "
             "(evaluation/rewrite_prompts.py)",
    )
    parser.add_argument("--variants", type=int, default=REWRITE_VARIANTS)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument(
        "--max-tokens", type=int, default=DEFAULT_MAX_TOKENS,
        help="output ceiling per call. The 8192 default mirrors the "
             "production Gemini call; lower it (e.g. 1024) for a "
             "non-thinking local model, where an unreachable ceiling lets a "
             "degenerate repetition run forever.",
    )
    parser.add_argument(
        "--out", required=True,
        help="artifact path (JSON). A relative path is resolved against the "
             "evaluation/ directory, like --scenarios.",
    )
    parser.add_argument(
        "--scenarios", default=str(SCENARIOS_FILE),
        help="scenario set. A relative path is resolved against the "
             "evaluation/ directory, like --out.",
    )
    parser.add_argument(
        "--only", default="",
        help="comma-separated scenario ids — a probe run over a subset",
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--via-app", action="store_true",
        help="call the PRODUCTION rewriter (app.query_rewrite."
             "OpenAICompatQueryRewriter) instead of this tool's own chat "
             f"request: same prompt, same parser, same retry ladder as "
             f"POST /api/ai/scripture. Requires --prompt-version "
             f"{PRODUCTION_PROMPT_NAME} (the production instruction) and "
             f"ignores --temperature/--max-tokens, which the stage pins.",
    )
    args = parser.parse_args(argv)

    if args.variants < 1:
        parser.error("--variants must be >= 1")
    if args.max_tokens < 1:
        parser.error("--max-tokens must be >= 1")
    gemini_app = args.via_app and args.provider == "gemini"
    if not args.endpoint and not gemini_app:
        parser.error("--endpoint is required (except --via-app --provider gemini)")
    if args.provider == "gemini" and not args.via_app:
        parser.error("--provider gemini only makes sense with --via-app")
    if args.via_app and args.prompt_version != PRODUCTION_PROMPT_NAME:
        # No silent substitution: the app sends the production prompt whatever
        # this flag says, so a run labelled with another version would be a
        # lie in the artifact.
        parser.error(
            f"--via-app runs the production instruction; use "
            f"--prompt-version {PRODUCTION_PROMPT_NAME} or drop --via-app"
        )
    if not args.api_key:
        args.api_key = (
            os.environ.get("REWRITE_BENCH_API_KEY")
            or (os.environ.get("GEMINI_API_KEY") if gemini_app else "")
            or os.environ.get("OPENROUTER_API_KEY")
            or ""
        )

    dataset = json.loads(resolve_path(args.scenarios).read_text(encoding="utf-8"))
    # Everything the rewrite stage would ever be asked for: `empty` never
    # reaches it in production (safe pool), so a run that covers every other
    # scenario is a FULL run even though the set has more entries.
    eligible = [s for s in dataset["scenarios"] if s["category"] != "empty"]
    wanted = {s.strip() for s in args.only.split(",") if s.strip()}
    scenarios = [s for s in eligible if not wanted or s["id"] in wanted]
    if wanted:
        missing = wanted - {s["id"] for s in scenarios}
        if missing:
            parser.error(
                f"--only names ids that are absent or empty-category: "
                f"{sorted(missing)}"
            )
    if not scenarios:
        parser.error("no scenarios selected")

    url = completions_url(args.endpoint) if args.endpoint else ""
    out = resolve_path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    records: list[dict] = []
    print(
        f"{args.model} @ {endpoint_host(args.endpoint) or args.provider} — prompt "
        f"v{args.prompt_version}, {args.variants} variants, "
        f"{len(scenarios)} of {len(eligible)} eligible scenarios"
        + (" — through the production rewriter" if args.via_app else ""),
        flush=True,
    )

    def snapshot() -> None:
        write_artifact(out, build_artifact(args, dataset, records, eligible))

    def report(record: dict) -> None:
        mark = "ok " if record["error"] is None else "ERR"
        note = record.get("error") or record.get("warning")
        print(
            f"  {mark} {record['id']:<7} {len(record['variants'])} "
            f"variants {record['latency_ms'] / 1000:6.1f}s "
            f"attempts={record['attempts']}"
            + (f" — {note}" if note else ""),
            flush=True,
        )

    # The artifact is rewritten after EVERY scenario, and once more in
    # `finally`. Reason: an interrupted run used to leave nothing at all on
    # disk, and a completed 21-scenario run was lost that way (86cbe4nd3).
    # A partial file that says it is partial beats no file.
    try:
        if args.via_app:
            with build_app_rewriter(args) as rewriter:
                for scenario in scenarios:
                    record = generate_via_app(rewriter, args, scenario)
                    records.append(record)
                    report(record)
                    snapshot()
        else:
            with httpx.Client(timeout=httpx.Timeout(args.timeout)) as client:
                for scenario in scenarios:
                    record = generate_one(client, url, args, scenario)
                    records.append(record)
                    report(record)
                    snapshot()
    finally:
        snapshot()

    artifact = build_artifact(args, dataset, records, eligible)
    failures = sum(1 for r in records if r["error"])
    warnings = sum(1 for r in records if r.get("warning"))
    state = "PARTIAL" if artifact["meta"]["partial"] else "full"
    print(
        f"\nwrote {out} — {state}: {len(records)} of {len(eligible)} scenarios, "
        f"{failures} failed, {warnings} short"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
