#!/usr/bin/env python3
"""
Generate answers of the "leading question" feature (`POST /api/ai/question`)
for the evaluation inputs, on either provider (ClickUp 86cbegctz).

The endpoint is a single provider call: `system_instruction` =
`app/question_prompt.build_question_prompt(safety.detect_language(message))`,
`contents` = the one user message the app sends (it packs the WHOLE
conversation into that one string), temperature 0.7, maxOutputTokens 1024.
This tool reproduces exactly that call against two providers so the answers
are comparable:

* `--provider gemini` — the production path, byte for byte: the same REST URL
  shape, the same payload, the same `x-goog-api-key` header as
  `app/twinkler_ai.complete`. Model and key come from the environment
  (`AI_QUESTION_MODEL`, `GEMINI_API_KEY`); when they are not exported, `.env`
  of the repository is parsed INTO MEMORY only (never copied, never logged).
* `--provider qwen` — the same system/user pair through an OpenAI-compatible
  `/chat/completions` endpoint (Maria's Qwen3-30B behind the SSH tunnel), the
  protocol `gen_rewrites.py` / `gen_reranks.py` already use.

The prompt itself is NOT re-typed here: it is imported from `app/`, so a
change of the production wording changes this measurement too.

Inputs are the approved benchmark set (`scenarios.json`, topic + replies
joined by newlines — that is what "the whole conversation" looks like) plus
`question_probe_inputs.json`, which covers what the scripture set does not
(despair, one word, pure joy, a conversation with a previous question in it).
`empty` scenarios are SKIPPED and reported as skipped: `CompleteRequest.user`
declares `min_length=1`, so an empty message never reaches the model at all —
FastAPI answers 422 before the handler runs.

Artifacts: one JSONL row per (input, sample) plus a `<out>.meta.json`
sidecar. Neither ever carries a key: transport failures are recorded as a
category and, for an HTTP error, a status code — never the URL.

Examples:

    # Maria's 30B over the tunnel (key read by the caller, see
    # run_local_picker_qwen.sh — it must never land in a file)
    python gen_questions.py --provider qwen \\
        --endpoint https://llm.ai2.ru:8443/v1 \\
        --model qwen3-30b-a3b-instruct-2507 --api-key "$KEY" \\
        --out bench_data/questions_qwen30b_v1.jsonl

    # production Gemini, free tier: 15 requests per minute
    python gen_questions.py --provider gemini --sleep 4.5 \\
        --out bench_data/questions_gemini35lite_v1.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

import httpx

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "app"))

# `question_prompt` imports nothing — not even `config` — which is exactly why
# it was split out of `twinkler_ai.py` (see its module docstring). So the
# production prompt can be imported here without the fail-fast environment
# dance the other generators need. `safety` imports only `prompt_safety`, both
# of them from `app/`, for the same reason: the language the prompt names is
# resolved by THE production detector, never by a copy of it living here.
from question_prompt import (  # noqa: E402
    QUESTION_PROMPT_VERSION,
    build_question_prompt,
)
from safety import check_input, detect_language  # noqa: E402

SCENARIOS_FILE = HERE / "scenarios.json"
PROBE_FILE = HERE / "question_probe_inputs.json"
ENV_FILE = HERE.parent / ".env"

# The production values of `app/twinkler_ai.complete`. They are the
# measurement, not a knob: changing one makes the run incomparable with the
# endpoint, so they are constants with CLI overrides only where a provider
# forces one.
TEMPERATURE = 0.7
MAX_OUTPUT_TOKENS = 1024
GEMINI_URL_TEMPLATE = (
    "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
)
# `complete()` uses 20 s. A CPU/GPU-shared local model over an SSH tunnel is
# slower than the production provider, so the default is generous and the
# value actually used is recorded in the sidecar.
DEFAULT_TIMEOUT_SECONDS = 180.0
TRANSPORT_ATTEMPTS = 3
# Free-tier Gemini is 15 requests per minute; a 429 wants more than the
# ordinary backoff, and burning the daily quota on retries helps nobody.
RATE_LIMIT_PAUSE_SECONDS = 65.0


def endpoint_host(endpoint: str) -> str:
    """Host of the endpoint, never its query string (keys can live there)."""
    parts = urlsplit(endpoint)
    return f"{parts.scheme}://{parts.netloc}" if parts.netloc else endpoint


def transport_error(exc: Exception) -> str:
    """Failure category — never the provider's message and never a URL.

    httpx renders an HTTPStatusError as "... for url '<full url>'"; these
    artifacts are committed to a PUBLIC repository, so only the exception
    type and, for an HTTP error, the status code are recorded.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return f"transport: HTTPStatusError (HTTP {exc.response.status_code})"
    return f"transport: {type(exc).__name__}"


def is_rate_limited(exc: Exception) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code == 429
    )


def resolve_path(value: str) -> Path:
    """Relative paths are relative to evaluation/, not to the caller's cwd."""
    path = Path(value)
    return path if path.is_absolute() else HERE / path


def meta_path(out: Path) -> Path:
    """Sidecar holding the run's metadata (same convention as gen_descriptions)."""
    return out.with_name(out.name + ".meta.json")


def write_meta(out: Path, meta: dict) -> None:
    """Write the sidecar atomically: a crash must not truncate a good one."""
    path = meta_path(out)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    tmp.replace(path)


def append_record(out: Path, record: dict) -> None:
    """Append one row and get it onto the disk.

    One `write` of a complete line followed by fsync, so an interrupted run
    leaves whole records behind it — a 90-call run against a metered provider
    must never be lost to a crash at call 89.
    """
    with out.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def read_env_file(path: Path) -> dict[str, str]:
    """`.env` as a dict, in memory only.

    Read, never written; the caller pulls out the two values it needs and
    nothing is echoed. Deliberately minimal parsing (KEY=VALUE, `#` comments,
    optional surrounding quotes) — this is not a dotenv implementation, it is
    a way to run the production call outside the container.
    """
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def gemini_settings() -> tuple[str, str]:
    """(model, key) for the production call — environment first, then `.env`.

    Not a silent fallback: both places are named in the error when the value
    is missing, and the run aborts rather than quietly measuring a different
    model (the rule that came out of 2026-08-29).
    """
    env_file = read_env_file(ENV_FILE)
    model = os.environ.get("AI_QUESTION_MODEL") or env_file.get("AI_QUESTION_MODEL", "")
    key = os.environ.get("GEMINI_API_KEY") or env_file.get("GEMINI_API_KEY", "")
    missing = [
        name
        for name, value in (("AI_QUESTION_MODEL", model), ("GEMINI_API_KEY", key))
        if not value
    ]
    if missing:
        raise SystemExit(
            f"{', '.join(missing)} not set: export it, or put it in {ENV_FILE}"
        )
    return model, key


def conversation_text(context: dict) -> str:
    """The one string the app sends: the topic and every reply, one per line.

    `POST /api/ai/question` takes a single `user` field and the prompt says so
    ("The incoming message may contain the whole conversation so far"), so the
    scenario's structured context is flattened the same way here.
    """
    parts = [context.get("topic") or ""]
    parts.extend(context.get("user_replies") or [])
    return "\n".join(part for part in parts if part.strip())


def load_inputs(scenarios_path: Path, probes_path: Path) -> tuple[list[dict], list[dict]]:
    """(inputs to send, inputs skipped with a reason).

    Two kinds of input are skipped, both because the endpoint itself never
    sends them to a provider — measuring them would measure a request the app
    cannot make:

    * `empty` scenarios: the request model declares
      `user: str = Field(min_length=1, ...)`, so an empty conversation is
      rejected with 422 by FastAPI before the handler runs. Sending a space
      instead would not be the same request.
    * anything `safety.check_input` catches (tier 1): since ClickUp 86cbegg23
      the despair rule is code, so `probe-despair` is answered by
      `app/safety.py` with the fixed reply and the model is never called. It
      stayed in the v1 measurement because the rule was still an instruction
      in the prompt then; from v2 it is not, and a model answer to it would
      grade a code path the model no longer has.
    """
    dataset = json.loads(scenarios_path.read_text(encoding="utf-8"))
    probes = json.loads(probes_path.read_text(encoding="utf-8"))
    inputs: list[dict] = []
    skipped: list[dict] = []

    def answered_in_code(entry: dict) -> bool:
        finding = check_input(entry["text"])
        if not finding.matched:
            return False
        skipped.append({
            "id": entry["id"],
            "reason": (
                f"app/safety.py answers this in code (tier {finding.tier}, "
                f"pattern {finding.pattern_id}): POST /api/ai/question returns "
                "the fixed reply and never calls a provider, so there is no "
                "model answer to measure"
            ),
        })
        return True

    for scenario in dataset["scenarios"]:
        text = conversation_text(scenario["prayer_context"])
        entry = {
            "id": scenario["id"],
            "source": "scenarios",
            "language": scenario["language"],
            "category": scenario["category"],
            "expect_question": True,
            "text": text,
        }
        if scenario["category"] == "empty" or not text:
            skipped.append({
                "id": scenario["id"],
                "reason": (
                    "empty conversation: CompleteRequest.user has min_length=1, "
                    "so POST /api/ai/question answers 422 before the model is "
                    "called — there is nothing to measure"
                ),
            })
            continue
        if answered_in_code(entry):
            continue
        inputs.append(entry)

    for probe in probes["inputs"]:
        entry = {
            "id": probe["id"],
            "source": "probes",
            "language": probe["language"],
            "category": probe["category"],
            "expect_question": bool(probe.get("expect_question", True)),
            "text": probe["text"],
        }
        if probe.get("avoid_question"):
            entry["avoid_question"] = probe["avoid_question"]
        if answered_in_code(entry):
            continue
        inputs.append(entry)

    return inputs, skipped


def call_qwen(
    client: httpx.Client, url: str, api_key: str, model: str, user: str, prompt: str
) -> str:
    """One OpenAI-compatible chat completion with the production pair."""
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": user},
        ],
        "temperature": TEMPERATURE,
        "max_tokens": MAX_OUTPUT_TOKENS,
    }
    response = client.post(url, json=payload, headers=headers)
    response.raise_for_status()
    data = response.json()
    try:
        message = data["choices"][0]["message"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("response has no choices") from exc
    text = message.get("content") or ""
    if not isinstance(text, str) or not text.strip():
        raise ValueError("response content is empty")
    return text.strip()


def call_gemini(
    client: httpx.Client, url: str, api_key: str, user: str, prompt: str
) -> str:
    """One `:generateContent` call — the payload of `twinkler_ai.complete`."""
    payload = {
        "system_instruction": {"parts": [{"text": prompt}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": {
            "maxOutputTokens": MAX_OUTPUT_TOKENS,
            "temperature": TEMPERATURE,
        },
    }
    response = client.post(url, json=payload, headers={"x-goog-api-key": api_key})
    response.raise_for_status()
    data = response.json()
    # `twinkler_ai._extract_text`, kept in sync deliberately: a run must fail
    # the same way production fails, not more forgivingly.
    try:
        parts = data["candidates"][0]["content"]["parts"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("response has no candidates") from exc
    if not isinstance(parts, list):
        raise ValueError("response parts are not a list")
    text = "".join(
        part.get("text", "")
        for part in parts
        if isinstance(part, dict) and isinstance(part.get("text"), str)
    ).strip()
    if not text:
        raise ValueError("response text is empty")
    return text


def generate_one(
    client: httpx.Client,
    args: argparse.Namespace,
    url: str,
    api_key: str,
    model: str,
    entry: dict,
    sample: int,
) -> dict:
    """One sample for one input, with the transport ladder and wall time.

    The prompt is built per input, exactly as `twinkler_ai.complete` builds
    it: since v2 it names the language the detector resolved for THIS message
    (`None` — the detector has no evidence — is a case the prompt itself
    handles, see `question_prompt.UNDETERMINED_LANGUAGE`).
    """
    started = time.monotonic()
    attempts = 0
    last_error = ""
    text = ""
    prompt_language = detect_language(entry["text"])
    prompt = build_question_prompt(prompt_language)

    while attempts < TRANSPORT_ATTEMPTS:
        attempts += 1
        try:
            if args.provider == "gemini":
                text = call_gemini(client, url, api_key, entry["text"], prompt)
            else:
                text = call_qwen(client, url, api_key, model, entry["text"], prompt)
            break
        except (httpx.HTTPError, ValueError) as exc:
            last_error = transport_error(exc) if isinstance(exc, httpx.HTTPError) \
                else f"empty: {exc}"
            if attempts >= TRANSPORT_ATTEMPTS:
                break
            time.sleep(
                RATE_LIMIT_PAUSE_SECONDS if is_rate_limited(exc) else 2.0 * attempts
            )

    record = {
        "id": entry["id"],
        "sample": sample,
        "source": entry["source"],
        "language": entry["language"],
        "category": entry["category"],
        "expect_question": entry["expect_question"],
        "provider": args.provider,
        "model": model,
        # What the prompt was told to answer in, `null` when the detector had
        # no evidence — the one number that says whether a language violation
        # is the model's or the detector's.
        "prompt_language": prompt_language,
        "prompt_version": QUESTION_PROMPT_VERSION,
        "text": text,
        "latency_ms": int((time.monotonic() - started) * 1000),
        "attempts": attempts,
        "error": None if text else (last_error or "no text"),
    }
    if entry.get("avoid_question"):
        record["avoid_question"] = entry["avoid_question"]
    return record


def build_meta(
    args: argparse.Namespace,
    model: str,
    url_host: str,
    inputs: list[dict],
    skipped: list[dict],
    records: list[dict],
) -> dict:
    expected = len(inputs) * args.samples
    return {
        "provider": args.provider,
        "model": model,
        "endpoint": url_host,
        "date": date.today().isoformat(),
        "ticket": "ClickUp 86cbegctz, 86cbegg3f",
        "prompt": "app/question_prompt.build_question_prompt(detect_language(message))",
        "prompt_version": QUESTION_PROMPT_VERSION,
        "scenarios_file": args.scenarios,
        "probes_file": args.probes,
        "sampling": {
            "temperature": TEMPERATURE,
            "max_output_tokens": MAX_OUTPUT_TOKENS,
            "samples_per_input": args.samples,
            "timeout_seconds": args.timeout,
            "sleep_between_calls_seconds": args.sleep,
        },
        "inputs_sent": len(inputs),
        "inputs_skipped": skipped,
        "records_expected": expected,
        "records_written": len(records),
        "records_failed": sum(1 for r in records if r["error"]),
        "partial": len(records) < expected,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate `POST /api/ai/question` answers for the evaluation "
            "inputs on Gemini (production) or an OpenAI-compatible endpoint."
        )
    )
    parser.add_argument("--provider", required=True, choices=("qwen", "gemini"))
    parser.add_argument(
        "--endpoint", default="",
        help="qwen only: base URL of the OpenAI-compatible API "
             "(…/chat/completions is appended)",
    )
    parser.add_argument(
        "--model", default="",
        help="qwen only: model id at that endpoint. For gemini the model is "
             "AI_QUESTION_MODEL, exactly as production reads it.",
    )
    parser.add_argument(
        "--api-key", default="",
        help="qwen only: bearer token; falls back to $QWEN_API_KEY. For "
             "gemini the key is GEMINI_API_KEY (environment or .env).",
    )
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument(
        "--sleep", type=float, default=0.0,
        help="pause between calls, seconds. Free-tier Gemini allows 15 "
             "requests per minute, so use 4.5 there.",
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--out", required=True,
        help="JSONL artifact; metadata goes to <out>.meta.json. A relative "
             "path is resolved against the evaluation/ directory.",
    )
    parser.add_argument("--scenarios", default="scenarios.json")
    parser.add_argument("--probes", default="question_probe_inputs.json")
    parser.add_argument(
        "--only", default="",
        help="comma-separated input ids — a probe run over a subset",
    )
    args = parser.parse_args(argv)

    if args.samples < 1:
        parser.error("--samples must be >= 1")

    api_key = ""
    if args.provider == "gemini":
        if args.endpoint or args.model or args.api_key:
            parser.error(
                "--endpoint/--model/--api-key are qwen options; the gemini run "
                "must use the production model and key (AI_QUESTION_MODEL, "
                "GEMINI_API_KEY) or it measures something else"
            )
        model, api_key = gemini_settings()
        url = GEMINI_URL_TEMPLATE.format(model=model)
        url_host = "https://generativelanguage.googleapis.com"
    else:
        if not args.endpoint or not args.model:
            parser.error("--endpoint and --model are required for --provider qwen")
        model = args.model
        api_key = args.api_key or os.environ.get("QWEN_API_KEY", "")
        base = args.endpoint.rstrip("/")
        url = base if base.endswith("/chat/completions") else f"{base}/chat/completions"
        url_host = endpoint_host(args.endpoint)

    inputs, skipped = load_inputs(
        resolve_path(args.scenarios), resolve_path(args.probes)
    )
    wanted = {value.strip() for value in args.only.split(",") if value.strip()}
    if wanted:
        inputs = [entry for entry in inputs if entry["id"] in wanted]
        missing = wanted - {entry["id"] for entry in inputs}
        if missing:
            parser.error(f"--only names unknown or skipped ids: {sorted(missing)}")
    if not inputs:
        parser.error("no inputs selected")

    out = resolve_path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        parser.error(f"{out} exists — move it aside rather than mixing two runs")

    records: list[dict] = []
    print(
        f"{args.provider}: {model} @ {url_host} — {len(inputs)} inputs x "
        f"{args.samples} samples, {len(skipped)} inputs skipped",
        flush=True,
    )

    try:
        with httpx.Client(timeout=httpx.Timeout(args.timeout)) as client:
            for entry in inputs:
                for sample in range(1, args.samples + 1):
                    record = generate_one(
                        client, args, url, api_key, model, entry, sample
                    )
                    records.append(record)
                    append_record(out, record)
                    mark = "ok " if record["error"] is None else "ERR"
                    preview = record["text"].replace("\n", " ⏎ ")[:70]
                    print(
                        f"  {mark} {record['id']:<14} #{sample} "
                        f"{record['latency_ms'] / 1000:5.1f}s  "
                        f"{preview or record['error']}",
                        flush=True,
                    )
                    write_meta(
                        out, build_meta(args, model, url_host, inputs, skipped, records)
                    )
                    if args.sleep > 0:
                        time.sleep(args.sleep)
    finally:
        write_meta(out, build_meta(args, model, url_host, inputs, skipped, records))

    failed = sum(1 for record in records if record["error"])
    print(f"\nwrote {out} — {len(records)} records, {failed} failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
