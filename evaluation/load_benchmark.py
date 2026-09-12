#!/usr/bin/env python3
"""Bounded streaming benchmark for an OpenAI-compatible chat server.

It uses production prompt builders, performs no retries, and writes no prompt
or answer text to the result. Credentials are read only from the environment.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import statistics
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from llm_client import completions_url  # noqa: E402
from passage_rerank import (  # noqa: E402
    build_rerank_instruction,
    build_rerank_user_content,
)
from passage_rerank import parse_rerank_response  # noqa: E402
from question_format import parse_question  # noqa: E402
from question_prompt import build_question_prompt, build_user_message  # noqa: E402
from query_rewrite import (  # noqa: E402
    build_rewrite_instruction,
    build_rewrite_user_content,
)
from query_rewrite import parse_rewrite_response  # noqa: E402


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return math.nan
    return ordered[math.ceil(fraction * len(ordered)) - 1]


def prompt_for(case: dict) -> tuple[str, str, bool]:
    stage = case["stage"]
    if stage == "question":
        return (
            build_question_prompt(case.get("language")),
            build_user_message(
                case["topic"],
                case["question_stage"],
                case.get("messages", []),
                language=case.get("language"),
            ),
            True,
        )
    if stage == "rewrite":
        return (
            build_rewrite_instruction(case["language"]),
            build_rewrite_user_content(case["topic"], case.get("replies", [])),
            True,
        )
    if stage == "rerank":
        count = len(case["candidates"])
        return (
            build_rerank_instruction(count),
            build_rerank_user_content(
                case["topic"], case.get("replies", []), case["candidates"]
            ),
            True,
        )
    raise ValueError(f"unknown stage: {stage}")


async def stream_one(
    client: httpx.AsyncClient,
    url: str,
    key: str,
    model: str,
    case: dict,
    max_tokens: int,
) -> dict:
    instruction, user_content, json_object = prompt_for(case)
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": instruction},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.7 if case["stage"] == "question" else 0,
        "max_tokens": max_tokens,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    if json_object:
        payload["response_format"] = {"type": "json_object"}
    started = time.perf_counter()
    first_token = None
    completion_tokens = None
    chunks = 0
    content_parts = []
    finish_reason = None
    try:
        async with client.stream(
            "POST", url, headers={"Authorization": f"Bearer {key}"}, json=payload
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if (
                    not line.startswith("data:")
                    or line.removeprefix("data:").strip() == "[DONE]"
                ):
                    continue
                event = json.loads(line.removeprefix("data:").lstrip())
                usage = event.get("usage") or {}
                if usage.get("completion_tokens") is not None:
                    completion_tokens = int(usage["completion_tokens"])
                choices = event.get("choices") or []
                content = (
                    choices[0].get("delta", {}).get("content") if choices else None
                )
                if choices and choices[0].get("finish_reason") is not None:
                    finish_reason = choices[0]["finish_reason"]
                if content:
                    content_parts.append(content)
                    chunks += 1
                    if first_token is None:
                        first_token = time.perf_counter()
        ended = time.perf_counter()
        if first_token is None:
            raise RuntimeError("stream contained no content")
        text = "".join(content_parts)
        try:
            contract_valid = True
            if case["stage"] == "question":
                parsed = parse_question(text)
                valid = True
                contract_valid = parsed.parsed
            elif case["stage"] == "rewrite":
                parse_rewrite_response(text)
                valid = True
            else:
                parse_rerank_response(text, len(case["candidates"]))
                valid = True
        except Exception:
            valid = False
            contract_valid = False
        generation_seconds = max(ended - first_token, 1e-9)
        return {
            "case_id": case["id"],
            "stage": case["stage"],
            "ok": True,
            "ttft_seconds": first_token - started,
            "latency_seconds": ended - started,
            "completion_tokens": completion_tokens,
            "content_chunks": chunks,
            "finish_reason": finish_reason,
            "valid": valid and finish_reason != "length",
            "contract_valid": contract_valid and finish_reason != "length",
            "tokens_per_second": (
                completion_tokens / generation_seconds
                if completion_tokens is not None
                else None
            ),
        }
    except Exception as exc:
        return {
            "case_id": case["id"],
            "stage": case["stage"],
            "ok": False,
            "error": type(exc).__name__,
            "latency_seconds": time.perf_counter() - started,
        }


def summarize(records: list[dict]) -> dict:
    records = [record for record in records if not record.get("skipped")]
    good = [record for record in records if record["ok"]]
    latencies = [record["latency_seconds"] for record in good]
    ttfts = [record["ttft_seconds"] for record in good]
    rates = [
        record["tokens_per_second"] for record in good if record["tokens_per_second"]
    ]
    elapsed = max((record["batch_elapsed_seconds"] for record in records), default=0)
    result = {
        "requests": len(records),
        "errors": len(records) - len(good),
        "latency_p50_seconds": statistics.median(latencies) if latencies else None,
        "latency_p95_seconds": percentile(latencies, 0.95) if latencies else None,
        "ttft_p50_seconds": statistics.median(ttfts) if ttfts else None,
        "ttft_p95_seconds": percentile(ttfts, 0.95) if ttfts else None,
        "tokens_per_second_p50": statistics.median(rates) if rates else None,
        "throughput_requests_per_second": len(good) / elapsed if elapsed else None,
        "invalid_responses": sum(not record.get("valid", False) for record in good),
        "contract_violations": sum(
            not record.get("contract_valid", False) for record in good
        ),
        "finish_reasons": {
            reason: sum(record.get("finish_reason") == reason for record in good)
            for reason in sorted(
                {
                    record.get("finish_reason")
                    for record in good
                    if record.get("finish_reason")
                }
            )
        },
    }
    stages = sorted({record["stage"] for record in records})
    if len(stages) > 1:
        result["by_stage"] = {
            stage: summarize([record for record in records if record["stage"] == stage])
            for stage in stages
        }
    return result


def write_checkpoint(path: str, payload: dict) -> None:
    """Atomically preserve every completed level, including abort evidence."""
    target = Path(path)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(target)


def append_progress(path: str, record: dict) -> None:
    """Append one sanitized completed request so interruption loses no evidence."""
    with Path(path).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        stream.flush()


async def run(args: argparse.Namespace) -> dict:
    key = os.environ.get(args.api_key_env)
    endpoint = os.environ.get(args.endpoint_env)
    if not key:
        raise SystemExit(f"missing {args.api_key_env}")
    if not endpoint:
        raise SystemExit(f"missing {args.endpoint_env}")
    fixture = json.loads(Path(args.cases).read_text())
    cases = fixture["cases"]
    levels = [int(value) for value in args.concurrency.split(",")]
    if not cases or args.requests <= 0 or any(level <= 0 for level in levels):
        raise SystemExit("cases, requests and concurrency must be positive")
    if args.abort_factor <= 1:
        raise SystemExit("--abort-factor must be greater than 1")
    output = {
        "meta": {
            "ticket": "86cbh0p8t",
            "model": args.model,
            "endpoint_host": httpx.URL(endpoint).host,
            "workload": "application prompts",
            "max_tokens": args.max_tokens,
            "requests_per_level": args.requests,
            "warmup": "one excluded request per case",
        },
        "levels": [],
    }
    progress_path = args.output + ".progress.jsonl"
    Path(progress_path).write_text("")
    baseline_p95 = None
    timeout = httpx.Timeout(args.timeout)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # vLLM's first pass over new prompt text is not reproducible. Warm each
        # case once, identically for every model, and exclude it from results.
        for case in cases:
            async with asyncio.timeout(args.hard_timeout):
                warm = await stream_one(
                    client,
                    completions_url(endpoint),
                    key,
                    args.model,
                    case,
                    args.max_tokens,
                )
            if not warm["ok"] or not warm.get("valid"):
                output["meta"]["aborted"] = f"warm-up failed for {case['id']}"
                output["meta"]["warmup_failure"] = warm
                write_checkpoint(args.output, output)
                return output
        for concurrency in levels:
            semaphore = asyncio.Semaphore(concurrency)
            stop = asyncio.Event()
            selected = [cases[index % len(cases)] for index in range(args.requests)]
            batch_started = time.perf_counter()

            async def bounded(case: dict) -> dict:
                async with semaphore:
                    if stop.is_set():
                        return {
                            "case_id": case["id"],
                            "stage": case["stage"],
                            "ok": False,
                            "skipped": True,
                        }
                    try:
                        async with asyncio.timeout(args.hard_timeout):
                            record = await stream_one(
                                client,
                                completions_url(endpoint),
                                key,
                                args.model,
                                case,
                                args.max_tokens,
                            )
                            if not record["ok"] or not record.get("valid"):
                                stop.set()
                            append_progress(
                                progress_path, {"concurrency": concurrency, **record}
                            )
                            return record
                    except TimeoutError:
                        stop.set()
                        record = {
                            "case_id": case["id"],
                            "stage": case["stage"],
                            "ok": False,
                            "error": "HardTimeout",
                            "latency_seconds": args.hard_timeout,
                        }
                        append_progress(
                            progress_path, {"concurrency": concurrency, **record}
                        )
                        return record

            records = await asyncio.gather(*(bounded(case) for case in selected))
            batch_elapsed = time.perf_counter() - batch_started
            for record in records:
                record["batch_elapsed_seconds"] = batch_elapsed
            summary = summarize(records)
            output["levels"].append(
                {"concurrency": concurrency, "summary": summary, "records": records}
            )
            write_checkpoint(args.output, output)
            print(
                json.dumps({"concurrency": concurrency, **summary}, ensure_ascii=False),
                flush=True,
            )
            if summary["errors"] or summary["invalid_responses"]:
                output["meta"]["aborted"] = "request error or invalid response"
                break
            if baseline_p95 is None:
                baseline_p95 = summary["latency_p95_seconds"]
            elif summary["latency_p95_seconds"] > baseline_p95 * args.abort_factor:
                output["meta"]["aborted"] = "latency degradation"
                break
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument(
        "--cases", default=str(Path(__file__).with_name("load_benchmark_cases.json"))
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--endpoint-env", default="AI_OPENAI_COMPAT_ENDPOINT")
    parser.add_argument("--api-key-env", default="AI_OPENAI_COMPAT_API_KEY")
    parser.add_argument("--concurrency", default="1,2,4")
    parser.add_argument("--requests", type=int, default=6)
    parser.add_argument("--max-tokens", type=int, default=1024)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--hard-timeout", type=float, default=75)
    parser.add_argument("--abort-factor", type=float, default=5)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    result = asyncio.run(run(arguments))
    write_checkpoint(arguments.output, result)
