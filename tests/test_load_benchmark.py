import importlib.util
import asyncio
import json
from pathlib import Path

import pytest
import httpx


@pytest.fixture(scope="module")
def benchmark():
    path = Path(__file__).parents[1] / "evaluation" / "load_benchmark.py"
    spec = importlib.util.spec_from_file_location("load_benchmark", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_percentile_uses_observed_nearest_rank(benchmark):
    assert benchmark.percentile([4, 1, 3, 2], 0.5) == 2
    assert benchmark.percentile([4, 1, 3, 2], 0.95) == 4


def test_fixture_uses_all_production_prompt_builders(benchmark):
    fixture = __import__("json").loads(
        Path(benchmark.__file__).with_name("load_benchmark_cases.json").read_text()
    )
    stages = set()
    for case in fixture["cases"]:
        instruction, user_content, json_object = benchmark.prompt_for(case)
        stages.add(case["stage"])
        assert instruction and user_content and json_object
    assert stages == {"question", "rewrite", "rerank"}


def test_summary_reports_latency_errors_rate_and_throughput(benchmark):
    records = [
        {
            "ok": True,
            "stage": "question",
            "valid": True,
            "latency_seconds": 2.0,
            "ttft_seconds": 0.2,
            "tokens_per_second": 20.0,
            "batch_elapsed_seconds": 3.0,
        },
        {
            "ok": True,
            "stage": "question",
            "valid": True,
            "latency_seconds": 1.0,
            "ttft_seconds": 0.1,
            "tokens_per_second": 10.0,
            "batch_elapsed_seconds": 3.0,
        },
        {
            "ok": False,
            "stage": "question",
            "latency_seconds": 0.1,
            "batch_elapsed_seconds": 3.0,
        },
    ]
    summary = benchmark.summarize(records)
    assert summary["errors"] == 1
    assert summary["latency_p50_seconds"] == 1.5
    assert summary["latency_p95_seconds"] == 2.0
    assert summary["ttft_p50_seconds"] == pytest.approx(0.15)
    assert summary["tokens_per_second_p50"] == 15.0
    assert summary["throughput_requests_per_second"] == pytest.approx(2 / 3)


def test_checkpoint_is_atomic_and_replaces_previous_result(benchmark, tmp_path):
    target = tmp_path / "result.json"
    benchmark.write_checkpoint(str(target), {"levels": [1]})
    benchmark.write_checkpoint(str(target), {"levels": [1, 2]})
    assert json.loads(target.read_text()) == {"levels": [1, 2]}
    assert not target.with_suffix(".json.tmp").exists()


def test_progress_keeps_each_completed_record(benchmark, tmp_path):
    target = tmp_path / "progress.jsonl"
    benchmark.append_progress(str(target), {"case_id": "a", "ok": True})
    benchmark.append_progress(str(target), {"case_id": "b", "ok": False})
    assert [
        json.loads(line)["case_id"] for line in target.read_text().splitlines()
    ] == ["a", "b"]


def test_stream_extracts_ttft_usage_finish_reason_and_valid_json(benchmark):
    item = '{"ref":"Psalm 23","query":"The Lord is my shepherd"}'
    events = [
        {"choices": [{"delta": {"content": '{"queries": ['}}]},
        {
            "choices": [
                {
                    "delta": {"content": ",".join([item] * 6) + "]}"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"completion_tokens": 12},
        },
    ]
    body = (
        "".join(f"data:{json.dumps(event)}\n\n" for event in events)
        + "data: [DONE]\n\n"
    )
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, text=body)

    case = {
        "id": "rewrite",
        "stage": "rewrite",
        "language": "en",
        "topic": "peace",
        "replies": [],
    }

    async def invoke():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await benchmark.stream_one(
                client,
                "https://model.example/v1/chat/completions",
                "secret",
                "model",
                case,
                256,
            )

    result = asyncio.run(invoke())
    assert calls == 1
    assert result["ok"] and result["valid"]
    assert result["ttft_seconds"] <= result["latency_seconds"]
    assert result["completion_tokens"] == 12
    assert result["finish_reason"] == "stop"


def test_length_finish_is_invalid_and_http_error_is_not_retried(benchmark):
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    case = {
        "id": "question",
        "stage": "question",
        "language": "en",
        "topic": "ordinary day",
        "question_stage": "first",
        "messages": [],
    }

    async def invoke():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await benchmark.stream_one(
                client,
                "https://model.example/v1/chat/completions",
                "secret",
                "model",
                case,
                256,
            )

    result = asyncio.run(invoke())
    assert calls == 1
    assert not result["ok"] and result["error"] == "HTTPStatusError"

    length_body = (
        'data: {"choices":[{"delta":{"content":"{\\"subject\\":\\"day\\",'
        '\\"question\\":\\"What mattered today?\\"}"},"finish_reason":"length"}],'
        '"usage":{"completion_tokens":12}}\n\ndata: [DONE]\n\n'
    )

    async def invoke_length():
        transport = httpx.MockTransport(
            lambda request: httpx.Response(200, text=length_body)
        )
        async with httpx.AsyncClient(transport=transport) as client:
            return await benchmark.stream_one(
                client,
                "https://model.example/v1/chat/completions",
                "secret",
                "model",
                case,
                256,
            )

    length_result = asyncio.run(invoke_length())
    assert length_result["ok"] and not length_result["valid"]
    assert length_result["finish_reason"] == "length"
