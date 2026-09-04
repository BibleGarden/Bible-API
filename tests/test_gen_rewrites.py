"""Tests for evaluation/gen_rewrites.py (ClickUp 86cbe4nd3, 86cbed851).

Scope is deliberately narrow: the tool has been exercised by real runs since
86cbe4nd3, and what is guarded here is the one property no run would reveal —
a failed request must not write the endpoint URL into the artifact. The
artifacts (`bench_data/*_rewrites_*.json`) are committed to a PUBLIC
repository, and httpx renders an HTTPStatusError as "... for url '<full
url>'", query string included, which is where an API key lives on several of
the endpoints this tool is pointed at.

No network: the httpx client is a stand-in. The module is loaded by path
because `evaluation/` is deliberately not on pytest's pythonpath.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import httpx
import pytest

EVALUATION = Path(__file__).resolve().parents[1] / "evaluation"
TOOL = EVALUATION / "gen_rewrites.py"

pytestmark = pytest.mark.skipif(
    not TOOL.exists(), reason="evaluation/ is not present in this container copy"
)


def _load_tool():
    # `evaluation/` must be importable: the tool imports rewrite_prompts.
    if str(EVALUATION) not in sys.path:
        sys.path.insert(0, str(EVALUATION))
    spec = importlib.util.spec_from_file_location("gen_rewrites", TOOL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def tool():
    return _load_tool()


SCENARIO = {
    "id": "ru-001",
    "language": "ru",
    "category": "regular",
    "prayer_context": {"topic": "О детях", "user_replies": ["дочь родилась"]},
}


class FakeClient:
    def __init__(self, answers):
        self.answers = list(answers)

    def post(self, url, json=None, headers=None, **_kwargs):
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        raise AssertionError("this test only exercises the failure ladder")


def _args(tool, **overrides):
    defaults = dict(
        endpoint="http://localhost:11434/v1", model="qwen3-4b", api_key="",
        prompt_version="7", variants=6, temperature=0.0,
        max_tokens=tool.DEFAULT_MAX_TOKENS, out="out.json",
        scenarios=str(EVALUATION / "scenarios.json"), only="", timeout=1.0,
    )
    return argparse.Namespace(**{**defaults, **overrides})


def _status_error(url: str, status: int = 400) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", url)
    return httpx.HTTPStatusError(
        f"Client error '{status}' for url '{url}'",
        request=request, response=httpx.Response(status, request=request),
    )


def test_an_http_error_never_leaks_the_endpoint_url_into_the_artifact(
    tool, monkeypatch
):
    monkeypatch.setattr(tool.time, "sleep", lambda _s: None)
    url = "https://llm.example.com/v1/chat/completions?key=SECRET-TOKEN"
    client = FakeClient([_status_error(url)] * tool.TRANSPORT_ATTEMPTS)
    record = tool.generate_one(client, url, _args(tool), SCENARIO)

    assert record["error"] == "transport: HTTPStatusError (HTTP 400)"
    dataset = {"version": "0.7.0", "scenarios": [SCENARIO]}
    artifact = tool.build_artifact(
        _args(tool, endpoint=url, api_key="SECRET-TOKEN"),
        dataset, [record], [SCENARIO])
    dumped = json.dumps(artifact, ensure_ascii=False)
    assert "SECRET-TOKEN" not in dumped
    assert "llm.example.com/v1" not in dumped


def test_transport_error_keeps_the_status_and_drops_the_message(tool):
    assert tool.transport_error(_status_error("http://x?key=S", 429)) == \
        "transport: HTTPStatusError (HTTP 429)"
    assert tool.transport_error(httpx.ConnectError("boom to host 10.0.0.1")) \
        == "transport: ConnectError"


def test_a_short_answer_warning_carries_no_url_either(tool, monkeypatch):
    """`warning` embeds the last provider error, so it needs the same care."""
    monkeypatch.setattr(tool.time, "sleep", lambda _s: None)
    url = "https://llm.example.com/v1/chat/completions?key=SECRET-TOKEN"
    monkeypatch.setattr(
        tool, "call_model",
        lambda *_a, **_k: '{"queries": ["одна переформулировка"]}')
    record = tool.generate_one(FakeClient([]), url, _args(tool), SCENARIO)
    assert record.get("warning")
    assert "SECRET-TOKEN" not in json.dumps(record, ensure_ascii=False)
