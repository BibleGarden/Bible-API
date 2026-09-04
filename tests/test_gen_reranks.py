"""Tests for evaluation/gen_reranks.py (ClickUp 86cbed851).

The tool answers the exported rerank inputs with an arbitrary
OpenAI-compatible endpoint and writes the `--reranks-file` artifact. Two
things must hold whatever the model says:

* an answer is accepted only if the PRODUCTION validator
  (`passage_rerank.parse_rerank_response`) accepts it against the exported
  candidate count — otherwise the file could carry a choice the service
  would have refused, and the benchmark would score a passage the model
  never legally chose;
* a failure is recorded as a failure (`error`) instead of a guess, because
  `pipeline --reranks-file` degrades exactly those rows to retrieval rank-1,
  the way production degrades on `PassageRerankError`.

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
TOOL = EVALUATION / "gen_reranks.py"

pytestmark = pytest.mark.skipif(
    not TOOL.exists(), reason="evaluation/ is not present in this container copy"
)


def _load_tool():
    spec = importlib.util.spec_from_file_location("gen_reranks", TOOL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def tool():
    return _load_tool()


ENTRY = {
    "id": "ru-001",
    "language": "ru",
    "category": "regular",
    "candidate_count": 3,
    "candidates_hash": "0123456789abcdef",
    "key_verses": True,
    "candidates": [
        {"number": 1, "canonical_id": "v3:19.127.001-005"},
        {"number": 2, "canonical_id": "v3:19.023.001-006"},
        {"number": 3, "canonical_id": "v3:40.011.027-030"},
    ],
    "instruction": "system instruction",
    "user_content": "prayer context and candidates",
}


class FakeResponse:
    def __init__(self, text: str):
        self._text = text

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"choices": [{"message": {"content": self._text}}]}


class FakeClient:
    """Returns the queued answers in order; records what was sent."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.requests = []

    def post(self, url, json=None, headers=None, **_kwargs):
        self.requests.append({"url": url, "payload": json, "headers": headers})
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return FakeResponse(answer)


def _args(tool, **overrides):
    defaults = dict(
        endpoint="http://localhost:11434/v1", model="qwen3-4b", api_key="",
        temperature=0.0, max_tokens=tool.DEFAULT_MAX_TOKENS,
        input="bench_data/rerank_input_flash37.json", out="out.json", only="",
        timeout=1.0,
    )
    return argparse.Namespace(**{**defaults, **overrides})


def test_a_valid_answer_is_recorded_with_its_span(tool):
    client = FakeClient([json.dumps({
        "candidate": 2, "key_verse_start": 1, "key_verse_end": 2,
        "reason": "speaks to the situation",
    })])
    record = tool.generate_one(client, "http://x/chat/completions",
                               _args(tool), ENTRY)
    assert record["error"] is None
    # the artifact stores the 0-based index the benchmark indexes with
    assert record["chosen_index"] == 1
    assert record["chosen_canonical_id"] == "v3:19.023.001-006"
    assert record["key_verse_span"] == [1, 2]
    assert record["attempts"] == 1
    assert record["candidates_hash"] == ENTRY["candidates_hash"]
    assert "warning" not in record


def test_the_request_carries_the_exported_prompt_and_json_mode(tool):
    client = FakeClient([json.dumps({"candidate": 1, "reason": "x"})])
    tool.generate_one(client, "http://x/chat/completions",
                      _args(tool, api_key="secret"), ENTRY)
    payload = client.requests[0]["payload"]
    assert payload["messages"] == [
        {"role": "system", "content": "system instruction"},
        {"role": "user", "content": "prayer context and candidates"},
    ]
    assert payload["temperature"] == 0.0
    assert payload["response_format"] == {"type": "json_object"}
    assert client.requests[0]["headers"]["Authorization"] == "Bearer secret"


def test_a_broken_answer_is_retried_once_and_then_recorded_as_an_error(tool):
    client = FakeClient(["not json at all", "still not json"])
    record = tool.generate_one(client, "http://x/chat/completions",
                               _args(tool), ENTRY)
    assert record["chosen_index"] is None
    assert record["error"].startswith("parse:")
    assert record["attempts"] == tool.PARSE_ATTEMPTS == 2
    # the raw answer is kept so the failure can be read afterwards
    assert record["raw"] == "still not json"


def test_a_broken_answer_followed_by_a_good_one_succeeds(tool):
    client = FakeClient(["oops", json.dumps({"candidate": 3, "reason": "y"})])
    record = tool.generate_one(client, "http://x/chat/completions",
                               _args(tool), ENTRY)
    assert record["chosen_index"] == 2 and record["error"] is None
    assert record["attempts"] == 2


def test_an_out_of_range_candidate_is_refused_like_in_production(tool):
    # 4 of 3 candidates: parse_rerank_response rejects it, so it never
    # reaches the artifact as a choice.
    answer = json.dumps({"candidate": 4, "reason": "z"})
    record = tool.generate_one(FakeClient([answer, answer]),
                               "http://x/chat/completions", _args(tool), ENTRY)
    assert record["chosen_index"] is None
    assert "outside 1..3" in record["error"]


def test_a_missing_key_verse_span_is_a_warning_not_an_error(tool):
    # ADR 0005: a broken highlight never invalidates the passage choice.
    client = FakeClient([json.dumps({"candidate": 1, "reason": "ok"})])
    record = tool.generate_one(client, "http://x/chat/completions",
                               _args(tool), ENTRY)
    assert record["error"] is None
    assert record["key_verse_span"] is None
    assert record["warning"]


def test_transport_failures_use_the_three_attempt_ladder(tool, monkeypatch):
    monkeypatch.setattr(tool.time, "sleep", lambda _s: None)
    client = FakeClient([httpx.ConnectError("boom")] * tool.TRANSPORT_ATTEMPTS)
    record = tool.generate_one(client, "http://x/chat/completions",
                               _args(tool), ENTRY)
    assert record["attempts"] == tool.TRANSPORT_ATTEMPTS == 3
    assert record["error"].startswith("transport:")


def _status_error(url: str, status: int = 400) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", url)
    return httpx.HTTPStatusError(
        f"Client error '{status}' for url '{url}'",
        request=request, response=httpx.Response(status, request=request),
    )


def test_an_http_error_never_leaks_the_endpoint_url_into_the_artifact(
    tool, monkeypatch
):
    """The artifact is committed to a PUBLIC repo; the URL can carry a key.

    httpx puts the full request URL into an HTTPStatusError's message, so
    recording `str(exc)` would write `?key=…` straight into the file.
    """
    monkeypatch.setattr(tool.time, "sleep", lambda _s: None)
    url = "https://llm.example.com/v1/chat/completions?key=SECRET-TOKEN"
    client = FakeClient([_status_error(url)] * tool.TRANSPORT_ATTEMPTS)
    record = tool.generate_one(client, url, _args(tool), ENTRY)

    assert record["error"] == "transport: HTTPStatusError (HTTP 400)"
    artifact = tool.build_artifact(
        _args(tool, endpoint=url, api_key="SECRET-TOKEN"), {}, [record], 21)
    dumped = json.dumps(artifact, ensure_ascii=False)
    assert "SECRET-TOKEN" not in dumped
    assert "llm.example.com/v1" not in dumped


def test_transport_error_keeps_the_status_and_drops_the_message(tool):
    assert tool.transport_error(_status_error("http://x?key=S", 429)) == \
        "transport: HTTPStatusError (HTTP 429)"
    assert tool.transport_error(httpx.ConnectError("boom to host 10.0.0.1")) \
        == "transport: ConnectError"


def test_a_reasoning_block_before_the_json_is_stripped(tool):
    client = FakeClient([
        '<think>{"candidate": 9}</think>{"candidate": 1, "reason": "r"}'
    ])
    record = tool.generate_one(client, "http://x/chat/completions",
                               _args(tool), ENTRY)
    assert record["chosen_index"] == 0 and record["error"] is None


def test_partial_is_derived_and_the_key_never_reaches_the_file(tool):
    args = _args(tool, api_key="secret-token",
                 endpoint="https://llm.example.com/v1?key=secret-token")
    source_meta = {"source_results": "bench_data/results.json",
                   "rerank_prompt_version": tool.RERANK_PROMPT_VERSION,
                   "scenarios_version": "0.7.0"}
    artifact = tool.build_artifact(args, source_meta, [{"id": "ru-001"}], 21)
    assert artifact["meta"]["partial"] is True
    assert artifact["meta"]["note"].startswith("PARTIAL run")
    assert artifact["meta"]["endpoint"] == "https://llm.example.com"
    assert "secret-token" not in json.dumps(artifact)

    full = tool.build_artifact(args, source_meta, [{"id": "ru-001"}], 1)
    assert full["meta"]["partial"] is False and "note" not in full["meta"]


def test_the_artifact_is_written_atomically(tool, tmp_path):
    out = tmp_path / "reranks.json"
    tool.write_artifact(out, {"meta": {}, "scenarios": []})
    assert json.loads(out.read_text()) == {"meta": {}, "scenarios": []}
    assert not (tmp_path / "reranks.json.tmp").exists()
