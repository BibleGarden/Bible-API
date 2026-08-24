"""Unit tests for the LLM query-reformulation stage of the retrieval
pipeline. No network: Gemini is mocked through httpx.MockTransport."""

import json
import os

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")

from query_rewrite import (
    GeminiQueryRewriter,
    QueryRewriteError,
    REWRITE_VARIANTS,
    build_rewrite_instruction,
    build_rewrite_user_content,
    build_search_query,
    parse_rewrite_response,
)


# ---------------------------------------------------------------------------
# Raw query building
# ---------------------------------------------------------------------------

def test_search_query_joins_topic_and_replies():
    query = build_search_query("Тема", ["ответ один", " ответ два "])
    assert query == "Тема\nответ один\nответ два"


def test_search_query_skips_blank_parts():
    assert build_search_query("  ", ["", "   "]) == ""
    assert build_search_query("Тема", []) == "Тема"


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

def test_instruction_mentions_language_and_variant_count():
    text = build_rewrite_instruction("ru", 6)
    assert "Russian" in text
    assert "exactly 6" in text


def test_instruction_rejects_unknown_language():
    with pytest.raises(KeyError):
        build_rewrite_instruction("de")


def test_user_content_contains_topic_and_replies():
    content = build_rewrite_user_content("Умерла мама", ["Больно.", ""])
    assert content == "Topic: Умерла мама\nRemarks:\n- Больно."


def test_user_content_without_replies_has_no_remarks_block():
    assert build_rewrite_user_content("дякую", []) == "Topic: дякую"


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def test_parse_plain_json():
    text = json.dumps({"queries": ["один", "два"]})
    assert parse_rewrite_response(text) == ["один", "два"]


def test_parse_tolerates_fences_and_prose():
    text = 'Вот ответ:\n```json\n{"queries": ["q1", "q2"]}\n```\nГотово.'
    assert parse_rewrite_response(text) == ["q1", "q2"]


def test_parse_deduplicates_and_normalizes_whitespace():
    text = json.dumps({"queries": ["a  b", "a b", "  ", "c"]})
    assert parse_rewrite_response(text) == ["a b", "c"]


def test_parse_skips_non_strings_and_truncates_to_variants():
    queries = [str(i) for i in range(10)]
    text = json.dumps({"queries": [None, 42] + queries})
    assert parse_rewrite_response(text, variants=4) == ["0", "1", "2", "3"]


def test_parse_rejects_junk():
    for bad in ("no json here", "{broken", '{"queries": "not-a-list"}',
                '{"queries": []}', '{"queries": [null]}'):
        with pytest.raises(QueryRewriteError):
            parse_rewrite_response(bad)


# ---------------------------------------------------------------------------
# Rewriter client (mocked transport)
# ---------------------------------------------------------------------------

def make_rewriter(handler, **kwargs) -> GeminiQueryRewriter:
    transport = httpx.MockTransport(handler)
    return GeminiQueryRewriter(
        api_key=kwargs.pop("api_key", "test-key"),
        model=kwargs.pop("model", "gemini-test"),
        http_client=httpx.Client(transport=transport),
        **kwargs,
    )


def gemini_response(queries) -> dict:
    return {
        "candidates": [{
            "content": {"parts": [{"text": json.dumps({"queries": queries})}]},
        }],
    }


def test_rewrite_returns_parsed_variants():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        captured["url"] = str(request.url)
        return httpx.Response(200, json=gemini_response(["в1", "в2"]))

    rewriter = make_rewriter(handler)
    assert rewriter.rewrite("ru", "Тема", ["ответ"]) == ["в1", "в2"]
    assert "gemini-test:generateContent" in captured["url"]
    body = captured["body"]
    assert body["generationConfig"]["responseMimeType"] == "application/json"
    assert body["generationConfig"]["temperature"] == 0.0
    user_text = body["contents"][0]["parts"][0]["text"]
    assert "Тема" in user_text and "ответ" in user_text


def test_rewrite_requires_api_key():
    rewriter = make_rewriter(lambda r: httpx.Response(200), api_key="")
    with pytest.raises(QueryRewriteError, match="not configured"):
        rewriter.rewrite("ru", "Тема", [])


def test_rewrite_rejects_bad_model_name():
    rewriter = make_rewriter(lambda r: httpx.Response(200), model="bad model!")
    with pytest.raises(QueryRewriteError, match="invalid characters"):
        rewriter.rewrite("ru", "Тема", [])


def test_rewrite_rejects_unknown_language():
    rewriter = make_rewriter(lambda r: httpx.Response(200))
    with pytest.raises(QueryRewriteError, match="unsupported language"):
        rewriter.rewrite("de", "Thema", [])


def test_rewrite_retries_transient_errors(monkeypatch):
    import query_rewrite as module

    monkeypatch.setattr(module.time, "sleep", lambda _s: None)
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(429, json={"error": {}})
        return httpx.Response(200, json=gemini_response(["в1"]))

    rewriter = make_rewriter(handler)
    assert rewriter.rewrite("uk", "тема", []) == ["в1"]
    assert calls["n"] == 3


def test_rewrite_fails_after_retries_without_leaking_context(monkeypatch):
    import query_rewrite as module

    monkeypatch.setattr(module.time, "sleep", lambda _s: None)
    rewriter = make_rewriter(lambda r: httpx.Response(503))
    with pytest.raises(QueryRewriteError) as exc_info:
        rewriter.rewrite("ru", "секретная тема молитвы", [])
    # failure category only — never the prayer context
    assert "секретная" not in str(exc_info.value)


def test_rewrite_fails_on_empty_candidates():
    rewriter = make_rewriter(
        lambda r: httpx.Response(200, json={"candidates": []})
    )
    with pytest.raises(QueryRewriteError, match="no candidates"):
        rewriter.rewrite("en", "topic", [])


def test_rewrite_fails_on_hard_http_error():
    rewriter = make_rewriter(lambda r: httpx.Response(400, json={}))
    with pytest.raises(QueryRewriteError, match="request failed"):
        rewriter.rewrite("en", "topic", [])


def test_default_variant_count_is_requested():
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        system = body["system_instruction"]["parts"][0]["text"]
        assert f"exactly {REWRITE_VARIANTS}" in system
        return httpx.Response(200, json=gemini_response(["q"]))

    make_rewriter(handler).rewrite("en", "topic", [])
