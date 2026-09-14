"""Tests for the OpenAI-compatible transport and the per-stage provider
switch (ClickUp 86cbegg2f, ADR 0009).

Three things are pinned here:

1. the transport itself — payload, `<think>` stripping, retries, budget, and
   the rule that no failure message ever carries the key or the URL;
2. the **tripwire**: with the three CHAT stages on `openai_compat` and
   `EMBEDDING_PROVIDER=local`, not one request of a whole selection leaves
   for a Gemini host — asserted on the hostname each stage's client actually
   dials, not on configuration. Step 3 (ClickUp 86cbegg2r, ADR 0010) widened
   it from the chat stages to everything: with local embeddings there is no
   Google host left in the picture at all;
3. **parity**: the same model answer, delivered through either provider's
   response envelope, produces the same parsed result — and the prompt bytes
   the two transports send are identical.

No network: both providers are `httpx.MockTransport`.
"""

import asyncio
import json
import logging
import os

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")

import config
import embeddings
import llm_client
import passage_rerank
import query_rewrite
import twinkler_ai
from deadline import Deadline
from embeddings import build_embedding_client
from llm_client import (
    AsyncChatClient,
    ChatClient,
    LLMError,
    auth_headers,
    build_payload,
    completions_url,
    content_of,
    strip_think,
)
from passage_rerank import (
    OpenAICompatPassageReranker,
    RerankChoice,
    build_passage_reranker,
    build_rerank_instruction,
    build_rerank_user_content,
)
from query_rewrite import (
    OpenAICompatQueryRewriter,
    build_query_rewriter,
    build_rewrite_instruction,
    build_rewrite_user_content,
)
from question_prompt import build_question_prompt, build_user_message
from safety import detect_language

ENDPOINT = "https://llm.example:8443/v1"
GEMINI_HOST = "generativelanguage.googleapis.com"
SECRET_KEY = "sk-do-not-print-me"


def question_language(source: str, code: str) -> twinkler_ai.ResolvedQuestionLanguage:
    return twinkler_ai.ResolvedQuestionLanguage(source, code)


def stage(name: str, model: str = "qwen3-30b", **kwargs) -> config.StageProvider:
    provider = kwargs.pop("provider", config.PROVIDER_OPENAI_COMPAT)
    reasoning_effort = kwargs.pop(
        "reasoning_effort",
        (
            "omit"
            if provider == config.PROVIDER_OPENAI_COMPAT
            else "none" if provider == config.PROVIDER_OPENROUTER else None
        ),
    )
    return config.StageProvider(
        stage=name,
        provider=provider,
        model=model,
        endpoint=kwargs.pop("endpoint", ENDPOINT),
        api_key=kwargs.pop("api_key", SECRET_KEY),
        reasoning_effort=reasoning_effort,
    )


def chat_response(content: str) -> dict:
    """The OpenAI-compatible envelope around one answer."""
    return {"choices": [{"message": {"role": "assistant", "content": content}}]}


def gemini_response(content: str) -> dict:
    """The Gemini envelope around the very same answer."""
    return {"candidates": [{"content": {"parts": [{"text": content}]}}]}


def mock_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Request shape
# ---------------------------------------------------------------------------

def test_completions_url_accepts_both_spellings():
    assert completions_url(ENDPOINT) == f"{ENDPOINT}/chat/completions"
    assert completions_url(ENDPOINT + "/") == f"{ENDPOINT}/chat/completions"
    assert (
        completions_url(f"{ENDPOINT}/chat/completions")
        == f"{ENDPOINT}/chat/completions"
    )


def test_auth_header_is_omitted_for_an_unauthenticated_endpoint():
    assert auth_headers("k")["Authorization"] == "Bearer k"
    # An empty key is a statement, not a reason to send an empty bearer.
    assert "Authorization" not in auth_headers("")


def test_payload_carries_the_json_contract_only_when_asked():
    payload = build_payload(
        "m",
        "system",
        "user",
        temperature=0.0,
        max_tokens=1024,
        json_object=True,
        reasoning_effort="omit",
    )
    assert payload["messages"] == [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "user"},
    ]
    assert payload["response_format"] == {"type": "json_object"}
    assert payload["temperature"] == 0.0 and payload["max_tokens"] == 1024
    assert "provider" not in payload
    assert "reasoning" not in payload
    prose = build_payload(
        "m",
        "s",
        "u",
        temperature=0.7,
        max_tokens=8,
        json_object=False,
        reasoning_effort="omit",
    )
    assert "response_format" not in prose


def test_transport_and_config_share_the_literal_reasoning_contract():
    expected = ("omit", "none", "low", "medium", "high")
    assert config.REASONING_EFFORTS == expected
    assert llm_client.REASONING_EFFORTS == expected


@pytest.mark.parametrize("effort", ["none", "low", "medium", "high"])
def test_payload_sends_each_reasoning_effort_literal_exactly(effort):
    payload = build_payload(
        "m",
        "s",
        "u",
        temperature=0.0,
        max_tokens=8,
        json_object=False,
        reasoning_effort=effort,
    )
    assert payload["reasoning_effort"] == effort


def test_payload_omit_is_an_explicit_instruction_not_to_send_the_field():
    payload = build_payload(
        "m",
        "s",
        "u",
        temperature=0.0,
        max_tokens=8,
        json_object=False,
        reasoning_effort="omit",
    )
    assert "reasoning_effort" not in payload


def test_openrouter_payload_has_the_fixed_strict_policy_and_reasoning_off():
    payload = build_payload(
        "google/gemma-4-31b-it",
        "system",
        "user",
        temperature=0.7,
        max_tokens=4096,
        json_object=True,
        reasoning_effort="none",
        request_profile="openrouter",
    )
    assert payload == {
        "model": "google/gemma-4-31b-it",
        "messages": [
            {"role": "system", "content": "system"},
            {"role": "user", "content": "user"},
        ],
        "temperature": 0.7,
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
        "provider": {
            "allow_fallbacks": False,
            "data_collection": "deny",
        },
        "reasoning": {"enabled": False},
    }
    assert "reasoning_effort" not in payload


@pytest.mark.parametrize("reasoning_effort", ["omit", "low", "medium", "high"])
def test_openrouter_payload_rejects_any_non_disabled_reasoning(reasoning_effort):
    with pytest.raises(ValueError, match="requires reasoning_effort=none"):
        build_payload(
            "google/gemma-4-31b-it",
            "s",
            "u",
            temperature=0.7,
            max_tokens=4096,
            json_object=True,
            reasoning_effort=reasoning_effort,
            request_profile="openrouter",
        )


def test_payload_rejects_an_unknown_reasoning_effort():
    with pytest.raises(ValueError, match="reasoning_effort"):
        build_payload(
            "m",
            "s",
            "u",
            temperature=0.0,
            max_tokens=8,
            json_object=False,
            reasoning_effort="default",
        )


def test_the_client_sends_model_prompt_and_key():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("Authorization")
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=chat_response('{"ok": true}'))

    client = ChatClient(
        ENDPOINT,
        SECRET_KEY,
        "qwen3-30b",
        "none",
        http_client=mock_client(handler),
    )
    assert client.complete("instruction", "content") == '{"ok": true}'
    assert captured["url"] == f"{ENDPOINT}/chat/completions"
    assert captured["auth"] == f"Bearer {SECRET_KEY}"
    assert captured["body"]["model"] == "qwen3-30b"
    assert captured["body"]["reasoning_effort"] == "none"


@pytest.mark.parametrize(
    ("field", "value"),
    [("endpoint", ""), ("model", ""), ("reasoning_effort", "")],
)
def test_an_unconfigured_client_refuses_before_it_dials(field, value):
    kwargs = {
        "endpoint": ENDPOINT,
        "api_key": "k",
        "model": "m",
        "reasoning_effort": "omit",
        field: value,
    }
    client = ChatClient(
        kwargs["endpoint"],
        kwargs["api_key"],
        kwargs["model"],
        kwargs["reasoning_effort"],
        http_client=mock_client(lambda r: httpx.Response(200)),
    )
    with pytest.raises(LLMError, match="not configured"):
        client.complete("i", "u")


# ---------------------------------------------------------------------------
# Answer extraction
# ---------------------------------------------------------------------------

def test_think_blocks_are_stripped():
    assert strip_think("<think>reasoning</think>\n{\"a\": 1}") == '{"a": 1}'
    assert strip_think("<THINK>a\nb</THINK> tail") == "tail"
    assert strip_think("plain") == "plain"


def test_a_reasoning_block_cannot_swallow_the_json_object():
    """The production parsers extract `{...}` greedily, so a brace inside the
    reasoning would take the closing brace of the real object with it."""
    answer = '<think>maybe {"candidate": 9}?</think>{"candidate": 2, "reason": "fits"}'
    client = ChatClient(
        ENDPOINT, "k", "m", "omit",
        http_client=mock_client(lambda r: httpx.Response(200, json=chat_response(answer))),
    )
    text = client.complete("i", "u")
    assert passage_rerank.parse_rerank_response(text, 5).index == 1


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"choices": []},
        {"choices": [{}]},
        {"choices": [{"message": {"content": ""}}]},
        {"choices": [{"message": {"content": "   "}}]},
        {"choices": [{"message": {"content": None}}]},
        {"choices": [{"message": {"content": "<think>only reasoning</think>"}}]},
    ],
)
def test_an_unusable_answer_is_an_error_not_an_empty_string(payload):
    with pytest.raises(LLMError):
        content_of(payload)


# ---------------------------------------------------------------------------
# Retries, budget and privacy
# ---------------------------------------------------------------------------

def test_retries_transient_statuses_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, json=chat_response("done"))

    client = ChatClient(
        ENDPOINT, "k", "m", "omit", http_client=mock_client(handler),
        attempts=3, sleep=lambda _s: None,
    )
    assert client.complete("i", "u") == "done"
    assert calls["n"] == 3


def test_gives_up_after_the_configured_attempts():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(429, json={})

    client = ChatClient(
        ENDPOINT, "k", "m", "omit", http_client=mock_client(handler),
        attempts=2, sleep=lambda _s: None,
    )
    with pytest.raises(LLMError, match="after retries"):
        client.complete("i", "u")
    assert calls["n"] == 2


def test_an_exhausted_budget_starts_no_call():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=chat_response("x"))

    client = ChatClient(
        ENDPOINT, "k", "m", "omit", http_client=mock_client(handler)
    )
    with pytest.raises(LLMError, match="budget exhausted"):
        client.complete("i", "u", deadline=Deadline(0.0))
    assert calls["n"] == 0


def test_one_call_is_capped_across_all_four_httpx_phases():
    """A bare number would authorise the budget FOUR times over (one per
    phase) — the bug `gemini_retry.provider_timeout` exists for."""
    seen = {}

    class RecordingClient:
        def post(self, url, json=None, headers=None, timeout=None):
            seen["timeout"] = timeout
            return httpx.Response(
                200,
                json=chat_response("x"),
                request=httpx.Request("POST", url),
            )

    client = ChatClient(
        ENDPOINT, "k", "m", "omit", http_client=RecordingClient()
    )
    client.complete("i", "u", deadline=Deadline(4.0))
    timeout = seen["timeout"]
    assert isinstance(timeout, httpx.Timeout)
    total = (
        timeout.connect + timeout.write + timeout.pool + timeout.read
    )
    assert total <= 4.0 + 1e-6


@pytest.mark.parametrize(
    "handler",
    [
        lambda r: httpx.Response(400, json={"error": "bad request"}),
        lambda r: httpx.Response(200, content=b"not json"),
    ],
)
def test_a_failure_never_carries_the_key_the_url_or_the_content(handler, caplog):
    secret_prayer = "секретная тема молитвы"
    client = ChatClient(
        ENDPOINT, SECRET_KEY, "m", "omit", http_client=mock_client(handler),
        attempts=2, sleep=lambda _s: None,
    )
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(LLMError) as exc_info:
            client.complete("instruction", secret_prayer)
    chain = []
    error: BaseException | None = exc_info.value
    while error is not None:
        chain.append(str(error))
        error = error.__cause__
    # The whole cause chain, not only the message the caller reads: an httpx
    # exception quotes the request URL, which is why this transport does not
    # chain one (`from None`).
    for message in chain:
        assert "llm.example" not in message
    # The key and the prayer appear nowhere at all — not in our errors, and
    # not in anything logged while the call failed. (httpx logs the request
    # URL itself at INFO; that URL is why `config.validate_endpoint` refuses
    # an endpoint carrying credentials or a query string.)
    for message in chain + [caplog.text]:
        assert SECRET_KEY not in message
        assert secret_prayer not in message


def test_the_async_client_answers_and_maps_its_failures():
    async_ok = mock_async(lambda r: httpx.Response(200, json=chat_response("Ответ")))
    with async_ok:
        assert asyncio.run(
            AsyncChatClient(ENDPOINT, "k", "m", "omit").complete("i", "u")
        ) == "Ответ"
    with mock_async(lambda r: httpx.Response(500)):
        with pytest.raises(LLMError, match="after retries"):
            asyncio.run(
                AsyncChatClient(
                    ENDPOINT, "k", "m", "omit", sleep=no_sleep()
                ).complete(
                    "i", "u"
                )
            )


def test_the_async_client_sends_reasoning_effort_exactly():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=chat_response("Ответ"))

    with mock_async(handler):
        answer = asyncio.run(
            AsyncChatClient(ENDPOINT, "k", "m", "high").complete("i", "u")
        )
    assert answer == "Ответ"
    assert captured["reasoning_effort"] == "high"


def test_the_async_openrouter_client_sends_the_strict_profile():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=chat_response("Ответ"))

    with mock_async(handler):
        answer = asyncio.run(
            AsyncChatClient(
                config.OPENROUTER_QUESTION_ENDPOINT,
                "openrouter-key",
                config.OPENROUTER_QUESTION_MODEL,
                "none",
                max_tokens=4096,
                request_profile="openrouter",
            ).complete("i", "u", json_object=True, temperature=0.7)
        )
    assert answer == "Ответ"
    assert captured["provider"] == {
        "allow_fallbacks": False,
        "data_collection": "deny",
    }
    assert captured["reasoning"] == {"enabled": False}
    assert "reasoning_effort" not in captured


def test_question_stage_selects_openrouter_by_provider_not_endpoint(monkeypatch):
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=chat_response("Ответ"))

    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        stage(
            "question",
            config.OPENROUTER_QUESTION_MODEL,
            provider=config.PROVIDER_OPENROUTER,
            endpoint=config.OPENROUTER_QUESTION_ENDPOINT,
            api_key="openrouter-key",
            reasoning_effort="none",
        ),
    )
    with mock_async(handler):
        answer = asyncio.run(
            twinkler_ai.complete("Запрос", question_language("Запрос", "ru"))
        )
    assert answer == "Ответ"
    assert captured["provider"] == {
        "allow_fallbacks": False,
        "data_collection": "deny",
    }
    assert captured["reasoning"] == {"enabled": False}
    assert "reasoning_effort" not in captured


def no_sleep(recorded: list | None = None):
    """An `asyncio.sleep` stand-in: the ladder is asserted, never waited out."""
    async def _sleep(seconds):
        if recorded is not None:
            recorded.append(seconds)
    return _sleep


def test_the_async_ladder_is_the_same_one_the_gemini_stages_climb():
    """Three attempts, 2 s then 4 s of backoff — asserted, not slept."""
    calls = {"n": 0}
    pauses: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    with mock_async(handler):
        with pytest.raises(LLMError, match="after retries"):
            asyncio.run(
                AsyncChatClient(
                    ENDPOINT,
                    "k",
                    "m",
                    "omit",
                    attempts=3,
                    sleep=no_sleep(pauses),
                ).complete("i", "u")
            )
    assert calls["n"] == 3
    assert pauses == [2.0, 4.0]


class mock_async:
    """Context manager swapping `httpx.AsyncClient` for a mocked transport."""

    def __init__(self, handler):
        self._handler = handler
        self._real = httpx.AsyncClient

    def __enter__(self):
        real = self._real
        transport = httpx.MockTransport(self._handler)

        def factory(*args, **kwargs):
            return real(*args, transport=transport, **kwargs)

        httpx.AsyncClient = factory
        return self

    def __exit__(self, *exc_info):
        httpx.AsyncClient = self._real


# ---------------------------------------------------------------------------
# The tripwire: with every stage on openai_compat, nothing dials Gemini
# ---------------------------------------------------------------------------

class HostRecorder:
    """Records every host dialled and refuses the ones this run may not use.

    The structural half of the same guarantee lives below
    (`test_the_factories_build_no_gemini_client`): this one catches a request
    that is actually attempted, that one catches the object that would
    attempt it.
    """

    def __init__(self, allowed: str, response: dict):
        self.allowed = allowed
        self.response = response
        self.hosts: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.hosts.append(request.url.host)
        if request.url.host != self.allowed:
            raise AssertionError(
                f"outbound connection to {request.url.host!r} — this run may "
                f"only reach {self.allowed!r}"
            )
        return httpx.Response(200, json=self.response)


class FakeEncoder:
    """Stand-in for bge-m3: the tripwire is about hosts, not vectors.

    Its width follows the configured one, because the client refuses a model
    that disagrees with `EMBEDDING_DIMENSIONS` (ADR 0010) and this test is
    not about that check.
    """

    max_seq_length = 8192

    def get_sentence_embedding_dimension(self) -> int:
        return config.EMBEDDING_DIMENSIONS

    def encode(self, texts, **kwargs):
        import numpy as np

        row = np.zeros(config.EMBEDDING_DIMENSIONS, dtype="float32")
        row[0] = 1.0
        return np.tile(row, (len(texts), 1))


def test_no_gemini_host_is_dialled_by_a_fully_local_selection(monkeypatch):
    """Every provider call of one selection — rewrite, embeddings, rerank —
    plus the question endpoint, with the chat stages on `openai_compat` and
    `EMBEDDING_PROVIDER=local`. Nothing may reach Google."""
    rewrite = HostRecorder(
        "llm.example", chat_response(json.dumps({"queries": ["в1", "в2"]}))
    )
    rerank = HostRecorder(
        "llm.example", chat_response(json.dumps({"candidate": 1, "reason": "ok"}))
    )
    question = HostRecorder("llm.example", chat_response("Ответ"))

    rewriter = build_query_rewriter(
        stage("scripture_rewrite"), http_client=mock_client(rewrite)
    )
    reranker = build_passage_reranker(
        stage("scripture_rerank"), http_client=mock_client(rerank)
    )
    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        stage("question", reasoning_effort="none"),
    )
    monkeypatch.setattr(
        embeddings, "load_embedding_model", lambda: FakeEncoder()
    )
    embedder = build_embedding_client(
        provider=config.EMBEDDING_PROVIDER_LOCAL
    )

    assert rewriter.rewrite("ru", "Тема", ["ответ"]) == ["в1", "в2"]
    assert len(embedder.embed_query("в1")) == config.EMBEDDING_DIMENSIONS
    assert reranker.choose("Тема", [], ["текст"]).index == 0
    with mock_async(question):
        assert asyncio.run(
            twinkler_ai.complete("Запрос", question_language("Запрос", "ru"))
        ) == "Ответ"

    dialled = rewrite.hosts + rerank.hosts + question.hosts
    assert dialled == ["llm.example"] * 3
    assert GEMINI_HOST not in dialled


def test_the_factories_build_no_gemini_client(monkeypatch):
    monkeypatch.setattr(
        embeddings, "load_embedding_model", lambda: FakeEncoder()
    )
    forbidden = (
        query_rewrite.GeminiQueryRewriter,
        passage_rerank.GeminiPassageReranker,
        embeddings.GeminiEmbeddingClient,
    )
    built = (
        build_query_rewriter(stage("scripture_rewrite")),
        build_passage_reranker(stage("scripture_rerank")),
        build_embedding_client(provider=config.EMBEDDING_PROVIDER_LOCAL),
    )
    for client in built:
        assert not isinstance(client, forbidden)
        client.close()


def test_the_chat_factories_preserve_each_stage_reasoning_effort():
    rewriter = build_query_rewriter(
        stage("scripture_rewrite", reasoning_effort="low")
    )
    reranker = build_passage_reranker(
        stage("scripture_rerank", reasoning_effort="medium")
    )
    assert rewriter._chat.reasoning_effort == "low"
    assert reranker._chat.reasoning_effort == "medium"
    rewriter.close()
    reranker.close()


def test_the_factories_build_gemini_clients_for_a_gemini_deployment():
    gemini = {"provider": config.PROVIDER_GEMINI, "endpoint": "", "api_key": "g"}
    rewriter = build_query_rewriter(
        stage("scripture_rewrite", "gemini-3.7-flash", **gemini)
    )
    reranker = build_passage_reranker(
        stage("scripture_rerank", "gemini-3.5-flash-lite", **gemini)
    )
    assert isinstance(rewriter, query_rewrite.GeminiQueryRewriter)
    assert isinstance(reranker, passage_rerank.GeminiPassageReranker)
    # ...billing the stage's own key and running the stage's own model.
    assert (rewriter.api_key, rewriter.model) == ("g", "gemini-3.7-flash")
    assert (reranker.api_key, reranker.model) == ("g", "gemini-3.5-flash-lite")
    rewriter.close()
    reranker.close()


# ---------------------------------------------------------------------------
# Parity: one answer, two envelopes, the same result
# ---------------------------------------------------------------------------

REWRITE_ANSWER = json.dumps(
    {"queries": ["уповай на Господа", "не бойся, Я с тобою", "  уповай на Господа  "]}
)
RERANK_ANSWER = json.dumps(
    {"candidate": 2, "key_verse_start": 1, "key_verse_end": 2, "reason": "fits grief"}
)
QUESTION_ANSWER = "Господь близок к сокрушённым сердцем."


def both_transports(answer: str):
    """(gemini handler, openai_compat handler) returning the same answer."""
    captured = {"gemini": {}, "openai_compat": {}}

    def gemini(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        captured["gemini"] = {
            "system": body["system_instruction"]["parts"][0]["text"],
            "user": body["contents"][0]["parts"][0]["text"],
        }
        return httpx.Response(200, json=gemini_response(answer))

    def openai_compat(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        captured["openai_compat"] = {
            "system": body["messages"][0]["content"],
            "user": body["messages"][1]["content"],
        }
        return httpx.Response(200, json=chat_response(answer))

    return gemini, openai_compat, captured


def test_rewrite_parity_between_providers():
    gemini, openai_compat, captured = both_transports(REWRITE_ANSWER)
    on_gemini = query_rewrite.GeminiQueryRewriter(
        api_key="g", model="gemini-test", http_client=mock_client(gemini)
    ).rewrite("ru", "Умерла мама", ["Больно."])
    on_local = OpenAICompatQueryRewriter(
        stage("scripture_rewrite"), http_client=mock_client(openai_compat)
    ).rewrite("ru", "Умерла мама", ["Больно."])

    assert on_local == on_gemini == ["уповай на Господа", "не бойся, Я с тобою"]
    # The prompt is the transport's business only to carry it.
    assert captured["gemini"] == captured["openai_compat"]
    assert captured["openai_compat"]["system"] == build_rewrite_instruction(
        "ru", query_rewrite.REWRITE_VARIANTS
    )
    assert captured["openai_compat"]["user"] == build_rewrite_user_content(
        "Умерла мама", ["Больно."]
    )


def test_rerank_parity_between_providers():
    gemini, openai_compat, captured = both_transports(RERANK_ANSWER)
    candidates = ["[1] текст один [2] хвост", "[1] текст два [2] хвост"]
    on_gemini = passage_rerank.GeminiPassageReranker(
        api_key="g", model="gemini-test", http_client=mock_client(gemini)
    ).choose("Тема", ["ответ"], candidates)
    on_local = OpenAICompatPassageReranker(
        stage("scripture_rerank"), http_client=mock_client(openai_compat)
    ).choose("Тема", ["ответ"], candidates)

    assert on_local == on_gemini == RerankChoice(
        index=1, reason="fits grief", key_verse_start=1, key_verse_end=2
    )
    assert captured["gemini"] == captured["openai_compat"]
    assert captured["openai_compat"]["system"] == build_rerank_instruction(2, True)
    assert captured["openai_compat"]["user"] == build_rerank_user_content(
        "Тема", ["ответ"], candidates
    )


def test_question_parity_between_providers(monkeypatch):
    gemini, openai_compat, captured = both_transports(QUESTION_ANSWER)

    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MODEL", "gemini-test")
    monkeypatch.setattr(
        twinkler_ai, "QUESTION_PROVIDER",
        stage("question", "gemini-test", provider=config.PROVIDER_GEMINI, endpoint=""),
    )
    with mock_async(gemini):
        on_gemini = asyncio.run(
            twinkler_ai.complete("Мне тяжело", question_language("Мне тяжело", "ru"))
        )

    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        stage("question", reasoning_effort="none"),
    )
    with mock_async(openai_compat):
        on_local = asyncio.run(
            twinkler_ai.complete("Мне тяжело", question_language("Мне тяжело", "ru"))
        )

    assert on_local == on_gemini == QUESTION_ANSWER
    assert captured["gemini"] == captured["openai_compat"]
    assert captured["openai_compat"]["system"] == build_question_prompt(
        "ru"
    )
    assert captured["openai_compat"]["user"] == "Мне тяжело"


# The three stages of the structured request (ClickUp 86cbegmzz). The stage
# instructions are assembled by the server now, so "the same bytes on both
# providers" has to be re-established for the assembled message, not only for
# the system prompt: an assembly that ran on one transport and not the other
# would be invisible to every other test in this file.
QUESTION_LANGUAGE_DATA = {
    "ru": ("Отношения с семьёй", "Мне одиноко и я хочу восстановить общение."),
    "uk": ("Стосунки з родиною", "Мені самотньо і я хочу відновити спілкування."),
    "en": ("Relationships with family", "I feel alone and want to reconnect."),
}
QUESTION_REQUESTS = [
    (
        language,
        topic,
        question_stage,
        [] if question_stage == "first" else [("user", reply)],
        topic if question_stage == "first" else reply,
    )
    for language, (topic, reply) in QUESTION_LANGUAGE_DATA.items()
    for question_stage in ("first", "next", "reflect")
]


@pytest.mark.parametrize(
    ("language", "topic", "question_stage", "messages", "language_source"),
    QUESTION_REQUESTS,
)
def test_question_parity_on_every_stage(
    monkeypatch, language, topic, question_stage, messages, language_source
):
    gemini, openai_compat, captured = both_transports(QUESTION_ANSWER)
    user_message = build_user_message(
        topic, question_stage, messages, language=language
    )
    resolved = twinkler_ai.ResolvedQuestionLanguage(language_source, language)

    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MODEL", "gemini-test")
    monkeypatch.setattr(
        twinkler_ai, "QUESTION_PROVIDER",
        stage("question", "gemini-test", provider=config.PROVIDER_GEMINI, endpoint=""),
    )
    with mock_async(gemini):
        on_gemini = asyncio.run(twinkler_ai.complete(user_message, resolved))

    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        stage("question", reasoning_effort="none"),
    )
    with mock_async(openai_compat):
        on_local = asyncio.run(twinkler_ai.complete(user_message, resolved))

    assert on_local == on_gemini == QUESTION_ANSWER
    assert captured["gemini"] == captured["openai_compat"]
    assert captured["openai_compat"]["user"] == user_message
    assert captured["openai_compat"]["system"] == build_question_prompt(language)


def test_the_stage_instructions_do_not_choose_the_language(monkeypatch):
    """An English prayer must not be answered in Russian by our own wrapper.

    The stage blocks are localized independently from the prayer data. Both
    transports must receive the same English block and system prompt.
    """
    gemini, openai_compat, captured = both_transports(QUESTION_ANSWER)
    reply = "My mother stopped calling after the wedding."
    user_message = build_user_message(
        "Отношения с семьёй",
        "next",
        [("assistant", "Что сейчас тревожит тебя больше всего?"), ("user", reply)],
        language="en",
    )
    assert "Conversation so far:" in user_message

    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MODEL", "gemini-test")
    monkeypatch.setattr(
        twinkler_ai, "QUESTION_PROVIDER",
        stage("question", "gemini-test", provider=config.PROVIDER_GEMINI, endpoint=""),
    )
    with mock_async(gemini):
        asyncio.run(twinkler_ai.complete(user_message, reply))
    monkeypatch.setattr(twinkler_ai, "QUESTION_PROVIDER", stage("question"))
    with mock_async(openai_compat):
        asyncio.run(twinkler_ai.complete(user_message, reply))

    assert captured["gemini"] == captured["openai_compat"]
    assert captured["gemini"]["system"] == build_question_prompt("en")


@pytest.mark.parametrize(
    ("message", "language"),
    [
        ("Мне очень тяжело сейчас и я не знаю, что делать дальше", "ru"),
        ("Син не дзвонить уже місяць", "uk"),
        ("I got the job! Three years of trying", "en"),
    ],
)
def test_the_language_named_in_the_prompt_is_the_same_on_both_providers(
    monkeypatch, message, language
):
    """Prompt v2 names the language — and both transports must name the same
    one, or ADR 0009's "same bytes" claim would hold for v1 only."""
    gemini, openai_compat, captured = both_transports(QUESTION_ANSWER)

    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MODEL", "gemini-test")
    monkeypatch.setattr(
        twinkler_ai, "QUESTION_PROVIDER",
        stage("question", "gemini-test", provider=config.PROVIDER_GEMINI, endpoint=""),
    )
    with mock_async(gemini):
        asyncio.run(twinkler_ai.complete(message))

    monkeypatch.setattr(twinkler_ai, "QUESTION_PROVIDER", stage("question"))
    with mock_async(openai_compat):
        asyncio.run(twinkler_ai.complete(message))

    assert captured["gemini"] == captured["openai_compat"]
    assert captured["gemini"]["system"] == build_question_prompt(language)


def test_the_question_stage_asks_for_the_v6_object(monkeypatch):
    """Since prompt v6 this stage IS a parsed contract (ClickUp 86cbejvt2).

    It asked for prose until 2026-09-06, and the comment said so; v6 makes the
    answer `{"subject": …, "question": …}`, and the server-side grammar of the
    endpoint is a stronger guarantee than the prompt's format section. The
    person is still shown one question — `app/question_format.py` unwraps it.
    """
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=chat_response("Ответ"))

    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        stage("question", reasoning_effort="none"),
    )
    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MAX_TOKENS", 8192)
    with mock_async(handler):
        asyncio.run(twinkler_ai.complete("Запрос", question_language("Запрос", "ru")))

    assert captured["response_format"] == {"type": "json_object"}
    assert captured["temperature"] == 0.7
    assert captured["max_tokens"] == 8192
    assert captured["reasoning_effort"] == "none"


def test_openai_question_diagnostics_log_bodies_without_credentials(
    monkeypatch, caplog
):
    request_text = "private prayer text"
    raw_response = json.dumps(
        {
            **chat_response(
                '{"subject":"hope","question":"What matters?"}'
            ),
            "echoed_key": SECRET_KEY,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=raw_response.encode(),
            headers={"Content-Type": "application/json"},
        )

    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        stage("question", reasoning_effort="none"),
    )
    monkeypatch.setattr(
        twinkler_ai, "AI_QUESTION_LOG_PROVIDER_BODIES", True
    )
    with caplog.at_level(logging.INFO, logger=twinkler_ai.logger.name):
        with mock_async(handler):
            asyncio.run(twinkler_ai.complete(request_text, question_language(request_text, "en")))
            asyncio.run(twinkler_ai.complete(request_text, question_language(request_text, "en")))

    messages = [record.getMessage() for record in caplog.records]
    request_lines = [
        message for message in messages if "provider request" in message
    ]
    response_lines = [
        message for message in messages if "provider response" in message
    ]
    assert len(request_lines) == len(response_lines) == 2
    call_ids = []
    for request_line, response_line in zip(request_lines, response_lines):
        request_call_id = request_line.split("call_id=", 1)[1].split()[0]
        response_call_id = response_line.split("call_id=", 1)[1].split()[0]
        assert request_call_id == response_call_id
        assert len(request_call_id) == 32
        call_ids.append(request_call_id)
    assert len(set(call_ids)) == 2

    request_line = request_lines[0]
    response_line = response_lines[0]
    assert "provider=openai_compat" in request_line
    assert request_text in request_line
    assert (
        json.dumps(
            raw_response.replace(SECRET_KEY, "[REDACTED_API_KEY]"),
            ensure_ascii=False,
        )
        in response_line
    )
    assert "[REDACTED_API_KEY]" in response_line
    for message in (request_line, response_line):
        assert SECRET_KEY not in message
        assert "Authorization" not in message


def test_question_provider_bodies_are_not_logged_by_default(monkeypatch, caplog):
    request_text = "private prayer text"
    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        stage("question", reasoning_effort="none"),
    )
    monkeypatch.setattr(
        twinkler_ai, "AI_QUESTION_LOG_PROVIDER_BODIES", False
    )
    with caplog.at_level(logging.INFO, logger=twinkler_ai.logger.name):
        with mock_async(
            lambda _request: httpx.Response(
                200,
                json=chat_response("Provider answer"),
            )
        ):
            asyncio.run(twinkler_ai.complete(request_text, question_language(request_text, "en")))

    assert request_text not in caplog.text
    assert "Provider answer" not in caplog.text


def test_empty_openai_content_is_logged_before_the_expected_error(
    monkeypatch, caplog
):
    raw_response = '{"choices":[{"message":{"content":""}}],"finish_reason":"length"}'
    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        stage("question", reasoning_effort="none"),
    )
    monkeypatch.setattr(
        twinkler_ai, "AI_QUESTION_LOG_PROVIDER_BODIES", True
    )

    with caplog.at_level(logging.INFO, logger=twinkler_ai.logger.name):
        with mock_async(
            lambda _request: httpx.Response(
                200,
                content=raw_response.encode(),
                headers={"Content-Type": "application/json"},
            )
        ):
            with pytest.raises(
                twinkler_ai.AIError, match="response content is empty"
            ):
                asyncio.run(
                    twinkler_ai.complete(
                        "private prayer text",
                        question_language("private prayer text", "en"),
                    )
                )

    request_line = next(
        record.getMessage()
        for record in caplog.records
        if "provider request" in record.getMessage()
    )
    response_line = next(
        record.getMessage()
        for record in caplog.records
        if "provider response" in record.getMessage()
    )
    assert json.dumps(raw_response) in response_line
    assert (
        request_line.split("call_id=", 1)[1].split()[0]
        == response_line.split("call_id=", 1)[1].split()[0]
    )


def test_gemini_question_diagnostics_log_bodies_without_credentials(
    monkeypatch, caplog
):
    request_text = "private prayer text"
    real_async_client = httpx.AsyncClient

    def factory(*args, **kwargs):
        return real_async_client(
            *args,
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    200,
                    json={
                        **gemini_response("Provider answer"),
                        "echoed_key": SECRET_KEY,
                    },
                )
            ),
            **kwargs,
        )

    monkeypatch.setattr(twinkler_ai.httpx, "AsyncClient", factory)
    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MODEL", "gemini-test")
    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        stage(
            "question",
            "gemini-test",
            provider=config.PROVIDER_GEMINI,
            endpoint="",
        ),
    )
    monkeypatch.setattr(
        twinkler_ai, "AI_QUESTION_LOG_PROVIDER_BODIES", True
    )
    with caplog.at_level(logging.INFO, logger=twinkler_ai.logger.name):
        asyncio.run(twinkler_ai.complete(request_text, question_language(request_text, "en")))

    messages = [record.getMessage() for record in caplog.records]
    request_line = next(
        message for message in messages if "provider request" in message
    )
    response_line = next(
        message for message in messages if "provider response" in message
    )
    assert "provider=gemini" in request_line
    assert request_text in request_line
    assert "Provider answer" in response_line
    assert "[REDACTED_API_KEY]" in response_line
    assert (
        request_line.split("call_id=", 1)[1].split()[0]
        == response_line.split("call_id=", 1)[1].split()[0]
    )
    for message in (request_line, response_line):
        assert SECRET_KEY not in message
        assert "Authorization" not in message


def test_a_failing_local_question_is_the_same_502_as_a_failing_gemini_one(monkeypatch):
    monkeypatch.setattr(twinkler_ai, "QUESTION_PROVIDER", stage("question"))
    with mock_async(lambda r: httpx.Response(500)):
        with pytest.raises(twinkler_ai.GeminiError):
            asyncio.run(twinkler_ai.complete("Запрос", question_language("Запрос", "ru")))


def test_the_stage_errors_stay_the_stage_errors():
    """A caller of rewrite()/choose() sees the same exception type whoever
    served it — that is what lets `retrieval` degrade identically."""
    failing = mock_client(lambda r: httpx.Response(400, json={}))
    with pytest.raises(query_rewrite.QueryRewriteError):
        OpenAICompatQueryRewriter(
            stage("scripture_rewrite"), http_client=failing
        ).rewrite("ru", "тема", [])
    with pytest.raises(passage_rerank.PassageRerankError):
        OpenAICompatPassageReranker(
            stage("scripture_rerank"), http_client=failing
        ).choose("тема", [], ["текст"])


def test_the_startup_banner_names_the_providers_and_never_the_key(monkeypatch, caplog):
    import main

    monkeypatch.setattr(main, "QUESTION_PROVIDER", stage("question"))
    monkeypatch.setattr(
        main, "SCRIPTURE_REWRITE_PROVIDER", stage("scripture_rewrite")
    )
    monkeypatch.setattr(
        main, "SCRIPTURE_RERANK_PROVIDER", stage("scripture_rerank")
    )
    monkeypatch.setattr(main, "AI_QUESTION_MAX_TOKENS", 8192)
    monkeypatch.setattr(main, "AI_QUESTION_TIMEOUT_SECONDS", 23.5)
    with caplog.at_level(logging.INFO):
        main.log_ai_providers()

    # Four since ADR 0012: the three chat stages plus transcription, which is
    # left as the suite configured it (gemini) — a banner that named only the
    # chat stages would hide exactly the provider that hears a person's voice.
    assert caplog.text.count("AI stage") == 4
    assert "AI stage transcribe" in caplog.text
    assert "openai_compat" in caplog.text
    assert "reasoning_effort=omit" in caplog.text
    assert "llm.example" in caplog.text          # the host, for the operator
    assert SECRET_KEY not in caplog.text         # never the key itself
    assert "8443" not in caplog.text             # host only, not the URL
    question_line = next(
        line for line in caplog.text.splitlines() if "AI stage question" in line
    )
    assert "max_tokens=8192 timeout_seconds=23.5" in question_line
    assert caplog.text.count("max_tokens=") == 1
    assert caplog.text.count("timeout_seconds=") == 1


def test_the_startup_banner_names_the_strict_openrouter_profile(monkeypatch, caplog):
    import main

    monkeypatch.setattr(
        main,
        "QUESTION_PROVIDER",
        stage(
            "question",
            config.OPENROUTER_QUESTION_MODEL,
            provider=config.PROVIDER_OPENROUTER,
            endpoint=config.OPENROUTER_QUESTION_ENDPOINT,
            api_key=SECRET_KEY,
            reasoning_effort="none",
        ),
    )
    with caplog.at_level(logging.INFO):
        main.log_ai_providers()

    line = next(
        line for line in caplog.text.splitlines() if "AI stage question" in line
    )
    assert "provider=openrouter" in line
    assert "model=google/gemma-4-31b-it" in line
    assert " at openrouter.ai" in line
    assert "reasoning=disabled" in line
    assert "allow_fallbacks=false" in line
    assert "data_collection=deny" in line
    assert "reasoning_effort=" not in line
    assert SECRET_KEY not in line


def test_the_ai_banner_is_visible_when_uvicorn_left_the_root_bare():
    """A banner nothing handles is not a banner.

    Uvicorn configures its own loggers and leaves the ROOT logger without
    handlers, so an INFO record from `main` reaches only logging's
    last-resort handler (WARNING+) and never `docker logs`.
    `trusted_proxies.ensure_visible_handler` installs one on the emitting
    logger — it does so for its own banner, and `log_ai_providers` must ask
    for the same or the AI banner is silently dropped in production.
    """
    import main

    root = logging.getLogger()
    saved = (root.handlers, main.logger.handlers, main.logger.level)
    try:
        root.handlers = []
        main.logger.handlers = []
        main.logger.setLevel(logging.NOTSET)
        main.log_ai_providers()
        assert main.logger.handlers, "the AI banner has no handler: invisible"
        assert main.logger.isEnabledFor(logging.INFO)
    finally:
        root.handlers, main.logger.handlers, level = saved
        main.logger.setLevel(level)


def test_the_question_timeout_variable_reaches_the_gemini_client(monkeypatch):
    """`AI_QUESTION_TIMEOUT_SECONDS` is documented as the ceiling of the
    question endpoint's single call, so it must reach BOTH providers. Its
    default (20.0) is the literal this call always used, so no existing
    deployment changes."""
    seen = {}
    real = httpx.AsyncClient

    def factory(*args, **kwargs):
        seen["timeout"] = kwargs.get("timeout")
        return real(
            *args,
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json=gemini_response("Ответ"))
            ),
            **kwargs,
        )

    monkeypatch.setattr(httpx, "AsyncClient", factory)
    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MODEL", "gemini-test")
    monkeypatch.setattr(
        twinkler_ai, "QUESTION_PROVIDER",
        stage("question", "gemini-test", provider=config.PROVIDER_GEMINI, endpoint=""),
    )
    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_TIMEOUT_SECONDS", 3.5)

    assert asyncio.run(
        twinkler_ai.complete("Мне тяжело", question_language("Мне тяжело", "ru"))
    ) == "Ответ"
    # Carved across httpx's four phases since ClickUp 86cbehyg0, where this
    # call handed over the bare number: httpx applies a bare `timeout` to
    # EACH phase, so 3.5 would authorise 14 s for one call — and twice that
    # for a request that generates twice. What the variable bounds is the
    # whole call, so the phases sum to it.
    timeout = seen["timeout"]
    assert isinstance(timeout, httpx.Timeout)
    assert (
        timeout.connect + timeout.write + timeout.pool + timeout.read
        == pytest.approx(3.5)
    )
    assert timeout.read == pytest.approx(3.5 - 3 * (3.5 / 12.0))


def test_an_out_of_range_local_answer_is_refused_like_a_gemini_one():
    """There is no `responseSchema` in this protocol, so the server-side
    validation is the whole contract — and it is the same function."""
    answer = mock_client(
        lambda r: httpx.Response(
            200, json=chat_response(json.dumps({"candidate": 99, "reason": "hm"}))
        )
    )
    with pytest.raises(passage_rerank.PassageRerankError, match="outside"):
        OpenAICompatPassageReranker(
            stage("scripture_rerank"), http_client=answer
        ).choose("тема", [], ["текст 1", "текст 2"])
