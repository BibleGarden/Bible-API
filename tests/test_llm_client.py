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
from question_prompt import build_question_prompt
from safety import detect_language

ENDPOINT = "https://llm.example:8443/v1"
GEMINI_HOST = "generativelanguage.googleapis.com"
SECRET_KEY = "sk-do-not-print-me"


def stage(name: str, model: str = "qwen3-30b", **kwargs) -> config.StageProvider:
    return config.StageProvider(
        stage=name,
        provider=kwargs.pop("provider", config.PROVIDER_OPENAI_COMPAT),
        model=model,
        endpoint=kwargs.pop("endpoint", ENDPOINT),
        api_key=kwargs.pop("api_key", SECRET_KEY),
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
        "m", "system", "user", temperature=0.0, max_tokens=1024, json_object=True
    )
    assert payload["messages"] == [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "user"},
    ]
    assert payload["response_format"] == {"type": "json_object"}
    assert payload["temperature"] == 0.0 and payload["max_tokens"] == 1024
    prose = build_payload(
        "m", "s", "u", temperature=0.7, max_tokens=8, json_object=False
    )
    assert "response_format" not in prose


def test_the_client_sends_model_prompt_and_key():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("Authorization")
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json=chat_response('{"ok": true}'))

    client = ChatClient(ENDPOINT, SECRET_KEY, "qwen3-30b", http_client=mock_client(handler))
    assert client.complete("instruction", "content") == '{"ok": true}'
    assert captured["url"] == f"{ENDPOINT}/chat/completions"
    assert captured["auth"] == f"Bearer {SECRET_KEY}"
    assert captured["body"]["model"] == "qwen3-30b"


@pytest.mark.parametrize(
    ("field", "value"),
    [("endpoint", ""), ("model", "")],
)
def test_an_unconfigured_client_refuses_before_it_dials(field, value):
    kwargs = {"endpoint": ENDPOINT, "api_key": "k", "model": "m", field: value}
    client = ChatClient(
        kwargs["endpoint"], kwargs["api_key"], kwargs["model"],
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
        ENDPOINT, "k", "m",
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
        ENDPOINT, "k", "m", http_client=mock_client(handler),
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
        ENDPOINT, "k", "m", http_client=mock_client(handler),
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

    client = ChatClient(ENDPOINT, "k", "m", http_client=mock_client(handler))
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

    client = ChatClient(ENDPOINT, "k", "m", http_client=RecordingClient())
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
        ENDPOINT, SECRET_KEY, "m", http_client=mock_client(handler),
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
            AsyncChatClient(ENDPOINT, "k", "m").complete("i", "u")
        ) == "Ответ"
    with mock_async(lambda r: httpx.Response(500)):
        with pytest.raises(LLMError, match="after retries"):
            asyncio.run(
                AsyncChatClient(ENDPOINT, "k", "m", sleep=no_sleep()).complete(
                    "i", "u"
                )
            )


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
                    ENDPOINT, "k", "m", attempts=3, sleep=no_sleep(pauses)
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
    monkeypatch.setattr(twinkler_ai, "QUESTION_PROVIDER", stage("question"))
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
        assert asyncio.run(twinkler_ai.complete("Запрос")) == "Ответ"

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

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "g")
    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MODEL", "gemini-test")
    monkeypatch.setattr(
        twinkler_ai, "QUESTION_PROVIDER",
        stage("question", "gemini-test", provider=config.PROVIDER_GEMINI, endpoint=""),
    )
    with mock_async(gemini):
        on_gemini = asyncio.run(twinkler_ai.complete("Мне тяжело"))

    monkeypatch.setattr(twinkler_ai, "QUESTION_PROVIDER", stage("question"))
    with mock_async(openai_compat):
        on_local = asyncio.run(twinkler_ai.complete("Мне тяжело"))

    assert on_local == on_gemini == QUESTION_ANSWER
    assert captured["gemini"] == captured["openai_compat"]
    assert captured["openai_compat"]["system"] == build_question_prompt(
        detect_language("Мне тяжело")
    )
    assert captured["openai_compat"]["user"] == "Мне тяжело"


@pytest.mark.parametrize(
    ("message", "ending"),
    [
        ("Мне очень тяжело сейчас, я не сплю", "Answer in Russian."),
        ("Син не дзвонить уже місяць", "Answer in Ukrainian."),
        ("I got the job! Three years of trying", "Answer in English."),
        # No evidence of ru vs uk: the prompt keeps v1's "detect it yourself"
        # rather than inventing a language (86cbegg3f).
        ("Помоги", "Answer in exactly the language of the person's message."),
    ],
)
def test_the_language_named_in_the_prompt_is_the_same_on_both_providers(
    monkeypatch, message, ending
):
    """Prompt v2 names the language — and both transports must name the same
    one, or ADR 0009's "same bytes" claim would hold for v1 only."""
    gemini, openai_compat, captured = both_transports(QUESTION_ANSWER)

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "g")
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
    assert captured["gemini"]["system"].endswith(ending)


def test_the_question_stage_asks_for_prose_not_json(monkeypatch):
    """`/api/ai/question` answers a person; a JSON contract there would make
    the model wrap the reply in an object."""
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=chat_response("Ответ"))

    monkeypatch.setattr(twinkler_ai, "QUESTION_PROVIDER", stage("question"))
    with mock_async(handler):
        asyncio.run(twinkler_ai.complete("Запрос"))

    assert "response_format" not in captured
    assert captured["temperature"] == 0.7
    assert captured["max_tokens"] == llm_client.DEFAULT_MAX_TOKENS


def test_a_failing_local_question_is_the_same_502_as_a_failing_gemini_one(monkeypatch):
    monkeypatch.setattr(twinkler_ai, "QUESTION_PROVIDER", stage("question"))
    with mock_async(lambda r: httpx.Response(500)):
        with pytest.raises(twinkler_ai.GeminiError):
            asyncio.run(twinkler_ai.complete("Запрос"))


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
    with caplog.at_level(logging.INFO):
        main.log_ai_providers()

    assert caplog.text.count("AI stage") == 3
    assert "openai_compat" in caplog.text
    assert "llm.example" in caplog.text          # the host, for the operator
    assert SECRET_KEY not in caplog.text         # never the key itself
    assert "8443" not in caplog.text             # host only, not the URL


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
    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "g")
    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MODEL", "gemini-test")
    monkeypatch.setattr(
        twinkler_ai, "QUESTION_PROVIDER",
        stage("question", "gemini-test", provider=config.PROVIDER_GEMINI, endpoint=""),
    )
    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_TIMEOUT_SECONDS", 3.5)

    assert asyncio.run(twinkler_ai.complete("Мне тяжело")) == "Ответ"
    assert seen["timeout"] == 3.5


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
