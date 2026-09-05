import asyncio
import base64
from collections import deque
import hashlib
import hmac
import inspect
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("AI_CLIENT_HMAC_KEY", "test-hmac-key")

from fastapi.testclient import TestClient

import twinkler_ai
import client_ip
import middleware
import question_prompt
import rate_limit
import safety
from main import app
from trusted_proxies import TrustedProxies

from pathlib import Path

client = TestClient(app)
real_reserve_rate_limit = twinkler_ai._reserve_rate_limit
EVALUATION = Path(__file__).resolve().parent.parent / "evaluation"


def _probe_text(probe_id: str) -> str:
    payload = json.loads(
        (EVALUATION / "question_probe_inputs.json").read_text(encoding="utf-8")
    )
    return next(
        probe["text"] for probe in payload["inputs"] if probe["id"] == probe_id
    )


@pytest.fixture(autouse=True)
def allow_ai_requests(monkeypatch):
    twinkler_ai._limiter.reset()
    reservation = Mock()
    monkeypatch.setattr(twinkler_ai, "_reserve_rate_limit", reservation)
    monkeypatch.setattr(middleware, "_insert_request_log", Mock())
    return reservation


def test_question_prompt_is_a_usable_constant():
    """The prompt is code, not configuration (ClickUp 86cbbmy8d).

    Replaces the former "TWINKLER_SYSTEM_PROMPT is not configured" /
    "is too long" runtime branches: those guarded an environment value that
    no longer exists, and the properties they protected are now asserted
    here, once, against the literal.
    """
    template = question_prompt.QUESTION_PROMPT_TEMPLATE
    assert isinstance(template, str)
    assert template.strip() == template != ""
    # The provider request budget the removed guard used to enforce — checked
    # on what is actually sent, which is the filled template.
    for language in ("ru", "uk", "en", None):
        prompt = question_prompt.build_question_prompt(language)
        assert prompt.strip() == prompt != ""
        assert len(prompt) <= 8000
        assert "{" not in prompt and "}" not in prompt
    # Pins the wording itself, not just its shape: if this fails, the
    # prompt text changed. Update the hash/len together with a bump of
    # QUESTION_PROMPT_VERSION (app/question_prompt.py says why).
    assert len(template) == 2335
    assert (
        hashlib.sha256(template.encode("utf-8")).hexdigest()
        == "3858a569ff894e7dfe89290115c0a8a1496e8b11179c5aff543de8cf9dd2010f"
    )


def test_question_prompt_is_versioned():
    version = question_prompt.QUESTION_PROMPT_VERSION
    assert isinstance(version, int) and version >= 1
    # v2 = the language/interpretation revision of 2026-09-05 (86cbegg3f);
    # bumped together with the hash above.
    assert version == 2


# --- the prompt names the language of the message (ClickUp 86cbegg3f) -----
#
# v1 asked the model to detect the language itself and Qwen3-30B answered two
# whole English inputs in Ukrainian (6 of 81 answers, measurement 86cbegctz).
# v2 states the language instead, taken from the detector the despair rule
# already runs on the same message.


@pytest.mark.parametrize(
    ("language", "name"),
    [("ru", "Russian"), ("uk", "Ukrainian"), ("en", "English")],
)
def test_the_prompt_names_the_language_twice(language, name):
    prompt = question_prompt.build_question_prompt(language)

    assert f"ask your question in {name}, and in no other language" in prompt
    # Repeated as the last sentence: the position a model loses last.
    assert prompt.endswith(f"Answer in {name}.")
    for other in ("Russian", "Ukrainian", "English"):
        if other != name:
            # The other names appear only inside the register rule
            # ("Russian ty, Ukrainian ty") and the grammar rule, never as an
            # instruction to answer in them.
            assert f"Answer in {other}." not in prompt


def test_an_undecidable_language_keeps_the_v1_instruction():
    """`None` must not become English.

    `detect_language` answers `None` for a Cyrillic message carrying none of
    the four letters that separate Russian from Ukrainian ("Помоги"). Naming
    English there would manufacture the very violation this version removes,
    so the prompt falls back to v1's behaviour — the model decides — for
    exactly those inputs.
    """
    prompt = question_prompt.build_question_prompt(None)

    assert question_prompt.UNDETERMINED_LANGUAGE in prompt
    assert "Answer in English." not in prompt
    assert safety.detect_language("Помоги") is None


def test_the_despair_sentence_left_the_prompt_for_safety_py():
    """The rule is code now (86cbegg23); the prompt must not claim it too."""
    template = question_prompt.QUESTION_PROMPT_TEMPLATE.lower()

    for word in ("despair", "self-harm", "suicide", "emergency"):
        assert word not in template


def test_the_prompt_bans_interpreting_and_rhetorical_questions():
    """Maria's two findings on the v1 measurement, in the wording itself."""
    template = question_prompt.QUESTION_PROMPT_TEMPLATE

    assert "Do not name a feeling they have not named themselves" in template
    assert "'it sounds like you ...'" in template
    assert "how much they are suffering" in template
    assert "'Is God near?'" in template
    # And the two rules that came out of the v2 measurement itself: without
    # them both providers turned interrogative ("Как зовут дочку?", "У якому
    # районі зараз твоє нове житло?") — concrete, and useless to pray with.
    assert "never ask for a fact that only fills in your own picture" in template
    assert "never one that can be answered with yes or no" in template
    # And nothing was added to make the companion nicer (Maria, 2026-09-05:
    # a prompt must not turn the model faceless and monotonously positive).
    # The one tone sentence is v1's, unchanged.
    assert template.count("Tone: warm and quiet.") == 1
    for softener in ("supportive", "encourag", "positive", "comforting"):
        assert softener not in template.lower()


def test_the_prompt_is_built_from_whatever_text_it_is_given():
    """The seam the structured request will move (ClickUp 86cbegmzz).

    `question_prompt_for` is a pure function of the text handed to it, so the
    follow-up that replaces the single `user` string with topic + stage +
    messages passes the LAST user message here and nothing else changes.
    """
    conversation = (
        "I have been praying about my father.\n"
        "Що для тебе найважче в цьому мовчанні?\n"
        "Мне страшно, что он снова скажет что-нибудь злое."
    )
    last_message = conversation.splitlines()[-1]

    assert twinkler_ai.question_prompt_for(last_message).endswith(
        "Answer in Russian."
    )
    assert twinkler_ai.question_prompt_for(
        "I got the job! Three years of trying"
    ).endswith("Answer in English.")


def test_complete_names_the_language_of_the_message(monkeypatch):
    """The system instruction follows the message, not a configured default."""
    sent = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.read()))
        return httpx.Response(
            200,
            json={"candidates": [{"content": {"parts": [{"text": "Ответ"}]}}]},
        )

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(
        twinkler_ai.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(*args, transport=transport, **kwargs),
    )

    asyncio.run(twinkler_ai.complete("I got the job! Three years of trying"))
    asyncio.run(twinkler_ai.complete("Син не дзвонить уже місяць"))

    instructions = [
        payload["system_instruction"]["parts"][0]["text"] for payload in sent
    ]
    assert instructions[0].endswith("Answer in English.")
    assert instructions[1].endswith("Answer in Ukrainian.")


def test_question_prompt_module_reads_no_environment_variable():
    """No env variable can change the prompt any more — not even the old one.

    Setting the old TWINKLER_SYSTEM_PROMPT env var after `config` and
    `question_prompt` are already imported would prove nothing (the module
    is imported once, at collection time, so a post-hoc monkeypatch is
    inert either way). The real claim is that the module never reads the
    environment at all — assert that directly against its source.
    """
    import config

    assert not hasattr(config, "TWINKLER_SYSTEM_PROMPT")
    source = inspect.getsource(question_prompt)
    assert "environ" not in source
    assert "getenv" not in source
    assert twinkler_ai.build_question_prompt is question_prompt.build_question_prompt


def test_complete_sends_the_prompt_constant(monkeypatch):
    """`complete()` sends the built prompt verbatim as the system instruction."""
    sent = {}

    def handler(request: httpx.Request) -> httpx.Response:
        sent.update(json.loads(request.read()))
        return httpx.Response(
            200,
            json={"candidates": [{"content": {"parts": [{"text": "Ответ"}]}}]},
        )

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient

    def async_client(*args, **kwargs):
        return real_async_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(twinkler_ai.httpx, "AsyncClient", async_client)

    asyncio.run(twinkler_ai.complete("Запрос"))
    assert sent["system_instruction"]["parts"] == [
        {"text": question_prompt.build_question_prompt(
            safety.detect_language("Запрос")
        )}
    ]


def test_extracts_text_parts():
    data = {
        "candidates": [{
            "content": {"parts": [{"text": "Тихий "}, {"text": "ответ"}]},
        }],
    }
    assert twinkler_ai._extract_text(data) == "Тихий ответ"


def test_requires_api_key():
    response = client.post(
        "/api/ai/question",
        json={"user": "Запрос"},
    )
    assert response.status_code == 403


def test_returns_generated_text(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
    )

    assert response.status_code == 200
    assert response.json() == {"text": "Ответ"}
    generated.assert_awaited_once_with("Запрос")


# --- the despair rule lives in code (ClickUp 86cbegg23) -------------------
#
# The measurement of 2026-09-05 (86cbegctz) had Qwen3-30B answer the explicit
# despair input with a guiding question in 3 samples out of 3 while Gemini
# dropped the format as instructed. So the endpoint no longer depends on the
# instruction: `app/safety.py` decides, and these tests pin both tiers at the
# level the client sees. The detector itself is tested in tests/test_safety.py.


def test_an_explicit_despair_message_never_reaches_the_model(monkeypatch):
    generated = AsyncMock(return_value="Ты сейчас очень одинок?")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": _probe_text("probe-despair")},
    )

    assert response.status_code == 200
    assert response.json() == {"text": safety.SAFETY_REPLIES["ru"]}
    generated.assert_not_awaited()
    assert "?" not in response.json()["text"]


def test_the_fixed_reply_still_costs_a_request_slot(monkeypatch, allow_ai_requests):
    """A tier-1 answer consumes the client's quota like any other answer.

    Deliberate: the short-circuit happens after the reservation, so the limit
    counts replies, not provider calls, and one code path cannot be used to
    probe the endpoint for free.
    """
    monkeypatch.setattr(twinkler_ai, "complete", AsyncMock(return_value="unused"))

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": _probe_text("probe-despair")},
    )

    assert response.status_code == 200
    allow_ai_requests.assert_called_once_with("testclient")


def test_a_weak_signal_answered_with_a_question_is_replaced(monkeypatch):
    """Tier 2: the model was asked, and answered the wrong way."""
    generated = AsyncMock(return_value="What would you like to tell God?")
    monkeypatch.setattr(twinkler_ai, "complete", generated)
    # en-005 of the approved scenarios, as the app sends a conversation.
    user = "Feeling worthless\nI keep thinking everyone would be fine without me."

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": user},
    )

    assert response.status_code == 200
    assert response.json() == {"text": safety.SAFETY_REPLIES["en"]}
    generated.assert_awaited_once_with(user)


def test_a_weak_signal_answered_warmly_keeps_the_model_answer(monkeypatch):
    """Tier 2 is a floor under the answer, not a replacement for a good one."""
    warm = "You are not alone in this, and it matters that you are still here."
    monkeypatch.setattr(twinkler_ai, "complete", AsyncMock(return_value=warm))

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "I am so tired of living"},
    )

    assert response.status_code == 200
    assert response.json() == {"text": warm}


def test_an_ordinary_message_is_untouched_by_either_tier(monkeypatch):
    generated = AsyncMock(return_value="Что тебе сейчас труднее всего?")
    monkeypatch.setattr(twinkler_ai, "complete", generated)
    user = _probe_text("probe-tech")

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": user},
    )

    assert response.status_code == 200
    assert response.json() == {"text": "Что тебе сейчас труднее всего?"}
    generated.assert_awaited_once_with(user)


@pytest.mark.parametrize(
    ("user", "reply", "expected", "private_words"),
    [
        (
            "Я больше не хочу жить, началась паника",
            "Ты сейчас очень одинок?",
            "tier=1 pattern=ru.no-wish-to-live language=ru",
            ("больше", "жить", "паника", "одинок", "сейчас"),
        ),
        (
            "Feeling worthless\nEveryone would be fine without me",
            "What hurts the most right now?",
            "tier=2 pattern=en.better-without-me language=en",
            ("worthless", "Everyone", "fine", "hurts"),
        ),
    ],
)
def test_the_safety_log_records_the_rule_and_not_the_message(
    monkeypatch, caplog, user, reply, expected, private_words
):
    monkeypatch.setattr(twinkler_ai, "complete", AsyncMock(return_value=reply))

    with caplog.at_level("WARNING", logger="twinkler_ai"):
        response = client.post(
            "/api/ai/question",
            headers={"X-API-Key": "test-api-key"},
            json={"user": user},
        )

    assert response.status_code == 200
    assert expected in caplog.text
    assert f"reply_version={safety.SAFETY_REPLY_VERSION}" in caplog.text
    for word in private_words:
        assert word not in caplog.text


def test_ignores_forwarded_for_from_untrusted_peer(monkeypatch, allow_ai_requests):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={
            "X-API-Key": "test-api-key",
            "X-Forwarded-For": "203.0.113.7",
        },
        json={"user": "Запрос"},
    )

    assert response.status_code == 200
    allow_ai_requests.assert_called_once_with("testclient")


def test_uses_forwarded_for_from_trusted_peer(monkeypatch, allow_ai_requests):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(twinkler_ai, "complete", generated)
    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={"testclient"})
    )

    response = client.post(
        "/api/ai/question",
        headers={
            "X-API-Key": "test-api-key",
            "X-Forwarded-For": "203.0.113.7, 192.0.2.1",
        },
        json={"user": "Запрос"},
    )

    assert response.status_code == 200
    # The RIGHTMOST element: the address the trusted proxy itself appended.
    # The left one is whatever the caller put in the header (ClickUp 86cbbq6vz).
    allow_ai_requests.assert_called_once_with("192.0.2.1")


@pytest.mark.parametrize(
    "payload",
    [
        {"user": ""},
        {"user": "x" * 16001},
        {"system": "Клиентская система", "user": "Запрос"},
    ],
)
def test_rejects_invalid_prompts(payload):
    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=payload,
    )

    assert response.status_code == 422


def test_hides_provider_failure(monkeypatch):
    generated = AsyncMock(side_effect=twinkler_ai.GeminiError("provider details"))
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
    )

    assert response.status_code == 502
    assert response.json() == {"detail": "AI service unavailable"}
    assert "provider details" not in response.text


# --- what "AI is not configured" means since 2026-08-30 -------------------
#
# Two variables, and only these two, decide it for /api/ai/question. The
# system prompt used to be a third (TWINKLER_SYSTEM_PROMPT, empty -> 502);
# it is a code constant now, so the surface below is the whole contract.


def test_missing_provider_key_is_502(monkeypatch):
    """GEMINI_API_KEY unset -> GeminiError -> 502, no provider call."""
    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "")

    with pytest.raises(twinkler_ai.GeminiError, match="GEMINI_API_KEY"):
        asyncio.run(twinkler_ai.complete("Запрос"))

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
    )
    assert response.status_code == 502
    assert response.json() == {"detail": "AI service unavailable"}


def test_missing_hmac_key_is_503(monkeypatch):
    """AI_CLIENT_HMAC_KEY unset -> the per-client limiter fails closed -> 503.

    The limit is not silently dropped: without the pseudonymization key the
    server cannot count per client, so it refuses instead of serving unlimited.
    """
    monkeypatch.setattr(client_ip, "AI_CLIENT_HMAC_KEY", "")
    monkeypatch.setattr(twinkler_ai, "_reserve_rate_limit", real_reserve_rate_limit)

    with pytest.raises(twinkler_ai.HTTPException) as error:
        asyncio.run(twinkler_ai._enforce_rate_limit("203.0.113.7"))

    assert error.value.status_code == 503
    assert error.value.detail == "AI service temporarily unavailable"


def test_rate_limits_requests(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    limiter = AsyncMock()
    monkeypatch.setattr(twinkler_ai, "complete", generated)
    monkeypatch.setattr(twinkler_ai, "_enforce_rate_limit", limiter)
    limiter.side_effect = [
        None,
        twinkler_ai.HTTPException(
            status_code=429,
            detail="AI request limit exceeded",
            headers={"Retry-After": "60"},
        ),
    ]

    first_response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Первый запрос"},
    )
    second_response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Второй запрос"},
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 429
    assert second_response.headers["Retry-After"] == "60"
    generated.assert_awaited_once()


def test_trailing_slash_is_recorded_without_request_body(monkeypatch):
    started_threads = []

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            started_threads.append((self.args, self.kwargs))

    monkeypatch.setattr(
        middleware,
        "threading",
        SimpleNamespace(Thread=FakeThread),
    )

    response = client.post(
        "/api/ai/question/",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос"},
        follow_redirects=False,
    )

    assert response.status_code == 307
    assert len(started_threads) == 1
    args, kwargs = started_threads[0]
    assert args == ()
    assert kwargs["args"][0:3] == (
        "/api/ai/question/",
        "POST",
        307,
    )
    assert len(kwargs["args"]) == 6
    expected_client = hmac.new(
        b"test-hmac-key",
        b"testclient",
        hashlib.sha256,
    ).hexdigest()[:40]
    assert kwargs["args"][4:] == (expected_client, "")


def test_openapi_documents_public_errors():
    operation = app.openapi()["paths"]["/api/ai/question"]["post"]

    assert {"200", "403", "422", "429", "502", "503"} <= set(
        operation["responses"]
    )
    assert "Retry-After" in operation["responses"]["429"]["headers"]


def test_sends_expected_gemini_request(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url == (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            "gemini-test:generateContent"
        )
        assert request.headers["x-goog-api-key"] == "secret-test-key"
        assert request.headers["content-type"] == "application/json"
        assert json.loads(request.read()) == {
            "system_instruction": {
                "parts": [{"text": question_prompt.build_question_prompt(
                    safety.detect_language("Запрос")
                )}]
            },
            "contents": [{"role": "user", "parts": [{"text": "Запрос"}]}],
            "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.7},
        }
        return httpx.Response(
            200,
            json={"candidates": [{"content": {"parts": [{"text": "Ответ"}]}}]},
        )

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient

    def async_client(*args, **kwargs):
        return real_async_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(twinkler_ai, "AI_QUESTION_MODEL", "gemini-test")
    monkeypatch.setattr(twinkler_ai.httpx, "AsyncClient", async_client)

    assert asyncio.run(twinkler_ai.complete("Запрос")) == "Ответ"


@pytest.mark.parametrize(
    ("response", "expected_message"),
    [
        (httpx.Response(429), "Gemini request failed"),
        (httpx.Response(200, json={"candidates": []}), "Gemini returned no text"),
    ],
)
def test_handles_gemini_failures(monkeypatch, response, expected_message):
    transport = httpx.MockTransport(lambda request: response)
    real_async_client = httpx.AsyncClient

    def async_client(*args, **kwargs):
        return real_async_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(twinkler_ai.httpx, "AsyncClient", async_client)

    with pytest.raises(twinkler_ai.GeminiError, match=expected_message):
        asyncio.run(twinkler_ai.complete("Запрос"))


def test_rate_limit_reservation_is_hashed_in_memory(monkeypatch):
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 100.0)
    real_reserve_rate_limit("203.0.113.7")

    expected_hash = hmac.new(
        b"test-hmac-key",
        b"203.0.113.7",
        hashlib.sha256,
    ).hexdigest()
    assert twinkler_ai._request_times == deque([100.0])
    assert twinkler_ai._client_request_times == {expected_hash: deque([100.0])}
    assert "203.0.113.7" not in twinkler_ai._client_request_times


def test_global_in_memory_limit(monkeypatch):
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(twinkler_ai, "AI_REQUESTS_PER_MINUTE", 1)

    real_reserve_rate_limit("203.0.113.7")
    with pytest.raises(twinkler_ai.RateLimitError) as error:
        real_reserve_rate_limit("198.51.100.9")

    assert error.value.retry_after == 60


def test_per_client_in_memory_limit(monkeypatch):
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(twinkler_ai, "AI_REQUESTS_PER_MINUTE", 10)
    monkeypatch.setattr(twinkler_ai, "AI_REQUESTS_PER_CLIENT_PER_MINUTE", 1)

    real_reserve_rate_limit("203.0.113.7")
    with pytest.raises(twinkler_ai.RateLimitError) as limited_error:
        real_reserve_rate_limit("203.0.113.7")

    assert limited_error.value.retry_after == 60


def test_in_memory_limit_expires(monkeypatch):
    request_times = iter([100.0, 161.0])
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: next(request_times))
    monkeypatch.setattr(twinkler_ai, "AI_REQUESTS_PER_MINUTE", 1)
    monkeypatch.setattr(twinkler_ai, "AI_REQUESTS_PER_CLIENT_PER_MINUTE", 1)

    real_reserve_rate_limit("203.0.113.7")
    real_reserve_rate_limit("203.0.113.7")

    assert twinkler_ai._request_times == deque([161.0])


def test_rate_limiter_fails_closed(monkeypatch):
    monkeypatch.setattr(
        twinkler_ai,
        "_reserve_rate_limit",
        lambda client_key: (_ for _ in ()).throw(
            twinkler_ai.RateLimitError("limiter unavailable")
        ),
    )

    with pytest.raises(twinkler_ai.HTTPException) as error:
        asyncio.run(twinkler_ai._enforce_rate_limit("203.0.113.7"))

    assert error.value.status_code == 503


def test_transcription_requires_api_key():
    response = client.post(
        "/api/ai/transcribe",
        files={"file": ("recording.m4a", b"audio", "audio/mp4")},
    )

    assert response.status_code == 403


def test_returns_transcript_with_soft_locale_hint(monkeypatch):
    generated = AsyncMock(return_value="Господи, помоги мне.")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.m4a", b"m4a-bytes", "audio/mp4")},
        data={"locale": "ru-RU"},
    )

    assert response.status_code == 200
    assert response.json() == {"text": "Господи, помоги мне."}
    generated.assert_awaited_once_with(b"m4a-bytes", "audio/mp4", "ru-RU")


@pytest.mark.parametrize(
    "content_type",
    ["audio/mp4", "audio/x-m4a", "audio/m4a"],
)
def test_transcription_accepts_every_m4a_mime_spelling(monkeypatch, content_type):
    generated = AsyncMock(return_value="Transcript")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.m4a", b"m4a-bytes", content_type)},
    )

    assert response.status_code == 200
    generated.assert_awaited_once_with(b"m4a-bytes", content_type, None)


def test_transcription_locale_is_optional_and_m4a_has_safe_mime_fallback(monkeypatch):
    generated = AsyncMock(return_value="Original language")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={
            "file": (
                "recording.M4A",
                b"m4a-bytes",
                "application/octet-stream",
            )
        },
    )

    assert response.status_code == 200
    generated.assert_awaited_once_with(b"m4a-bytes", "audio/mp4", None)


@pytest.mark.parametrize("locale", ["r", "../../ru", "ru_RU"])
def test_transcription_rejects_invalid_locale(monkeypatch, locale):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.m4a", b"audio", "audio/mp4")},
        data={"locale": locale},
    )

    assert response.status_code == 422
    generated.assert_not_awaited()


def test_transcription_rejects_empty_audio(monkeypatch):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.m4a", b"", "audio/x-m4a")},
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "Audio file is empty"}
    generated.assert_not_awaited()


def test_transcription_rejects_oversized_audio(monkeypatch):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={
            "file": (
                "recording.m4a",
                b"x" * (twinkler_ai._MAX_AUDIO_BYTES + 1),
                "audio/mp4",
            )
        },
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "Audio file is too large"}
    generated.assert_not_awaited()


@pytest.mark.parametrize(
    "filename, content_type",
    [
        ("recording.wav", "audio/wav"),
        ("recording.m4a", "image/png"),
        ("recording.bin", "application/octet-stream"),
    ],
)
def test_transcription_rejects_unsupported_audio(monkeypatch, filename, content_type):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": (filename, b"audio", content_type)},
    )

    assert response.status_code == 415
    assert response.json() == {"detail": "Unsupported audio format"}
    generated.assert_not_awaited()


def test_invalid_audio_does_not_consume_rate_limit(monkeypatch, allow_ai_requests):
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.wav", b"audio", "audio/wav")},
    )

    assert response.status_code == 415
    allow_ai_requests.assert_not_called()
    generated.assert_not_awaited()


def test_transcription_hides_provider_failure(monkeypatch, caplog):
    generated = AsyncMock(side_effect=twinkler_ai.GeminiError("private details"))
    monkeypatch.setattr(twinkler_ai, "transcribe", generated)

    response = client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("private-name.m4a", b"private audio", "audio/mp4")},
    )

    assert response.status_code == 502
    assert response.json() == {"detail": "AI service unavailable"}
    assert "private details" not in response.text
    assert "private-name" not in response.text
    assert "private audio" not in response.text
    assert "private details" not in caplog.text
    assert "private-name" not in caplog.text
    assert "private audio" not in caplog.text


def test_sends_expected_gemini_transcription_request(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url == (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            "gemini-test:generateContent"
        )
        assert request.headers["x-goog-api-key"] == "secret-test-key"
        payload = json.loads(request.read())
        assert payload["generationConfig"] == {
            "maxOutputTokens": 4096,
            "temperature": 0,
        }
        parts = payload["contents"][0]["parts"]
        assert "original language" in parts[0]["text"]
        assert "Do not translate" in parts[0]["text"]
        assert "app locale is uk-UA" in parts[0]["text"]
        assert "weak hint" in parts[0]["text"]
        assert parts[1] == {
            "inline_data": {
                "mime_type": "audio/mp4",
                "data": base64.b64encode(b"audio bytes").decode("ascii"),
            }
        }
        return httpx.Response(
            200,
            json={"candidates": [{"content": {"parts": [{"text": "Текст"}]}}]},
        )

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient

    def async_client(*args, **kwargs):
        assert kwargs["timeout"] == 60.0
        return real_async_client(*args, transport=transport, **kwargs)

    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "secret-test-key")
    monkeypatch.setattr(twinkler_ai, "AI_TRANSCRIBE_MODEL", "gemini-test")
    monkeypatch.setattr(twinkler_ai.httpx, "AsyncClient", async_client)

    result = asyncio.run(
        twinkler_ai.transcribe(b"audio bytes", "audio/mp4", "uk-UA")
    )
    assert result == "Текст"


def test_transcription_stats_are_pseudonymized_without_user_agent(monkeypatch):
    started_threads = []

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            started_threads.append((self.args, self.kwargs))

    monkeypatch.setattr(middleware, "threading", SimpleNamespace(Thread=FakeThread))
    monkeypatch.setattr(twinkler_ai, "transcribe", AsyncMock(return_value="Текст"))

    response = client.post(
        "/api/ai/transcribe",
        headers={
            "X-API-Key": "test-api-key",
            "User-Agent": "private-device-details",
        },
        files={"file": ("private-name.m4a", b"private audio", "audio/mp4")},
    )

    assert response.status_code == 200
    assert len(started_threads) == 1
    args, kwargs = started_threads[0]
    assert args == ()
    expected_client = hmac.new(
        b"test-hmac-key",
        b"testclient",
        hashlib.sha256,
    ).hexdigest()[:40]
    assert kwargs["args"][:3] == (
        "/api/ai/transcribe",
        "POST",
        200,
    )
    assert kwargs["args"][4:] == (expected_client, "")
    assert "private-name" not in repr(kwargs)
    assert "private audio" not in repr(kwargs)


def test_openapi_documents_transcription_contract():
    operation = app.openapi()["paths"]["/api/ai/transcribe"]["post"]

    assert operation["requestBody"]["content"].keys() == {"multipart/form-data"}
    assert {"200", "403", "413", "415", "422", "429", "502", "503"} <= set(
        operation["responses"]
    )
    assert "Retry-After" in operation["responses"]["429"]["headers"]
