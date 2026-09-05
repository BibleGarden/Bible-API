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

import config
import threading

import twinkler_ai
import transcription
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


def _probe_topic(probe_id: str) -> str:
    """The topic of a `first`-stage probe — its whole text, in this schema."""
    payload = json.loads(
        (EVALUATION / "question_probe_inputs.json").read_text(encoding="utf-8")
    )
    probe = next(
        probe for probe in payload["inputs"] if probe["id"] == probe_id
    )
    assert probe["stage"] == "first" and not probe["messages"]
    return probe["topic"]


def question_body(
    topic: str = "", stage: str = "first", messages: tuple = ()
) -> dict:
    """One request body of the structured contract (ClickUp 86cbegmzz).

    `messages` is given as `(role, text)` pairs for readability; the wire
    format is the list of objects this builds.
    """
    return {
        "topic": topic,
        "stage": stage,
        "messages": [{"role": role, "text": text} for role, text in messages],
    }


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
    assert len(template) == 2143
    assert (
        hashlib.sha256(template.encode("utf-8")).hexdigest()
        == "7b860999e1ce7df3a349a585b9791e5e5f587c473596012dd38016ed365f6a45"
    )


def test_question_prompt_is_versioned():
    version = question_prompt.QUESTION_PROMPT_VERSION
    assert isinstance(version, int) and version >= 1
    # v3 = the structured request of 2026-09-05 (86cbegmzz), which took the
    # layout sentence out of the prompt; bumped with the hash above.
    assert version == 3


def test_v3_dropped_only_the_sentence_about_the_layout():
    """The one v2 rule the stage blocks replace, and nothing else.

    `question_prompt.build_user_message` now lists the questions already asked
    and the answers given, so the prompt no longer describes an incoming
    "whole conversation" it never receives (ClickUp 86cbegmzz). Every other
    rule of v2 is about the person, not the request shape, and stays.
    """
    template = question_prompt.QUESTION_PROMPT_TEMPLATE

    assert "the whole conversation so far" not in template
    assert "never repeat a question you have already asked" not in template
    assert "Respond to the most recent thing the person said" not in template
    # The neighbouring sentences are untouched, so the removal is a deletion
    # and not a rewrite.
    assert (
        "closely when you compress a sentence to fit the line. Never speak as "
        "God" in template
    )


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
    """The seam the structured request moved (ClickUp 86cbegmzz).

    `question_prompt_for` stayed a pure function of the text handed to it;
    what changed is who chooses that text — `language_source` now hands it the
    LAST user message instead of the whole `user` string.
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
        json=question_body(topic="Запрос"),
    )
    assert response.status_code == 403


# --- the structured request (ClickUp 86cbegmzz) ---------------------------
#
# The single `user` string became topic + stage + messages on 2026-09-05, and
# the server assembles the stage instructions the app used to build itself.
# The assembly itself is tested in tests/test_question_prompt.py; what is
# pinned here is what the endpoint does with it — which text the model gets,
# which text the language comes from, and which text each safety tier reads.


def test_returns_generated_text(monkeypatch):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(topic="Запрос"),
    )

    assert response.status_code == 200
    assert response.json() == {"text": "Ответ"}
    generated.assert_awaited_once_with(
        question_prompt.build_user_message("Запрос", "first", []), "Запрос"
    )


@pytest.mark.parametrize(
    ("body", "expected_language_source"),
    [
        (question_body(topic="Умерла мама"), "Умерла мама"),
        (
            question_body(
                topic="Отношения с семьёй",
                stage="next",
                messages=(
                    ("assistant", "Что сейчас тревожит тебя?"),
                    ("user", "Мне одиноко.\nХочу восстановить общение."),
                ),
            ),
            "Мне одиноко.\nХочу восстановить общение.",
        ),
        # No topic, no history: legal for next/reflect, and there is nothing
        # to detect a language from at all.
        (question_body(stage="next"), ""),
        (
            question_body(
                topic="Прошу сил",
                stage="reflect",
                messages=(("user", "Сегодня было легче, чем вчера."),),
            ),
            "Сегодня было легче, чем вчера.",
        ),
    ],
)
def test_the_model_gets_the_assembled_message_and_the_language_of_the_person(
    monkeypatch, body, expected_language_source
):
    generated = AsyncMock(return_value="Ответ")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=body,
    )

    assert response.status_code == 200
    expected_message = question_prompt.build_user_message(
        body["topic"],
        body["stage"],
        [(message["role"], message["text"]) for message in body["messages"]],
    )
    generated.assert_awaited_once_with(expected_message, expected_language_source)


def test_the_language_follows_the_last_reply_not_the_question_it_answers(
    monkeypatch,
):
    """An assistant question is OUR text and must not vote on their language.

    The app asks in the language of the prayer; a person who answers in
    another one has switched, and prompt v2's rule is to follow them.
    """
    sent = {}

    async def fake_complete(user, language_source_text=None):
        sent["prompt"] = twinkler_ai.question_prompt_for(language_source_text)
        return "Answer"

    monkeypatch.setattr(twinkler_ai, "complete", fake_complete)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(
            topic="Отношения с семьёй",
            stage="next",
            messages=(
                ("assistant", "Что сейчас тревожит тебя больше всего?"),
                ("user", "My mother stopped calling after the wedding."),
            ),
        ),
    )

    assert response.status_code == 200
    assert sent["prompt"].endswith("Answer in English.")


def test_without_any_words_of_the_person_the_prompt_names_english(monkeypatch):
    """`next`/`reflect`, empty topic, empty history — the documented fallback.

    Not `UNDETERMINED_LANGUAGE`: "answer in exactly the language of the
    person's message" points at nothing when there is no message.
    """
    assert twinkler_ai.question_prompt_for("").endswith("Answer in English.")
    assert twinkler_ai.language_source(
        twinkler_ai.CompleteRequest(topic="", stage="reflect", messages=[])
    ) == ""


def test_the_language_falls_back_to_the_assistant_turn_last():
    """Only when the person wrote nothing at all: no topic, no reply of theirs.

    Unreachable through HTTP — a non-empty history must end with a `user`
    turn, so a request that has any message has a reply of the person's in it
    — hence `model_construct`, which skips the validators. `language_source`
    is kept total anyway: it is the ordering the contract names, and a partial
    function here would fail at the one moment it is asked something new.
    """
    request = twinkler_ai.CompleteRequest.model_construct(
        topic="",
        stage="reflect",
        messages=[
            twinkler_ai.QuestionMessage(role="assistant", text="Що зараз найважче?")
        ],
    )

    assert twinkler_ai.language_source(request) == "Що зараз найважче?"
    assert twinkler_ai.question_prompt_for(
        twinkler_ai.language_source(request)
    ).endswith("Answer in Ukrainian.")


# --- the language chain is walked by decidability (86cbegmzz, review) -----
#
# `detect_language` answers `None` for a message that does not say — a short
# Cyrillic line with none of the four letters and none of the function words
# that separate Russian from Ukrainian. Stopping at the first *non-empty*
# candidate handed the prompt v2's "answer in exactly the language of the
# person's message" for those, which is the sentence v2 exists to avoid (Qwen
# broke it 6/81). So the walk continues to the next thing the SAME PERSON
# wrote. Measured on the evaluation set: 9 of 33 inputs undetermined before,
# 6 after.


def _prompt_language_of(**body) -> str:
    request = twinkler_ai.CompleteRequest(**body)
    return twinkler_ai.question_prompt_for(twinkler_ai.language_source(request))


def test_an_undecidable_reply_lets_the_topic_name_the_language():
    """«Помоги» says nothing; the goal the same person typed does."""
    assert _prompt_language_of(
        topic="Что делать с обидой на брата",
        stage="next",
        messages=[
            {"role": "assistant", "text": "Що зараз найважче?"},
            {"role": "user", "text": "Помоги"},
        ],
    ).endswith("Answer in Russian.")


def test_the_topic_answers_for_an_undecidable_reply_in_english_too():
    assert _prompt_language_of(
        topic="Praying about my father",
        stage="next",
        messages=[{"role": "user", "text": "Помоги"}],
    ).endswith("Answer in English.")


def test_an_earlier_reply_answers_when_neither_the_last_one_nor_the_topic_can():
    """The shape of `ru-001`: the evidence is two turns back, and it counts.

    Which is why the walk does not stop at the topic — on the evaluation set
    the topic alone recovered none of the three inputs that regressed.
    """
    assert _prompt_language_of(
        topic="Благодарность за рождение дочки",
        stage="next",
        messages=[
            {"role": "user", "text": "Мы ждали её несколько лет, и вот она родилась здоровой."},
            {"role": "assistant", "text": "Что сейчас важнее всего сказать Богу?"},
            {"role": "user", "text": "Хочу просто сказать Богу спасибо."},
        ],
    ).endswith("Answer in Russian.")


def test_when_nothing_the_person_wrote_decides_the_prompt_says_so():
    """v2's wording, and our own question still does not get a vote.

    The assistant turn here IS decidable Russian. It must not be reached: a
    question of ours is not evidence about the language of their answer.
    """
    prompt = _prompt_language_of(
        topic="Помоги",
        stage="next",
        messages=[
            {"role": "assistant", "text": "Что сейчас тяжелее всего?"},
            {"role": "user", "text": "Помоги"},
        ],
    )

    assert not prompt.endswith("Answer in Russian.")
    assert prompt.endswith(
        f"Answer in {question_prompt.UNDETERMINED_LANGUAGE}."
    )


# --- the despair rule lives in code (ClickUp 86cbegg23) -------------------
#
# The measurement of 2026-09-05 (86cbegctz) had Qwen3-30B answer the explicit
# despair input with a guiding question in 3 samples out of 3 while Gemini
# dropped the format as instructed. So the endpoint no longer depends on the
# instruction: `app/safety.py` decides, and these tests pin both tiers at the
# level the client sees. The detector itself is tested in tests/test_safety.py.


@pytest.mark.parametrize(
    "body",
    [
        question_body(topic=_probe_topic("probe-despair")),
        # The same phrase as the newest reply of a conversation.
        question_body(
            topic="Развод",
            stage="next",
            messages=(
                ("assistant", "Что сейчас тяжелее всего?"),
                ("user", _probe_topic("probe-despair")),
            ),
        ),
        question_body(
            topic="Развод",
            stage="reflect",
            messages=(("user", _probe_topic("probe-despair")),),
        ),
    ],
)
def test_an_explicit_despair_message_never_reaches_the_model(monkeypatch, body):
    generated = AsyncMock(return_value="Ты сейчас очень одинок?")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=body,
    )

    assert response.status_code == 200
    assert response.json() == {"text": safety.SAFETY_REPLIES["ru"]}
    generated.assert_not_awaited()
    assert "?" not in response.json()["text"]


def test_despair_in_an_older_reply_lets_the_conversation_go_on(monkeypatch):
    """The bug this ticket closes (ClickUp 86cbegmzz).

    Tier 1 read the whole conversation while the request was one string, so
    the phrase that was answered with the fixed reply kept answering every
    later question of that prayer with it. Both tiers read the LAST reply now
    (Maria, 2026-09-05): the model is called again, and this time tier 2 no
    longer refuses its question either — the older phrase is someone else's
    turn now, and the companion must be able to keep asking for the rest of
    the prayer.
    """
    generated = AsyncMock(return_value="Что помогло тебе сегодня?")
    monkeypatch.setattr(twinkler_ai, "complete", generated)
    body = question_body(
        topic="Развод",
        stage="next",
        messages=(
            ("assistant", "Что сейчас тяжелее всего?"),
            ("user", _probe_topic("probe-despair")),
            ("assistant", "Что помогло тебе продержаться сегодня?"),
            ("user", "Позвонила сестре, стало чуть легче, но вечером снова пусто."),
        ),
    )

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=body,
    )

    assert response.status_code == 200
    generated.assert_awaited_once()
    # The last reply carries no despair signal of its own, so the model's
    # real question is kept — the fixed reply does not answer for it.
    assert response.json() == {"text": "Что помогло тебе сегодня?"}


def test_an_older_despair_reply_keeps_a_warm_answer(monkeypatch):
    """Tier 2 is a floor, not a mute button: a non-question answer stands."""
    warm = "Хорошо, что ты позвонила сестре — ты не одна в этом."
    monkeypatch.setattr(twinkler_ai, "complete", AsyncMock(return_value=warm))

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(
            topic="Развод",
            stage="next",
            messages=(
                ("user", _probe_topic("probe-despair")),
                ("assistant", "Что помогло тебе продержаться сегодня?"),
                ("user", "Позвонила сестре, стало чуть легче."),
            ),
        ),
    )

    assert response.status_code == 200
    assert response.json() == {"text": warm}


def test_a_topic_is_never_read_as_a_reply_the_person_did_not_send(monkeypatch):
    """`next`/`reflect` with an empty history: tier 1 has nothing to read.

    Substituting the topic there would answer every question of the prayer
    with the fixed reply — the same forever-loop as above, wearing a topic.
    The model is called normally and tier 2 still guards the answer.
    """
    generated = AsyncMock(return_value="Ты не один с этим.")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(topic=_probe_topic("probe-despair"), stage="next"),
    )

    assert response.status_code == 200
    generated.assert_awaited_once()
    assert response.json() == {"text": "Ты не один с этим."}


def test_the_same_topic_on_the_first_question_is_answered_in_code(monkeypatch):
    """…but at `first` the topic IS the newest thing the person wrote."""
    generated = AsyncMock(return_value="unused")
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(topic=_probe_topic("probe-despair"), stage="first"),
    )

    assert response.status_code == 200
    assert response.json() == {"text": safety.SAFETY_REPLIES["ru"]}
    generated.assert_not_awaited()


def test_the_fixed_reply_is_in_the_language_of_the_person_not_of_the_blocks(
    monkeypatch,
):
    """The assembled message is Russian whatever the prayer is; the reply is not.

    Tier 2 reads the person's own last reply, same as tier 1, precisely so the
    stage instructions cannot outvote an English prayer.
    """
    monkeypatch.setattr(
        twinkler_ai,
        "complete",
        AsyncMock(return_value="What would you like to tell God?"),
    )

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(
            topic="Feeling worthless",
            stage="next",
            messages=(
                ("assistant", "What is heaviest right now?"),
                ("user", "I keep thinking everyone would be fine without me."),
            ),
        ),
    )

    assert response.status_code == 200
    assert response.json() == {"text": safety.SAFETY_REPLIES["en"]}


def test_the_last_reply_now_outvotes_a_long_russian_topic(monkeypatch):
    """Maria's 2026-09-05 decision: tier 2's reply language follows
    `language_source`, same as tier 1 and the prompt — not a vote over
    topic + every reply.

    A long Russian topic used to outvote a short English reply that actually
    carried the tier-2 phrase (`written_by_the_person` joined them, and
    `safety.check_reply` resolved the language from that one string), so a
    person answering in English could get the fixed reply in Russian. Now the
    last reply alone decides, the same source the prompt and tier 1 already
    used — so the two halves of one request can no longer disagree.
    """
    monkeypatch.setattr(
        twinkler_ai, "complete", AsyncMock(return_value="Что тебя сейчас держит?")
    )

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(
            topic="Развод и постоянные ссоры с мамой, не знаю, что делать",
            stage="next",
            messages=(
                ("assistant", "Что сейчас тяжелее всего?"),
                ("user", "I'm a burden"),
            ),
        ),
    )

    assert response.status_code == 200
    assert response.json() == {"text": safety.SAFETY_REPLIES["en"]}
    # The person's last words are English, and that is what the prompt and
    # tier 1 already used — tier 2's reply language now agrees with them.
    assert safety.detect_language("I'm a burden") == "en"


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
        json=question_body(topic=_probe_topic("probe-despair")),
    )

    assert response.status_code == 200
    allow_ai_requests.assert_called_once_with("testclient")


def test_a_weak_signal_answered_with_a_question_is_replaced(monkeypatch):
    """Tier 2: the model was asked, and answered the wrong way."""
    generated = AsyncMock(return_value="What would you like to tell God?")
    monkeypatch.setattr(twinkler_ai, "complete", generated)
    # en-005 of the approved scenarios, as the app sends it now.
    body = question_body(
        topic="Feeling worthless",
        stage="reflect",
        messages=(("user", "I keep thinking everyone would be fine without me."),),
    )

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=body,
    )

    assert response.status_code == 200
    assert response.json() == {"text": safety.SAFETY_REPLIES["en"]}
    generated.assert_awaited_once()


def test_a_weak_signal_answered_warmly_keeps_the_model_answer(monkeypatch):
    """Tier 2 is a floor under the answer, not a replacement for a good one."""
    warm = "You are not alone in this, and it matters that you are still here."
    monkeypatch.setattr(twinkler_ai, "complete", AsyncMock(return_value=warm))

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(topic="I am so tired of living"),
    )

    assert response.status_code == 200
    assert response.json() == {"text": warm}


def test_an_ordinary_message_is_untouched_by_either_tier(monkeypatch):
    generated = AsyncMock(return_value="Что тебе сейчас труднее всего?")
    monkeypatch.setattr(twinkler_ai, "complete", generated)
    topic = _probe_topic("probe-tech")

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(topic=topic),
    )

    assert response.status_code == 200
    assert response.json() == {"text": "Что тебе сейчас труднее всего?"}
    generated.assert_awaited_once_with(
        question_prompt.build_user_message(topic, "first", []), topic
    )


@pytest.mark.parametrize(
    ("body", "reply", "expected", "private_words"),
    [
        (
            question_body(topic="Я больше не хочу жить, началась паника"),
            "Ты сейчас очень одинок?",
            "tier=1 pattern=ru.no-wish-to-live language=ru reply_version=2 stage=first",
            ("больше", "жить", "паника", "одинок", "сейчас"),
        ),
        (
            question_body(
                topic="Feeling worthless",
                stage="next",
                messages=(("user", "Everyone would be fine without me"),),
            ),
            "What hurts the most right now?",
            "tier=2 pattern=en.better-without-me language=en reply_version=2 stage=next",
            ("worthless", "Everyone", "fine", "hurts"),
        ),
    ],
)
def test_the_safety_log_records_the_rule_and_not_the_message(
    monkeypatch, caplog, body, reply, expected, private_words
):
    monkeypatch.setattr(twinkler_ai, "complete", AsyncMock(return_value=reply))

    with caplog.at_level("WARNING", logger="twinkler_ai"):
        response = client.post(
            "/api/ai/question",
            headers={"X-API-Key": "test-api-key"},
            json=body,
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
        json=question_body(topic="Запрос"),
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
        json=question_body(topic="Запрос"),
    )

    assert response.status_code == 200
    # The RIGHTMOST element: the address the trusted proxy itself appended.
    # The left one is whatever the caller put in the header (ClickUp 86cbbq6vz).
    allow_ai_requests.assert_called_once_with("192.0.2.1")


@pytest.mark.parametrize(
    ("payload", "why"),
    [
        ({"topic": "x" * 2001, "stage": "first", "messages": []}, "topic too long"),
        ({"topic": "", "stage": "later", "messages": []}, "unknown stage"),
        ({"stage": "first", "messages": []}, "topic missing"),
        ({"topic": "", "messages": []}, "stage missing"),
        ({"topic": "", "stage": "first"}, "messages missing"),
        (
            {"topic": "", "stage": "first", "messages": [], "system": "мой промпт"},
            "unknown field",
        ),
        (
            {
                "topic": "",
                "stage": "next",
                "messages": [{"role": "user", "text": ""}],
            },
            "empty turn",
        ),
        (
            {
                "topic": "",
                "stage": "next",
                "messages": [{"role": "system", "text": "x"}],
            },
            "unknown role",
        ),
        (
            {
                "topic": "",
                "stage": "next",
                "messages": [{"role": "user", "text": "x", "at": 1}],
            },
            "unknown field in a turn",
        ),
        (
            {
                "topic": "",
                "stage": "next",
                "messages": [{"role": "user", "text": "x"}] * 41,
            },
            "more than 40 turns",
        ),
        (
            {
                "topic": "x" * 2000,
                "stage": "next",
                "messages": [{"role": "user", "text": "y" * 14001}],
            },
            "topic and turns over 16000 together",
        ),
    ],
)
def test_rejects_invalid_requests(payload, why):
    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=payload,
    )

    assert response.status_code == 422, why


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {"user": "Запрос"},
            "the 'user' field was removed on 2026-09-05",
        ),
        (
            {"topic": "", "stage": "next", "messages": [], "last_user_message": "x"},
            "the 'last_user_message' field was removed on 2026-09-05",
        ),
        (
            question_body(
                topic="Тема",
                stage="first",
                messages=(("user", "Мне одиноко"),),
            ),
            "stage 'first' is the opening question and takes no history",
        ),
        (
            question_body(
                topic="Тема",
                stage="next",
                messages=(
                    ("user", "Мне одиноко"),
                    ("assistant", "Что для тебя тяжелее всего?"),
                ),
            ),
            "a non-empty history must end with a 'user' turn",
        ),
    ],
)
def test_the_422_says_what_is_wrong(payload, expected):
    """A rejected request must be diagnosable from its answer alone.

    The mobile app is the only client and both ends changed at once, so the
    old body has no transitional support — but "Extra inputs are not
    permitted" would send someone reading a log to the wrong place.
    """
    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=payload,
    )

    assert response.status_code == 422
    assert expected in response.text


def test_the_limits_are_the_ones_the_client_enforces(monkeypatch):
    """40 turns and 16 000 characters together are accepted, one more is not."""
    monkeypatch.setattr(twinkler_ai, "complete", AsyncMock(return_value="Ответ"))
    body = question_body(
        topic="т" * 2000,
        stage="next",
        messages=tuple(("user", "о" * 350) for _ in range(40)),
    )
    assert len(body["topic"]) + sum(
        len(m["text"]) for m in body["messages"]
    ) == 16000

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=body,
    )
    assert response.status_code == 200

    body["messages"].append({"role": "user", "text": "ещё"})
    assert (
        client.post(
            "/api/ai/question",
            headers={"X-API-Key": "test-api-key"},
            json=body,
        ).status_code
        == 422
    )


def test_hides_provider_failure(monkeypatch):
    generated = AsyncMock(side_effect=twinkler_ai.GeminiError("provider details"))
    monkeypatch.setattr(twinkler_ai, "complete", generated)

    response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(topic="Запрос"),
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
        json=question_body(topic="Запрос"),
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
        json=question_body(topic="Первый запрос"),
    )
    second_response = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(topic="Второй запрос"),
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
        json=question_body(topic="Запрос"),
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


# --- the transcription provider seam (ClickUp 86cbegg3m, ADR 0012) ---------


def transcribe_provider(provider: str, model: str = "whisper-large-v3",
                        endpoint: str = "", api_key: str = "") -> object:
    """One resolved transcription stage, as `config.resolve_stage` builds it."""
    return config.StageProvider(
        "transcribe", provider, model, endpoint, api_key
    )


def post_recording(locale: str | None = "ru-RU"):
    data = {"locale": locale} if locale is not None else None
    return client.post(
        "/api/ai/transcribe",
        headers={"X-API-Key": "test-api-key"},
        files={"file": ("recording.m4a", b"m4a-bytes", "audio/mp4")},
        data=data,
    )


def test_the_local_provider_runs_the_model_off_the_event_loop(monkeypatch):
    """A transcription is seconds of arithmetic: on the loop it would stall
    every other request this worker is serving."""
    recorded = {}

    class FakeLocalTranscriber:
        def transcribe(self, audio, mime_type, locale):
            recorded["thread"] = threading.get_ident()
            recorded["args"] = (audio, mime_type, locale)
            return "Господи, помоги мне."

    monkeypatch.setattr(
        twinkler_ai, "TRANSCRIBE_PROVIDER", transcribe_provider("local")
    )
    monkeypatch.setattr(
        twinkler_ai, "LocalTranscriber", lambda: FakeLocalTranscriber()
    )

    async def run():
        recorded["loop_thread"] = threading.get_ident()
        return await twinkler_ai.transcribe(b"m4a-bytes", "audio/mp4", "ru-RU")

    assert asyncio.run(run()) == "Господи, помоги мне."
    assert recorded["args"] == (b"m4a-bytes", "audio/mp4", "ru-RU")
    assert recorded["thread"] != recorded["loop_thread"]


def test_the_local_provider_answers_the_documented_shape(monkeypatch):
    class FakeLocalTranscriber:
        def transcribe(self, audio, mime_type, locale):
            return "Господи, помоги мне."

    monkeypatch.setattr(
        twinkler_ai, "TRANSCRIBE_PROVIDER", transcribe_provider("local")
    )
    monkeypatch.setattr(
        twinkler_ai, "LocalTranscriber", lambda: FakeLocalTranscriber()
    )

    response = post_recording()

    assert response.status_code == 200
    assert response.json() == {"text": "Господи, помоги мне."}


def test_a_local_model_failure_is_the_same_502(monkeypatch, caplog):
    """Contract parity: the endpoint's failure says nothing about which
    provider was serving it, or about the recording."""
    class FakeLocalTranscriber:
        def transcribe(self, audio, mime_type, locale):
            raise twinkler_ai.TranscriptionUnavailable(
                "local transcription failed on private-recording"
            )

    monkeypatch.setattr(
        twinkler_ai, "TRANSCRIBE_PROVIDER", transcribe_provider("local")
    )
    monkeypatch.setattr(
        twinkler_ai, "LocalTranscriber", lambda: FakeLocalTranscriber()
    )

    response = post_recording()

    assert response.status_code == 502
    assert response.json() == {"detail": "AI service unavailable"}
    assert "private-recording" not in response.text
    assert "private-recording" not in caplog.text


def test_the_remote_provider_posts_to_the_audio_api(monkeypatch):
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["auth"] = request.headers.get("authorization")
        captured["body"] = request.read()
        return httpx.Response(200, json={"text": "Господи, помоги мне."})

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(
            *args, transport=transport, **kwargs
        ),
    )
    monkeypatch.setattr(
        twinkler_ai,
        "TRANSCRIBE_PROVIDER",
        transcribe_provider(
            "openai_compat",
            model="Systran/faster-whisper-large-v3",
            endpoint="https://whisper.example:8000/v1",
            api_key="audio-key",
        ),
    )

    response = post_recording()

    assert response.status_code == 200
    assert response.json() == {"text": "Господи, помоги мне."}
    assert captured["url"] == (
        "https://whisper.example:8000/v1/audio/transcriptions"
    )
    assert captured["auth"] == "Bearer audio-key"
    assert b'name="model"' in captured["body"]
    assert b"m4a-bytes" in captured["body"]


def test_a_remote_failure_is_the_same_502(monkeypatch):
    transport = httpx.MockTransport(
        lambda request: httpx.Response(500, text="model server on fire")
    )
    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(
            *args, transport=transport, **kwargs
        ),
    )
    monkeypatch.setattr(
        twinkler_ai,
        "TRANSCRIBE_PROVIDER",
        transcribe_provider(
            "openai_compat",
            endpoint="https://whisper.example:8000/v1",
            api_key="audio-key",
        ),
    )
    # One attempt and no backoff: the ladder itself is tested in
    # tests/test_transcription.py, and this test is about the 502.
    monkeypatch.setattr(
        twinkler_ai,
        "RemoteTranscriber",
        lambda *args, **kwargs: transcription.RemoteTranscriber(
            *args, **{**kwargs, "attempts": 1}
        ),
    )

    response = post_recording()

    assert response.status_code == 502
    assert response.json() == {"detail": "AI service unavailable"}
    assert "fire" not in response.text


def test_no_google_host_is_dialled_when_no_stage_is_gemini(monkeypatch):
    """The tripwire of the whole local-models umbrella: with the chat stages
    on an OpenAI-compatible server and transcription on the audio one, a
    request must not reach generativelanguage.googleapis.com — the address
    every Gemini path in this module hard-codes."""
    hosts = []

    def handler(request: httpx.Request) -> httpx.Response:
        hosts.append(request.url.host)
        if request.url.path.endswith("/audio/transcriptions"):
            return httpx.Response(200, json={"text": "Господи, помоги мне."})
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "Что ты сейчас чувствуешь?"}}]},
        )

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(
            *args, transport=transport, **kwargs
        ),
    )
    monkeypatch.setattr(
        twinkler_ai,
        "QUESTION_PROVIDER",
        config.StageProvider(
            "question", "openai_compat", "qwen3-30b",
            "https://llm.example:8443/v1", "chat-key",
        ),
    )
    monkeypatch.setattr(
        twinkler_ai,
        "TRANSCRIBE_PROVIDER",
        transcribe_provider(
            "openai_compat",
            endpoint="https://whisper.example:8000/v1",
            api_key="audio-key",
        ),
    )

    question = client.post(
        "/api/ai/question",
        headers={"X-API-Key": "test-api-key"},
        json=question_body(topic="Мне тревожно перед разговором"),
    )
    recording = post_recording()

    assert question.status_code == 200
    assert recording.status_code == 200
    assert set(hosts) == {"llm.example", "whisper.example"}
    assert not any("google" in host for host in hosts)


def test_the_gemini_path_is_still_the_default_provider(monkeypatch):
    """`gemini` is the last branch rather than an `else: raise` — an unknown
    provider cannot reach the seam, config refuses it at start-up."""
    monkeypatch.setattr(
        twinkler_ai, "TRANSCRIBE_PROVIDER", transcribe_provider("gemini")
    )
    monkeypatch.setattr(twinkler_ai, "GEMINI_API_KEY", "")

    with pytest.raises(twinkler_ai.AIError, match="GEMINI_API_KEY"):
        asyncio.run(twinkler_ai.transcribe(b"m4a", "audio/mp4", None))
