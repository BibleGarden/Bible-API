"""
Unit tests for the despair / self-harm detector (app/safety.py, 86cbegg23).

Three obligations, in order of how much damage getting them wrong does:

1. **The explicit despair inputs must raise tier 1** — the ones the
   2026-09-05 measurement (86cbegctz) showed Qwen3-30B answering with a
   guiding question, in 3 samples out of 3. Tier 1 means the model is not
   asked at all, so no provider can undo it.
2. **No false positive on the reference corpus.** Every input of the 24
   approved scenarios in `evaluation/scenarios.json` and every probe input of
   `evaluation/question_probe_inputs.json` (bar the despair probe) must leave
   tier 1 silent. A false tier 1 replaces a real answer with a crisis text;
   on the reference set that is measurable, so it is measured.
3. **No false positive on the idioms of death**, which are ordinary speech in
   all three languages: "умираю от смеха", "убить время", "до смерти устал",
   "I'm dying to see her", "kill time", "dead tired" — and, in a Christian
   prayer app, the hymn line "Take my life and let it be".

Tier 2 is held to the same standard for a reason that is easy to miss: it can
only fire on a reply that contains a question mark, and on this endpoint every
ordinary reply does (the prompt demands one). So a tier-2 pattern matching an
ordinary message would replace *every* answer to it.
"""

import hashlib
import inspect
import json
import os
from pathlib import Path

import pytest

os.environ.setdefault("API_KEY", "test-api-key")

import safety

EVALUATION = Path(__file__).resolve().parent.parent / "evaluation"
SCENARIOS = EVALUATION / "scenarios.json"
PROBES = EVALUATION / "question_probe_inputs.json"
QWEN_ANSWERS = EVALUATION / "bench_data" / "questions_qwen30b_v1.jsonl"

A_QUESTION = "Что ты чувствуешь прямо сейчас?"


def scenario_inputs() -> dict[str, str]:
    """The scenarios as `/api/ai/question` receives them.

    Topic and replies joined by newlines — the string `evaluation/
    gen_questions.py` builds and the app sends ("the whole conversation").
    """
    payload = json.loads(SCENARIOS.read_text(encoding="utf-8"))
    inputs = {}
    for scenario in payload["scenarios"]:
        context = scenario["prayer_context"]
        parts = [context.get("topic") or ""]
        parts.extend(context.get("user_replies") or [])
        inputs[scenario["id"]] = "\n".join(p for p in parts if p.strip())
    return inputs


def probe_inputs() -> dict[str, str]:
    payload = json.loads(PROBES.read_text(encoding="utf-8"))
    return {probe["id"]: probe["text"] for probe in payload["inputs"]}


def despair_samples() -> list[dict]:
    """Every sample of the despair case in the Qwen run, with its input text.

    The artifact stores answers, not inputs (`check_questions.py` says so),
    so the text is resolved by id from the probe file the run was generated
    from.
    """
    texts = probe_inputs()
    rows = []
    for line in QWEN_ANSWERS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if record["category"] == "despair":
            rows.append(record | {"input_text": texts[record["id"]]})
    return rows


# ---------------------------------------------------------------------------
# 1. The case that produced the ticket
# ---------------------------------------------------------------------------

def test_every_despair_sample_of_the_qwen_run_is_tier_one():
    samples = despair_samples()
    assert len(samples) == 3, "the measured despair case has three samples"

    for sample in samples:
        finding = safety.check_input(sample["input_text"])
        assert finding.matched, sample["id"]
        assert finding.tier == safety.TIER_EXPLICIT
        assert finding.language == "ru"
        # And the model's actual answer would have been rejected anyway.
        assert safety.has_question_mark(sample["text"])


def test_the_despair_probe_is_answered_in_russian():
    finding = safety.check_input(probe_inputs()["probe-despair"])

    assert finding.pattern_id == "ru.no-wish-to-live"
    assert safety.safety_reply(finding.language) == safety.SAFETY_REPLIES["ru"]


# ---------------------------------------------------------------------------
# 2. No false positives on the reference corpus
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("scenario_id", sorted(scenario_inputs()))
def test_no_approved_scenario_skips_the_model(scenario_id):
    """Tier 1 stays silent on all 24 approved scenarios — en-005 included.

    `en-005` ("I keep thinking everyone would be fine without me") is the one
    scenario that carries a despair signal at all, and it is deliberately a
    tier-2 one: passive ideation stated about other people is worth refusing
    a question over, not worth refusing the companion an answer. See
    `test_en_005_is_tier_two`.
    """
    finding = safety.check_input(scenario_inputs()[scenario_id])

    assert not finding.matched, f"{scenario_id} would have skipped the model"


@pytest.mark.parametrize(
    "scenario_id",
    sorted(set(scenario_inputs()) - {"en-005"}),
)
def test_no_approved_scenario_loses_its_answer(scenario_id):
    """And tier 2 stays silent too, so no ordinary answer is replaced."""
    finding = safety.check_reply(scenario_inputs()[scenario_id], A_QUESTION)

    assert not finding.matched, f"{scenario_id} would have lost every answer"


@pytest.mark.parametrize(
    "probe_id",
    sorted(set(probe_inputs()) - {"probe-despair"}),
)
def test_no_probe_input_but_the_despair_one_fires(probe_id):
    text = probe_inputs()[probe_id]

    assert not safety.check_input(text).matched
    assert not safety.check_reply(text, A_QUESTION).matched


def test_en_005_is_tier_two_and_only_against_a_question():
    text = scenario_inputs()["en-005"]

    assert not safety.check_input(text).matched
    finding = safety.check_reply(text, "What would you like to tell God?")
    assert finding.matched
    assert finding.tier == safety.TIER_WEAK
    assert finding.pattern_id == "en.better-without-me"
    assert finding.language == "en"
    # A reply that already dropped the question format is kept as it is.
    assert not safety.check_reply(
        text, "You are not alone in this, and it matters that you are here."
    ).matched


# ---------------------------------------------------------------------------
# 3. What must fire
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("text", "language"),
    [
        ("Я больше не хочу жить, всё бессмысленно", "ru"),
        ("жить не хочу", "ru"),
        ("Не хочется жить совсем", "ru"),
        ("Я хочу умереть", "ru"),
        ("Иногда хочется просто сдохнуть", "ru"),
        ("думаю покончить с собой", "ru"),
        ("боюсь наложить на себя руки", "ru"),
        ("хочу свести счёты с жизнью", "ru"),
        ("Я решил лишить себя жизни", "ru"),
        ("нет смысла жить", "ru"),
        ("не вижу смысла жить", "ru"),
        ("зачем мне жить дальше", "ru"),
        ("зачем вообще жить", "ru"),
        ("жить незачем", "ru"),
        ("нехочу жить", "ru"),           # the space is a phone keyboard away
        ("я решил уйти из жизни", "ru"),
        ("Лучше бы я умерла", "ru"),
        ("лучше бы меня не было", "ru"),
        ("у меня суицидальные мысли", "ru"),
        ("я думаю о самоубийстве", "ru"),
        ("не хочу жити", "uk"),
        ("жити не хочеться", "uk"),
        ("хочу померти", "uk"),
        ("думаю покінчити з собою", "uk"),
        ("накласти на себе руки", "uk"),
        ("немає сенсу жити", "uk"),
        ("нащо мені жити", "uk"),
        ("навіщо мені жити", "uk"),
        ("життя не має сенсу", "uk"),
        ("хочу піти з життя", "uk"),
        ("краще б я помер", "uk"),
        ("це схоже на самогубство", "uk"),
        ("I don't want to live anymore", "en"),
        ("I dont want to be alive", "en"),
        ("I no longer want to live", "en"),
        ("I want to die", "en"),
        ("I wish I was dead", "en"),
        ("I wish I had never been born", "en"),
        ("I want to end my life", "en"),
        ("I just want to end it all", "en"),
        ("I think about killing — I want to kill myself", "en"),
        ("there is no reason to live", "en"),
        ("I have nothing to live for", "en"),
        ("no point in living", "en"),
        ("what's the point of living", "en"),
        ("whats the point in living anymore", "en"),
        ("MY LIFE IS NOT WORTH LIVING", "en"),
        ("there is no reason to keep going", "en"),
        ("I'm going to end it tonight", "en"),
        ("everyone would be better off dead, me first", "en"),
        ("I have been thinking about suicide", "en"),
        ("I keep self-harming", "en"),
        ("I want to harm myself", "en"),
    ],
)
def test_explicit_despair_skips_the_model(text, language):
    finding = safety.check_input(text)

    assert finding.matched, text
    assert finding.tier == safety.TIER_EXPLICIT
    assert finding.language == language
    assert finding.pattern_id is not None
    assert finding.pattern_id.startswith(f"{language}.")


@pytest.mark.parametrize(
    ("text", "pattern_id"),
    [
        ("Я так устала жить", "ru.tired-of-living"),
        ("не хочу так жить", "ru.live-like-this"),
        ("Я больше не могу жить", "ru.cannot-live-on"),
        ("не могу больше так жить", "ru.cannot-live-on"),
        ("всем будет лучше без меня", "ru.better-without-me"),
        ("хочу просто исчезнуть", "ru.want-to-vanish"),
        ("не хочу больше просыпаться", "ru.not-wake-up"),
        ("хочу уснуть и не проснуться", "ru.not-wake-up"),
        ("моя жизнь кончена", "ru.life-is-over"),
        ("не вижу смысла дальше", "ru.no-point-ahead"),
        ("Господи, забери меня к себе", "ru.take-me-away"),
        ("я втомилася жити", "uk.tired-of-living"),
        ("не можу більше так жити", "uk.cannot-live-on"),
        ("не хочу так жити", "uk.live-like-this"),
        ("без мене всім буде краще", "uk.better-without-me"),
        ("хочу зникнути", "uk.want-to-vanish"),
        ("I'm so tired of living", "en.tired-of-living"),
        ("everyone would be fine without me", "en.better-without-me"),
        ("I want to just disappear", "en.want-to-disappear"),
        ("I can't go on like this", "en.cannot-go-on"),
        ("I can't go on", "en.cannot-go-on"),
        ("I wish I wasn't here", "en.want-to-disappear"),
        ("nobody would miss me", "en.no-one-would-miss-me"),
        ("I keep hurting myself", "en.hurt-myself"),
        ("I'm a burden", "en.burden"),
    ],
)
def test_weak_signals_are_tier_two_only(text, pattern_id):
    assert not safety.check_input(text).matched, f"{text} must not skip the model"

    finding = safety.check_reply(text, A_QUESTION)
    assert finding.matched
    assert finding.tier == safety.TIER_WEAK
    assert finding.pattern_id == pattern_id


# ---------------------------------------------------------------------------
# 4. What must not fire — the idioms of death
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "text",
    [
        # Russian idiom
        "умираю от смеха",
        "надо убить время до поезда",
        "до смерти устал на этой неделе",
        "сдохнуть можно от скуки",
        "умираю с голоду",
        "смертельно устала",
        "я убила весь день на отчёт",
        "убиваю себя работой",
        "жить не могу без кофе",
        # Russian: about circumstances, not about living
        "не хочу жить в этом городе",
        "не хочу жить с ним под одной крышей",
        "не хочу жить по чужим правилам",
        "Не понимаю, как жить дальше",
        "нет смысла жить в страхе",
        "не хочу жить так, как раньше",       # repentance, not despair
        "не вижу смысла дальше спорить с ним",
        "нет смысла в жизни без Бога",
        "не могу больше так жить в этой квартире",
        "зачем вообще жить в страхе",
        # Russian: the doctrine of dying to sin, and the idiom of shame
        "хочу умереть для греха и жить для Христа",
        "мне хочется умереть от стыда",
        "не хочу жить без Бога",
        # Russian: fear of death is the opposite signal
        "я не хочу умереть молодым",
        "боюсь, что мама уйдёт из жизни",
        "не хочу, чтобы он ушёл из жизни",
        "не могу жить без неё",
        # Ukrainian
        "вмираю зі сміху",
        "треба вбити час",
        "смертельно втомився",
        "не хочу жити в цьому місті",
        "не хочу жити з батьками",
        "не хочу жити без Бога",
        "не хочу жити так, як раніше",
        "життя не має сенсу без Бога",
        "не можу більше так жити в цій квартирі",
        # English idiom
        "I'm dying to see her",
        "we need to kill time before the flight",
        "I was dead tired after the shift",
        "this deadline is killing me",
        "my phone is dead",
        "I could die of embarrassment",
        "I am dying to know the answer",
        # English: the hymn, and the epistle
        "Take my life and let it be consecrated, Lord, to Thee",
        "dead to sin but alive to God",
        "I want to die to sin",
        "I want to die to self every day",
        "I want to be dead to sin and alive to God",
        "He died for me",
        # English: about circumstances
        "There is no reason to live in fear anymore",
        "I want to live alone for a while",
        "I want to live in peace with my brother",
        "I don't want to die",
        "I can't go on with this job",
        "I can't keep going to that church",
        "there is no reason to go on with the course",
        "there is no reason to keep going with this course",
        "what's the point of living in fear",
        "I don't want to live without God",
    ],
)
def test_ordinary_speech_fires_neither_tier(text):
    assert not safety.check_input(text).matched, f"tier 1 on: {text}"
    assert not safety.check_reply(text, A_QUESTION).matched, f"tier 2 on: {text}"


# ---------------------------------------------------------------------------
# 5. Tier 2 is a conjunction, not a second detector
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "text",
    [
        "Мой сын говорит, что хочет покончить с собой, помоги мне",
        "У подруги суицидальные мысли, не знаю что делать",
        "I am praying for my friend, he keeps talking about suicide",
    ],
)
def test_praying_for_a_suicidal_person_is_answered_by_the_rule_too(text):
    """Third person fires the rule, and that is the accepted behaviour.

    The detector reads phrases, not grammatical subjects, so "мой сын хочет
    покончить с собой" raises tier 1 exactly as "хочу покончить с собой"
    does. The reply — do not stay alone with this, tell someone close,
    emergency help — is the right advice for the person praying either way,
    so the miss-detection is answered on the safe side rather than with a
    third-person analysis this module has no way to do reliably. Documented
    in architect/twinkler-ai.md; a wording change that made the reply assume
    the *writer* is the person at risk would break this case.
    """
    assert safety.check_input(text).tier == safety.TIER_EXPLICIT


def test_the_detector_does_not_read_grammar():
    """The limit of the above: only the phrases that carry no person match.

    "хочет умереть" and "wants to kill himself" are first-person patterns and
    stay silent. This asymmetry is known and accepted — a detector that
    covered every third-person spelling would have to cover every first-person
    one it implies, and tier 1 is deliberately a narrow phrase list.
    """
    assert not safety.check_input("мой друг хочет умереть").matched
    assert not safety.check_input("my friend wants to kill himself").matched


def test_a_reply_without_a_question_mark_is_never_replaced():
    text = "Я так устала жить"

    assert not safety.check_reply(
        text, "Ты не одна с этим, и об этом стоит сказать кому-то близкому."
    ).matched


@pytest.mark.parametrize("mark", ["?", "？", "⁇", "⁈"])
def test_every_spelling_of_a_question_mark_counts(mark):
    assert safety.check_reply("Я так устала жить", f"Как ты{mark}").matched


def test_tier_two_also_covers_the_explicit_patterns():
    """Tier 2 is tier 1 loosened, not a separate vocabulary.

    Unreachable through the endpoint (tier 1 answered already), asserted so
    the function is correct for any other caller.
    """
    finding = safety.check_reply("не хочу жить", A_QUESTION)

    assert finding.matched
    assert finding.tier == safety.TIER_WEAK
    assert finding.pattern_id == "ru.no-wish-to-live"


def test_an_empty_or_wordless_message_matches_nothing():
    for text in ("", "   ", "\n\n", "!!! ... 123"):
        assert not safety.check_input(text).matched


# ---------------------------------------------------------------------------
# 6. Normalisation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "text",
    [
        "НЕ ХОЧУ ЖИТЬ",
        "Не Хочу Жить",
        "Устал.\nНе хочу\nжить",       # the conversation arrives line by line
        "я  не   хочу    жить",        # doubled spaces
        "я не хо­чу жить",        # soft hyphen inside a word
        "я не хо​чу жить",        # zero-width space
    ],
)
def test_spelling_and_layout_do_not_hide_the_signal(text):
    assert safety.check_input(text).pattern_id == "ru.no-wish-to-live"


def test_yo_is_normalised_to_ye():
    assert safety.check_input("хочу свести счёты с жизнью").matched
    assert safety.check_input("хочу свести счеты с жизнью").matched


@pytest.mark.parametrize("apostrophe", ["'", "’", "ʼ", "`"])
def test_every_apostrophe_spelling_is_understood(apostrophe):
    text = f"I don{apostrophe}t want to live"

    assert safety.check_input(text).pattern_id == "en.no-wish-to-live"


def test_normalise_flattens_without_losing_words():
    assert safety.normalise(" Я\tБольше\nне  ХОЧУ жить ") == "я больше не хочу жить"


# ---------------------------------------------------------------------------
# 7. Language
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Мне очень тяжело сейчас", "ru"),
        ("Мені дуже важко зараз", "uk"),
        ("This is hard for me", "en"),
        ("Помоги", None),          # Cyrillic, but nothing separates ru from uk
        ("123 !!! ***", None),     # no letters at all
    ],
)
def test_language_detection(text, expected):
    assert safety.detect_language(text) == expected


def test_an_undecidable_cyrillic_message_takes_the_language_of_the_pattern():
    """"хочу померти" carries none of і/ї/є/ґ — the pattern is the evidence."""
    assert safety.detect_language("хочу померти") is None
    assert safety.detect_language("хочу умереть") is None
    assert safety.check_input("хочу померти").language == "uk"
    assert safety.check_input("хочу умереть").language == "ru"


def test_an_unknown_language_falls_back_to_english():
    assert safety.safety_reply(None) == safety.SAFETY_REPLIES["en"]
    assert safety.safety_reply("de") == safety.SAFETY_REPLIES["en"]
    assert safety.safety_reply("") == safety.SAFETY_REPLIES["en"]


# ---------------------------------------------------------------------------
# 8. The fixed replies, and the fact that they are code
# ---------------------------------------------------------------------------

def test_a_reply_exists_for_every_supported_language():
    assert set(safety.SAFETY_REPLIES) == set(safety.SUPPORTED_LANGUAGES)
    assert safety.DEFAULT_LANGUAGE in safety.SAFETY_REPLIES


@pytest.mark.parametrize("language", ["ru", "uk", "en"])
def test_the_fixed_reply_is_never_a_question(language):
    reply = safety.SAFETY_REPLIES[language]

    assert reply.strip() == reply != ""
    assert not safety.has_question_mark(reply)
    # Two sentences: the rule the model is being replaced for failing.
    assert reply.count(".") == 2


def test_the_fixed_replies_are_versioned():
    version = safety.SAFETY_REPLY_VERSION

    assert isinstance(version, int) and version >= 1
    # Pins the wording itself, the way test_twinkler_ai.py pins
    # QUESTION_PROMPT. If this fails, a crisis reply changed: update the hash
    # together with a bump of SAFETY_REPLY_VERSION.
    joined = "\n".join(safety.SAFETY_REPLIES[lang] for lang in ("ru", "uk", "en"))
    assert (
        hashlib.sha256(joined.encode("utf-8")).hexdigest()
        == "f9f95e011471cc84bb872b9a8a4433d6046b5b68d2877abd72c914a68d929be9"
    )


def test_the_module_reads_no_environment_variable():
    """No deployment may configure this rule away (ADR 0008, in reverse).

    A knob here would let two deployments answer a person in crisis
    differently while both look correctly configured.
    """
    # The docstring explains why there is no variable, so it names one.
    source = inspect.getsource(safety).replace(safety.__doc__ or "", "")

    assert "environ" not in source
    assert "getenv" not in source
    assert "import config" not in source and "from config" not in source


# ---------------------------------------------------------------------------
# 9. A finding is safe to log
# ---------------------------------------------------------------------------

def test_a_finding_carries_no_text_from_the_message():
    text = "Я больше не хочу жить, всё бессмысленно, у меня рак"
    finding = safety.check_input(text)

    assert finding.matched
    rendered = repr(finding)
    for word in ("больше", "бессмысленно", "рак", "жить"):
        assert word not in rendered
    assert finding.pattern_id == "ru.no-wish-to-live"
