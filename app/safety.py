"""
Despair and self-harm detection for `POST /api/ai/question` (ClickUp 86cbegg23).

Prompt v1 ended with the one rule that is not about style: when the person
shows despair, self-harm or thoughts of suicide, the companion must drop the
one-question format and say plainly that they should not stay alone with
this (v2 dropped that sentence — the rule is this module now, and a prompt
carrying a rule it does not enforce misleads its next reader). Gemini obeyed
the instruction; the 2026-09-05 provider
measurement (ClickUp 86cbegctz) showed Qwen3-30B answering the explicit
despair input with a guiding question in **3 samples out of 3**. A rule that
matters this much cannot be an instruction one provider happens to follow —
so it lives here, in code, and behaves identically whatever model the
endpoint is pointed at.

Two tiers, deliberately separate — and, since the request became structured
(ClickUp 86cbegmzz), reading different parts of it. This module is handed the
text; the choice is `app/twinkler_ai.py`'s (`safety_input_text`,
`written_by_the_person`): **tier 1 gets the person's last reply**, tier 2 the
topic and every reply. That split is what lets a prayer continue after the
fixed reply has been given once — while the whole conversation was one string,
tier 1 kept re-firing on a phrase that had already been answered.

**Tier 1 — skip the model.** An explicit statement of not wanting to live, of
wanting to die, of ending one's own life or of harming oneself
(`EXPLICIT_PATTERNS`). The provider is never called: the endpoint answers the
fixed reply in the person's language. Nothing a model returns can change this
outcome, which is the whole point.

**Tier 2 — post-check of the model's answer.** A lower-threshold set
(`WEAK_PATTERNS`, plus every tier-1 pattern) covering wordings that are
despair-shaped but have a plausible ordinary reading — "everyone would be
fine without me", "I'm tired of living", "I can't go on like this". These are
**not** enough to skip the model: a good answer to them is a warm sentence
without a question, which is exactly what the prompt asks for and what Gemini
produces. They are enough, however, to reject a *question-shaped* answer: if
the reply carries a question mark, the model ignored the rule and the fixed
reply replaces it.

The asymmetry is on purpose. Tier 1 protects the person from the provider;
tier 2 protects them from a provider that half-obeyed. Tier 2 can only ever
fire when the reply contains `?`, and on this endpoint every ordinary reply
does — so a tier-2 pattern that matched an ordinary message would silently
replace every answer to it. That is why the weak set is curated and narrow
rather than a broad net, and why both sets are swept over the whole reference
corpus in `tests/test_safety.py`: the 24 approved scenarios of
`evaluation/scenarios.json` and the probe inputs of
`evaluation/question_probe_inputs.json` must not raise tier 1 at all.

The one deliberate tier-2 hit on the reference set is `en-005` ("I keep
thinking everyone would be fine without me"). It is passive ideation stated
about other people, not an intent stated about oneself: worth refusing a
question over, not worth refusing to let the companion answer.

Dictionary and regular expressions only — no model, no network call, no
external service. The detector is the cheapest and least failable part of the
request path, and it must stay that way: it is what still works when the
provider does not.

**Nothing here is configurable.** The reply texts are a reviewed code
constant versioned by `SAFETY_REPLY_VERSION`, the way the question prompt is
versioned by `question_prompt.QUESTION_PROMPT_VERSION`.
An environment variable would let two deployments answer a person in crisis
differently with identical, valid-looking configuration — the class of
failure ADR 0008 exists to remove.

**What may be logged: the fact, never the text.** A finding carries the
matched pattern id, its tier and the resolved language, and nothing derived
from the message. Prayer text never reaches the log, here or anywhere else in
this service.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from prompt_safety import neutralize_prompt_markers

# Bump on ANY change to the wording of SAFETY_REPLIES. v1 is the first
# reviewed draft (ClickUp 86cbegg23, 2026-09-05).
SAFETY_REPLY_VERSION = 2  # v2: reviewer's wording of the second sentence, approved by Maria 2026-09-05

DEFAULT_LANGUAGE = "en"
SUPPORTED_LANGUAGES = ("ru", "uk", "en")

# Two sentences each, no question mark anywhere: the fixed reply is what the
# prompt asks the model for and the model may fail to produce. Deliberately
# free of hotline numbers — the app is worldwide, so "emergency help" is named
# generically and the person is pointed at someone close to them first.
# Informal register (ru/uk "ты"/"ти"), matching the companion's usual voice.
SAFETY_REPLIES: dict[str, str] = {
    "ru": (
        "Мне очень жаль, что тебе сейчас так больно, и с этим не нужно "
        "оставаться один на один. Пожалуйста, прямо сейчас поговори с "
        "кем-то близким о том, что ты чувствуешь, или обратись за "
        "экстренной помощью."
    ),
    "uk": (
        "Мені дуже шкода, що тобі зараз так боляче, і з цим не треба "
        "залишатися сам на сам. Будь ласка, просто зараз поговори з кимось "
        "близьким про те, що відчуваєш, або звернися по екстрену допомогу."
    ),
    "en": (
        "I am so sorry it hurts this much right now, and this is not "
        "something you have to carry on your own. Please tell someone close "
        "to you how you are feeling, or reach out for emergency help right "
        "away."
    ),
}

TIER_NONE = 0
TIER_EXPLICIT = 1
TIER_WEAK = 2

# `?` in every script a client keyboard is likely to produce.
QUESTION_MARKS = "?？⁇⁈"


@dataclass(frozen=True)
class SafetyFinding:
    """What the detector saw — never what the person wrote.

    `pattern_id` names the rule, not the words that matched it, so the whole
    object is safe to log.
    """

    matched: bool
    tier: int = TIER_NONE
    pattern_id: str | None = None
    language: str = DEFAULT_LANGUAGE


NO_MATCH = SafetyFinding(matched=False)


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------
# The text arrives typed on a phone keyboard, and it may span several lines: a
# single answer can be a typed line plus two transcriptions joined with
# newlines, and tier 2 is handed the topic and every reply at once. So a
# pattern must survive line breaks, casing, the two spellings of `ё`, curly
# apostrophes and stray invisible characters — none of which change what the
# person said.

_WHITESPACE_RE = re.compile(r"\s+")
_APOSTROPHES = str.maketrans({"’": "'", "‘": "'", "ʼ": "'", "`": "'", "´": "'"})


def normalise(text: str) -> str:
    """Casefold, unify apostrophes and `ё`, and flatten whitespace.

    `prompt_safety.neutralize_prompt_markers` is reused for the invisible
    characters (Unicode `Cf` and the C0 controls): a soft hyphen inside a word
    is produced by ordinary keyboards and copy-paste, and it must not be able
    to hide "не хо­чу жить" from this module.
    """
    cleaned = neutralize_prompt_markers(text).translate(_APOSTROPHES)
    return _WHITESPACE_RE.sub(" ", cleaned.casefold().replace("ё", "е")).strip()


# ---------------------------------------------------------------------------
# Language
# ---------------------------------------------------------------------------
# Alphabet first, distinguishing letters second, a small function-word list as
# the tie-break — the same shape as `evaluation/check_questions.py`, which is
# a benchmark script and cannot be imported from the application.

_CYRILLIC_RE = re.compile(r"[а-яёіїєґ]")
_LATIN_RE = re.compile(r"[a-z]")
_UK_LETTERS_RE = re.compile(r"[іїєґ]")
_RU_LETTERS_RE = re.compile(r"[ыэъ]")  # `ё` is normalised away before this
_WORD_RE = re.compile(r"[^\W\d_]+", re.UNICODE)
# Every entry must exist in one of the two languages and NOT in the other, or
# it votes for the wrong one: "хочу" is spelled identically in both and would
# have made every Russian sentence containing it Ukrainian. Words present in
# both lists ("про", "тебе") are harmless — they cancel out.
_UK_WORDS = frozenset({
    "що", "чи", "зараз", "тобі", "твоє", "твої", "хочеш", "серці",
    "найбільше", "хочеться", "розкажи", "коли", "чому", "як", "про", "більше",
    "тебе", "від", "цьому", "жити", "життя", "мене", "буде", "немає", "нема",
})
_RU_WORDS = frozenset({
    "что", "тебе", "сейчас", "твоё", "твое", "твои", "хочешь", "сердце",
    "больше", "расскажи", "когда", "почему", "как", "про", "этом", "этот",
    "хочется", "жить", "жизни", "жизнь", "меня", "будет", "нет", "смысла",
})


def detect_language(text: str) -> str | None:
    """`ru`, `uk`, `en` — or `None` when the text does not say.

    `None` is not a failure: it is the honest answer for a Cyrillic message
    that carries none of the four letters that separate Russian from
    Ukrainian ("не хочу жити"). The caller resolves it from the language the
    matched pattern belongs to, which is exactly the missing evidence.
    """
    normalised = normalise(text)
    cyrillic = len(_CYRILLIC_RE.findall(normalised))
    latin = len(_LATIN_RE.findall(normalised))
    if cyrillic == 0 and latin == 0:
        return None
    if cyrillic <= latin:
        # Latin script: `en` is the only Latin-script language this endpoint
        # answers in, so it is the answer rather than a guess at another one.
        return "en"
    if _UK_LETTERS_RE.search(normalised):
        return "uk"
    if _RU_LETTERS_RE.search(normalised):
        return "ru"
    words = {word for word in _WORD_RE.findall(normalised)}
    uk_hits = len(words & _UK_WORDS)
    ru_hits = len(words & _RU_WORDS)
    if uk_hits > ru_hits:
        return "uk"
    if ru_hits > uk_hits:
        return "ru"
    return None


# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------
# Every pattern is a phrase, never a single loaded word: "умираю от смеха",
# "убить время", "до смерти устал", "I'm dying to see her", "kill time" and
# "dead tired" all contain the vocabulary of death and none of the meaning.
# The guards below are what separates them.
#
# 1. A **tail guard** after "жить" / "жити" / "live": a preposition or an
#    adverb of place turns the sentence into a complaint about circumstances
#    ("не хочу жить в этом городе", "no reason to live in fear"), not about
#    living. `без` / `without` are NOT in the guard — "не хочу жить без неё"
#    is grief at its most dangerous, not a housing preference — with one
#    named exception, because this is a prayer app: "не хочу жить без Бога",
#    "I don't want to live without God" is a confession of faith.
# 2. A **negation guard** before the verbs of wanting: "не хочу умереть" and
#    "I don't want to die" are fear of death, the opposite of the signal.
#    Written as a fixed-width lookbehind, which whitespace normalisation makes
#    safe (every separator is one space by then).
# 3. A **tail guard** after the verbs of dying (`_RU_DIE_TAIL` and friends):
#    dying to sin is doctrine, not intent, and "умереть от стыда" is the idiom
#    of shame.
# 4. First person where the language allows it: "she wants to die" is somebody
#    else's illness, "I want to die" is this person. Where the phrase carries
#    no person at all ("покончить с собой", "суицид"), a prayer *for* someone
#    at risk matches too — accepted deliberately, see architect/twinkler-ai.md.

_RU_LIVE_TAIL = (
    r"(?!\s+(?:в|во|с|со|у|на|при|под|за|над|между|около|возле|рядом|тут|"
    r"здесь|там|дома|вместе|один|одна|одному|одной|одним|так|такой|по|для|"
    r"ради)\b)"
    # `без` is not a guard on purpose (see above) — with one exception, which
    # is what this app is for: "не хочу жить без Бога" is a confession of
    # faith, not a statement about staying alive.
    r"(?!\s+без (?:бога|христа|господа|иисуса|веры|молитвы|церкви)\b)"
)
_UK_LIVE_TAIL = (
    r"(?!\s+(?:в|у|з|зі|із|на|при|під|за|над|між|біля|коло|поруч|тут|там|"
    r"вдома|разом|сам|сама|самому|самій|так|такому|по|для|заради)\b)"
    r"(?!\s+без (?:бога|христа|господа|ісуса|віри|молитви|церкви)\b)"
)
_EN_LIVE_TAIL = (
    r"(?!\s+(?:in|with|at|near|like|under|on|by|through|among|alone|here|"
    r"there|together|for|as)\b)"
    r"(?!\s+without (?:god|christ|jesus|the lord|faith|prayer)\b)"
)
# "не хочу умереть" / "не хочеться померти" — fear, not intent. The negation
# is spelled the same in both languages, so one guard serves both.
_NOT_NEGATED = r"(?<!не )"
# What follows the verb of dying and takes the intent out of it.
# 1. "умереть для греха", "умереть со Христом" (Rom 6) — the doctrine of
#    dying to sin is ordinary speech in a prayer app, and its English form
#    ("die to sin", "die to self", "be dead to sin") is even more common.
# 2. "умереть от стыда / смеха / скуки" — the idiom of embarrassment. A
#    closed list, because "умереть от боли" is not one of these.
_RU_DIE_TAIL = (
    r"(?!\s+(?:для|со)\b)"
    r"(?!\s+от (?:стыда|смеха|скуки|голода|усталости|счастья|восторга)\b)"
)
_UK_DIE_TAIL = (
    r"(?!\s+(?:для|з[іа])\b)"
    r"(?!\s+від (?:сорому|сміху|нудьги|голоду|втоми|щастя)\b)"
)
_EN_DIE_TAIL = r"(?!\s+to\b)"
# "нет смысла в жизни без Бога" / "життя не має сенсу без Бога" are sermons,
# not despair. Same exception as `_RU_LIVE_TAIL`, for the patterns that talk
# about life as a noun rather than as a verb.
_RU_MEANING_TAIL = r"(?!\s+без (?:бога|христа|господа|иисуса|веры)\b)"
_UK_MEANING_TAIL = r"(?!\s+без (?:бога|христа|господа|ісуса|віри)\b)"

# Tier 1: an explicit statement. The model is not called.
EXPLICIT_PATTERNS: tuple[tuple[str, str, str], ...] = (
    # -- Russian ------------------------------------------------------------
    (
        "ru.no-wish-to-live",
        "ru",
        # "не ?хоч": "нехочу" is a frequent phone-keyboard spelling and is not
        # a word in any other reading.
        r"не ?хоч(?:у|ется)(?: (?:больше|уже|дальше|вообще|совсем)){0,2} жить\b"
        + _RU_LIVE_TAIL
        + r"|жить(?: (?:больше|уже))? не ?хоч(?:у|ется)\b",
    ),
    (
        "ru.want-to-die",
        "ru",
        _NOT_NEGATED
        + r"хоч(?:у|ется)(?: (?:просто|уже|поскорее|скорее|наконец))* "
        r"(?:умереть|сдохнуть|помереть|не жить)\b"
        + _RU_DIE_TAIL,
    ),
    (
        "ru.end-own-life",
        "ru",
        r"поконч\w* (?:с собой|жизнь самоубийством)\b"
        r"|налож\w+ на себя руки\b"
        r"|свести счеты с жизнью\b"
        r"|лиш\w+ себя жизни\b"
        r"|уби(?:ть|ю) себя\b"
        # A verb of intent is required: "боюсь, что мама уйдёт из жизни" and
        # "не хочу, чтобы он ушёл из жизни" are about somebody else's death.
        r"|(?:хочу|решил\w*|собираюсь|думаю) уйти из жизни\b",
    ),
    ("ru.suicide-word", "ru", r"самоубийств\w*|суицид\w*"),
    (
        "ru.no-point-living",
        "ru",
        r"нет смысла жить\b" + _RU_LIVE_TAIL
        + r"|не вижу смысла жить\b" + _RU_LIVE_TAIL
        + r"|смысла жить нет\b"
        + r"|нет смысла (?:в )?жизни\b" + _RU_MEANING_TAIL
        + r"|(?:не)?зачем (?:(?:мне|дальше|вообще|теперь|больше) )*жить\b"
        + _RU_LIVE_TAIL
        + r"|жить незачем\b",
    ),
    (
        "ru.better-dead",
        "ru",
        r"лучше бы (?:я )?(?:умер|умерла|сдох|сдохла)\b"
        r"|лучше бы я не (?:родил(?:ся|ась)|жил|жила)\b"
        r"|лучше бы меня не было\b",
    ),
    (
        "ru.self-harm",
        "ru",
        r"причин\w+ себе вред\w*\b|наврежу себе\b|ре(?:жу|зать) себя\b",
    ),
    # -- Ukrainian ----------------------------------------------------------
    (
        "uk.no-wish-to-live",
        "uk",
        r"не ?хоч(?:у|еться)(?: (?:більше|вже|далі|взагалі)){0,2} жити\b"
        + _UK_LIVE_TAIL
        + r"|жити(?: (?:більше|вже))? не ?хоч(?:у|еться)\b",
    ),
    (
        "uk.want-to-die",
        "uk",
        _NOT_NEGATED
        + r"хоч(?:у|еться)(?: (?:просто|вже|швидше|нарешті))* "
        r"(?:померти|вмерти|здохнути|не жити)\b"
        + _UK_DIE_TAIL,
    ),
    (
        "uk.end-own-life",
        "uk",
        r"покінч\w* (?:з|із) собою\b"
        r"|накласти на себе руки\b"
        r"|вкоротити (?:собі )?віку\b"
        r"|позбавити себе життя\b"
        r"|вбити себе\b"
        # Same rule as the Russian "уйти из жизни": a verb of intent, so that
        # somebody else's death does not match.
        r"|(?:хочу|вирішив\w*|збираюся) піти з життя\b",
    ),
    ("uk.suicide-word", "uk", r"самогубств\w*|суїцид\w*"),
    (
        "uk.no-point-living",
        "uk",
        r"нема(?:є)? сенсу жити\b" + _UK_LIVE_TAIL
        + r"|не бачу сенсу жити\b" + _UK_LIVE_TAIL
        + r"|нема(?:є)? сенсу (?:в )?житті\b" + _UK_MEANING_TAIL
        + r"|життя не ма(?:є|ло) сенсу\b" + _UK_MEANING_TAIL
        + r"|(?:на)?віщо мені жити\b"
        r"|нащо мені жити\b",
    ),
    (
        "uk.better-dead",
        "uk",
        r"краще б (?:я )?(?:помер|померла)\b"
        r"|краще б я не народ(?:ився|илася)\b"
        r"|краще б мене не було\b",
    ),
    (
        "uk.self-harm",
        "uk",
        r"завдати собі шкоди\b|заподіяти собі шкоду\b|рі(?:жу|зати) себе\b",
    ),
    # -- English ------------------------------------------------------------
    (
        "en.no-wish-to-live",
        "en",
        r"(?:do ?n'?t|do not|no longer) (?:want|wanna) (?:to )?"
        r"(?:live|be alive)\b" + _EN_LIVE_TAIL,
    ),
    (
        "en.want-to-die",
        "en",
        # `_EN_DIE_TAIL`: "die to sin", "die to self", "be dead to sin" are the
        # ordinary English of a prayer app (Rom 6), not a statement of intent.
        r"(?<!not )(?<!n't )want to die\b" + _EN_DIE_TAIL
        + r"|\bwanna die\b" + _EN_DIE_TAIL
        + r"|\bwish i (?:was|were) dead\b"
        r"|\bwish i (?:had )?never (?:been born|existed)\b"
        r"|\bi want to be dead\b" + _EN_DIE_TAIL,
    ),
    (
        "en.end-own-life",
        "en",
        r"\bend my life\b|\bend it all\b|\btake my own life\b"
        r"|\bkill myself\b|\bhang myself\b"
        # A time, not an object: "end it with him tonight" (a relationship)
        # keeps its words apart, "end it tonight" does not.
        r"|\bend it (?:tonight|today|tomorrow)\b",
    ),
    ("en.suicide-word", "en", r"\bsuicid(?:e|al)\b"),
    (
        "en.no-point-living",
        "en",
        r"\bno reason to live\b" + _EN_LIVE_TAIL
        + r"|\bnothing to live for\b"
        r"|\bno point (?:in )?(?:living|being alive)\b" + _EN_LIVE_TAIL
        + r"|\bno reason to (?:keep|go on) living\b"
        # "no reason to keep going" — the same sentence with the verb dropped.
        # Guarded so that abandoning a *thing* ("no reason to go on with the
        # course") is not read as abandoning a life.
        r"|\bno reason to (?:keep going|go on)\b(?!\s+(?:with|to|in|on|for|about)\b)"
        r"|\bwhat'?s the point (?:of|in) (?:living|being alive|life)\b"
        + _EN_LIVE_TAIL
        + r"|\bnot worth living\b|n'?t worth living\b",
    ),
    (
        "en.better-dead",
        "en",
        r"\bbetter off dead\b"
        r"|\bi should (?:just )?(?:be dead|die)\b"
        r"|\bwish i (?:was|were) never born\b",
    ),
    (
        "en.self-harm",
        "en",
        r"\bself[- ]?harm(?:ing)?\b|\bharm(?:ing)? myself\b",
    ),
)

# Tier 2: despair-shaped, but each one has an ordinary reading. Alone they are
# not enough to refuse the model an attempt — they are enough to refuse it a
# question mark.
WEAK_PATTERNS: tuple[tuple[str, str, str], ...] = (
    # -- Russian ------------------------------------------------------------
    (
        "ru.live-like-this",
        "ru",
        # "не хочу жить так, как раньше" is repentance, not despair: the
        # comparison names the life being left behind.
        r"не хоч(?:у|ется)(?: больше)? так жить\b(?!,? как\b)"
        r"|не хоч(?:у|ется) жить так\b(?!,? как\b)",
    ),
    (
        "ru.cannot-live-on",
        "ru",
        # "больше" and "так" are independent: "не могу больше так жить" is the
        # commonest spelling of all and carries both.
        r"(?:больше )?не могу(?: больше)?(?: так)? жить\b"
        + r"(?!\s+(?:без|в|во|с|со|у|на|тут|здесь|там|дома|вместе|рядом|по|"
        r"для|ради)\b)",
    ),
    (
        "ru.tired-of-living",
        "ru",
        r"устал[аи]?(?: уже)? жить\b|устал[аи]? от жизни\b|надоело жить\b",
    ),
    (
        "ru.want-to-vanish",
        "ru",
        r"хоч(?:у|ется)(?: просто| навсегда)* исчезнуть\b",
    ),
    (
        "ru.better-without-me",
        "ru",
        r"(?:будет|было бы|станет) (?:намного |гораздо )?"
        r"(?:лучше|легче|спокойнее) без меня\b"
        r"|без меня (?:всем |им )?(?:будет|было бы|станет) "
        r"(?:лучше|легче|спокойнее)\b",
    ),
    (
        "ru.not-wake-up",
        "ru",
        r"не хоч(?:у|ется)(?: больше)? просыпаться\b"
        r"|(?:уснуть|заснуть) и не проснуться\b",
    ),
    ("ru.life-is-over", "ru", r"жизнь (?:моя )?(?:кончена|закончена|окончена)\b"),
    # "не вижу смысла жить" is tier 1; with the verb dropped the sentence can
    # still be about a situation, so it only refuses a question.
    # An infinitive after it names what is pointless ("не вижу смысла дальше
    # спорить"); with nothing after it, what is pointless is going on at all.
    (
        "ru.no-point-ahead",
        "ru",
        r"не вижу смысла (?:жить )?дальше\b(?!\s+\w+(?:ть|ться)\b)",
    ),
    # Asking God to take you *to himself* is a wish to die; "забери меня
    # отсюда" is not, which is why the destination is part of the pattern.
    ("ru.take-me-away", "ru", r"забери меня к себе\b"),
    # -- Ukrainian ----------------------------------------------------------
    (
        "uk.live-like-this",
        "uk",
        r"не хоч(?:у|еться)(?: більше)? так жити\b(?!,? як\b)"
        r"|не хоч(?:у|еться) жити так\b(?!,? як\b)",
    ),
    (
        "uk.cannot-live-on",
        "uk",
        r"(?:більше )?не можу(?: більше)?(?: так)? жити\b"
        + r"(?!\s+(?:без|в|у|з|зі|із|на|тут|там|вдома|разом|поруч|по|для|"
        r"заради)\b)",
    ),
    (
        "uk.tired-of-living",
        "uk",
        r"втоми(?:вся|лася|лись|лися)(?: вже)? жити\b"
        r"|втоми(?:вся|лася|лись|лися) від життя\b"
        r"|набридло жити\b",
    ),
    ("uk.want-to-vanish", "uk", r"хоч(?:у|еться)(?: просто| назавжди)* зникнути\b"),
    (
        "uk.better-without-me",
        "uk",
        r"(?:буде|було б|стане) (?:набагато |значно )?(?:краще|легше) без мене\b"
        r"|без мене (?:всім |їм )?(?:буде|було б|стане) (?:краще|легше)\b",
    ),
    (
        "uk.not-wake-up",
        "uk",
        r"не хоч(?:у|еться)(?: більше)? прокидатися\b"
        r"|заснути і не прокинутися\b",
    ),
    ("uk.life-is-over", "uk", r"життя (?:моє )?(?:скінчилося|закінчилося)\b"),
    ("uk.no-point-ahead", "uk", r"не бачу сенсу (?:жити )?далі\b(?!\s+\w+ти\b)"),
    ("uk.take-me-away", "uk", r"забери мене до себе\b"),
    # -- English ------------------------------------------------------------
    (
        "en.live-like-this",
        "en",
        r"(?:do ?n'?t|do not) want to (?:live|go on) like this\b"
        r"|\bca ?n'?t live like this\b",
    ),
    (
        "en.cannot-go-on",
        "en",
        # The object is optional — bare "I can't go on" is the commonest form
        # — but a *named* object turns the sentence into a complaint about
        # that thing ("I can't go on with this job", "can't keep going to
        # church"), so anything introduced by a preposition disarms it.
        r"\bca ?n'?t (?:go on|keep going|carry on)"
        r"(?:\s+(?:living|any ?more|any longer|like this))?\b"
        r"(?!\s+(?:with|to|in|on|at|about|for|a|an|the)\b)",
    ),
    (
        "en.tired-of-living",
        "en",
        r"\btired of (?:living|being alive|life)\b"
        r"|\bsick of (?:living|being alive)\b",
    ),
    (
        "en.want-to-disappear",
        "en",
        r"\bwant to (?:just )?(?:disappear|vanish)\b"
        r"|\bwish i could (?:just )?disappear\b"
        # "I wish I wasn't here" — passive, and about a place as often as
        # about a life, so it refuses a question rather than the model.
        r"|\bwish i (?:wasn'?t|weren'?t|was not|were not) here\b",
    ),
    (
        "en.better-without-me",
        "en",
        r"\b(?:be|are|is|were|was) (?:so |much |way )?"
        r"(?:better|fine|happier|ok|okay|alright|easier) (?:off )?without me\b"
        r"|\bbetter off without me\b",
    ),
    (
        "en.no-one-would-miss-me",
        "en",
        r"\b(?:no one|nobody|noone) would (?:miss me|notice if i)\b",
    ),
    ("en.hurt-myself", "en", r"\bhurt(?:ing)? myself\b|\bcut(?:ting)? myself\b"),
    ("en.burden", "en", r"\bi'?m a burden\b|\ba burden to everyone\b"),
)


def _compile(
    patterns: tuple[tuple[str, str, str], ...],
) -> tuple[tuple[str, str, re.Pattern[str]], ...]:
    return tuple(
        (pattern_id, language, re.compile(expression))
        for pattern_id, language, expression in patterns
    )


_EXPLICIT = _compile(EXPLICIT_PATTERNS)
_WEAK = _compile(WEAK_PATTERNS)
# Tier 2 is a superset of tier 1, not a different vocabulary: an explicit
# statement the endpoint somehow let through must not earn a question either.
# In the endpoint the explicit half is unreachable (tier 1 answered already);
# it is here so `check_reply` is correct on its own terms.
_REPLY_SCAN = _EXPLICIT + _WEAK


def _scan(
    text: str,
    compiled: tuple[tuple[str, str, re.Pattern[str]], ...],
    tier: int,
) -> SafetyFinding:
    normalised = normalise(text)
    if not normalised:
        return NO_MATCH
    for pattern_id, pattern_language, expression in compiled:
        if expression.search(normalised):
            return SafetyFinding(
                matched=True,
                tier=tier,
                pattern_id=pattern_id,
                # The message decides the language; the pattern only answers
                # when the message cannot ("не хочу жити" carries none of the
                # four Ukrainian letters).
                language=detect_language(text) or pattern_language,
            )
    return NO_MATCH


def check_input(text: str) -> SafetyFinding:
    """Tier 1: does this message forbid calling the model at all."""
    return _scan(text, _EXPLICIT, TIER_EXPLICIT)


def has_question_mark(text: str) -> bool:
    return any(mark in text for mark in QUESTION_MARKS)


def check_reply(user_text: str, reply_text: str) -> SafetyFinding:
    """Tier 2: may this reply stand for this message.

    Fires only on the conjunction — a despair signal in the message *and* a
    question mark in the reply. A reply without one is the model doing what
    the prompt asks, and it is left exactly as it came back: the fixed text is
    a floor under the answer, not a replacement for a good one.
    """
    if not has_question_mark(reply_text):
        return NO_MATCH
    return _scan(user_text, _REPLY_SCAN, TIER_WEAK)


def safety_reply(language: str | None) -> str:
    """The fixed reply in `language`, falling back to English."""
    return SAFETY_REPLIES.get(language or "", SAFETY_REPLIES[DEFAULT_LANGUAGE])
