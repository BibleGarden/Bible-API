#!/usr/bin/env python3
"""
Candidate wordings of the leading-question prompt, for the v4 measurement
(ClickUp 86cbehyf8, child of 86cbehxm2, bug 86cbehtkh).

The same shape as `rewrite_prompts.py`: the production text is imported, the
candidates are built by explicit surgery on it, and the losing candidates stay
here afterwards so the published table can be rebuilt. Nothing in this file is
read by the application.

**What the measurement is about.** The baseline of 86cbehyez showed that
pressing «заменить вопрос» six times on the journal case `series-scale-ru`
returns the same thought six times: one opening in 6/6 samples, 11 verbatim
duplicate pairs out of 36 answers, mean max-similarity 0.98. The reading is
that v3's `next` instruction — «задай один новый вопрос, который **смотрит на
ситуацию с другой стороны**» — pushes the model into *contesting* what the
person just wrote («а что, если завтра окажется, что готовое — не то, что
нужно»), and with an identical request every time it re-derives the same
contestation. The second defect of the same input is grammatical gender: the
person writes «я рада» and is asked «что ты считал», and in
`series-exhaustion-uk` a woman is addressed as «зробив» in 30 answers of 30.

Three candidates, one added lever each, so a table row means something:

* **a** — the `next` instruction only. «Разверни то, что человек написал» +
  «не повторяй мысль уже прозвучавшего вопроса другими словами» + «не спорь и
  не ставь сказанное под сомнение, если он сам не усомнился». The system
  prompt is v3 byte for byte.
* **b** — a + one sentence of the system prompt about grammatical gender and
  number: take it from the person's own words, and never fall back to the
  masculine.
* **c** — b + the `skipped_questions` block of ADR 0015 reworded («Эти вопросы
  человеку не подошли, он их пропустил:») and a sharper "another direction"
  sentence. Both are inert unless the request actually carries skipped
  questions, so **in the identical-body mode c renders exactly b's bytes** —
  it is measured in the accumulating mode, where the field is sent.
* **c2** — added after c was measured and lost: c's header with v3's sentence.
  c asked for «другой момент, другого человека, другое дело», and the model
  took «другого человека» literally — it started asking the person about an
  invented third party («а что, если кто-то из тех, кто будет использовать
  приложение…»). c2 exists to say which of c's two edits did that, since the
  header rewording is the one the review of 86cbehyfe actually asked for.

What every candidate keeps byte for byte (Maria's constraints, 2026-09-05):
the language rule named twice with the last sentence, the interpretation ban,
the open-question rule, the 160-character rule, the "no advice / never speak as
God" sentences and the informal register — and **nothing** was added to make
the companion warmer, more supportive or more encouraging. The despair rule
stays out of the prompt (`app/safety.py`). The stage blocks stay Russian in
every language, for the reason `app/question_prompt.py` gives.

**Since prompt v6 (ClickUp 86cbejvt2)** the file also carries the name `v6`,
which is not a frozen copy but a label for the live wording: `system_prompt`
and `user_message` delegate to `app/question_prompt.py` for it exactly as they
do for `production`, `tests/test_question_prompt.py` pins the two byte for
byte, and `candidate()` refuses the name the moment the module stops being v6
(freeze it then, the way v4 is frozen here). `structured_answer(variant)` says
whether a run must read the answer as v6's JSON object
(`app/question_format.py`) or as the bare line every earlier artifact holds.

The v3 texts below are **frozen copies**, not imports: once the winner is
promoted the production module renders v4, and a baseline that moved with it
would stop being a baseline. The copies are pinned by the sha256 of the v3
system prompt (the same digest `tests/test_twinkler_ai.py` carried while v3 was
production), so a typo in the transcription is an import-time failure rather
than a silently different measurement.
"""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "app"))

# The production assembly is imported, never re-typed: a candidate must differ
# from production in exactly the strings it names below and in nothing else.
import question_prompt  # noqa: E402

# ---------------------------------------------------------------------------
# v3, frozen
# ---------------------------------------------------------------------------

V3_SYSTEM_TEMPLATE = (
    'You are Twinkler, a quiet companion inside a personal Christian '
    'prayer app. Your only job is to ask one question at a time that '
    'helps the person pray in their own words - honestly and deeply, '
    'never in cliches. Language rule, and it overrides everything '
    'else: ask your question in {language}, and in no other language. '
    'Never answer in a language the person did not use, and never '
    'switch languages mid-conversation unless they switch first. '
    'Where the language distinguishes registers, use the informal, '
    'intimate one (Russian ty, Ukrainian ty). Tone: warm and quiet. '
    'No pathos, no moralising, no praise, no advice, and no '
    'interpreting back at the person what they just said. Do not name '
    'a feeling they have not named themselves, and do not offer them '
    "one to confirm: never 'you feel that ...', 'it sounds like you "
    "...', 'it seems you ...', never 'are you afraid that ...', and "
    'never the same constructions in Russian or Ukrainian. Anchor the '
    'question in something concrete they wrote - a person, a moment, '
    'something they did or have to do - and ask what is alive for '
    'them in it. Never ask about how much they are suffering or '
    'whether they can still bear it, and never ask for a fact that '
    'only fills in your own picture: a name, a date, an address, a '
    'schedule. Ask an open question they answer in their own words, '
    'never one that can be answered with yes or no, and nothing whose '
    'answer is already inside it: no rhetorical or devotional '
    "formulas such as 'Is God near?' or 'Do you feel that God is with "
    "you?'. Every reply is exactly one question: one simple thought, "
    'one line, ending in a question mark, usually no longer than 160 '
    'characters. Use living, spoken language - no bureaucratic or '
    'churchy phrasing, no long subordinate clauses. Your grammar must '
    'be flawless in whatever language you write. In inflected '
    'languages such as Russian and Ukrainian, watch case endings and '
    'preposition agreement especially closely when you compress a '
    'sentence to fit the line. Never speak as God or claim to deliver '
    "a verdict on God behalf, never suggest that someone's pain is a "
    'punishment, and give no medical, legal or financial advice. '
    'Answer in {language}.'
)
# The digest of the text that ran in production as v3 (and as v2 before the
# stage blocks moved server-side — v3 changed no byte of the system prompt).
V3_SYSTEM_SHA256 = (
    "7b860999e1ce7df3a349a585b9791e5e5f587c473596012dd38016ed365f6a45"
)
assert hashlib.sha256(V3_SYSTEM_TEMPLATE.encode("utf-8")).hexdigest() == (
    V3_SYSTEM_SHA256
), "the frozen v3 system prompt is not the text v3 shipped"

V3_NEXT_OPENING = (
    "Задай один новый вопрос, который смотрит на ситуацию с другой стороны и "
    "не повторяет прозвучавшие."
)
V3_SKIPPED_HEADER = "Человек попросил другой вопрос вместо этих:\n"
V3_SKIPPED_SENTENCE = (
    " Выбери другое направление, а не переформулировку тех вопросов, и "
    "оттолкнись от того, что человек написал сам."
)

# ---------------------------------------------------------------------------
# The candidate levers
# ---------------------------------------------------------------------------

# (a) The `next` instruction. Three sentences replacing one: develop what the
# person wrote (instead of "look at the situation from another side", which the
# model read as "contest it"), do not restate an earlier question's thought,
# and do not doubt what they said unless they doubted it themselves.
A_NEXT_OPENING = (
    "Задай один новый вопрос: разверни то, что человек написал в последнем "
    "ответе. Не повторяй мысль уже прозвучавшего вопроса другими словами. Не "
    "спорь с тем, что он сказал, и не ставь это под сомнение, если он сам не "
    "усомнился."
)

# (b) One sentence of the system prompt, inserted after the existing sentence
# about inflected languages — the place the prompt already talks about Russian
# and Ukrainian grammar. The examples are Cyrillic on purpose: v2's history
# (see `app/question_prompt.py`) is that a rule stated abstractly was read as
# something else, and a named form is what made the previous rules hold.
B_ANCHOR = (
    "sentence to fit the line."
)
B_GENDER_SENTENCE = (
    " Take the person's grammatical gender and number from their own words - "
    "'рада', 'сделала', 'втомилася' are a woman speaking about herself, 'рад', "
    "'сделал', 'втомився' a man - and address them in that same form; when "
    "their words do not say, word the question so that it needs no gender, and "
    "never fall back to the masculine."
)

# (c) The block of ADR 0015. The header states the fact and stops: they skipped
# these questions. «попросил другой вопрос» asserted a request, which the
# client cannot promise (a skip is a tap, not an argument), and ADR 0015
# forbids attributing a position to the person either way — that constraint
# survives this rewording and is pinned by `tests/test_question_prompt.py`.
C_SKIPPED_HEADER = "Эти вопросы человеку не подошли, он их пропустил:\n"
C_SKIPPED_SENTENCE = (
    " Возьми в новом вопросе то, чего в пропущенных не было: другой момент, "
    "другого человека, другое дело из того, что он написал сам."
)


@dataclass(frozen=True)
class Candidate:
    """One measurable wording: a system template plus three `next` strings."""

    name: str
    system_template: str
    next_opening: str
    skipped_header: str
    skipped_sentence: str
    note: str


def _with_gender_sentence(template: str) -> str:
    """v3's system prompt plus the gender sentence, or a loud failure."""
    if template.count(B_ANCHOR) != 1:
        raise SystemExit(
            "question_prompts: the anchor sentence of the gender rule is not "
            "in the system prompt exactly once — the prompt moved, so the "
            "candidate must be re-derived rather than silently misplaced"
        )
    return template.replace(B_ANCHOR, B_ANCHOR + B_GENDER_SENTENCE)


CANDIDATES: dict[str, Candidate] = {
    "v3": Candidate(
        name="v3",
        system_template=V3_SYSTEM_TEMPLATE,
        next_opening=V3_NEXT_OPENING,
        skipped_header=V3_SKIPPED_HEADER,
        skipped_sentence=V3_SKIPPED_SENTENCE,
        note="production on 2026-09-06, the baseline of 86cbehyez",
    ),
    "a": Candidate(
        name="a",
        system_template=V3_SYSTEM_TEMPLATE,
        next_opening=A_NEXT_OPENING,
        skipped_header=V3_SKIPPED_HEADER,
        skipped_sentence=V3_SKIPPED_SENTENCE,
        note="v3 + the rewritten `next` instruction",
    ),
    "b": Candidate(
        name="b",
        system_template=_with_gender_sentence(V3_SYSTEM_TEMPLATE),
        next_opening=A_NEXT_OPENING,
        skipped_header=V3_SKIPPED_HEADER,
        skipped_sentence=V3_SKIPPED_SENTENCE,
        note="a + the gender/number sentence in the system prompt",
    ),
    "c": Candidate(
        name="c",
        system_template=_with_gender_sentence(V3_SYSTEM_TEMPLATE),
        next_opening=A_NEXT_OPENING,
        skipped_header=C_SKIPPED_HEADER,
        skipped_sentence=C_SKIPPED_SENTENCE,
        note=(
            "b + the reworded skipped-questions block (visible only when the "
            "request carries skipped questions)"
        ),
    ),
    "c2": Candidate(
        name="c2",
        system_template=_with_gender_sentence(V3_SYSTEM_TEMPLATE),
        next_opening=A_NEXT_OPENING,
        skipped_header=C_SKIPPED_HEADER,
        skipped_sentence=V3_SKIPPED_SENTENCE,
        note="c's header with v3's sentence — which half of c did the damage",
    ),
}
V4_SYSTEM_TEMPLATE = _with_gender_sentence(V3_SYSTEM_TEMPLATE)
V4_NEXT_OPENING = A_NEXT_OPENING
V4_FIRST_TOPIC = "Человек начинает молитву. Его цель: «{topic}».\n"
V4_FIRST_NO_TOPIC = "Человек начинает молитву без конкретной темы.\n"
V4_FIRST_INSTRUCTION = (
    "Задай первый наводящий вопрос — про то, что сейчас происходит и что он "
    "чувствует. Не пересказывай цель дословно. Ответь только текстом вопроса, "
    "без кавычек и пояснений."
)
V4_NEXT_TOPIC = "Цель молитвы: «{topic}».\n"
V4_NEXT_NO_TOPIC = "Молитва без конкретной темы.\n"
V4_ASKED = "Уже прозвучали вопросы:\n"
V4_ANSWERED = "Что человек ответил (опирайся на это, но не цитируй дословно):\n"
V4_NEXT_CLOSING = " Ответь только текстом вопроса, без кавычек и пояснений."
V4_REFLECT_OPENING = "Молитва закончилась, человек готов записать один вывод.\n"
V4_REFLECT_TOPIC = "Цель была: «{topic}».\n"
V4_REFLECT_ANSWERS = "Его ответы во время молитвы:\n"
V4_REFLECT_SILENT = "Он молился молча, письменных ответов нет.\n"
V4_REFLECT_INSTRUCTION = (
    "Задай один тёплый итоговый вопрос, который поможет ему назвать главное "
    "из этой молитвы. Не цитируй его ответы дословно. Ответь только текстом "
    "вопроса."
)

V4 = "v4"
V5_STRUCTURED = "v5-structured"
# The shipped prompt of ClickUp 86cbejvt2, addressable by name. It is NOT a
# frozen copy: `v6` and `production` build the same bytes today, and a test
# (`tests/test_question_prompt.py`) pins that byte for byte in both directions.
# The name exists so a run of THIS version can be recorded as such in an
# artifact sidecar — `production` means "whatever the module says today", which
# stops being readable the moment v7 lands. When that happens, v6 is the entry
# to freeze here, the way v4 is frozen above.
V6 = "v6"
VARIANTS = (*tuple(CANDIDATES), V4, V5_STRUCTURED, V6)
# The one name that always means "whatever `app/question_prompt.py` says right
# now" — it is what a run of the shipped prompt is called after the winner is
# promoted, and it is the only entry that must never be frozen here.
PRODUCTION = "production"


def candidate(variant: str) -> Candidate | None:
    """The frozen candidate, or `None` for the live production wording."""
    if variant == V6 and question_prompt.QUESTION_PROMPT_VERSION != 6:
        # `v6` is a NAME for the live module while the live module is v6. Once
        # production moves on, a run asking for `v6` would silently measure the
        # newer text under the older label — the one failure this file exists
        # to prevent (see the `user_message` docstring). Freeze v6 here the way
        # v4 is frozen above, then delete this guard.
        raise SystemExit(
            "question_prompts: --prompt-variant v6 names the live wording, but "
            f"app/question_prompt.py is v{question_prompt.QUESTION_PROMPT_VERSION} "
            "now — freeze v6 as a Candidate before measuring it again"
        )
    if variant in (PRODUCTION, V4, V5_STRUCTURED, V6):
        return None
    try:
        return CANDIDATES[variant]
    except KeyError:
        raise SystemExit(
            f"unknown prompt variant {variant!r}: "
            f"{', '.join((*VARIANTS, PRODUCTION))}"
        ) from None


def system_prompt(variant: str, language: str | None) -> str:
    """The system instruction of one call, in this variant."""
    if variant == V4:
        return V4_SYSTEM_TEMPLATE.format(
            language=question_prompt.LANGUAGE_NAMES.get(
                language or "", question_prompt.UNDETERMINED_LANGUAGE
            )
        )
    if variant == V5_STRUCTURED:
        prompt = question_prompt.build_question_prompt(None)
        language_name = question_prompt.LANGUAGE_NAMES.get(language or "")
        if language_name:
            anchor = (
                "Detect the language from the person's own words and write in "
                "exactly that language."
            )
            if prompt.count(anchor) != 1:
                raise ValueError("Universal language rule changed: rebuild the structured ablation explicitly")
            prompt = prompt.replace(anchor, f"Write in {language_name}, and in no other language.")
        return prompt
    item = candidate(variant)
    if item is None:
        return question_prompt.build_question_prompt(language)
    return item.system_template.format(
        language=question_prompt.LANGUAGE_NAMES.get(
            language or "", question_prompt.UNDETERMINED_LANGUAGE
        )
    )


def _v4_user_message(
    topic: str,
    stage: str,
    messages: Sequence[tuple[str, str]],
    skipped_questions: Sequence[str],
) -> str:
    """Frozen assembly of the user message sent with prompt v4."""
    if stage not in question_prompt.STAGES:
        raise ValueError(f"unknown stage: {stage!r}")
    topic = topic.strip()
    asked = [text.strip() for role, text in messages if role == "assistant" and text.strip()]
    answered = [text.strip() for role, text in messages if role == "user" and text.strip()]
    skipped = [text.strip() for text in skipped_questions if text.strip()]
    def bullets(values: Sequence[str]) -> str:
        return "".join(f"— {value}\n" for value in values)
    if stage == "first":
        return (V4_FIRST_TOPIC.format(topic=topic) if topic else V4_FIRST_NO_TOPIC) + V4_FIRST_INSTRUCTION
    if stage == "next":
        parts = [V4_NEXT_TOPIC.format(topic=topic) if topic else V4_NEXT_NO_TOPIC]
        if asked:
            parts.append(V4_ASKED + bullets(asked))
        if skipped:
            parts.append(V3_SKIPPED_HEADER + bullets(skipped))
        if answered:
            parts.append(V4_ANSWERED + bullets(answered))
        instruction = V4_NEXT_OPENING
        if skipped:
            instruction += V3_SKIPPED_SENTENCE
        parts.append(instruction + V4_NEXT_CLOSING)
        return "".join(parts)
    parts = [V4_REFLECT_OPENING]
    if topic:
        parts.append(V4_REFLECT_TOPIC.format(topic=topic))
    parts.append(V4_REFLECT_ANSWERS + bullets(answered) if answered else V4_REFLECT_SILENT)
    parts.append(V4_REFLECT_INSTRUCTION)
    return "".join(parts)


def user_message(
    variant: str,
    topic: str,
    stage: str,
    messages: Sequence[tuple[str, str]],
    skipped_questions: Sequence[str] = (),
    language: str | None = None,
    gender: str | None = None,
    used_subjects: Sequence[str] = (),
) -> str:
    """The user content of one call, in this variant.

    Assembled by the PRODUCTION `build_user_message` and then edited, so a
    candidate can never drift in the parts it does not claim to change (the
    bullets, the headers, the stage rules, the whitespace). Each edit must
    apply where its string is present, or the run stops: a candidate that
    silently measured the production wording is the worst outcome here.

    `gender` and `used_subjects` are prompt v6's own
    (`app/person_gender.detect_gender` and `app/question_format.SubjectMemory`,
    ClickUp 86cbejvt2). Both reach the production assembly and nothing else:
    the frozen v4 message never had either line, and the `v5-structured`
    ablation is kept exactly as it was measured.
    """
    if variant == V4:
        return _v4_user_message(topic, stage, messages, skipped_questions)
    if variant == V5_STRUCTURED:
        return question_prompt.build_user_message(
            topic, stage, messages, skipped_questions, "en"
        )
    item = candidate(variant)
    if item is None:
        return question_prompt.build_user_message(
            topic, stage, messages, skipped_questions, language, gender,
            used_subjects,
        )
    message = _v4_user_message(topic, stage, messages, skipped_questions)
    for live, replacement in (
        (V4_NEXT_OPENING, item.next_opening),
        (V3_SKIPPED_HEADER, item.skipped_header),
        (V3_SKIPPED_SENTENCE, item.skipped_sentence),
    ):
        if live in message:
            message = message.replace(live, replacement)
    for live in (
        V4_NEXT_OPENING,
        V3_SKIPPED_HEADER,
        V3_SKIPPED_SENTENCE,
    ):
        if live in message and live not in (
            item.next_opening,
            item.skipped_header,
            item.skipped_sentence,
        ):
            raise SystemExit(
                "question_prompts: a production string survived the surgery — "
                f"variant {variant!r} would have measured production"
            )
    return message


def structured_answer(variant: str) -> bool:
    """Does this variant ask the model for the v6 JSON object?

    `v6` by name, and `production` while production is v6 or later — so a
    default run parses whatever the shipped prompt asks for, and a run of a
    frozen pre-v6 wording keeps reading the answer as a bare line, which is
    what those artifacts contain.
    """
    if variant == V6:
        return True
    if variant == PRODUCTION:
        return question_prompt.QUESTION_PROMPT_VERSION >= 6
    return False


def describe(variant: str) -> str:
    """One line for the run banner and the artifact sidecar."""
    if variant == V4:
        return "frozen production prompt v4"
    if variant == V5_STRUCTURED:
        return "structured prompt v5 with English instructions"
    if variant == V6:
        candidate(variant)  # refuse to run under a newer production wording
        return (
            "prompt v6 (the live wording): structured JSON answer, per-step "
            "angle, gender stated by code"
        )
    item = candidate(variant)
    if item is None:
        return (
            f"production wording (app/question_prompt.py v"
            f"{question_prompt.QUESTION_PROMPT_VERSION})"
        )
    return f"candidate {item.name}: {item.note}"


def prompt_version(variant: str) -> int:
    """Version represented by an artifact row, including frozen variants."""
    candidate(variant)  # validate before returning metadata
    if variant == "v3":
        return 3
    if variant in ("a", "b", "c", "c2", V4):
        return 4
    if variant == V6:
        return 6
    return question_prompt.QUESTION_PROMPT_VERSION


if __name__ == "__main__":  # a human check of what each candidate sends
    for name in (*VARIANTS, PRODUCTION):
        print(f"=== {name} — {describe(name)}")
        print(system_prompt(name, "ru"))
        print("---")
        print(
            user_message(
                name,
                "Понять масштаб целей на завтра",
                "next",
                [("assistant", "Что сейчас внутри тебя?"), ("user", "Я рада.")],
                ["А что, если завтра окажется, что готовое — не то?"],
            )
        )
        print()
