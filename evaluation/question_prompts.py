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
VARIANTS = tuple(CANDIDATES)
# The one name that always means "whatever `app/question_prompt.py` says right
# now" — it is what a run of the shipped prompt is called after the winner is
# promoted, and it is the only entry that must never be frozen here.
PRODUCTION = "production"


def candidate(variant: str) -> Candidate | None:
    """The frozen candidate, or `None` for the live production wording."""
    if variant == PRODUCTION:
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
    item = candidate(variant)
    if item is None:
        return question_prompt.build_question_prompt(language)
    return item.system_template.format(
        language=question_prompt.LANGUAGE_NAMES.get(
            language or "", question_prompt.UNDETERMINED_LANGUAGE
        )
    )


def user_message(
    variant: str,
    topic: str,
    stage: str,
    messages: Sequence[tuple[str, str]],
    skipped_questions: Sequence[str] = (),
) -> str:
    """The user content of one call, in this variant.

    Assembled by the PRODUCTION `build_user_message` and then edited, so a
    candidate can never drift in the parts it does not claim to change (the
    bullets, the headers, the stage rules, the whitespace). Each edit must
    apply where its string is present, or the run stops: a candidate that
    silently measured the production wording is the worst outcome here.
    """
    message = question_prompt.build_user_message(
        topic, stage, messages, skipped_questions
    )
    item = candidate(variant)
    if item is None:
        return message
    for live, replacement in (
        (question_prompt.NEXT_INSTRUCTION_OPENING, item.next_opening),
        (question_prompt.SKIPPED_HEADER, item.skipped_header),
        (question_prompt.NEXT_SKIPPED_SENTENCE, item.skipped_sentence),
    ):
        if live in message:
            message = message.replace(live, replacement)
    for live in (
        question_prompt.NEXT_INSTRUCTION_OPENING,
        question_prompt.SKIPPED_HEADER,
        question_prompt.NEXT_SKIPPED_SENTENCE,
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


def describe(variant: str) -> str:
    """One line for the run banner and the artifact sidecar."""
    item = candidate(variant)
    if item is None:
        return (
            f"production wording (app/question_prompt.py v"
            f"{question_prompt.QUESTION_PROMPT_VERSION})"
        )
    return f"candidate {item.name}: {item.note}"


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
