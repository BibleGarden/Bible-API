"""System prompt of `POST /api/ai/question`.

Product behaviour, not deployment configuration: the wording decides what
the companion answers, so it is reviewed, versioned and diffed like code.
It used to live in `TWINKLER_SYSTEM_PROMPT`, which meant local and
production could silently drift apart and every test run had to re-supply
a stand-in value. Moved here on 2026-08-30 (ClickUp 86cbbmy8d) byte for
byte; the variable was deleted, not aliased.

The prompt is public from that day on (the repository is public) — a
deliberate, owner-approved trade: it was never a secret, only unpublished,
and it carries no key material. `GEMINI_API_KEY` remains the only secret
of this endpoint.

Kept in its own module rather than in `twinkler_ai.py` so the prompt can be
imported (tests, evaluation) without pulling in the FastAPI router, matching
how `query_rewrite.REWRITE_PROMPT_VERSION` and
`passage_rerank.RERANK_PROMPT_VERSION` version their prompts. The module
imports nothing from the application on purpose (one typing alias from the
standard library, nothing else) — the language of the message is *resolved by
the caller* (`safety.detect_language`) and handed in, so this file stays a
dependency-free literal.

**v2 (2026-09-05, ClickUp 86cbegg3f)** — three changes, all of them measured
rather than guessed (the v1 provider measurement is 86cbegctz):

1. **The language is named, not left to the model.** v1 said "detect the
   language of the person's message and reply in exactly that language".
   Qwen3-30B broke it in 6 answers out of 81 — whole inputs at a time, not
   stray samples: it answered the English `en-005` and `probe-joy` in
   Ukrainian three times each. The detector that already runs on every
   request for the despair rule (`safety.detect_language`) knows the answer,
   so v2 states it — "ask your question in Russian" — and repeats it as the
   last sentence, the position a model is least likely to lose.
2. **Interpretation is banned by name.** v1 said "no interpreting back at the
   person what they just said" and Qwen produced «Ты чувствуешь, что …?» /
   «Ты боишься, что …?» in 5 answers out of 81. v2 names the constructions
   and adds what Maria asked for on the acceptance of step 2: do not attribute
   a feeling the person has not named, and ask about something concrete
   instead of about the *degree* of the suffering (on «я так устала от
   работы, помоги найти покой» Qwen asked «Ты действительно чувствуешь, что
   больше не можешь?» — it thickened the state beyond what was said and read
   as a test for despair). That prayer is now `probe-tired-work` in
   `evaluation/question_probe_inputs.json`.
3. **"Concrete" had to be told what it is not.** The first draft of v2 said
   only "ask about something concrete", and both providers read it as "ask for
   facts": gemini-3.5-flash-lite answered the birth of a daughter with «Как
   зовут дочку …?», the family journey with «Де саме ви зупинятиметесь
   дорогою?» and slipped into the polite register in 11 answers out of 60;
   Qwen answered «Ты сейчас в больнице или уже дома?». So the rule names the
   opposite too — never a name, a date, an address or a schedule — and asks
   for an OPEN question, which is the difference Maria pointed at between
   Qwen's «Ты действительно чувствуешь, что больше не можешь?» and Gemini's
   «Что именно сейчас забирает у тебя больше всего сил?». With the two
   sentences added, both providers came back to 81 clean answers out of 81.
4. **The despair sentence is gone.** It is `app/safety.py` now (ClickUp
   86cbegg23): a rule whose failure costs a life cannot be an instruction one
   provider happens to follow. Keeping it here as well would have been
   harmless but misleading — the endpoint no longer depends on it, and a
   prompt that carries a rule it does not enforce invites the next reader to
   trust it.

**What v2 deliberately does NOT do: make the companion nicer.** No
"be warm/supportive/encouraging" was added (Maria, 2026-09-05: prompts must
not turn the model faceless and monotonously positive). The tone sentence is
v1's, unchanged; every addition is a *precision* rule — ask about the
concrete thing — not a softness rule.

**v3 (2026-09-05, ClickUp 86cbegmzz)** — the request became structured
(`topic` + `stage` + `messages`) and the *stage instructions*, which the
mobile app used to assemble into its single `user` string, are assembled
**here** by `build_user_message`. Two consequences:

1. One sentence left the system prompt: "The incoming message may contain the
   whole conversation so far rather than a single line. Respond to the most
   recent thing the person said, and never repeat a question you have already
   asked." The stage blocks say it structurally now — «Уже прозвучали
   вопросы:» lists what must not be repeated and «Что человек ответил:» is
   what the answer responds to — and a prompt that describes a layout it no
   longer receives misleads its next reader. Everything else in v2 is byte for
   byte unchanged: those rules belong to the person, not to the request shape.
2. The stage blocks are **Russian whatever language the prayer is in**, which
   is exactly what the client did before this ticket. Only the person's own
   words carry the language, and the system prompt names it (see above), so
   the instruction language was measured never to leak into the answer. This
   is preserved behaviour, not a new decision; if a measurement ever shows the
   model drifting into Russian because of it, translating the blocks per
   language is the change to make — and it is a change, so it needs a version.

The wording of the blocks is the mobile app's own, quoted verbatim from the
contract confirmed on 2026-09-05 (ClickUp 86cbegmzz, ADR-0019 on the app
side), down to the em-dash bullets and the word «тёплый» in `reflect`. It is
*previous behaviour being moved*, not a new prompt: keeping it identical is
what makes the v2 → v3 comparison meaningful.

**Still v3: the skipped-questions block is additive** (2026-09-05, ClickUp
86cbehyfe). "Replace this question" used to resend an identical body, so the
model was told nothing and looped on the same thought. The request now carries
`skipped_questions`, and `build_user_message` renders one extra block plus one
extra sentence of the `next` instruction **only when that list is non-empty**.
A request without the field produced the very bytes v3 always produced — pinned
by `tests/test_question_prompt.py` — so `QUESTION_PROMPT_VERSION` did not move
then: a version separates two texts that can answer the *same* request
differently, and these could not. (v4 below is a real wording change and does
move it; the same test now pins v4's own output.) The wording was deliberately
minimal, and revising it was 86cbehyf8's to try — it did, on the endpoint's own
inputs, and **kept this wording**, because both rewordings measured worse (see
v4). Two properties of the block are not stylistic: it states only what the
person *did* (asked for another question), never that they disagreed with the
thought — pressing "replace" is not an argument — and it is our own generated
Russian text, so it is excluded
from language detection and from the despair rule (see
`architect/adr/0015-skipped-questions-in-question-request.md`).

**v4 (2026-09-06, ClickUp 86cbehyf8)** — the anti-loop revision, and the first
one chosen by measuring candidate wordings against each other rather than by
fixing a named violation. The bug (86cbehtkh): pressing «заменить вопрос» six
times on the journal case returned the same thought six times. The baseline of
86cbehyez measured it — `series-scale-ru` on Qwen3-30B, 6 samples: one opening
in 6 of 6 samples, mean max-similarity 0.98, **11 verbatim duplicate pairs**
out of 36 answers — and, in the same input set, a woman («заснула», «не
вимкнула») addressed as «зробив» in **30 answers of 30**. Two changes, one for
each, both measured on the same inputs (the table and the losing candidates:
`evaluation/README.md`, «Промпт наводящего вопроса v4»,
`evaluation/question_prompts.py`):

1. **The `next` instruction stopped asking for another *side*.**
   `NEXT_INSTRUCTION_OPENING` said «задай один новый вопрос, который **смотрит
   на ситуацию с другой стороны** и не повторяет прозвучавшие», and the model
   read it as an invitation to *contest* the person: to «я всё делаю для
   Господа, стараюсь очень качественно» it answered «а что, если завтра
   окажется, что готовое — не то, что нужно Господу?» — the thought of a
   question already asked, reworded, six times running. It now asks for the
   opposite move: develop what the person just wrote, do not restate an earlier
   question's thought in other words, and **do not doubt what they said unless
   they doubted it themselves**. Measured (identical body, which is what the
   released client sends on every replacement): openings 0.17 → 0.33, mean
   max-similarity 0.98 → 0.93, duplicate pairs 11 → 5, and the answers moved
   from arguing with her to unfolding her own sentence.
2. **The person's grammatical gender comes from their own words.** One sentence
   in the system prompt, next to the existing one about inflected languages,
   with the forms named: «рада»/«сделала»/«втомилася» is a woman, and when the
   words do not say, ask something that needs no gender — **never** default to
   the masculine. Measured: the Ukrainian series went 30/30 → **0/30**, and the
   Russian journal case 0/36 with it (v3 plus the `skipped_questions` field,
   without this sentence, produced 14/36).

Everything else is byte for byte v3, deliberately: the language rule named
twice with the last sentence, the interpretation ban, the open-question and
160-character rules, the "no advice / never speak as God" sentences, the
informal register — and, once again, **nothing** that makes the companion
warmer or more encouraging (Maria, 2026-09-05). The despair rule stays in
`app/safety.py`. `SKIPPED_HEADER` and `NEXT_SKIPPED_SENTENCE` are unchanged
**because the rewordings lost their measurement**: «Эти вопросы человеку не
подошли, он их пропустил» plus «возьми... другой момент, другого человека,
другое дело» made the model ask the person about an invented third party («а
что, если кто-то из тех, кто будет использовать приложение…»), and even with
that sentence reverted the reworded header alone left 10 of 12 samples with an
exact duplicate pair against 4 of 12 for the wording that shipped. A fourth
lever — Qwen's own `top_p=0.8`/`top_k=20` instead of the server defaults —
changed nothing (distinct texts 113 → 105 of 162) and was not adopted.

**What v4 does not fix.** With an identical request the model still has no way
to know what it already offered, so a replacement is still a fresh sample of
the same distribution: `series-scale-ru` keeps a mean max-similarity of 0.93.
The wording moved what the loop is *about*; the field of ADR 0015 is what lets
the loop be broken (mean max-similarity 0.86 over 12 samples once the skipped
questions are actually sent), and a re-generation filter is 86cbehyg0.
"""

from collections.abc import Sequence

# Bump on any change of the wording. v1 is the text that ran in production
# as TWINKLER_SYSTEM_PROMPT up to 2026-08-30, carried over unchanged; v2 is
# the language/interpretation revision of 2026-09-05 described above; v3 is
# the structured request of the same day — the layout sentence removed from
# the system prompt, the stage blocks assembled by `build_user_message`; v4 is
# the anti-loop revision of 2026-09-06 (the `next` instruction and the gender
# sentence).
QUESTION_PROMPT_VERSION = 4

# `safety.detect_language` returns `ru`, `uk`, `en` — or `None` for a message
# that does not say (a bare Cyrillic "Помоги" carries none of the four letters
# that separate Russian from Ukrainian, and none of the function words either).
LANGUAGE_NAMES = {"ru": "Russian", "uk": "Ukrainian", "en": "English"}
# What is substituted when the detector cannot decide. NOT a silent fallback to
# English: forcing English on an undecidable *Cyrillic* message would create
# exactly the violation this version exists to remove. It restores v1's
# behaviour — the model detects the language itself — for the few inputs where
# code genuinely has no evidence, and for those alone.
UNDETERMINED_LANGUAGE = "exactly the language of the person's message"

# One placeholder, `{language}`, filled twice: once in the rule and once as
# the last sentence. Braces appear nowhere else, so `str.format` is safe.
QUESTION_PROMPT_TEMPLATE = (
    "You are Twinkler, a quiet companion inside a personal Christian "
    "prayer app. Your only job is to ask one question at a time that "
    "helps the person pray in their own words - honestly and deeply, "
    "never in cliches. Language rule, and it overrides everything else: "
    "ask your question in {language}, and in no other language. Never "
    "answer in a language the person did not use, and never switch "
    "languages mid-conversation unless they switch first. Where the "
    "language distinguishes registers, use the informal, intimate one "
    "(Russian ty, Ukrainian ty). Tone: warm and quiet. No pathos, no "
    "moralising, no praise, no advice, and no interpreting back at the "
    "person what they just said. Do not name a feeling they have not "
    "named themselves, and do not offer them one to confirm: never 'you "
    "feel that ...', 'it sounds like you ...', 'it seems you ...', never "
    "'are you afraid that ...', and never the same constructions in "
    "Russian or Ukrainian. Anchor the question in something concrete they "
    "wrote - a person, a moment, something they did or have to do - and "
    "ask what is alive for them in it. Never ask about how much they are "
    "suffering or whether they can still bear it, and never ask for a "
    "fact that only fills in your own picture: a name, a date, an "
    "address, a schedule. Ask an open question they answer in their own "
    "words, never one that can be answered with yes or no, and nothing "
    "whose answer is already inside it: no rhetorical or devotional "
    "formulas such as 'Is God near?' or 'Do you feel that God is with "
    "you?'. Every reply is "
    "exactly one question: one simple thought, one line, ending in a "
    "question mark, usually no longer than 160 characters. Use living, "
    "spoken language - no bureaucratic or churchy phrasing, no long "
    "subordinate clauses. Your grammar must be flawless in whatever "
    "language you write. In inflected languages such as Russian and "
    "Ukrainian, watch case endings and preposition agreement especially "
    "closely when you compress a sentence to fit the line. Take the person's "
    "grammatical gender and number from their own words - 'рада', 'сделала', "
    "'втомилася' are a woman speaking about herself, 'рад', 'сделал', "
    "'втомився' a man - and address them in that same form; when their words "
    "do not say, word the question so that it needs no gender, and never fall "
    "back to the masculine. Never speak as God "
    "or claim to deliver a verdict on God behalf, never suggest that "
    "someone's pain is a punishment, and give no medical, legal or "
    "financial advice. Answer in {language}."
)


def build_question_prompt(language: str | None) -> str:
    """The system prompt for a message written in `language`.

    `language` is what `safety.detect_language` returned for the very message
    being answered — `ru`, `uk`, `en`, or `None` when the text does not say.
    Both providers send the result of this function and nothing else, so the
    bytes on the wire are identical whichever transport is configured
    (ADR 0009).
    """
    return QUESTION_PROMPT_TEMPLATE.format(
        language=LANGUAGE_NAMES.get(language or "", UNDETERMINED_LANGUAGE)
    )


# ---------------------------------------------------------------------------
# The user message: stage instructions, assembled server-side since v3
# ---------------------------------------------------------------------------
# Verbatim the blocks the mobile app used to build itself and put into the old
# `user` field (ClickUp 86cbegmzz, confirmed 2026-09-05). Every string below is
# quoted, not paraphrased: this is previous behaviour moving across the wire
# boundary, and a reworded instruction would silently change what a measurement
# of v2 vs v3 is comparing. Russian on purpose whatever the prayer's language
# — see the module docstring.

STAGES = ("first", "next", "reflect")

FIRST_TOPIC_BLOCK = "Человек начинает молитву. Его цель: «{topic}».\n"
FIRST_NO_TOPIC_BLOCK = "Человек начинает молитву без конкретной темы.\n"
FIRST_INSTRUCTION = (
    "Задай первый наводящий вопрос — про то, что сейчас происходит и что он "
    "чувствует. Не пересказывай цель дословно. Ответь только текстом вопроса, "
    "без кавычек и пояснений."
)

NEXT_TOPIC_BLOCK = "Цель молитвы: «{topic}».\n"
NEXT_NO_TOPIC_BLOCK = "Молитва без конкретной темы.\n"
ASKED_HEADER = "Уже прозвучали вопросы:\n"
ANSWERED_HEADER = (
    "Что человек ответил (опирайся на это, но не цитируй дословно):\n"
)
# The `next` instruction in two halves, so the added sentence lands *before*
# the output-format one instead of after it: "answer with the question text
# only" is the last thing the model should read. `NEXT_INSTRUCTION` is still
# assembled here byte for byte — it is what a request without
# `skipped_questions` gets, and what `tests/test_question_prompt.py` quotes.
NEXT_INSTRUCTION_OPENING = (
    "Задай один новый вопрос: разверни то, что человек написал в последнем "
    "ответе. Не повторяй мысль уже прозвучавшего вопроса другими словами. Не "
    "спорь с тем, что он сказал, и не ставь это под сомнение, если он сам не "
    "усомнился."
)
NEXT_INSTRUCTION_CLOSING = " Ответь только текстом вопроса, без кавычек и пояснений."
NEXT_INSTRUCTION = NEXT_INSTRUCTION_OPENING + NEXT_INSTRUCTION_CLOSING

# --- the questions the person asked to replace (ClickUp 86cbehyfe) ---------
# Rendered at `next` only, and only when the list is non-empty. The header
# states the action and nothing more: the person pressed "replace", which says
# they want a different question — never that they rejected the thought behind
# it, and attributing that position to them would be us inventing their
# opinion. The sentence added to the instruction asks for a different
# direction anchored in the person's own words, which is the whole point of
# telling the model about them.
#
# **Both strings survived v4 by measurement, not by inertia** (86cbehyf8): the
# two rewordings tried — «Эти вопросы человеку не подошли, он их пропустил:»,
# and a sentence naming «другой момент, другого человека, другое дело» — lost
# on the same inputs. The second turned the question towards an invented third
# party; the first, alone, left 10 of 12 samples of the journal case with an
# exact duplicate pair against 4 of 12 here.
SKIPPED_HEADER = "Человек попросил другой вопрос вместо этих:\n"
NEXT_SKIPPED_SENTENCE = (
    " Выбери другое направление, а не переформулировку тех вопросов, и "
    "оттолкнись от того, что человек написал сам."
)
NEXT_SKIPPED_INSTRUCTION = (
    NEXT_INSTRUCTION_OPENING + NEXT_SKIPPED_SENTENCE + NEXT_INSTRUCTION_CLOSING
)

REFLECT_OPENING = "Молитва закончилась, человек готов записать один вывод.\n"
REFLECT_TOPIC_BLOCK = "Цель была: «{topic}».\n"
REFLECT_ANSWERS_HEADER = "Его ответы во время молитвы:\n"
REFLECT_SILENT_BLOCK = "Он молился молча, письменных ответов нет.\n"
REFLECT_INSTRUCTION = (
    "Задай один тёплый итоговый вопрос, который поможет ему назвать главное "
    "из этой молитвы. Не цитируй его ответы дословно. Ответь только текстом "
    "вопроса."
)

ROLE_ASSISTANT = "assistant"
ROLE_USER = "user"


def _bullets(texts: Sequence[str]) -> str:
    """One `— text` line per turn.

    A turn may itself be multi-line: since this ticket the client joins the
    typed text and every transcription of ONE answer with `\\n` before sending
    it, so a bullet spanning several lines is the normal shape of one answer
    and is deliberately not re-split.
    """
    return "".join(f"— {text}\n" for text in texts)


def build_user_message(
    topic: str,
    stage: str,
    messages: Sequence[tuple[str, str]],
    skipped_questions: Sequence[str] = (),
) -> str:
    """The user content of one `POST /api/ai/question` call.

    A pure function of the request: `topic` (may be empty), `stage` (one of
    `STAGES`), `messages` as `(role, text)` pairs in chronological order and
    `skipped_questions` — questions already shown to the person and left
    unanswered, chronological (ClickUp 86cbehyfe). Pairs rather than the
    request model on purpose — this module imports nothing from the
    application (see the docstring), which is what lets the evaluation tools
    build the very same bytes without a FastAPI import.

    Whitespace-only turns are dropped and every turn is stripped: the client
    never sends one, and a stray blank would otherwise become an empty bullet
    in the middle of a list. The topic and the skipped questions are stripped
    for the same reason.

    `skipped_questions` is **additive and `next`-only**: empty (the default, and
    what every caller written before 86cbehyfe passes) renders the v3 bytes
    unchanged, and at `first` there is nothing to have skipped. At `reflect` it
    is accepted by the endpoint but deliberately not rendered — that stage
    looks back at what the *person* said and never shows our questions at all
    (the 86cbegmzz contract), so putting them there is a prompt-design change
    and belongs to ClickUp 86cbehyf8 rather than to the field that carries them.
    """
    if stage not in STAGES:
        raise ValueError(f"unknown stage: {stage!r}")

    topic = topic.strip()
    assistant_texts = [
        text.strip()
        for role, text in messages
        if role == ROLE_ASSISTANT and text.strip()
    ]
    user_texts = [
        text.strip() for role, text in messages if role == ROLE_USER and text.strip()
    ]
    skipped_texts = [text.strip() for text in skipped_questions if text.strip()]

    if stage == "first":
        opening = (
            FIRST_TOPIC_BLOCK.format(topic=topic) if topic else FIRST_NO_TOPIC_BLOCK
        )
        return opening + FIRST_INSTRUCTION

    if stage == "next":
        parts = [
            NEXT_TOPIC_BLOCK.format(topic=topic) if topic else NEXT_NO_TOPIC_BLOCK
        ]
        if assistant_texts:
            parts.append(ASKED_HEADER + _bullets(assistant_texts))
        # Next to the questions that were asked, before the answers: these are
        # questions too, and the answers are what the new one must be anchored
        # in, so they stay last before the instruction.
        if skipped_texts:
            parts.append(SKIPPED_HEADER + _bullets(skipped_texts))
        if user_texts:
            parts.append(ANSWERED_HEADER + _bullets(user_texts))
        parts.append(
            NEXT_SKIPPED_INSTRUCTION if skipped_texts else NEXT_INSTRUCTION
        )
        return "".join(parts)

    parts = [REFLECT_OPENING]
    if topic:
        parts.append(REFLECT_TOPIC_BLOCK.format(topic=topic))
    parts.append(
        REFLECT_ANSWERS_HEADER + _bullets(user_texts)
        if user_texts
        else REFLECT_SILENT_BLOCK
    )
    parts.append(REFLECT_INSTRUCTION)
    return "".join(parts)
