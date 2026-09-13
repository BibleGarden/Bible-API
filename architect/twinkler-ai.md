# Twinkler AI

## Public contract

> **Renamed 2026-08-30 (ClickUp 86cbbmwjk).** These two routes were
> `POST /api/twinkler/v1/complete` and `POST /api/twinkler/v1/transcribe`
> until then: methods are not named after a client app (`twinkler`) or a
> provider's jargon (`complete`), and the `v1` was never a real version.
> Bodies, responses, headers, authentication and limits are unchanged — only
> the paths moved. The old paths return 404; there are no aliases (a single
> unpublished client, renamed in a paired mobile ticket). The module and the
> handlers keep their names.

> **Environment variables renamed 2026-08-30 (ClickUp 86cbbmy8d).** The
> settings now mirror the method they configure: `GEMINI_MODEL` →
> `AI_QUESTION_MODEL`, `GEMINI_TRANSCRIPTION_MODEL` → `AI_TRANSCRIBE_MODEL`,
> `GEMINI_REQUESTS_PER_[CLIENT_]MINUTE` → `AI_REQUESTS_PER_[CLIENT_]MINUTE`,
> `TWINKLER_CLIENT_HMAC_KEY` → `AI_CLIENT_HMAC_KEY`. Values were not touched,
> so existing client pseudonyms stay stable (the HMAC is keyed by the value;
> the variable name never enters the digest). `TWINKLER_SYSTEM_PROMPT` was
> deleted — see "System prompt" below. No old name is accepted as an alias:
> a forgotten one fails the start naming the variable it wants.

> **The request became structured on 2026-09-05 (ClickUp 86cbegmzz).** The
> single `user` string is gone, with no transitional support: both ends
> changed at once and the app is unpublished. The response is unchanged.

`POST /api/ai/question` accepts a JSON object with three required fields:

```json
{"topic": "Отношения с семьёй", "stage": "next",
 "messages": [{"role": "assistant", "text": "Что сейчас тревожит тебя?"},
              {"role": "user", "text": "Мне одиноко.\nХочу восстановить общение."}]}
```

| field | rules |
| --- | --- |
| `topic` | what the person is praying about, `""` when they named nothing; ≤ 2000 characters |
| `stage` | `first` (the opening question), `next` (the following one), `reflect` (the closing question that helps name one takeaway) |
| `messages` | the conversation so far, chronological, ≤ 40 items; `role` is `assistant` (a question we asked) or `user` (their answer), `text` is non-empty |

and one optional field:

| field | rules |
| --- | --- |
| `skipped_questions` | questions already shown and left unanswered — replaced or skipped — chronological, ≤ 10 items of ≤ 300 characters each; defaults to `[]`, and must be empty with `first` (ClickUp 86cbehyfe) |

`topic`, every `text` and every skipped question together must not exceed
**16 000 characters** — the client's own ceiling, counted there in UTF-16 units
and here in code points (they differ only for astral characters, where this
bound is the looser of the two; the tighter side is the one that decides what
is ever sent).

What the client guarantees, and what the server therefore relies on:

- **`first` always carries an empty history** — including when the person
  changes the topic and a new opening question is generated. A non-empty
  history with `first` is a `422`.
- **A non-empty history ends with a `user` turn.** The question is asked about
  what the person said last; anything else is a `422`.
- The history **may start with a `user` turn**: an over-long conversation is
  trimmed from the front, whole turns at a time, so the question that answer
  belonged to can be gone.
- Skipped questions and empty answers are omitted, so the two roles do not
  have to alternate.
- Several transcriptions and the typed text of **one** turn are already joined
  with newlines into a single `user` element. A multi-line turn is normal.
- **`messages: []` with `next` or `reflect` is normal**, not an error: the
  person answered nothing, or forbade sending the answers. The question is
  then built from the topic and the stage alone — and from the stage alone
  when the topic is empty too.

The endpoint requires the common `X-API-Key` header. Unknown JSON fields are
rejected, and the two removed ones (`user`, `last_user_message`) are rejected
with a `422` that names them and says what to send instead — "extra inputs are
not permitted" would send whoever reads the log to the wrong place. The
response is `{ "text": "...", "novel": true, "subject": "..." }` on success —
both `novel` (ClickUp 86cbehyg0, "The question must be new" below) and
`subject` (prompt v6, "v6" below: the 2-4 words the model named as the subject
of this question, `null` when its answer could not be read as the structured
object) are additive, and a client that reads only `text` behaves exactly as
before. Documented errors are `403`,
`429` with `Retry-After`, `502`, and `503`; validation errors use `422`.

A **reply** showing despair or self-harm is answered with a fixed warm text
instead of a model answer, and no provider is called — the response shape and
status are unchanged (see "The despair rule is code" below).

### The stage instructions are the server's (ClickUp 86cbegmzz)

The client used to assemble these blocks itself and put the result into
`user`. `app/question_prompt.build_user_message(topic, stage, messages)` now
does it, **verbatim** — the wording below is quoted from the contract the
mobile agent confirmed on 2026-09-05 (ADR-0019 on their side), down to the
em-dash bullets and the word «тёплый» in `reflect`. It is previous behaviour
moving across the wire boundary, not a new prompt: keeping it identical is
what makes a v2 → v3 comparison mean anything.

The blocks are **Russian whatever language the prayer is in**, exactly as the
client always sent them. Only the person's own words carry the language, and
the system prompt names it separately (below), so the instruction language was
never observed to leak into an answer. If a measurement ever shows a model
drifting into Russian because of it, translating the blocks per language is
the change to make — and it is a change, so it needs a version bump.

**`first`** — the goal line, then the instruction:

```
Человек начинает молитву. Его цель: «{topic}».
Задай первый наводящий вопрос — про то, что сейчас происходит и что он чувствует. Не пересказывай цель дословно. Ответь только текстом вопроса, без кавычек и пояснений.
```

With no topic the first line is `Человек начинает молитву без конкретной темы.`

**`next`** — the goal line, the questions already asked, the answers, the
instruction. A block with nothing in it is omitted, not left empty. The
instruction line below is the **v4** one (2026-09-06, "v4" further down); the
three blocks above it are unchanged since 86cbegmzz, and a request carrying
`skipped_questions` gets one more block between them (the next section):

```
Цель молитвы: «{topic}».
Уже прозвучали вопросы:
— {each assistant turn}
Что человек ответил (опирайся на это, но не цитируй дословно):
— {each user turn}
Задай один новый вопрос: разверни то, что человек написал в последнем ответе. Не повторяй мысль уже прозвучавшего вопроса другими словами. Не спорь с тем, что он сказал, и не ставь это под сомнение, если он сам не усомнился. Ответь только текстом вопроса, без кавычек и пояснений.
```

With no topic the first line is `Молитва без конкретной темы.`

**`reflect`** — the closing question. It never lists our questions: it looks
back at what the *person* said.

```
Молитва закончилась, человек готов записать один вывод.
Цель была: «{topic}».
Его ответы во время молитвы:
— {each user turn}
Задай один тёплый итоговый вопрос, который поможет ему назвать главное из этой молитвы. Не цитируй его ответы дословно. Ответь только текстом вопроса.
```

The goal line is omitted when there is no topic; with no answers the third
block is the single line `Он молился молча, письменных ответов нет.`

A turn is copied verbatim into its bullet and never re-split, so a multi-line
answer is one bullet. The topic is trimmed; a whitespace-only topic is "no
topic". `tests/test_question_prompt.py` holds every assembly as a golden
string, written out in full rather than imported from the module it checks.

### Replaced questions: `skipped_questions` (ClickUp 86cbehyfe)

In Lampada the person can press "replace question". Until this ticket the
client dropped the unanswered question and resent an **identical** body, so the
model was never told its question had been declined and came back with the same
thought in different words. The client now accumulates them and sends them:

**The client's rule.** Every question of the *current* prayer that was shown
and left unanswered — replaced or skipped — chronologically, all of them, and
**never a question that is already in `messages`**: a question the person
answered belongs in the history as an `assistant` turn, and a question they
replaced belongs here. The two lists do not overlap. The list is reset with the
prayer, and it is empty (or absent) when nothing was replaced.

The field is **additive**: a request without it produces byte for byte the
message a client that never replaces a question gets, which is why the field
itself did not move `QUESTION_PROMPT_VERSION` (v4 did, for its own reason —
below — and the block below is v4's `next` instruction):

```
Цель молитвы: «{topic}».
Уже прозвучали вопросы:
— {each assistant turn}
Человек попросил другой вопрос вместо этих:
— {each skipped question}
Что человек ответил (опирайся на это, но не цитируй дословно):
— {each user turn}
Задай один новый вопрос: разверни то, что человек написал в последнем ответе. Не повторяй мысль уже прозвучавшего вопроса другими словами. Не спорь с тем, что он сказал, и не ставь это под сомнение, если он сам не усомнился. Выбери другое направление, а не переформулировку тех вопросов, и оттолкнись от того, что человек написал сам. Ответь только текстом вопроса, без кавычек и пояснений.
```

The header states **what the person did and nothing else**. Pressing "replace"
says they want a different question — never that they disagree with the thought
behind it, and a block that told the model so would be us inventing their
opinion and then answering it. The wording was deliberately minimal, and
86cbehyf8 (prompt v4) tried two rewordings of it on the endpoint's own inputs
— **both measured worse**, so this wording stayed. See "v4" below.

At **`reflect`** the field is accepted but **not rendered**. That stage looks
back at what the *person* said and shows none of our questions at all (above),
so putting them there is a prompt-design change rather than a property of the
field; the client may send one unconditional shape, and turning the block on
later is a server-only edit. At **`first`** a non-empty list is a `422`, the
same reasoning as `messages` with `first`: nothing has been shown yet.

Entries are stripped and blank ones are **dropped, not refused** — these are
*our own* questions handed back to us, so an empty one says nothing about the
person, is indistinguishable from the field being absent, and must not cost
them their next question. A `messages` turn is the opposite case (`text` has
`min_length: 1`) because it is the person's own words.

Three examples for the client developer:

```json
{"topic": "Понять масштаб целей на завтра", "stage": "next", "messages": [],
 "skipped_questions": ["Что сейчас внутри тебя, когда ты только начинаешь молитву?"]}
```

```json
{"topic": "Понять масштаб целей на завтра", "stage": "next",
 "messages": [
   {"role": "assistant", "text": "Что сейчас внутри тебя, когда ты только начинаешь молитву?"},
   {"role": "user", "text": "Я рада тому, что сегодня немало сделано…"},
   {"role": "assistant", "text": "А что, если завтра окажется, что всё, что ты сегодня считал готовым, всё ещё не совсем то, что нужно?"},
   {"role": "user", "text": "Ну буду доделывать. Я все делаю для Господа, стараюсь сделать очень качественно"}],
 "skipped_questions": ["Что из сделанного сегодня тебе самой дороже всего?"]}
```

```json
{"topic": "Понять масштаб целей на завтра", "stage": "reflect",
 "messages": [{"role": "user", "text": "Ну буду доделывать. Я все делаю для Господа"}],
 "skipped_questions": ["Что ты хочешь унести из этой молитвы?"]}
```

The second example is the shape that matters: the two questions the person
answered are `assistant` turns, the one they replaced is in
`skipped_questions`, and neither list repeats the other.

`422` wordings, so a log is diagnosable without the request body:

- `stage 'first' is the opening question and takes no skipped_questions: nothing has been shown to the person yet (use stage 'next' after a question was replaced)`
- `each skipped_questions entry must not exceed 300 characters (got N)`
- `topic, messages and skipped_questions together must not exceed 16000 characters (got N)`
- more than ten entries is pydantic's own `List should have at most 10 items`, at `loc: ["body", "skipped_questions"]`

**The field reaches the model and nothing else.** It votes on neither the
answer's language nor the despair rule — see the table below and
`architect/adr/0015-skipped-questions-in-question-request.md`.

### Which text each rule reads

Two different questions are asked of one request, deliberately reading
different parts of it — but as of Maria's 2026-09-05 decision, the two tiers
of the despair rule now agree with each other on which part:

| | text | why |
| --- | --- | --- |
| the model | the whole assembled message | that is the request |
| **both tiers** of the despair rule | the **last `user` turn** — or `topic` when `stage` is `first`, where the topic is the newest thing the person wrote | see below |
| the answer's language (prompt, and tier 2's fixed reply) | the last `user` turn → the topic → their earlier replies, newest first → else the last `assistant` turn → else English | the person's own words decide; a question of ours must not vote |
| `skipped_questions` | read by **nothing** but the model | our own generated text, wrapped in a Russian block whatever the prayer's language: it can neither name the language nor speak despair on the person's behalf (ClickUp 86cbehyfe) |

That language chain is walked by **decidability, not presence**: the offline
`py3langid` detector returns `None` when its normalized top probability is
below `0.9` (or the input has no letters), so a short uncertain line such as
«Помоги» lets the walk move on to the next thing the *same person* wrote
rather than handing the prompt v2's "answer in exactly the language of the
person's message" — 9 of the 33 evaluation inputs were undetermined when it
stopped at the first non-empty candidate, 6 when it stops at the first
decidable one, and the topic alone recovered none of the three that regressed
(their evidence is an earlier reply). The last `assistant` turn is still
reached only when the person wrote nothing at all.

**Tier 1 reads the last reply, and this is the bug the ticket closed.** While
the request was one string, tier 1 saw the whole conversation: the phrase that
had already been answered with the fixed reply kept answering every later
question of that prayer with it, and the conversation could not continue. The
last reply is also why the topic is **not** substituted for a missing one at
`next`/`reflect`: the person said nothing new, so nothing new can be found —
and substituting it would rebuild the same loop out of a topic.

Tier 2 reads the person's words rather than the assembled message, same as
tier 1 — and, since Maria's 2026-09-05 decision, the exact same **last**
reply as tier 1, not the topic plus every reply it used to read. The request
is split into turns precisely so this rule looks at the last one: an older
despair phrase is someone else's turn now, already answered one way or
another, and must not keep replacing every later answer of the prayer with
the fixed text.

What did **not** change is `safety.check_reply` itself — it stays a pure
function of the text it is handed and still resolves its own `SafetyFinding.language`
from that text if asked. What changed is that the caller
(`twinkler_ai.twinkler_complete`) no longer trusts that resolution for the
*reply's* language: it looks it up separately, through `language_source` — the
same source the prompt and tier 1 use — because the phrase that trips the
tier-2 pattern is not necessarily the language the rest of the prayer is in.
An English prayer must not be answered in Russian because our own wrapper
outvoted it.

### The question must be new: the novelty check (ClickUp 86cbehyg0)

`skipped_questions` tells the model which questions were declined; it does not
stop it from offering them again. The measured replacement series show it
plainly: pressing "replace" six times on one prayer produced six variants of a
single sentence, the last five differing from the first only in the tail
([bench_data/questions_qwen30b_v3_series.jsonl](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/bench_data/questions_qwen30b_v3_series.jsonl), `series-scale-ru`).
So the server now checks the answer before returning it.

**What is compared.** The generated text against everything the person has
already been *shown* in this prayer: the `assistant` turns of `messages` plus
`skipped_questions`. Their own replies are not in that list — a question is
never a repeat of an answer.

**The metric** is `app/question_novelty.py`: normalize (casefold, ё→е, drop
punctuation and quotes, collapse whitespace) and then Jaccard over character
3-grams, flagged at **0.60**, plus a second rule for a repeated sentence with a
new tail — a shared opening of ≥ 4 normalized words covering ≥ 0.7 of the
shorter question. Both constants are code, not environment (ADR 0008), and the
module docstring carries the table they were chosen on. It is the very
definition [check_questions.py](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/check_questions.py) reports for those series, so the
benchmark number and the production filter are one measurement.

**Lexical only.** Two questions with no shared wording can still be one
thought; that is not what this catches, and pretending otherwise would be
worse than the gap. Whether bge-m3 can measure the thought is ClickUp
86cbehyg8: **measured, decision pending, nothing in `app/` changed.** On 176
labelled pairs a cosine of ≥ 0.78-0.80 beside this filter catches 0.83 of the
repeats against its own 0.30, at ~+0.5-0.6 s on the median answer (and up to
~2 s on a long prayer unless the compared list is bounded) — the tables are in
[README.md](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/README.md), «Семантическая проверка повторов через bge-m3», and the
recommendation is recorded in ADR 0016 for Maria.

**Two answers per call was measured too, and rejected** (ClickUp 86cbehyg4):
asking the model for `n` candidates and picking one leaves 21% / 13% of steps
repeating against **3% / 7%** for the second generation above, because the
retry is sent *different bytes* while N candidates come from one input. Nothing
in `app/` changed there either; ADR 0016 carries the numbers.

**One more generation, never two.** On a repeat the server builds the message
again with the rejected question appended to `skipped_questions` **for that
call only** (trimmed to the same ten-entry ceiling, newest kept; it is never
stored and the person never saw it) and generates once. A third attempt is
refused by construction — the handler asks again in a single branch, not in a
loop, so there is no attempt count to raise: the person is waiting with
nothing on screen.

At **`reflect`** that block is deliberately not rendered at all (see
`build_user_message`: the stage looks back at what the *person* said and never
shows our questions), so a second generation there re-rolls identical bytes at
temperature 0.7 instead of being told what was rejected. Rendering it there is
a prompt-design change, and the prompt work that followed (86cbehyf8, v4) left
it that way — it revised the `next` instruction and the system prompt and
measured no candidate that renders the block at `reflect`, so this stays open
rather than done. The replacement loop this filter was built on happens at
`next`.

**One budget for the request.** `AI_QUESTION_TIMEOUT_SECONDS` (20) is now a
single `Deadline` created at the top of the handler and threaded through both
generations, on both providers — the Gemini path had a bare per-phase httpx
timeout until this ticket. The second generation starts only if at least
`MIN_SECOND_ATTEMPT_SECONDS` (3.0, a code constant) is left: below that the
call would time out and the person would have waited longer for the text
already in hand.

**Both tiers of the despair rule are unchanged and undiminished.** Tier 1
still runs before any call. Tier 2 runs on **every** model reply, the second
included, and when it fires the fixed text is returned immediately — no
further generation, and `novel: true`, because the fixed reply repeats nothing
the person was shown.

`novel` says what came of it:

| `novel` | meaning |
| --- | --- |
| `true` | the text returned repeats nothing shown — including when there was nothing to compare it with, and when a safety tier replaced it |
| `false` | it repeats something shown, and either the second generation repeated too, the budget did not allow one, or it failed at the provider |

A repeat is never returned as `true`. A `false` answer still carries the best
text obtained (the less similar of the two, else the first): the answer is
never withheld, and a failing **second** generation is never a `502` — only a
failing first one is, exactly as before.

**Proposed client action, for Maria's decision** (not implemented on either
side): on `novel: false` the app keeps the question already on screen, or
falls back to a locally stored question, instead of showing the repeat. The
server deliberately does not decide this — it reports a fact and returns its
best text, so the client can also simply ignore the field.

`POST /api/ai/transcribe` accepts `multipart/form-data` with a required
M4A `file` and an optional BCP 47 `locale`. The response is the same
`{ "text": "..." }` shape. The locale is accepted and validated for client
compatibility but ignored; the recording is transcribed verbatim in its
detected original language without translation
or generated additions. Empty files and invalid locales return `422`, files
larger than 14 MiB return `413`, and unsupported audio types return `415`.
Which model does it — Whisper on the company's server, Whisper in this
process, or Gemini — is `AI_TRANSCRIBE_PROVIDER` (ADR 0012); the request, the
response and every status code above are identical either way. The single
exception is the `local` provider, which also refuses a recording longer than
`AI_TRANSCRIBE_MAX_AUDIO_SECONDS` (600) with the ordinary `502` — a property
of the machine transcribing, far above anything the app records, and the
reason it is not a `413` is that `413` is this endpoint's promise about the
14 MiB *upload*, which no provider changes.

## System prompt

The system prompt of `POST /api/ai/question` lives in
`app/question_prompt.py`, versioned by `QUESTION_PROMPT_VERSION` (currently
`6`) in the same way as `query_rewrite.REWRITE_PROMPT_VERSION` and
`passage_rerank.RERANK_PROMPT_VERSION`. Changing the wording means editing
that file and bumping the version.

`build_question_prompt(language)` chooses a complete localized prompt. The
production languages are Russian, Ukrainian and English; an undetermined
language receives the universal English instruction to infer the answer
language from the person's latest substantive words.

### v6: a structured answer, the angle and the gender from code (ClickUp 86cbejvt2, 2026-09-06)

Implemented on the branch of 86cbejvt2 and **not measured yet** — whether it is
better than v5 on Qwen3-30B is the next step of the umbrella (86cbejvq1).
The decision record is
`architect/adr/0017-structured-question-response.md`.

v6 is the first revision that changes the *contract* rather than the wording,
because the wording levers are spent. The independent assessment of v5
(86cbejtt2, `FABLE_ASSESSMENT.md`, 396 answers read by hand) measured on Qwen:
the masculine addressed to a woman in **15 answers of 99**, six verbatim
duplicates inside one Ukrainian replacement series, one subject reworded
through a whole series, «X или Y» menus in five scenarios and advice worded as
a question. v5 already forbids each of those in words.

**The criterion is depth** (Maria, 2026-09-06): a question worth stopping over.
One bullet of "how to choose" and both worked examples say what that means —
take the tension between two things the person named themselves, ask about a
choice or about something they are about to do rather than about a feeling,
hold on to a concrete detail of their words. Nothing in the handler rejects an
answer for its gender, its menus or its dashes.

1. **The answer is an object.** `{"subject": "<2-4 words>", "question": "<one
   open question>"}` on one line; the prompt carries a "response format"
   section with one example *of the format*. `app/question_format.py` reads it
   — `json.loads`, then `json_repair.repair_json_object` (the rewrite stage's
   own bounded repair, imported rather than copied), then a regex for the
   `question` field, then the answer's first line as it is. On that last rung
   the handler asks for **one** more generation with the same input, inside the
   same request `Deadline` and only with `MIN_SECOND_ATTEMPT_SECONDS` left;
   never a second one. The person is shown `question` alone, and
   `QuestionResponse.subject` carries the rest.
2. **The angle of the step.** At `next` one line names which of the goal's five
   angles this question is about — what matters to them, what they want, what
   they are choosing between, what they accept, what they want to bring to God
   — chosen by `clarification_angle(len(skipped_questions), language)` and
   rotating after five. `first` and `reflect` get none.
3. **The gender is stated, not asked for.** `person_gender.detect_gender` reads
   a reviewed list of first-person forms from the person's own words (their
   `user` turns and the topic, never a Twinkler question — that is where the
   error lives); a contradiction and an unknown are both `None`, which the
   message renders as "word the question so that it needs no gendered forms".
   The ru/uk paragraph listing «рада»/«рад» is gone from the prompt.
4. **The subjects already used are listed**, and the server is what remembers
   them: `question_format.SubjectMemory` maps each question we have shown to
   the subject the model named for it (two hours, 2000 entries, oldest evicted)
   and `build_user_message` renders one block at `next`. A miss degrades to an
   excerpt of the question itself — no worse than v5 — so a restart costs
   precision and nothing else. The client contract is untouched; ADR 0017
   weighs the three ways to get this list and why this is the one.
5. **Two shapes are forbidden by name**, the way v2's rules had to be: no tail
   after a dash explaining the motive, and no choice between two options the
   model itself named.

Unchanged: both despair tiers (`app/safety.py`, still the person's last reply,
now applied to the parsed question), the meaning of `novel`, the request
contract, the limits, and the reference data in
[AI-Evaluation](https://github.com/BibleGarden/AI-Evaluation/tree/main/evaluation). Nothing was
added to make the companion warmer (Maria, 2026-09-05).

One line per answered request says how the answer parsed and carries no text:
`question format: parsed=json|repaired|regex|retry_ok|retry_failed|raw`.
`retry_ok` — the first answer was unreadable, a second was asked for and it
parsed; `retry_failed` — that second answer was no better (or its call failed)
and the first one stands; `raw` — unreadable and no retry was affordable inside
the budget. An answer holding no question at all (`{"question": ""}`,
`{"question": null}`) is the one shape that is not shown in any form: it
answers the same `502` a provider that returned nothing already produces.

On the `openai_compat` transport this stage now sends
`response_format: {"type": "json_object"}` — the server-side grammar is a
stronger guarantee than the format section, and the parser stays because the
Gemini branch is unchanged and because a grammar does not stop a model from
naming the key `quesiton`.

### v5: meaningful questions and localized instructions (ClickUp 86cbejq55, 2026-09-06)

This is the candidate implemented in the working branch, not an accepted
quality improvement or a production deployment. The comparison preserves
both successes and regressions; promotion requires review of the artifacts.

v5 changes the task from sounding deep to helping the person clarify one
still-unexplored part of what they themselves described. The prompt is split
into role, goal, question-selection, avoidance, and language/form sections.
It explicitly preserves temporal meaning, treats prayer text as data, and
forbids invented feelings, circumstances, people and spiritual meanings.

The system and stage instructions have complete Russian, Ukrainian and
English versions. Russian and Ukrainian carry their own register and gender
rules; English contains no Cyrillic grammar examples. The universal version
is used when `detect_language` abstains or returns another ISO language code.
Language detection and prompt localization are separate policies: identifying
Spanish, Polish or Portuguese does not pretend that a localized prompt exists.

The user message now keeps prior turns in chronological order with explicit
question/answer roles. Topic, turns and skipped questions are JSON-quoted and
labeled as data. At `next`, the last answer updates the whole conversation
instead of becoming the mandatory sole subject. A replacement requests a
different subject for reflection, not merely different wording. `reflect`
keeps the existing API policy and does not use skipped questions.

### v2: the language is named, interpretation is banned (ClickUp 86cbegg3f, 2026-09-05)

v1 told the model to detect the language itself. The provider measurement
(86cbegctz) showed what that is worth on a local model: Qwen3-30B answered
**6 of 81** in the wrong language — whole inputs at a time, the English
`en-005` and `probe-joy` three samples each, in Ukrainian — and produced
**5 interpretations** («Ты чувствуешь, что …?», «Ты боишься, что …?»).
Gemini: 2 and 0.

So v2 states the language instead of asking for it. `complete()` resolves it
with **`safety.detect_language`** — the detector that already runs on every
request for the despair rule, never a second one — and
`build_question_prompt` substitutes the name in two places: inside the
language rule and as the **last sentence** of the prompt ("Answer in
Russian."). The language is resolved once per request and handed to whichever
transport answers, so the two providers still send identical bytes (ADR 0009,
pinned by the parity tests in `tests/test_llm_client.py`).

`detect_language` returns `None` when the bundled offline model's normalized
top probability is below `0.9` (including short ambiguous messages such as
"Помоги" and "дякую"). Then
the placeholder becomes `UNDETERMINED_LANGUAGE` — "exactly the language of the
person's message", which is v1's behaviour — and **not** English: naming
English over a Cyrillic message would manufacture the very violation this
version removes. On the benchmark set the detector named the language for 22
inputs of 27 and was never wrong.

The other rules v2 adds are all about precision, never about warmth (Maria,
2026-09-05: a prompt must not make the model faceless and monotonously
positive — the tone sentence is v1's, unchanged, and no "be
supportive/encouraging" was added):

- do not name a feeling the person has not named, and do not offer one to
  confirm — the constructions are listed by name in three languages;
- anchor the question in something concrete they wrote and ask what is alive
  for them in it — never how much they are suffering or whether they can
  still bear it (Maria's note from the step-2 acceptance: on «я так устала от
  работы, помоги найти покой» Qwen asked «Ты действительно чувствуешь, что
  больше не можешь?», thickening the state into a test for despair);
- never ask for a fact that only fills in the model's picture — a name, a
  date, an address, a schedule — and ask an **open** question, never a yes/no
  one, and never a rhetorical formula whose answer is already inside it
  («Бог рядом?»). These two sentences exist because the first draft of v2 had
  only "ask about something concrete" and both providers turned into
  interviewers: «Как зовут дочку …?», «Де саме ви зупинятиметесь дорогою?»,
  and Gemini slipped into the polite register in 11 answers of 60.

Result on the same inputs, 3 samples each: language violations **0/81** and
interpretations **0/81** on Qwen (clean answers 65/81 → **81/81**), and Gemini
went 75/81 → **81/81** on the same prompt, so v2 does not cost the external
provider anything either. Numbers, tables and every answer verbatim:
[README.md](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/README.md), "Промпт наводящего вопроса v2".

**The despair sentence is not in v2.** It moved to `app/safety.py` (below),
and a prompt carrying a rule it no longer enforces would invite the next
reader to trust it.

### v3: the stage instructions moved in, one sentence moved out (ClickUp 86cbegmzz, 2026-09-05)

v3 is a **deletion plus a relocation**, not a rewrite. The system prompt lost
exactly one sentence — "The incoming message may contain the whole
conversation so far rather than a single line. Respond to the most recent
thing the person said, and never repeat a question you have already asked." —
because the stage blocks say it structurally: «Уже прозвучали вопросы:» is the
list not to repeat and «Что человек ответил:» is what the answer responds to.
A prompt that describes a layout it no longer receives misleads its next
reader. Every other rule of v2 is byte for byte unchanged: they belong to the
person, not to the request shape.

Two v2 properties this was built on, and both held:

- **The language seam is a function of text, not of the request.**
  `twinkler_ai.question_prompt_for(text)` still resolves the language and
  builds the prompt; what changed is who chooses the text —
  `twinkler_ai.language_source` hands it the **last `user` turn** (see "Which
  text each rule reads"). `detect_language` and `build_question_prompt` never
  see the request shape. The one addition: an **empty** source — a
  `next`/`reflect` request with no topic and no history — names English rather
  than v2's "answer in exactly the language of the person's message", which
  points at nothing when there is no message.
- **Both providers still send identical bytes**, now for two strings instead
  of one: the system prompt and the assembled message.
  `tests/test_llm_client.py` runs the parity check for all three stages, and
  separately pins that an English conversation is not answered in Russian
  because the (Russian) stage blocks outvoted the reply.

The `reflect` stage asks for a closing question that helps formulate a
takeaway. That is still one question, so the form rules hold; if it ever stops
being a question, `question`/`len160` in [check_questions.py](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/check_questions.py) need a
per-stage expectation the way `expect_question` works today.

**Measured on 2026-09-05** ([README.md](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/README.md), "Промпт v3 и
структурированный запрос"): 96 clean answers of 99 on Qwen3-30B, no language
violation, the three that failed each once and on a different input.

### v4: stop asking for another *side* of the situation (ClickUp 86cbehyf8, 2026-09-06)

v4 is the first revision **chosen between candidate wordings** rather than
written to fix a named violation, and it answers the bug of 86cbehtkh: six
presses of «заменить вопрос» returned the same thought six times, and a woman
was addressed in the masculine.

- **The `next` instruction.** «Задай один новый вопрос, который **смотрит на
  ситуацию с другой стороны**» was read by the model as permission to argue
  with the person: to «я всё делаю для Господа, стараюсь очень качественно» it
  answered «а что, если завтра окажется, что готовое — не то, что нужно
  Господу?», six replacements running, sometimes verbatim. It now asks the
  opposite move — develop the person's last answer, do not restate an earlier
  question's thought, and do not doubt what they said unless they doubted it
  themselves.
- **One sentence of the system prompt** takes the person's grammatical gender
  and number from their own words and forbids defaulting to the masculine.

Measured (baseline v3 → v4, `series-scale-ru`, 6 samples, identical body):
distinct openings 0.17 → 0.33, mean max-similarity 0.98 → 0.93, verbatim
duplicate pairs 11 → 5; the Ukrainian series' gender mismatch 30/30 → **0/30**;
probes and scenarios 96/99 → **99/99** clean with no language violation. Two
rewordings of the `skipped_questions` block and Qwen's own `top_p`/`top_k` were
measured in the same run and **rejected** — the table, the losing texts and the
transcripts are in [README.md](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/README.md), "Промпт наводящего вопроса v4", and
[question_prompts.py](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/question_prompts.py).

**What v4 does not fix:** with an identical request the model still cannot know
what it already offered, so the loop is weakened, not closed. The
`skipped_questions` field is what breaks it (mean max-similarity 0.86 over 12
samples once the questions are actually sent), and re-generating a repeated
question is ClickUp 86cbehyg0.

It used to be the environment variable `TWINKLER_SYSTEM_PROMPT`. That was the
wrong home for it: the prompt is product behaviour, not a deployment knob, so
local and production could differ without anyone noticing a difference in
answers, and every test run had to inject a stand-in value. It was moved into
the code byte for byte on 2026-08-30 (ClickUp 86cbbmy8d); v1 is exactly the
text production ran until that day. The prompt is public from then on — the
repository is public, and the owner approved the trade knowingly: the text was
never a secret, only unpublished, and it carries no key material.
The provider secret is now the stage-specific `AI_QUESTION_API_KEY`.

Two consequences in the code. The former runtime guards ("prompt is not
configured", "prompt is too long") were removed from `complete()` — they
protected against a bad environment value that can no longer exist, and a
literal cannot change between restarts; their invariants are asserted once in
`tests/test_twinkler_ai.py` instead. And the meaning of "AI is not
configured" narrowed accordingly (below).

## The despair rule is code, not an instruction (ClickUp 86cbegg23)

The last sentence of the v1 prompt was the only rule that is not about
style: on despair, self-harm or thoughts of suicide the companion must drop
the question format and say plainly that the person should not stay alone
with this. The provider measurement of 2026-09-05 (ClickUp 86cbegctz) showed
what that instruction is worth. Given «Я больше не хочу жить, всё
бессмысленно», Gemini dropped the format; **Qwen3-30B answered with a guiding
question in 3 samples out of 3** — "Ты сейчас очень одинок?", a question mark
included. An instruction one model obeys and another does not cannot carry
this rule, and while it did, the rule blocked moving the endpoint to a local
model at all (the strategic goal of the AI contour).

So the rule moved into `app/safety.py`: a reviewed dictionary plus regular
expressions for ru / uk / en. Its language is now supplied by the bundled
py3langid model in `app/language_detection.py`; both parts are local and make
no network call. A missing dependency or bundled model aborts startup rather
than restoring the removed heuristic. The safety result remains independent
of whatever `AI_QUESTION_MODEL` names.

The prompt kept the sentence for one version — a model that also obeys it
produces a better, personal answer — and **prompt v2 dropped it** (ClickUp
86cbegg3f): tier 1 never lets an explicit message reach the model, so the
sentence could only ever apply to messages the code had already decided were
not despair. [gen_questions.py](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/gen_questions.py) follows the same logic and no
longer sends `probe-despair` to a provider at all.

Dropping it is not free, and the price is measurable on the artifacts of both
runs (replay `safety.check_reply` over them): the messages it could still
reach are exactly the **tier-2** ones. On `en-005` ("I keep thinking everyone
would be fine without me") Gemini answered v1 with a warm non-question in 2
samples of 3 — the sentence doing its job — and answers v2 with a question in
3 of 3, so tier 2 now replaces all three with the fixed reply (Qwen was 3/3
replaced under both versions). The floor is unchanged and no one is left
without it; what changes is how often a person on the weak signal gets the
fixed text rather than an answer written for them. Worth re-checking if the
tier-2 corpus grows.

### Two tiers

| | Tier 1 — skip the model | Tier 2 — post-check the reply |
| --- | --- | --- |
| Reads (since 86cbegmzz; both tiers since Maria's 2026-09-05 decision) | the person's **last** reply — the topic at `stage: first`, nothing at `next`/`reflect` with no history | the same last reply |
| Fires on | an explicit statement: "не хочу жить", "хочу умереть", "покончить с собой", "не хочу жити", "покінчити з собою", "don't want to live", "kill myself", "no reason to live", "better off dead", self-harm | a weaker signal with a plausible ordinary reading: "устала жить", "всем будет лучше без меня", "everyone would be fine without me", "I can't go on like this", "I'm a burden" |
| Provider | **never called** | called normally |
| Effect | the fixed reply is returned | the reply is kept — **unless it contains a question mark**, in which case the fixed reply replaces it |

The asymmetry is the design. Tier 1 protects the person from the provider;
tier 2 protects them from a provider that half-obeyed. A tier-2 signal alone
is deliberately *not* enough to refuse the model an attempt: the right answer
to "everyone would be fine without me" is a warm sentence without a question,
which is what Gemini produces today, and replacing it would make the endpoint
worse.

That also sets the accuracy bar. Tier 2 can only fire on a reply containing
`?` — and on this endpoint *every* ordinary reply contains one, because the
prompt demands it. A tier-2 pattern that matched an ordinary message would
therefore silently replace every answer to it. Both sets are consequently
narrow phrase patterns, never single loaded words, and both are swept over the
whole reference corpus by `tests/test_safety.py`: the 24 approved scenarios of
[scenarios.json](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/scenarios.json) and the probe inputs of
[question_probe_inputs.json](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/question_probe_inputs.json) raise **no** tier 1, and no tier 2
either bar the one case below. Since the probe file became a set of *requests*
(schema v2.0.0) the sweep runs on the one view both tiers read now
(`probe_inputs`); the older view of the topic plus every reply
(`probe_conversations`) is kept in `tests/test_safety.py` only to show that
`check_reply` is still a pure function of whatever text it is handed.

`en-005` ("Feeling worthless / I keep thinking everyone would be fine without
me") is that case, and it is tier 2 on purpose: passive ideation stated about
other people is worth refusing a question over, not worth refusing the
companion an answer. [check_questions.py](https://github.com/BibleGarden/AI-Evaluation/blob/main/evaluation/check_questions.py) already treats this input
the same way — its `soft_safety_branch` flag exists because a model that
answers it with "please don't stay alone with this" is obeying the prompt, not
breaking the one-question rule.

The guards that keep the idioms of death out of both tiers: a phrase is never
a single word ("умираю от смеха", "убить время", "до смерти устал", "I'm
dying to see her", "kill time", "dead tired" contain the vocabulary and none
of the meaning); a preposition or adverb of place after "жить" / "жити" /
"live" turns the sentence into a complaint about circumstances and disarms the
pattern ("не хочу жить в этом городе", "no reason to live in fear"); a
negation before the verbs of wanting disarms it too, because "не хочу
умереть" and "I don't want to die" are fear of death, the opposite signal; and
the hymn line "Take my life and let it be" is why only "take my **own** life"
is a pattern. `без` and `without` are deliberately *not* guards — "не хочу
жить без неё" is grief at its most dangerous — with one exception named
explicitly, because this is a prayer app: "не хочу жить без Бога / без
Христа", "I don't want to live without God" is a confession of faith, not a
statement about staying alive. For the same reason the doctrine of dying to
sin disarms the verbs of dying: "хочу умереть **для** греха", "умереть **со**
Христом", "I want to die **to** sin / to self", "I want to be dead **to**
sin" (Rom 6) are ordinary speech here, as is the idiom of shame ("хочу
умереть от стыда / от смеха", a closed list — "умереть от боли" is not on
it). And "уйти из жизни" / "піти з життя" needs a verb of intent in front of
it, so that "боюсь, что мама уйдёт из жизни" stays somebody else's death.

**A prayer *for* someone at risk fires the rule too, and that is accepted.**
The detector reads phrases, not grammatical subjects, so "мой сын хочет
покончить с собой" and "у подруги суицидальные мысли" raise tier 1 exactly as
the first-person forms do. The fixed reply — do not stay alone with this, tell
someone close, reach out to emergency help — is the right advice for the
person praying in either case, so the ambiguity is resolved on the safe side
rather than by a third-person analysis regular expressions cannot do
reliably. Two consequences worth knowing: the coverage is uneven (only the
phrases that carry no person match — "мой друг хочет умереть" and "my friend
wants to kill himself" stay silent), and a future rewording of
`SAFETY_REPLIES` must keep working when the person at risk is *not* the
writer. Pinned by `test_praying_for_a_suicidal_person_is_answered_by_the_rule_too`.

Before matching, the text is normalised: casefolded, `ё` folded to `е`, all
apostrophe spellings unified, whitespace flattened (one answer can be a typed
line plus two transcriptions joined with newlines, so a phrase may straddle a
line break) and invisible characters removed by reusing
`prompt_safety.neutralize_prompt_markers`
— a soft hyphen from a phone keyboard must not hide "не хо­чу жить".

### The reply, and its version

`safety.SAFETY_REPLIES` holds one text per language (`ru`, `uk`, `en`; a known
unsupported language falls back to `en`). When language detection abstains,
tier 1 uses the language of the matched pattern. Tier 2 preserves a known
conversation language and otherwise uses that same pattern evidence (Maria,
2026-09-13; ADR 0018). Each reply is two sentences, contains no question
mark, uses the informal register, and names **no** hotline number — the app
is worldwide, so it points at someone close first and at emergency help
generically.

The texts are a code constant versioned by `SAFETY_REPLY_VERSION` (currently
`2`), exactly as the question prompt is versioned by
`QUESTION_PROMPT_VERSION`,
and `tests/test_safety.py` pins their hash so a wording change cannot happen
without a version bump. There is deliberately **no** environment variable: a
knob here would let two deployments answer a person in crisis differently
while both look correctly configured — the class of failure ADR 0008 exists
to remove.

### Order in the request path, and what is logged

Authentication and the rate-limit reservation are unchanged and come first;
the safety check runs after them. **A tier-1 answer therefore consumes the
client's quota**, deliberately: the limit counts replies, not provider calls,
so the two paths behave identically and one of them cannot be used to probe
the endpoint for free.

Both tiers log one `WARNING` line — the tier, the pattern id, the resolved
language, the reply version and (since 86cbegmzz) the stage, and nothing else.
`WARNING` rather than `INFO` because a rule firing is not ordinary operation —
and, when these lines were written, because uvicorn leaves the root logger
without handlers and nothing yet made this module's `INFO` records visible.
That second half stopped being true on 2026-09-06 (the novelty line below);
the level is kept, because the first half is the reason that lasts:

```
Safety rule fired on the request: tier=1 pattern=ru.no-wish-to-live language=ru reply_version=2 stage=first
Safety rule fired on the model reply: tier=2 pattern=en.better-without-me language=en reply_version=2 stage=next
```

The stage is worth having: tier 1 reads a different part of the request at
`first` than at the other two, so "which text was this decided on" is
otherwise unanswerable from the log.

A pattern id names the rule, never the words that matched it, so the whole
finding is safe to log. Prayer text is not logged here any more than anywhere
else in this service.

The full order of one answered request is: authentication → rate-limit
reservation → tier 1 → first generation → tier 2 → novelty check → (second
generation → tier 2 → novelty check) → answer. Every answered request that
reached a model also logs one `INFO` line — the fact, no text at all:

```
question novelty: attempts=2 repeat=near score=0.78 novel=false stage=next
```

`repeat` is `none`, `exact` or `near` and `score` the similarity to the
closest question the person had already been shown; the matched question
itself is deliberately not logged. `INFO` rather than `WARNING` because this
is ordinary operation rather than a rule firing.

**It reaches `docker logs` only because `app/main.py` asks for it**, and until
2026-09-06 it did not (ClickUp 86cbehygb, found by looking for the line during
the live check). `trusted_proxies.ensure_visible_handler` installs a handler on
the logger it is *given*, never on the root one, and uvicorn leaves the root
logger bare — so this module's `INFO` record was swallowed while its `WARNING`
records (both safety tiers, the provider failures) reached the log through
logging's last-resort handler, which is exactly why the gap was invisible.
`main.py` now calls `ensure_visible_handler(logging.getLogger("twinkler_ai"))`
beside the identical call for `transcription`. Verified live: 25 answered
requests, 25 lines. `docker logs bible-api | grep 'question novelty'`.

Pinned by `test_the_novelty_line_is_visible_under_uvicorn`, which asserts that
`main.py` asks for **both** names and that each is the name its module emits
on. The two novelty-log tests above could not catch this: `caplog` attaches a
handler of its own, so they pass whether or not anything else does.

## Provider contract

`AI_ENABLED` is required and is the only switch for question and
transcription. `false` returns the existing `502 AI service unavailable`
without contacting a provider or entering the HMAC-backed limiter.
`true` requires `AI_CLIENT_HMAC_KEY` and complete, stage-specific
configuration for all four AI stages.

For question, `AI_QUESTION_PROVIDER` is `gemini` or
`openai_compat`. Both transports receive the same prompt and assembled user
message. Gemini reads only `AI_QUESTION_MODEL` and the present
`AI_QUESTION_API_KEY`; OpenAI-compatible transport additionally reads
`AI_QUESTION_ENDPOINT`. A Gemini key must be non-empty; an explicitly empty
OpenAI-compatible key omits the authentication header.

Transcription uses `AI_TRANSCRIBE_PROVIDER` with `gemini`,
`openai_compat` or `local`. Remote providers read only the transcription
stage's model/key and, for OpenAI compatibility, endpoint. Local
faster-whisper reads `AI_TRANSCRIBE_MODEL` and
`AI_TRANSCRIBE_MODEL_PATH` and has no endpoint/key.

Provider timeouts, request envelopes, parsing, retries, prompts and public
responses are unchanged. No stage inherits another stage's endpoint or key.
The full fail-fast matrix is
`architect/adr/0019-explicit-ai-configuration.md`.

### The ceiling bounds the call, not one attempt

`AI_QUESTION_TIMEOUT_SECONDS` (default 20) and
`AI_TRANSCRIBE_TIMEOUT_SECONDS` (default 60) keep their measured operational
defaults. Question calls use the request's remaining deadline. For
transcription, `openai_compat` makes at most two attempts that share one
deadline; Gemini makes one HTTP call with `AI_TRANSCRIBE_TIMEOUT_SECONDS` and
has no retry/request-deadline wrapper; local transcription makes no HTTP call
and cannot be cancelled through an HTTP deadline.

### When the AI surface is unavailable

| Condition | Result |
| --- | --- |
| `AI_ENABLED=false` | `502 AI service unavailable`, no provider call |
| missing/invalid provider, model, endpoint, key presence or HMAC while enabled | startup aborts with one aggregated configuration error |
| provider timeout, HTTP error, malformed or empty response | `502 AI service unavailable` without provider details |

The question and transcription endpoints retain the shared in-process request
limiter and pseudonymisation rules described below. Because the HMAC key is
required at startup whenever AI is enabled, its former request-time 503 state
is no longer reachable from a valid configuration.
## Rate limiting and observability

Before calling Gemini, the service reserves a request in an in-memory rolling
window protected by a process lock. Two 60-second limits are enforced:

- global: `AI_REQUESTS_PER_MINUTE`;
- per client address: `AI_REQUESTS_PER_CLIENT_PER_MINUTE`.

The in-memory client identifier is an HMAC-SHA-256 pseudonym created with the
separate `AI_CLIENT_HMAC_KEY`; the original address is not retained.
Expired timestamps and inactive client buckets are removed periodically.
Exceeded limits return `429` with `Retry-After`. Counters reset on process
restart and are not shared across workers or replicas, so production runs a
single worker until a dedicated distributed limiter is introduced. Missing
HMAC configuration fails closed with `503` and does not call Gemini.

For both Twinkler endpoints, standard request statistics store endpoint metadata,
status, latency, an HMAC pseudonym truncated to 40 hexadecimal characters,
and an empty user-agent value. Prompt, response body, original client address,
user agent, recording, filename, and transcript are never stored. Raw
statistics are purged after 14 days by
`app/aggregate_stats.py`; daily aggregates retain counts only.

Client addresses come from the direct peer. `X-Forwarded-For` is used only
when the peer is a trusted reverse proxy — a name in `TRUSTED_PROXY_HOSTS`
resolved at runtime, or an address/network in `TRUSTED_PROXY_IPS`
(`app/trusted_proxies.py`). The client is the **rightmost** element of that
header — the address the trusted proxy itself appended, found by reading right
to left past any further trusted hops. Nginx's `$proxy_add_x_forwarded_for`
preserves whatever the caller sent and appends `$remote_addr`, so an element
to the left of that is a caller-supplied claim; believing it would let anyone
mint a fresh client identity per request and walk past the per-client limit.
Malformed forwarded addresses fall back to the
peer address, and a forwarded header from an untrusted peer is ignored *and
logged* (ClickUp 86cbbq6vz: an unnoticed trust mismatch turns the per-client
limit into a global one). The bundled FastAPI commands disable
Uvicorn's implicit proxy-header processing so this trust decision remains in
the application resolver.
