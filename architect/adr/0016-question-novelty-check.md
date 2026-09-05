# ADR 0016: A repeated question is caught in code and generated once more

Status: accepted (2026-09-06).
Ticket: ClickUp 86cbehyg0, child of 86cbehxm2 (parent bug 86cbehtkh).
Follows ADR 0015 (`skipped_questions`), which told the model about the
declined questions, and ADR 0008 (no silent defaults) for why the two
constants here are code.

## Context

Pressing "replace question" now sends `skipped_questions` (ADR 0015), so the
model is *told* what was declined. Telling it is not making it obey. The
replacement-series baseline measured immediately afterwards
(`evaluation/bench_data/questions_qwen30b_v3_series.jsonl`, ClickUp 86cbehyez)
shows Qwen3-30B answering six replacements of one prayer with six variants of
one sentence — the last five differ from the first only in the tail, «— как ты
будешь узнавать это?» and its cousins. The person presses "replace" and is
handed the same question back.

A prompt cannot be trusted with this for the same reason the despair rule is
not a prompt sentence (ADR of `app/safety.py`, ClickUp 86cbegg23): it is a
property of the answer, checkable in code, and a model that ignores the
instruction ignores it silently.

## Decision

Three parts, all in `app/question_novelty.py` and the handler.

### 1. A lexical repeat filter

`is_repeat(candidate, shown)` where `shown` is the `assistant` turns of the
request plus its `skipped_questions` — everything the person has *seen* this
prayer. Their own replies are excluded: a question is not a repeat of an
answer.

- **Exact**: equal after `normalize` (casefold, ё→е, punctuation and quotes
  dropped, whitespace collapsed).
- **Near**: Jaccard over character 3-grams ≥ **0.60**, or a shared opening of
  ≥ **4** normalized words covering ≥ **0.7** of the shorter question.

The metric is `evaluation/check_questions.py`'s `normalise_series_text` /
`trigram_similarity`, reused character for character and pinned by a test, so
the number the benchmark prints about a series and the filter that runs in
production are one measurement rather than two definitions that drift.

The threshold and the prefix rule were chosen on the artifact above; the table
is in the module docstring and reproduced by `tests/test_question_novelty.py`
against the artifact itself. They are **code constants, not environment
variables**: which question a person is shown is product behaviour, and a
deployment able to move a threshold quietly would make two installations
answer the same prayer differently while both look correctly configured.

The **prefix rule is a share, not "the first four words"** — the shape the
ticket proposed. Qwen opens every Ukrainian question of the artifact with «А
що б ти зробив, якби …» (six words) and most English ones with «What would it
feel like to say yes, knowing …» (nine), so plain prefix equality flags nearly
every pair in two of the four series, including questions offering plainly
different things. Requiring the shared opening to cover most of the shorter
question leaves a fixed frame unable to decide on its own: on the 180 pairs of
the three non-loop series it fires on **3**, and all three are the same
sentence with the tail swapped.

### 2. Exactly one more generation

On a repeat the handler rebuilds the user message with the rejected question
appended to `skipped_questions` for that call only — trimmed to the request's
own ten-entry ceiling, newest kept — and generates once. Never twice.

Why not more: the person is waiting with nothing on screen, and the second
attempt already doubles the worst case. Why not zero (i.e. only report the
repeat): the client would have nothing to show, and the measured series say a
resample usually *does* move — 5 of the 6 replacements in the loop series were
at least reworded.

The whole request — both generations — runs under **one** `Deadline` of
`AI_QUESTION_TIMEOUT_SECONDS`, created at the top of the handler. The second
generation starts only when `MIN_SECOND_ATTEMPT_SECONDS` (3.0) remain, because
`gemini_retry.provider_timeout` gives the read phase three quarters of what it
is handed and a question needs more than a couple of seconds to arrive. This
also closed a latent hole on the Gemini transport, which handed httpx a bare
number: httpx applies a bare timeout to each of its four phases, so a 20 s
ceiling authorised 80 s for one call and would have authorised 160 s for a
request that generates twice.

### 3. An additive `novel` field

`true` when the returned text repeats nothing shown — including when there was
nothing to compare it with, and when a safety tier replaced the text (a fixed
reply repeats nothing). `false` when it repeats something shown and the second
generation repeated too, was unaffordable, or failed at the provider.

- A repeat is **never** returned as `true`.
- A `false` answer still carries the best text obtained (the less similar of
  the two, else the first). The answer is never withheld, and a failing
  *second* generation is never a `502` — only a failing first one is.
- Both candidates are scored against the same `shown` list, never against each
  other: the rejected text was never on screen, so comparing them on it would
  rank two questions by a similarity the person will never see.
- The field is on a `QuestionResponse(CompleteResponse)` subclass rather than
  on `CompleteResponse`, which is also the response of `/api/ai/transcribe`:
  novelty is meaningless for a transcript, and an always-`true` field there
  would be noise in a contract every client reads.

**The client action is a proposal, not a decision** (`architect/twinkler-ai.md`
records it for Maria): on `novel: false`, keep the question already on screen
or fall back to a local one. The server reports a fact and returns its best
text, so ignoring the field is a valid client.

## What this deliberately does not do

**It does not measure semantic diversity.** Two questions sharing no wording
can be one thought — in the very same series, question 2 and the loop that
followed it are one idea in several dresses, and a differently worded return
to it passes untouched. Whether bge-m3 can measure the thought rather than the
letters is ClickUp 86cbehyg8; claiming it here would have been the more
expensive mistake, because a "novelty check" that misses the thought is
trusted for something it never did.

**It does not touch the prompt.** Prompt v4 is ClickUp 86cbehyf8 and lands
separately; this filter is the floor under whatever v4 achieves, not a
substitute for it.

**It does not weaken either despair tier.** Tier 1 still runs before any
provider call; tier 2 runs on every model reply the retry produces, and its
fixed text is returned immediately when it fires.

## Consequences

- A repeated question costs one extra generation. The worst case latency of
  the endpoint is unchanged — it was always the budget, and the budget is now
  actually enforced on both transports.
- Logs gain one `INFO` line per answered request:
  `question novelty: attempts=2 repeat=near score=0.78 novel=false stage=next`.
  No text, no topic, not even the matched question.
- The threshold has no headroom to spare: the closest pair judged *different*
  in the measured material scores 0.500 against the 0.610 of the loop. A
  mistake in either direction costs at most one generation, never a wrong
  answer — but a materially different model (another provider, another prompt
  version) should have the table redone rather than assumed.
