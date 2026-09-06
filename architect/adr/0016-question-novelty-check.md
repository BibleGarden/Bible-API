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
own ten-entry ceiling, newest kept — and generates once. Never twice; the
bound is structural (one `if`, no loop), not a constant to raise.

One honest gap: at `reflect` the skipped block is not rendered at all
(`build_user_message`, ADR 0015 — that stage looks back at what the *person*
said), so a second generation there is a re-roll of identical bytes at
temperature 0.7 rather than an informed retry. Rendering it there is a
prompt-design change (ClickUp 86cbehyf8); `next` is where the replacement loop
was measured.

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

## Re-measured on prompt v4 (2026-09-06, same ticket)

The prompt version this warned about arrived the same day. Redone on the three
v4 artifacts (`questions_qwen30b_v4b_series{,_accum,_accum_r2}.jsonl`, 810
within-series pairs; the table is in the module docstring), the headroom is not
small — it is gone. v4's `next` instruction tells the model to unfold the
person's own last reply, so every answer of a series sits on one frame built
from the same words, and the metric now measures the frame: reworded repeats
run from 0.33 to 0.97 and the pairs a reader calls genuinely different run from
0.05 to 0.60, interleaved. What 0.60 buys on v4 is **no false positive at all**
in the material read (0/13 different pairs flagged) and a miss on 75 of the 155
reworded repeats above 0.45 — it is a floor under the worst repeats, not a
detector of the loop. Replayed as the endpoint runs it, it fires on 34/126
steps with the identical body and 42/252 with the accumulating client, i.e. it
still earns the retry it costs. The constants stay: every candidate that buys
more repeats (0.55, 0.50) starts paying false positives immediately, and the
class of miss it leaves — the same thought in a new dress — is exactly what
this ADR said a lexical metric cannot see (ClickUp 86cbehyg8).

## Candidates: measured, decision pending (ClickUp 86cbehyg4, 2026-09-06)

The alternative to "one more generation" is "N answers from one generation":
`n` of the OpenAI-compatible chat API, which vLLM serves from a single prefill,
with the pick made server-side. It was measured against this ADR's mechanism —
`evaluation/gen_questions.py --candidates N` and `--retry-on-repeat`, prompt v4,
Qwen3-30B, the four replacement series x 6 samples, both client modes, six
artifacts `bench_data/questions_qwen30b_v4_cand_*.jsonl`. Nothing in
`app/` changed: the endpoint still does exactly what the Decision section says.

**The measurement.** Steps still ending `novel: false` out of 126: this ADR's
retry **4 (3%)** with the identical body and **9 (7%)** with the accumulating
client, against 26/16 for `n=2` and 22/10 for `n=3`. Counted only over the
steps where the first answer *was* flagged, the retry escapes the repeat in
90% (identical) and 53% (accumulating) of them, `n=2` in 37%/48% and `n=3` in
61%/58%.

**Why, and it is not a tuning matter.** The second generation is sent
*different bytes* — the rejected question is in `skipped_questions` — while N
candidates come from one and the same input, so a model that has settled into a
loop on that input returns the loop N times. The `n=3` transcript of the journal
series has steps 2-6 differing by a single word.

It is the input, **not** the shared prefill (measured on review): sampling per
choice is independent, and in the identical-body mode every sample of a series
step sends the same bytes, so answers from *separate* calls are the control for
candidates of one call. Mean (median) pairwise similarity within a call 0.417
(0.383) at `n=2` and 0.405 (0.350) at `n=3`; between calls on the same bytes
0.405 (0.349) and 0.407 (0.350) — indistinguishable. All N come back
byte-identical on only 6.2% / 1.2% of the steps (7.4% / 6.2% accumulating), so
`n` does deliver genuinely different samples. That sharpens the conclusion
rather than softening it: what buys novelty is a changed input, not more draws
from the same one — and it is also the case FOR the reinforcement below, whose
extra draws would sit behind a changed input.

Latency decides nothing: the worst step of all six runs is **1.17 s** against
the 20 s budget. Tokens favour candidates on the total (a repeat costs the
retry a second 872-token prefill; `n` pays one) and disfavour them on decoding,
which is the half that is spent on every step rather than on the ~1 in 4 that
repeat.

**Selection rule, if it is ever adopted**: drop what despair tier 2 would
replace, drop what `is_repeat` flags, take the **first survivor in the model's
order**, and fall back to the least similar with `novel: false`. "Most distant"
was rejected as a rule and measured as a counterfactual: the two would have
disagreed on 223 of 648 steps, with a median similarity gap of **0.038** — a
coin toss dressed as a criterion — and the formal checks cannot tell them apart
(31 differences, 29 of them the documented `informal` «вы»-to-the-couple class).

**Recommendation recorded, not enacted: do not adopt.** Worth revisiting only
as a *reinforcement* of the retry (the second call asking for `n=2`), or once a
semantic filter (ClickUp 86cbehyg8) gives the selection something real to
choose on. The numbers and the transcripts are in `evaluation/README.md`,
«Несколько кандидатов за вызов против повторной генерации».
