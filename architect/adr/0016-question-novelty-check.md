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

## Semantic check: measured, decision pending (ClickUp 86cbehyg8, 2026-09-06)

The question this ADR left open has been measured and **not** implemented:
nothing in `app/` changed, and `POST /api/ai/question` still answers exactly as
it did. What exists is the evidence for a decision — the labelled set
`evaluation/question_pairs_labelled.json` (176 pairs, ru 83 / uk 51 / en 42,
116 `repeat` / 60 `different`), the tool `evaluation/question_semantic_bench.py`
that scores each pair three ways, and the table in `evaluation/README.md`,
«Семантическая проверка повторов через bge-m3».

**bge-m3 cosine sees the thought where the trigram score sees the frame.** On
the same pairs, the classes overlap by 0.525 on trigrams and by 0.179 on
cosine (0.109 with the arguable pairs removed) — so neither signal separates
them perfectly, but only one of them is usable. Against the 0.97 precision /
0.30 recall of the filter as it stands, cosine ≥ 0.80 gives **0.96 / 0.83**,
and its best F1 (0.92, at 0.76) is twice the filter's 0.46. The 52 repeats
whose trigram score is below 0.45 — the band the filter never reads — include
36 that cosine ≥ 0.80 catches.

The one class it does not catch is a repeat rewritten with no shared words at
all: on the artifact pairs 0.80 recalls 0.89, on the hand-written ones 0.50.
That is the honest ceiling of the number, and it is the right way round — the
repeats a model actually produces are the ones being filtered.

**It would be a second signal, never a replacement.** The rule to implement,
if it is implemented, is `is_repeat(candidate, shown) or cosine >= 0.80`. The
OR adds one catch over cosine alone on this set, which is not why it is there:
the lexical branch costs nothing, needs no network, and is the whole check
when the embedding provider is down — and an `EmbeddingUnavailable` must
degrade to today's behaviour, never to a `502` for someone waiting on a
question. The threshold would be a code constant for the same reason 0.60 is
(ADR 0008), and this table would have to be redone under another prompt
version or another embedding model, exactly as the lexical one was.

**The cost is latency, and it is the real argument against.** One batched
call to `https://llm.ai2.ru/v1/embeddings` carrying the candidate plus the
questions already shown is 549 ms median / 708 ms p90 for a batch of twelve,
measured from this machine (904 / 1008 back to back), with no cold start — the
API process holds no weights. Sequentially after the generation, the whole path
measures 1002 ms median against 397 ms for the question alone: **the check
roughly doubles a median answer**, while using 3–5% of the 20 s budget. The
worst case, a repeat plus its one retry, is ~1.5 s.

Twelve is the typical prayer and **not** the ceiling (review of this ticket):
`shown` is the `assistant` turns of `messages` plus `skipped_questions`, and
`MAX_MESSAGES` is 40 against `MAX_SKIPPED_QUESTIONS` 10 — a long prayer sends
~30 texts, which at the measured 60–80 ms per text is 1.5–2.4 s, not 0.55.
Whoever implements this should bound the list (the last N shown) rather than
let the people who pray longest pay for it. Two further notes for that
implementation: the numbers are measured from this machine, not from the
production VM, so the ratio transfers and the absolute milliseconds do not;
and `RemoteEmbeddingClient` is synchronous, so it has to be called through
`run_in_threadpool` the way `scripture_select` calls it, or half a second
parks the whole event loop rather than one request.

**The review of this ticket argues for 0.78 rather than 0.80**, and the
disagreement is about the cost model rather than the data. A false positive is
invisible to the person — the handler generates a second question and shows
that one, which is a legitimate new question — so it costs one generation plus
one embedding, ~540 ms and no harm to meaning; a miss is the bug this ADR was
opened on. F1 weighs the two equally and therefore over-values precision here.
On the same rows, `is_repeat OR cosine`: 0.80 lets 19 of 116 repeats through
at a 0.07 false-positive rate (+36 ms expected), 0.78 lets 12 through at 0.13
(+72 ms), 0.76 lets 8 through at 0.20 (+108 ms). Halving the misses costs tens
of milliseconds against a ~1 s answer. Against that: below ~0.78 the false
positives start landing on pairs a reader would call plainly different, so
which threshold is right is exactly the "same topic / same thought" judgement
below.

Open for Maria, and the reason this is "pending" rather than "accepted": 19 of
the 176 labels are marked `ambiguous` (12 of them sit on the "same topic /
same thought" line the threshold is drawn through), the labels are one
reader's proposal, the 176 rows are not 176 independent judgements (the 45
hand-written pairs are 15 items translated into three languages, and seven
rows are byte-identical questions), and doubling the median latency of a
companion's reply is a product decision rather than a technical one.
