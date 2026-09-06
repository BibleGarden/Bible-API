# ADR 0017: The question endpoint asks the model for a subject, and remembers it

Status: proposed (2026-09-06) — implemented on the branch, not accepted.
Ticket: ClickUp 86cbejvt2 («v6-B»), umbrella 86cbejvq1.
Follows ADR 0015 (`skipped_questions`) and ADR 0016 (the novelty retry), which
are the two previous attempts at the same failure, and ADR 0008 (no silent
defaults) for why the constants here are code.

## Context

Prompt v5 was accepted on 2026-09-06 (86cbejq55) and independently assessed the
same day (86cbejtt2,
`evaluation/bench_data/question_comparison_prompt_v5_before_after/FABLE_ASSESSMENT.md`,
396 answers read by hand). On Qwen3-30B — the model production runs — v5 fixed
what it was written for (temporal sense, no interrogation of feelings, the
prayer goal held) and produced, in the same run:

* **the masculine addressed to a woman in 15 answers of 99**, ten of them in
  the Ukrainian series where the prompt carried Ukrainian example forms — worse
  than v4's 1;
* **six verbatim duplicates inside one Ukrainian replacement series**, one
  sample answering all five replacements with a single identical line;
* **one subject reworded through a whole series**: `series-scale-ru` sample 1
  spends six steps on "what do you want finished tomorrow", the tail changing
  and nothing else;
* «X или Y» menus in five scenarios, and advice worded as a question.

The assessment's own reading, which this ADR accepts: *these are not prompt
problems any more.* v5 already says «при замене меняй предмет размышления, а не
только формулировку» and «если род неясен, не выбирай мужской род по
умолчанию»; saying it again in other words is what v4 → v5 already tried. Its
first recommendation is to compute the gender in code, and its third is to make
the repeat check see the *subject* rather than the string.

So v6 changes what is asked of the model and what the server does with the
answer, rather than how politely the same thing is requested.

## Decision

Four changes, of which this ADR is chiefly about the first and the fourth.

### 1. The model answers with an object

`{"subject": "<2-4 words>", "question": "<one open question>"}` on one line.
`app/question_format.parse_question` reads it: `json.loads`, then
`json_repair.repair_json_object` (the rewrite stage's own bounded repair,
imported — moving it into a dependency-free module is the only change to
`query_rewrite`), then a regex for the `question` field, then the answer's
first line as it is. The endpoint asks for **one** further generation when the
answer reaches that last rung, guarded by `MIN_SECOND_ATTEMPT_SECONDS` of the
request budget, and never a second one.

Naming the subject before writing the question is the part that cannot be
achieved by instruction: it makes "a different subject" a thing the model has
to write down, not a thing it can believe it did.

`QuestionResponse` gains an additive `subject: string | null`. The person is
shown `question` and nothing else; the request contract is untouched.

### 2-3. The angle and the gender come from code

At `next`, one line names which of the goal's five angles this step is about
(`clarification_angle`, rotating by `len(skipped_questions)`), and one line
states the person's grammatical gender
(`app/person_gender.detect_gender` over their `user` turns and the topic;
`None` — nothing matched or the words contradict — asks for a question that
needs no gendered forms). The ru/uk paragraph that listed «рада»/«рад» is gone:
the gender is given now, and the prompt says not to infer it.

### 4. Which subjects are taken: the server remembers

The list of subjects already used in this prayer is the third lever, and there
were three ways to get one. **This ADR takes the second.**

1. **Extract it server-side from the question texts.** The request carries the
   `assistant` turns and `skipped_questions` as text; a model call (or a
   heuristic) could name what each was about. Rejected: a call per shown
   question is latency and cost inside a 20 s budget, and a heuristic would
   invent subjects with no more grounding than the first-80-characters excerpt
   below.
2. **The server remembers what it already named.** ← chosen. Every one of those
   questions was generated *by this service*, and since change 1 the model
   names a subject for each. `question_format.SubjectMemory` maps
   `normalize(question) -> subject` — the same normalisation
   `question_novelty` uses — with a two-hour TTL and 2000 entries, oldest
   evicted. `build_user_message` renders one block, «Предметы, о которых уже
   спрашивали:», at `next` only and only when it is non-empty.
3. **The client returns `subject` with each replaced question.** Rejected *for
   now*: it is the only option that survives a restart and a multi-worker
   deployment, but it reopens a client contract confirmed on 2026-09-05 and
   would have to ship in an app release before the server could rely on it.
   It remains the upgrade path, and it is compatible with what is built here —
   a `subject` supplied by the client would simply take priority over the
   memory.

**The memory is allowed to be lossy, and that is the point.** A miss — a
restart, an expiry, an eviction, a question from before this deployment — falls
back to an excerpt of the question itself (80 characters), which is what v5
would have said anyway. So the block degrades to "no worse than v5" rather than
to an error, nothing depends on it for correctness, and no answer is withheld
because of it. It holds prayer text in memory for at most two hours, is never
persisted and never logged.

**It assumes one worker.** Production runs a single API worker already — the
rate limiters of `app/rate_limit.py` have the same requirement — and on several
workers the block would simply be right less often, never wrong: a worker that
did not generate a question names it by its excerpt. That is the same
degradation as a restart.

## What is deliberately NOT done

* **No rejection of an answer for its gender, its menus or its dashes**
  (Maria, 2026-09-06). The prompt states those rules and the assessment counts
  them; the handler makes exactly two kinds of extra generation — one for an
  answer that cannot be read at all, and the novelty retry that predates this
  version. A style-driven re-roll ladder would spend every person's latency
  enforcing what a measurement has not yet shown to be worth it. **The
  criterion of v6 is depth**: a question worth stopping over, taking the
  tension between two things the person named, asking about a choice or an
  action rather than a feeling.
* **The despair rule is untouched** — both tiers of `app/safety.py`, still
  reading the person's last reply, now applied to the parsed `question`. So is
  the meaning of `novel`, the request contract (`topic`/`stage`/`messages`/
  `skipped_questions`), the rate limits, and the thresholds of
  `evaluation/thresholds.json` and `scenarios.json`.
* **Nothing was added to make the companion warmer** (Maria, 2026-09-05).

## Consequences

* One more failure mode to watch, and it is visible: `question format:
  parsed=json|repaired|regex|retry_ok|retry_failed|raw` per answered request.
  A model that stops honouring the contract shows up as `raw`/`retry_failed`
  in the logs and as a doubled call count, not as a worse question; the two
  retry labels are separate so that "asking again helps" can be read off the
  logs rather than assumed.
* **The person is never shown the envelope.** The `openai_compat` transport
  asks for `response_format: {"type": "json_object"}` (vLLM's server-side
  grammar; the Gemini branch is untouched), the `regex` rung is forgiving about
  the key's spelling, quoting and markdown, and anything that still looks like
  machinery after every rung counts as unreadable — so the retry fires, and a
  failed retry falls back to a brace-free sentence. An answer with no question
  in it at all is a `502`, the same error a provider that returned nothing
  already produces.
* Worst case three provider calls in one request (unreadable answer, then a
  repeat). Bounded by the single request `Deadline`, not by a count: the
  endpoint's promise is its latency.
* The evaluation stand builds the same bytes and parses with the same parser
  (`evaluation/question_prompts.py` variant `v6`,
  `evaluation/gen_questions.py` — `text` is the question, plus `subject`,
  `raw_text`, `format_parse`). The `v6` name is the live wording rather than a
  frozen copy, and refuses to run once production moves past v6.
* **Nothing here is measured yet.** Whether v6 is better than v5 on Qwen is
  86cbejvq1's next step (v6-C); this branch changes no threshold and no
  reference data.

## Open questions

1. Should the client return `subject` (option 3)? It is the only version that
   survives a restart, and it is an app release.
2. Is the raw fallback the right last rung, or should an unreadable second
   answer be a `502`? Today the person gets the model's line; an answer is
   never withheld, which is ADR 0016's rule applied to a different failure.
3. The angle rotates by `len(skipped_questions)` and therefore restarts with
   every new prayer. Whether the cycle should instead follow the conversation's
   length is a question for the measurement, not for this ADR.
