# ADR 0009: Provider-independent LLM client, provider per stage

Status: accepted (2026-09-05).
Ticket: ClickUp 86cbegg2f (step 2 of the local-models umbrella 86cbe4mtq).

## Context

Bible API's AI surface is four provider calls: the guiding question
(`POST /api/ai/question`), voice transcription (`POST /api/ai/transcribe`),
the retrieval query rewrite (ADR 0004) and the grounded passage rerank
(ADR 0005), plus the embeddings behind the vector index (ADR 0002). Until
this ADR every one of them spoke Gemini's `:generateContent` protocol
inline — the URL template, the `x-goog-api-key` header, the
`system_instruction`/`contents` envelope and the `candidates[0].content.parts`
reader were hand-written once per module.

Maria's decision of 2026-09-05 (recorded in the monorepo `CLAUDE.md`) is that
the AI contour moves to models the project can serve itself: the app must be
usable by anyone, of any age, in any country, and every external provider's
terms fail at least one of those. The first target is Qwen3-30B on the
company's OpenAI-compatible server for the chat stages and bge-m3 for the
embeddings, keeping the measured production scheme (rewrite → embeddings +
BM25 → interleave → blacklist → diversity → rerank) exactly as it is.

With the transport welded into four modules, "run this stage on another
model" was a code change and a deploy. It has to be an `.env` edit plus a
benchmark — and, during the migration, stages must be movable **one at a
time**, because each one is pinned by its own measurement.

The transport itself was not a research question: `evaluation/trace_picker.py`
has been running the production pipeline on Qwen through hand-written
adapters (`QwenQueryRewriter`, `QwenPassageReranker`) since ClickUp
86cbegcmm. This step moves that proven code into `app/` with tests.

## Decision

### 1. One OpenAI-compatible client, `app/llm_client.py`

`ChatClient` (synchronous, for the rewrite and rerank stages) and
`AsyncChatClient` (awaited, for the question handler) speak
`POST {endpoint}/chat/completions` with a system message, one user message,
`temperature`, `max_tokens` and — where the stage parses the answer —
`response_format: {"type": "json_object"}`. They strip `<think>…</think>`
from the content before returning it, because both production parsers extract
JSON greedily and a brace inside a reasoning block would swallow the real
object (and because the question endpoint would otherwise show the reasoning
to a person).

The retry policy is **not** re-implemented: `app/gemini_retry.py` is imported
as it stands. Nothing in it is Gemini-specific except the reader of a 429
quota body, which reports "details unknown" for any other shape — and an
unknown quota keeps the ordinary backoff, which is the correct default. So an
openai_compat call gets the same guarantees the Gemini stages have had since
ClickUp 86cbbnaxn: one call's budget carved across httpx's four timeout
phases (`provider_timeout`), and a backoff that is only slept when the
attempt after it still fits in the request's `Deadline` (`retry_pause`).

What the module deliberately does **not** contain: prompts, parsers,
validation. Those stay in the stage modules and are shared by both
transports, so a provider switch cannot move a prompt version or loosen a
check. `tests/test_llm_client.py` asserts the two transports send
byte-identical system and user messages.

### 2. A provider per stage, named in the environment

```
AI_QUESTION_PROVIDER            gemini | openai_compat
AI_SCRIPTURE_REWRITE_PROVIDER   gemini | openai_compat
AI_SCRIPTURE_RERANK_PROVIDER    gemini | openai_compat
```

Each stage keeps the model variable it already had (`AI_QUESTION_MODEL`,
`AI_SCRIPTURE_REWRITE_MODEL`, `AI_SCRIPTURE_RERANK_MODEL`) — the provider
decides how the value is interpreted: a Gemini model id in the request URL,
or the `model` field of a chat-completions body.

For `openai_compat` the endpoint and key are shared by default and
overridable per stage:

```
AI_OPENAI_COMPAT_ENDPOINT       shared base URL, e.g. https://host:8443/v1
AI_OPENAI_COMPAT_API_KEY        shared key; may be EMPTY = no Authorization header
AI_<STAGE>_ENDPOINT             this stage's endpoint instead of the shared one
AI_<STAGE>_API_KEY              this stage's key instead of the shared one
```

`AI_SCRIPTURE_REWRITE_API_KEY` is the pre-existing member of that last family
(ADR 0004's paid-key split) and keeps its meaning exactly: "the key THIS
stage bills", whoever serves it. The two other stages simply gained the same
option.

Resolution is one pure function, `config.resolve_stage(env, stage_vars)`,
producing a frozen `StageProvider(stage, provider, model, endpoint, api_key)`
— one object per stage, so no caller can pair a model with another stage's
endpoint. `config.resolve_rewrite_api_key` is now a thin wrapper over it and
`config.REWRITE_API_KEY` is unchanged in meaning.

### 3. Enabling rule, and what happens to an old `.env`

**The AI surface is configured when `GEMINI_API_KEY` is set OR any provider
variable is named.** Once it is, all three provider variables are required
and must name a known provider; an unknown value aborts the start.

Consequently an `.env` that predates this ADR — a Gemini key, no providers —
**does not start**, and says which three variables it wants (the aggregated
ADR 0008 error). This was a deliberate choice against a transitional "assume
gemini" default:

- which provider answers a request is precisely the class of decision ADR
  0008 forbids defaulting in code. The 2026-08-29 incident was a defaulted
  *model*; a defaulted *provider* is the same failure with a larger blast
  radius, and it would have been invisible in `.env` for as long as it lasted.
- the alternative failure mode is worse than a refusal to start. Had the rule
  been "AI is on only when a provider is named", the same old `.env` would
  have started with AI silently switched **off**: `/api/ai/question` and
  `/api/ai/transcribe` answering 502 and `/api/ai/scripture` degrading to the
  safe pool, with nothing in the log naming a variable. Refusing to start is
  the loud version of the same information.
- the cost is one line per stage in three `.env` files, once — and the deploy
  procedure already requires editing prod `.env` before rebuilding
  (`Deploy/env-checklist.md`).

What is **not** required: `GEMINI_API_KEY` itself. A stage on `gemini`
without a key is still the supported "deploy without AI" state ADR 0008
promised — 502 from the chat endpoints, `fallback_reason=ai_unavailable` from
the selection endpoint — not a startup failure. Model variables keep their
existing gate (`GEMINI_API_KEY` is set) and gain one more case: a stage on
`openai_compat` must name its model whether or not a Gemini key exists,
because naming that provider IS the statement that the stage runs.

### 4. Transcription stays Gemini-only, explicitly

There is no `AI_TRANSCRIBE_PROVIDER`, and setting one is a startup error
naming the variable rather than a setting that quietly does nothing. Speech
is the one stage the chat-completions protocol does not cover; it moves to a
local speech model in its own step (ClickUp 86cbe4mtq, step 6).

So a deployment with all three chat stages on `openai_compat` still needs
`GEMINI_API_KEY` **for transcription only** — and `AI_TRANSCRIBE_MODEL` with
it. Without a key, `/api/ai/transcribe` answers its documented
`502 AI service unavailable` (an explicit refusal raised before any provider
call) while every other endpoint, including the fully local chat stages,
works normally.

Embeddings (`EMBEDDING_MODEL`) are untouched here: they name the stored index
rather than only a call, and they are step 3 (bge-m3, ClickUp 86cbegg2r).

### 5. Two per-call timeouts became operational variables

- `AI_QUESTION_TIMEOUT_SECONDS` (default **20.0** — the value the question
  endpoint always ran with) — the ceiling of its single provider call.
- `AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS` (default **8.0** — the measured
  Gemini value of ADR 0006) — the ceiling of ONE call inside a selection.

Both defaults are today's behaviour, so no existing deployment changes. They
had to become variables because `provider_timeout` takes the **minimum** of
the per-call ceiling and what is left of the request budget: with the ceiling
hard-wired in code, a stage moved to a slower self-hosted model could not be
given more time by raising `AI_SCRIPTURE_TIMEOUT_SECONDS` — every call would
still be cut at 8 s and the endpoint would degrade to the safe pool on a
model that was merely slower, not broken. Timeouts are operational parameters
under ADR 0008: the default is the reviewed operating point, and a malformed
value is still a startup error.

### 6. Keys never reach a log or an error

Failures are reported by category (exception type, HTTP status), never by
quoting the request or the response body — the policy `query_rewrite` and
`passage_rerank` already follow, extended to the new transport, which
additionally does **not** chain the httpx exception (`raise ... from None`),
because an httpx message quotes the request URL.

For the same reason `config.validate_endpoint` refuses an endpoint that
carries credentials (`https://user:pass@host/v1`) or a query string: a key
pasted there would reach httpx's own INFO log line and every error that
quotes a URL. The key belongs in `AI_*_API_KEY`, which nothing prints. The
validation error names the variable and never echoes its value.

## Alternatives considered

**One abstract `LLMProvider` interface with a Gemini and an OpenAI
implementation.** Rejected as more machinery than the problem has: two
transports, three stages, and the Gemini path must keep behaving exactly as
today (it is the currently deployed one and every existing test measures it).
What exists instead is a small class per stage per transport, sharing the
stage's prompts and parser, plus one factory per stage
(`build_query_rewriter`, `build_passage_reranker`, and a branch in
`twinkler_ai.complete`) that maps the configured provider onto a class. The
Gemini classes were not touched.

**Reuse `evaluation/trace_picker.py`'s adapters from `app/`.** The dependency
would point the wrong way (production importing the evaluation stand), and
the stand's rewriter deliberately runs a *different* prompt (8c, measured for
small models) while production runs v7. The adapters were moved into `app/`
in spirit — same protocol, same `<think>` handling, same error mapping — not
imported.

**Requiring `GEMINI_API_KEY` whenever a stage names `gemini`.** Tempting for
symmetry with the openai_compat requirements, but it deletes the "deploy
without AI" configuration ADR 0008 explicitly preserved, and would refuse to
start a container whose AI is intentionally unconfigured (including the test
image). The key stays the switch for Gemini stages.

**A provider for the transcription stage too.** There is no chat-completions
counterpart for audio; a `AI_TRANSCRIBE_PROVIDER=openai_compat` would have to
be either ignored or half-implemented. Refusing the variable is the honest
version, and it will be replaced by a real setting in step 6.

## Consequences

- Moving a stage to another model is an `.env` edit: provider, endpoint, key,
  model. Stages move independently, so a benchmark can pin them one at a time.
- Every deployment must add the three `AI_*_PROVIDER` lines before the next
  restart — local, production, and any tooling that imports `app/config.py`.
  `tests/conftest.py` names them (`gemini`) for the same reason it names the
  models. See `Deploy/env-checklist.md`.
- The Gemini path is unchanged: same URL, same headers, same payload, same
  retry ladder, same errors. `tests/test_query_rewrite.py`,
  `tests/test_passage_rerank.py` and `tests/test_twinkler_ai.py` still measure
  it, essentially untouched.
- `twinkler_ai.GeminiError` is now an alias of the provider-independent
  `AIError`. The name every caller imports still works and still means "this
  AI call failed" → 502.
- `tests/test_llm_client.py` adds a tripwire (with the three CHAT stages on
  `openai_compat`, the hostname each stage's client actually dials is the
  configured one and never `generativelanguage.googleapis.com`; and the
  factories build no Gemini client at all) and parity tests (one answer,
  either envelope, identical parsed result, identical prompt bytes). The
  tripwire is scoped to those three stages on purpose: embeddings are still
  Gemini in this step, so a selection whose chat stages are fully local does
  still dial Google for its vectors, and a tripwire claiming otherwise would
  be false. Step 3 (ClickUp 86cbegg2r) widens it.
- Prompts are untouched: rewrite v7, rerank v9, question v1. Whether those
  prompts are the right ones for a smaller model is steps 4 and 5, measured
  on the standing benchmark.

## Open questions

1. `AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS` and `AI_SCRIPTURE_TIMEOUT_SECONDS`
   have to be raised together for a self-hosted model, and the right pair is
   a measurement, not a guess (step 8). Until then an all-Qwen selection on
   the shipped defaults will often degrade to the safe pool with
   `fallback_reason=deadline` — the honest fallback, but not a usable
   operating point.
2. The rerank contract loses Gemini's `responseSchema` on `openai_compat`:
   `json_object` plus `parse_rerank_response` carry it. That is the half that
   was ever load-bearing (ADR 0005: the server never trusted the schema), but
   a weaker model may now produce more refused answers, each costing a
   fallback to retrieval's top-1. Worth watching in the benchmark of step 5.
3. `evaluation/trace_picker.py` still owns private copies of the same
   adapters (with prompt 8c). Once the production prompts and the stand's
   agree, its rerank column could import
   `passage_rerank.OpenAICompatPassageReranker` instead.
