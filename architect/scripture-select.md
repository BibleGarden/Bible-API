# Scripture selection

Public contract of `POST /api/ai/scripture` — contextual Bible passage
selection for the prayer app. Design decisions and measurements:
`architect/adr/0006-scripture-select-api.md` and
`architect/adr/0007-reference-translation-rendering.md`; the pipeline behind
them is ADR 0004 (retrieval) and ADR 0005 (grounded rerank).

> **Renamed 2026-08-30 (ClickUp 86cbbmwjk).** The endpoint was
> `POST /api/scripture/v1/select` until then, and a companion
> `GET /api/scripture/v1/translations` published the renderable-translation
> catalogue. The catalogue was removed with the rename: the translation is
> chosen once in the app, and the selection serves any active translation of
> an indexed language anyway (ADR 0007), so a separate list had no consumer.
> Request and response bodies, headers, authentication and limits are
> unchanged — only the path moved. The old paths return 404; there are no
> aliases (a single unpublished client, renamed in a paired mobile ticket).

> **Environment variables renamed 2026-08-30 (ClickUp 86cbbmy8d).** Every
> setting of this pipeline now carries the `AI_SCRIPTURE_` prefix, so a name
> says which method it configures: `RETRIEVAL_REWRITE_MODEL` →
> `AI_SCRIPTURE_REWRITE_MODEL`, `RETRIEVAL_RERANK_MODEL` →
> `AI_SCRIPTURE_RERANK_MODEL`, `RETRIEVAL_REWRITE_API_KEY` →
> `AI_SCRIPTURE_REWRITE_API_KEY`, `SCRIPTURE_SELECT_REQUESTS_PER_MINUTE` and
> `SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE` →
> `AI_SCRIPTURE_REQUESTS_PER_[CLIENT_]MINUTE`,
> `SCRIPTURE_SELECT_TIMEOUT_SECONDS` → `AI_SCRIPTURE_TIMEOUT_SECONDS`,
> `SCRIPTURE_INDEX_CACHE_SECONDS` → `AI_SCRIPTURE_INDEX_CACHE_SECONDS`,
> `SCRIPTURE_PRIMARY_TRANSLATIONS` → `AI_SCRIPTURE_PRIMARY_TRANSLATIONS`, and
> the shared `TWINKLER_CLIENT_HMAC_KEY` → `AI_CLIENT_HMAC_KEY`. Names only —
> no value, limit or default changed, and no old name is accepted as an
> alias. Everything below uses the new names; benchmark reports written
> before that date named the same knobs by their old ones.

## Public contract

The endpoint requires the common `X-API-Key` header. Unknown JSON fields
are rejected.

```json
{
  "language": "ru",
  "topic": "Тревога перед операцией",
  "user_replies": ["Боюсь за исход", "Прошу мира в сердце"],
  "exclude_canonical_ids": ["v3:19.023.001-006"],
  "translation": 1
}
```

| field | required | limits |
|---|---|---|
| `language` | yes | `ru`, `en` or `uk` |
| `topic` | no (default `""`) | <= 500 characters; an empty topic is valid and served from the safe pool without any AI call |
| `user_replies` | no | <= 10 items, <= 1000 characters each, <= 4000 characters in total; blank items are dropped |
| `exclude_canonical_ids` | no | <= 200 items, each matching `v<version>:BB.CCC.VVV-VVV` |
| `translation` | no | translation code; must be one of the codes accepted for `language` (see [Available translations](#available-translations)); defaults to that language's `primary` |

Response:

```json
{
  "language": "ru",
  "canonical": {
    "canonical_id": "v3:19.023.001-006",
    "book_number": 19, "chapter_number": 23,
    "verse_start": 1, "verse_end": 6
  },
  "passage": {
    "translation": 1, "translation_alias": "syn",
    "book_number": 19, "chapter_number": 22,
    "verse_start": 1, "verse_end": 6,
    "title": "Псалом Давида",
    "text": "Господь — Пастырь мой; я ни в чем не буду нуждаться…",
    "verses": [
      {"number": 1, "text": "Господь — Пастырь мой; я ни в чем не буду нуждаться:", "paragraph_start": true},
      {"number": 2, "text": "Он покоит меня на злачных пажитях…", "paragraph_start": false}
    ]
  },
  "highlight": {
    "canonical": {
      "book_number": 19, "chapter_number": 23,
      "verse_start": 4, "verse_end": 4
    },
    "passage": {"chapter_number": 22, "verse_start": 4, "verse_end": 4}
  },
  "source": "rerank",
  "fallback_reason": null,
  "history_reset": false
}
```

`canonical` holds the canonical (english-masoretic) coordinates and the
stable `canonical_id`; `passage` holds the same place in the requested
translation's own numbering — the two differ for Psalms. The text is read
from the database; the AI stage only picks among candidates the server
retrieved and can never introduce a passage, a reference or scripture text
of its own.

## Verse boundaries (`passage.verses`)

`passage.text` is the passage as one string. `passage.verses` is the same
text split into its verses, in order, so the client can decorate individual
verses — first of all `highlight` — without parsing the text or counting
characters:

| field | meaning |
|---|---|
| `number` | verse number **in the numbering of the returned translation**, i.e. the same numbering `highlight.passage` speaks. Not a position in the list: a passage may start mid-chapter, a Psalm superscription is verse 1 wherever the translation counts it (`syn`, `bti`, `ubh`) and absent where it does not (`bsb`), and the numbering may have **holes** where the translation carries several canonical verses in one (`bti` has no Genesis 35:10 — its verse 9 says both) |
| `text` | the verse text, whitespace-trimmed, exactly as stored |
| `paragraph_start` | true when the verse opens a paragraph of `passage.text` |

Rendering rule: **highlight by `number`**. The verses to emphasise are
those whose `number` lies between `highlight.passage.verse_start` and
`verse_end` inclusive; the two fields are guaranteed to be in the same
coordinate system, because they are read from the same database rows.

`verses` and `highlight` are **independent fields**. A response may carry
both, either one alone, or neither: in particular a `highlight` can arrive
without `verses` in the degraded case below, and the client must then find
the verses in `text` itself. Presence of one never implies presence of the
other.

Joining the verses of a paragraph with single spaces and the paragraphs
with a blank line reproduces `passage.text` byte for byte (the rule
`chunking.build_text` uses, tested over the live corpus). A client can
therefore render the passage entirely from `verses` and ignore `text`, or
use `text` and only take the boundaries from `verses`.

The field is **additive**: every previously published field of `passage`
keeps its value and its meaning. It is **omitted entirely** (never `null`,
never an empty array) in the degraded case where the server has the passage
text but not its verse breakdown — the verse query failed, or the served
chunk belongs to an indexed translation other than the one the candidates
were rendered in (not reachable with today's one indexed translation per
language). Clients must fall back to `text` then.

## Available translations

Every active translation of a language whose corpus is indexed is accepted
in `translation`; anything else is rejected with 422. Today that is:

| language | translations | primary |
|---|---|---|
| `en` | 16 `bsb`, 17 `webus`, 779 `webbe` | 16 `bsb` |
| `ru` | 1 `syn`, 11 `bti` | 1 `syn` |
| `uk` | 20 `ubh`, 21 `npu` | 20 `ubh` |

The primary is the one served when `translation` is omitted; it is also the
translation the corpus is indexed in. The set follows the database and the
`AI_SCRIPTURE_PRIMARY_TRANSLATIONS` setting, so it changes when the corpus is
rebuilt — it is not a published constant. Names, descriptions and audio
voices of the same translations are in `GET /api/translations`, joined by
`code`.

Until 2026-08-30 this list was also served as `GET
/api/scripture/v1/translations`. That endpoint was removed (ClickUp
86cbbmwjk): the app picks its translation once, and the selection renders
any accepted code, so nothing consumed the catalogue.

The selection itself always runs over ONE indexed corpus per language (the
primary), and the chosen canonical passage is then rendered in the
translation that was asked for — from the database, through the Psalm
versification table where the numbering differs (ADR 0007). A translation
that does not contain a passage never has it proposed: candidates are
filtered by what that translation actually covers before the AI stage, so
an incomplete Bible (`npu` is the New Testament and the Psalms) is served
from a smaller pool rather than refused. The passage is never silently
replaced by another translation's text — if the chosen window cannot be
rendered, the request fails with 503.

The list is derived from the corpus and is cached with it: activating a
translation in the database publishes it after the corpus cache expires or
`POST /api/cache/clear` is called. Because both the list and the request
validation read the same cached object, they cannot disagree.

## Key verses (`highlight`)

The same AI call also marks the 1 to 3 verses carrying the central thought
of the passage for this prayer, so the client can emphasise them. Both
coordinate systems are given, in the same shape as above: `canonical` for
storing and comparing, `passage` for locating the verses inside the text
just returned. The `passage` range is always a real sub-range of the
passage returned — and when `passage.verses` is present as well, both its
boundary numbers are guaranteed to occur among those verses, so the client
always finds where the highlight begins and ends; a highlight that could
not satisfy this is omitted (see below). It is never longer than 3 verses.
`canonical` names the
same words and can be slightly wider: where the translation merges verses
the canon numbers separately, one translation verse maps onto several
canonical ones (syn 114:8 is canonical 116:8-9), so a 3-verse highlight can
have a 4-verse canonical range. Render from `passage`, store `canonical`.

`highlight` is **optional and additive**: when there is no highlight the
key is ABSENT (not `null`), and the rest of the response is byte-for-byte
what it was before the field existed. Clients must then render the passage
unchanged. There is no highlight when

- the passage came from any fallback (`source` is not `rerank`);
- the AI did not answer the key verses, or answered a range the server
  refused (outside the passage, reversed, longer than 3 verses);
- the coordinates cannot be mapped exactly — a Psalm whose versification
  mapping is missing, a highlighted Psalm superscription, which the
  canonical numbering does not number, or a span that lands outside the
  passage after being converted into the numbering of a translation other
  than the one the AI stage read;
- the converted span ends on a number the served translation does not have
  at all. Translations merge verses the canon numbers separately, which
  leaves holes in their numbering: a key verse the AI marked in `syn`
  (Genesis 35:10) may simply not exist in `bti`, whose verse 9 carries both.
  The range would be inside the passage and still unfindable in its verses,
  so it is dropped — better no highlight than one the client cannot locate.

Like the passage itself, the highlight is numbers only: the model answers
with positions of verses the server put in front of it, and the server
resolves them against the database and the Psalm versification table. No
verse text ever comes from the model.

Store `canonical.canonical_id` and send the accumulated list back in
`exclude_canonical_ids` to avoid repeats.

The model's internal reason for the choice is a server-side diagnostic and
is never part of the response.

## Selection source and fallbacks

Degradation is part of the contract, not an error:

| `source` | `fallback_reason` | when |
|---|---|---|
| `rerank` | `null` | the AI chose among the retrieved candidates (the only source that can carry a `highlight`) |
| `retrieval_fallback` | `rerank_failed` | the AI choice failed (timeout, transport, malformed answer) |
| `retrieval_fallback` | `no_reranker` | the rerank stage is not configured |
| `retrieval_fallback` | `deadline` | the time budget ran out before the AI choice |
| `safe_pool` | `empty_topic` | no topic and no replies were sent |
| `safe_pool` | `ai_unavailable` | no query could be embedded (provider outage) |
| `safe_pool` | `deadline` | the time budget ran out before retrieval could run |
| `safe_pool` | `coverage_empty` | retrieval ran, but the candidate pool narrowed by the requested translation's coverage together with the caller's exclusions and the genre blacklist left nothing (only possible for a translation other than the language's primary — ADR 0007) |
| `safe_pool` | `ranking_empty` | the same, without a coverage filter: retrieval ran and the caller's exclusions or the genre blacklist left the ranking empty (a long repeat history on a narrow topic; ADR 0007 fix F1 on the primary path) |

The safe pool is a curated, versioned list of comforting passages
(`app/data/safe_pool.json`, 9 places in version 1.1.0) rotated with the same
exclusion list. It is resolved through the requested translation's coverage
set as well, so a pool place missing from an incomplete Bible is skipped
rather than served from somewhere else: `npu` resolves 8 of the 9 places,
every other active translation all 9.

The rotation resets when the exclusion list covers every place the pool
resolves to, so an exhaustive history makes the pool repeat a passage
rather than refuse. 503 is therefore reached only when the pool resolves to
nothing at all — a coverage set that hides every place, an empty pool file,
or a non-empty pool file whose entries name windows absent from the
language's corpus entirely (every entry resolves to `None`, a data bug
rather than a translation gap) — never through `exclude_canonical_ids`.

## Errors

| code | when |
|---|---|
| 403 | invalid or missing API key |
| 422 | unknown field; oversized topic, reply, reply total or exclusion list; malformed canonical ID; unsupported language; a translation that is not accepted for the language (another language's, inactive, unknown, or not renderable from the canonical corpus) |
| 429 | global or per-client request limit exceeded (with `Retry-After`) |
| 503 | no verified passage can be produced: database unavailable, vector index empty (run `app/index_cli.py rebuild`), the rate limiter is misconfigured, the chosen passage cannot be rendered in the requested translation, or the time budget was already exhausted when the rendering would have started |

There is no `502`: provider failures are absorbed by the fallbacks above.

Every error — validation included — uses the same flat shape:

```json
{"detail": "topic is too long"}
```

Unlike the rest of the API, this endpoint does **not** return FastAPI's
`HTTPValidationError` array for 422: that body quotes the offending input
verbatim, which would send the prayer text back to the caller. The
`detail` string names the category and the field only — for example
`unknown field: extra`, `user_replies[0] is too long`,
`exclude_canonical_ids[0] has an invalid format`,
`language has an unsupported value`, `request body is not valid JSON` — and
reports at most three of them. No submitted value ever appears in it.

## Already-shown passages and corpus migrations

A canonical ID is only meaningful inside the chunking version that produced
it (`v3:…`). IDs of another version are ignored for filtering and the
response sets `history_reset: true` — the client should drop its stored
history and keep only IDs returned from then on. Malformed IDs are a client
bug and are rejected with 422.

## Configuration

The models of the three AI stages — `AI_SCRIPTURE_REWRITE_MODEL` (rewrite),
`EMBEDDING_MODEL` + `EMBEDDING_DIMENSIONS` (vector index) and
`AI_SCRIPTURE_RERANK_MODEL` (final choice) — have **no defaults in code**: a
missing one aborts startup with an aggregated list of what is unset. Their
values are pinned by the benchmark (ADR 0002, 0004, 0005) but must be spelled
out in the environment; the 2026-08-29 degradation was invisible because a
silent default sent the rewrite stage to a model the key could not reach.

The two provider-call models are required only when `GEMINI_API_KEY` is set.
`EMBEDDING_MODEL` and `EMBEDDING_DIMENSIONS` (positive) are required always:
they name the stored index this endpoint READS, which the no-AI answer below
also needs. Without them the index version would degrade to `c3:@0` — an
index nobody ever wrote — and the documented safe-pool 200 would turn into a
503.

The operational parameters below (limits, TTL, timeout) stay optional and keep
their defaults — but a non-numeric value in any of them is a startup error,
never a silent fallback.

### Which provider serves which stage (ClickUp 86cbegg2f, ADR 0009)

`AI_SCRIPTURE_REWRITE_PROVIDER` and `AI_SCRIPTURE_RERANK_PROVIDER` name the
transport of their stage — `gemini` or `openai_compat` (any OpenAI-compatible
`/chat/completions` server) — and are required, together with
`AI_QUESTION_PROVIDER`, as soon as the AI surface is configured at all. The
model variable of each stage is unchanged; the provider decides how it is
read. An `openai_compat` stage additionally needs an endpoint and a key
statement, shared (`AI_OPENAI_COMPAT_ENDPOINT` / `AI_OPENAI_COMPAT_API_KEY`,
the key may be empty) or per stage
(`AI_SCRIPTURE_RERANK_ENDPOINT` / `AI_SCRIPTURE_RERANK_API_KEY`).

What does **not** change with the provider: the prompts (rewrite v7, rerank
v9), the parsers, the server-side validation of the rerank answer, the
fallbacks and every `fallback_reason`. `build_query_rewriter` /
`build_passage_reranker` return the configured transport and
`ScriptureRetriever` cannot tell them apart. The one contract that has no
counterpart outside Gemini is the rerank `responseSchema`; on
`openai_compat` the request asks for `response_format: json_object` and
`parse_rerank_response` carries the rest — the half the server ever trusted
(ADR 0005).

The embedding stage has its own provider variable, `EMBEDDING_PROVIDER`
(ClickUp 86cbegg2r, ADR 0010): `gemini` (the API) or `local` (BAAI/bge-m3 on
CPU inside this process, weights from a read-only volume, no network and no
key). It is separate from the three chat providers, and required in every
deployment rather than only when AI is configured, because the model and its
dimensions name the **stored index version** — `c3:BAAI/bge-m3@1024` against
`c3:gemini-embedding-001@768` — which the read path needs even when nothing
else is configured. `build_embedding_client` returns the configured client
and `ScriptureRetriever` cannot tell those apart either.

Two consequences inside a selection. Variant embeddings are **not**
overlapped on the local provider (`embed_workers=1`): there is no round trip
to overlap and torch already uses every core for one encode. Measured: 39 ms
median per query, 334 ms for six variants in sequence and 320 ms for the same
six through a thread pool — against ~0.31 s of concurrent network on Gemini,
so the stage costs the same wall time and spends CPU instead of waiting.

And the retrieval
quality is the one measured in 86cbe4n7e (hit@10 0.875, recall@10 0.688,
MRR 0.524 against 1.000 / 0.789 / 0.664): the passage is found and ranked
worse, which is the work the grounded rerank does over the candidate list.

`AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS` (default 8, the measured Gemini
value) is the ceiling of ONE stage call inside the selection budget. It has
to be raised together with `AI_SCRIPTURE_TIMEOUT_SECONDS` for a slower
self-hosted model: `provider_timeout` takes the minimum of the two, so a
larger total budget alone changes nothing.

### Which key pays for which stage

`AI_SCRIPTURE_REWRITE_API_KEY` (optional) is the key of the **rewrite stage
only**. Unset or blank means "one shared key": rewrites go out on the shared
key of that stage's provider (`GEMINI_API_KEY`, or
`AI_OPENAI_COMPAT_API_KEY`), exactly as before the variable existed. That default is
operational, not a hidden fallback in the ADR 0008 sense — the absent value
has a single intended meaning, and the *configured* behaviour is the same
either way: same model, same prompt, same request. What the key changes is
the quota and the bill — and, as a consequence, availability: a stage on an
exhausted free quota is answered with 429 and degrades to `rewrite_failed`,
which is logged rather than silent.

The split exists because the stages have very different quota pressure.
Rewrite is pinned to `gemini-3.7-flash` (ADR 0004) and one selection spends a
rewrite call per request, which exhausts that model's free daily quota;
embeddings (`gemini-embedding-001`) and the rerank
(`gemini-3.5-flash-lite`, ADR 0005) sit comfortably inside the free quotas of
their lite models. Moving one stage to a paid key therefore buys the whole
endpoint's reliability at the cost of ~1 call per selection.

The key is resolved in exactly one place — `config.resolve_stage`, of which
`config.resolve_rewrite_api_key` → `config.REWRITE_API_KEY` is a wrapper —
and reaches the provider through the stage's client, so every creation point
(`scripture_select._provider_clients`, `app/retrieval_cli.py`, and
`evaluation/retrieval_benchmark.py` through `require_rewrite_api_key()`)
bills the same key without repeating the rule. Since ADR 0009 the two other
chat stages have the same option (`AI_QUESTION_API_KEY`,
`AI_SCRIPTURE_RERANK_API_KEY`) and fall back to their provider's shared key.
`GeminiEmbeddingClient` reads `GEMINI_API_KEY` directly; on
`EMBEDDING_PROVIDER=local` the embedding stage bills nothing at all.

A stage key set while that stage runs on Gemini and `GEMINI_API_KEY` is empty
is a configuration error in the aggregated startup list: it pays for one
stage of a pipeline whose remaining stages cannot run at all.

### Rewrite prompt and implicit provider caching

The rewrite request is built so the static part is strictly a prefix of the
variable part: the instruction goes into `system_instruction` (constant per
language and variant count — `build_rewrite_instruction`) and the prayer text
appears only in `contents` (`build_rewrite_user_content`). Nothing
request-specific precedes the instruction, which is the ordering a provider's
implicit prompt cache requires. No change was needed and none was made: the
prompt text is fingerprinted by the benchmark (`REWRITE_PROMPT_VERSION`), so
any edit forces a re-run.

Reality check on the benefit, measured 2026-08-29: the instruction is ~2.0 kB
(~500 tokens for each of ru/en/uk). Flash-class models document a minimum
cacheable prefix of ~1024 tokens, so today's prefix is roughly half of what
an implicit cache hit needs — the ordering is correct and costs nothing, but
no cache discount should be assumed in cost estimates until the prefix is
measured against the model's actual minimum. Padding the instruction to reach
the threshold is explicitly not done: it would change retrieval quality (and
the fingerprint) to chase a discount on ~500 tokens per selection.

## Time budget

One selection is bounded by `AI_SCRIPTURE_TIMEOUT_SECONDS` (default
15 s): stages that cannot finish are skipped in favour of a verified
fallback, and every provider call is capped by what is left of the budget.
Measured production latency (warm process): median 6.2 s, max 6.6 s; the
query rewrite is ~75 % of it. The six query embeddings run concurrently.

### Why it used to be exceeded (ClickUp 86cbbnaxn, 2026-08-31)

Answers of 16.3 s, 21.4 s and 13.9 s were observed against a 15 s budget
while the free-tier provider was answering 429. Four defects, each of which
alone could outlive the budget:

1. **`timeout=<number>` is per PHASE in httpx.** A bare number is expanded
   into `Timeout(connect=t, read=t, write=t, pool=t)`, and each phase is
   bounded on its own — so `timeout=remaining` authorised up to four times
   the remaining budget for a single call. `gemini_retry.provider_timeout`
   now carves the four phases OUT of the budget (connect/write/pool get a
   twelfth of the call each, capped at 1 s; the read phase keeps the
   remaining three quarters), so their worst case sums to it. The split is
   sized against the stage measurements above, not chosen for symmetry:
   `:generateContent` is not streamed, so the model's whole thinking time
   lands in the **first read**, and the rewrite call measures 3.7-4.7 s
   against the 8 s base — a half-and-half split would leave read 4.0 s and
   time out the median healthy request, paying for the budget with a
   permanent quality loss. At base 8 s read gets 6.0 s, 1.3x the slowest
   measured call.
2. **A backoff was clipped to the remaining budget and then slept.**
   `deadline.sleep_budget` returned `min(delay, remaining)`, so a Gemini
   `RetryInfo` of 30-55 s on a 429 slept out **every second the request had
   left** and the attempt it was waiting for then found no budget at all.
   `gemini_retry.retry_pause` returns `None` — "degrade now" — whenever the
   pause plus a minimally useful call (1 s) no longer fit. `sleep_budget`
   is deleted, not fixed: it invited exactly this.
3. **A 429 was a 429.** Gemini says in the body which quota rejected the
   call; a per-DAY free-tier quota cannot reopen inside a 15 s request, so
   every retry against it was guaranteed waste. See below.
4. **The budget started after the corpus load.** `Deadline` was created
   just before the pipeline, so a cold `get_resources()` (full index load,
   ~1 s and several DB round trips, and every refresh after the TTL) sat
   outside the budget entirely. It is created at the top of the handler
   now: the budget is the endpoint's promise about its own latency, and the
   AI stages get what is left of it.

What is still outside the budget, by design: resolving the chosen
candidates' texts from MySQL and building the response. These are local,
bounded and unavoidable — the answer needs them — which is why the contract
is "the budget plus local DB work", not "the budget".

Not solvable here, and stated rather than hidden: httpx's `read` timeout
bounds the wait for the *next chunk* of a response, not the whole response,
so a provider dribbling bytes forever inside its read timeout cannot be cut
off by httpx at all. These stages receive one small JSON document each, and
the stage boundaries re-check the deadline, so the residual exposure is one
hung call rather than a summed retry ladder.

### Which quota rejected us (429 details)

The `generativelanguage` API answers a quota rejection with
`error.details` carrying a `google.rpc.QuotaFailure` — `quotaId` /
`quotaMetric` naming the quota, e.g.
`GenerateRequestsPerDayPerProjectPerModel-FreeTier` versus
`GenerateRequestsPerMinutePerProjectPerModel-FreeTier` — and usually a
`google.rpc.RetryInfo` with a `retryDelay` such as `"14s"`. The delay does
**not** carry the scope: the same daily violation is observed answering
`"0s"`, `"14s"` and `"55s"`, so the quota id is what is read
(`app/gemini_retry.py`).

| 429 says | Behaviour |
| --- | --- |
| a per-day (or per-hour) quota is exhausted | no retry at all — degrade immediately down the existing ladder |
| a per-minute quota is exhausted | retry after `max(our backoff, retryDelay)`, but only if the pause plus the call still fit in the budget; otherwise degrade now |
| 429 without recognisable details, or any 5xx | unchanged: the documented backoff ladder, inside the deadline |

Parsing is total: a body that is not JSON, or is shaped differently, or
carries an unparsable delay, means "details unknown" and takes the ordinary
branch. It never raises.

`fallback_reason` values are unchanged by all of this — the stages degrade
through the same `retrieval_fallback` / `rerank_failed` / safe-pool ladder
as before, only sooner. Their *distribution* shifts, though: an exhausted
daily quota used to sleep the budget out and surface as `deadline`, and now
degrades immediately as `ai_unavailable` — a report crossing this deploy
should expect that swap, not a regression.

## Rate limiting and observability

Two 60-second windows, independent of the Twinkler budget because one
selection costs ~8 provider calls:

- global: `AI_SCRIPTURE_REQUESTS_PER_MINUTE` (default 10);
- per client address: `AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE`
  (default 3).

The in-memory client identifier is an HMAC-SHA-256 pseudonym built with
`AI_CLIENT_HMAC_KEY` (shared with the Twinkler endpoints); the
address itself is not retained. Missing HMAC configuration fails closed
with 503. Counters are process-local, so production runs a single API
worker. Client addresses come from the direct peer; `X-Forwarded-For` is
honoured only for trusted reverse proxies — a name in `TRUSTED_PROXY_HOSTS`
resolved at runtime, or an address/network in `TRUSTED_PROXY_IPS`
(`app/trusted_proxies.py`, ClickUp 86cbbq6vz) — and then the client is its
**rightmost** element, the address the proxy appended. Elements to the left
were supplied by the caller (nginx preserves the header it receives), so
believing them would hand out a fresh rate-limit bucket per request.

Standard request statistics store the endpoint, method, status, latency, an
HMAC pseudonym truncated to 40 hexadecimal characters, and an empty user
agent. The prayer topic, the replies **and the selected passage** are never
stored or logged: the reference alone is not private, but next to a client
identity it would reveal what the person prayed about. Logs carry failure
categories only.

**The statistics history is split at the rename.** `app/middleware.py`
stores the request path verbatim, so `api_requests` and
`api_request_daily_stats` hold this endpoint under
`/api/scripture/v1/select` up to 2026-08-30 and under `/api/ai/scripture`
after it (likewise `/api/twinkler/v1/complete` → `/api/ai/question` and
`/api/twinkler/v1/transcribe` → `/api/ai/transcribe`). The rows were
deliberately NOT rewritten: they are an honest record of what was called.
Any report spanning the boundary has to union the two names — the raw table
keeps only 14 days, so the seam disappears from it by 2026-09-13 and
survives only in the permanent daily aggregate.

## Caching

The vector index, the per-language BM25 index, the Psalm versification maps,
the renderable-translation catalogue and the per-translation coverage sets
are cached in the process for `AI_SCRIPTURE_INDEX_CACHE_SECONDS` (default
1 hour, minimum 1 second) — they depend only on the corpus, cost ~1.4 s to
build and are identical for every request. `POST /api/cache/clear` drops them immediately, so a rebuilt
index can be published without restarting the service. If a refresh fails,
the previous copy keeps being served (a 503 only happens when there is no
cached corpus at all). Nothing derived from a request — rewrites,
embeddings, candidates, chosen passages — is ever cached.
