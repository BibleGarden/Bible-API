# ADR 0006: Public scripture-selection API

Status: accepted (2026-08-24).
Ticket: ClickUp 86cb8vw1m

## Context

ADR 0004 (retrieval) and ADR 0005 (grounded rerank) produce
`ScriptureRetriever.select_final`: a prayer context in, ONE verified
passage out, with the whole quality argument already measured against
`evaluation/thresholds.json`. What was missing is the public contract the
iOS app calls: request validation, authentication, rate limiting, a time
budget, and the privacy rules for a request whose input is a prayer.

Acceptance (ticket): the endpoint returns only a verse range that exists in
the requested translation, correctly excludes already-shown places, has
documented error and fallback responses, and is covered by rate-limit and
privacy tests.

## Decision

### Endpoint

`POST /api/scripture/v1/select` (`app/scripture_select.py`), version in the
path exactly like the Twinkler endpoints. The existing
`/api/twinkler/v1/*` contract is untouched (regression test:
`test_twinkler_contract_is_unchanged`).

Request (unknown fields rejected, `extra="forbid"`):


| field | type | limit | notes |
|---|---|---|---|
| `language` | enum `ru`/`en`/`uk` | required | mirrors `query_rewrite.SUPPORTED_LANGUAGES` (asserted by a test) |
| `topic` | string | <= 500 chars | empty is valid -> safe pool, no AI call |
| `user_replies` | string[] | <= 10 items, <= 1000 chars each, <= 4000 total | the replies the person picked in the dialog |
| `exclude_canonical_ids` | string[] | <= 200 items, canonical-ID pattern | already shown passages |
| `translation` | int | optional | must be one of the language's renderable translations (ADR 0007); defaults to its primary |

Limits are ~10x the observed corpus (the evaluation scenarios top out at a
47-character topic and 62-character replies) — generous for a dialog that
produces a theme line and picked replies, while bounding the rewrite prompt
and the number of IDs the server parses. They are deliberately far below
Twinkler's `user` field (1..16000): that endpoint carries a whole free-form
message, this one carries dialog metadata.

Response: canonical coordinates + `canonical_id` (the repeat key the client
stores), the exact title/text/coordinates/verse boundaries of the chosen
translation, an optional `highlight`, `source`, `fallback_reason`,
`history_reset`.

### The verse boundaries of `passage` (ClickUp 86cbb1mq7)

`highlight.passage` names verse NUMBERS, but the passage was published as
one opaque string, so a client had to find those verses inside it by
guessing where a verse begins. `passage.verses` publishes the boundaries
the server already has:

```json
"verses": [
  {"number": 1, "text": "Господь — Пастырь мой; …", "paragraph_start": true},
  {"number": 2, "text": "Он покоит меня …", "paragraph_start": false}
]
```

Four decisions:

- **Verse numbers, not character offsets.** Offsets are a second encoding
  of the text that silently rots against any whitespace or import change,
  and they are not the coordinate system the rest of the contract uses.
  `number` is the verse number of the translation the passage is RETURNED
  in — literally the same rows `highlight.passage` is resolved from — so
  the rendering rule is "emphasise the verses whose `number` is inside
  `highlight.passage`", with no third coordinate system to keep in sync.
  For the Psalter this is what makes the field usable at all: the
  superscription is verse 1 in `syn`/`bti`/`ubh` and absent in `bsb`, and
  the Septuagint chapter shift is already accounted for in both fields.
- **The same source as the text, on every path.** For an indexed
  translation the verses are the ones the pipeline loaded for the chosen
  chunk (`retrieval.make_db_verse_loader`, one query it already made for
  the key-verse markers of ADR 0005); for a translation rendered from its
  own verses (ADR 0007) they are the very list `passage_render` assembled
  the text from. Neither path re-derives anything from the string, so the
  two views of the passage cannot drift apart. The safe pool and the
  retrieval fallbacks go through whichever of the two paths their
  translation uses, and carry the field like any other answer.
- **`build_text` equivalence is the contract.** Joining the verses of a
  paragraph with single spaces and the paragraphs with a blank line gives
  `passage.text` back byte for byte, so a client may render the passage
  entirely from `verses`. That required publishing the SECOND paragraph
  rule `chunking.build_text` uses: a section title standing before a verse
  also opens a paragraph. `translation_verses.start_paragraph` alone misses
  it in 278 `ubh` chunks (the boundaries come from the pivot translation's
  plan, so a title can fall inside a chunk), so both verse loaders now mark
  it as `VerseText.title_break` and the public `paragraph_start` is the OR
  of the two rules plus "first verse of the passage". `title_break` is
  display metadata only — the rerank prompt is rendered by `number_verses`,
  which reads `start_paragraph` alone, so the prompt is unchanged to the
  byte.
- **Additive, and absent rather than null.** Every previously published
  field of `passage` keeps its value. Degradation exists — the verse load
  is best-effort by design (it also powers the highlight, and a failure
  there must cost at most the highlight), and a served chunk of an indexed
  translation other than the one the candidates were rendered in has no
  verses attached — so the key is DROPPED by a `model_serializer` in those
  cases rather than serialised as `null` or as an empty array, exactly like
  `highlight`. Clients fall back to `text`.

No separate ADR: the change adds one nested object to an existing response
field, introduces no new stage, no new provider call and no new coordinate
system, and its only non-obvious consequence (the title paragraph rule) is
recorded above.

### The optional `highlight` field

The rerank stage also marks the 1-3 key verses of the passage (ADR 0005
prompt v9). The public shape mirrors the response's own two coordinate
systems, because the client needs both — the canonical one to store or
compare, the translation one to find the verses in the text it renders:

```json
"highlight": {
  "canonical": {"book_number": 19, "chapter_number": 23,
                "verse_start": 4, "verse_end": 4},
  "passage":   {"chapter_number": 22, "verse_start": 4, "verse_end": 4}
}
```

Three decisions:

- **Optional, and absent rather than null.** Every fallback path, every
  refused span and every unmappable coordinate answers without a highlight,
  so a client must render the passage unchanged in that case. To keep the
  previous contract byte-identical, the key is dropped entirely by a
  `model_serializer` instead of serialising as `null` (`fallback_reason:
  null` stays, it is published). A test asserts the no-highlight body has
  exactly the six original keys.
- **`passage` is always inside the returned passage.** The span the model
  answers is re-validated three times (ADR 0005) and the last checks happen
  here, in `build_response`: against the verses the reranker was actually
  shown, and — when the translation served is not the one shown — against
  the served passage's own chapter and verse range, since the mapped span
  is built from a versification table rather than from that passage's
  verses. A range check is not enough once `passage.verses` is published:
  a translation that carries several canonical verses in one leaves HOLES
  in its numbering (`bti` has no Genesis 35:10), so the boundary numbers
  are finally required to occur in the served passage's own verse list.
  Nothing is ever clamped into range: out of range, or onto a number the
  served translation does not use, means no highlight at all.
- **Coordinates come from the versification table, not from arithmetic.**
  `passage_highlight` converts through `psalm_verse_mappings` (ADR 0003):
  outside the Psalms the two systems coincide, inside them they do not
  (syn 22:4 is canonical 23:4; ubh 65:12 is canonical 65:11; syn 114:8 is
  canonical 116:8-9). A Psalm whose mapping is missing gets no highlight.
  The maps are corpus-derived and are cached with the vector index.
  Consequence of the merged verses in that last example: `canonical` may
  span MORE than 3 verses where the translation merges canonical ones
  (3 marked verses syn 114:6-8 = canonical 116:6-9). The exact range is
  published rather than truncated, so both systems name the same words;
  the 1-3 rule is a statement about `passage` (ADR 0005).

`FinalSelection.reason` — the model's diagnostic sentence — is
**not** returned. ADR 0005 left showing an explanation to users as an open
product decision; until it is taken, the field stays server-side
(`test_model_reason_is_never_returned_to_the_client`, plus an OpenAPI
assertion that the response schema has no `reason` property).

### Fallbacks are part of the contract, not errors

| `source` | `fallback_reason` | meaning |
|---|---|---|
| `rerank` | null | the AI chose among verified candidates |
| `retrieval_fallback` | `rerank_failed` / `no_reranker` / `deadline` | retrieval's top-1 was served |
| `safe_pool` | `empty_topic` / `ai_unavailable` / `deadline` | the curated no-AI list was served |

ADR 0007 adds two more values to the last row. `coverage_empty`: retrieval
ran, but the candidate pool — narrowed by the requested (non-primary)
translation's coverage together with the caller's exclusions and the genre
blacklist — was left with nothing, so the coverage-filtered safe pool
answered. `ranking_empty`: the same outcome where there is no coverage
filter, i.e. the caller's exclusions or the genre blacklist emptied the
ranking of a fully covered translation.

A Gemini outage therefore never produces a 5xx: it degrades along the ADR
0005 ladder. `503` is reserved for "no verified passage exists at all"
(DB down, empty vector index, rate limiter misconfigured). There is no
`502` on this endpoint — provider failures are absorbed by the fallbacks.

### Validation errors do not echo the prayer

FastAPI's default 422 body is an array of `HTTPValidationError` items, each
carrying the offending `input` **verbatim** — for this endpoint that means
returning the prayer topic and replies to the caller, and onward into any
crash reporter or proxy that records response bodies. That contradicts
every other privacy rule here, and it also made the declared `ErrorResponse`
schema a lie (a generated client would break on the array).

Decision: one app-level `RequestValidationError` handler
(`scripture_select.validation_exception_handler`, registered in
`app/main.py`) answers requests to this path with the same flat
`{"detail": "..."}` shape as its other errors, built from the error
CATEGORY and the field NAME only — "topic is too long",
"unknown field: extra", "exclude_canonical_ids[0] has an invalid format".
Field names come from the schema, except for `extra_forbidden` where the
name is client-supplied: anything that is not a plain identifier is
replaced with `field`, so a prayer sentence sent as a field name is not
echoed either. At most three distinct categories are reported.

Every other route — the Twinkler endpoints included — keeps FastAPI's
default handler and its published body shape.

### Time budget

`select_final` is a chain of provider calls, each with its own retry
ladder; before this ADR the worst case was their sum (minutes). A
`deadline.Deadline` is now created per request
(`SCRIPTURE_SELECT_TIMEOUT_SECONDS`, default 15 s) and threaded through
every stage:

- stage boundaries check `expired()` and degrade instead of starting a
  stage that cannot finish (`fallback_reason=deadline`);
- every provider HTTP call gets `timeout=min(base, remaining)`, and no
  attempt starts with a zero budget, so an in-flight call cannot outlive
  the budget either.

This is an approximation, not a wall-clock guarantee: httpx applies the
value per operation (connect / write / read / pool), so a pathological
connection could spend the budget on each phase in turn, and the local
work between checks (DB texts, BM25, response building — 0.12 s measured)
is not preempted at all. The point is the order of magnitude: the worst
case becomes "the budget plus a stage", not "the sum of every stage's
retry ladder" (which was minutes).

Serve-time provider budgets are deliberately tighter than the CLI /
benchmark defaults (which stay patient for offline indexing): 8 s timeout
and 2 attempts per stage instead of 20-60 s and 3-6 attempts.

The six variant embeddings are now embedded **concurrently**
(`ScriptureRetriever(embed_workers=REWRITE_VARIANTS)`), closing ADR 0004
open question 2. The default stays sequential so the CLI and the benchmark
keep the deterministic path (and the ADR 0005 m2 fail-fast); under
concurrency the fail-fast heuristic is moot — all calls are already in
flight — and the deadline bounds them instead.

### Measurements (live Gemini, production corpus c3, 2026-08-24)

Stage costs, mean over 3 languages:

| stage | cost |
|---|---|
| query rewrite (gemini-3.7-flash) | 3.7-4.7 s |
| 6 variant embeddings, sequential | 1.56-1.70 s |
| 6 variant embeddings, concurrent | **0.31 s** |
| vector search + BM25 + fusion + diversity + DB texts | 0.12 s |
| grounded rerank (gemini-3.5-flash-lite) | 0.72-0.85 s |
| corpus load (vector + BM25 index), cached | 0.94 s |

End-to-end `_run_selection` with the production configuration (6 requests,
ru/en/uk, warm corpus cache): min 4.85 s, **median 6.20 s**, max 6.62 s —
every one `method=rerank`. Over HTTP through the container: 4.4-5.4 s warm,
6.9 s on the first request of a process (cold corpus cache).

So the 15 s budget is ~2.4x the median: it is a guard against a slow
provider, not a routine limiter. The rewrite stage is ~75 % of the wall
clock and is the only remaining lever worth pulling if the budget must
shrink (a faster rewrite model fails the ADR 0004 thresholds, so this is a
quality trade, not a free one).

### Caching

Cached: the vector index, the per-language BM25 index and the Psalm
versification maps — all derived only from the corpus, identical for every
request, ~45 MB, ~0.94 s to build.
Process-local with a TTL (`SCRIPTURE_INDEX_CACHE_SECONDS`, default 1 h) and
dropped immediately by `POST /api/cache/clear` (so an index rebuild can be
published without a restart).

Not cached, deliberately: anything derived from a request — rewrites,
query embeddings, candidate lists, chosen passages. A cache keyed by prayer
context would turn a per-request secret into stored state and could leak
one person's context into another's timing/behaviour; the saving (a repeat
of the same topic) is not worth it.

Two failure rules: a failed refresh keeps serving the stale copy (a cached
corpus is still a valid corpus, and a DB blip must not take the endpoint
down) — only a cold cache answers 503; and the TTL is floored at 1 second,
because a 0 would rebuild ~45 MB under the global lock on every request.

### Rate limiting

Own budget, own env vars: `SCRIPTURE_SELECT_REQUESTS_PER_MINUTE` (default
10) and `SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE` (default 3), same
shape and defaults as Twinkler's. Separate counters because one selection
costs ~8 provider calls (1 rewrite + 6 embeddings + 1 rerank) against
Twinkler's 1: a shared window would let chat traffic starve selection or
vice versa, and the two features need to be tuned independently in
production.

The pseudonymisation key is shared (`TWINKLER_CLIENT_HMAC_KEY`): a second
key would double the configuration surface without adding privacy — the
pseudonym is already unlinkable to an address, and both endpoints serve the
same app.

The global limit is deliberately low and is cost control, not capacity
planning: 10 selections/minute is ~80 rewrite+embedding+rerank calls per
minute on a billed key. It is also weak as an abuse defence — the
per-client window is per address, so four distinct addresses can consume
the entire global budget and make the endpoint answer 429 for everyone
else. That is accepted for the current stage (one small app, a single
worker, a bill that must stay predictable); a real defence needs
per-installation identity or a distributed limiter, which is also what a
multi-worker deployment will require.

The limiter implementation moved to `app/rate_limit.py` (`RateLimiter`)
and is now used by both endpoints; limits are passed per reservation
instead of captured at import. Twinkler's public behaviour, error codes and
`Retry-After` are unchanged.

### Observability and privacy

Statistics are exactly Twinkler's: endpoint, method, status, latency, an
HMAC client pseudonym truncated to 40 hex chars, empty user agent
(`middleware.PRIVATE_PATHS`). The middleware never reads bodies.

The **selected passage is deliberately NOT recorded** either. A Bible
reference is not private in itself, but stored next to a client identity it
reveals what that person prayed about — which is precisely the data the
Twinkler privacy design keeps out of the database. Diagnostics that need it
(benchmarks, manual review) run offline on the evaluation dataset, not on
production traffic.

Logs carry failure categories only. Tests assert that neither topic,
replies, passage text nor canonical ID reach the statistics arguments or
the log records, on both the success and the failure path.

### Exclusion semantics across a CHUNKING_VERSION bump (retrieval m1)

Canonical IDs carry the chunking version (`v3:19.023.001-006`). After a
version bump the chunk boundaries move, so an old ID matches no current
chunk and describes no current window — keeping it in the filter would
silently hide nothing while the client's history grows dead entries.

Decision (`retrieval.split_exclusions`): IDs of another version are
**ignored for filtering and reported back** via `history_reset: true`,
telling the client to drop its stored history and keep only IDs returned
from now on. A client sending last version's IDs is normal after a corpus
migration; a client sending garbage is a bug, so malformed IDs never reach
this function — request validation rejects them with 422. (The function
itself is defensive and classifies anything unparseable as stale, so a
non-HTTP caller cannot make it raise.)

The alternative — mapping old IDs onto new chunks by canonical range — was
rejected: it is only approximate (boundaries change), and the cost of
being wrong is one repeated passage right after a corpus migration.

### Prompt hermetisation (rerank n1)

`app/prompt_safety.py::neutralize_prompt_markers` is applied to the topic,
every reply and every candidate text before they are embedded in the
rewrite and rerank prompts. It does two things:

1. drops every Unicode `Cf` format character (zero-width space, ZWNJ, ZWJ,
   word joiner, BOM, soft hyphen, bidi controls) and the C0 controls other
   than tab/newline — otherwise `<`ZWSP`<`ZWSP`<PRAYER_CONTEXT` survives a
   naive collapse while a model still reads three brackets;
2. collapses any run of two or more angle brackets — ASCII plus the
   fullwidth (`＜`), single-guillemet (`‹`), modifier (`˂`), CJK (`〈`)
   and mathematical (`⟨`) look-alikes — to its first character.

Marker *words* are left alone on purpose: "candidate" and "context" are
ordinary words in a prayer, and only the `<<<WORD` / `WORD>>>` combination
carries structure.

Honest scope: this closes the spellings above, it is not a proof that no
delimiter can ever be forged. The double guillemets `«»` are explicitly
NOT folded — they are ordinary punctuation in 2362 of the 11960 indexed
chunks, so folding them would rewrite scripture text; a reply spelling
`«««PRAYER_CONTEXT` therefore passes through unchanged. Nothing about the
grounding depends on this layer: the rerank answer is a validated index
into the server's candidate list (ADR 0005), so even a successful
delimiter confusion cannot introduce a passage or a text.

Benign text is unchanged, verified rather than assumed: byte-identical
over all 57 topic/reply strings of the evaluation dataset and over 11958
of the 11960 corpus prompt texts. The two exceptions are stray soft
hyphens inside words in the Ukrainian corpus (`вавилонсько­го` in
Mt 1:1-17, `По­ворот` in Mt 2:19-23) whose removal is itself a fix. The
benchmark caches are unaffected: rewrite keys are scenario + model +
prompt version, embedding keys are the query text, rerank keys are the
candidate ID list — none of them moved (`fresh_calls=0` on a cached
re-run).

## Consequences

- New modules: `app/scripture_select.py` (endpoint), `app/deadline.py`
  (time budget), `app/rate_limit.py` (shared limiter),
  `app/prompt_safety.py` (delimiter hermetisation),
  `app/passage_highlight.py` (key-verse coordinates).
- The selection now runs one extra query per request
  (`translation_verses` for the candidate chunks) so the rerank prompt can
  carry verse markers. Without it the endpoint still works and simply
  returns no `highlight`.
- Publishing `passage.verses` adds one more query to the same loader
  (`translation_titles` for the same chunk ranges, one statement per
  selection). It is guarded separately: if it fails, the verses are still
  returned, only without the section-title paragraph breaks.
- New env vars: `SCRIPTURE_SELECT_REQUESTS_PER_MINUTE`,
  `SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE`,
  `SCRIPTURE_SELECT_TIMEOUT_SECONDS`, `SCRIPTURE_INDEX_CACHE_SECONDS`.
- `GeminiQueryRewriter`, `GeminiPassageReranker` and
  `GeminiEmbeddingClient` accept a `deadline` and configurable
  timeout/attempts; defaults are unchanged for the CLI and the benchmark.
- The endpoint needs a built vector index (`app/index_cli.py rebuild`) —
  without one it answers 503, it does not degrade silently.
- The limiter is still process-local: production must run a single API
  worker (unchanged constraint, now documented in two places).
- Public contract documentation: `architect/scripture-select.md`.

## Open questions

1. Product: should `reason` (or any explanation) ever be shown? Until
   decided, it stays server-side. (Inherited from ADR 0005.) `highlight` is
   the first piece of the model's judgement that IS returned — but as
   coordinates the server verified, never as words.
2. Safe-pool size (9 places since version 1.1.0, 2026-08-28; 6 when this ADR
   was written) is small for long exclusion histories — with `history_reset`
   the client now has a defined way to start over, but a heavy user still
   cycles the pool quickly (ADR 0004 open question 3). An incomplete Bible
   sees fewer still: `npu` resolves 8 of the 9.
3. The rewrite stage is 75 % of the latency. Options if it must shrink:
   a cheaper rewrite model (fails the ADR 0004 thresholds today), fewer
   variants (costs recall), or overlapping the rerank with retrieval —
   none of them free.
4. `history_reset` is a boolean; if several corpora ever coexist (e.g. a
   staged migration), the client may need the server's current chunking
   version instead. Left out until there is a second corpus.
5. ~~A single Bible per language is indexed today, so `translation` is
   effectively fixed per language. When a second translation of one
   language is indexed, the primary-translation default ("first in index
   order") should become an explicit per-language configuration.~~
   **Closed by ADR 0007 (2026-08-27):** the default is now the explicit
   `SCRIPTURE_PRIMARY_TRANSLATIONS` configuration (falling back to the
   lowest indexed code, deterministic and identical to today's behaviour),
   and `translation` is no longer fixed per language — every ACTIVE
   translation of an indexed language can be requested. The corpus is still
   indexed once per language; a non-indexed translation is rendered from
   `translation_verses` for the same canonical window, and candidates are
   filtered by a per-translation coverage set before the rerank so the
   chosen passage always exists in it. `GET /api/scripture/v1/translations`
   publishes the list.
