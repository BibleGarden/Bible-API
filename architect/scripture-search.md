# General Bible search

Implemented for task 123pfqn46h2; ranking and expansion revised for task
123pfqn4yz5, reference numbering for task 123pfqn54zc. Contract and source references checked on 2026-11-02 against
`app/scripture_search.py` and its offline contract tests.

`POST /api/ai/scripture/search` accepts only the existing Bible Garden
`X-API-Key` (`bible-garden` identity). No key is rotated. Lampada, site and
operations keys cannot use this endpoint.

```json
{"query": "Who were the sons of Adam?", "language": "en", "translation": 16, "limit": 5}
```

`query` is trimmed, must be non-empty and at most 500 characters. `language`
is `ru`, `en` or `uk`. `translation` is an optional positive translation code
from that language's renderable catalogue; omission selects its configured
primary translation. `limit` is an integer from 1 to 10, default 5.
Unknown fields are rejected. OpenAPI is served at `/openapi.json`; the
operation ID is `search_scripture`.

The response contains `language`, the resolved `translation`, and `results`
in descending relevance order. Each result has:

- `canonical`: `canonical_id`, `book_number`, `chapter_number`,
  `verse_start`, `verse_end` in canonical coordinates;
- `passage`: `translation`, `translation_alias`, `book_number`,
  `chapter_number`, `verse_start`, `verse_end`, optional `title`, exact `text`
  and `verses` (`number`, `text`, `paragraph_start`);
- `highlight`: required key-verse range in the same format as scripture
  selection: `canonical` (`book_number`, `chapter_number`, `verse_start`,
  `verse_end`) and `passage` (`chapter_number`, `verse_start`, `verse_end`).

Open reading using **passage coordinates**, which belong to the requested
translation. Psalm numbers may differ from canonical numbers: canonical
Psalm 23 is Psalm 22 in the Synodal translation. Canonical IDs identify
windows within the current chunking version. No model-generated text,
explanation or confidence score is returned. Results may contain
fewer than `limit` passages, including an empty list.

Every result's `highlight` identifies the 1–3 consecutive verses most directly
answering the query. Highlight the entries of `passage.verses` whose `number`
is between `highlight.passage.verse_start` and `verse_end`, inclusive. Open
reading using the result's `passage.book_number`, translation and
`highlight.passage` coordinates. Both highlight boundary numbers are guaranteed
to exist in the returned verse list; they are verse numbers, not array indices.
The full passage text and coordinates remain available for context.

As in scripture selection, a canonical highlight can span more than three
verse numbers when one translation verse merges several canonical verses.
The server retains the exact mapping in both coordinate systems.

## Pipeline

This is a separate search intent over the existing corpus. It shares the
index cache, BM25, hybrid interleave/deduplication, diversity, translation
catalogue, coverage and Psalm mapping with scripture selection. It does not
call `select` or `select_final` and does not apply their prayer genre blacklist,
safe pool, comforting-text preferences or degraded answers.

1. Load the corpus, refusing a failed refresh or failed catalogue/Psalm-map
   loading instead of serving stale or degraded metadata.
2. Expand the query with the configured scripture rewrite stage and a
   separate search prompt. The query goes to that stage's provider: Gemini
   Flash-Lite in production (ADR 0016); the development server's stages are
   listed in `Deploy/local-development.md`. The model recalls up to three
   best-known passages answering the query, covering each meaning of an
   ambiguous query (Noah's ark and the ark of the covenant are both
   «ковчег»), and returns only their references: `book` (one of the 66 codes
   of `canon.CANONICAL_BOOK_CODES`, e.g. `gen`, `1pe`), `chapter`,
   `verse_start`, `verse_end` in English numbering, Psalms included. It
   writes no Bible text, so Gemini has nothing to block as a recitation
   (`RECITATION`, which made the earlier retold queries fail with 503; see
   `experiments/search-refs-2026-11-02/report.md`). A query with no
   biblical subject gets none. The answer is constrained by Gemini
   `responseSchema` (book enum, integers from 1) and validated by the server
   (`verse_start` ≤ `verse_end`). The call runs before the database
   connection is opened.
3. Map each reference to the canonical window of the request language that
   the requested translation can serve and that overlaps it most, located in
   English numbering. Psalm windows are matched by their canonical
   coordinates (ADR 0003). Other windows are matched by their stored verses
   converted from the indexed translation's own numbering
   (`versification.TRANSLATION_VERSIFICATIONS`): English Jonah 1:17 finds
   the `syn` and `ubh` window of their 2:1, Romans 16:25-27 that of `syn`
   14:24-26, Malachi 4 that of `ubh` 3:19-24, Numbers 16:36-50 that of
   `ubh` 17:1-15. The conversion uses Paratext versification files in
   `app/vrs/` (`eng`, `org`, `rso` from sillsdev/machine.py, MIT; `bsb` and
   `ubh` corrections checked against the stored texts). Corpus loading
   refuses an indexed translation without a versification or with chapter
   lengths differing from it; search then answers 503 and scripture
   selection is unaffected. A reference without a window is not an
   error, and every window still has to pass the ranker, so a wrong reference
   cannot become an unverified result. Embed the original query (the only
   embedding call). Search each window by its stored vector (semantic
   top-50: the window itself, then its nearest passages, e.g. the parallel
   Exodus 37 for Exodus 25), then the
   query by semantic top-50 and BM25 top-20. Fuse them by round-robin
   interleave with windows first in reference order (the query last),
   deduplicate canonical IDs, apply translation coverage and diversity,
   retaining up to 20 candidates.
4. Read candidate text from the database in the requested translation using
   the existing range renderer. Deduplicate identical reading ranges.
5. Use the configured scripture rerank provider/model with a separate search
   prompt, the original query and server-numbered verse markers `[1]`, `[2]`,
   etc. It returns an object keyed by candidate id (`c0`, `c1`, …) holding
   only relevant candidates: `relevance` is `direct` (the passage narrates,
   states or quotes what is asked, including the narrative of an event a
   question is about) or `related` (same event, person, object or topic,
   answered indirectly), with `key_verse_start` and `key_verse_count`
   (1–3 consecutive markers inside that candidate). Unrelated candidates are
   left out; `{}` is valid. On Gemini a `responseSchema` bounds every
   candidate's markers and span length; a repeated key is rejected. Results
   are ordered direct before related, in retrieval order within a grade.
   The server never adds unverified candidates to reach `limit`.
6. Resolve marker positions against the exact verses shown to the model,
   using the existing scripture-highlight Psalm mapping. Malformed JSON,
   an invalid expansion, unknown/duplicate candidates or grades,
   missing/out-of-range key-verse markers,
   unmappable highlights or a failed stage return 503; the server does not
   guess or silently omit the required highlight.
7. Return at most `limit` verified passages. No safe-pool or raw-only answer
   replaces a failed stage, even if some embeddings succeeded.

Both stages accept `gemini` and `openai_compat` through the existing rewrite
and rerank transports and credentials; chat completions have no counterpart
of `responseSchema`, so there the server validation alone carries the
contract. Search makes one expansion call, one embedding call and, with
candidates, one ranking call. There are no new model choices, indexes,
databases or services.
The prayer rewrite prompt, rerank prompt and `/api/ai/scripture` behavior
remain governed by their existing ADRs. Search ranking quality requires its
own human-reviewed live evaluation; prayer benchmark scores do not transfer.

The [2026-10-25 ablation](experiments/search-rewrite-2026-10-25/report.md)
removed paraphrase rewriting: on 15 queries without ambiguous ones it added no
anchor coverage and about 0.45 s. The
[2026-11-01 measurement](experiments/search-quality-2026-11-01/report.md)
(22 ru/en queries, two passes) introduced passage-recall expansion and graded
ranking. With the final prompts the primary reference reached the top 5 in
33/35 completed passes (baseline 27/33), every broad query returned at least
three passages (16/16, baseline 7/13), unrelated queries stayed empty (8/8,
baseline 6/8) and the invalid-span failures disappeared. The median request
grew by about 1.1 s, mostly the expansion call. Open cases and the partial
final pass (Gemini daily quota) are listed in that report.
The [2026-11-02 measurement](experiments/search-refs-2026-11-02/report.md)
replaced retold queries with references and stored window vectors: no
blocked expansion in 10 calls on a recitation-prone query (was 1/10), the
primary reference in the top 5 in 36/36 passes, anchors in the top 5 67/72
(was 62/72), and a median request of 2.9 s (was 3.3 s).
The [versification replay](experiments/search-versification-2026-11-02/report.md)
(offline, 13 references to differently numbered places) put the right
passage first in 13/13 for each of `en`, `ru` and `uk` (was 10, 10 and 6),
with the control set unchanged.

## Limits, privacy and errors

Search has its own process-local global/per-client rolling 60-second limiter
with its own counters: `AI_SCRIPTURE_SEARCH_REQUESTS_PER_MINUTE` (global,
default 60) and the per-client setting `AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE`
of scripture selection (default 3). Client identifiers are HMAC pseudonyms.
The overall budget is `AI_SCRIPTURE_TIMEOUT_SECONDS`, starting before corpus
loading. Each model call is additionally capped by
`AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS` and the remaining budget. Each call
has two attempts, as in scripture selection, so a hung or overloaded provider
call is cut and retried instead of consuming the whole budget; this works only
while the per-call ceiling is under about half the budget (8 s of 30 s). The
HTTP wait is bounded even if a blocking worker has not returned; the worker
checks the deadline before starting further stages.
Cancellation cannot forcibly terminate a native operation already running.

| Status | Meaning |
|---|---|
| 200 | Search completed; `results: []` means no relevant passages |
| 403 | Missing/invalid key or identity other than `bible-garden` |
| 422 | Invalid input or translation unavailable for the requested language |
| 429 | Search rate limit reached; `Retry-After` contains seconds |
| 503 | AI disabled, limiter failure, or model/index/database/rendering failure |
| 504 | Overall request time budget exhausted |

Error bodies contain only `detail`. Validation errors do not echo query
values or unknown user-controlled field names. Queries and model answers are
never logged or cached. A failure is logged as its exception chain by class
and HTTP status, e.g. `ExpansionError <- QueryRewriteError <- ReadTimeout`;
`ExpansionError`/`RankingError` name the failed model stage. An answer Gemini
blocked is logged with its finish reason (`GenerationStopped RECITATION`) and
is not retried. OpenAI-compatible stages hide the cause by design (an endpoint
may embed a key), so their chain ends at `QueryRewriteError`. Statistics omit the user agent for this private
route and retain the existing application identity and HMAC client pseudonym.
Keep Uvicorn access logging disabled as required by `AGENTS.md`.

## Verification

`tests/test_scripture_search.py` exercises the endpoint and real hybrid/range
rendering logic over a tiny synthetic corpus, with model and database stand-ins.
It covers expansion references and their validation, reference-to-window
matching (overlap, absent references, Psalm numbering, English-numbered
windows), windows leading
retrieval with a single embedding, grade ordering, unknown or
repeated candidates, the Gemini `responseSchema`, limits, genre eligibility,
translation selection, ru/en/uk, Psalm and key-verse coordinates,
deduplication, authorization, validation/privacy, empty results, partial-stage
failures, rate limiting and deadlines. `tests/test_versification.py` checks
the versification files against a snapshot of the stored chapter lengths
(`tests/data/chapter_max_verses.json`) and converts the differently numbered
places to English numbering. Existing
`test_scripture_select.py`, `test_retrieval.py`, `test_query_rewrite.py` and
`test_passage_render.py` remain regression checks for shared behavior.
GitHub Actions runs these through the ordinary offline `pytest -q` suite.

Offline tests do not establish real model relevance, empty-result accuracy
or live latency. Evaluate those separately on an agreed ru/en/uk query set
(topics, events, people, approximate quotes and unrelated requests), preserving
outputs and timings. No existing AI-Evaluation dataset or threshold is changed.
