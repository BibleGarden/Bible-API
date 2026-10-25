# General Bible search

Implemented for task 123pfqn46h2. Contract and source references checked on
2026-10-25 against `app/scripture_search.py` and its offline contract tests.

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
2. Embed the original query once. General search makes no rewrite request
   and does not send its query to Cerebras.
3. Require successful embedding and combine semantic top-50 with BM25 top-20
   for that same original query. Deduplicate canonical IDs, apply
   translation coverage and diversity, retaining up to 20 candidates.
4. Read candidate text from the database in the requested translation using
   the existing range renderer. Deduplicate identical reading ranges.
5. Use the configured scripture rerank provider/model with a separate search
   prompt and server-numbered verse markers `[1]`, `[2]`, etc. In the same
   call it returns `matches`: unique candidate indices in relevance order,
   each with `key_verse_start`/`key_verse_end` marker positions. Each span
   must cover 1–3 consecutive markers inside its own candidate. Irrelevant
   passages are omitted; an empty list is valid. No extra model call is added.
6. Resolve marker positions against the exact verses shown to the model,
   using the existing scripture-highlight Psalm mapping. Malformed JSON,
   invalid/duplicate candidates, missing/out-of-range key-verse markers,
   unmappable highlights or a failed stage return 503; the server does not
   guess or silently omit the required highlight.
7. Return at most `limit` verified passages. No safe-pool or raw-only answer
   replaces a failed stage, even if some embeddings succeeded.

Both `gemini` and `openai_compat` use the existing rerank-stage transports and
credentials. With candidates, search makes one embedding call and one ranking
call. There are no new model choices, indexes, databases or services.
The prayer rewrite prompt, rerank prompt and `/api/ai/scripture` behavior
remain governed by their existing ADRs. Search ranking quality requires its
own human-reviewed live evaluation; prayer benchmark scores do not transfer.

The [2026-10-25 rewrite ablation](experiments/search-rewrite-2026-10-25/report.md)
compared the same 18 synthetic queries in both modes. Direct search found
reference anchors among candidates in 13/15 positive cases versus 12/15 with
rewriting. Among 12 paired successful positive cases it returned anchors in
10 versus 9 and reduced median latency from 2.423 to 1.970 seconds. This small
set supports removing the extra stage for the development API; it is not a
general quality guarantee. Gemini 429 responses prevented a complete comparison
of unrelated queries, and the Russian abstract state-collapse query remains
unresolved in both modes. Preserved inputs and results are linked in the report.

## Limits, privacy and errors

Search has its own process-local global/per-client rolling 60-second limiter,
using `AI_SCRIPTURE_REQUESTS_PER_MINUTE` and
`AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE`. Client identifiers are HMAC
pseudonyms. The overall budget is `AI_SCRIPTURE_TIMEOUT_SECONDS`, starting
before corpus loading. Each model call is additionally capped by
`AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS` and the remaining budget. Each call
has one attempt. The HTTP wait is bounded even if a blocking worker has not
returned; the worker checks the deadline before starting further stages.
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
never logged or cached. Statistics omit the user agent for this private
route and retain the existing application identity and HMAC client pseudonym.
Keep Uvicorn access logging disabled as required by `AGENTS.md`.

## Verification

`tests/test_scripture_search.py` exercises the endpoint and real hybrid/range
rendering logic over a tiny synthetic corpus, with model and database stand-ins.
It covers ordering, limits, genre eligibility, translation selection, ru/en/uk,
Psalm and key-verse coordinates, deduplication, authorization, validation/privacy, empty
results, partial-stage failures, rate limiting and deadlines. Existing
`test_scripture_select.py`, `test_retrieval.py`, `test_query_rewrite.py` and
`test_passage_render.py` remain regression checks for shared behavior.
GitHub Actions runs these through the ordinary offline `pytest -q` suite.

Offline tests do not establish real model relevance, empty-result accuracy
or live latency. Evaluate those separately on an agreed ru/en/uk query set
(topics, events, people, approximate quotes and unrelated requests), preserving
outputs and timings. No existing AI-Evaluation dataset or threshold is changed.
