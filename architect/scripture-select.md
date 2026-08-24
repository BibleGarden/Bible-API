# Scripture selection

Public contract of `POST /api/scripture/v1/select` — contextual Bible
passage selection for the prayer app. Design decisions and measurements:
`architect/adr/0006-scripture-select-api.md`; the pipeline behind it is
ADR 0004 (retrieval) and ADR 0005 (grounded rerank).

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
| `translation` | no | translation code; must belong to `language`; defaults to the language's primary indexed translation |

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
    "text": "Господь — Пастырь мой; я ни в чем не буду нуждаться…"
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

Store `canonical.canonical_id` and send the accumulated list back in
`exclude_canonical_ids` to avoid repeats.

The model's internal reason for the choice is a server-side diagnostic and
is never part of the response.

## Selection source and fallbacks

Degradation is part of the contract, not an error:

| `source` | `fallback_reason` | when |
|---|---|---|
| `rerank` | `null` | the AI chose among the retrieved candidates |
| `retrieval_fallback` | `rerank_failed` | the AI choice failed (timeout, transport, malformed answer) |
| `retrieval_fallback` | `no_reranker` | the rerank stage is not configured |
| `retrieval_fallback` | `deadline` | the time budget ran out before the AI choice |
| `safe_pool` | `empty_topic` | no topic and no replies were sent |
| `safe_pool` | `ai_unavailable` | no query could be embedded (provider outage) |
| `safe_pool` | `deadline` | the time budget ran out before retrieval could run |

The safe pool is a curated, versioned list of comforting passages
(`app/data/safe_pool.json`) rotated with the same exclusion list.

## Errors

| code | when |
|---|---|
| 403 | invalid or missing API key |
| 422 | unknown field; oversized topic, reply, reply total or exclusion list; malformed canonical ID; unsupported language; translation that does not belong to the language |
| 429 | global or per-client request limit exceeded (with `Retry-After`) |
| 503 | no verified passage can be produced: database unavailable, vector index empty (run `app/index_cli.py rebuild`), or the rate limiter is misconfigured |

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

## Time budget

One selection is bounded by `SCRIPTURE_SELECT_TIMEOUT_SECONDS` (default
15 s): stages that cannot finish are skipped in favour of a verified
fallback, and every provider call is capped by what is left of the budget.
Measured production latency (warm process): median 6.2 s, max 6.6 s; the
query rewrite is ~75 % of it. The six query embeddings run concurrently.

## Rate limiting and observability

Two 60-second windows, independent of the Twinkler budget because one
selection costs ~8 provider calls:

- global: `SCRIPTURE_SELECT_REQUESTS_PER_MINUTE` (default 10);
- per client address: `SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE`
  (default 3).

The in-memory client identifier is an HMAC-SHA-256 pseudonym built with
`TWINKLER_CLIENT_HMAC_KEY` (shared with the Twinkler endpoints); the
address itself is not retained. Missing HMAC configuration fails closed
with 503. Counters are process-local, so production runs a single API
worker. Client addresses come from the direct peer; `X-Forwarded-For` is
honoured only for peers listed in `TRUSTED_PROXY_IPS`.

Standard request statistics store the endpoint, method, status, latency, an
HMAC pseudonym truncated to 40 hexadecimal characters, and an empty user
agent. The prayer topic, the replies **and the selected passage** are never
stored or logged: the reference alone is not private, but next to a client
identity it would reveal what the person prayed about. Logs carry failure
categories only.

## Caching

The vector index and the per-language BM25 index are cached in the process
for `SCRIPTURE_INDEX_CACHE_SECONDS` (default 1 hour, minimum 1 second) —
they depend only on the corpus, cost ~0.9 s to build and are identical for
every request. `POST /api/cache/clear` drops them immediately, so a rebuilt
index can be published without restarting the service. If a refresh fails,
the previous copy keeps being served (a 503 only happens when there is no
cached corpus at all). Nothing derived from a request — rewrites,
embeddings, candidates, chosen passages — is ever cached.
