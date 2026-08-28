# Scripture selection

Public contract of `POST /api/scripture/v1/select` — contextual Bible
passage selection for the prayer app — and of
`GET /api/scripture/v1/translations`, which lists the translations it can
render. Design decisions and measurements:
`architect/adr/0006-scripture-select-api.md` and
`architect/adr/0007-reference-translation-rendering.md`; the pipeline behind
them is ADR 0004 (retrieval) and ADR 0005 (grounded rerank).

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
| `translation` | no | translation code; must be one of the codes `GET /api/scripture/v1/translations` lists for `language`; defaults to that language's `primary` |

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

```
GET /api/scripture/v1/translations
```

```json
{
  "languages": [
    {"language": "en", "translations": [
      {"code": 16, "alias": "bsb", "primary": true},
      {"code": 17, "alias": "webus", "primary": false},
      {"code": 779, "alias": "webbe", "primary": false}]},
    {"language": "ru", "translations": [
      {"code": 1, "alias": "syn", "primary": true},
      {"code": 11, "alias": "bti", "primary": false}]},
    {"language": "uk", "translations": [
      {"code": 20, "alias": "ubh", "primary": true},
      {"code": 21, "alias": "npu", "primary": false}]}
  ]
}
```

Every code listed here is accepted in `translation`; anything else is
rejected with 422. `primary` is the one served when the field is omitted.
Names, descriptions and audio voices of the same translations are in
`GET /api/translations`, joined by `code`.

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
| 422 | unknown field; oversized topic, reply, reply total or exclusion list; malformed canonical ID; unsupported language; a translation that is not listed for the language by `GET /api/scripture/v1/translations` (another language's, inactive, unknown, or not renderable from the canonical corpus) |
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

## Time budget

One selection is bounded by `SCRIPTURE_SELECT_TIMEOUT_SECONDS` (default
15 s): stages that cannot finish are skipped in favour of a verified
fallback, and every provider call is capped by what is left of the budget.
Measured production latency (warm process): median 6.2 s, max 6.6 s; the
query rewrite is ~75 % of it. The six query embeddings run concurrently.

## Rate limiting and observability

`GET /api/scripture/v1/translations` is a cached, prayer-independent read
and has no rate limit of its own. The limits below are the selection's.

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

The vector index, the per-language BM25 index, the Psalm versification maps,
the renderable-translation catalogue and the per-translation coverage sets
are cached in the process for `SCRIPTURE_INDEX_CACHE_SECONDS` (default
1 hour, minimum 1 second) — they depend only on the corpus, cost ~1.4 s to
build and are identical for every request. `POST /api/cache/clear` drops them immediately, so a rebuilt
index can be published without restarting the service. If a refresh fails,
the previous copy keeps being served (a 503 only happens when there is no
cached corpus at all). Nothing derived from a request — rewrites,
embeddings, candidates, chosen passages — is ever cached.
