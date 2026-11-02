# General-search expansion: paraphrase instead of recitation — 2026-11-02

Task: ClickUp 123pfqn4yz5 (parent 123pfqn4yyg). Baseline: Bible-API `516df79`
(`feat/123pfqn46h2-semantic-search`).

## Problem

The expansion prompt asked for a query "worded like that passage itself".
Gemini then sometimes quoted a Bible translation and blocked its own answer
with `finishReason: RECITATION`: HTTP 200 without content. The client raised
"no candidates" (`KeyError` in the log), the call was not retried and search
answered 503 within about 1 s. On the stand, "How can I cope with worry?" was
blocked in 1 of 3 calls.

## Method

Same tool and control set as
[search-quality-2026-11-01](../search-quality-2026-11-01/report.md):
`measure.py run` on its `queries.json` (22 queries), two passes, 8 s pause,
calling `_run_search` in a disposable container on the development corpus.
Rewrite and rerank: `gemini-3.5-flash-lite` (production model), free-tier key,
per-call ceiling 8 s, two attempts per call.

| File | Expansion prompt |
|---|---|
| `before-516df79.jsonl` | "worded like that passage itself" |
| `after-paraphrase.jsonl` | retell with distinctive words; never copy more than three consecutive words |
| `after-paraphrase-names.jsonl` | as above, but keep the exact key nouns and names of people, places and objects (final) |

## Results

| Metric | Before | Paraphrase | Paraphrase + names |
|---|---|---|---|
| Errors (of 44) | 4 (1 blocked answer, 3 timeouts) | 0 | 0 |
| Primary passage in top 5 | 31/32 | 33/36 | 36/36 |
| Primary among candidates | 32/32 | 33/36 | 36/36 |
| Broad queries with ≥3 results | 14/14 | 16/16 | 16/16 |
| Unrelated queries empty | 8/8 | 8/8 | 8/8 |
| Median seconds | 3.67 | 3.43 | 3.28 |

Successful runs only; errored runs are excluded from the hit counts.

- The plain paraphrase dropped decisive words: one Russian ark pass retold
  Genesis 6 as "a righteous builder… a huge vessel" without «ковчег» and lost
  it, and the Russian neighbour query lost Leviticus 19 in both passes.
  Keeping the passage's key nouns and names fixed both.
- The final prompt also found Leviticus 19 for "О любви к ближнему" in both
  passes, which the 2026-11-01 report left unresolved.
- The gain comes from keeping the key words, not from the paraphrase: the
  model still often quotes a translation verbatim (Matthew 6:34, 1 Peter 5:7,
  Ecclesiastes 3:2 in the final run). The blocked "before" run recorded only
  `KeyError: 'parts'`; its finish reason was seen as RECITATION on the stand.

## Block rate

`blockrate.py` ran the expansion call 10 times per query on each prompt and
counted Gemini finish reasons:

| Query | Old prompt | Final prompt |
|---|---|---|
| How can I cope with worry? | 4/10 RECITATION | 1/10 RECITATION |
| Who built the ark? | 0/10 | 0/10 |

The paraphrase lowers blocking on a recitation-prone query but does not
remove it; the samples are small. A blocked answer is logged as
`GenerationStopped RECITATION`, is not retried and fails the search with 503.
