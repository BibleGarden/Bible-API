# General-search expansion: references instead of text — 2026-11-02

Task: ClickUp 123pfqn4yz5 (parent 123pfqn4yyg). Baseline: Bible-API `8321c85`
(`feat/123pfqn46h2-semantic-search`, paraphrase prompt of
[search-paraphrase-2026-11-02](../search-paraphrase-2026-11-02/report.md)).

## Problem

The expansion asked Gemini for a query retelling each passage. The model
still quoted translations at times and Gemini blocked its own answer
(`finishReason: RECITATION`, HTTP 200 without content), so search answered
503: 1 of 10 calls on "How can I cope with worry?". The filter cannot be
switched off.

## Change

The expansion returns only references, constrained by `responseSchema`:
`book` (enum of the 66 `canon.CANONICAL_BOOK_CODES`), `chapter`,
`verse_start`, `verse_end` in English numbering. Each reference maps to the
canonical window of the request language that overlaps it most; a reference
the corpus lacks yields nothing. With no Bible text in the answer there is
nothing to recite.

## How windows enter retrieval

`variants.py` replays, without any Gemini call, the references recorded in
`after-paraphrase-names.jsonl` (36 positive runs, 72 anchors) through three
designs; `variants.jsonl` holds the output. Ranks are candidate positions
before the ranker, which keeps retrieval order within a grade.

| Design | Primary anchor in candidate top 3 | Anchors among candidates | Anchors in candidate top 5 | Median retrieval s |
|---|---|---|---|---|
| Recorded (retold queries) | 32/36 | 66/72 | 53/72 | 0.21 (live) |
| Window text as queries (embedding + BM25 each) | 36/36 | 69/72 | 63/72 | 3.52 |
| Windows pinned ahead of the query | 36/36 | 63/72 | 61/72 | 0.10 |
| Stored window vectors as semantic queries | 36/36 | 67/72 | 63/72 | 0.12 |

The stored vector of a window is the embedding of its own text, so searching
by it puts the window first and then its nearest passages (Exodus 37 for
Exodus 25, Mark 12 for Leviticus 19) without an embedding call. Pinning alone
loses those parallels; embedding the window texts finds two more anchors at
about 3.4 s extra per request. Production uses stored vectors: one embedding
call (the query) instead of four.

## Live result

`measure.py run` on the 22-query control set, two passes, 8 s pause, in a
disposable container on the development corpus; rewrite and rerank
`gemini-3.5-flash-lite` (production model), free-tier key, per-call ceiling
8 s, two attempts. Output: `after-refs.jsonl`. Compared with
`after-paraphrase-names.jsonl`:

| Metric | Paraphrase + names | References |
|---|---|---|
| Errors (of 44) | 0 | 0 |
| Primary passage in top 5 | 36/36 | 36/36 |
| Anchors in top 5 | 62/72 | 67/72 |
| Primary among candidates | 36/36 | 36/36 |
| Broad queries with ≥3 results | 16/16 | 16/16 |
| Unrelated queries empty | 8/8 | 8/8 |
| Median seconds | 3.28 | 2.92 |

- All 44 answers were valid references; the unrelated queries got
  `passages: []`.
- Ambiguous "ark" queries list both meanings (Genesis 6, Hebrews 11,
  Exodus 25/37); the ranker keeps only Noah for "Who built the ark?".
- Median expansion call 0.88 s (was 1.03 s), retrieval 0.13 s (was 0.21 s).
- Open: one Russian ark pass named no Hebrews 11, so that secondary anchor
  was missed; the neighbour queries still miss Luke 10, as before.

## Block rate

`blockrate.py` ran the expansion call 10 times on "How can I cope with
worry?": 10/10 `STOP`, no `RECITATION` (paraphrase prompt: 1/10, original
prompt: 4/10). Every answer named Matthew 6:25-34, Philippians 4:6-7 and
1 Peter 5:7.
