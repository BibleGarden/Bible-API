# General-search quality: ranker grades and query expansion — 2026-11-01

Task: ClickUp 123pfqn4yz5 (parent 123pfqn4yyg). Baseline: Bible-API `d9acc57`
(`feat/123pfqn46h2-semantic-search`); changes measured from the uncommitted
worktree of `feat/123pfqn4yz5-search-ranking`.

## Method

`measure.py run` calls `scripture_search._run_search` directly (no HTTP, auth or
rate limit) in a disposable container built from the development API image, on
the development corpus and database, limit 5, 30-second budget, sequential, 5 s
pause, no retries. Each of the 22 queries in `queries.json` ran twice (two full
passes). Per pass it records the retrieval queries, the 20 candidate canonical
IDs, every model call (stage, model, HTTP statuses, raw answer, seconds), final
results and errors. `measure.py summary` computes the metrics below.

- Set: the 12 acceptance queries of the task (6 intents × ru/en, including the
  unrelated "borscht recipe") plus 10 earlier queries (sea crossing, forgiving
  enemies, Jonah, "a season for everything", Wi-Fi router) from the
  [2026-10-25 experiment](../search-rewrite-2026-10-25/report.md).
- Anchors are non-exhaustive canonical ranges; a hit is any overlap with a
  returned canonical range. The first anchor of each query is its primary one
  (Genesis 6 for "who built the ark"). New Testament book numbers follow
  `app/canon.py` (Synodal order: Romans 52, Philippians 57, 1 Peter 46,
  Hebrews 65); the 2026-10-25 Romans 12 anchor used 45 (James) and is corrected.
- Broad queries (at least 3 results expected): ark of the covenant, worry,
  loving your neighbour, forgiving enemies.
- Models: ranking `gemini-3.5-flash-lite` (development configuration). The
  development rewrite stage is Cerebras; for these runs the container
  environment set it to `gemini` / `gemini-3.5-flash-lite`, the production
  selection of ADR 0016, reusing the rerank key from the running container's
  environment (never written to disk). The development stand configuration was
  not changed.

## Arms

| Arm | Expansion (rewrite stage) | Ranking answer |
|---|---|---|
| baseline | none | `matches` list of indices with `key_verse_start/end`, no schema |
| A `after-a-senses` | only for ambiguous queries: one Bible-worded query per meaning, original query first | `{"direct": [...], "related": [...]}` lists, `responseSchema` |
| B `after-b-passages` | up to 3 best-known passages answering any biblical query (each meaning of an ambiguous one), original first | as A |
| C `after-c-grades` | as B | object keyed by candidate id with only relevant candidates, `responseSchema` with per-candidate bounds |
| D `after-d-order` | as B, expansions first and original query last | as C, plus "a passage narrating the event a question asks about is direct" |
| final `after` | as D, embeddings concurrent, expansion query cap 1000 characters | as D |

## Results

| Metric | baseline | A | B | C | D | final (partial) |
|---|---:|---:|---:|---:|---:|---:|
| Completed passes | 41/44 | 42/44 | 37/44 | 44/44 | 43/44 | 16/44 |
| Primary anchor in top 5 | 27/33 | 32/34 | 29/29 | 32/36 | 33/35 | 12/14 |
| Any anchor in top 5 | 29/33 | 32/34 | 29/29 | 35/36 | 35/35 | 14/14 |
| Anchors returned | 37/65 | 47/67 | 49/52 | 61/72 | 63/71 | 25/30 |
| Primary anchor among candidates | 29/33 | 32/34 | 29/29 | 36/36 | 35/35 | 14/14 |
| Broad queries with ≥3 results | 7/13 | 11/14 | 11/11 | 16/16 | 16/16 | 7/7 |
| Unrelated queries empty | 6/8 | 8/8 | 8/8 | 8/8 | 8/8 | 2/2 |
| Mean results | 1.71 | 2.67 | 3.00 | 2.84 | 2.72 | 3.12 |
| Median seconds (completed) | 2.114 | 3.163 | 2.996 | 3.193 | 3.551 | 3.477 |

Per query and pass: `P` primary anchor hit, `A` any anchor hit, digit = number
of results, `E` empty, `X` unrelated query with results.

| Query | baseline | D | final |
|---|---|---|---|
| en-noah-ark | -A1 -A1 | PA3 PA2 | -A1 429 |
| ru-noah-ark | --1 --1 | PA1 PA3 | PA2 429 |
| en-covenant-ark | PA5 PA5 | PA5 PA5 | PA5 429 |
| ru-covenant-ark | error error | PA5 PA5 | PA5 429 |
| en-worry | PA2 error | PA5 PA5 | timeout 429 |
| ru-worry | --1 --2 | PA4 PA5 | PA4 429 |
| en-neighbour | PA3 PA3 | PA5 PA5 | PA5 429 |
| ru-neighbour | PA5 PA5 | -A5 -A5 | -A5 429 |
| en-goliath | PA3 PA2 | error PA1 | PA5 429 |
| ru-goliath | PA1 PA1 | PA1 PA2 | PA4 429 |
| en-borscht | X1 X1 | E E | E 429 |
| ru-borscht | E E | E E | E 429 |
| en-sea | PA2 PA3 | PA4 PA5 | PA4 429 |
| ru-sea | PA2 PA2 | PA3 PA2 | PA2 429 |
| en-enemies | PA3 PA2 | PA4 PA5 | PA4 429 |
| ru-enemies | PA2 PA2 | PA5 PA3 | PA3 429 |
| en-jonah | PA1 PA1 | PA5 PA1 | PA1 429 |
| ru-jonah | PA1 PA1 | PA1 PA1 | 429 429 |
| en-season | PA1 PA1 | PA1 PA1 | 429 429 |
| ru-season | PA1 PA1 | PA2 PA2 | 429 429 |
| en-router | E E | E E | 429 429 |
| ru-router | E E | E E | 429 429 |

Median stage seconds (completed passes): ranking 0.957 → 1.254 (D) / 1.455
(final); expansion 0.999 (D) / 1.038 (final); retrieval 0.125 → 0.410 (D,
sequential embeddings) / 0.238 (final, concurrent). On the 14 queries completed
in all of baseline, D and final, the median request time was 2.067, 3.555 and
3.199 seconds: the final implementation adds about 1.1 s, slightly above the
task's "~1 s" target. The expansion call is about 1.0 s of it.

### Errors

No call was retried; every failure below is an explicit stage error (503).

- baseline: 3 `ValueError` — key-verse spans of 4–7 verses and one repeated
  candidate.
- A: 1 repeated candidate, 1 Gemini 429. B: 7 repeated candidates (16%): the
  list format cannot prevent repeats, and more relevant candidates made them
  frequent. C: none. D: 1 expansion query longer than the 500-character cap
  (a full quotation of 1 Samuel 17); the final cap is 1000, as in `81a6c94`.
- final: 27 Gemini 429 and 1 read timeout. The 429s are the development
  project's daily free-tier quota (`GenerateRequestsPerDayPerProjectPerModel-FreeTier`,
  one probe at 14:08 UTC, retry delay about 18 hours), exhausted by this
  day's measurements. The development stand on port 9084 uses the same quota
  until it resets. The final code therefore has one partial pass; its quality
  is represented by D, which has the same prompts and answer formats.

### Ranking answer format and latency

Side measurement on real candidates for two to four queries, three calls each,
same model (seconds per ranking call): every candidate graded with all fields
required 2.0–2.4; the same with short keys 1.5–1.9; direct/related lists
0.9–1.2; only relevant candidates keyed by id (selected) 0.8–1.7, `{}` for the
unrelated query. The keyed object keeps per-candidate grading and bounds while
omitting unrelated candidates, so the answer stays short. Gemini emits object
properties in schema order, so a candidate cannot repeat; a repeated key in the
raw JSON is still rejected.

## Decision

- Ranking: grades `direct`/`related` per candidate, unrelated candidates
  omitted, `responseSchema` bounds the markers of each candidate and the span
  length. The invalid-span and repeat failures of the baseline are no longer
  possible on Gemini; the server still validates everything.
- Related results are returned without requiring a direct one. On D the kept
  candidates were 151 direct and 23 related; unrelated queries kept none (0 of
  160 candidates) and every broad query had at least 3 results. The data gives
  no reason to suppress related passages.
- Expansion is kept, in the passage-recall form (B–final). Senses-only
  expansion (A) fixed the ambiguous ark queries but not the vocabulary gap of
  "Как справиться с тревогой?" (Matthew 6, Philippians 4 and 1 Peter 5 were
  never retrieved; Synodal wording is «не заботьтесь»). Passage recall
  retrieves them in every completed pass. Unlike the removed 2026-10-25
  paraphrases, it names a real passage first and is worded like it, the method
  measured for prayer rewriting (ADR 0004, prompt v8).
- Latency is the open trade-off: about +1.1 s median, mostly the expansion
  call. Accepting it, or a faster rewrite model, is a human decision; changing
  models is outside this task (ADR 0016).

## Unresolved

- ru-neighbour: Leviticus 19:18 is retrieved but falls outside the top 5 in D
  and final (eleven candidates graded direct, ordered by retrieval); Matthew 22
  and Romans 13 are returned.
- en-noah-ark: one of three passes with the final prompt returned only
  Hebrews 11 (the ranker still reads "who built" literally at times).
- Hebrews 11:7 never reaches the candidates for the Russian ark question.
- Not covered: Ukrainian, abstract history (the 2026-10-25 split query) and an
  independent relevance review. Human review on the test stand is pending.
- Rerun the final code in two passes after the Gemini quota resets, or with
  the paid production project, before release.

## Reproduction

Inside the worktree, with the rewrite stage switched as described above
(`<env>` prints the running container's environment with the
`AI_SCRIPTURE_REWRITE_*` lines replaced; no key is stored):

```bash
docker run --rm --network container:bible-api --env-file <(<env>) \
  -e PYTHONPATH=/code/app -v "$PWD":/code -v /root/models:/models \
  -w /code/app bible-api-bible-api python \
  ../architect/experiments/search-quality-2026-11-01/measure.py run \
  ../architect/experiments/search-quality-2026-11-01/queries.json \
  ../architect/experiments/search-quality-2026-11-01/<output>.jsonl --label <label>
python3 measure.py summary queries.json <output>.jsonl
```

The script refuses to overwrite output and makes billable calls; it must never
run as a unit test or against production. Arms A–D were measured on successive
working-tree states that are not preserved as code; their raw answers are in
the JSONL files.

Artifact SHA-256:

- `queries.json`: `b156df6ce622e47453ab4cf09836e2275d1beac9c2112d7309b321069ee96c9f`
- `baseline-d9acc57.jsonl`: `d1ef236f61bf841cde31162f2552b3f19de471f77015db325a83a925fc102d9a`
- `after-a-senses.jsonl`: `e137eb13bb7ea5b4e8619e2cb0cbba526f1bdb4d3ca44d519c98516fd26445bd`
- `after-b-passages.jsonl`: `25c87a00c5589c5818e6343bf80ac894166c6c75418f678065fe00eae39a5f5e`
- `after-c-grades.jsonl`: `07a01905dfbbb054d0344f484ea191123ef712ca9bf653ad6b7c4b0fb542079a`
- `after-d-order.jsonl`: `1932ebd0b7e94f5474593b12b330fdd7d608feacd0eebe11fc596aa3be6db297`
- `after.jsonl`: `8d802a33b936d94fda499977d7a373a0e13382463127f8280b90775dc70350c2`
