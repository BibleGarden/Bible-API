# ADR 0004: Scripture-selection retrieval pipeline

Status: accepted (2026-08-24), thresholds passed on the approved benchmark.
Ticket: ClickUp 86cb8vw1g

> Note (2026-08-30, ClickUp 86cbbmy8d): the variables of this pipeline were
> renamed to carry the `AI_SCRIPTURE_` prefix of the method they configure —
> `RETRIEVAL_REWRITE_MODEL` → `AI_SCRIPTURE_REWRITE_MODEL`,
> `RETRIEVAL_REWRITE_API_KEY` → `AI_SCRIPTURE_REWRITE_API_KEY`,
> `RETRIEVAL_RERANK_MODEL` → `AI_SCRIPTURE_RERANK_MODEL`,
> `SCRIPTURE_SELECT_TIMEOUT_SECONDS` → `AI_SCRIPTURE_TIMEOUT_SECONDS`. Names
> only: the pinned models, the rewrite-vs-shared-key rule and every threshold
> below are unchanged, and the text uses the new names throughout. Benchmark
> reports produced before that date name the same knobs by their old ones.

> Note (2026-09-05, ClickUp 86cbegg2f, ADR 0009): "Gemini" below names the
> transport this pipeline was measured on, not a requirement. The rewrite
> stage now takes its transport from `AI_SCRIPTURE_REWRITE_PROVIDER`
> (`gemini` or `openai_compat`) and is built by
> `query_rewrite.build_query_rewriter`; the prompt, the parser, the
> variant count, the fallbacks and every threshold below are untouched —
> `GeminiQueryRewriter` and `OpenAICompatQueryRewriter` share all of them and
> send byte-identical messages. Which model passes the thresholds is still a
> measurement, and the pinned Gemini values below are the ones that have been
> measured.

> Note (2026-09-05, ClickUp 86cbegg36): **the rewrite prompt is v8 now.**
> Everything below about the pipeline holds; what changed is the wording of
> step 2 and the shape of its answer. See "Prompt v8" at the end of this
> file. The measurements in this ADR were taken on v7 with
> `gemini-3.7-flash` and have NOT been re-taken on v8 — the free daily quota
> of that model is 20 requests and it ran out after 12 of the 21 scenarios.

## Context

The retrieval layer must turn a prayer context (topic + allowed user replies
from the Twinkler dialog) into a ranked top-K of Bible passage candidates —
with canonical coordinates, exact texts from the DB and diagnostic scores —
for the downstream LLM reranker (86cb8vw1h, out of scope here).

Acceptance criterion (moved by Maria from the embeddings stage): the
retrieval layer as a whole must pass the approved retrieval thresholds
(`evaluation/thresholds.json` v0.2.0) on the approved dataset
(`evaluation/scenarios.json` v0.2.0, 24 scenarios): hit_rate@10 >= 0.90,
recall@10 >= 0.60, MRR >= 0.60, unacceptable@10 <= 0.05.

Starting point: raw embedding search over the c3 index
(`gemini-embedding-001@768`, ADR 0002/0003) scores 0.208 / 0.066 / 0.082 /
0.004 — an order of magnitude short. The ADR 0002 diagnostic identified the
dominant failure: the register gap between everyday prayer wording and
biblical language (raw query ranks the reference passage ~356th; a
scripture-styled rephrasing of the same intent ranks it 4th).

Maria's design decisions incorporated here:

- **Global genre blacklist** (question 6): versioned repo file, canonical
  coordinates, range-intersection blocking at retrieval for ALL queries;
  seeded with the dataset's genre traps (genealogies/censuses, offering
  lists, imprecatory fragments, covenant curses) plus obvious same-genre
  representatives; the dataset traps are regression tests of the filter.
- **Safe pool** (question 5): versioned list of the relevant/acceptable
  places of scenarios ru-009/en-006/uk-006; used for an empty topic (no
  retrieval, no Gemini) and as the deterministic fallback when AI is
  unavailable for any topic; rotation with exclusion of already-shown.

## Architecture

Modules (all in `app/`):

| module | role |
|---|---|
| `query_rewrite.py` | Gemini rewrite of the prayer context into 6 scripture-register query variants (`GeminiQueryRewriter`); raw-query builder |
| `lexical_index.py` | in-process BM25 over `title + text` per language (hybrid lexical signal) |
| `retrieval.py` | pipeline: fusion, genre blacklist, safe pool, diversity, `ScriptureRetriever` service |
| `retrieval_cli.py` | end-to-end smoke CLI against live DB + Gemini |
| `data/genre_blacklist.json` | versioned global genre blacklist (canonical coords) |
| `data/safe_pool.json` | versioned safe pool (canonical coords) |

`ScriptureRetriever.select(SelectionRequest)`:

1. **Raw query** = topic + replies. Empty -> safe pool (`empty_topic`),
   zero Gemini calls.
2. **Rewrite** (the main quality lever): Gemini
   (`AI_SCRIPTURE_REWRITE_MODEL`, pinned by the benchmark to gemini-3.7-flash
   and set explicitly in the environment, temperature 0,
   JSON output, prompt v8 — v7 when this ADR was written, see "Prompt v8"
   below) recalls well-known passages fitting the
   situation and writes 6 near-quote paraphrases in the register of the
   indexed translation (syn/bsb/ubh), ordered most-central-first, each a
   different spiritual angle. The prompt is generic — it never sees the
   evaluation dataset; its worked examples are de-fingerprinted against it by
   a test. On rewrite failure the raw query alone is searched
   (`rewrite_failed` flag).
3. **Hybrid search per variant**: embedding (`RETRIEVAL_QUERY`, 768d) ->
   exact cosine over the c3 index with the language filter (top-50), plus
   BM25 top-20; lexical hits get their true cosine (vectors are in memory)
   and are merged into the variant ranking sem1, lex1, sem2, lex2, …
   If NO variant can be embedded, Gemini is down -> safe pool
   (`ai_unavailable`): raw embedding search is impossible without the API
   embedder, so the pool IS the deterministic no-AI retrieval fallback.
4. **Interleave fusion**: round-robin across variants (rank-1 of each
   variant in the model's centrality order, then rank-2, …), deduplicated
   by canonical chunk ID — which also groups identical canonical places
   across translations (shared plan-slot IDs, ADR 0003).
5. **Filters**: already-shown canonical IDs (repeat exclusion), then the
   global genre blacklist by canonical-range intersection.
6. **Diversity**: greedy selection keeping fusion order with caps — max 1
   candidate per chapter (near-duplicate windows are redundant for the
   reranker and starve the book quota) and max 4 per book (thresholds
   `diversity.max_share_single_book_in_window` = 0.4); skipped candidates
   backfill if the caps leave the list short.
7. **Resolution**: every translation of the language contributes its own
   verse coordinates and exact text from `translation_chunks` (one query
   per translation). Candidates carry canonical coordinates
   (english-masoretic for Psalms), fused score, best variant and
   per-variant scores.

Passage windows: chunk boundaries ARE natural passage boundaries by
construction (ADR 0001 — section/paragraph aligned, never crossing chapters
or titles), so no additional window expansion is performed; the candidate's
window is the chunk.

Privacy: prayer context and rewrite variants are sent to Gemini (pre-cleared
— the same data already goes there for Twinkler) but are never logged; only
failure categories are.

## Measurements (approved dataset v0.2.0, thresholds v0.2.0)

Benchmark: `evaluation/retrieval_benchmark.py pipeline` — runs the full
pipeline over the production modules with cached corpus embeddings
(MRL-identical to the production index); every Gemini call is disk-cached,
so ablation re-runs are free. Defaults reproduce the approved
configuration.

### Final configuration vs thresholds

hit@10 / recall@10 / MRR / unacc@10 (thresholds: >=0.90 / >=0.60 / >=0.60 /
<=0.05):

| group | n | hit@10 | recall@10 | MRR | unacc@10 |
|---|---|---|---|---|---|
| **ALL** | 24 | **0.958** | **0.753** | **0.650** | **0.000** |
| no-empty | 21 | 0.952 | 0.718 | 0.600 | 0.000 |
| ru | 10 | 1.000 | 0.858 | 0.750 | 0.000 |
| en | 7 | 1.000 | 0.738 | 0.586 | 0.000 |
| uk | 7 | 0.857 | 0.619 | 0.571 | 0.000 |

All four thresholds PASS. A re-sampling run (fresh rewrites, temperature 0)
gives 0.958 / 0.729 / 0.684 / 0.004 — also passing; the configuration is
stable, not a lucky sample. The single miss (uk-007) is dominated by
annotation coverage: the produced candidates are legitimate passages for
the topic that the non-exhaustive reference set does not list.

### Component contributions (ablations from the final configuration)

| configuration | hit@10 | recall@10 | MRR | verdict |
|---|---|---|---|---|
| raw embedding search (ADR 0002 baseline) | 0.208 | 0.066 | 0.082 | fail |
| final minus rewrite (raw + lexical + fusion + pool) | 0.292 | 0.177 | 0.188 | fail |
| final with gemini-3.5-flash-lite rewrites | 0.875 | 0.556 | 0.517 | fail |
| final with max-cosine fusion | 0.958 | 0.663 | 0.568 | fail (MRR) |
| final with RRF fusion | 0.750 | 0.413 | 0.356 | fail |
| final minus lexical (BM25) | 0.958 | 0.691 | 0.650 | pass, −0.06 recall |
| final minus safe pool (empty topics searched) | 0.917 | 0.684 | 0.588 | fail (MRR) |
| final minus diversity caps | 0.958 | 0.771 | 0.650 | pass (+0.02 recall) |
| final minus blacklist | 0.958 | 0.753 | 0.650 | pass (guardrail, see below) |
| **final** | **0.958** | **0.753** | **0.650** | **PASS** |

Reading:

- **The LLM rewrite is the dominant lever** (+0.67 hit@10 alone), exactly
  as the ADR 0002 diagnosis predicted. Rewrite quality is decisive:
  gemini-3.5-flash-lite fails the thresholds with the identical pipeline —
  hence `AI_SCRIPTURE_REWRITE_MODEL` is pinned to gemini-3.7-flash and
  deliberately does not follow `AI_QUESTION_MODEL`.
- Prompt lessons (versions are benchmarked in the pipeline cache): near-quote
  paraphrases of concrete passages beat "prayer-style" reformulations
  (hit 0.667 -> 0.792 at 4 variants); 6 variants beat 4 (recall +0.08) and
  8 (dilution); matching the *exact* translation register matters — telling
  the en prompt "Berean, not King James" lifted en recall 0.560 -> 0.738,
  because KJV-isms miss BSB wording both lexically and semantically.
- **Interleave fusion** (round-robin in the model's centrality order) is
  required: max-cosine lets variants with generically higher cosines flood
  the list (a rank-1 hit of one variant drowns), RRF rewards consensus
  mediocrity while our variants are deliberately diverse angles. Honouring
  the model's own most-central-first order beats cosine ordering inside a
  round (MRR 0.577 -> 0.650).
- **BM25 hybrid** adds +0.06 recall@10: rewrite variants are near-quotes,
  and exact wording is what a lexical signal ranks first where the semantic
  rank dilutes (measured: Prov 22:6 semantic rank 26 -> BM25 rank 1 for the
  same variant). Pure python over ~12k docs, no new services.
- **Diversity caps** cost ~0.02 recall and are kept by mandate (candidate
  variety for the reranker); the per-chapter cap also removes near-duplicate
  windows.
- **The genre blacklist changes no metric on this set** (the rewrites avoid
  the traps by themselves) — its value is the *guarantee*: genre ranges are
  blocked for all queries including degraded/raw paths, enforced by
  regression tests against the dataset traps
  (`tests/test_retrieval.py::test_blacklist_blocks_every_benchmark_genre_trap`).
- The sanctioned Psalter-granularity lever (ADR 0003 open question 1) was
  NOT needed: thresholds pass at the current chunking; no version bump, no
  re-embedding. It remains available if the dataset grows harder.

Costs: full pipeline development + all ablations consumed ~233 rewrite
calls and ~1.1k query embeddings — well under $0.5 on the billed key.
Serve-time cost per selection: 1 rewrite call + 6 query embeddings
(~40 tokens each); latency ~2–4 s sequential (acceptable for the feature;
parallel embedding is an obvious later optimisation).

## Consequences

- The reranker (86cb8vw1h) receives a top-10 that already passes the
  retrieval thresholds, with per-candidate diagnostics (fused score, best
  variant, per-variant scores) and per-translation exact texts.
- New env var: `AI_SCRIPTURE_REWRITE_MODEL`. Value pinned by the benchmark to
  gemini-3.7-flash, but it has no default in `app/config.py`: it is required
  whenever `GEMINI_API_KEY` is set (a default here once hid an unreachable
  model behind a config the owner believed was flash-lite everywhere).
  New data files: `app/data/genre_blacklist.json`, `app/data/safe_pool.json`
  (versioned; edits require re-running the benchmark).
- New optional env var (2026-08-29): `AI_SCRIPTURE_REWRITE_API_KEY` — the key
  this stage bills. Because the stage is pinned to gemini-3.7-flash, its
  free daily quota is the pipeline's binding constraint, while the embedding
  and rerank stages stay inside the free quotas of their lite models; the
  variable lets a deployment move this one stage to a paid key. Unset or
  blank keeps the previous behaviour (`GEMINI_API_KEY` for everything) —
  an operational default explicitly sanctioned by ADR 0008, which also
  rejects the asymmetric case (rewrite key without a shared key). Resolved
  once in `config.resolve_rewrite_api_key()`; `GeminiQueryRewriter` defaults
  to it, so no creation point repeats the rule.
- The lexical index rebuilds in-process at load from `translation_chunks`
  (no schema changes); memory ~10 MB per corpus.
- Blacklist and safe pool live in canonical coordinates: they survive
  rechunks (canonical IDs change, ranges do not).
- The rewrite prompt is versioned (`REWRITE_PROMPT_VERSION`); any change
  must be re-benchmarked (`retrieval_benchmark.py pipeline`, cache keys
  include the version).

## Prompt v8 (2026-09-05, ClickUp 86cbegg36)

Step 2 above is unchanged in role and in every threshold; what changed is the
instruction it sends and the shape of the answer it accepts. v8 is the
benchmark prompt "8c" of the 7/8a/8b/8c matrix (evaluation/README.md,
86cbea05x) moved into `app/query_rewrite.py`, plus one closing line.

**Why.** v7 was written for `gemini-3.7-flash` and measured on it. On
`qwen3-30b-a3b-instruct-2507` — the model Maria chose for the local AI
contour on 2026-09-05 — the same prompt collapses: the variants become short
generic pious formulas carrying none of the situation (mean 58 characters
against 89 for the Gemini baseline) and 10 of 21 scenarios end up without a
single relevant passage in top-10 (0.583 / 0.312 / 0.404 against
1.000 / 0.789 / 0.664). Two changes fixed most of it, measured separately:

| on qwen3-30b, gemini index | hit@10 | recall@10 | MRR |
|---|---|---|---|
| v7 | 0.583 | 0.312 | 0.404 |
| + reference anchor (8a) | 0.875 | 0.496 | 0.490 |
| + worked examples (8b) | 0.667 | 0.361 | 0.447 |
| **both (8c → v8)** | **0.875** | **0.547** | **0.558** |

1. **Reference anchor.** The model answers objects
   `{"ref": "Psalm 32:8", "query": "…"}` — it must name the passage before
   paraphrasing it. That is what turns a formula into a near-quote; it is
   also what stopped the model copying the examples (8b copied one variant in
   nine, 8c one in 126). **`ref` never leaves the parser**:
   `parse_rewrite_response` reads it and drops it, so no book name or chapter
   number reaches an embedding whose corpus contains neither — the v7 rule
   "only the passage's own words" is unchanged, it is merely satisfied by a
   separate field instead of by suppression.
2. **Six worked examples**, two per language, shown whatever the target
   language is. They are de-fingerprinted by the rule the rerank prompt v6
   established — no example topic and no example passage may touch
   `evaluation/scenarios.json` — and that is a test against the live dataset
   (`tests/test_rewrite_prompts.py`), not a claim in a comment.
3. **A closing reminder of the answer language**, the last line of the
   instruction: the model has just read six examples in three languages.
   This one is a precaution and not a measured gain — the language was
   already right for 21 of 21 scenarios without it. Dropping this paragraph
   would make the production prompt byte-identical to the measured 8c
   revision 2; see the reproducibility note below for why the measurement
   cannot currently tell the two texts apart.

   **Decision (review, 2026-09-05): the line stays.** It was re-examined
   against the control artifacts, because "unmeasured benefit" is a reason to
   delete a line only if deleting it is measurably better. It is not. The one
   *reproducible* comparison on the production embedder — warm run against
   warm run, the state the server actually settles into — has v8 **ahead** of
   8c revision 2 on hit@10 (+0.042) and recall@10 (+0.026) and behind by
   0.044 on MRR, which is smaller than the 0.072 spread of a single prompt.
   The earlier "all three 8c-rev2 MRR points are above both v8 points"
   reading does not survive: one of those three points was taken on a
   different endpoint, a different `max_tokens` and the benchmark's own
   transport rather than the production rewriter, and the other two mix a
   cold run with a warm one. So the metrics disagree, the direction flips
   with the server state, and nothing clears the noise floor. Against that,
   the line defends a **user-visible correctness failure** (a Ukrainian
   speaker served Russian scripture) in a prompt that shows examples in three
   languages, and the 21/21 language result comes from one model while the
   prompt is shared by every provider. An unmeasurable ranking cost does not
   buy back that risk.

The prompt is still shared by both transports (ADR 0009) and is now the
single source for the benchmark too: `rewrite_prompts.build_instruction("8c")`
returns the production text, and 7/8a/8b are frozen historical copies there.

**The parser repairs bounded JSON breakage** (`repair_json_object`): a closer
of the wrong type (`{"queries": ["a", "b"}}` — the stereotypical failure of a
4B model, 8 of 21 scenarios in 86cbe4nd3), a truncation at a clean boundary,
a trailing comma. It may only delete or re-type punctuation; an answer cut
off inside a string is refused rather than closed, because half a sentence
the model never finished is not a search query. A refused answer is
`rewrite_failed` — the documented degradation to the raw query.

**What v8 was measured to do, and what it was not.** On the reference set,
`qwen3-30b-a3b-instruct-2507` through the production `OpenAICompatQueryRewriter`:
JSON failures 0/21, variant language = scenario language 21/21, hit@10 0.875
(equal to the 8c reference), recall@10 0.524-0.546 and MRR 0.525-0.541 across
two samples against the 8c reference 0.547 / 0.558. It did **not** clear the
retrieval thresholds (0.90 / 0.60 / 0.50) — no local-model configuration has
yet, which is the open work of umbrella 86cbe4mtq.

**The measurement's own limit, and it is a large one.** The self-hosted vLLM
server is not reproducible at temperature 0 *on the first run of a new prompt
text*: that run shares only 42 of 125 variants with the ones after it, and
the retrieval metrics of one prompt move by up to 0.125 hit@10 / 0.072 recall
/ 0.072 MRR between a "cold" and a "warm" sample. A single run is therefore
not evidence that one prompt beats another by less than that, and the
published 8c numbers are themselves one draw. Details, sample tables and the
artifacts: evaluation/README.md, subsection 86cbegg36.

What is *not* random is the warm state. Reproduced independently at review
(2026-09-05) with nine production-rewriter calls at temperature 0 — three
scenarios, three consecutive calls each: all nine were byte-identical per
scenario, and all eighteen variants matched the warm artifact taken hours
earlier byte for byte, while matching the cold artifact in only 8 of 18. So
the server settles into a reproducible state and it is the *first* run that
cannot be repeated. **Protocol for future prompt comparisons** (recommended,
not yet adopted): discard the first run of any new prompt text as warm-up and
publish the second; take a third only to confirm it equals the second, and
treat a disagreement as an unstable configuration rather than averaging it
away. A median of N adds nothing when runs 2..N are identical. Every
published figure should say which state it came from.

On `gemini-3.7-flash` v8 is **not worse than v7**, measured on the 12
scenarios its free daily quota (20 requests) allowed before it ran out:
0.625 / 0.484 / 0.313 against v7's 0.583 / 0.482 / 0.319 under identical
conditions. That is a weak measurement on a reduced set, and it is what
exists; a full-set Gemini re-run needs a day's quota or a paid key. One
prompt for both providers is therefore kept — no
`REWRITE_PROMPT_VERSION_BY_PROVIDER`, because nothing measured asks for one.

## Open questions

1. uk recall/MRR are the weakest (0.619/0.571 vs thresholds 0.60/0.60 for
   the group; ALL passes) — a ubh-specific register hint iteration or the
   Psalter granularity lever are the known next steps if the uk group must
   pass standalone.
2. Serve-time latency: embed the 6 variants concurrently when the public
   endpoint (next task) defines its budget.
3. Safe-pool size (6 places) is small for long exclusion histories; extend
   with Maria when the mobile API defines the repeat window. Partly done:
   `safe_pool.json` 1.1.0 (Мария, 2026-08-28, ADR 0007) has 9 places, chosen
   so that every active translation carries them.
4. (2026-09-05, 86cbegg36) **The first benchmark run of a new prompt text on
   the self-hosted model is one draw, not a measurement.** Runs after it are
   byte-reproducible (verified at review), so the cheap protocol above —
   discard run 1, publish run 2, confirm with run 3 — costs two calls and
   removes most of the problem. Until the cause is known (a fixed seed, or a
   documented prefix-cache behaviour), a prompt or model comparison on this
   21-scenario set only resolves differences larger than about 0.07 MRR, and
   every published figure should say which server state it came from.
