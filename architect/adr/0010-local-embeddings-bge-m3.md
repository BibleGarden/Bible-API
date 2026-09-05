# ADR 0010: Local embeddings — BAAI/bge-m3 in the API process

Status: accepted (2026-09-05).
Ticket: ClickUp 86cbegg2r (step 3 of the local-models umbrella 86cbe4mtq).
Supersedes the *model* decision of ADR 0002; its storage decision
(`chunk_embeddings` in MySQL + in-process cosine search) is untouched.

## Context

ADR 0002 chose `gemini-embedding-001@768` because every local model measured
at the time failed the retrieval thresholds by an order of magnitude. That
measurement was taken **without a query-rewrite stage** — the pipeline of
ADR 0004 did not exist yet. Re-measured with it (ClickUp 86cbe4n7e, package
in `evaluation/README.md`), the verdict no longer holds: rewrite v7 lifts
bge-m3 from 0.238 to 0.857 hit@10 on the vector signal alone. In the full
production pipeline (rewrite + BM25 + interleave + blacklist + diversity)
bge-m3 measures

| metric | threshold | gemini-embedding-001@768 | BAAI/bge-m3@1024 |
|---|---|---|---|
| hit@10 | ≥ 0.90 | 1.000 | 0.875 |
| recall@10 | ≥ 0.60 | 0.789 | 0.688 |
| MRR | ≥ 0.60 | 0.664 | 0.524 |
| unacceptable@10 | ≤ 0.05 | 0.004 | 0.004 |

— that is, recall passes and ranking is worse, which is exactly the part the
grounded rerank of ADR 0005 does over the candidate list. The decision to
move anyway is Maria's, of 2026-09-05, and it is not a quality decision: the
app must be usable by anyone, of any age, in any country, and every external
provider's terms fail at least one of those (monorepo `CLAUDE.md`). Step 2
(ADR 0009) moved the three chat stages; embeddings are the last Gemini call
left in a selection.

## Decision

### 1. `EMBEDDING_PROVIDER`, required in every deployment

`gemini` (the API of ADR 0002, unchanged) or `local` (bge-m3 in this
process). It joins `EMBEDDING_MODEL` and `EMBEDDING_DIMENSIONS` in
`ALWAYS_REQUIRED_VARS` — required with or without any AI key, unlike the
three chat providers, because those three together name **the index this
service reads**, and the read path runs even in the documented no-AI
deployment (the safe-pool answer with `fallback_reason=ai_unavailable` is
still resolved through the loaded corpus).

There is no default, for the reason ADR 0008 exists: the two providers
produce different vector spaces of different widths, so a guessed value
would search a 1024-dimension index with a 768-dimension query — or rebuild
the index in the other space. This breaks every `.env` written before this
change, exactly as step 2 did, and the startup error names the variable.

`EMBEDDING_MODEL_PATH` is required when, and only when, the provider is
`local`: the directory the weights are mounted at. The model **identity**
stays `EMBEDDING_MODEL` (`BAAI/bge-m3`), because that is what the index
version carries; the path is where its bytes happen to live on this machine
and must never reach `chunk_embeddings.embedding_version`. Setting the path
with `EMBEDDING_PROVIDER=gemini` is a startup error, not a harmless
leftover: it states a fact about the deployment that is false.

The index version becomes `c3:BAAI/bge-m3@1024`. The slash is harmless —
the string is compared, never parsed — and 22 characters fit
`VARCHAR(120)` with room to spare.

### 2. `LocalEmbeddingClient`, the same interface as the Gemini one

`app/embeddings.py` grows a second client with the same three entry points
(`embed_documents`, `embed_query`, the context manager) and the same
`EmbeddingUnavailable` contract, so no caller knows which one it holds.
`build_embedding_client()` is the single place that maps `EMBEDDING_PROVIDER`
onto a class, used by the endpoint, `index_cli`, `retrieval_cli` and the
tests — the same shape `build_query_rewriter` / `build_passage_reranker`
have had since ADR 0009.

Details, each one a property the measurement depended on:

- **CPU, `max_seq_length=512`, encode batch 4.** Code constants, not
  environment knobs: they are properties of this model on a CPU host, not
  of a deployment, and they are what the published corpus pass ran with.
  bge-m3 advertises an 8192-token window; a batch of 16 of those allocates
  gigabytes of activations, while the longest chunk of this corpus is far
  below 512 tokens.
- **`normalize_embeddings=True`.** `InMemoryVectorIndex` computes cosine
  similarity as a plain dot product over stored rows, so unit length is a
  correctness requirement.
- **No query/passage prefix.** bge-m3 is symmetric and the benchmark's
  model registry gives it empty prefixes on both sides — so the query side
  is embedded exactly the way the documents were. A model that needs
  prefixes (e5) is not supported by this client without a code change, and
  that is deliberate: a silently missing prefix is a quality loss nothing
  reports.
- **Loaded once per process, at start-up.** 2.3 GB of fp32 weights; a second
  copy fits on no machine this project uses. `app/main.py` loads it at import
  time when the provider is `local`, and a missing, unreadable or wrong
  model **aborts the start**. Lazily loading it would answer the first
  prayer with `fallback_reason=ai_unavailable` and look, from outside, like a
  provider being briefly down.
- **The width is verified on load** against `EMBEDDING_DIMENSIONS`. The path
  and the identity are separate variables, so a directory holding another
  model would otherwise write vectors of the wrong space under the right
  version string.
- **Encodes are serialised by a lock**, and the selection endpoint passes
  `embed_workers=1` on this provider. On Gemini the six variant embeddings
  are independent round trips and overlapping them is the biggest serve-time
  saving (ADR 0006: 1.6 s → 0.31 s). Locally there is no round trip to
  overlap: torch already spreads one encode across the cores, so six at once
  only oversubscribe a box that also runs MySQL. Measured in the acceptance
  container, six query variants: **334 ms sequential, 320 ms through a
  six-thread pool** — the lock costs nothing, because the work was never
  parallel to begin with.
- **`HF_HUB_OFFLINE=1` in the image** and a filesystem path rather than a hub
  id: the container cannot phone home, structurally rather than by habit.

### 3. A rebuild no longer deletes other index versions

This is the operational half of the decision and the reason the migration is
not a two-hour outage.

`plan_reindex` used to delete every row whose version differed from the
target one, which made two index versions impossible to hold at once. They
are now kept unless `index_cli.py rebuild --drop-other-versions` asks for
the cleanup, so:

1. the running container keeps serving `c3:gemini-embedding-001@768`;
2. the rebuild writes `c3:BAAI/bge-m3@1024` beside it (two hours locally);
3. the switch is an `.env` edit plus a restart — and the rollback is the
   same edit back, with the old vectors still there;
4. the old rows are dropped later, deliberately, once the new index has been
   verified.

Two smaller corrections came with it, both latent bugs the moment two
versions coexist: the stored rows are read as a **set of (canonical_id,
version) pairs** (a dict keyed by canonical id kept whichever row came last),
and the DELETE names the version in its predicate (deleting by canonical id
alone took every version's row with it). A row of the *current* version whose
chunk has disappeared is still deleted whatever the flag says — that is not
another version's data, it is this index pointing at a passage that is gone.

`--force` no longer deletes anything: the INSERT is an upsert on
`(translation, canonical_id, embedding_version)`, so re-embedding replaces a
vector in place.

## Consequences

- **Image**: 1.4 GB → 2.46 GB (+1.06 GB). torch is installed from
  `https://download.pytorch.org/whl/cpu` in its own first layer, and the same
  file is passed as a `-c` constraint to the second `pip install`: without
  that constraint pip backtracks on `torch>=2.2`, "upgrades" the +cpu build
  from PyPI and pulls the whole CUDA stack — measured, 7.24 GB. Three
  version bumps came with the dependency: `huggingface-hub` 1.x needs
  `click >= 8.4.2` and typer 0.12 breaks on click ≥ 8.2 (click 8.1.7 →
  8.5.0, typer 0.12.3 → 0.27.2, rich 13.7.1 → 14.3.0), and torch 2.14 needs
  `setuptools >= 77.0.3` (70.3.0 → 81.0.0). `fastapi-cli` deliberately stays
  at **0.0.4**: 0.0.32 installs no `fastapi` console script at all, and the
  container's CMD — and every command in the documentation — is
  `fastapi run`. All of these serve that command or the build; the
  application imports none of them, and `fastapi run app/main.py` was
  smoke-tested against the new set (`GET /api/languages` → 200).
- **Memory**: the API process holds the weights permanently. Measured on this
  machine: **2.13 GiB RSS** for the API process after warm-up (`ps`), flat
  across selections, and **3.09 GiB peak** for the rebuild container, whose
  encode activations are the difference. `docker stats` reports ~1.0 GiB for
  the API process, not 2.13: the weights are mapped from the read-only
  volume, so most of them are file-backed pages it excludes. That is a real
  property, not an accounting quirk — under pressure those pages are
  evictable and re-read from disk (slower, not OOM) — but **size the VM by
  the 2.13 GiB**, because a machine that has to evict them constantly will
  embed at disk speed. Production therefore moves to the 8 GB VM; the
  current 2-4 GB one cannot run this.
- **Weights are a volume, not a layer.** 2.3 GB that never change have no
  business in an image that is rebuilt on every deploy. `docker-compose.yml`
  mounts `${EMBEDDING_MODELS_DIR:-./models}` read-only at `/models`; the
  default keeps a Gemini deployment working with no new variable, because it
  loads no model at all.
- **A rebuild is ~1 hour of CPU** on this 8-core host: 11 960 chunks in
  3647 s (syn 3963 in 1044 s, bsb 4032 in 1245 s, ubh 3965 in 1333 s ≈
  3.3 chunks/s), against ~12 days of free Gemini quota or a billed key. It
  costs nothing but time and needs no network — which is the point. The
  benchmark's 6875 s for the same corpus was measured at an encode-list of
  50; see `index_cli.DEFAULT_BATCH_SIZE`.
- **Query embedding is faster than the network call it replaces**: measured
  in the acceptance container, median **39 ms**, max 103 ms per query
  (target for this ticket: ≤ 300 ms), against ~350 ms for a Gemini round
  trip. It is CPU the API worker spends rather than time it waits, which is
  a different kind of cost on a busy box, but at 39 ms it is not the
  bottleneck of a selection.
- Retrieval quality drops as tabulated above and the rerank absorbs it. That
  trade is Maria's decision, taken with the numbers in front of her.
- `evaluation/trace_picker.py` keeps its own `LocalEmbedder`; the production
  client is the same design, now with tests. Once the stand's prompts and
  production's agree it could import this one.

## Alternatives considered

- **Keep Gemini for embeddings, local chat only.** Cheapest, and it is what
  step 2 left in place — but it leaves a Google call in every selection,
  which is the thing the umbrella exists to remove.
- **e5-small (ADR 0002's designated fallback).** 1.2 GB RSS instead of
  2.5 GB, but its own embedding contribution is the *worst* of the four
  measured locals (0.381 hit@10 on the vector signal alone against bge-m3's
  0.857); its headline pipeline numbers are BM25's work. Rejected on quality.
- **Importing the benchmark's cached bge-m3 matrix** (`bench_data/
  emb_bge-m3_title_text.npy`) instead of recomputing 11 960 vectors. The
  document text is provably the same string (`title_text` in the benchmark is
  `build_embedding_text`, and bge-m3 has no passage prefix), so it would have
  saved two hours. Rejected: it would have skipped the very code path this
  ticket delivers, and the matrix was built from an August corpus export
  rather than from `translation_chunks` as it stands. Used instead as a
  **verification**: 40 random rows of the rebuilt index, compared against the
  cached matrix, agree at cosine 1.000000 (min = median = max, 0 chunks
  missing from the cached corpus). So the vectors this code writes are the
  vectors 86cbe4n7e measured, and the retrieval numbers above are numbers
  about *this* index — which is what makes re-running the harness
  unnecessary (it cannot read the DB index anyway; see below).
- **A dedicated embedding service** (a container speaking HTTP, keeping the
  weights out of the API process). It would decouple the API's memory from
  the model and allow more than one API worker — but it is a second stateful
  service on a small VM for a call that takes 200 ms, and the umbrella's
  target is fewer moving parts, not more. Revisit if the API ever needs
  multiple workers.

## Why the benchmark was not re-run

`evaluation/retrieval_benchmark.py` never reads `chunk_embeddings`: every
one of its four matrix readers goes through `load_corpus_matrix`, which
loads `bench_data/emb_<model>_<variant>.npy` built from `chunks.jsonl`. It
therefore cannot measure the DB index this ticket produces, and re-running
`pipeline --embedder bge-m3` would only re-measure the same model on the
same corpus through its own code — the 86cbe4n7e numbers, again. The
40-chunk identity check above answers the question that run would have
asked, at zero cost, and is stronger: it compares the actual stored vectors
rather than two runs of the same encoder.

## Open questions

1. The retrieval thresholds of `evaluation/thresholds.json` are **not** met
   by this embedder on MRR (0.524 against 0.60). The rerank compensates in
   the final top-1 measurement, but the retrieval-stage threshold now fails
   by design. Whether to re-baseline those thresholds for the local pipeline
   or keep them as a record of what Gemini did is Maria's call (step 8).
2. `AI_SCRIPTURE_TIMEOUT_SECONDS` / `AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS`
   now have to cover local CPU embedding *and* a self-hosted chat model; the
   right pair is a measurement (step 8, inherited from ADR 0009).
3. The production VM move (8 GB) is a prerequisite, not a consequence: the
   current one cannot hold the weights. Until it happens, production stays on
   `EMBEDDING_PROVIDER=gemini` — which is exactly why the two index versions
   can coexist.
4. bge-m3's sparse and ColBERT heads are unused; only the dense CLS vector is
   read. Whether the sparse head could replace or strengthen BM25 was not
   measured.
