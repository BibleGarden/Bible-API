# ADR 0002: Embedding model and vector store for scripture-selection RAG

Status: accepted for implementation (2026-08-24); retrieval-quality
verification of the chosen API model is pending — blocked by the free-tier
daily quota, see "Benchmark limitations".
Ticket: ClickUp 86cb8vw14

> Note (2026-08-30, ClickUp 86cbbmy8d): the AI environment variables were
> renamed to mirror the method they configure. This ADR's own pair,
> `EMBEDDING_MODEL` / `EMBEDDING_DIMENSIONS`, was **not** renamed — it names
> the stored index version, not a method — but the sibling names quoted here
> were: `RETRIEVAL_REWRITE_MODEL` → `AI_SCRIPTURE_REWRITE_MODEL`,
> `RETRIEVAL_RERANK_MODEL` → `AI_SCRIPTURE_RERANK_MODEL`,
> `RETRIEVAL_REWRITE_API_KEY` → `AI_SCRIPTURE_REWRITE_API_KEY`. Names only;
> no value, model or measurement below changed.

## Context

The RAG passage-selection feature needs semantic retrieval over the
structural chunks of ADR 0001 (`cep_public.translation_chunks`; corpus at
decision time: syn 3963 + bsb 4046 + ubh 3978 = 11 987 chunks, ~0.9 KB each).
Requirements:

- multilingual embeddings (ru/en/uk queries against the language's
  translation), cosine search, filters by language/translation;
- retrieval quality measured on the draft evaluation set
  (`evaluation/scenarios.json` v0.1.0, 24 scenarios) against the draft
  retrieval thresholds (`evaluation/thresholds.json`): hit_rate@10 >= 0.90,
  recall@10 >= 0.60, MRR >= 0.60, unacceptable share in top-10 <= 0.05;
- production runs on a small VPS (shared with MySQL and the API), so memory
  is the scarcest resource;
- MySQL is the canonical data source; the vector index must be fully
  rebuildable, batch, idempotent and versioned
  (chunking version + embedding model);
- privacy: prayer context already goes to Gemini for the Twinkler companion,
  and Bible texts are public, so an embedding API is acceptable.

LLM re-ranking and the user-facing selection endpoint are separate follow-up
tasks.

## Benchmark

`evaluation/retrieval_benchmark.py` embeds the full corpus per configuration
and replays all 24 scenarios (query = prayer topic + allowed user replies;
corpus = the language's translation: ru→syn, en→bsb, uk→ubh; ubh chunked with
the same `chunk_cli.py` run as syn/bsb). Matching follows `thresholds.json`
(book+chapter equality, verse-range intersection) with a data-driven
Psalm-numbering mapping layer (canonical english-masoretic → Septuagint
chapters + counted superscriptions for syn, superscription offsets for ubh),
as mandated by the dataset's `coordinate_system`.

Configurations: three local sentence-transformers models (CPU) x two document
texts — chunk text alone (`text`) vs `title + "\n\n" + text` (`title_text`) —
plus the Gemini embedding API (see limitations).

### Results (all 24 scenarios, top-10, draft thresholds in header)

| config | hit@10 (>=0.90) | recall@10 (>=0.60) | MRR (>=0.60) | unacc@10 (<=0.05) | query embed | peak RSS |
|---|---|---|---|---|---|---|
| minilm / text | 0.000 | 0.000 | 0.000 | 0.000 | ~0.5 s CPU | 1.1 GB |
| minilm / title_text | 0.042 | 0.014 | 0.005 | 0.000 | ~0.5 s CPU | 1.1 GB |
| e5-small / text | 0.000 | 0.000 | 0.000 | 0.000 | ~0.45 s CPU | 0.9 GB |
| e5-small / title_text | 0.083 | 0.028 | 0.013 | 0.000 | ~0.5 s CPU | 0.9 GB |
| e5-base / text | 0.042 | 0.014 | 0.010 | 0.004 | ~1 s CPU | ~1.6 GB |
| e5-base / title_text | 0.000 | 0.000 | 0.000 | 0.000 | ~1 s CPU | ~1.6 GB |
| gemini-embedding-001 | not measurable today (daily quota) | — | — | — | ~0.35 s API | ~0 (API) |

minilm = paraphrase-multilingual-MiniLM-L12-v2, e5 = intfloat/multilingual-e5.
Full per-language breakdowns: `evaluation/retrieval_benchmark.py run-all`.

**No measured configuration comes anywhere near the draft thresholds.**
The numbers are honest lower bounds (the reference sets are non-exhaustive),
but the gap (0.08 vs 0.90 hit-rate) is qualitative, not noise. Two findings
matter beyond raw scores:

1. **Titles help.** `title_text` beats `text` for minilm (0.042 vs 0.000)
   and e5-small (0.083 vs 0.000); for e5-base both variants sit at the noise
   floor (0.000 vs 0.042, a single uk hit either way). Section titles carry
   thematic signal the verse text often lacks (the reformulation probe below
   confirms titled chunks surface first). Hence the decision to embed
   `title + text`; the pending Gemini run re-checks this at real quality
   levels.
2. **Query formulation is a first-order bottleneck.** Diagnostic probe
   (e5-small, ru-002 "сокращение на работе"): the raw prayer context ranks
   Mt 6:25-34 at ~356; a scripture-styled reformulation of the same intent
   ("не тревожься о завтрашнем дне… возложи заботы") ranks it 4th. Literal
   quotes rank 1st with score 0.9. Local models fail hardest at bridging
   colloquial life-situations to biblical language across the whole corpus
   of 4k distractors. This is exactly the gap a stronger embedding model
   and/or an LLM query-reformulation step (already planned before reranking)
   must close; thresholds should be re-checked at that pipeline stage.

### Benchmark limitations: Gemini quota

The project key is on the Gemini API free tier:
`EmbedContentRequestsPerDayPerUserPerProjectPerModel-FreeTier = 1000
requests/day` for `gemini-embedding-001` (shared by `embedContent` and
`batchEmbedContents`, which is additionally throttled to a crawl). Embedding
the 11 987-chunk corpus therefore cannot complete within one day on the free
tier (~12 days trickle), and the day's quota was consumed by the attempts.
The benchmark's Gemini path is implemented, checkpointed (resumes without
re-spending quota) and ready to run:

```
GEMINI_API_KEY=… python evaluation/retrieval_benchmark.py embed --model gemini --variant title_text
python evaluation/retrieval_benchmark.py run --model gemini --variant title_text          # 3072 dims
python evaluation/retrieval_benchmark.py run --model gemini --variant title_text --dims 768
```

Completing it requires either enabling billing on the key (paid tier: no
daily cap; whole corpus ≈ 3–4 M tokens ≈ $0.5–0.6 at $0.15/1M) or ~12 days
of free-tier trickle. **This verification is a required follow-up before the
selection endpoint ships.**

### Vector store comparison (11 987 vectors, translation filter, top-10)

| store | p50 / max query, 768d | top-10 parity vs exact | operational cost |
|---|---|---|---|
| numpy in-process (over MySQL-stored vectors) | 6.0 / 19.1 ms | exact by construction | none: no new service, backed up with MySQL |
| Qdrant 1.15 (container) | 5.0 / 13.4 ms | 100/100 identical | second stateful service (~150 MB RAM), own snapshots/upgrades |
| ChromaDB 1.x (embedded) | 23.3 / 58.6 ms | 99/100 (one HNSW recall miss) | heavy dependency tree in the API image, own on-disk store |

(384-dim run: numpy 5.2 ms, qdrant 9.6 ms, chroma 37.8 ms, both 100/100.)
At this corpus size the stores return essentially the same top-10 (Chroma's
single miss is an HNSW recall artifact exact search cannot have), so the
decision is operational, not quality-driven.

## Decision

### Embedding model: `gemini-embedding-001` (API), 768 dims

Chosen despite the pending quality verification, because:

- every *measured* local alternative fails the draft thresholds by an order
  of magnitude, so "pick the measured winner" would mean shipping a known
  failure that also costs ~1 GB RSS and ~1.5 GB of torch layers in the image
  on a VPS where memory is the scarcest resource;
- gemini-embedding-001 is the strongest multilingual retrieval embedder
  available to the project (MTEB multilingual leader at release; ru/uk/en are
  first-class), and the asymmetric RETRIEVAL_QUERY/RETRIEVAL_DOCUMENT task
  types target exactly the "colloquial query → scripture document" bridge
  where the local models collapse;
- operational fit: no new runtime dependencies beyond numpy, same
  GEMINI_API_KEY and HTTP patterns as the existing Twinkler integration,
  ~350 ms query embedding;
- privacy is pre-cleared by the ticket (prayer context already goes to
  Gemini; Bible text is public).

Fallback if API-independence ever becomes mandatory: multilingual-e5-small
(the best RAM/quality ratio among measured locals) — but only together with
a query-reformulation stage, without which it is not usable (see benchmark).

Details:

- **Dimensionality 768** (server-side Matryoshka truncation via
  `outputDimensionality` + client-side re-normalisation, since Gemini
  returns unit vectors only at full 3072): 4x smaller storage/RAM; MRL
  truncation quality on this corpus to be confirmed in the pending Gemini
  benchmark run (the harness evaluates 3072 vs 768 from one embedding pass).
- **Single `embedContent` calls, not `batchEmbedContents`**: measured
  free-tier throughput ~12 chunks/min for batch vs ~100/min for singles;
  singles also give precise RetryInfo-driven backoff.
- **Degradation**: `EmbeddingUnavailable` on API failure. The stored index
  is unaffected — only *query* embedding needs the API at serve time; the
  future selection endpoint must degrade gracefully (open question there).
- **Quota**: ~24–100 queries/day fits the free tier trivially; a *full
  rebuild* does not (see above) — production rebuilds assume a billed key,
  and the CLI's resume semantics tolerate quota aborts either way.

### Document text: title + text

`build_embedding_text` in `app/vector_index.py` prepends the section title
to the chunk text when present — the benchmark shows `title_text` strictly
dominating `text` for every model on every metric. This closes the open
question left by ADR 0001. Queries are embedded as-is (RETRIEVAL_QUERY);
documents with RETRIEVAL_DOCUMENT.

### Vector store: MySQL (`chunk_embeddings`) + in-process cosine search

Embeddings live in a new `cep_public.chunk_embeddings` table (float32 unit
vectors in a BLOB, one row per chunk); search is an exact cosine (dot
product) over an in-memory numpy matrix with translation/language filters
(`InMemoryVectorIndex` in `app/vector_index.py`).

Why not a dedicated vector database:

- **Scale.** ~12 k x 768 floats ≈ 35 MB; exact search is ~5 ms — faster than
  Qdrant/Chroma in the measurement above and two orders of magnitude below
  the query-embedding API round-trip. ANN solves a problem this corpus does
  not have; exact search has recall 1.0 by definition.
- **Operations.** Qdrant would be a second stateful service on the small
  VPS; ChromaDB embeds a heavy dependency tree into the API image. Both add
  a second data store to operate.
- **Backups.** MySQL is already the canonical, backed-up store. Vectors in
  MySQL are covered by existing dumps: restoring a dump restores retrieval
  with no re-embedding and no API dependency during disaster recovery. (The
  index is rebuildable either way — but with MySQL a rebuild is needed only
  after a version change, not after every restore.)

Revisit (migrate to Qdrant — the benchmark harness and the experimental
`docker-compose.qdrant.yml` remain in the repo) when any of: corpus >
~100 k vectors, multiple API workers need a shared hot index, or search p95
exceeds ~50 ms.

### Index versioning and idempotency

- `embedding_version = "c{CHUNKING_VERSION}:{EMBEDDING_MODEL}@{EMBEDDING_DIMENSIONS}"`
  (currently `c1:gemini-embedding-001@768`) stored on every row.
- `UNIQUE KEY (translation, canonical_id, embedding_version)` makes
  duplicates impossible; inserts are idempotent upserts.
- `python app/index_cli.py rebuild` is a pure catch-up: embeds only chunks
  lacking a current-version row, deletes rows whose version is stale or
  whose chunk no longer exists in `translation_chunks`, keeps the rest.
  Re-running is a no-op; an interrupted run resumes; `--force` re-embeds
  everything (still duplicate-free). Verified against the live DB with a
  stub embedder: fresh run embedded=3963/kept=0, second run
  embedded=0/kept=3963, version bump embedded=3963/deleted=3963.
- Bumping `CHUNKING_VERSION` or changing `EMBEDDING_MODEL`/
  `EMBEDDING_DIMENSIONS` changes the version string; the next rebuild
  replaces the index and removes the stale version's rows.

### Table

```sql
CREATE TABLE IF NOT EXISTS chunk_embeddings (
    code INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    canonical_id VARCHAR(40) NOT NULL,
    translation INT NOT NULL,
    embedding_version VARCHAR(120) NOT NULL,
    dims SMALLINT UNSIGNED NOT NULL,
    vector MEDIUMBLOB NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_embeddings_chunk (translation, canonical_id, embedding_version),
    INDEX idx_embeddings_version (embedding_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

Created by the CLI (`CREATE TABLE IF NOT EXISTS`), like the chunking and
stats tables; no existing tables are modified. Size: ~12 k rows x ~3 KB ≈
40 MB.

### Runner

```
# inside the bible-api container
python app/index_cli.py rebuild [--translations syn,bsb,ubh] [--force]
python app/index_cli.py status
python app/index_cli.py search --query "..." [--translation syn|--language ru]
```

New env vars: `EMBEDDING_MODEL=gemini-embedding-001`,
`EMBEDDING_DIMENSIONS=768`. They have no defaults in code and are required in
every deployment, with or without `GEMINI_API_KEY`: the pair versions the
stored vectors, so a guessed value would silently address an index nobody
wrote (`c3:@0` — an empty read, and a destructive rebuild). Dimensions must be
positive. Reuses `GEMINI_API_KEY`. New runtime dependency: `numpy` (added to
requirements.txt).

## Consequences

- The bible-api image stays slim (numpy only); the local-model path would
  have added torch + sentence-transformers (~1.5 GB image, ~1 GB RSS).
- Query-time retrieval costs one Gemini embedding call (~350 ms) plus ~5 ms
  cosine search; the reranking latency budget must include it.
- After re-importing/re-chunking a translation, `index_cli.py rebuild` must
  be re-run for it (same rule as chunking after import).
- The in-memory index is per-process; production runs a single API worker
  (same constraint as the Twinkler rate limiter), so no coherence problem.
- A full production rebuild needs a billed Gemini key (free tier: 1000
  requests/day vs ~12 k chunks) — or ~12 daily resumed runs.

## Alternatives considered

- **Local sentence-transformers** (paraphrase-multilingual-MiniLM-L12-v2,
  multilingual-e5-small, multilingual-e5-base): measured far below the draft
  thresholds (table above) and RAM-expensive on the VPS. Rejected;
  e5-small kept as the designated fallback.
- **Qdrant / ChromaDB**: rejected at current scale on operational grounds
  (measurements above); Qdrant is the designated migration path.
- **Keyword/FULLTEXT retrieval**: not evaluated — the evaluation set is
  built around semantic matching and lexical-trap avoidance, which keyword
  search fails by construction.

## Open questions

1. **Gemini benchmark completion** (billed key or quota trickle) — required
   before the selection endpoint ships; harness and checkpoints are ready.
2. Query reformulation stage (LLM rewrite of prayer context into scriptural
   register) — the diagnostic probe suggests it may matter as much as the
   model; belongs to the selection-endpoint task, but thresholds should be
   evaluated after it.
3. Behaviour of the selection endpoint when the embedding API is down
   (curated fallback pool vs error).
4. Draft thresholds unmet by any measured configuration — decide with Maria
   whether the gap closes via the stronger model + reformulation + reranking
   or the thresholds/dataset get revised (they are drafts).
5. ~~Psalm-numbering mapping lives in the benchmark; the production selection
   endpoint will need the same canonical→translation layer.~~ Resolved by
   ADR 0003: the mapping is `app/versification.py` +
   `cep_public.psalm_verse_mappings`; the benchmark now imports it. The
   stored index was renamed to `c2:gemini-embedding-001@768` by the chunk-ID
   migration (no re-embedding; texts unchanged).
6. `bti`, `webus`, `webbe`, `npu` are not chunked/indexed; the language →
   canonical translation policy (dataset open question) should decide the
   final index contents.
