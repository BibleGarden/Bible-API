# ADR 0014: Embeddings by API from the company server

Status: accepted (2026-09-05).
Ticket: ClickUp 86cbehd6h (step 10 of the local-models umbrella 86cbe4mtq),
depending on the admins' 86cbeh9q6 / 86cbehdq1.
Supersedes ADR 0010 *in production only*: same model, same dimensions, same
index version — a third answer to "who runs it". Follows ADR 0012's shape,
which did the same for Whisper.

## Context

ADR 0010 moved the embeddings of the RAG index off Gemini and onto
`BAAI/bge-m3` inside the API process. It worked, and it cost **2.13 GiB of
RSS, permanently**, which is why it also said the production VM had to grow to
8 GB first. That move never happened, so production stayed on
`EMBEDDING_PROVIDER=gemini` — the last Google call in a selection, and exactly
what the umbrella exists to remove.

Then the same thing happened here that happened to transcription two tickets
earlier (ADR 0012): the company's admins put the model on the **CPU of the
`llm` server** (86cbeh9q6) and gave it an OpenAI-compatible endpoint. So the
choice is no longer "2.1 GB of weights here or a Google key"; it is "ask a
server inside the company", the way the four other stages already do.

Live since 2026-09-05: `POST https://llm.ai2.ru/v1/embeddings`, server
**Infinity** on CPU, model name `BAAI/bge-m3`, `Authorization: Bearer`,
direct HTTPS from this machine (the SSH tunnel of 2026-09-05 morning is
retired). Measured from here: 2 short texts → 200 in 0.25 s, 1024 dims,
‖v‖ = 1.0 to 4e-8.

## Decision

### 1. `EMBEDDING_PROVIDER` ∈ `gemini` | `local` | `openai_compat`

A third value on the variable ADR 0010 introduced, required in every
deployment for the reason it always was: it says which vector space this
service **reads**, and the read path runs even with no AI configured.
`openai_compat` is the production value from this date.

What it does **not** change is as important: `EMBEDDING_MODEL=BAAI/bge-m3`
and `EMBEDDING_DIMENSIONS=1024` are unchanged, so the index version stays
`c3:BAAI/bge-m3@1024`. It is the same index — built locally on this machine
(ADR 0010's `local` provider, which stays exactly for that) and shipped to
production by `GET /api/import` (ClickUp 86cbegwr9). Nothing is rebuilt and
nothing is re-imported for this switch: it is an `.env` edit and a restart,
and the rollback is the edit back.

Endpoint and key resolve through `config.resolve_stage`, the same function
every chat stage and transcription use: the shared
`AI_OPENAI_COMPAT_ENDPOINT` / `AI_OPENAI_COMPAT_API_KEY`, overridden by the
per-stage `EMBEDDING_ENDPOINT` / `EMBEDDING_API_KEY`. The override is not
decoration — the embedding server is a different process from the chat one
and may be on another path (today it happens to share `…/v1`, while Whisper
is at `…/whisper/v1`).

Three refusals at start-up, all of the ADR 0008 kind ("`.env` must not say one
thing while the deployment does another"):

- `EMBEDDING_MODEL_PATH` set on `openai_compat` — the weights would never be
  loaded. The message **names the configured provider**, not a hard-coded
  one: an operator reading "set while EMBEDDING_PROVIDER=gemini" on an
  openai_compat deployment doubts the error instead of the variable (the
  lesson of the 86cbegg3m review).
- `EMBEDDING_ENDPOINT` / `EMBEDDING_API_KEY` set on `gemini` or `local` —
  the mirror image: a variable that could never be read.
- an endpoint that carries credentials or a query string, or is not an
  absolute http(s) URL (`config.validate_endpoint`, unchanged).

And one placement decision worth naming: the endpoint/key requirement for
embeddings is checked **before** the `ai_configured` gate, unlike every chat
stage's. Embeddings are not part of the AI surface that switch guards — a
deployment with no chat provider and no key still reads the index, and if it
reads it over the network it must say where.

### 2. `RemoteEmbeddingClient`, the third client behind one interface

`POST {endpoint}/embeddings`, body `{"model", "input": [texts…]}`, answer
`data[]` re-ordered **by each item's own `index`** (the protocol says it
carries one; a silently mis-ordered batch would attach every chunk's vector to
its neighbour — the one rebuild failure nothing downstream can detect).
Batches of at most **64** texts, which is also the unit of retry: one refusal
costs 64 chunks, not 500.

Same three entry points and the same `EmbeddingUnavailable` contract as the
other two clients, including the `provider_down` split that lets retrieval
fail fast instead of burning the budget per variant. Transport discipline is
`RemoteTranscriber`'s, which is `llm_client`'s, which is `gemini_retry`'s:
`provider_timeout` carves one call across httpx's four phases, `retry_pause`
refuses to sleep unless the attempt after it still fits, `RETRYABLE_STATUS`
decides what is worth another attempt, and every transport failure is
re-raised **`from None`** with its category only — an httpx message quotes
the URL, and the request body is a query derived from the prayer context.

**Normalisation is verified, not assumed.** The server answers unit vectors
and the stored index is unit-length — cosine search over it *is* a dot
product — so a vector of another length would rank against the stored ones on
magnitude and simply return worse passages, silently. Each vector's norm is
therefore checked (`|‖v‖-1| < 1e-3`); outside that it is normalised through
the existing `normalize()` and a **WARNING is logged once per process**,
naming `EMBEDDING_MODEL`. Once, because a server that changed its pooling
answers that way for every request and a per-vector warning would bury the log
it exists to be found in.

### 3. `index_cli rebuild` works on this provider, and says what differs

Allowed and useful (no weights, no key, no Google), with the batch defaulting
to 64. It prints one thing first, because the index version cannot say it:
the two bge-m3 providers do **not** write byte-identical rows.

`LocalEmbeddingClient` caps its input at 512 tokens (ADR 0010's
`LOCAL_MAX_SEQ_LENGTH`, chosen for CPU memory). The server applies its own,
larger window. ADR 0010 claimed the cap cost nothing because the corpus was
"far below 512 tokens"; measured here against the bge-m3 tokenizer, **811 of
the 11 960 indexed chunks (6.8%) are longer**, up to 1168 tokens. So a
rebuild through this provider gives those 811 chunks a *fuller* vector than
the stored one. Harmless in itself — but a catch-up rebuild (without
`--force`) would leave one index version half-written by each provider, which
is why the CLI says it out loud and points at `--force`.

### 4. Six query variants keep their thread pool

`scripture_select._EMBED_WORKERS` is `REWRITE_VARIANTS` (6) on this provider,
as it is on Gemini and unlike `local` (1). Measured, not assumed — this host
against the live server, 5 rounds of six realistic variants: **842 ms
sequential, 592 ms through a 6-thread pool**, one query 137 ms median. The
gain is smaller than Gemini's because the server batches on its own CPU
rather than answering six requests in parallel, but it is 250 ms of a 15 s
budget for one shared, thread-safe `httpx.Client` and no other cost.

Sending all six as one request would be faster still (504 ms) and is
deliberately not done: `retrieval._embed_queries` degrades **per variant**,
and one batch would make a single bad variant lose all six.

## Consequences

- **Memory.** The API process holds no weights: 72 MB after importing the
  application, against ADR 0010's 2.13 GiB. What it does hold is the corpus
  cache — the vector index, BM25, chunk texts, Psalm maps — and that is
  **provider-independent**: measured on this machine, `get_resources()` alone
  costs 208 MB on `@1024` and 189 MB on `@768`, with no network call in
  either case. So a serving instance is ~280 MB after its first selection and
  peaked at **344 MB** through the live acceptance below. That is above the
  200 MB this ticket hoped for, and no provider can go below it on this
  corpus; it is 6x smaller than `local` (2.13 GiB + the same cache).
- **Vectors are identical where it matters, and the exception is known.**
  40 random rows of the live `c3:BAAI/bge-m3@1024` index, re-embedded through
  this client and compared against the stored vectors: **median 1.000000, max
  1.000000, min 0.936332**, 38 of 40 at 1.000000. The two below are the two
  longest texts (2155 and 2116 characters), i.e. the 512-token truncation of
  §3 — proved by cutting the corpus's longest chunk (1168 tokens) at 512 and
  re-embedding it: cosine against the stored row **1.000000** exactly, against
  0.969813 for the full text. Same model, same pooling, same normalisation;
  the only difference is how much of a long chunk each side reads. **Queries
  are never near the window**, so the serve path is unaffected.
- **Retrieval is unchanged, byte for byte.** `retrieval_benchmark.py pipeline
  --embedder bge-m3 --embedder-provider openai_compat` (the new flag: remote
  QUERY embeddings against the cached document matrix, which is the matrix the
  production index was built from — i.e. the deployed configuration) against
  the local bge-m3 run of the same rewrites
  (`results_v080_localemb_bge-m3_qwen30b_p8appr4_run2.json`): hit@10 0.875,
  recall@10 0.5104, MRR 0.4557, unacceptable 0.0042 — **identical to six
  decimals**, every scenario's top-10 identical including the cosine scores,
  and both fallback top-1 lists identical. Peak RSS 360 MB against 2335 MB;
  query embedding median 172 ms against the local run's 179 ms.
- **Latency.** One query 137 ms median (78-172) from this host, against ADR
  0010's 39 ms local and Gemini's ~350 ms. A selection with six variants
  spends ~0.6 s embedding. Live selections answered in 3.8-7.2 s end to end.
- **A dependency, and it is a network one.** A selection now needs the
  company server for four of its five stages. The failure is the one the
  pipeline already handles — `EmbeddingUnavailable` with `provider_down`,
  degrading to the safe pool with `fallback_reason` — but it is a real
  reduction in independence compared with weights on disk, which is precisely
  why `local` stays supported and documented as the fallback.
- **The 8 GB VM stopped being a prerequisite.** ADR 0010's open question 3 is
  closed: production can read this index version on the VM it has.
- **The tripwire now covers all five stages.**
  `test_no_google_host_is_dialled_on_any_of_the_five_stages` drives a question
  call, a transcription and the three selection stages through their real
  client classes over a recording httpx transport, and asserts that no host
  under `googleapis.com` is dialled and that no factory built a Gemini class.

## Alternatives considered

- **Keep `local` and move the VM to 8 GB.** Fully independent of the network,
  and it is what ADR 0010 planned. Rejected for now, not forever: 2.1 GB of
  RSS on a VM that also runs MySQL buys nothing the server does not give, the
  move has been pending for a day already, and `local` remains one `.env` edit
  away.
- **Keep Gemini for embeddings.** The status quo, and the thing the umbrella
  exists to remove: it leaves a Google call in every selection.
- **Truncate to 512 tokens on our side, to match the stored index exactly.**
  Would need the tokenizer — and therefore the model directory — in
  production, which is most of what this ADR removes, to make 6.8% of the
  *document* vectors bit-identical on a path that only embeds *queries* at
  serve time. Rejected; the difference is documented instead.
- **One request for all six variants.** Faster (504 ms vs 592 ms), but it
  couples the six degradation paths into one. Rejected.

## Open questions

1. The production key (`bible-api-prod`, Passbolt) and the production VM's
   place in the server's IP allow-list — the same two items ADR 0012 is
   waiting on, for the same server. Everything measured here used the
   `bible-api-test` key from this host.
2. Whether the fuller (untruncated) vectors of the 811 long chunks retrieve
   *better* than the stored truncated ones. Answering it means a full remote
   rebuild plus a benchmark against a matrix built the same way; nobody has
   asked the question yet, and today's index is the one both providers agree
   with on 93.2% of chunks.
3. What happens to a selection when the company server is unreachable has
   been reasoned about (safe pool, `fallback_reason=ai_unavailable`) but not
   rehearsed against the live production wiring.
