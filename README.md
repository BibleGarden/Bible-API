# Bible API

Public read-only REST API for the [Bible Garden](https://github.com/Bible-Garden) app. Serves Bible texts, translations, and word-level audio alignments.

Built with FastAPI and MySQL.

## Setup

```bash
cp .env.example .env
# fill in DB credentials and API_KEY in .env

docker compose up -d --build
```

The API will be available at `http://localhost:9084/api`.

## Endpoints

- `GET /api/languages` — available languages
- `GET /api/translations` — available translations
- `GET /api/translations/{code}/books` — books in a translation
- `GET /api/excerpt_with_alignment` — text with word-level audio timing
- `GET /api/audio/{translation}/{voice}/{book}/{chapter}.mp3` — audio files
- `GET /api/about` — Bible Garden About page (unchanged default); use
  `GET /api/about?app=lampada` for Lampada contacts and description.
  `app=bible-garden` explicitly selects the default; unknown values return 422.
  Both variants require the existing API key.
- `GET /api/version-check` — app version check
- `POST /api/ai/question` — Gemini companion reply (see below)
- `POST /api/ai/transcribe` — voice recording to text (see below)
- `POST /api/ai/scripture` — contextual Bible passage selection
  (`architect/scripture-select.md`)

All endpoints require `X-API-Key` header.

### AI endpoints

`POST /api/ai/question` accepts `{ "user" }` and calls the configured model.
The system prompt is applied server-side and the provider key never leaves
the server:

```dotenv
GEMINI_API_KEY=your-google-ai-studio-key
AI_QUESTION_PROVIDER=gemini
AI_SCRIPTURE_REWRITE_PROVIDER=gemini
AI_SCRIPTURE_RERANK_PROVIDER=gemini
AI_QUESTION_MODEL=gemini-3.7-flash
AI_TRANSCRIBE_MODEL=gemini-3.5-flash-lite
EMBEDDING_PROVIDER=gemini
EMBEDDING_MODEL=gemini-embedding-001
EMBEDDING_DIMENSIONS=768
AI_SCRIPTURE_REWRITE_MODEL=gemini-3.7-flash
AI_SCRIPTURE_RERANK_MODEL=gemini-3.5-flash-lite
AI_REQUESTS_PER_MINUTE=10
AI_REQUESTS_PER_CLIENT_PER_MINUTE=3
AI_CLIENT_HMAC_KEY=generate-a-separate-random-secret
TRUSTED_PROXY_HOSTS=my-nginx-container
```

### Choosing a provider per stage

The three chat stages — the question endpoint, the retrieval query rewrite
and the passage rerank — each name their transport, `gemini` or
`openai_compat` (any OpenAI-compatible `/chat/completions` server: vLLM,
Ollama, llama.cpp). Moving a stage to another model is an `.env` edit:

```dotenv
AI_QUESTION_PROVIDER=openai_compat
AI_SCRIPTURE_REWRITE_PROVIDER=openai_compat
AI_SCRIPTURE_RERANK_PROVIDER=openai_compat
AI_OPENAI_COMPAT_ENDPOINT=https://your-model-server:8443/v1
AI_OPENAI_COMPAT_API_KEY=server-key      # may be empty: no Authorization header
AI_QUESTION_MODEL=your-model
AI_SCRIPTURE_REWRITE_MODEL=your-model
AI_SCRIPTURE_RERANK_MODEL=your-model
```

A stage can override the shared endpoint and key with
`AI_QUESTION_ENDPOINT` / `AI_QUESTION_API_KEY` (and the
`AI_SCRIPTURE_REWRITE_*` / `AI_SCRIPTURE_RERANK_*` pairs) — that is how one
stage runs on a different server, or bills a different key. The key must
never be part of the URL; an endpoint carrying credentials or a query string
is rejected at startup. Model names have no defaults on either provider, and
the three `AI_*_PROVIDER` variables are required as soon as any AI is
configured: an `.env` with only `GEMINI_API_KEY` refuses to start and names
them. See `architect/adr/0009-provider-independent-llm-client.md`.

`POST /api/ai/transcribe` is Gemini-only for now — there is no
`AI_TRANSCRIBE_PROVIDER` — so a deployment whose chat stages are fully local
still needs `GEMINI_API_KEY` and `AI_TRANSCRIBE_MODEL` for it; without them
that one endpoint answers `502` and everything else keeps working.

### Choosing the embedding provider

Embeddings have their own variable, because they name the stored vector
index and not merely a call:

```dotenv
EMBEDDING_PROVIDER=local                 # or: gemini
EMBEDDING_MODEL=BAAI/bge-m3              # gemini: gemini-embedding-001
EMBEDDING_DIMENSIONS=1024                # gemini: 768
EMBEDDING_MODEL_PATH=/models/bge-m3      # local only; must NOT be set on gemini
```

`EMBEDDING_PROVIDER` is required in **every** deployment, with or without any
AI key. `local` runs BAAI/bge-m3 on CPU inside the API process: no network
and no key, ~2.1 GiB of RSS held for the life of the process, ~39 ms per
query, and a ~1-hour CPU rebuild of the index (11 960 chunks, 8 cores). The weights are a read-only volume, never a
download — the image runs with `HF_HUB_OFFLINE=1`, so a missing directory
fails the start instead of fetching 2.3 GB:

```bash
huggingface-cli download BAAI/bge-m3 --local-dir /srv/models/bge-m3
# .env: EMBEDDING_MODELS_DIR=/srv/models   (mounted read-only at /models)
python app/index_cli.py rebuild           # writes the new index version
# ...verify, switch EMBEDDING_PROVIDER, restart, then:
python app/index_cli.py rebuild --drop-other-versions
```

A rebuild keeps the rows of every other index version, so the old index
keeps serving while the new one is built and the switch is an `.env` edit
plus a restart. That edit is **four lines, not one**, in both directions:
provider, model, dimensions, and the path — which is required on `local` and
a startup error on `gemini`. Rolling back to Gemini therefore means removing
`EMBEDDING_MODEL_PATH` as well, or the service refuses to start naming it.
See
`architect/adr/0010-local-embeddings-bge-m3.md`.

The system prompt is **not** configurable: it is the versioned
`QUESTION_PROMPT_TEMPLATE` in `app/question_prompt.py`, filled per request by
`build_question_prompt` with the one thing that varies — the language to
answer in, detected from the message itself. It is product behaviour, is
reviewed and diffed like the rest of the code, and cannot drift between
environments. Changing it means editing that file and bumping
`QUESTION_PROMPT_VERSION` (currently `2`).

There are no defaults for the model names in the code. Once `GEMINI_API_KEY`
is set, `AI_QUESTION_MODEL`, `AI_TRANSCRIBE_MODEL`,
`AI_SCRIPTURE_REWRITE_MODEL` and `AI_SCRIPTURE_RERANK_MODEL` must be set too, or the
service refuses to start and prints all the missing ones at once. A chat
stage on `openai_compat` must name its model with or without a Gemini key.
`EMBEDDING_MODEL`, `EMBEDDING_DIMENSIONS` and `EMBEDDING_PROVIDER` are
required with or without a key: they name the vector index the service
reads, not a provider call.
Without `GEMINI_API_KEY` the AI endpoints simply report that AI is not
configured and the rest of the API runs normally: `/api/ai/question` and
`/api/ai/transcribe` answer `502`, `/api/ai/scripture` degrades to the safe
pool with `fallback_reason=ai_unavailable`. Two variables, and only these
two, decide whether the AI surface is usable — `GEMINI_API_KEY` (missing →
`502`) and `AI_CLIENT_HMAC_KEY` (missing → `503`, because the per-client
rate limiter fails closed rather than silently dropping its limit).

`AI_SCRIPTURE_REWRITE_API_KEY` is optional and applies to the retrieval rewrite
stage only — set it to bill that one stage (the only one whose model exhausts
its free daily quota) to a separate, paid key; leave it out and rewrites use
the shared key of that stage's provider, like every other call. Setting a
stage key while the stage runs on Gemini and `GEMINI_API_KEY` is empty is a
startup error.

The Google key must never be included in the mobile application. Before a
production deployment, also set a hard quota for the key in Google AI Studio;
the server-side request limit is only an additional safeguard. The counters
are held in process memory, reset on restart, and are not shared across API
workers or replicas. Run a single worker or add an external distributed
limiter before scaling the service horizontally.

Only a direct reverse-proxy peer may speak for its clients through
`X-Forwarded-For`. Name it with `TRUSTED_PROXY_HOSTS` (comma-separated
container or DNS names, resolved at startup and re-resolved every
`TRUSTED_PROXY_DNS_TTL_SECONDS`, default 30) rather than pinning an address:
container addresses do not survive a host reboot, and a stale pin makes the
service record every caller as the proxy itself while the per-client AI rate
limit silently becomes a global one. `TRUSTED_PROXY_IPS` still accepts literal
addresses and CIDR networks for deployments that need them — a whole subnet
trusts every workload in it, so prefer the name. Leave all of them unset when
the API is exposed directly; the service says which mode it is in on startup
(`docker logs <container> | grep 'Trusted prox'`) and logs a forwarded header
arriving from an untrusted peer. In a header from a trusted peer the client is
the **rightmost** address — the one the proxy itself appended; everything to
the left of it was supplied by the caller and is never believed. `AI_CLIENT_HMAC_KEY` pseudonymizes client
addresses and must be different from both API keys.

`POST /api/ai/transcribe` accepts `multipart/form-data` with a required
M4A `file` and an optional BCP 47 `locale` (for example, `ru-RU`). The locale is
only a weak language hint: Gemini transcribes the recording verbatim in its
original language and does not translate it. Files larger than 14 MiB are
rejected. Recordings, filenames, and transcripts are not written to logs or
persistent storage.

## License

[GPLv3](LICENSE)
