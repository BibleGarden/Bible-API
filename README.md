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
- `GET /api/excerpt_with_alignment` — text with word-level audio timing.
  `?excerpt=` is `<book alias> <chapter>[:<verse>[-<verse>]]` (`gen 1`,
  `gen 1:1`, `gen 1:1-3`), several references may be listed in one value. The
  book is named by its **alias as returned by
  `GET /api/translations/{code}/books`** — Latin letters, **case-insensitive**
  (`gen`, `Gen`, `GEN` are one book). A value that does not parse returns 422
  naming the format; an alias that belongs to no book of this translation
  returns 404.
- `GET /api/audio/{translation}/{voice}/{book}/{chapter}.mp3` — audio files
- `GET /api/about` — Bible Garden About page (unchanged default); use
  `GET /api/about?app=lampada` for Lampada contacts and description.
  `app=bible-garden` explicitly selects the default; unknown values return 422.
  Both variants require the existing API key.
- `GET /api/version-check` — app version check. `?app_version=` is required and
  takes one to three numeric components (missing ones are read as zero);
  anything else returns 422. `GET /api/version-check?app=lampada&app_version=1.0.0`
  applies Lampada's own thresholds and store link; omitting `app` keeps the
  Bible Garden response released clients already receive, with `app` added to
  it. `update_type` is `none`, `soft` or `hard`; Lampada always answers `none`
  until its App Store listing is public — see
  `architect/adr/0013-application-version-policies.md` for the activation
  constants.
- `POST /api/ai/question` — AI companion question (see below)
- `POST /api/ai/transcribe` — voice recording to text (see below)
- `POST /api/ai/scripture` — contextual Bible passage selection
  (`architect/scripture-select.md`)

All endpoints require `X-API-Key` header.

### AI endpoints

`POST /api/ai/question` asks one leading question about a prayer. It accepts
`{ "topic", "stage", "messages" }` — the topic (may be empty), the stage
(`first`, `next` or `reflect`) and the conversation so far as
`{ "role": "assistant" | "user", "text" }` turns — and answers
`{ "text", "novel" }`. The instructions for the stage and the system prompt are
built server-side and the provider key never leaves the server:

```json
{"topic": "Отношения с семьёй", "stage": "next",
 "messages": [{"role": "assistant", "text": "Что сейчас тревожит тебя?"},
              {"role": "user", "text": "Мне одиноко."}]}
```

`first` carries no history; a non-empty history ends with a `user` turn;
`messages: []` is normal for the other two stages.

An optional `skipped_questions` (list of strings, at most 10 of at most 300
characters, empty by default) carries the questions the person asked to
replace, so the next one takes another direction. It must be empty with
`first`, and it counts towards the same 16 000-character total. Send **every**
replaced or skipped question of the current prayer, chronologically, and never
one that is already an `assistant` turn of `messages` — an answered question
belongs in the history, a replaced one here, and the two lists do not overlap.

`novel` is additive and says whether the returned text repeats a question the
person has already been shown in this prayer (those `assistant` turns plus
`skipped_questions`). The server checks it, and on a repeat generates once more
inside the same request budget; `novel: false` means that second question
repeated too, was not affordable or failed — the best text obtained is still
returned, so a client that reads only `text` behaves exactly as before. Details:
`architect/twinkler-ai.md`,
`architect/adr/0015-skipped-questions-in-question-request.md` and
`architect/adr/0016-question-novelty-check.md`.

The whole AI surface is configured by environment variables:

```dotenv
GEMINI_API_KEY=your-google-ai-studio-key
AI_QUESTION_PROVIDER=gemini
AI_SCRIPTURE_REWRITE_PROVIDER=gemini
AI_SCRIPTURE_RERANK_PROVIDER=gemini
AI_TRANSCRIBE_PROVIDER=gemini
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
the four `AI_*_PROVIDER` variables are required as soon as any AI is
configured: an `.env` with only `GEMINI_API_KEY` refuses to start and names
them. See `architect/adr/0009-provider-independent-llm-client.md`.

### Choosing the transcription provider

`POST /api/ai/transcribe` has its own provider, because speech is not the
chat protocol — `gemini`, `openai_compat` (Whisper behind the OpenAI **audio**
API: `POST {endpoint}/audio/transcriptions`, which vLLM, speaches and
faster-whisper-server all serve) or `local` (faster-whisper on this machine's
CPU, no network and no key):

```dotenv
AI_TRANSCRIBE_PROVIDER=openai_compat
AI_TRANSCRIBE_ENDPOINT=https://your-audio-server:8000/v1   # else AI_OPENAI_COMPAT_ENDPOINT
AI_TRANSCRIBE_API_KEY=server-key                           # may be empty
AI_TRANSCRIBE_MODEL=Systran/faster-whisper-large-v3

# ...or in this process:
AI_TRANSCRIBE_PROVIDER=local
AI_TRANSCRIBE_MODEL=small                       # small | medium
AI_TRANSCRIBE_MODEL_PATH=/models/whisper/small  # local only; must NOT be set otherwise
```

The request, the response and every status code are identical whoever
answers: the recording is transcribed verbatim in its own language and the
optional locale is only a hint. `local` weights are the same read-only
`/models` volume the embeddings use and are loaded at start-up (a missing
directory fails the start, nothing is ever downloaded):

```bash
hf download Systran/faster-whisper-small --local-dir /srv/models/whisper/small
```

Measured on 8 CPU cores, int8: 0.07-0.22x the audio duration, 0.85 GB
(`small`) to 2.1 GB (`medium`) of RSS, word error rate on Russian 0.153
(`small`) / 0.100 (`medium`) against Gemini's 0.019. The production provider —
`large-v3-turbo` on a CPU behind the audio API — measures 0.037 on Russian
(character error rate 0.003, Gemini's own) at 0.20x the audio and no memory on
this side. Full tables, the side-by-side transcripts and how to re-measure
(`evaluation/transcribe_bench.py remote`): `evaluation/README.md` and
`architect/adr/0012-speech-transcription-providers.md`.

### Choosing the embedding provider

Embeddings have their own variable, because they name the stored vector
index and not merely a call:

```dotenv
EMBEDDING_PROVIDER=openai_compat         # or: local, gemini
EMBEDDING_MODEL=BAAI/bge-m3              # gemini: gemini-embedding-001
EMBEDDING_DIMENSIONS=1024                # gemini: 768
EMBEDDING_ENDPOINT=https://llm.ai2.ru/v1 # openai_compat only (else the shared
EMBEDDING_API_KEY=...                    #   AI_OPENAI_COMPAT_* pair is used)
# EMBEDDING_MODEL_PATH=/models/bge-m3    # local only; a startup error otherwise
```

`EMBEDDING_PROVIDER` is required in **every** deployment, with or without any
AI key.

`openai_compat` is the production value (ADR 0014): the same BAAI/bge-m3, run
on the company server, over `POST {EMBEDDING_ENDPOINT}/embeddings`. No weights
in this process (72 MB of application against 2.1 GiB; a *serving* instance is
~300-350 MB either way, because the corpus cache — vector index, BM25, texts —
is 208 MB at `@1024` whichever provider computes the vectors), 137 ms per query, and
the index version is unchanged — `openai_compat` and `local` name the **same**
`c3:BAAI/bge-m3@1024`, so switching between them is an `.env` edit and a
restart, never a rebuild. Verified against 40 rows of the live index: cosine
1.000000 median, and the two exceptions are the 512-token window of the local
client, which queries never reach (ADR 0014).

`local` runs BAAI/bge-m3 on CPU inside the API process: no network
and no key, ~2.1 GiB of RSS held for the life of the process, ~39 ms per
query, and a ~1-hour CPU rebuild of the index (11 960 chunks, 8 cores). It is
the fallback, and it is how the index is built on the machine that owns it
(production receives it through `GET /api/import`). The weights are a read-only volume, never a
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
plus a restart. Between bge-m3 and Gemini that edit is **four lines, not
one**, in both directions: provider, model, dimensions, and the path — which
is required on `local` and a startup error on the other two. Rolling back to
Gemini therefore means removing `EMBEDDING_MODEL_PATH` as well, or the service
refuses to start naming it; moving between `local` and `openai_compat` means
swapping the path for the endpoint/key pair, and nothing else. See
`architect/adr/0010-local-embeddings-bge-m3.md` and
`architect/adr/0014-remote-embeddings-openai-compat.md`.

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
