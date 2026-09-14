# Bible API

Public read-only REST API for the [Bible Garden](https://github.com/Bible-Garden) app. Serves Bible texts, translations, and word-level audio alignments.

Built with FastAPI and MySQL.

Model benchmarks, evaluation datasets, generated outputs and research notes
live in [BibleGarden/AI-Evaluation](https://github.com/BibleGarden/AI-Evaluation).
This repository keeps only the small immutable snapshots required by
production regression tests under `tests/fixtures/ai/`.

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
  (`chapters_count`, `chapters_without_text`, `has_text`). The canonical
  structure remains visible when a translation has no text for a declared
  book; excerpt navigation skips those books.
- `GET /api/excerpt_with_alignment` — text with word-level audio timing.
  `?excerpt=` is `<book alias> <chapter>[:<verse>[-<verse>]]` (`gen 1`,
  `gen 1:1`, `gen 1:1-3`), several references may be listed in one value. The
  book is named by its **alias as returned by
  `GET /api/translations/{code}/books`** — Latin letters, **case-insensitive**
  (`gen`, `Gen`, `GEN` are one book). A value that does not parse returns 422
  naming the format; an alias that belongs to no book of this translation
  returns 404.
- `GET /api/audio/{translation}/{voice}/{book}/{chapter}.mp3` — audio files
- `GET /api/health` — API-key-protected readiness check. Returns 200 only when
  the database has at least one language; database failures and an empty
  database return a generic 503. It does not call AI or record usage stats.
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

Question and scripture requests accept an optional strict boolean `prefetch`
(default `false`). Clients set it to `true` only for speculative work. The
server can decline before generation or corpus loading with HTTP 429 and
`{"detail":"prefetch_disabled"}` or `{"detail":"prefetch_limit_exceeded"}`.
Only the latter includes `Retry-After` seconds. Clients keep a declined slot
empty, without retrying or showing an error; actual demand sends an ordinary
request. Existing requests without the field retain their behavior.

Prefetch is disabled by default, independently for questions and scripture.
`AI_QUESTION_PREFETCH_ENABLED` and `AI_SCRIPTURE_PREFETCH_ENABLED` enable it.
Each has `AI_<QUESTION|SCRIPTURE>_PREFETCH_REQUESTS_PER_MINUTE` (default 2) and
`AI_<QUESTION|SCRIPTURE>_PREFETCH_REQUESTS_PER_CLIENT_PER_MINUTE` (default 1).
Limits must be positive integers; invalid configuration stops startup. Enabling
prefetch requires `AI_ENABLED=true` and its valid provider/HMAC configuration. Accepted
prefetch also consumes the existing total quota; a policy refusal does not.
Counters use rolling 60-second windows, HMAC client-IP pseudonyms and the
existing single-worker, process-local storage. This controls request frequency,
not provider spending in currency. Apply configuration through the deployment
runbook; roll out the API before a client that sends `prefetch`.

`POST /api/ai/question` asks one leading question about a prayer. It accepts
`{ "topic", "stage", "messages" }` — the topic (may be empty), the stage
(`first`, `next` or `reflect`) and the conversation so far as
`{ "role": "assistant" | "user", "text" }` turns — and answers
`{ "text", "novel", "subject" }`; `subject` is the optional short subject of
reflection identified by the model. The instructions for the stage and the
system prompt are built server-side and the provider key never leaves the
server:

```json
{"topic": "Отношения с семьёй", "stage": "next",
 "messages": [{"role": "assistant", "text": "Что сейчас тревожит тебя?"},
              {"role": "user", "text": "Мне одиноко."}]}
```

`first` carries no history; a non-empty history ends with a `user` turn;
`messages: []` is normal for the other two stages.

An optional `default_language` is `"ru"`, `"uk"`, `"en"` or `null` (the
default). It is a UI language hint used only when the detector abstains across
the entire existing source chain. Any detected language wins; in particular,
the hint never turns a detected unsupported language into a supported one.
It affects the question system and stage prompts only, including retries, and
does not select safety replies, transcription, scripture or quotation
languages.

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

The AI configuration is explicit and has no shared provider credentials or
migration fallbacks. `AI_ENABLED` is required in every deployment:

| Block | Required values | Forbidden / omitted values |
|---|---|---|
| AI disabled | `AI_ENABLED=false` | every stage-specific `AI_*_PROVIDER/MODEL/ENDPOINT/API_KEY/REASONING_EFFORT` and `AI_CLIENT_HMAC_KEY` |
| Gemini stage | `AI_ENABLED=true`, stage `PROVIDER=gemini`, `MODEL`, non-empty stage `API_KEY` | stage `ENDPOINT`; chat-stage `REASONING_EFFORT` |
| OpenAI-compatible chat stage | `AI_ENABLED=true`, stage `PROVIDER=openai_compat`, `MODEL`, `ENDPOINT`, present stage `API_KEY` (may be empty), `REASONING_EFFORT=omit\|none\|low\|medium\|high` | shared endpoint/key/reasoning variables |
| Strict OpenRouter question | `AI_QUESTION_PROVIDER=openrouter`, `MODEL=google/gemma-4-31b-it`, `ENDPOINT=https://openrouter.ai/api/v1`, non-empty `API_KEY`, `REASONING_EFFORT=none` | OpenRouter on rewrite/rerank; any other model, endpoint or reasoning value; environment-provided request JSON |
| OpenAI-compatible transcription | `AI_TRANSCRIBE_PROVIDER=openai_compat`, `MODEL`, `ENDPOINT`, present `API_KEY` (may be empty) | transcription reasoning variables |
| Local transcription | `AI_TRANSCRIBE_PROVIDER=local`, `MODEL`, `MODEL_PATH` | transcription `ENDPOINT` and `API_KEY` |
| Embeddings | `EMBEDDING_PROVIDER`, `MODEL`, `DIMENSIONS` in every deployment | provider-specific unused fields |
| OpenAI-compatible embeddings | `EMBEDDING_ENDPOINT` and present `EMBEDDING_API_KEY` (may be empty) | `EMBEDDING_MODEL_PATH` |
| Gemini embeddings | non-empty `EMBEDDING_API_KEY` | `EMBEDDING_ENDPOINT` and `EMBEDDING_MODEL_PATH` |
| Local embeddings | `EMBEDDING_MODEL_PATH` | `EMBEDDING_ENDPOINT` and `EMBEDDING_API_KEY` |

An `openai_compat` API-key variable may be present and empty. That is the
explicit statement that the endpoint needs no Authorization header. Gemini
and OpenRouter keys must be non-empty. Provider secrets are exported from the
shell before `docker compose up` and are never placed in `.env`:

```bash
export AI_QUESTION_API_KEY='question-provider-key'
export AI_SCRIPTURE_REWRITE_API_KEY=
export AI_SCRIPTURE_RERANK_API_KEY=
export AI_TRANSCRIBE_API_KEY='audio-key'
export EMBEDDING_API_KEY='embedding-key'
```

Compose preserves the distinction between an unexported variable and an
explicitly empty export. An unexported key becomes a private sentinel that
startup validation treats as missing; an empty export remains valid no-auth
only for an `openai_compat` stage.
When `AI_ENABLED=false`, the four unused AI keys need not be exported.
Remote embeddings remain independent and still need `EMBEDDING_API_KEY`.

For a controlled local/test incident investigation only,
`AI_QUESTION_LOG_PROVIDER_BODIES=true` writes each question-provider JSON
payload and raw response body to the service log. These records contain
prayer-derived text. They never include request headers, the provider URL or
the configured API key; if a provider echoes that key in its body, it is
replaced with `[REDACTED_API_KEY]`. Each provider call gets a new `call_id`,
shared by its request and response records so concurrent calls and a second
question generation can be separated. The setting defaults to `false`, accepts
only the exact strings `true` and `false`, and a malformed value aborts
startup. Keep it `false` in production and disable it after the investigation.

The local Compose service uses Docker's `json-file` driver with `max-size=10m`
and `max-file=3`, bounding its active log history to about 30 MiB. Rotation is
size-based, not time-based. Setting the diagnostic flag back to `false` or
restarting the existing container stops new body records but does **not**
delete records already held in its current or rotated log files.

After the investigation, purge those local Docker logs by replacing exactly
the `bible-api` container. First set
`AI_QUESTION_LOG_PROVIDER_BODIES=false` in `.env` and export the five provider
keys through the normal shell-only secret source described above. Then run:

```bash
cd /root/cep/Bible-API
old_container_id="$(docker compose ps -q bible-api)"
test -n "$old_container_id"
docker compose up -d --force-recreate --no-deps bible-api
new_container_id="$(docker compose ps -q bible-api)"
test -n "$new_container_id"
test "$new_container_id" != "$old_container_id"
! docker inspect "$old_container_id" >/dev/null 2>&1
docker inspect "$new_container_id" \
  --format '{{json .HostConfig.LogConfig}}'
```

Compose removes that one old container and its Docker-managed `json-file`
logs; no filesystem-wide deletion or direct Docker-storage edit is needed.
This does not erase copies already exported to an external log collector,
backup or host snapshot; remove those through that system's own scoped
retention procedure if such a copy exists.

A mixed deployment can use OpenRouter for the user-facing question and a local
Qwen server through its OpenAI-compatible endpoint for rewrite and rerank:

```dotenv
AI_ENABLED=true
AI_CLIENT_HMAC_KEY=generate-a-separate-random-secret

AI_QUESTION_PROVIDER=openrouter
AI_QUESTION_MODEL=google/gemma-4-31b-it
AI_QUESTION_ENDPOINT=https://openrouter.ai/api/v1
AI_QUESTION_REASONING_EFFORT=none

AI_SCRIPTURE_REWRITE_PROVIDER=openai_compat
AI_SCRIPTURE_REWRITE_MODEL=qwen3-30b
AI_SCRIPTURE_REWRITE_ENDPOINT=http://qwen:8000/v1
AI_SCRIPTURE_REWRITE_REASONING_EFFORT=omit

AI_SCRIPTURE_RERANK_PROVIDER=openai_compat
AI_SCRIPTURE_RERANK_MODEL=qwen3-30b
AI_SCRIPTURE_RERANK_ENDPOINT=http://qwen:8000/v1
AI_SCRIPTURE_RERANK_REASONING_EFFORT=omit

AI_TRANSCRIBE_PROVIDER=openai_compat
AI_TRANSCRIBE_MODEL=deepdml/faster-whisper-large-v3-turbo-ct2
AI_TRANSCRIBE_ENDPOINT=https://your-audio-server/v1
```

For Maria's current local configuration, the question stage uses the strict
OpenRouter profile while rewrite remains on Cerebras and rerank on the
independently served company model. The three keys stay separate stage inputs;
none is stored in `.env`:

```bash
: "${OPENROUTER_API_KEY:?export the shell-only OpenRouter key first}"
: "${CEREBRAS_API_KEY:?export the existing shell-only Cerebras key first}"
: "${VLLM_SECONDARY_API_KEY:?export the shell-only llm.ai2.ru dev key first}"
export AI_QUESTION_API_KEY="$OPENROUTER_API_KEY"
export AI_SCRIPTURE_REWRITE_API_KEY="$CEREBRAS_API_KEY"
export AI_SCRIPTURE_RERANK_API_KEY="$VLLM_SECONDARY_API_KEY"
```

```dotenv
AI_QUESTION_PROVIDER=openrouter
AI_QUESTION_MODEL=google/gemma-4-31b-it
AI_QUESTION_ENDPOINT=https://openrouter.ai/api/v1
AI_QUESTION_REASONING_EFFORT=none
AI_QUESTION_TIMEOUT_SECONDS=20
AI_QUESTION_MAX_TOKENS=4096

AI_SCRIPTURE_REWRITE_PROVIDER=openai_compat
AI_SCRIPTURE_REWRITE_MODEL=qwen-3.8-27b
AI_SCRIPTURE_REWRITE_ENDPOINT=https://api.cerebras.ai/v1
AI_SCRIPTURE_REWRITE_REASONING_EFFORT=none

AI_SCRIPTURE_RERANK_PROVIDER=openai_compat
AI_SCRIPTURE_RERANK_MODEL=qwen3-30b-a3b-instruct-2507
AI_SCRIPTURE_RERANK_ENDPOINT=https://llm.ai2.ru/v1
AI_SCRIPTURE_RERANK_REASONING_EFFORT=omit
```

`none`, `low`, `medium` and `high` are sent byte-for-byte as the
`reasoning_effort` request field. `omit` is an explicit compatibility choice
that leaves the field out; it is not a default or fallback. Missing or invalid
reasoning configuration aborts startup for an OpenAI-compatible chat stage.
Transcription and embeddings do not have a reasoning setting.

The `openrouter` value is a distinct question-only request profile, never an
endpoint-host inference. It requires the exact model, endpoint and `none`
reasoning declaration shown above, plus a non-empty key. Its
OpenAI-compatible request always contains
`provider: {allow_fallbacks: false, data_collection: "deny"}` and
`reasoning: {enabled: false}`, and never contains the flat
`reasoning_effort` field. These objects are fixed in reviewed code; no
environment variable can inject or replace provider JSON. Rewrite, rerank,
transcription and embeddings retain their previous providers and wire shapes.

`AI_QUESTION_MAX_TOKENS` is the question stage's output safety ceiling for all
transports: chat-completions requests send it as `max_tokens`, while Gemini
requests send it as `maxOutputTokens`. It defaults to `4096`; an
explicit blank, non-integer or non-positive value aborts startup. The limit is
not shared with scripture rewrite, scripture rerank or transcription.

A disabled AI surface is intentionally short; embeddings remain fully
configured because scripture retrieval and index identity are separate:

```dotenv
AI_ENABLED=false

EMBEDDING_PROVIDER=openai_compat
EMBEDDING_MODEL=BAAI/bge-m3
EMBEDDING_DIMENSIONS=1024
EMBEDDING_ENDPOINT=https://your-embedding-server/v1
```

For local transcription, replace its remote endpoint/key with
`AI_TRANSCRIBE_MODEL_PATH`. For local embeddings, replace
`EMBEDDING_ENDPOINT`/`EMBEDDING_API_KEY` with
`EMBEDDING_MODEL_PATH`. Model names have no defaults. Endpoints accept only
absolute HTTP(S) URLs without credentials or query strings.

The removed `GEMINI_API_KEY`, `AI_OPENAI_COMPAT_ENDPOINT` and
`AI_OPENAI_COMPAT_API_KEY` variables are startup errors even when blank. This
deliberately exposes incomplete migrations. See
`architect/adr/0019-explicit-ai-configuration.md`; it supersedes the
configuration-resolution parts of ADR 0008, 0009, 0012 and 0014 without
changing their provider transports, models, prompts or pipeline.

`DEBUG` defaults to `false`. In local development only, `DEBUG=true` makes
`POST /api/ai/question` use the complete English prompt when language
detection returns no code or a code outside `ru`, `uk`, `en`. With the default
false, either result is a `422` before a provider call. Only exact `true` and
`false` are accepted; detector exceptions are never converted into this route.

The system prompt is not configurable: it is the versioned
`QUESTION_PROMPT_TEMPLATE` in `app/question_prompt.py`. Provider and model
changes still require the benchmark and architectural process documented by
the existing AI ADRs. Operational limits and timeouts retain their documented
defaults.
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
