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
- `GET /api/about` — about page
- `GET /api/version-check` — app version check
- `POST /api/ai/question` — Gemini companion reply (see below)
- `POST /api/ai/transcribe` — voice recording to text (see below)
- `POST /api/ai/scripture` — contextual Bible passage selection
  (`architect/scripture-select.md`)

All endpoints require `X-API-Key` header.

### AI endpoints

`POST /api/ai/question` accepts `{ "user" }` and calls Gemini. The system
prompt is applied server-side and the Google AI Studio key never leaves the
server:

```dotenv
GEMINI_API_KEY=your-google-ai-studio-key
AI_QUESTION_MODEL=gemini-3.7-flash
AI_TRANSCRIBE_MODEL=gemini-3.5-flash-lite
EMBEDDING_MODEL=gemini-embedding-001
EMBEDDING_DIMENSIONS=768
AI_SCRIPTURE_REWRITE_MODEL=gemini-3.7-flash
AI_SCRIPTURE_RERANK_MODEL=gemini-3.5-flash-lite
AI_REQUESTS_PER_MINUTE=10
AI_REQUESTS_PER_CLIENT_PER_MINUTE=3
AI_CLIENT_HMAC_KEY=generate-a-separate-random-secret
TRUSTED_PROXY_HOSTS=my-nginx-container
```

The system prompt is **not** configurable: it is the versioned constant
`QUESTION_PROMPT` in `app/question_prompt.py`. It is product behaviour, is
reviewed and diffed like the rest of the code, and cannot drift between
environments. Changing it means editing that file and bumping
`QUESTION_PROMPT_VERSION`.

There are no defaults for the model names in the code. Once `GEMINI_API_KEY`
is set, `AI_QUESTION_MODEL`, `AI_TRANSCRIBE_MODEL`,
`AI_SCRIPTURE_REWRITE_MODEL` and `AI_SCRIPTURE_RERANK_MODEL` must be set too, or the
service refuses to start and prints all the missing ones at once.
`EMBEDDING_MODEL` and `EMBEDDING_DIMENSIONS` are required with or without a
key: they name the vector index the service reads, not a provider call.
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
`GEMINI_API_KEY` like every other call. Setting it without `GEMINI_API_KEY`
is a startup error.

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
