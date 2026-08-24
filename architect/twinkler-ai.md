# Twinkler AI

## Public contract

`POST /api/twinkler/v1/complete` accepts a JSON object with one required field:

```json
{"user": "User message"}
```

The endpoint requires the common `X-API-Key` header. Unknown JSON fields are
rejected. `user` must contain 1–16000 characters. The response is
`{ "text": "..." }` on success. Documented errors are `403`, `429` with
`Retry-After`, `502`, and `503`; FastAPI validation errors use `422`.

`POST /api/twinkler/v1/transcribe` accepts `multipart/form-data` with a required
M4A `file` and an optional BCP 47 `locale`. The response is the same
`{ "text": "..." }` shape. The locale is a weak disambiguation hint only; the
recording is transcribed verbatim in its original language without translation
or generated additions. Empty files and invalid locales return `422`, files
larger than 14 MiB return `413`, and unsupported audio types return `415`.

## Gemini contract

The service calls
`POST https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent`
with the user message as user content. `TWINKLER_SYSTEM_PROMPT` is always read
from the server environment and sent as `system_instruction`; clients cannot
override it. `GEMINI_API_KEY` is sent only in the `x-goog-api-key` header. The
system prompt is limited to 8000 characters. The request sets
`maxOutputTokens` to `1024` and `temperature` to `0.7`.

Provider timeouts, HTTP errors, malformed responses and empty output are
returned to the client as `502 AI service unavailable` without provider
details. Missing server configuration has the same public response.

Transcription uses `GEMINI_TRANSCRIPTION_MODEL` (default
`gemini-3.5-flash-lite`) and the same configured Gemini API key. The M4A bytes
are base64-encoded into an `inline_data` part alongside a server-controlled
verbatim-transcription instruction. `audio/mp4`, `audio/x-m4a` and `audio/m4a`
are accepted;
a `.m4a` filename is used as a fallback only when the client sends no MIME type
or `application/octet-stream`. The request uses temperature `0` and a 60-second
provider timeout. The uploaded file is closed after it is read and is never
persisted by the application.

## Rate limiting and observability

Before calling Gemini, the service reserves a request in an in-memory rolling
window protected by a process lock. Two 60-second limits are enforced:

- global: `GEMINI_REQUESTS_PER_MINUTE`;
- per client address: `GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE`.

The in-memory client identifier is an HMAC-SHA-256 pseudonym created with the
separate `TWINKLER_CLIENT_HMAC_KEY`; the original address is not retained.
Expired timestamps and inactive client buckets are removed periodically.
Exceeded limits return `429` with `Retry-After`. Counters reset on process
restart and are not shared across workers or replicas, so production runs a
single worker until a dedicated distributed limiter is introduced. Missing
HMAC configuration fails closed with `503` and does not call Gemini.

For both Twinkler endpoints, standard request statistics store endpoint metadata,
status, latency, an HMAC pseudonym truncated to 40 hexadecimal characters,
and an empty user-agent value. Prompt, response body, original client address,
user agent, recording, filename, and transcript are never stored. Raw
statistics are purged after 14 days by
`app/aggregate_stats.py`; daily aggregates retain counts only.

Client addresses come from the direct peer. `X-Forwarded-For` is used only
when the peer is explicitly listed in `TRUSTED_PROXY_IPS`; malformed forwarded
addresses fall back to the peer address. The bundled FastAPI commands disable
Uvicorn's implicit proxy-header processing so this trust decision remains in
the application resolver.
