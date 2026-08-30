# Twinkler AI

## Public contract

> **Renamed 2026-08-30 (ClickUp 86cbbmwjk).** These two routes were
> `POST /api/twinkler/v1/complete` and `POST /api/twinkler/v1/transcribe`
> until then: methods are not named after a client app (`twinkler`) or a
> provider's jargon (`complete`), and the `v1` was never a real version.
> Bodies, responses, headers, authentication and limits are unchanged — only
> the paths moved. The old paths return 404; there are no aliases (a single
> unpublished client, renamed in a paired mobile ticket). The module and the
> handlers keep their names.

> **Environment variables renamed 2026-08-30 (ClickUp 86cbbmy8d).** The
> settings now mirror the method they configure: `GEMINI_MODEL` →
> `AI_QUESTION_MODEL`, `GEMINI_TRANSCRIPTION_MODEL` → `AI_TRANSCRIBE_MODEL`,
> `GEMINI_REQUESTS_PER_[CLIENT_]MINUTE` → `AI_REQUESTS_PER_[CLIENT_]MINUTE`,
> `TWINKLER_CLIENT_HMAC_KEY` → `AI_CLIENT_HMAC_KEY`. Values were not touched,
> so existing client pseudonyms stay stable (the HMAC is keyed by the value;
> the variable name never enters the digest). `TWINKLER_SYSTEM_PROMPT` was
> deleted — see "System prompt" below. No old name is accepted as an alias:
> a forgotten one fails the start naming the variable it wants.

`POST /api/ai/question` accepts a JSON object with one required field:

```json
{"user": "User message"}
```

The endpoint requires the common `X-API-Key` header. Unknown JSON fields are
rejected. `user` must contain 1–16000 characters. The response is
`{ "text": "..." }` on success. Documented errors are `403`, `429` with
`Retry-After`, `502`, and `503`; FastAPI validation errors use `422`.

`POST /api/ai/transcribe` accepts `multipart/form-data` with a required
M4A `file` and an optional BCP 47 `locale`. The response is the same
`{ "text": "..." }` shape. The locale is a weak disambiguation hint only; the
recording is transcribed verbatim in its original language without translation
or generated additions. Empty files and invalid locales return `422`, files
larger than 14 MiB return `413`, and unsupported audio types return `415`.

## System prompt

The system prompt of `POST /api/ai/question` is the constant
`QUESTION_PROMPT` in `app/question_prompt.py`, versioned by
`QUESTION_PROMPT_VERSION` (currently `1`) in the same way as
`query_rewrite.REWRITE_PROMPT_VERSION` and
`passage_rerank.RERANK_PROMPT_VERSION`. Changing the wording means editing
that file and bumping the version.

It used to be the environment variable `TWINKLER_SYSTEM_PROMPT`. That was the
wrong home for it: the prompt is product behaviour, not a deployment knob, so
local and production could differ without anyone noticing a difference in
answers, and every test run had to inject a stand-in value. It was moved into
the code byte for byte on 2026-08-30 (ClickUp 86cbbmy8d); v1 is exactly the
text production ran until that day. The prompt is public from then on — the
repository is public, and the owner approved the trade knowingly: the text was
never a secret, only unpublished, and it carries no key material.
`GEMINI_API_KEY` remains the only secret this endpoint has.

Two consequences in the code. The former runtime guards ("prompt is not
configured", "prompt is too long") were removed from `complete()` — they
protected against a bad environment value that can no longer exist, and a
literal cannot change between restarts; their invariants are asserted once in
`tests/test_twinkler_ai.py` instead. And the meaning of "AI is not
configured" narrowed accordingly (below).

## Gemini contract

The service calls
`POST https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent`
with the user message as user content and `QUESTION_PROMPT` as
`system_instruction`; clients cannot override it. `GEMINI_API_KEY` is sent
only in the `x-goog-api-key` header. The request sets `maxOutputTokens` to
`1024` and `temperature` to `0.7`.

Provider timeouts, HTTP errors, malformed responses and empty output are
returned to the client as `502 AI service unavailable` without provider
details. Missing server configuration has the same public response.

### When the AI surface is unavailable

Since 2026-08-30 exactly two variables decide it, and the prompt is not one
of them:

| Condition | `/api/ai/question` and `/api/ai/transcribe` |
| --- | --- |
| `GEMINI_API_KEY` unset or blank | `502 AI service unavailable` (no provider call is attempted) |
| `AI_QUESTION_MODEL` / `AI_TRANSCRIBE_MODEL` malformed | `502` — but unreachable in practice: with a key set, a missing model name aborts startup (ADR 0008) |
| `AI_CLIENT_HMAC_KEY` unset or blank | `503 AI service temporarily unavailable` — the per-client limiter fails closed instead of silently serving without a limit |

The 503 is raised before the provider is contacted. `POST /api/ai/scripture`
fails closed the same way, but not because the three endpoints share a
limiter — they don't: `twinkler_ai.py` (`RateLimiter(name="AI")`) and
`scripture_select.py` (`RateLimiter(name="scripture selection")`) each own a
separate limiter instance with its own counters and its own budget
(`config.py` spells out why they must not share one — one selection costs
~8 Gemini calls, so it must not starve, or be starved by, the chat-shaped
Twinkler endpoints). What the two limiters do share is the pseudonymisation
key: both reserve through `client_ip.pseudonymize_twinkler_client`, which
raises when `AI_CLIENT_HMAC_KEY` is unset or blank — so both fail closed on
the same missing variable, independently rather than jointly. The scripture
endpoint's 503 body is its own wording, `Scripture selection temporarily
unavailable`, not the `AI service temporarily unavailable` text in the table
above. Both branches are pinned by `test_missing_provider_key_is_502` and
`test_missing_hmac_key_is_503`.

Transcription uses `AI_TRANSCRIBE_MODEL` (required whenever
`GEMINI_API_KEY` is set; no default in code) and the same configured Gemini
API key. The M4A bytes
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

- global: `AI_REQUESTS_PER_MINUTE`;
- per client address: `AI_REQUESTS_PER_CLIENT_PER_MINUTE`.

The in-memory client identifier is an HMAC-SHA-256 pseudonym created with the
separate `AI_CLIENT_HMAC_KEY`; the original address is not retained.
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
when the peer is a trusted reverse proxy — a name in `TRUSTED_PROXY_HOSTS`
resolved at runtime, or an address/network in `TRUSTED_PROXY_IPS`
(`app/trusted_proxies.py`). The client is the **rightmost** element of that
header — the address the trusted proxy itself appended, found by reading right
to left past any further trusted hops. Nginx's `$proxy_add_x_forwarded_for`
preserves whatever the caller sent and appends `$remote_addr`, so an element
to the left of that is a caller-supplied claim; believing it would let anyone
mint a fresh client identity per request and walk past the per-client limit.
Malformed forwarded addresses fall back to the
peer address, and a forwarded header from an untrusted peer is ignored *and
logged* (ClickUp 86cbbq6vz: an unnoticed trust mismatch turns the per-client
limit into a global one). The bundled FastAPI commands disable
Uvicorn's implicit proxy-header processing so this trust decision remains in
the application resolver.
