# Lampada AI

## Public contract

`POST /api/lampada/v1/complete` accepts a JSON object with one required field:

```json
{"user": "User message"}
```

The endpoint requires the common `X-API-Key` header. Unknown JSON fields are
rejected. `user` must contain 1–16000 characters. The response is
`{ "text": "..." }` on success. Documented errors are `403`, `429` with
`Retry-After`, `502`, and `503`; FastAPI validation errors use `422`.

## Gemini contract

The service calls
`POST https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent`
with the user message as user content. `LAMPADA_SYSTEM_PROMPT` is always read
from the server environment and sent as `system_instruction`; clients cannot
override it. `GEMINI_API_KEY` is sent only in the `x-goog-api-key` header. The
system prompt is limited to 8000 characters. The request sets
`maxOutputTokens` to `1024` and `temperature` to `0.7`.

Provider timeouts, HTTP errors, malformed responses and empty output are
returned to the client as `502 AI service unavailable` without provider
details. Missing server configuration has the same public response.

## Rate limiting and observability

Before calling Gemini, the service reserves a request in the MySQL table
`lampada_rate_limit_events`. A MySQL advisory lock serializes reservations
across workers and replicas. Two rolling 60-second limits are enforced:

- global: `GEMINI_REQUESTS_PER_MINUTE`;
- per client address: `GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE`.

The stored client identifier is an HMAC-SHA-256 pseudonym created with the
separate `LAMPADA_CLIENT_HMAC_KEY`; the limiter table does not contain the
original address. Rows live for at most one rolling window and expired rows
are deleted during the next reservation. Exceeded limits return `429` with
`Retry-After`. If MySQL or its advisory lock is unavailable, the endpoint
fails closed with `503` and does not call Gemini. The table is installed before
deployment from `sql/001_create_lampada_rate_limit_events.sql`; the runtime
database account does not need DDL privileges.

For this endpoint, standard request statistics store endpoint metadata,
status, latency, an HMAC pseudonym truncated to 40 hexadecimal characters,
and an empty user-agent value. Prompt, response body, original client address,
and user agent are never stored. Raw statistics are purged after 14 days by
`app/aggregate_stats.py`; daily aggregates retain counts only.

Client addresses come from the direct peer. `X-Forwarded-For` is used only
when the peer is explicitly listed in `TRUSTED_PROXY_IPS`; malformed forwarded
addresses fall back to the peer address. The bundled FastAPI commands disable
Uvicorn's implicit proxy-header processing so this trust decision remains in
the application resolver.
