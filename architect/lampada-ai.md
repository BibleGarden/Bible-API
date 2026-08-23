# Lampada AI

## Public contract

`POST /api/lampada/v1/complete` accepts a JSON object with one required field:

```json
{"user": "User message"}
```

The endpoint requires the common `X-API-Key` header. Unknown JSON fields are
rejected. The response is `{ "text": "..." }` on success.

## Gemini contract

The service calls
`POST https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent`
with the user message as user content. `LAMPADA_SYSTEM_PROMPT` is always read
from the server environment and sent as `system_instruction`; clients cannot
override it. `GEMINI_API_KEY` is sent only in the `x-goog-api-key` header.

Provider timeouts, HTTP errors, malformed responses and empty output are
returned to the client as `502 AI service unavailable` without provider
details. Missing server configuration has the same public response.

## Rate limiting and observability

Before calling Gemini, the service reserves a request in the MySQL table
`lampada_rate_limit_events`. A MySQL advisory lock serializes reservations
across workers and replicas. Two rolling 60-second limits are enforced:

- global: `GEMINI_REQUESTS_PER_MINUTE`;
- per client address: `GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE`.

The stored client identifier is a SHA-256 hash; the limiter table does not
contain the original address. Exceeded limits return `429` with `Retry-After`.
If MySQL or its advisory lock is unavailable, the endpoint fails closed with
`503` and does not call Gemini. Expired limiter events are deleted during the
next reservation.

Standard request statistics record only endpoint metadata, status, latency,
client address and user agent. Prompt and response bodies are never stored.
