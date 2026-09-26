# ADR 0019: Explicit AI configuration without shared credentials

Status: accepted (2026-09-13).
Ticket: ClickUp 86cbh1ap9.

> Later change (2026-09-26, ClickUp 123pfqmzumj): the shared HMAC key was
> renamed to `CLIENT_HMAC_KEY` and became mandatory even with AI disabled,
> because every API request-log row now uses it. The old key name is rejected.
> The configuration rule below describes the original AI-only scope.

> Note (2026-09-14, ClickUp 86cbh9vh6): ADR 0022 adds the question-only
> `openrouter` provider. Unlike `openai_compat`, its key must be non-empty and
> its model, endpoint and reasoning declaration are pinned and fail-fast.

Supersedes the configuration-resolution parts of ADR 0008, ADR 0009,
ADR 0012 and ADR 0014. Their provider transports, models, prompts, retry
policies, retrieval pipeline and public API contracts are unchanged.

## Context

The previous contract inferred whether AI was enabled from a Gemini key or a
provider name and let every OpenAI-compatible stage inherit one shared
endpoint/key pair. Gemini stages inherited `GEMINI_API_KEY`. An old or partial
environment could therefore configure a stage without saying where its model
ran or which credential it used. A blank key passed by Compose was also
indistinguishable from a key deliberately exported empty for a no-auth server.

## Decision

`AI_ENABLED` is required and accepts exactly `true` or `false`.

With `false`, question and transcription return their existing unavailable
response and scripture selection follows its existing safe degradation path.
Stage configuration and `AI_CLIENT_HMAC_KEY` must be absent because nothing
would read them. Embeddings remain independently required: scripture retrieval
still has to name and query the stored vector index.

With `true`, question, rewrite, rerank and transcription each require their own
`AI_<STAGE>_PROVIDER`, `AI_<STAGE>_MODEL` and `AI_<STAGE>_API_KEY`. Gemini
requires a non-empty stage key. For `openai_compat`, the key must be present
but may be empty to explicitly mean no Authorization header; that provider
additionally requires the stage's own
`AI_<STAGE>_ENDPOINT`; Gemini must not have an endpoint. Only transcription
may use `local`, which requires `AI_TRANSCRIBE_MODEL_PATH` and forbids its
endpoint/key. `AI_CLIENT_HMAC_KEY` is required with enabled AI so the
per-client limiter cannot start already degraded.

Embeddings keep their separate explicit block. `openai_compat` requires
`EMBEDDING_ENDPOINT` and the presence of `EMBEDDING_API_KEY`, which may be
empty; Gemini requires a non-empty key and no endpoint; `local` requires
`EMBEDDING_MODEL_PATH` and forbids endpoint/key.

There are no aliases or inheritance. `GEMINI_API_KEY`,
`AI_OPENAI_COMPAT_ENDPOINT` and `AI_OPENAI_COMPAT_API_KEY` are removed and
their presence, including a blank value, aborts startup. Provider-specific
unused variables are rejected as well.

Compose passes all five provider-key variables from the invoking shell. Its
private sentinel preserves unset versus explicitly empty: startup rejects an
unset key, accepts an explicitly empty key only for `openai_compat`, and does
not force four irrelevant exports when `AI_ENABLED=false`.

## Consequences

- The effective provider, model, endpoint and credential source of every stage
  are visible in one environment; changing one stage cannot silently affect
  another.
- Deployments must migrate atomically and remove the three legacy variables.
- Disabling chat/audio is one explicit value and does not disable embeddings.
- The AI-enabled startup contract is stricter because a missing HMAC key now
  fails at deploy time instead of making the first request fail with 503.
- Secrets remain outside `.env`; explicitly unauthenticated endpoints remain
  supported without weakening missing-key validation.
