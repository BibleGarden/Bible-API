# ADR 0023: Direct Together profile for the local question stage

Status: accepted (2026-09-15).
Ticket: ClickUp 86cbhb9h2.

## Context

Maria requested the existing local Gemma question trial through Together
and explicit configuration of disabled reasoning. The generic
`openai_compat` profile sends flat `reasoning_effort`; `omit` leaves reasoning
to the provider. Together requires `reasoning: {enabled: false}` to disable
it for the selected Gemma model. OpenRouter already has its own profile
with additional routing and privacy fields (ADR 0022).

## Decision

Add `AI_QUESTION_PROVIDER=together`, selected by provider value, never by URL.
The question-only profile requires the exact explicit configuration:

```dotenv
AI_QUESTION_PROVIDER=together
AI_QUESTION_MODEL=google/gemma-4-31B-it
AI_QUESTION_ENDPOINT=https://api.together.ai/v1
AI_QUESTION_REASONING_EFFORT=none
```

The stage's `AI_QUESTION_API_KEY` must be present and non-empty. A local
operator exports it from `TOGETHER_API_KEY`; credentials are never stored
in the application's `.env`. Missing or unsupported configuration aborts
startup. `AI_QUESTION_OPENROUTER_PROVIDER_ENDPOINT` must be absent.

The Chat Completions request adds `reasoning: {enabled: false}` and sends
neither flat `reasoning_effort` nor OpenRouter's `provider` object. No JSON
override or alternate reasoning mode is introduced. OpenRouter and generic
`openai_compat` retain their existing contracts.

Prompts, temperature, response parsing and `response_format: {type:
"json_object"}` without a schema remain unchanged. Operational defaults
remain `AI_QUESTION_TIMEOUT_SECONDS=20` and `AI_QUESTION_MAX_TOKENS=4096`.
The startup banner names Together, model, endpoint host and
`reasoning=disabled`, without credentials.

Rewrite, rerank, transcription and embeddings reject this provider. This
local trial does not change production provider policy or deployment.

## Consequences

- Reasoning is explicitly disabled rather than delegated to a model default.
- Switching profiles cannot accidentally send OpenRouter routing fields to
  Together; leftover OpenRouter configuration prevents startup.
- Any other model, endpoint or reasoning mode requires a reviewed change to
  this pinned profile and a new decision.
- Timing must be measured through the application; transport support alone
  makes no latency guarantee.
