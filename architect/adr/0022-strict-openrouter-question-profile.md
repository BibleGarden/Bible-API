# ADR 0022: Strict OpenRouter profile for the local question stage

Status: accepted (2026-09-14); endpoint selection superseded on 2026-09-18 by
[ADR 0026](0026-openrouter-crusoe-question-route.md).
Ticket: ClickUp 86cbh9vh6.

## Context

Maria rejected the wording produced by the local Cerebras question trial and
asked to test Gemma through OpenRouter. OpenRouter speaks Chat Completions, but
it is not equivalent to the generic `openai_compat` transport of ADR 0009:
the request must also make routing, data collection and reasoning behaviour
explicit.

The live OpenRouter catalog on 2026-09-14 names
`google/gemma-4-31b-it`, supports `response_format` and `reasoning`, and says
reasoning is not mandatory and is disabled by default. A provider default is
still not a configuration contract under ADR 0008/0020. Likewise, OpenRouter's
default provider routing may fall back, and data-collection policy must not be
left implicit for prayer-derived content.

The same live endpoint catalog names the exact Venice BF16 endpoint as
`venice/bf16`. OpenRouter's current provider-routing documentation explicitly
allows a full endpoint slug in `provider.only`; using only the base `venice`
slug would also match any other Venice variants and would not preserve the
model instance Maria selected.

## Decision

`AI_QUESTION_PROVIDER=openrouter` selects a distinct, question-only request
profile. It is never inferred from `AI_QUESTION_ENDPOINT` or any hostname.
Scripture rewrite, scripture rerank, transcription and embeddings do not gain
this provider.

The profile requires the following exact configuration:

```dotenv
AI_QUESTION_PROVIDER=openrouter
AI_QUESTION_MODEL=google/gemma-4-31b-it
AI_QUESTION_ENDPOINT=https://openrouter.ai/api/v1
AI_QUESTION_REASONING_EFFORT=none
AI_QUESTION_OPENROUTER_PROVIDER_ENDPOINT=venice/bf16
```

`AI_QUESTION_API_KEY` must be present and non-empty. The model, API endpoint,
provider endpoint and reasoning values above have no aliases; a missing or
different value aborts startup through the aggregated `ConfigError`. The
provider endpoint is an ordinary explicit string, not arbitrary JSON. The
existing operational defaults remain `AI_QUESTION_MAX_TOKENS=4096` and
`AI_QUESTION_TIMEOUT_SECONDS=20`.

The OpenAI-compatible request keeps the existing model, messages,
temperature, max-tokens and JSON response-format fields and always adds:

```json
{
  "provider": {
    "allow_fallbacks": false,
    "data_collection": "deny",
    "only": ["venice/bf16"]
  },
  "reasoning": {
    "enabled": false
  }
}
```

It never sends the flat OpenAI `reasoning_effort` field. These objects are
fixed in reviewed code: there is no environment variable containing arbitrary
JSON and no operator override that can weaken the profile. The existing
`openai_compat` request shape is unchanged, including its explicit flat
reasoning-effort contract.

The startup banner identifies `provider=openrouter`, the endpoint host, model,
`provider_endpoint=venice/bf16`, `reasoning=disabled`, `allow_fallbacks=false` and
`data_collection=deny`, without logging the key. The existing opt-in provider
body diagnostic continues to log the exact JSON request without headers or
credentials.

Only the ignored local `.env` moves to this profile. Rewrite, rerank,
transcription and embeddings retain their current independent configuration.
This local product test does not change production provider policy or
topology.

## Consequences

- An OpenRouter endpoint written under `provider=openai_compat` does not
  silently receive the OpenRouter privacy/routing fields. The explicit
  provider value is required.
- The local question request cannot route to a fallback provider and asks
  OpenRouter to use only the exact `venice/bf16` endpoint and deny providers
  that may store training data.
- Reasoning is explicitly disabled on the wire even though the selected model
  currently defaults to disabled reasoning.
- Changing the OpenRouter model, endpoint or policy requires reviewed code and
  a new architectural decision, not only an environment edit.

## References

- [ADR 0008: fail-fast configuration](0008-fail-fast-configuration.md)
- [ADR 0009: provider-independent LLM client](0009-provider-independent-llm-client.md)
- [ADR 0019: explicit AI configuration](0019-explicit-ai-configuration.md)
- [ADR 0020: explicit chat reasoning effort](0020-explicit-chat-reasoning-effort.md)
