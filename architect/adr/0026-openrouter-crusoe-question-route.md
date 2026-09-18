# ADR 0026: Route OpenRouter Gemma questions to Crusoe BF16

Status: accepted (2026-09-18).
Approval: Maria explicitly approved `crusoe/bf16` on 2026-09-18.
Supersedes: the endpoint selection in ADR 0022.

## Context

On 2026-09-18, the local question API returned 502 after OpenRouter returned
404 for the strict `venice/bf16` route. The live model endpoint catalog at
`GET https://openrouter.ai/api/v1/models/google/gemma-4-31b-it/endpoints`
no longer listed that route. It listed `crusoe/bf16` with BF16 quantization
and support for reasoning and JSON response formatting; Venice listed FP4.

## Decision

Require `AI_QUESTION_OPENROUTER_PROVIDER_ENDPOINT=crusoe/bf16` for the strict
OpenRouter question profile, in both startup validation and transport guards.
Keep `google/gemma-4-31b-it`, disabled reasoning, `allow_fallbacks=false`
and `data_collection=deny`. Reject the retired Venice route explicitly.

The change selects another serving endpoint for the same Gemma model and
precision. It does not change prompts or the retrieval pipeline. Other
provider profiles, including the production Gemini profile, are unchanged.

## Operations

Update the local environment together with this code and recreate only
`bible-api`, preserving all independent stage credentials. Keep provider-body
logging disabled. Verify health and one synthetic ordinary question request;
record the runtime result in `Deploy/local-development.md`.
