# ADR 0024: Explicit model-specific thinking levels for Gemini questions

Status: accepted (2026-09-15).
Ticket: ClickUp 86cbhbw4u.

## Context

The question stage already supports Google's `generateContent` API. Gemini
models support different thinking levels. The operator must state the chosen
level rather than silently inherit a model default.

## Decision

For `AI_QUESTION_PROVIDER=gemini`, the following reviewed models require an
explicit `AI_QUESTION_REASONING_EFFORT`:

| Model | Accepted levels |
|---|---|
| `gemini-3.8-flash` | `low`, `medium`, `high` |
| `gemini-3.5-flash-lite` | `minimal`, `low`, `medium`, `high` |

Startup rejects missing, blank, padded or unsupported values, including
`none` and `omit`. A small model-to-level table defines the accepted pairs;
`minimal` remains invalid for Gemini 3.8 Flash and generic `openai_compat`.
The Gemini request maps the validated declaration to
`generationConfig.thinkingConfig.thinkingLevel` using Google's uppercase enum
`MINIMAL`, `LOW`, `MEDIUM` or `HIGH`. `minimal` is the lowest supported level
for Flash Lite, not a guaranteed thinking-off mode.

Other Gemini models and stages continue to reject the reasoning variable.
The startup banner reports the effective `thinking_level`. No `thinkingBudget`,
`includeThoughts`, `reasoning_effort`, `reasoning` or OpenRouter routing fields
are sent to Google. Prompts, response parsing, output budget, timeouts,
retries and all other providers remain unchanged.

Google credentials use the existing shell-only `AI_QUESTION_API_KEY`.
Question `ENDPOINT` and `AI_QUESTION_OPENROUTER_PROVIDER_ENDPOINT` must be
removed when selecting Gemini. Existing question deployments on either
reviewed model must declare an accepted level before upgrading. Runtime
activation belongs to the private Deploy repository; performance observations
belong to AI-Evaluation.

## References

- Google's [thinking guide](https://ai.google.dev/gemini-api/docs/thinking)
  lists the supported thinking levels by model.
- The [generateContent API](https://ai.google.dev/api/generate-content#ThinkingConfig)
  defines `thinkingConfig.thinkingLevel` and its uppercase enum values.
  This integration keeps the existing `generateContent` transport.
