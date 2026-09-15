# ADR 0024: Explicit thinking level for Gemini 3.8 Flash questions

Status: accepted (2026-09-15).
Ticket: ClickUp 86cbhbw4u.

## Context

The question stage already supports Google's `generateContent` API. Gemini
3.8 Flash supports the thinking levels low, medium and high; it does not
support disabling thinking. The operator must state the chosen level rather
than silently inherit the model's default.

## Decision

Only the question combination `AI_QUESTION_PROVIDER=gemini` and
`AI_QUESTION_MODEL=gemini-3.8-flash` requires an explicit
`AI_QUESTION_REASONING_EFFORT=low|medium|high`. Startup rejects a missing,
blank, padded or other value, including `none` and `omit`. The existing stage
configuration carries the validated declaration; the Gemini question request
maps it to `generationConfig.thinkingConfig.thinkingLevel` using Google's
uppercase enum `LOW`, `MEDIUM` or `HIGH`.

Other Gemini models and stages continue to reject the reasoning variable.
The startup banner reports the effective `thinking_level`. No `thinkingBudget`,
`includeThoughts`, `reasoning_effort`, `reasoning` or OpenRouter routing fields
are sent to Google. Prompts, response parsing, output budget, timeouts,
retries and all other providers remain unchanged.

Google credentials use the existing shell-only `AI_QUESTION_API_KEY`.
Question `ENDPOINT` and `AI_QUESTION_OPENROUTER_PROVIDER_ENDPOINT` must be
removed when selecting Gemini. Runtime activation belongs to the private
Deploy repository; performance observations belong to AI-Evaluation.

## References

- Google's [thinking guide](https://ai.google.dev/gemini-api/docs/thinking)
  lists the supported thinking levels for Gemini 3.8 Flash.
- The [generateContent API](https://ai.google.dev/api/generate-content#ThinkingConfig)
  defines `thinkingConfig.thinkingLevel` and its uppercase enum values.
  This integration keeps the existing `generateContent` transport.
