# ADR 0024: Explicit thinking-off for Gemini 2.5 Flash questions

Status: accepted (2026-09-15).
Ticket: ClickUp 86cbhbw4u.

## Context

The question stage already supports Google's `generateContent` API. Gemini
2.5 Flash needs an explicit thinking budget to disable reasoning; omitting it
would leave this behavior to the model's default. The question provider switch
must preserve the operator's explicit reasoning-off choice.

## Decision

Only the question combination `AI_QUESTION_PROVIDER=gemini` and
`AI_QUESTION_MODEL=gemini-2.5-flash` requires
`AI_QUESTION_REASONING_EFFORT=none`. Startup rejects a missing, blank or other
value. The existing stage configuration carries the validated declaration and
the Gemini question request maps it to
`generationConfig.thinkingConfig.thinkingBudget=0`.

Other Gemini models and stages continue to reject the reasoning variable.
The startup banner reports `reasoning=disabled thinking_budget=0` for this
profile. No `includeThoughts`, `reasoning_effort`, `reasoning` or OpenRouter
routing fields are sent to Google. Prompts, response parsing, output budget,
timeouts, retries and all other providers remain unchanged.

Google credentials use the existing shell-only `AI_QUESTION_API_KEY`.
Question `ENDPOINT` and `AI_QUESTION_OPENROUTER_PROVIDER_ENDPOINT` must be
removed when selecting Gemini. Runtime activation belongs to the private
Deploy repository; performance observations belong to AI-Evaluation.

## Reference

Google's [generateContent API](https://ai.google.dev/api/generate-content#ThinkingConfig)
defines `thinkingConfig.thinkingBudget`; zero disables thinking for Gemini
2.5 Flash. This integration keeps the existing `generateContent` transport.
