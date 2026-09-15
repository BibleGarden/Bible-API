# ADR 0025: Explicit Gemini question service tier and Priority confirmation

Status: accepted (2026-09-15).
Ticket: ClickUp 86cbhdy48.

## Context

Google's `generateContent` API accepts a service tier. Priority requests may
be downgraded by Google to Standard processing, so sending `priority` alone
cannot establish which service handled the answer.

## Decision

When AI is enabled and the question provider is `gemini`,
`AI_QUESTION_SERVICE_TIER` is required and accepts exactly `standard` or
`priority`. Missing, blank or other values fail startup. Other question
providers and disabled AI reject the variable as unused. Existing Gemini
question deployments must declare a tier before upgrading.

The Gemini question request sends the value in the top-level `serviceTier`
field. It does not affect thinking levels, prompts, output budget, timeouts,
other providers or other AI stages.

For a requested Priority response, the documented `x-gemini-service-tier`
header must equal `priority`. If `usageMetadata.serviceTier` is present, it
must agree with the header. A downgrade, missing/invalid acknowledgment or
conflict raises the existing question error (`502`) before text extraction.
There is no header-to-body fallback, tier fallback or additional provider call.
Standard requests retain normal answer handling even without tier metadata.

The startup banner names the requested tier. Response logs report only the
requested tier and the actual header's allowed enum, or `missing`/`invalid`.
No other response headers or unrecognized raw header values are logged.
Runtime activation belongs to Deploy; trial observations belong to AI-Evaluation.

## References

- [GenerateContent](https://ai.google.dev/api/generate-content) documents
  top-level `serviceTier` and `usageMetadata.serviceTier`.
- [Priority inference](https://ai.google.dev/gemini-api/docs/priority-inference)
  documents automatic downgrades and the `x-gemini-service-tier` response header.
