# ADR 0021: Question language routing is strict outside debug

Status: accepted (2026-09-14).
Ticket: ClickUp 86cbehkmg.
Amends: ADR 0018 question-prompt routing only.

## Context

`POST /api/ai/question` has complete system prompts and stage messages for
Russian, Ukrainian and English. ADR 0018 previously sent an undetermined or
unsupported language to a universal prompt and asked the model to infer the
answer language. That makes language correctness depend on the model after the
offline detector has explicitly abstained or identified a language for which
the application has no reviewed prompt.

Local development still needs a deliberate way to exercise the remainder of
the question flow with such input. The owner authorised English for that case
only when `.env` explicitly sets `DEBUG=true`; the default is false.

## Decision

The request walks the existing language-source chain once. The first detected
code, including an unsupported code, is the result; an abstention continues to
the next candidate. The selected source and code travel together through the
request.

Codes `ru`, `uk` and `en` build both the complete system prompt and the
localized `first`, `next` or `reflect` message. Any other code or a final
`None` returns HTTP 422 before a provider call. With `DEBUG=true`, and only
then, those two expected outcomes select `en` for both prompt parts.

`DEBUG` is an optional operational boolean with the reviewed default `false`.
Only the exact strings `true` and `false` are valid; a malformed value aborts
startup through the existing aggregated configuration error.

Exceptions raised by the language detector are not routing outcomes. They
propagate as internal failures and never activate the English debug route.
Prompt builders perform strict dictionary lookup as a second boundary, so a
caller cannot bypass the routing policy and receive an implicit prompt.

## Consequences

- Production never asks a model to guess an unsupported or undetermined
  response language.
- Local English routing is visible in `.env` and cannot be enabled by a typo.
- A new language requires complete system and stage prompt entries plus the
  3×3-style golden and provider-parity coverage described in
  `architect/adding-a-language.md`.
- Prompt version 6 does not change: this decision changes routing, while the
  ru/uk/en prompt bytes remain unchanged.
