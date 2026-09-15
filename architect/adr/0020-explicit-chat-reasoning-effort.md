# ADR 0020: Explicit reasoning effort for OpenAI-compatible chat

Status: accepted (2026-09-13).
Ticket: ClickUp 86cbh2v21.
Evaluation: ClickUp 86cbh1apk.

## Context

OpenAI-compatible chat providers do not share one safe implicit reasoning
mode. Cerebras can select a model default when `reasoning_effort` is absent,
while other endpoints or models may not support the field at all. Leaving it
unset would therefore hide either a behaviour choice or a compatibility
choice from the stage configuration, contrary to ADR 0008 and ADR 0019.

Maria chose a local trial on 2026-09-13: question, scripture rewrite and
scripture rerank use Cerebras at `https://api.cerebras.ai/v1`, model
`qwen-3.8-27b`, with `reasoning_effort=none`. The evaluation and its
limitations are recorded in ClickUp 86cbh1apk. The trial is an explicit
operational choice, not a change to the production model policy or topology.

## Decision

Each OpenAI-compatible chat stage requires its own variable:

- `AI_QUESTION_REASONING_EFFORT`;
- `AI_SCRIPTURE_REWRITE_REASONING_EFFORT`;
- `AI_SCRIPTURE_RERANK_REASONING_EFFORT`.

The only values are `omit`, `none`, `low`, `medium` and `high`. The latter four
are sent unchanged as the `reasoning_effort` field of the stage's
`/chat/completions` request. `omit` explicitly does not send the field for an
endpoint or model that lacks support. Missing configuration is not equivalent
to `omit` and aborts startup, so a provider default can never be selected by
silence.

Gemini chat stages reject this variable, except the reviewed question models
of [ADR 0024](0024-gemini-question-thinking-level.md), which map explicit
model-specific levels to the Google-specific thinking level. There is no shared reasoning variable or
cross-stage fallback. Transcription and embeddings have no reasoning setting.
The startup banner prints only the validated mode, never a credential.

The provider switch changes transport configuration only. Prompts, parsers,
retrieval stages, fallbacks, timeouts and public API contracts are unchanged.
The Cerebras key remains shell-only and is supplied independently to the three
existing stage-specific key variables.

### Amendment (2026-09-14)

Maria's manual product check found question quality unacceptable with `none`
and good with `low`. The current local question stage therefore keeps
Cerebras `qwen-3.8-27b` with `reasoning_effort=low`. Rewrite remains separately
configured as Cerebras `qwen-3.8-27b` with `none`; rerank remains the independent
`qwen3-30b-a3b-instruct-2507` stage with `omit` and its own endpoint/key.

Diagnostic responses also showed that hidden reasoning can consume the former
1024-token allowance before any answer content is produced. Question now has
the separate operational safety ceiling `AI_QUESTION_MAX_TOKENS`, default
`4096`, applied by both transports. This changes neither prompt nor retry
policy, and does not authorize a production change. The cross-repository
decision is recorded in
[Architecture ADR-0004](https://github.com/BibleGarden/Architecture/blob/main/decisions/0004-explicit-openai-reasoning-effort.md#amendment-note--2026-09-14).

### Amendment 2 (2026-09-14)

Maria ended the Cerebras question trial and moved only the local question
stage to the strict OpenRouter Gemma profile of ADR 0022. Its environment
still declares `AI_QUESTION_REASONING_EFFORT=none`, but the OpenRouter wire
shape is the provider's official `reasoning: {enabled: false}` object and does
not contain flat `reasoning_effort`. Rewrite and rerank keep the independent
values recorded above.

## Consequences

- Existing OpenAI-compatible chat deployments must choose an explicit value
  for every such stage before restart. `omit` preserves compatibility where
  the endpoint does not implement reasoning effort without creating a default.
- The original local trial used `none` on all three chat stages; the first
  amendment records its later question setting. ADR 0022 is the current local
  question configuration. Production model policy and production topology
  remain unchanged.
- The four ungraded top-1 results from evaluation 86cbh1apk remain ungraded.
  They must be passed to Maria and are not self-labelled as evidence for the
  trial decision.

## References

- [ADR 0008: fail-fast configuration](0008-fail-fast-configuration.md)
- [ADR 0009: provider-independent LLM client](0009-provider-independent-llm-client.md)
- [ADR 0019: explicit AI configuration](0019-explicit-ai-configuration.md)
- [AI model provider policy](https://github.com/BibleGarden/Architecture/blob/main/decisions/0002-ai-model-provider-policy.md)
- [Evaluation task 86cbh1apk](https://app.clickup.com/t/86cbh1apk)
