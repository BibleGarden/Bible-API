# ADR 0026: Answered questions missing from the history travel in `shown_questions`

Status: accepted (2026-09-29).
Ticket: ClickUp 123pfqn0t4h.
Follows ADR 0015 (`skipped_questions`) and ADR 0016 (the novelty check).

## Context

The novelty check (ADR 0016) compares a generated question with everything
the person has been shown in this prayer, and until now the server could see
only the `assistant` turns of `messages` and `skipped_questions`. Two client
cases leave an answered question out of both:

1. A voice-only answer whose transcription failed. The empty answer is
   omitted from `messages`, and the question was answered, so it is not a
   skipped one.
2. The person withheld consent to send their answers. `messages` is then
   empty, and every answered question is invisible.

The server could then return an already-shown question with `novel: true`.

Neither existing field fits. `skipped_questions` is rendered to the model as
"questions the person asked to replace", which is false here, and its length
rotates the clarification angle. `messages` must end with a non-empty `user`
turn and is a record of what the person said.

## Decision

`POST /api/ai/question` takes an optional `shown_questions: [string]`:
questions of the current prayer already shown to the person that are in
neither `messages` nor `skipped_questions`, chronological.

- **The same shape as `skipped_questions`**: at most 10 entries (the client
  sends the newest), at most 300 characters each after stripping, blank
  entries dropped, a non-empty list with `first` is a `422`, and it counts
  towards the same 16 000-character total.
- **It feeds the novelty check and the used subjects only.** Both read
  `CompleteRequest.seen_questions`: the `assistant` turns, then
  `shown_questions`, then `skipped_questions`. The request does not carry the
  relative order of the three lists, and neither reader needs it:
  `is_repeat` names the closest match wherever it is, and
  `build_user_message` deduplicates the subjects.
- **No prompt block of its own**, and the prompt templates are unchanged. The
  subjects of these questions already reach the model through the
  used-subjects block at `next`; the novelty retry still hands the model
  `skipped_questions` plus the rejected text.
- **It is never read as the person's words.** Like `skipped_questions`, it
  votes on neither the language (it does not even stand in for the last
  `assistant` turn when the person wrote nothing), nor the gender, nor either
  tier of the despair rule.

## Privacy

The field carries only questions this service generated, never the person's
words. It is therefore allowed without answer-context consent, as
`skipped_questions` is: the server learns only that these questions were
shown, which it already knew when it returned them.

## Compatibility and deploy order

Additive. A request without the field, or with an empty or all-blank list, is
answered byte for byte as before. An older server rejects the unknown field
with a `422` (`extra="forbid"`), so the API is deployed first and the client
starts sending the field afterwards.
