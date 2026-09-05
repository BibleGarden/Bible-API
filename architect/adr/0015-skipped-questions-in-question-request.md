# ADR 0015: Replaced questions travel in their own request field

Status: accepted (2026-09-05).
Ticket: ClickUp 86cbehyfe, child of 86cbehxm2 (parent bug 86cbehtkh).
Follows ADR 0009 (the provider seam) and the structured request of 86cbegmzz,
which is what made this field expressible at all.

## Context

In Lampada the person can press "replace question". The client dropped the
unanswered question and resent an **identical** body, so the model was told
nothing: the same conversation produced the same thought again, usually
reworded, and the person could press "replace" indefinitely without moving.

The app is unpublished (Maria, 2026-09-05), so the contract and the client
change together and no transitional shape is needed.

## Decision

`POST /api/ai/question` takes an optional `skipped_questions: [string]` —
questions already shown to the person and left unanswered, chronological, sent
in full for the current prayer. It is rendered at `next` as one extra block
plus one extra sentence of the instruction, and read by nothing else.

- **≤ 10 entries, ≤ 300 characters each after stripping**, counted with `topic`
  and `messages` against the same 16 000-character total. Both ceilings bound a
  list of *our own* questions, each of which the prompt keeps to about 160
  characters: a prayer that replaced ten has said everything an eleventh could,
  and an entry twice the length of any question we produce is a client bug.
- **Blank entries are dropped, not refused.** A `messages` turn with empty text
  is a `422` because it is the person's own words and an empty one is a
  contradiction; a blank *skipped question* is a string we generated coming
  back to us, says nothing about the person, and is indistinguishable from the
  field being absent — which is a request the endpoint must answer anyway.
  Refusing it would cost the person their next question over a client bug that
  costs us nothing. The length bound is checked after the strip, so trailing
  whitespace never decides a `422`.
- **`first` with a non-empty list is a `422`**, the same rule as `messages`
  with `first`: nothing has been shown yet, so nothing can have been replaced.
- **`reflect` accepts it and does not render it.** That stage deliberately
  shows none of our questions (86cbegmzz — it looks back at what the *person*
  said), so rendering them there is a prompt-design change, not a property of
  the field. Accepting it lets the client send one unconditional shape and
  makes turning the block on later a server-only edit. It is documented in the
  contract and pinned by a test, so it is a decision rather than a silent
  no-op.
- **`QUESTION_PROMPT_VERSION` stays 3.** A request without the field renders
  byte for byte what v3 always rendered (pinned in
  `tests/test_question_prompt.py`), so the two texts cannot answer the same
  request differently — which is what a version distinguishes. The wording is
  minimal on purpose; revising it is prompt work (ClickUp 86cbehyf8, v4) and
  that is where the version moves.

  **Outcome (2026-09-06, 86cbehyf8).** The version did move — for the `next`
  instruction and a gender sentence, not for this block. Both rewordings of the
  block tried there measured **worse** on the same inputs («Эти вопросы
  человеку не подошли, он их пропустил» left 10 series of 12 with a verbatim
  duplicate pair against 4 of 12 for the wording above; naming «другого
  человека» in the instruction sent the question off to an invented third
  party), so the header and the sentence of this ADR are unchanged and are now
  measured rather than merely provisional. The `tests/test_question_prompt.py`
  guard on what the block may not say is unchanged and still applies to any
  future rewording.

## Why a field and not a fake `user` turn

The cheapest implementation would have been to fold the replaced questions into
`messages` — as `assistant` turns, or as a synthetic `user` turn saying "ask
something else". Both were rejected:

1. **`messages` is a record of what happened, and both safety rules read it.**
   The last `user` turn decides the answer's language and is the only text the
   despair rule looks at (86cbegmzz). A synthetic turn would be *our* text
   voting on the person's language and, worse, standing where the person's last
   word should stand — the exact confusion the structured request was
   introduced to remove.
2. **An `assistant` turn means something else already**: a question that was
   *asked and answered*, listed under «Уже прозвучали вопросы» precisely so the
   model does not repeat it. A replaced question is a different fact and wants a
   different instruction — not "do not repeat this", but "take another
   direction". Overloading one list would have made the two indistinguishable
   to the model and to us.
3. **A separate field is separately bounded and separately revisable.** Its
   limits, its stage rules and its wording move without touching the history,
   and a client that never replaces a question sends exactly what it sent
   before.

## Why it is excluded from language detection and from the despair rule

`skipped_questions` carries **our own generated text**, wrapped in a Russian
block whatever language the prayer is in.

- **Language** (`twinkler_ai.language_source`): the chain walks the person's
  own words only — the last reply, the topic, their earlier replies — because
  a question of ours must never outvote the language they chose. The block
  around the skipped questions is Russian in every prayer, so letting it vote
  would answer an English prayer in Russian for no reason but our own wrapper.
- **Despair** (`twinkler_ai.safety_input_text`): both tiers read the person's
  last reply and nothing else, so that a phrase already answered once does not
  end the rest of the prayer (86cbegmzz, Maria's decision of the same day). A
  despair phrase can only reach `skipped_questions` because a **model** wrote
  it — and that case is already covered by tier 2, which judges the model's
  reply against the person's own last words. Reading it here would let one bad
  generated question answer every later question of that prayer with the fixed
  reply: the loop this project has now closed twice.

Both exclusions are tests, not comments: an English prayer with Russian
skipped questions gets the English prompt, and a despair phrase placed in the
field never fires tier 1.

## What the block may not say

Pressing "replace" says the person wants a different question. It does **not**
say they disagree with the thought behind it — they may not have understood it,
may not be ready for it, or may simply want another angle. The block therefore
states the action and stops: «Человек попросил другой вопрос вместо этих:».
Telling the model the person rejected the idea would have us invent their
position and then answer it, which is the interpretation prompt v2 bans in
plain words. This constraint outlives the current wording and applies to the
v4 rewrite as well; `tests/test_question_prompt.py` guards it.

## Consequences

- The client must accumulate every replaced or skipped question of the current
  prayer and send them all, and must **not** duplicate a question already in
  `messages`: an answered question is an `assistant` turn, a replaced one is a
  skipped question, and the two lists never overlap.
- Out of scope here, deliberately: any repetition filter or re-generation of a
  question the model returns anyway (86cbehyg0), and the prompt tuning proper
  (86cbehyf8).
