"""Is this question one the person has already been shown?

ClickUp 86cbehyg0 (child of 86cbehxm2, bug 86cbehtkh). `skipped_questions`
(ADR 0015) tells the model which questions were declined; it does not stop it
from offering them again, and the measured series show it doing exactly that:
pressing "replace question" six times on `series-scale-ru` produced six
variants of one sentence, the last five differing from the first only in the
tail («— как ты будешь узнавать это?»). This module is the filter that catches
that before the person sees it — the handler asks for one more generation and
reports the outcome in the `novel` field.

**Lexical closeness only, and that is not a check of semantic diversity.** Two
questions that share no wording can still be the same thought: in the same
series, question 2 («…то, что ты считаешь готовым, на самом деле ещё не то,
что нужно…») and the loop that followed it are one idea in several dresses,
and a *differently worded* return to that idea would pass this filter
untouched. Whether an embedding model (bge-m3) can measure the thought rather
than the letters is ClickUp 86cbehyg8; this module deliberately does not
pretend to.

**No environment variables** (ADR 0008): the threshold and the prefix rule
below are chosen on the examples in the table, and a deployment that could
quietly move them would make two installations answer the same prayer
differently with identical, valid-looking configuration.

Dependency-free like `question_prompt`: the standard library only, so the
evaluation tools and the tests can import it without the application.

## The metric

`normalize` and `similarity` are the definition `evaluation/check_questions.py`
already uses for the replacement series (`normalise_series_text`,
`trigram_similarity`): casefold, ё→е, drop everything that is neither a word
character nor whitespace (quotes, dashes, «», …, ?, !), collapse whitespace;
then Jaccard over character 3-grams. Character trigrams rather than word sets
because a reworded loop keeps the letters and moves the words. Reusing that
definition is the point — the metric the benchmark reports and the filter that
runs in production must agree, or a number in `evaluation/README.md` says
nothing about what the endpoint does.

Ukrainian is safe under it: `і`, `ї`, `є`, `ґ` are word characters and only `ё`
is folded (it is Russian; the Ukrainian `є` is a different letter and is left
alone).

## Two ways to be a repeat

1. **Exact** — equal after `normalize`. Case, spacing, punctuation and the two
   spellings of `ё` are not a new question.
2. **Near** — `similarity >= NEAR_REPEAT_THRESHOLD` (0.60), **or** a shared
   opening of at least `MIN_PREFIX_WORDS` (4) normalized words that also
   covers at least `PREFIX_SHARE` (0.7) of the shorter question. The second
   branch catches the shape the ticket named — the same sentence with a new
   tail — when the tail is long enough to pull the trigram score down. It is
   not decoration: the stem of `series-scale-ru` step 6 with a fresh
   fifteen-word tail scores 0.488 and 0.450, i.e. below the threshold, and is
   caught by the opening alone.

The prefix branch is **not** the plain "the first four words are equal" rule
the ticket proposed, and the series say why: Qwen opens every Ukrainian
question with «А що б ти зробив, якби …» (six words) and most English ones
with «What would it feel like to say yes, knowing …» (nine), so bare prefix
equality flags nearly every pair of `series-exhaustion-uk` — including the
ones offering plainly different things (a half-hour break / not working this
weekend / permission to stop). Requiring the shared opening to be most of the
shorter question keeps a fixed frame from deciding on its own: those questions
are 10-14 words, so the frame reaches 0.6 of them and stops there, while a
repeated stem reaches 1.0.

## Where the threshold comes from

Measured on `evaluation/bench_data/questions_qwen30b_v3_series.jsonl`
(Qwen3-30B, prompt v3, four inputs x six replacement series). "flagged" is
this module's verdict at the constants above; the pair sets are reproduced in
`tests/test_question_novelty.py`.

| set | pairs | min | median | max | flagged |
| --- | --- | --- | --- | --- | --- |
| positives — `series-scale-ru` sample 1, all pairs | 15 | 0.610 | 0.721 | 0.910 | 15/15 |
| positives — byte-identical pairs in the other three series | 6 | 1.000 | 1.000 | 1.000 | 6/6 |
| positives — pairs of the other three series that are repeats by eye | 12 | 0.505 | 0.611 | 0.928 | 12/12 |
| negatives — the other three series, all remaining pairs | 162 | 0.042 | 0.222 | 0.500 | 0/162 |
| negatives — hand-written different questions on one topic, ru/uk/en | 5 | 0.025 | 0.104 | 0.345 | 0/5 |

The two distributions separate cleanly — 0.500 against 0.610 — but only once
the third row is read as positives rather than negatives. Those twelve pairs
live in the series the ticket offered as negative material ("their answers
differ"), and at the top of that material the answers do not differ: 0.928 is
«хоч на годину» against «хоч на годинку», 0.724 the same gratitude question
with another tail, 0.659 the same English question with another ending. They
are counted here as what they are. Three of the twelve (0.505, 0.557, 0.576)
are below the threshold and are caught by the opening rule instead — all three
are «А що б ти зробив, якби міг відпочити хоч на годинку …» with the tail
swapped, which is the branch doing exactly its job on real data rather than on
a constructed example.

The failure a mistuned threshold can produce is one wasted generation, never a
wrong answer: a flagged question is regenerated once and, if the second is no
better, the better of the two is returned anyway with `novel: false`.

Deliberately NOT tuned to three decimal places. 0.60 is a round number the
data supports; these are reviewed constants, and moving one is a code change
with this table redone under it.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

# --- the metric, byte for byte `evaluation/check_questions.py` -------------
_PUNCTUATION = re.compile(r"[^\w\s]", re.UNICODE)
_TRIGRAM_SIZE = 3

# --- the two rules ---------------------------------------------------------
# Jaccard over character 3-grams above which two questions are the same
# question. See the table in the docstring.
NEAR_REPEAT_THRESHOLD = 0.60
# A shared opening this long (in normalized words) and covering this much of
# the shorter question is a repeat whatever the tails do. The share is what
# separates a repeated sentence from a model's favourite opening frame — see
# the docstring; four words alone flags «А що б ти зробив, якби …» against
# itself forever.
MIN_PREFIX_WORDS = 4
PREFIX_SHARE = 0.7

KIND_NONE = "none"
KIND_EXACT = "exact"
KIND_NEAR = "near"


def normalize(text: str) -> str:
    """casefold, ё→е, no punctuation/quotes/dashes, single spaces.

    `і`, `ї`, `є`, `ґ` are word characters and survive: only `ё` is folded,
    and only because Russian spells the same word both ways.
    """
    lowered = text.casefold().replace("ё", "е")
    # Dashes, quotes, «», …, ?, ! are all non-word characters, so one
    # substitution covers them; replacing with a space keeps a hyphenated
    # word from fusing into one token.
    return " ".join(_PUNCTUATION.sub(" ", lowered).split())


def _trigrams(normalized: str) -> set[str]:
    if len(normalized) < _TRIGRAM_SIZE:
        return {normalized} if normalized else set()
    return {
        normalized[index : index + _TRIGRAM_SIZE]
        for index in range(len(normalized) - _TRIGRAM_SIZE + 1)
    }


def similarity(left: str, right: str) -> float:
    """Jaccard over character 3-grams: 1.0 identical, 0.0 nothing shared."""
    a, b = _trigrams(normalize(left)), _trigrams(normalize(right))
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _shares_an_opening(left: str, right: str) -> bool:
    """Do these two open with the same words, for enough of their length."""
    a, b = normalize(left).split(), normalize(right).split()
    if not a or not b:
        return False
    shared = 0
    for word_a, word_b in zip(a, b):
        if word_a != word_b:
            break
        shared += 1
    return shared >= MIN_PREFIX_WORDS and shared >= PREFIX_SHARE * min(
        len(a), len(b)
    )


@dataclass(frozen=True)
class Verdict:
    """What the filter saw, and which shown question it saw it against.

    `matched` is a question we generated earlier, not the person's words —
    but it is still request content, so **only `kind`, `score` and `index`
    may be logged**. Nothing in this service writes prayer text to a log.
    """

    kind: str = KIND_NONE
    score: float = 0.0
    index: int | None = None
    matched: str | None = None

    @property
    def repeat(self) -> bool:
        return self.kind != KIND_NONE


NOT_A_REPEAT = Verdict()


def is_repeat(candidate: str, shown: Sequence[str]) -> Verdict:
    """Has `candidate` already been shown, exactly or in another wording?

    `shown` is everything the person has seen in this prayer: the `assistant`
    turns of the request plus its `skipped_questions`. Order does not matter;
    the verdict names the closest match. An empty candidate or an empty
    `shown` is `NOT_A_REPEAT` — there is nothing to compare, which is not the
    same as having compared and found nothing.
    """
    normalized = normalize(candidate)
    if not normalized:
        return NOT_A_REPEAT

    best_score = 0.0
    best_index: int | None = None
    prefix_index: int | None = None
    prefix_score = 0.0
    for index, question in enumerate(shown):
        other = normalize(question)
        if not other:
            continue
        if other == normalized:
            return Verdict(KIND_EXACT, 1.0, index, question)
        score = similarity(candidate, question)
        if best_index is None or score > best_score:
            best_score, best_index = score, index
        if _shares_an_opening(candidate, question) and (
            prefix_index is None or score > prefix_score
        ):
            prefix_score, prefix_index = score, index

    if best_index is not None and best_score >= NEAR_REPEAT_THRESHOLD:
        return Verdict(KIND_NEAR, best_score, best_index, shown[best_index])
    if prefix_index is not None:
        return Verdict(KIND_NEAR, prefix_score, prefix_index, shown[prefix_index])
    if best_index is None:
        return NOT_A_REPEAT
    # Not a repeat — but the caller compares the two candidates of a retry by
    # how close each came, so the score is reported either way.
    return Verdict(KIND_NONE, best_score, best_index, shown[best_index])
