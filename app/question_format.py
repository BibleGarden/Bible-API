"""The answer contract of `POST /api/ai/question` since prompt v6.

Prompt v6 (ClickUp 86cbejvt2) stopped asking for a bare line of prose and asks
for one JSON object instead:

    {"subject": "<2-4 words, the subject of reflection>",
     "question": "<one open question>"}

Why a contract change rather than more prompt wording: the assessment of
2026-09-06 (`FABLE_ASSESSMENT.md`) found the model returning the *same subject*
in different words through a whole replacement series, and no rewording of "on
replacement, change the subject of reflection" moved that — v5 tried. Naming
the subject in a field makes the model commit to one before writing the
question, and gives the server something to show and, later, to compare
(§ "Which subjects are taken" of ADR 0017).

**The person is shown `question` and nothing else.** `subject` is additive on
the response (`QuestionResponse.subject`) so a client can use it later; it is
never concatenated into the text.

The parse is a ladder, and every rung is honest about what it did — the caller
logs `question format: parsed=…` so a model that stops obeying the contract is
visible in the logs rather than silently degrading:

1. `json` — `json.loads` of the object found in the answer;
2. `repaired` — the same after `json_repair.repair_json_object`, the bounded
   punctuation repair the rewrite stage has used since 86cbe4nd3 (a closer of
   the wrong type, a truncation at a clean boundary, a trailing comma — never
   invented content). Imported, not copied;
3. `regex` — no usable object, but a `"question"` field is visible in the text;
4. `raw` — nothing parsed. The answer's **first line**, stripped of wrapping
   quotes, is the question and `subject` is `None`. The handler may ask for one
   more generation before accepting this (that is its decision and its budget,
   not this module's).

`SubjectMemory` at the bottom is the other half of the same idea: the request
carries the *texts* of the questions the person has seen and never their
subjects, but this service wrote every one of them, so it can remember what
each was about and list those subjects in the next message. See its own
comment for why that memory is allowed to be lossy.

The module imports nothing from the application except `json_repair` and
`question_novelty`, which import nothing at all — so
`evaluation/gen_questions.py` parses production answers with the production
parser instead of a copy of it.
"""

from __future__ import annotations

import json
import re
import time
from collections import OrderedDict
from dataclasses import dataclass

from json_repair import repair_json_object
from question_novelty import normalize

# The rungs of the ladder above, in the order they are tried. `RAW` is not a
# failure code — it is an answer the person can be shown — but it is the one
# value that says the model ignored the contract.
JSON = "json"
REPAIRED = "repaired"
REGEX = "regex"
RAW = "raw"

QUESTION_FIELD = "question"
SUBJECT_FIELD = "subject"

# At most this many characters of `subject` are kept. The prompt asks for 2-4
# words; this is a bound on a field that reaches a client, not a quality rule,
# so an over-long one is trimmed rather than refused — the question is what the
# person waits for.
MAX_SUBJECT_CHARS = 120

_OBJECT_PATTERN = re.compile(r"\{.*\}", re.DOTALL)
# The third rung: a `question: "..."` pair inside text no repair could turn
# into an object. Deliberately forgiving about everything EXCEPT the value,
# because the five shapes the review of 86cbejvt2 collected are all
# decoration around a perfectly good question:
#
#   {"quesiton": "…"}          a typo in the key      -> `ques\w*`
#   {'question': '…'}          single quotes          -> either quote character
#   **question**: …            markdown, no quotes    -> optional `*`/`"`/`'`
#   "question" : "…"           stray whitespace       -> `\s*`
#   Question: "…"              a capital and no braces-> case-insensitive
#
# JSON string escapes are honoured inside a quoted value, so a question
# carrying an escaped quote survives. An unquoted value runs to the end of its
# line — the format is one line per field wherever this rung is reached.
_QUESTION_PATTERN = re.compile(
    r"""[*"']*\s*ques\w*\s*[*"']*\s*:\s*(?:"((?:[^"\\]|\\.)*)"|'([^'\n]*)'|([^\n,}]+))""",
    re.IGNORECASE,
)
_SUBJECT_PATTERN = re.compile(
    r"""[*"']*\s*subject\s*[*"']*\s*:\s*(?:"((?:[^"\\]|\\.)*)"|'([^'\n]*)'|([^\n,}]+))""",
    re.IGNORECASE,
)
# What a person must never be shown: the envelope instead of the question. If
# the text that survived every rung still looks like this, the answer counts as
# unreadable (so the handler asks once more) and the fallback below salvages
# something brace-free rather than printing machinery.
# A field name followed by a colon, however it is decorated. Deliberately
# narrower than "the word question appears": an English question may perfectly
# well BE about a question ("What question do you want to bring to God?"), and
# mangling that would be a worse bug than the one this guards against.
_KEY_PATTERN = re.compile(
    r"""["'*]*\s*(?:ques\w*|subject)\s*["'*]*\s*:""", re.IGNORECASE
)
_SENTENCE_PATTERN = re.compile(r"[^{}\n]*\?")
# A bare (unquoted) value that is a JSON literal is not a question. `null` is
# the shape the review found: `{"question": null}` parses as an object, has no
# string to take, and its unquoted tail must not become the text on screen.
_EMPTY_LITERALS = frozenset({"null", "none", "true", "false", "undefined", ""})
# Wrapping quotes of a raw answer. The prompt used to end with "return only the
# question, without quotes" and models put them there anyway.
_WRAPPING_QUOTES = "\"'«»“”„`"


@dataclass(frozen=True)
class ParsedQuestion:
    """One model answer, read as the v6 contract."""

    question: str
    subject: str | None
    kind: str

    @property
    def parsed(self) -> bool:
        """Did the model honour the contract at all (any rung but `raw`)?"""
        return self.kind != RAW


def _clean_question(value: str) -> str:
    """One line, no wrapping quotes — what the person is shown.

    Only the FIRST line is kept: an answer that carries a question plus an
    explanation below it is the shape the format section forbids, and showing
    the explanation would be worse than dropping it. Inner newlines cannot
    survive in a one-line question either way.
    """
    first_line = value.strip().splitlines()[0] if value.strip() else ""
    return first_line.strip().strip(_WRAPPING_QUOTES).strip()


def _clean_subject(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    subject = " ".join(value.split()).strip().strip(_WRAPPING_QUOTES).strip()
    if not subject:
        return None
    return subject[:MAX_SUBJECT_CHARS]


def looks_like_machinery(text: str) -> bool:
    """Would showing this text hand the person the envelope, not the question?

    Three signs, all of them observed in the review of 86cbejvt2 rather than
    imagined: a brace anywhere, a leading markdown emphasis (`**question**`),
    or a field name **followed by a colon**. The colon is what makes the last
    one safe: an English question may legitimately be about a question or a
    subject, and only `question:` / `"subject":` / `**question**:` is
    machinery.
    """
    if "{" in text or "}" in text:
        return True
    if text.lstrip().startswith("**"):
        return True
    return _KEY_PATTERN.search(text) is not None


def _salvage(text: str) -> str:
    """The least bad brace-free line when every rung failed.

    Takes the first sentence that ends in a question mark and carries no
    braces — the model almost always wrote one, even when it wrapped it in
    something unreadable — and **empty** when there is none.

    Empty is deliberate and is not a third-best string: an answer holding no
    question at all (`{"question": ""}`, `{"question": null}` — both from the
    review of 86cbejvt2) has nothing to show, and assembling one out of the
    envelope's leftovers would put «тема , null» on a person's screen. The
    handler turns an empty question into the same `502` a provider that
    answered nothing already produces.
    """
    for match in _SENTENCE_PATTERN.finditer(text):
        fragment = match.group(0)
        # A fragment that still carries a field name is a `key: value` pair
        # with the value at the end of it, so the last colon is where the
        # question starts. Only reached for text that already looks like
        # machinery, so a question of ours that legitimately contains a colon
        # is never cut here.
        if looks_like_machinery(fragment) and ":" in fragment:
            fragment = fragment.rsplit(":", 1)[1]
        candidate = _clean_question(fragment)
        candidate = candidate.lstrip("*").strip().strip(_WRAPPING_QUOTES).strip()
        if candidate and not looks_like_machinery(candidate):
            return candidate
    return ""


def _field_value(match: re.Match) -> str:
    """The value of a `field: value` match, whichever quoting it used.

    Group 1 is a double-quoted JSON string (escapes decoded), group 2 a
    single-quoted one, group 3 an unquoted run to the end of the line.
    """
    double, single, bare = match.group(1), match.group(2), match.group(3)
    if double is not None:
        try:
            return json.loads(f'"{double}"')
        except json.JSONDecodeError:
            return double
    if single is not None:
        return single
    value = (bare or "").strip().rstrip(",").strip()
    return "" if value.casefold() in _EMPTY_LITERALS else value


def _from_payload(payload: object, kind: str) -> ParsedQuestion | None:
    if not isinstance(payload, dict):
        return None
    question = payload.get(QUESTION_FIELD)
    if not isinstance(question, str):
        return None
    cleaned = _clean_question(question)
    if not cleaned:
        return None
    return ParsedQuestion(cleaned, _clean_subject(payload.get(SUBJECT_FIELD)), kind)


def parse_question(text: str) -> ParsedQuestion:
    """Read one model answer. Never raises, never returns an empty question.

    An empty or blank answer is a `raw` result with an empty `question`: the
    caller already treats "no text" as a provider failure before this is
    reached (`twinkler_ai.complete` raises on it), so this function has no
    error path of its own to invent.
    """
    stripped = text.strip()
    match = _OBJECT_PATTERN.search(stripped)
    # A truncated answer has an opening brace and no closing one, and closing
    # it is exactly what `repair_json_object` exists for — so the blob is taken
    # from the first brace to the end when the greedy search finds no pair.
    # (`query_rewrite` reports "no JSON object" in that case instead; there the
    # fallback is a documented degradation to the raw query, while here the
    # person is waiting for this very sentence.)
    blob = match.group(0) if match else (
        stripped[stripped.index("{"):] if "{" in stripped else None
    )
    if blob is not None:
        try:
            parsed = _from_payload(json.loads(blob), JSON)
        except json.JSONDecodeError:
            parsed = None
        if parsed is not None:
            return parsed
        repaired = repair_json_object(blob)
        if repaired is not None:
            try:
                parsed = _from_payload(json.loads(repaired), REPAIRED)
            except json.JSONDecodeError:
                parsed = None
            if parsed is not None:
                return parsed

    question_match = _QUESTION_PATTERN.search(stripped)
    if question_match:
        cleaned = _clean_question(_field_value(question_match))
        if cleaned and not looks_like_machinery(cleaned):
            subject_match = _SUBJECT_PATTERN.search(stripped)
            subject = (
                _clean_subject(_field_value(subject_match)) if subject_match else None
            )
            return ParsedQuestion(cleaned, subject, REGEX)

    # Nothing readable. The answer is `raw` — which is what makes the handler
    # ask once more — and whatever is shown if that fails must still be a
    # sentence rather than an envelope.
    question = _clean_question(stripped)
    if looks_like_machinery(question):
        question = _salvage(stripped)
    return ParsedQuestion(question, None, RAW)


# ---------------------------------------------------------------------------
# Which subjects this prayer has already used (ClickUp 86cbejvt2)
# ---------------------------------------------------------------------------
# The request carries the *texts* of the questions the person has seen — the
# `assistant` turns and `skipped_questions` — and never their subjects: the
# client has no reason to send back a field it does not use, and ADR 0015's
# contract is deliberately not being reopened. But this service generated every
# one of those questions, so it is the one party that can remember what each
# was about. That is what this cache is: question text -> subject, written when
# an answer is returned and read when the next message is assembled.
#
# Deliberately small and forgettable. It is a *quality* aid, not state the
# answer depends on: a miss degrades to an excerpt of the question itself,
# which is what v5 would have offered anyway, so a restart, an eviction or an
# expiry costs the prompt some precision and nothing else. Nothing here is
# persisted, nothing is logged, and a prayer text never leaves the process.

# Two hours: longer than any prayer, far shorter than a deployment. A reviewed
# constant rather than an environment knob for the reason ADR 0008 gives —
# which question a person is shown is product behaviour.
SUBJECT_MEMORY_TTL_SECONDS = 2 * 60 * 60
# A ceiling on the memory this holds, not on any conversation: 2000 questions
# of ~160 characters plus their subjects is well under a megabyte, and the
# oldest entry is dropped first.
SUBJECT_MEMORY_MAX_ENTRIES = 2000
# How much of a question stands in for a subject nobody remembered. Long enough
# to say what the question was about, short enough that a list of them does not
# become a second copy of the conversation.
MAX_SUBJECT_EXCERPT_CHARS = 80


def subject_excerpt(question: str) -> str:
    """A question shortened to stand in for the subject it was about."""
    text = " ".join(question.split()).strip()
    if len(text) <= MAX_SUBJECT_EXCERPT_CHARS:
        return text
    return text[:MAX_SUBJECT_EXCERPT_CHARS].rstrip() + "…"


class SubjectMemory:
    """Process-local `question -> subject`, with a TTL and a size ceiling.

    Keyed by `question_novelty.normalize` — the same normalisation the repeat
    filter uses — so a question that comes back with different punctuation or
    casing still finds its subject. Insertion order is kept, so eviction drops
    the oldest entry rather than an arbitrary one.

    Not thread-safe by construction, and it does not need to be: production
    runs a single API worker (the rate limiters have the same requirement), and
    the worst a race could do here is drop or duplicate a hint.
    """

    def __init__(
        self,
        ttl_seconds: float = SUBJECT_MEMORY_TTL_SECONDS,
        max_entries: int = SUBJECT_MEMORY_MAX_ENTRIES,
        clock=time.monotonic,
    ):
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._clock = clock
        self._entries: OrderedDict[str, tuple[float, str]] = OrderedDict()

    def remember(self, question: str, subject: str | None) -> None:
        """Record what a question we are about to show was about.

        A `None` subject records nothing: the model did not name one, and
        inventing one from the question here would put a guess into the next
        prompt under the name of something the model committed to.
        """
        key = normalize(question)
        if not key or not subject or not subject.strip():
            return
        self._entries.pop(key, None)
        self._entries[key] = (self._clock(), subject.strip())
        self._evict()

    def recall(self, question: str) -> str | None:
        """The subject of that question, or `None` — expired, evicted, unseen."""
        key = normalize(question)
        entry = self._entries.get(key)
        if entry is None:
            return None
        stored_at, subject = entry
        if self._clock() - stored_at > self.ttl_seconds:
            del self._entries[key]
            return None
        return subject

    def subject_of(self, question: str) -> str:
        """What to name this question by: its subject, else an excerpt of it."""
        return self.recall(question) or subject_excerpt(question)

    def _evict(self) -> None:
        now = self._clock()
        while self._entries:
            key, (stored_at, _) = next(iter(self._entries.items()))
            if now - stored_at <= self.ttl_seconds:
                break
            del self._entries[key]
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    def __len__(self) -> int:
        return len(self._entries)
