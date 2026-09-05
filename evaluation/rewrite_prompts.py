"""
Rewrite-prompt variants for benchmarking SMALL models on the rewrite stage
(ClickUp 86cbe4nd3, umbrella 86cbe4mtq).

**Since 86cbegg36 (2026-09-05) prompt "8c" IS the production prompt.** It won
the 7/8a/8b/8c matrix on `qwen3-30b-a3b-instruct-2507` and moved into
`app/query_rewrite.py` as v8, so `build_instruction("8c", ...)` returns
`query_rewrite.build_rewrite_instruction(...)` itself — one text, no copy that
could drift (`tests/test_rewrite_prompts.py` asserts the identity). What
version 8 adds to the winning 8c revision 2 is the closing reminder of the
answer language, hence `PROMPT_REVISIONS["8c"] = 3`: artifacts of revision 2
are a different prompt and are not comparable byte for byte.

The other three names are **history**. Version 7 is no longer production, so
it can no longer be imported; it is frozen below verbatim as
`_V7_INSTRUCTION`, and 8a/8b are still built by explicit surgery on that
frozen text, exactly as they were when they were measured. They exist so the
published matrix stays reproducible — nothing else reads them.

Why the 8x family existed. The 30B local candidate (86cbbm70n) failed hard on
v7: its variants were short generic pious formulas with none of the query's
semantics (mean 59 chars against 89 for the Gemini baseline), and 10 of 24
scenarios ended up without a single relevant passage in top-10. Two hypotheses
about *why* a small model does that, one prompt each:

- 8a "reference anchor": a small model recalls the actual scriptural wording
  far better when it is allowed to name the passage first. The model answers
  objects `{"ref": ..., "query": ...}`; only `query` reaches the artifact, the
  reference is kept aside for analysis. (The production contract forbids
  references inside the query — that constraint is preserved, it is just
  satisfied by a separate field instead of by suppression.)
- 8b "few-shot": a small model does not know how *specific* the queries are
  supposed to be without seeing examples. Two worked examples per language.
- 8c: both. This one won and is now production.

De-fingerprinting (mandatory, same rule as the rerank prompt v6): not one
example topic and not one example passage may coincide with anything in
`scenarios.json`. The examples now live in `app/query_rewrite.py` (they are
part of the shipped prompt) and are re-exported here so the benchmark's
statistics and the historical 8b keep reading the same list;
`tests/test_rewrite_prompts.py` enforces the rule against the live dataset —
book codes are resolved through `app/canon.py`, so the check is a real
comparison of coordinates rather than a comment claiming one. Example topics
are outside the evaluation set (exam, flat hunting, public speaking, moving
city, starting university, a wedding), and the quoted texts are written from
memory, close to the text, never copied from the dataset.
"""

from __future__ import annotations

import re

# Each prompt below is versioned by its own revision constant. Bump on any
# wording change: the artifact records `prompt_version` + `prompt_revision`,
# so two runs are only comparable when both match.
#
# History:
#   8a r1  initial "reference anchor" contract
#   8b r1  initial few-shot block
#   8b r2  closing reminder after the examples (answer language, and that the
#          examples show the FORM and must not be copied) — added after a 4B
#          probe copied an example verbatim into an unrelated scenario
#   8c r1/r2  same changes as 8b, since 8c embeds the same block
#   8c r3  8c became production v8 (86cbegg36) and gained the closing
#          language reminder; the text is now imported, not built here
PROMPT_REVISIONS = {
    "7": 0,    # frozen ex-production text, kept for the published matrix
    "8a": 1,
    "8b": 2,
    "8c": 3,
}

PROMPT_VERSIONS = tuple(PROMPT_REVISIONS)

# Anchors inside the production v7 instruction. A missing anchor is a hard
# error, never a fallback to some other wording.
_V7_REFERENCE_RULE = (
    "- Stay as close to the actual scriptural wording as you can recall; "
    "near-quotes are ideal. Never include book names, chapter numbers or "
    "verse numbers — only the passage's own words."
)
_V7_OUTPUT_MARKER = "Output strictly a JSON object:"

# Replacement of the rule above for the "reference anchor" prompts.
_8A_REFERENCE_RULE = (
    "- First recall WHICH passage you mean and write its reference in the "
    "\"ref\" field (book, chapter and verses, e.g. \"Psalm 32:8\"). Naming it "
    "first is what lets you then quote it accurately.\n"
    "- Then write \"query\" as a close paraphrase of THAT passage — as near to "
    "its actual wording as you can recall. The \"query\" field must contain "
    "only the passage's own words: no book names, no chapter numbers, no "
    "verse numbers (they belong in \"ref\" and nowhere else)."
)


def _8a_output_contract(variants: int, language_name: str) -> str:
    return (
        f"Output strictly a JSON object: "
        f'{{"queries": [{{"ref": "Book chapter:verses", "query": "..."}}, ...]}} '
        f"with exactly {variants} objects. Every \"query\" is in "
        f"{language_name}; \"ref\" may be in English."
    )


# --------------------------------------------------------------------------
# Few-shot examples: re-exported from the production prompt.
#
# They were defined here while the block was an experiment. Since 8c became
# production v8 they are part of the shipped instruction and live in
# `app/query_rewrite.py`; this module reads that same list, so the
# de-fingerprint test, the "copied an example verbatim" statistic of
# `gen_rewrites.py` and the historical 8b prompt can never disagree with what
# the application actually sends.
#
# Item = (canonical book code from app/canon.py, chapter, display reference,
# query text).
#
# The re-export is lazy, like every other app import in this module: importing
# `query_rewrite` pulls in `app/config.py`, which fails fast on a missing
# deployment variable, and the tools that import this module set their
# environment stubs first.
# --------------------------------------------------------------------------


def _examples() -> dict[str, list[dict]]:
    """The production example list."""
    from query_rewrite import _EXAMPLES  # noqa: WPS433

    return _EXAMPLES


def __getattr__(name: str):
    """`rewrite_prompts._EXAMPLES` still resolves, for the tests and tools."""
    if name == "_EXAMPLES":
        return _examples()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# «глава:стих» and its spaced variants — what a leaked reference looks like
# once it is inside a query string.
_REFERENCE_LEAK = re.compile(r"\d{1,3}\s*[:.]\s*\d{1,3}")


def find_reference_leaks(text: str) -> list[str]:
    """Chapter:verse-looking fragments inside a query string (should be none)."""
    return _REFERENCE_LEAK.findall(text)


def example_queries() -> set[str]:
    """Every example query text, whitespace-normalised.

    Used by the run statistics to measure how often a weak model copies an
    example verbatim instead of writing for the actual situation.
    """
    return {
        " ".join(query.split())
        for examples in _examples().values()
        for example in examples
        for _, _, _, query in example["items"]
    }


def _render_examples(with_refs: bool, variants: int, language_name: str) -> str:
    """The few-shot block — the production renderer, not a copy of it.

    `with_refs=False` is the plain-string answer form of the historical 8b;
    production (and therefore 8c) always renders the `{ref, query}` objects.
    """
    from query_rewrite import render_examples  # noqa: WPS433

    return render_examples(variants, language_name, with_refs=with_refs)


# The ex-production v7 instruction, frozen on 2026-09-05 when v8 replaced it
# (86cbegg36). Copied byte for byte out of `app/query_rewrite.py` at commit
# 79243c1 so that the published 7 / 8a / 8b columns of the matrix
# (README, 86cbea05x) can still be rebuilt. It is dead text: nothing in the
# application reads it, and it must NOT be "kept in sync" with v8 — the whole
# point of a frozen baseline is that it does not move.
_V7_INSTRUCTION = """You prepare search queries for a Bible passage retrieval system inside a prayer app.

Input: a prayer context — a topic and optional remarks from the person praying.

Task: recall which well-known Bible passages truly speak to this person's situation and state, then write exactly {variants} short standalone search queries in {language_name}. Each query must be a close paraphrase of a DIFFERENT well-known Bible passage — rendered in the wording of {register_hint}. The queries are matched against Bible passage texts by semantic similarity, so each query must sound like the passage itself — a promise, comfort, declaration or praise as Scripture phrases it — not like the person's own words and not like a prayer request.

Rules:
- Cover DIFFERENT passages and different spiritual angles of the situation (for example: thanksgiving, God's care, comfort in sorrow, God's presence, guidance, hope, peace of heart).
- Order the queries from the passage most directly fitting the situation to more complementary angles.
- When the prayer is for another person (intercession — a child, a friend, a family member), include passages about God's heart and promises toward that person: His desire to save, keep, guide and bless them.
- Stay as close to the actual scriptural wording as you can recall; near-quotes are ideal. Never include book names, chapter numbers or verse numbers — only the passage's own words.
- The person may be in grief, anxiety or crisis. Choose only passages of comfort, mercy, hope and God's closeness — never accusation, condemnation, punishment, curses or end-times fear.
- Beware of words with double meanings: resolve them by the person's intent (for example, peace of heart versus the world).
- 5-25 words per query. No explanations, no numbering inside the strings.

Output strictly a JSON object: {{"queries": ["...", "...", "...", "..."]}} with exactly {variants} strings in {language_name}."""


def _v7_instruction(language: str, variants: int) -> str:
    """The frozen v7 instruction, rendered for one language."""
    from query_rewrite import _LANGUAGES  # noqa: WPS433

    language_name, register_hint = _LANGUAGES[language]
    return _V7_INSTRUCTION.format(
        variants=variants,
        language_name=language_name,
        register_hint=register_hint,
    )


def _require(text: str, anchor: str, version: str) -> None:
    if anchor not in text:
        raise RuntimeError(
            f"prompt {version}: anchor not found in the frozen v7 "
            f"instruction — do not guess, fix "
            f"evaluation/rewrite_prompts.py: {anchor!r}"
        )


def build_instruction(version: str, language: str, variants: int) -> str:
    """System instruction for one prompt version.

    `8c` is the production prompt itself (v8), returned by the application's
    own builder. `7`, `8a` and `8b` are the frozen historical texts.
    """
    if version not in PROMPT_REVISIONS:
        raise ValueError(f"unknown prompt version: {version}")

    from query_rewrite import _LANGUAGES, build_rewrite_instruction  # noqa: WPS433

    if version == "8c":
        return build_rewrite_instruction(language, variants)

    base = _v7_instruction(language, variants)
    if version == "7":
        return base

    language_name = _LANGUAGES[language][0]
    text = base
    if version == "8a":
        _require(text, _V7_REFERENCE_RULE, version)
        text = text.replace(_V7_REFERENCE_RULE, _8A_REFERENCE_RULE)
        marker_at = text.find(_V7_OUTPUT_MARKER)
        if marker_at < 0:
            raise RuntimeError(
                f"prompt {version}: output contract of v7 not found "
                f"({_V7_OUTPUT_MARKER!r})"
            )
        text = text[:marker_at] + _8a_output_contract(variants, language_name)
    if version == "8b":
        block = _render_examples(False, variants, language_name)
        text = f"{text}\n\n{block}"
    return text


def parse_response(
    version: str, text: str, variants: int
) -> tuple[list[str], list[str]]:
    """Parse a model answer into (queries, refs).

    v7/8b reuse the production parser byte for byte. 8a/8c unpack the
    `{"ref": ..., "query": ...}` objects, hand the queries to the same
    production cleaning (dedup, whitespace, length cap) and return the
    references separately — they never reach the artifact's `variants`.

    For 8c (= production v8) the `queries` half is deliberately identical to
    what `parse_rewrite_response` returns for the same answer: the JSON is
    loaded by the production loader, including its bounded repair, and the
    per-item rules below match it case for case. The only thing this function
    adds is `refs`, which production reads and drops.
    """
    from query_rewrite import (  # noqa: WPS433
        _MAX_QUERY_CHARS,
        _load_rewrite_payload,
        QueryRewriteError,
        parse_rewrite_response,
    )

    if version in ("7", "8b"):
        return parse_rewrite_response(text, variants), []

    payload = _load_rewrite_payload(text)
    raw = payload.get("queries")
    if not isinstance(raw, list):
        raise QueryRewriteError("rewrite response has no 'queries' list")

    queries: list[str] = []
    refs: list[str] = []
    for item in raw:
        if isinstance(item, str):
            # Tolerated: the model ignored the object contract this time.
            query, ref = item, ""
        elif isinstance(item, dict):
            query = item.get("query")
            ref = item.get("ref")
            if not isinstance(query, str):
                continue
            # A non-string `ref` is not a reason to drop a usable query —
            # production would keep it, and the two must not diverge.
            ref = ref if isinstance(ref, str) else ""
        else:
            continue
        cleaned = " ".join(query.split()).strip()
        if not cleaned:
            continue
        cleaned = cleaned[:_MAX_QUERY_CHARS]
        if cleaned in queries:
            continue
        queries.append(cleaned)
        refs.append(" ".join(ref.split()).strip())
    if not queries:
        raise QueryRewriteError("rewrite response contains no usable queries")
    return queries[:variants], refs[:variants]
