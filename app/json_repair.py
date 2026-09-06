"""Bounded repair of the JSON small models actually break.

Moved here from `app/query_rewrite.py` on 2026-09-06 (ClickUp 86cbejvt2)
**unchanged, byte for byte** — the rewrite stage still exposes
`query_rewrite.repair_json_object`, which is the name every caller and test
already imports. The move exists because prompt v6 gave
`POST /api/ai/question` a JSON answer too (`app/question_format.py`), and that
parser must stay importable without `config`, `httpx` or FastAPI so the
evaluation tools can run the production parse outside the container.
Re-typing the function into a second module was the alternative and the worse
one: two copies of a repair rule drift, and this one is deliberately narrow
enough that a drift would be invisible.

The rule itself: this is NOT a general-purpose JSON fixer. It may only ever
delete or re-type a *punctuation* character — never add, complete or guess a
value.
"""

from __future__ import annotations


def repair_json_object(blob: str) -> str | None:
    """Repair the three malformations small models actually produce, or None.

    This is NOT a general-purpose JSON fixer, and deliberately so: it may only
    ever delete or re-type a *punctuation* character, never add, complete or
    guess a value. What it handles, each observed in a real run (86cbe4nd3:
    a 4B model broke the syntax on 8 of 21 scenarios, always the same way):

    1. **a closer of the wrong type** — `{"queries": ["a", "b"}}`, the array
       closed with a brace. The stack of open brackets says which closer was
       meant, so the character is re-typed;
    2. **a truncated answer** — the output ceiling cut the object off at a
       clean boundary. The missing closers are appended;
    3. **a trailing comma** before a closer, which JSON forbids and most
       models emit sooner or later.

    Anything else returns None and the caller reports invalid JSON: a broken
    answer is a `rewrite_failed` degradation to the raw query, which is a
    documented, harmless path — inventing content to avoid it would not be.
    In particular an answer cut off INSIDE a string is refused: closing the
    quote would hand the retrieval layer half a sentence the model never
    finished writing.

    Returns the repaired text (which the caller still has to parse), or None
    when nothing safe can be done. Never returns the input unchanged: if no
    repair applied, the input was not repairable, and pretending otherwise
    would hide a second parse failure behind the first.
    """
    out: list[str] = []
    stack: list[str] = []
    changed = False
    in_string = False
    escaped = False
    closed = False
    for char in blob:
        if closed:
            # The object ended; the greedy `\{.*\}` search simply captured
            # trailing junk (the second brace of case 1, prose, a fence).
            changed = True
            break
        if in_string:
            out.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            out.append(char)
            continue
        if char in "{[":
            stack.append("}" if char == "{" else "]")
            out.append(char)
            continue
        if char in "}]":
            if not stack:
                changed = True
                break
            expected = stack.pop()
            if char != expected:
                changed = True
            out.append(expected)
            closed = not stack
            continue
        out.append(char)
    if in_string:
        return None
    if stack:
        # Truncated at a clean boundary: close what is still open.
        changed = True
        out.extend(reversed(stack))
    repaired = _drop_trailing_commas("".join(out))
    if repaired != "".join(out):
        changed = True
    return repaired if changed else None


def _drop_trailing_commas(blob: str) -> str:
    """Remove commas that sit right before a closer, outside strings."""
    out: list[str] = []
    in_string = False
    escaped = False
    pending_comma_at: int | None = None
    for char in blob:
        if in_string:
            out.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            pending_comma_at = None
            out.append(char)
            continue
        if char == ",":
            pending_comma_at = len(out)
            out.append(char)
            continue
        if char in "]}" and pending_comma_at is not None:
            del out[pending_comma_at]
            pending_comma_at = None
            out.append(char)
            continue
        if not char.isspace():
            pending_comma_at = None
        out.append(char)
    return "".join(out)
