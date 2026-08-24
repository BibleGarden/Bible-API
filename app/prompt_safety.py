"""
Prompt hermetisation for the AI stages of scripture selection.

The rerank prompt frames the prayer context and every candidate as DATA
between explicit markers (`<<<PRAYER_CONTEXT ... PRAYER_CONTEXT>>>`,
`<<<CANDIDATE n ... CANDIDATE n>>>`, ADR 0005). A user reply containing
such a marker sequence could otherwise close the data block early and have
the rest of its text read as prompt structure — a delimiter injection.

`neutralize_prompt_markers` removes the obvious ways to build one:

1. invisible characters are dropped — every Unicode `Cf` format character
   (zero-width space/joiner/non-joiner, word joiner, BOM, soft hyphen,
   bidi controls) and the C0 controls other than tab/newline. Without this
   a reply could hide `<`ZWSP`<`ZWSP`<PRAYER_CONTEXT` from a naive
   collapse while the model still reads three angle brackets;
2. any run of two or more angle brackets — ASCII plus the fullwidth,
   single-guillemet, modifier and CJK/mathematical look-alikes — collapses
   to its first character, so `<<<`, `＜＜＜` and `‹‹‹` cannot spell a
   marker.

Marker WORDS are deliberately left alone: "candidate" and "context" are
ordinary words and stripping them would mangle real text; the markers only
exist as `<<<WORD` / `WORD>>>` combinations.

Known residual: the double guillemets `«»` are NOT treated as angle
brackets. They are ordinary punctuation in the Russian and Ukrainian
corpora (2362 of 11960 indexed chunks contain them), so folding them would
rewrite real scripture text. A reply spelling `«««PRAYER_CONTEXT` is
therefore passed through as-is — it does not reproduce the marker, but a
model could in principle read it as one. This is defence in depth, not the
grounding guarantee: the rerank answer is a validated index into the
server's candidate list (ADR 0005), so a successful delimiter confusion
still cannot introduce a passage.

Applied to every piece of text that reaches a prompt: topic, replies and
candidate texts from the DB (defence in depth — a chunk cannot contain
markers, but the sanitiser is applied uniformly rather than by trust).
Benign text is unchanged: verified byte-identical over all 57 strings of
the evaluation scenarios and 11958 of the 11960 corpus prompt texts, the
two exceptions being a stray soft hyphen inside a word in the Ukrainian
corpus (`вавилонсько­го`, `По­ворот`), whose removal is itself a fix.
"""

from __future__ import annotations

import re
import unicodedata

# Angle brackets and their look-alikes, split by direction. `«»` are
# excluded on purpose — see the module docstring.
_ANGLE_OPEN = "<＜‹˂〈⟨"
_ANGLE_CLOSE = ">＞›˃〉⟩"
_MARKER_RUN_RE = re.compile(
    f"[{re.escape(_ANGLE_OPEN)}]{{2,}}|[{re.escape(_ANGLE_CLOSE)}]{{2,}}"
)
# C0 controls other than tab and newline have no place in a prompt.
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _drop_invisible(text: str) -> str:
    """Remove Unicode format characters (Cf): zero-width, BOM, soft hyphen."""
    if not any(unicodedata.category(char) == "Cf" for char in text):
        return text
    return "".join(
        char for char in text if unicodedata.category(char) != "Cf"
    )


def neutralize_prompt_markers(text: str) -> str:
    """Strip the ability to forge prompt data-block delimiters."""
    cleaned = _CONTROL_RE.sub(" ", _drop_invisible(text))
    return _MARKER_RUN_RE.sub(lambda match: match.group(0)[0], cleaned)
