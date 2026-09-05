"""
Prompt for the "index by senses" experiment (ClickUp 86cbeef7h, umbrella
86cbe4mtq).

Benchmark-only, and — unlike `rewrite_prompts.py` — not derived from any
production prompt: no production stage describes a corpus fragment, so
nothing is imported from `app/` and nothing here can drift away from a served
prompt.

The hypothesis it serves. Vector search over the RAW prayer fails — hit@10
0.286 for Gemini, 0.238 for bge-m3 (README, 86cbe4n7e, "Главная поправка") —
because a prayer and a Scripture fragment use different words. Production
bridges that gap per request, in the rewrite stage, which a local model does
not carry (86cbea05x). This prompt moves the bridge into the INDEX: each
fragment is annotated once in the register of human situations, and it is
that annotation which gets embedded and matched against the raw prayer.

Why a LIST of senses, not one description (Maria, 2026-09-04)
-------------------------------------------------------------
A passage rarely serves one situation. Averaging "for someone who has lost
their footing" and "for someone giving thanks after a rescue" into a single
paragraph produces one vector between the two, close to neither. So the model
returns 2-5 short senses, each a different situation the passage can meet,
and the index stores each sense as its OWN vector under the same
`canonical_id` (`retrieval_benchmark.py --doc-text description`; the search
de-duplicates by `canonical_id`, keeping the best-scoring sense, before
fusion). One good sense is then enough to retrieve the passage, and a sense
that fits nothing costs nothing.

Why `caution` is a structured field, not a sentence
---------------------------------------------------
`caution` is a boolean and `caution_note` a short phrase: could a person who
is already suffering hear this fragment as accusation, threat, curse,
judgement or punishment directed at them, or be harmed by opening images of
death and violence or by hopeless lament (README, "Критерии качества",
aspect 3). Historical violence against third parties and ritual sacrifice do
not meet that reader-danger test by themselves.

It is kept out of the sense texts for a measurable reason: the senses ARE the
vectors. A sentence such as "звучит как обличение" inside an embedded sense
pulls the fragment TOWARD prayers about guilt and punishment — the exact
direction it must not be pulled. As a field beside them, the same information
stays available as a filter and as a diagnostic (what share of the corpus
reads hard) without touching the geometry. Nothing filters on it yet: this
package only records it and reports the share; production safety is still the
genre blacklist plus the grounded rerank (ADR 0004/0005).

De-fingerprinting (same rule as rewrite prompt 8x and rerank prompt v6): not
one state, situation or theme of `scenarios.json` may appear in the wording
below. The instruction names only generic registers (comfort, instruction,
thanksgiving, hope) and generic hard tones; the concrete situations are what
the model has to derive from the fragment itself.
`tests/test_gen_descriptions.py` enforces the rule against the live dataset
(that file covers both this prompt and the tool that sends it).
"""

from __future__ import annotations

import json
import re

# Bump on ANY change of the wording or the output contract. The artifact
# records it, and two description files are only comparable when it matches.
#
# History:
#   1  initial contract: a list of 2-5 senses + structured caution
#   2  explicit ban on the first person and on restating one situation in
#      different words — the two failures the 4B probe of 2026-09-04 showed
#      ("Когда я чувствую…" everywhere, five senses that were one situation)
#   3  require at least two present-day prayer situations instead of plot
#      retellings; caution only for danger to a suffering reader, not any
#      third-party historical violence, ritual or conditional blessing
#   4  a ritual or sacrifice alone is still not caution, but a separate direct
#      threat of death, destruction or punishment, or a direct accusation,
#      inside its law still is
DESCRIPTION_PROMPT_VERSION = 4

# Self-contained on purpose (see the module docstring): this prompt is not a
# variant of a production one, so it does not import `app.query_rewrite` and
# cannot be broken by a change there.
LANGUAGE_NAMES = {
    "ru": "Russian",
    "en": "English",
    "uk": "Ukrainian",
}

# The contract asked of the model. Fewer than MIN is tolerated for a resilient
# ingest but recorded as a warning; prompt v3 still requires at least two
# present-day situations. More than MAX is trimmed — a long tail of senses is
# model padding, and every extra sense is an extra row in the index.
MIN_SENSES = 2
MAX_SENSES = 5

# What a leaked reference looks like inside a sense ("32:8", "32.8").
_REFERENCE_LEAK = re.compile(r"\d{1,3}\s*[:.]\s*\d{1,3}")

# One or two sentences about people and situations; anything longer is the
# model retelling the passage. Enforced on the way in, so the artifact never
# carries a paragraph nobody asked for.
MAX_SENSE_CHARS = 400
MAX_CAUTION_CHARS = 200


class DescriptionError(RuntimeError):
    """The model's answer cannot be read as a batch of annotations."""


def find_reference_leaks(text: str) -> list[str]:
    """`chapter:verse`-looking fragments inside a sense (should be none)."""
    return _REFERENCE_LEAK.findall(text)


def build_description_instruction(language: str, count: int) -> str:
    """System instruction for one batch of `count` fragments in `language`."""
    if language not in LANGUAGE_NAMES:
        raise ValueError(f"unsupported language: {language}")
    language_name = LANGUAGE_NAMES[language]
    return f"""You annotate passages of Scripture for the search index of a prayer application.

A person writes, in their own everyday words, what is happening in their life and what they are praying about. The index has to find the passages that can meet them there. Your annotation is what those everyday words are compared against, so it must speak about people and circumstances — not about the text.

For each of the {count} numbered fragments below produce two things.

"senses" — a list of {MIN_SENSES} to {MAX_SENSES} strings in {language_name}. Each string is one or two sentences saying WHO this passage can serve and IN WHAT life situation or inner state, as comfort, as instruction, as thanksgiving or as hope.
- One sense = one situation. Give a separate sense for each genuinely different situation the passage can meet; use only two when it honestly supports no more. Two senses that describe the same situation in different words are a wrong answer — three sharply different senses are better than five that repeat each other. Before adding a sense, check that it names a circumstance none of the others already names.
- At least two senses in every list must name a PRESENT-DAY prayer situation or inner state in which a person today could recognise themselves. This is especially important for narrative passages.
- Write in the THIRD person, about the person the passage can serve: "someone who…", "a person who…". Never write in the first person ("when I feel…", "I know that…") and never address the reader as "you" — these are index entries, not a prayer and not a sermon.
- Write about the person and the situation. Do not retell the passage, do not summarise what happens in it, do not quote it.
- Changing the actors in the plot into generic roles is still retelling. "People and animals suffering from an insect plague" and "a young man thrown into a pit and sold into slavery" are plot summaries, not present-day prayers. "A person betrayed by people close to them out of envy", "someone who sees clear signs but remains stubborn", and "a person whom God asks to leave the familiar and go into the unknown" are present-day situations.
- Never name a book, a chapter or a verse number. Never write "this passage", "the text", "these verses" or any other reference to the fragment itself — each sense must read as the description of a situation.
- Do not invent a fit that is not in the fragment. A pious formula that would fit any passage at all is a wrong answer.
- Plain, concrete words, as a person would describe their own life.

"caution" — true or false, with "caution_note" a short phrase in {language_name} when it is true (and "" when it is false).
- The same index serves people in the hardest hours of their lives. Set "caution" to true ONLY when a person already suffering through grief, illness, crisis or guilt could reasonably hear the fragment as dangerous words directed at them: an accusation, a threat or curse against the reader, judgement, punishment presented as the explanation of their suffering, images of death or violence in the opening lines, or hopeless lament without consolation.
- Do NOT set "caution" merely for historical violence involving third parties (such as plagues, wars or a brothers' plot), rituals or sacrifices, or a blessing that conditionally mentions a curse on enemies. These describe history or other people and are not directed at the suffering reader.
- A ritual or sacrifice by itself is not caution. HOWEVER, if a ritual law separately addresses its participant with a direct threat of death, being destroyed or punished, or directly accuses them, that threat or accusation still requires "caution": true. Do not let the ritual context hide it.
- A social or historical detail not addressed to the reader is not caution. "Shepherds are despised here" remains false.
- "caution_note" says in a few words exactly WHAT would sound dangerous to the suffering reader.
- Never put this warning into "senses": the fields are used for different things.

Output strictly a JSON object: {{"descriptions": [{{"id": 1, "senses": ["...", "..."], "caution": false, "caution_note": ""}}, ...]}} with exactly {count} objects — one per fragment, carrying the id printed with that fragment. No other keys, no text outside the JSON object."""


def build_description_user_content(items: list[dict]) -> str:
    """User message for one batch.

    `items` are `{"id": int, "title": str, "text": str}` in the order they are
    shown. The id is what the answer has to carry back: small integers local
    to the batch rather than canonical ids, because a weak model copies a
    short number reliably and mangles `v3:19.127.001-005`.
    """
    blocks = []
    for item in items:
        lines = [f"Fragment {item['id']}"]
        title = (item.get("title") or "").strip()
        if title:
            lines.append(f"Heading: {title}")
        lines.append(item["text"].strip())
        blocks.append("\n".join(lines))
    return "\n\n---\n\n".join(blocks)


def _clean_senses(raw: object) -> list[str]:
    """Whitespace-normalised, de-duplicated, capped list of sense strings."""
    if isinstance(raw, str):
        # Tolerated: the model ignored the list contract this time.
        raw = [raw]
    if not isinstance(raw, list):
        return []
    senses: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        cleaned = " ".join(item.split()).strip()[:MAX_SENSE_CHARS]
        if cleaned and cleaned not in senses:
            senses.append(cleaned)
    return senses[:MAX_SENSES]


def parse_description_response(
    text: str, expected_ids: list[int]
) -> dict[int, dict]:
    """Annotations of one batch, keyed by fragment id.

    Returns only entries that are usable AND belong to this batch; the caller
    compares the keys with `expected_ids` and decides whether to retry.
    Structural garbage (no JSON object, no `descriptions` list, not one usable
    entry) raises instead — a broken answer and an incomplete one get
    different treatment in the retry ladder.
    """
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise DescriptionError("response contains no JSON object")
    try:
        payload = json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        raise DescriptionError("response is not valid JSON") from exc
    raw = payload.get("descriptions")
    if not isinstance(raw, list):
        raise DescriptionError("response has no 'descriptions' list")

    wanted = set(expected_ids)
    found: dict[int, dict] = {}
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        item_id = entry.get("id")
        if isinstance(item_id, str) and item_id.strip().isdigit():
            item_id = int(item_id.strip())
        # bool is an int in Python; an id of `true` is not an id.
        if isinstance(item_id, bool) or not isinstance(item_id, int):
            continue
        if item_id not in wanted or item_id in found:
            # An id from another batch (or none at all) is not something to
            # guess about: senses attached to the wrong fragment are worse
            # than missing ones. A repeated id keeps the first answer.
            continue
        senses = _clean_senses(entry.get("senses"))
        if not senses:
            continue
        note = entry.get("caution_note")
        note = "" if not isinstance(note, str) else " ".join(note.split()).strip()
        raw_caution = entry.get("caution")
        if isinstance(raw_caution, bool):
            caution = raw_caution
        elif isinstance(raw_caution, str):
            caution = raw_caution.strip().lower() in ("true", "yes", "1", "да")
        else:
            # No usable flag at all: a note is itself the statement that
            # something in this fragment is hard to hear.
            caution = bool(note)
        found[item_id] = {
            "senses": senses,
            "caution": bool(caution),
            "caution_note": note[:MAX_CAUTION_CHARS] if caution else "",
        }
    if not found:
        raise DescriptionError("response contains no usable annotations")
    return found
