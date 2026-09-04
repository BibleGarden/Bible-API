"""
Rewrite-prompt variants for benchmarking SMALL models on the rewrite stage
(ClickUp 86cbe4nd3, umbrella 86cbe4mtq).

Benchmark-only. The production prompt lives in `app/query_rewrite.py` and is
NOT touched here: version 7 is imported from it verbatim, and every 8x variant
is built by explicit, asserted surgery on that imported text. If the
production wording ever changes so that an anchor substring is gone, the build
raises instead of silently producing a different prompt (project rule: no
silent fallbacks).

Why the 8x family exists. The 30B local candidate (86cbbm70n) failed hard on
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
- 8c: both.

De-fingerprinting (mandatory, same rule as the rerank prompt v6): not one
example topic and not one example passage may coincide with anything in
`scenarios.json`. Every example below therefore carries its canonical
`(book_code, chapter)` explicitly, and `tests/test_rewrite_prompts.py`
enforces the rule against the live dataset — book codes are resolved through
`app/canon.py`, so the check is a real comparison of coordinates rather than a
comment claiming one. Example topics are outside the evaluation set (exam,
flat hunting, public speaking, moving city, starting university, a wedding),
and the quoted texts are written from memory, close to the text, never copied
from the dataset.
"""

from __future__ import annotations

import json
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
PROMPT_REVISIONS = {
    "7": 0,    # revision of the *production* prompt is REWRITE_PROMPT_VERSION
    "8a": 1,
    "8b": 2,
    "8c": 2,
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
# Few-shot examples (8b / 8c)
#
# Item = (canonical book code from app/canon.py, chapter, display reference,
# query text). The book code and chapter exist so the de-fingerprint test can
# compare coordinates instead of parsing English book names.
#
# Deliberately outside the evaluation set: no topic and no passage below
# appears in scenarios.json (enforced by tests/test_rewrite_prompts.py).
# --------------------------------------------------------------------------

_EXAMPLES: dict[str, list[dict]] = {
    "ru": [
        {
            "topic": "Готовлюсь к экзамену, боюсь не сдать",
            "replies": ["Учил всё лето", "Очень боюсь провалиться"],
            "items": [
                ("psa", 32, "Psalm 32:8",
                 "Вразумлю тебя, наставлю тебя на путь, по которому тебе идти, "
                 "буду руководить тебя, око Мое над тобою"),
                ("pro", 2, "Proverbs 2:6",
                 "Господь даёт мудрость, из уст Его — знание и разум"),
                ("isa", 30, "Isaiah 30:15",
                 "В тишине и уповании крепость ваша, в покое и надежде — "
                 "спасение ваше"),
                ("2ti", 1, "2 Timothy 1:7",
                 "Дал нам Бог духа не боязни, но силы, и любви, и целомудрия"),
                ("psa", 62, "Psalm 62:1-2",
                 "Только в Боге успокаивается душа моя, от Него спасение моё, "
                 "Он твердыня моя, не поколеблюсь более"),
                ("psa", 138, "Psalm 138:8",
                 "Господь совершит за меня, милость Твоя, Господи, вовек, "
                 "дела рук Твоих не оставляй"),
            ],
        },
        {
            "topic": "Ищу квартиру, скоро заканчивается аренда",
            "replies": ["Осталось меньше месяца", "Ничего подходящего не нахожу"],
            "items": [
                ("psa", 16, "Psalm 16:5-6",
                 "Господь есть часть наследия моего и чаши моей, межи мои "
                 "прошли по прекрасным местам, и наследие моё приятно для меня"),
                ("mat", 7, "Matthew 7:7-8",
                 "Просите, и дано будет вам, ищите, и найдёте, стучите, и "
                 "отворят вам"),
                ("psa", 145, "Psalm 145:15-16",
                 "Очи всех уповают на Тебя, и Ты даёшь им пищу их в своё "
                 "время, открываешь руку Твою и насыщаешь всё живущее"),
                ("jer", 29, "Jeremiah 29:11",
                 "Я знаю намерения, какие имею о вас, намерения во благо, а не "
                 "на зло, чтобы дать вам будущность и надежду"),
                ("luk", 12, "Luke 12:31",
                 "Ищите прежде Царствия Божия, и это всё приложится вам"),
                ("isa", 58, "Isaiah 58:11",
                 "Будет Господь вождём твоим всегда, и во время засухи будет "
                 "насыщать душу твою, и ты будешь как напоенный водою сад"),
            ],
        },
    ],
    "en": [
        {
            "topic": "I have to speak in front of a large audience tomorrow",
            "replies": ["My hands shake when everyone looks at me",
                        "I am afraid my mind will go blank"],
            "items": [
                ("jos", 1, "Joshua 1:9",
                 "Be strong and courageous, do not be afraid, for the LORD "
                 "your God is with you wherever you go"),
                ("psa", 138, "Psalm 138:3",
                 "On the day I called, You answered me, You emboldened me and "
                 "strengthened my soul"),
                ("isa", 50, "Isaiah 50:4",
                 "The Lord GOD has given me the tongue of a disciple, to know "
                 "how to sustain the weary with a word"),
                ("1jn", 4, "1 John 4:18",
                 "There is no fear in love, but perfect love drives out fear"),
                ("pro", 4, "Proverbs 4:11-12",
                 "I have guided you in the way of wisdom, when you walk your "
                 "steps will not be hindered, and when you run you will not "
                 "stumble"),
                ("psa", 19, "Psalm 19:14",
                 "May the words of my mouth and the meditation of my heart be "
                 "pleasing in Your sight, O LORD, my Rock and my Redeemer"),
            ],
        },
        {
            "topic": "Preparing to move to another city",
            "replies": ["We leave in three weeks", "I know no one there"],
            "items": [
                ("gen", 12, "Genesis 12:1-2",
                 "Go from your country to the land I will show you, and I will "
                 "bless you and make your name great"),
                ("exo", 33, "Exodus 33:14",
                 "My Presence will go with you, and I will give you rest"),
                ("psa", 143, "Psalm 143:8",
                 "Show me the way I should walk, for to You I lift up my soul"),
                ("jer", 17, "Jeremiah 17:7-8",
                 "Blessed is the man who trusts in the LORD, he is like a tree "
                 "planted by the waters, sending out its roots toward the "
                 "stream"),
                ("psa", 84, "Psalm 84:5",
                 "Blessed are those whose strength is in You, in whose heart "
                 "are the highways to Zion"),
                ("rom", 15, "Romans 15:13",
                 "May the God of hope fill you with all joy and peace as you "
                 "believe, so that you may overflow with hope"),
            ],
        },
    ],
    "uk": [
        {
            "topic": "Починаю навчання в університеті",
            "replies": ["Перший тиждень уже наступного понеділка",
                        "Боюся, що не впораюся"],
            "items": [
                ("pro", 9, "Proverbs 9:10",
                 "Початок мудрості — страх Господній, і пізнання Святого — "
                 "це розум"),
                ("psa", 143, "Psalm 143:10",
                 "Навчи мене чинити волю Твою, бо Ти Бог мій, Дух Твій добрий "
                 "нехай веде мене по рівній землі"),
                ("jhn", 15, "John 15:5",
                 "Я — виноградна лоза, а ви — гілки, хто перебуває в Мені, той "
                 "приносить рясний плід, бо без Мене нічого чинити не можете"),
                ("col", 1, "Colossians 1:9-10",
                 "Щоб ви наповнилися пізнанням волі Його в усякій мудрості й "
                 "розумінні духовному"),
                ("1co", 10, "1 Corinthians 10:13",
                 "Вірний Бог, Який не попустить, щоб ви були спокушені понад "
                 "силу, але при спокусі дасть і полегшення"),
                ("jer", 33, "Jeremiah 33:3",
                 "Клич до Мене — і Я тобі відповім, і подам тобі великі та "
                 "незрозумілі речі, яких ти не знаєш"),
            ],
        },
        {
            "topic": "Готуюся до весілля",
            "replies": ["Вінчання за місяць", "Хочу, щоб наш дім був у Господі"],
            "items": [
                ("gen", 2, "Genesis 2:24",
                 "Покине чоловік батька свого та матір свою, і пристане до "
                 "жінки своєї, і стануть вони одним тілом"),
                ("sng", 8, "Song of Songs 8:6-7",
                 "Поклади мене, як печатку, на серце своє, бо сильна любов, як "
                 "смерть, і великі води не зможуть згасити любові"),
                ("1co", 13, "1 Corinthians 13:4-7",
                 "Любов довготерпить, милосердствує, не заздрить, не "
                 "величається, не шукає свого, усе зносить, усе терпить"),
                ("psa", 67, "Psalm 67:1",
                 "Нехай Бог помилує нас і поблагословить нас, нехай засяє над "
                 "нами лице Його"),
                ("eph", 5, "Ephesians 5:25",
                 "Чоловіки, любіть своїх дружин, як і Христос полюбив Церкву "
                 "й видав Себе за неї"),
                ("psa", 37, "Psalm 37:5",
                 "Здай на Господа дорогу свою, і надійся на Нього, і Він "
                 "зробить"),
            ],
        },
    ],
}

_EXAMPLE_LANGUAGE_ORDER = ("ru", "en", "uk")

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
        for examples in _EXAMPLES.values()
        for example in examples
        for _, _, _, query in example["items"]
    }


def _render_context(topic: str, replies: list[str]) -> str:
    lines = [f"Topic: {topic}"]
    if replies:
        lines.append("Remarks:")
        lines.extend(f"- {reply}" for reply in replies)
    return "\n".join(lines)


def _render_examples(with_refs: bool, variants: int, language_name: str) -> str:
    """The few-shot block, identical in content for every target language.

    All three languages are shown on purpose: the register differs per
    language and a small model benefits from seeing what "close paraphrase"
    means in each of them. The closing reminder exists because a 4B probe
    copied one example verbatim into an unrelated scenario (revision 2).
    """
    blocks: list[str] = []
    for language in _EXAMPLE_LANGUAGE_ORDER:
        for example in _EXAMPLES[language]:
            items = example["items"][:variants]
            if with_refs:
                answer = {
                    "queries": [
                        {"ref": ref, "query": query}
                        for _, _, ref, query in items
                    ]
                }
            else:
                answer = {"queries": [query for _, _, _, query in items]}
            blocks.append(
                f"### Example ({language})\n"
                f"Input:\n{_render_context(example['topic'], example['replies'])}\n"
                f"Output:\n{json.dumps(answer, ensure_ascii=False)}"
            )
    reminder = (
        "The examples above are about OTHER situations and exist only to show "
        "the FORM of a good answer: how concrete a query is, and how closely it "
        "follows scriptural wording.\n"
        f"- Write your queries in {language_name}, whatever language the "
        "examples happen to use.\n"
        "- Never copy a sentence from an example. Every query must come from "
        "the prayer context you were actually given; a line lifted from an "
        "example is a wrong answer even if it sounds beautiful.\n"
        "- Choose passages that speak to THIS person's situation, not the "
        "passages the examples chose."
    )
    return (
        "Worked examples of the same task on unrelated situations. Match this "
        "level of concreteness: every query carries the meaning of THAT "
        "situation in scriptural wording — never a generic formula of praise "
        "that would fit any prayer.\n\n"
        + "\n\n".join(blocks)
        + "\n\n"
        + reminder
    )


def _v7_instruction(language: str, variants: int) -> str:
    """The production v7 instruction, imported — never re-typed here."""
    from query_rewrite import build_rewrite_instruction  # noqa: WPS433

    return build_rewrite_instruction(language, variants)


def _require(text: str, anchor: str, version: str) -> None:
    if anchor not in text:
        raise RuntimeError(
            f"prompt {version}: anchor not found in the production v7 "
            f"instruction — app/query_rewrite.py changed, update "
            f"evaluation/rewrite_prompts.py instead of guessing: {anchor!r}"
        )


def build_instruction(version: str, language: str, variants: int) -> str:
    """System instruction for one prompt version."""
    if version not in PROMPT_REVISIONS:
        raise ValueError(f"unknown prompt version: {version}")

    from query_rewrite import _LANGUAGES  # noqa: WPS433

    base = _v7_instruction(language, variants)
    if version == "7":
        return base

    language_name = _LANGUAGES[language][0]
    text = base
    if version in ("8a", "8c"):
        _require(text, _V7_REFERENCE_RULE, version)
        text = text.replace(_V7_REFERENCE_RULE, _8A_REFERENCE_RULE)
        marker_at = text.find(_V7_OUTPUT_MARKER)
        if marker_at < 0:
            raise RuntimeError(
                f"prompt {version}: output contract of v7 not found "
                f"({_V7_OUTPUT_MARKER!r})"
            )
        text = text[:marker_at] + _8a_output_contract(variants, language_name)
    if version in ("8b", "8c"):
        block = _render_examples(version == "8c", variants, language_name)
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
    """
    from query_rewrite import (  # noqa: WPS433
        _MAX_QUERY_CHARS,
        QueryRewriteError,
        parse_rewrite_response,
    )

    if version in ("7", "8b"):
        return parse_rewrite_response(text, variants), []

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise QueryRewriteError("rewrite response contains no JSON object")
    try:
        payload = json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        raise QueryRewriteError("rewrite response is not valid JSON") from exc
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
            ref = item.get("ref") or ""
            if not isinstance(query, str) or not isinstance(ref, str):
                continue
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
