"""
LLM query reformulation for scripture-selection retrieval.

Why this exists (architect/adr/0004-retrieval-pipeline.md): the dominant
failure mode of raw embedding search is the register gap between everyday
prayer wording and biblical language (ADR 0002 diagnostic probe: a raw query
ranked the reference passage ~356th, a scripture-styled rephrasing of the
same intent ranked it 4th). Before searching, the prayer context is rewritten
by the configured model into several short queries in the biblical register
of the target language, each covering a different spiritual angle of the
situation; the retrieval layer then searches with every variant and fuses the
results.

Prompt v8 (ClickUp 86cbegg36, 2026-09-05) is the former benchmark prompt
"8c": the model first names the passage it means in a `ref` field and only
then writes the `query`, the instruction carries per-language worked
examples, and a closing line repeats the answer language. Measured on
`qwen3-30b-a3b-instruct-2507`, the reference anchor alone lifted hit@10 from
0.583 to 0.875 and few-shot on top of it lifted recall@10 to 0.547 and MRR to
0.558 (evaluation/README.md, 86cbea05x). `ref` never reaches the retrieval
layer: `parse_rewrite_response` drops it — it is scaffolding for the model's
recall, and the queries themselves still carry no coordinates.

Privacy: the prayer context goes to the configured provider (pre-cleared: it
already goes there for the Twinkler companion). Neither the prayer text nor
the API key must ever be logged — callers log only failure categories.

The prompt is generic: it knows nothing about the evaluation dataset and
never receives reference answers. The worked examples are de-fingerprinted —
no example topic and no example passage may touch `evaluation/scenarios.json`,
which `tests/test_rewrite_prompts.py` checks against the live dataset. That
check is what moved two examples on 2026-09-05 (prompt revision 4): grading
made Ps 62 and Ps 16 references, so the "экзамен" and "квартира" examples
now anchor on Ps 94:19 and Deut 12:9-10. The numbers quoted above were
measured on revision 3; revision 4 was re-measured warm and is inside the
server's own run-to-run spread (evaluation/README.md, 86cbegg36).
"""

from __future__ import annotations

import json
import logging
import re
import time

import httpx

from config import (
    AI_SCRIPTURE_REWRITE_MODEL,
    REWRITE_API_KEY,
    SCRIPTURE_REWRITE_PROVIDER,
    StageProvider,
)
from deadline import Deadline
from gemini_retry import (
    RETRYABLE_STATUS,
    provider_timeout,
    rate_limit_of,
    retry_pause,
)
# `repair_json_object` lived in this module until 2026-09-06 and is re-exported
# here unchanged: it moved to a dependency-free module (ClickUp 86cbejvt2) so
# that `app/question_format.py` — the parser of the question endpoint's own JSON
# answer since prompt v6 — can import the SAME repair without dragging `config`
# and `httpx` in. Callers and tests keep importing it from here.
from json_repair import repair_json_object
from llm_client import ChatClient, LLMError
from prompt_safety import neutralize_prompt_markers

logger = logging.getLogger(__name__)

# Bump on any change of the prompt wording or output contract; benchmark
# caches are keyed by (model, prompt version).
REWRITE_PROMPT_VERSION = 8

# 6 variants + interleave fusion is the benchmark-approved configuration
# (fewer variants lose recall, more dilute the fused top-10 — ADR 0004).
REWRITE_VARIANTS = 6
_MAX_QUERY_CHARS = 200
_MODEL_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")
# Linear backoff of the retry ladder: 2 s before the second attempt, 4 s
# before the third (unchanged; only when the budget can still afford it).
_RETRY_BASE_SECONDS = 2.0

GEMINI_GENERATE_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "{model}:generateContent"
)

# Register hints steer the rewrite toward the vocabulary of the translation
# actually indexed for each language (ru: syn, en: bsb, uk: ubh).
_LANGUAGES = {
    "ru": (
        "Russian",
        "the classical Russian Synodal Bible (Синодальный перевод) — use its "
        "vocabulary and phrasing, e.g. «уповать на Господа», «не убойся», "
        "«утешение», «милость»",
    ),
    "en": (
        "English",
        "the Berean Standard Bible — modern literal English wording (never "
        "archaic King James English: no thee, thou, ye, -eth forms), e.g. "
        "\"do not be anxious\", \"the LORD is my refuge\", \"cast your cares\"",
    ),
    "uk": (
        "Ukrainian",
        "a classical Ukrainian Bible translation — use its vocabulary and "
        "phrasing, e.g. «уповати на Господа», «не бійся», «потіха», «милість»",
    ),
}

# Languages the rewrite stage (and therefore the selection endpoint) supports.
SUPPORTED_LANGUAGES = tuple(_LANGUAGES)


class QueryRewriteError(RuntimeError):
    """The rewrite backend is not configured, unreachable or returned junk."""


def build_search_query(topic: str, user_replies: list[str]) -> str:
    """The raw retrieval query: prayer topic + allowed user replies."""
    parts = [topic.strip()] + [r.strip() for r in user_replies]
    return "\n".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Worked examples of the instruction (prompt v8)
#
# Item = (canonical book code from app/canon.py, chapter, display reference,
# query text). The book code and chapter are carried so the de-fingerprint
# test can compare COORDINATES instead of parsing English book names.
#
# De-fingerprinting is mandatory (the rule the rerank prompt v6 established):
# not one example topic and not one example passage may coincide with anything
# in `evaluation/scenarios.json`, or the prompt would be measuring itself.
# Example topics are outside the evaluation set (exam, flat hunting, public
# speaking, moving city, starting university, a wedding) and the quoted texts
# are written from memory, close to the text, never copied from the dataset.
# `tests/test_rewrite_prompts.py` enforces this against the live dataset.
#
# The dataset grows, so this list has to move with it. On 2026-09-05 Maria's
# grading of the Russian top-1 pairs (86cbedtf8) turned Ps 62 and Ps 16 into
# graded references, and the two examples that quoted them had to go
# (86cbegg36, prompt revision 4): the "экзамен" example now anchors on
# Ps 94:19 and the "квартира" one on Deut 12:9-10 — same function, chapters
# that touch no reference of any grade. The test is what noticed; it is meant
# to fail exactly like this whenever the benchmark's answers move under the
# prompt.
# ---------------------------------------------------------------------------

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
                ("psa", 94, "Psalm 94:19",
                 "При умножении скорбей моих в сердце моём утешения Твои "
                 "услаждают душу мою"),
                ("psa", 138, "Psalm 138:8",
                 "Господь совершит за меня, милость Твоя, Господи, вовек, "
                 "дела рук Твоих не оставляй"),
            ],
        },
        {
            "topic": "Ищу квартиру, скоро заканчивается аренда",
            "replies": ["Осталось меньше месяца", "Ничего подходящего не нахожу"],
            "items": [
                ("deu", 12, "Deuteronomy 12:9-10",
                 "Господь Бог даёт вам место покоя и удел, и поселитесь на "
                 "земле, и будете жить безопасно"),
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


def _render_context(topic: str, replies: list[str]) -> str:
    """An example's input, rendered exactly like `build_rewrite_user_content`."""
    lines = [f"Topic: {topic}"]
    if replies:
        lines.append("Remarks:")
        lines.extend(f"- {reply}" for reply in replies)
    return "\n".join(lines)


def render_examples(
    variants: int, language_name: str, with_refs: bool = True
) -> str:
    """The few-shot block of the instruction, identical for every language.

    All three languages are shown on purpose: the register differs per
    language and a small model benefits from seeing what "close paraphrase"
    means in each of them. The closing reminder exists because a 4B probe
    copied an example verbatim into an unrelated scenario (86cbe4nd3).

    `with_refs=False` produces the answers as plain strings instead of
    `{ref, query}` objects. Production never asks for it: it is what the
    historical benchmark prompt 8b was made of, and
    `evaluation/rewrite_prompts.py` calls this function rather than keeping a
    second copy of the examples that could drift from these.
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


def build_rewrite_instruction(language: str, variants: int = REWRITE_VARIANTS) -> str:
    """System instruction for the rewrite call (static per language).

    Prompt v8. Three things distinguish it from v7, each measured on a small
    local model (evaluation/README.md, 86cbea05x):

    1. the reference anchor — the model names the passage in `ref` before
       writing the `query`, which turns generic pious formulas into near
       quotes (hit@10 0.583 -> 0.875 on qwen3-30b) and, as a side effect,
       almost eliminates verbatim copying of the examples (0.112 -> 0.008);
    2. the worked examples, which add the rest of recall@10 and MRR on top;
    3. the closing reminder of the answer language, the last line of the
       instruction: the examples are shown in three languages, and a small
       model that has just read six of them is where language drift would
       start. This one is a precaution, not a measured gain — on
       qwen3-30b the variants were already in the right language for 21 of
       21 scenarios without it. It stays because removing it is not measurably
       better either: warm run against warm run on the production embedder it
       is ahead on hit@10 and recall@10 and 0.044 behind on MRR, inside that
       server's own 0.072 spread (ADR 0004, "Prompt v8"; evaluation/README.md,
       86cbegg36). Dropping this paragraph makes the prompt byte-identical to
       the measured 8c revision 2.
    """
    language_name, register_hint = _LANGUAGES[language]
    instruction = f"""You prepare search queries for a Bible passage retrieval system inside a prayer app.

Input: a prayer context — a topic and optional remarks from the person praying.

Task: recall which well-known Bible passages truly speak to this person's situation and state, then write exactly {variants} short standalone search queries in {language_name}. Each query must be a close paraphrase of a DIFFERENT well-known Bible passage — rendered in the wording of {register_hint}. The queries are matched against Bible passage texts by semantic similarity, so each query must sound like the passage itself — a promise, comfort, declaration or praise as Scripture phrases it — not like the person's own words and not like a prayer request.

Rules:
- Cover DIFFERENT passages and different spiritual angles of the situation (for example: thanksgiving, God's care, comfort in sorrow, God's presence, guidance, hope, peace of heart).
- Order the queries from the passage most directly fitting the situation to more complementary angles.
- When the prayer is for another person (intercession — a child, a friend, a family member), include passages about God's heart and promises toward that person: His desire to save, keep, guide and bless them.
- First recall WHICH passage you mean and write its reference in the "ref" field (book, chapter and verses, e.g. "Psalm 32:8"). Naming it first is what lets you then quote it accurately.
- Then write "query" as a close paraphrase of THAT passage — as near to its actual wording as you can recall. The "query" field must contain only the passage's own words: no book names, no chapter numbers, no verse numbers (they belong in "ref" and nowhere else).
- The person may be in grief, anxiety or crisis. Choose only passages of comfort, mercy, hope and God's closeness — never accusation, condemnation, punishment, curses or end-times fear.
- Beware of words with double meanings: resolve them by the person's intent (for example, peace of heart versus the world).
- 5-25 words per query. No explanations, no numbering inside the strings.

Output strictly a JSON object: {{"queries": [{{"ref": "Book chapter:verses", "query": "..."}}, ...]}} with exactly {variants} objects. Every "query" is in {language_name}; "ref" may be in English."""
    examples = render_examples(variants, language_name)
    closing = (
        f"Before you answer, check the language once more: every \"query\" "
        f"must be written in {language_name} — the language of the prayer "
        f"context, not the language of the examples above and not the "
        f"language of the \"ref\" field."
    )
    return f"{instruction}\n\n{examples}\n\n{closing}"


def build_rewrite_user_content(topic: str, user_replies: list[str]) -> str:
    """User message for the rewrite call.

    Prayer text is passed through `neutralize_prompt_markers` so it cannot
    forge the data-block delimiters used by the downstream rerank prompt
    (prompt_safety); benign text is unchanged.
    """
    lines = [f"Topic: {neutralize_prompt_markers(topic.strip())}"]
    replies = [
        neutralize_prompt_markers(r.strip()) for r in user_replies if r.strip()
    ]
    if replies:
        lines.append("Remarks:")
        lines.extend(f"- {reply}" for reply in replies)
    return "\n".join(lines)


def _load_rewrite_payload(text: str) -> dict:
    """The JSON object of a rewrite answer, repaired if it needs it."""
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise QueryRewriteError("rewrite response contains no JSON object")
    blob = match.group(0)
    try:
        payload = json.loads(blob)
    except json.JSONDecodeError as exc:
        repaired = repair_json_object(blob)
        if repaired is None:
            raise QueryRewriteError(
                "rewrite response is not valid JSON"
            ) from exc
        try:
            payload = json.loads(repaired)
        except json.JSONDecodeError:
            raise QueryRewriteError(
                "rewrite response is not valid JSON"
            ) from exc
    if not isinstance(payload, dict):
        raise QueryRewriteError("rewrite response is not a JSON object")
    return payload


def parse_rewrite_response(text: str, variants: int = REWRITE_VARIANTS) -> list[str]:
    """Parse the model output into a clean list of query strings.

    Tolerates markdown fences, surrounding prose and the bounded syntax
    breakage `repair_json_object` describes; deduplicates, drops empties,
    truncates overlong strings. Raises QueryRewriteError when nothing usable
    remains — the caller then degrades to the raw query.

    Prompt v8 asks for `{"ref": ..., "query": ...}` objects: **`ref` is read
    and thrown away here**. It exists to make the model recall a real passage
    before paraphrasing it; letting it through would put book names and
    chapter numbers into an embedding whose corpus contains neither. A bare
    string is still accepted, so an answer in the v7 shape (an older model
    ignoring the object contract) degrades to exactly what it used to be
    rather than to nothing.
    """
    payload = _load_rewrite_payload(text)
    raw = payload.get("queries")
    if not isinstance(raw, list):
        raise QueryRewriteError("rewrite response has no 'queries' list")
    queries: list[str] = []
    for item in raw:
        if isinstance(item, str):
            query = item
        elif isinstance(item, dict):
            query = item.get("query")
            if not isinstance(query, str):
                continue
        else:
            continue
        cleaned = " ".join(query.split()).strip()
        if not cleaned:
            continue
        cleaned = cleaned[:_MAX_QUERY_CHARS]
        if cleaned not in queries:
            queries.append(cleaned)
    if not queries:
        raise QueryRewriteError("rewrite response contains no usable queries")
    return queries[:variants]


class GeminiQueryRewriter:
    """Synchronous Gemini wrapper producing rewrite variants for a query.

    One of two transports of the same stage since ADR 0009; the prompt
    (`build_rewrite_instruction` / `build_rewrite_user_content`) and the
    parser (`parse_rewrite_response`) are shared with the other one, so a
    provider switch cannot change what is asked or what is accepted. Pick the
    configured one with `build_query_rewriter()`.
    """

    def __init__(
        self,
        # Resolved at import: AI_SCRIPTURE_REWRITE_API_KEY when the deployment
        # bills this stage separately, GEMINI_API_KEY otherwise (config.
        # resolve_rewrite_api_key). Production creation points go through
        # `build_query_rewriter`, which passes the same value explicitly from
        # the stage configuration, so the key is chosen in exactly one place.
        api_key: str = REWRITE_API_KEY,
        model: str = AI_SCRIPTURE_REWRITE_MODEL,
        http_client: httpx.Client | None = None,
        variants: int = REWRITE_VARIANTS,
        timeout: float = 20.0,
        attempts: int = 3,
    ):
        self.api_key = api_key
        self.model = model
        self.variants = variants
        # Serve-time callers lower both (ADR 0006): the endpoint's budget,
        # not the retry ladder, must bound the request.
        self.timeout = timeout
        self.attempts = max(1, attempts)
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=httpx.Timeout(timeout))

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def _pause_before_retry(
        self, deadline: Deadline | None, attempt: int, rate_limit=None
    ) -> bool:
        """Wait out the backoff; False means "do not attempt again".

        False on the last attempt (no pointless sleep before giving up), on
        an exhausted daily quota, and whenever the pause plus a usable call
        no longer fit in the budget — the caller then degrades immediately
        instead of sleeping the request over its deadline.
        """
        if attempt + 1 >= self.attempts:
            return False
        pause = retry_pause(
            deadline, _RETRY_BASE_SECONDS * (attempt + 1), rate_limit
        )
        if pause is None:
            return False
        time.sleep(pause)
        return True

    def __enter__(self) -> "GeminiQueryRewriter":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def rewrite(
        self,
        language: str,
        topic: str,
        user_replies: list[str],
        deadline: Deadline | None = None,
    ) -> list[str]:
        """Return rewrite variants for the prayer context.

        Raises QueryRewriteError on configuration/transport/parse failure —
        never logs the prayer context itself. With a `deadline`, no attempt
        is started once the budget is gone, every HTTP call is capped by what
        is left of it (across all four httpx phases,
        `gemini_retry.provider_timeout`), and a backoff is only slept when
        the attempt after it still fits. A 429 naming an exhausted DAILY
        quota ends the ladder at once: it cannot reopen inside one request,
        so retrieval falls back to the raw query immediately instead of
        waiting for it (ClickUp 86cbbnaxn).
        """
        if language not in _LANGUAGES:
            raise QueryRewriteError(f"unsupported language: {language}")
        if not self.api_key:
            raise QueryRewriteError(
                "rewrite API key is not configured "
                "(AI_SCRIPTURE_REWRITE_API_KEY or GEMINI_API_KEY)"
            )
        if not _MODEL_PATTERN.fullmatch(self.model):
            raise QueryRewriteError("rewrite model name contains invalid characters")

        payload = {
            "system_instruction": {
                "parts": [{"text": build_rewrite_instruction(language, self.variants)}]
            },
            "contents": [{
                "role": "user",
                "parts": [{"text": build_rewrite_user_content(topic, user_replies)}],
            }],
            "generationConfig": {
                # Leave room for reasoning tokens: on "thinking" Gemini
                # models a small cap is consumed by the hidden reasoning and
                # the visible answer comes back empty.
                "maxOutputTokens": 8192,
                # 0.0 keeps rewrites (and therefore retrieval) reproducible;
                # measured as stable as sampled runs' best on the benchmark.
                "temperature": 0.0,
                "responseMimeType": "application/json",
            },
        }
        url = GEMINI_GENERATE_URL.format(model=self.model)
        data = None
        last_error: Exception | None = None
        for attempt in range(self.attempts):
            timeout = provider_timeout(deadline, self.timeout)
            if timeout is None:
                raise QueryRewriteError("rewrite budget exhausted") from last_error
            try:
                response = self._client.post(
                    url,
                    json=payload,
                    headers={"x-goog-api-key": self.api_key},
                    timeout=timeout,
                )
                if response.status_code in RETRYABLE_STATUS:
                    last_error = QueryRewriteError(
                        f"rewrite request failed (HTTP {response.status_code})"
                    )
                    if not self._pause_before_retry(
                        deadline, attempt, rate_limit_of(response)
                    ):
                        break
                    continue
                response.raise_for_status()
                data = response.json()
                break
            except httpx.TimeoutException as exc:
                last_error = exc
                if not self._pause_before_retry(deadline, attempt):
                    break
            except (httpx.HTTPError, ValueError) as exc:
                raise QueryRewriteError("rewrite request failed") from exc
        if data is None:
            raise QueryRewriteError(
                "rewrite request failed after retries"
            ) from last_error

        try:
            parts = data["candidates"][0]["content"]["parts"]
            text = "".join(
                part.get("text", "") for part in parts if isinstance(part, dict)
            )
        except (KeyError, IndexError, TypeError) as exc:
            raise QueryRewriteError("rewrite response has no candidates") from exc
        return parse_rewrite_response(text, self.variants)


class OpenAICompatQueryRewriter:
    """The same rewrite stage over an OpenAI-compatible chat endpoint.

    A duck of `GeminiQueryRewriter`: same method, same arguments, same
    exception type, so `ScriptureRetriever` cannot tell them apart. The
    instruction, the user content and the parser are the production
    functions above — only the transport differs (`llm_client.ChatClient`,
    which carries the shared retry/budget policy of `gemini_retry`).

    The model name is NOT checked against `_MODEL_PATTERN` here: on Gemini it
    is interpolated into the request URL and the pattern keeps it from
    escaping the path, while here it travels as the `model` field of a JSON
    body, where slashes and colons are ordinary characters of real model ids
    (`Qwen/Qwen3-30B-A3B`).
    """

    def __init__(
        self,
        stage: StageProvider = SCRIPTURE_REWRITE_PROVIDER,
        http_client: httpx.Client | None = None,
        variants: int = REWRITE_VARIANTS,
        timeout: float = 20.0,
        attempts: int = 3,
    ):
        self.stage = stage
        self.model = stage.model
        self.variants = variants
        self.timeout = timeout
        self.attempts = max(1, attempts)
        self._chat = ChatClient(
            stage.endpoint,
            stage.api_key,
            stage.model,
            http_client=http_client,
            timeout=timeout,
            attempts=self.attempts,
        )

    def close(self) -> None:
        self._chat.close()

    def __enter__(self) -> "OpenAICompatQueryRewriter":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def rewrite(
        self,
        language: str,
        topic: str,
        user_replies: list[str],
        deadline: Deadline | None = None,
    ) -> list[str]:
        """Rewrite variants for the prayer context (see GeminiQueryRewriter).

        Same contract, including the privacy one: `QueryRewriteError` carries
        a failure category only — never the prayer context, the answer or the
        key.
        """
        if language not in _LANGUAGES:
            raise QueryRewriteError(f"unsupported language: {language}")
        instruction = build_rewrite_instruction(language, self.variants)
        user_content = build_rewrite_user_content(topic, user_replies)
        try:
            text = self._chat.complete(
                instruction,
                user_content,
                deadline=deadline,
                json_object=True,
                temperature=0.0,
            )
        except LLMError as exc:
            raise QueryRewriteError(f"rewrite failed: {exc}") from None
        return parse_rewrite_response(text, self.variants)


def build_query_rewriter(
    stage: StageProvider = SCRIPTURE_REWRITE_PROVIDER,
    http_client: httpx.Client | None = None,
    variants: int = REWRITE_VARIANTS,
    timeout: float = 20.0,
    attempts: int = 3,
):
    """The rewriter this deployment configured (ADR 0009).

    The one place that maps `AI_SCRIPTURE_REWRITE_PROVIDER` onto a class, so
    every caller — the endpoint, the CLI — gets the same answer. An unknown
    provider cannot reach here: `config._validate` refuses it at start-up,
    and an unset one means "AI is not configured", where the Gemini client's
    own "not configured" error is the documented behaviour.
    """
    if stage.is_openai_compat:
        return OpenAICompatQueryRewriter(
            stage,
            http_client=http_client,
            variants=variants,
            timeout=timeout,
            attempts=attempts,
        )
    return GeminiQueryRewriter(
        api_key=stage.api_key,
        model=stage.model,
        http_client=http_client,
        variants=variants,
        timeout=timeout,
        attempts=attempts,
    )
