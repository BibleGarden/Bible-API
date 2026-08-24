"""
LLM query reformulation for scripture-selection retrieval.

Why this exists (architect/adr/0004-retrieval-pipeline.md): the dominant
failure mode of raw embedding search is the register gap between everyday
prayer wording and biblical language (ADR 0002 diagnostic probe: a raw query
ranked the reference passage ~356th, a scripture-styled rephrasing of the
same intent ranked it 4th). Before searching, the prayer context is rewritten
by Gemini into several short queries in the biblical register of the target
language, each covering a different spiritual angle of the situation; the
retrieval layer then searches with every variant and fuses the results.

Privacy: the prayer context goes to Gemini (pre-cleared: it already goes
there for the Twinkler companion). Neither the prayer text nor the API key
must ever be logged — callers log only failure categories.

The prompt is generic: it knows nothing about the evaluation dataset and
never receives reference answers.
"""

from __future__ import annotations

import json
import logging
import re
import time

import httpx

from config import GEMINI_API_KEY, RETRIEVAL_REWRITE_MODEL

logger = logging.getLogger(__name__)

# Bump on any change of the prompt wording or output contract; benchmark
# caches are keyed by (model, prompt version).
REWRITE_PROMPT_VERSION = 7

# 6 variants + interleave fusion is the benchmark-approved configuration
# (fewer variants lose recall, more dilute the fused top-10 — ADR 0004).
REWRITE_VARIANTS = 6
_MAX_QUERY_CHARS = 200
_MODEL_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")

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


class QueryRewriteError(RuntimeError):
    """The rewrite backend is not configured, unreachable or returned junk."""


def build_search_query(topic: str, user_replies: list[str]) -> str:
    """The raw retrieval query: prayer topic + allowed user replies."""
    parts = [topic.strip()] + [r.strip() for r in user_replies]
    return "\n".join(p for p in parts if p)


def build_rewrite_instruction(language: str, variants: int = REWRITE_VARIANTS) -> str:
    """System instruction for the rewrite call (static per language)."""
    language_name, register_hint = _LANGUAGES[language]
    return f"""You prepare search queries for a Bible passage retrieval system inside a prayer app.

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


def build_rewrite_user_content(topic: str, user_replies: list[str]) -> str:
    lines = [f"Topic: {topic.strip()}"]
    replies = [r.strip() for r in user_replies if r.strip()]
    if replies:
        lines.append("Remarks:")
        lines.extend(f"- {reply}" for reply in replies)
    return "\n".join(lines)


def parse_rewrite_response(text: str, variants: int = REWRITE_VARIANTS) -> list[str]:
    """Parse the model output into a clean list of query strings.

    Tolerates markdown fences and surrounding prose; deduplicates, drops
    empties and reference-like leftovers, truncates overlong strings.
    Raises QueryRewriteError when nothing usable remains.
    """
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
    for item in raw:
        if not isinstance(item, str):
            continue
        cleaned = " ".join(item.split()).strip()
        if not cleaned:
            continue
        cleaned = cleaned[:_MAX_QUERY_CHARS]
        if cleaned not in queries:
            queries.append(cleaned)
    if not queries:
        raise QueryRewriteError("rewrite response contains no usable queries")
    return queries[:variants]


class GeminiQueryRewriter:
    """Synchronous Gemini wrapper producing rewrite variants for a query."""

    def __init__(
        self,
        api_key: str = GEMINI_API_KEY,
        model: str = RETRIEVAL_REWRITE_MODEL,
        http_client: httpx.Client | None = None,
        variants: int = REWRITE_VARIANTS,
    ):
        self.api_key = api_key
        self.model = model
        self.variants = variants
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=httpx.Timeout(20.0))

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "GeminiQueryRewriter":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def rewrite(self, language: str, topic: str, user_replies: list[str]) -> list[str]:
        """Return rewrite variants for the prayer context.

        Raises QueryRewriteError on configuration/transport/parse failure —
        never logs the prayer context itself.
        """
        if language not in _LANGUAGES:
            raise QueryRewriteError(f"unsupported language: {language}")
        if not self.api_key:
            raise QueryRewriteError("GEMINI_API_KEY is not configured")
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
        for attempt in range(3):
            if attempt:
                time.sleep(2.0 * attempt)
            try:
                response = self._client.post(
                    url, json=payload, headers={"x-goog-api-key": self.api_key}
                )
                if response.status_code in (429, 500, 502, 503, 504):
                    last_error = QueryRewriteError(
                        f"rewrite request failed (HTTP {response.status_code})"
                    )
                    continue
                response.raise_for_status()
                data = response.json()
                break
            except httpx.TimeoutException as exc:
                last_error = exc
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
