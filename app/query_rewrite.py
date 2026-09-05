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
from llm_client import ChatClient, LLMError
from prompt_safety import neutralize_prompt_markers

logger = logging.getLogger(__name__)

# Bump on any change of the prompt wording or output contract; benchmark
# caches are keyed by (model, prompt version).
REWRITE_PROMPT_VERSION = 7

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
