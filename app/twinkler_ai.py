import base64
import logging
import re
from typing import Any, Literal

import httpx
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel, ConfigDict, Field, model_validator

from auth import RequireAPIKey
from client_ip import resolve_client_ip
from config import (
    GEMINI_API_KEY,
    AI_QUESTION_MODEL,
    AI_QUESTION_TIMEOUT_SECONDS,
    AI_TRANSCRIBE_MODEL,
    AI_REQUESTS_PER_CLIENT_PER_MINUTE,
    AI_REQUESTS_PER_MINUTE,
    QUESTION_PROVIDER,
)
from llm_client import AsyncChatClient, LLMError
from question_prompt import build_question_prompt, build_user_message
from rate_limit import RateLimiter, RateLimitError
from safety import (
    DEFAULT_LANGUAGE,
    NO_MATCH,
    SAFETY_REPLY_VERSION,
    check_input,
    check_reply,
    detect_language,
    safety_reply,
)

router = APIRouter()
MODEL_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")
LOCALE_PATTERN = re.compile(r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$")
logger = logging.getLogger(__name__)
# Inline data is base64-expanded inside Gemini's 20 MB request limit.
_MAX_AUDIO_BYTES = 14 * 1024 * 1024
_AUDIO_MIME_TYPES = frozenset({"audio/mp4", "audio/x-m4a", "audio/m4a"})
_M4A_FALLBACK_MIME_TYPES = frozenset({"", "application/octet-stream"})
_TRANSCRIPTION_PROMPT = (
    "Transcribe the speech verbatim in its original language. Preserve "
    "code-switching. Do not translate, summarize, add, omit, explain, or "
    "rewrite anything. Add only natural punctuation. Return only the transcript."
)
_limiter = RateLimiter(name="AI")
# Historical module-level names, kept as aliases of the very same deques so
# diagnostics and tests can inspect the limiter state directly.
_request_times = _limiter.request_times
_client_request_times = _limiter.client_request_times


# The client's own limits, mirrored so a request it would not have sent is
# refused here too (ClickUp 86cbegmzz, ADR-0019 on the app side). 16 000 is
# counted in UTF-16 units there and in code points here: the two agree for
# everything but astral characters (emoji), where this bound is the looser of
# the two — deliberately, since the tighter side is the one that decides what
# is ever sent.
MAX_TOPIC_LENGTH = 2000
MAX_MESSAGES = 40
MAX_TOTAL_LENGTH = 16000
_LEGACY_FIELDS = ("user", "last_user_message")


class QuestionMessage(BaseModel):
    """One turn of the prayer conversation."""

    model_config = ConfigDict(extra="forbid")

    role: Literal["assistant", "user"] = Field(
        description=(
            "Who said it: `assistant` is a question the companion asked, "
            "`user` is the person's own answer"
        ),
    )
    text: str = Field(
        min_length=1,
        max_length=MAX_TOTAL_LENGTH,
        description=(
            "The turn verbatim. One answer may be several lines: the client "
            "joins the typed text and every transcription of the same turn "
            "with newlines before sending them as one `user` element"
        ),
    )


class CompleteRequest(BaseModel):
    """The structured request of `POST /api/ai/question` (ClickUp 86cbegmzz).

    Replaced the single `user` string on 2026-09-05: the client used to
    assemble the stage instructions itself, so the server could not tell the
    person's own words from the wrapper around them — and the despair rule and
    the language detector both need exactly that distinction. There is no
    transitional support for the old field; both ends changed at once, the app
    is unpublished, and a request carrying `user` is answered with a 422 that
    says so.
    """

    model_config = ConfigDict(extra="forbid")

    topic: str = Field(
        max_length=MAX_TOPIC_LENGTH,
        description="What the person is praying about; empty when they named nothing",
    )
    stage: Literal["first", "next", "reflect"] = Field(
        description=(
            "`first` — the opening question (always with an empty history), "
            "`next` — the following question given the conversation so far, "
            "`reflect` — the closing question that helps name one takeaway"
        ),
    )
    messages: list[QuestionMessage] = Field(
        max_length=MAX_MESSAGES,
        description=(
            "The conversation so far, chronologically: skipped questions and "
            "empty answers are omitted, so it may start with a `user` turn "
            "(an over-long history is trimmed from the front) and it must end "
            "with one. Empty for `first`, and normal for the other two stages "
            "when the person answered nothing or forbade sending the answers"
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _name_the_removed_field(cls, data: Any) -> Any:
        """A helpful 422 for the pre-86cbegmzz body, not a bare "extra input"."""
        if isinstance(data, dict):
            for field in _LEGACY_FIELDS:
                if field in data:
                    raise ValueError(
                        f"the '{field}' field was removed on 2026-09-05: send "
                        "topic (may be empty), stage (first|next|reflect) and "
                        "messages ([{role: assistant|user, text}], empty for "
                        "first) instead"
                    )
        return data

    @model_validator(mode="after")
    def _check_history_matches_the_stage(self) -> "CompleteRequest":
        if self.stage == "first" and self.messages:
            raise ValueError(
                "stage 'first' is the opening question and takes no history: "
                "send messages: [] (use stage 'next' to continue a conversation)"
            )
        if self.messages and self.messages[-1].role != "user":
            raise ValueError(
                "a non-empty history must end with a 'user' turn: the question "
                "is asked about what the person said last"
            )
        total = len(self.topic) + sum(len(message.text) for message in self.messages)
        if total > MAX_TOTAL_LENGTH:
            raise ValueError(
                f"topic and messages together must not exceed "
                f"{MAX_TOTAL_LENGTH} characters (got {total})"
            )
        return self

    def turns(self) -> list[tuple[str, str]]:
        """`(role, text)` pairs — what `question_prompt.build_user_message` takes."""
        return [(message.role, message.text) for message in self.messages]

    def last_text(self, role: str) -> str | None:
        """The most recent turn of `role`, or `None` when there is none."""
        for message in reversed(self.messages):
            if message.role == role:
                return message.text
        return None


class CompleteResponse(BaseModel):
    text: str = Field(description="Text generated by the AI companion")


class ErrorResponse(BaseModel):
    detail: str = Field(description="Public error message")


class AIError(RuntimeError):
    """One AI call of this module failed, whichever provider served it."""


# The historical name, kept as an alias: it is the exception both handlers
# catch and the one tests raise. Since ADR 0009 the question stage may run on
# an OpenAI-compatible endpoint, so the *class* is provider-independent while
# the name stays what every caller already imports.
GeminiError = AIError


def question_prompt_for(text: str) -> str:
    """The system prompt to answer `text` with (prompt v2, ClickUp 86cbegg3f).

    One seam, on purpose: the prompt needs a *language*, and the language
    comes from whatever text the caller considers the person's own words.
    Since the request became structured (ClickUp 86cbegmzz) that text is the
    **last `user` turn**, chosen by `language_source`; this function stays a
    pure function of the string it is handed and `build_question_prompt` never
    sees the request shape at all.

    An **empty** text is the one case the detector cannot speak for: its
    `None` means "this message does not say which language it is", and the
    prompt turns that into v1's "answer in exactly the language of the
    person's message" — a sentence that points at nothing when there is no
    message (a `next`/`reflect` request with an empty topic and no history:
    legal, and asking for a generic question of that stage). English is the
    documented default there, the same one `safety.safety_reply` falls back
    to, so it is named rather than left to the model.
    """
    if not text.strip():
        return build_question_prompt(DEFAULT_LANGUAGE)
    return build_question_prompt(detect_language(text))


def person_language_candidates(request: CompleteRequest) -> list[str]:
    """Everything the person wrote, best evidence for the language first.

    The last `user` turn, then the topic, then their earlier replies newest
    first. Only their own words: an assistant question already in the
    conversation is *our* text and must never vote (an English answer to a
    Russian question is a language switch the person made, and the prompt
    honours it). Blank texts are dropped, so the list is empty exactly when
    the person contributed nothing.
    """
    replies = [
        message.text for message in request.messages if message.role == "user"
    ]
    ordered = (
        replies[-1:] + [request.topic] + list(reversed(replies[:-1]))
    )
    return [text for text in ordered if text.strip()]


def language_source(request: CompleteRequest) -> str:
    """The text whose language the answer must be written in.

    The candidates above are walked by **decidability**, not merely by
    presence (ClickUp 86cbegmzz, review of this ticket). `detect_language`
    answers `None` for a message that does not say — a short Cyrillic line
    carrying none of the four letters and none of the function words that
    separate Russian from Ukrainian, «Помоги» being the canonical one — and
    stopping at the first *non-empty* candidate threw away evidence the same
    person had already written in the same request. The prompt then got v2's
    "answer in exactly the language of the person's message", the wording v2
    exists to avoid: Qwen3-30B broke it in 6 answers out of 81. Measured on
    the evaluation set (`evaluation/gen_questions.py --dry-run`): 9 of 33
    inputs undetermined before, **6** after, and the topic alone recovered
    none of them — the evidence for `ru-001`/`ru-002`/`ru-005` is in an
    earlier reply, which is why the walk does not stop at the topic.

    When no candidate is decidable the newest one is returned anyway, so the
    prompt still gets that honest "the detector has no evidence" sentence
    rather than a guess. Only when the person wrote nothing at all does the
    last `assistant` question answer — it is at least the language the
    conversation has been happening in — and an empty string, meaning English,
    when there is not even that.

    Still one *text*, not a language: `question_prompt_for` stays a pure
    function of the string it is handed, both transports are handed the same
    one, and `evaluation/gen_questions.py` mirrors this selection (pinned
    against it by `tests/test_gen_questions.py`).
    """
    candidates = person_language_candidates(request)
    for text in candidates:
        if detect_language(text) is not None:
            return text
    if candidates:
        return candidates[0]
    return request.last_text("assistant") or ""


def safety_input_text(request: CompleteRequest) -> str | None:
    """What BOTH tiers of the despair rule read, or `None` when there is nothing.

    The person's **last reply**, which is the whole point of the structured
    request: a despair phrase two turns back has already been answered (by
    the fixed reply, or by a real question tier 2 let stand), and re-matching
    it forever would end the conversation the first time it happened (the bug
    that opened this ticket). Tier 2 used to read the topic plus every reply
    instead, precisely so an older phrase kept refusing a question-shaped
    answer — Maria reversed that on 2026-09-05: the request is split into
    turns exactly so this rule looks at the last one, and the companion must
    not keep answering the fixed text for the rest of the prayer.

    For `first` there is no reply yet and the topic is what the person typed,
    so the topic is checked. For `next`/`reflect` with an empty history there
    is no reply either — and the topic is deliberately **not** substituted:
    the person said nothing new, so nothing new can be found in it, and using
    the topic there would fire the fixed reply on every question of a prayer
    whose topic once carried the phrase.

    Tier 2's fixed reply still needs a *language*, and that is resolved
    separately by the caller through `language_source` — not by matching this
    same text a second time — because the language the matched phrase happens
    to be in is not necessarily the language the rest of the prayer is in.
    """
    last_user = request.last_text("user")
    if last_user is not None:
        return last_user
    if request.stage == "first":
        return request.topic
    return None


def _reserve_rate_limit(client_key: str) -> None:
    """Book one Twinkler AI slot; limits are read at call time."""
    _limiter.reserve(
        client_key,
        AI_REQUESTS_PER_MINUTE,
        AI_REQUESTS_PER_CLIENT_PER_MINUTE,
    )


async def _enforce_rate_limit(client_key: str) -> None:
    try:
        _reserve_rate_limit(client_key)
    except RateLimitError as error:
        if error.retry_after is not None:
            raise HTTPException(
                status_code=429,
                detail="AI request limit exceeded",
                headers={"Retry-After": str(error.retry_after)},
            ) from error
        logger.warning("Twinkler rate limiter unavailable: %s", error)
        raise HTTPException(
            status_code=503,
            detail="AI service temporarily unavailable",
        ) from error


def _extract_text(data: Any) -> str:
    try:
        parts = data["candidates"][0]["content"]["parts"]
    except (KeyError, IndexError, TypeError):
        return ""
    if not isinstance(parts, list):
        return ""
    return "".join(
        part.get("text", "")
        for part in parts
        if isinstance(part, dict) and isinstance(part.get("text"), str)
    ).strip()


async def _complete_openai_compat(user: str, prompt: str) -> str:
    """The question stage on an OpenAI-compatible endpoint (ADR 0009).

    Same prompt (built once by the caller, so both transports send the same
    bytes), same generation settings (temperature
    0.7, 1024 output tokens) and the same public failure — `AIError`, which
    the handler turns into `502 AI service unavailable` without provider
    detail. `json_object` is deliberately off: this answer is prose for a
    person, not a parsed contract.

    One attempt, like the Gemini path: this endpoint has no request budget to
    plan a retry ladder inside, and a second call would double the worst-case
    latency a person waits with no message on screen.
    """
    client = AsyncChatClient(
        QUESTION_PROVIDER.endpoint,
        QUESTION_PROVIDER.api_key,
        QUESTION_PROVIDER.model,
        timeout=AI_QUESTION_TIMEOUT_SECONDS,
        attempts=1,
    )
    try:
        text = await client.complete(
            prompt, user, json_object=False, temperature=0.7
        )
    except LLMError as error:
        raise AIError(f"question failed: {error}") from None
    if not text:
        raise AIError("AI returned no text")
    return text


async def complete(user: str, language_source_text: str | None = None) -> str:
    """Answer one user message, or raise AIError (the caller's 502).

    `user` is the message sent to the model — since ClickUp 86cbegmzz the
    text `question_prompt.build_user_message` assembled from the request.
    `language_source_text` is the text whose language the prompt names; it is
    a *different* string now, because the assembled message is mostly Russian
    stage instructions whatever language the person prays in. `None` means
    "the message itself", which is what a single-string caller (the parity
    tests, a one-off script) means by it.

    "AI is not configured" is decided by exactly two variables since
    2026-08-30, and the prompt is no longer one of them:
    `GEMINI_API_KEY` unset -> GeminiError here -> 502, and
    `AI_CLIENT_HMAC_KEY` unset -> the limiter fails closed before this
    function is reached -> 503 (see `_enforce_rate_limit`). A malformed
    `AI_QUESTION_MODEL` is also a 502, but is unreachable in practice: with
    a key set, an unnamed model aborts startup (ADR 0008).

    Since 2026-08-30 the system prompt is code
    (`app/question_prompt.py`), so the two guards this function used
    to carry — "prompt is not configured" and "prompt is too long" — are
    gone: neither can be true of a reviewed literal, and keeping them would
    have pretended a code change could arrive at runtime. What remains
    configurable is exactly what the deployment supplies: the provider (ADR
    0009), the key and the model name.

    Since prompt v2 (2026-09-05, ClickUp 86cbegg3f) the prompt names the
    language to answer in, resolved by `safety.detect_language` — the same
    detector the despair rule runs on the same message, never a second one.
    It is resolved ONCE, through `question_prompt_for`, and handed to
    whichever transport answers, so the two providers keep sending identical
    bytes.
    """
    prompt = question_prompt_for(
        user if language_source_text is None else language_source_text
    )
    if QUESTION_PROVIDER.is_openai_compat:
        return await _complete_openai_compat(user, prompt)
    if not GEMINI_API_KEY:
        raise GeminiError("GEMINI_API_KEY is not configured")
    if not MODEL_PATTERN.fullmatch(AI_QUESTION_MODEL):
        raise GeminiError("AI_QUESTION_MODEL contains invalid characters")

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{AI_QUESTION_MODEL}:generateContent"
    )
    payload = {
        "system_instruction": {"parts": [{"text": prompt}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": {
            "maxOutputTokens": 1024,
            "temperature": 0.7,
        },
    }

    try:
        # The same knob the openai_compat branch honours
        # (`AI_QUESTION_TIMEOUT_SECONDS`, default 20.0 = the literal this
        # replaces): a variable documented as "the ceiling of the question
        # endpoint's single call" must not be silently ignored on one of the
        # two providers. Still a bare number here, i.e. per httpx PHASE, which
        # is exactly what this call has always done — carving it the way
        # `gemini_retry.provider_timeout` does would be a behaviour change to
        # the Gemini path, and this endpoint has no request budget to carve.
        async with httpx.AsyncClient(timeout=AI_QUESTION_TIMEOUT_SECONDS) as client:
            response = await client.post(
                url,
                headers={"x-goog-api-key": GEMINI_API_KEY},
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
    except httpx.TimeoutException as error:
        raise GeminiError("Gemini request timed out") from error
    except (httpx.HTTPError, ValueError) as error:
        raise GeminiError("Gemini request failed") from error

    text = _extract_text(data)
    if not text:
        raise GeminiError("Gemini returned no text")
    return text


def _audio_mime_type(file: UploadFile) -> str:
    content_type = (file.content_type or "").split(";", 1)[0].strip().lower()
    if content_type in _AUDIO_MIME_TYPES:
        return content_type

    filename = file.filename or ""
    if filename.lower().endswith(".m4a") and content_type in _M4A_FALLBACK_MIME_TYPES:
        return "audio/mp4"

    raise HTTPException(status_code=415, detail="Unsupported audio format")


def _transcription_prompt(locale: str | None) -> str:
    if locale is None:
        return _TRANSCRIPTION_PROMPT
    return (
        f"{_TRANSCRIPTION_PROMPT} The app locale is {locale}; use it only as a "
        "weak hint when the spoken language is ambiguous."
    )


async def transcribe(audio: bytes, mime_type: str, locale: str | None) -> str:
    """Transcribe one recording with Gemini, or raise AIError (a 502).

    Deliberately Gemini-only, with no `AI_TRANSCRIBE_PROVIDER` to switch
    (config refuses that variable): speech is the one stage the
    OpenAI-compatible chat protocol does not cover, and it moves to a local
    speech model in its own step. So a deployment whose three CHAT stages run
    on another provider still needs `GEMINI_API_KEY` for this endpoint — and
    without one it answers the same explicit 502 it always has, while
    everything else keeps working.
    """
    if not GEMINI_API_KEY:
        raise GeminiError("GEMINI_API_KEY is not configured")
    if not MODEL_PATTERN.fullmatch(AI_TRANSCRIBE_MODEL):
        raise GeminiError("AI_TRANSCRIBE_MODEL contains invalid characters")

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{AI_TRANSCRIBE_MODEL}:generateContent"
    )
    payload = {
        "contents": [{
            "role": "user",
            "parts": [
                {"text": _transcription_prompt(locale)},
                {
                    "inline_data": {
                        "mime_type": mime_type,
                        "data": base64.b64encode(audio).decode("ascii"),
                    }
                },
            ],
        }],
        "generationConfig": {
            "maxOutputTokens": 4096,
            "temperature": 0,
        },
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                url,
                headers={"x-goog-api-key": GEMINI_API_KEY},
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
    except httpx.TimeoutException as error:
        raise GeminiError("Gemini transcription timed out") from error
    except (httpx.HTTPError, ValueError) as error:
        raise GeminiError("Gemini transcription failed") from error

    text = _extract_text(data)
    if not text:
        raise GeminiError("Gemini returned no transcript")
    return text


@router.post(
    "/ai/question",
    response_model=CompleteResponse,
    operation_id="ai_question",
    tags=["AI"],
    summary="Generate an AI companion response",
    description=(
        "Asks one leading question about the prayer described by `topic`, "
        "`stage` and the conversation so far. The instructions for the stage "
        "and the system prompt are built on the server and the provider API "
        "key is never exposed to the client. The person's last reply is what "
        "the question is asked about: a reply showing despair or self-harm is "
        "answered with a fixed supportive text in the same language and no "
        "model is called. `422` also covers the two shape rules: `first` "
        "takes no history, and a non-empty history must end with a `user` "
        "turn."
    ),
    responses={
        403: {"model": ErrorResponse, "description": "Invalid or missing API key"},
        429: {
            "model": ErrorResponse,
            "description": "Global or per-client request limit exceeded",
            "headers": {
                "Retry-After": {
                    "description": "Seconds until another request can be attempted",
                    "schema": {"type": "integer", "minimum": 1},
                }
            },
        },
        # Public wording only: the exact server-side conditions behind these
        # two codes are documented in architect/twinkler-ai.md, not in a spec
        # the client reads — naming server variables there would leak
        # deployment detail for no consumer benefit.
        502: {
            "model": ErrorResponse,
            "description": "AI is not configured or the provider request failed",
        },
        503: {"model": ErrorResponse, "description": "Rate limiter unavailable"},
    },
)
async def twinkler_complete(
    request: CompleteRequest,
    http_request: Request,
    api_key: bool = RequireAPIKey,
) -> CompleteResponse:
    client_key = resolve_client_ip(http_request)
    await _enforce_rate_limit(client_key)

    # The despair rule is code, not an instruction a provider may or may not
    # follow (app/safety.py, ClickUp 86cbegg23). Tier 1 answers here and the
    # model is never called; the reservation above is deliberately kept — the
    # client got a reply, and the quota must not depend on what was in it.
    # It reads the LAST reply only (`safety_input_text`): before this ticket
    # it read the whole conversation, so one despair phrase turned every
    # further question of that prayer into the fixed reply.
    checked = safety_input_text(request)
    finding = check_input(checked) if checked is not None else NO_MATCH
    if finding.matched:
        logger.warning(
            "Safety rule fired on the request: tier=%d pattern=%s language=%s "
            "reply_version=%d stage=%s",
            finding.tier,
            finding.pattern_id,
            finding.language,
            SAFETY_REPLY_VERSION,
            request.stage,
        )
        return CompleteResponse(text=safety_reply(finding.language))

    user_message = build_user_message(
        request.topic, request.stage, request.turns()
    )
    try:
        text = await complete(user_message, language_source(request))
    except GeminiError as error:
        # Log the failure category, but never the prayer text or provider key.
        logger.warning("Twinkler AI request failed: %s", error)
        raise HTTPException(status_code=502, detail="AI service unavailable") from error

    # Tier 2: the LAST reply carried a weaker despair signal and the model
    # answered it with a question anyway. Replace the answer, keep the fact.
    # Same text as tier 1 (`checked`) since 2026-09-05 (Maria): an OLDER
    # despair phrase is someone else's turn now, already answered one way or
    # another, and must not keep replacing every later question of the
    # prayer — only the person's last word does that.
    guard = check_reply(checked or "", text)
    if guard.matched:
        # The reply's language is not the matched phrase's language: it is
        # whatever `language_source`/the prompt already resolved for this
        # request, so the fixed reply speaks the prayer's language even when
        # the despair phrase itself was undecidable on its own.
        reply_language = detect_language(language_source(request))
        logger.warning(
            "Safety rule fired on the model reply: tier=%d pattern=%s "
            "language=%s reply_version=%d stage=%s",
            guard.tier,
            guard.pattern_id,
            reply_language,
            SAFETY_REPLY_VERSION,
            request.stage,
        )
        text = safety_reply(reply_language)
    return CompleteResponse(text=text)


@router.post(
    "/ai/transcribe",
    response_model=CompleteResponse,
    operation_id="ai_transcribe",
    tags=["AI"],
    summary="Transcribe a voice recording",
    description=(
        "Transcribes an M4A recording in its original language. The optional "
        "locale is used only as a weak language hint."
    ),
    responses={
        403: {"model": ErrorResponse, "description": "Invalid or missing API key"},
        413: {"model": ErrorResponse, "description": "Audio file is too large"},
        415: {"model": ErrorResponse, "description": "Unsupported audio format"},
        429: {
            "model": ErrorResponse,
            "description": "Global or per-client request limit exceeded",
            "headers": {
                "Retry-After": {
                    "description": "Seconds until another request can be attempted",
                    "schema": {"type": "integer", "minimum": 1},
                }
            },
        },
        502: {"model": ErrorResponse, "description": "Gemini request failed"},
        503: {"model": ErrorResponse, "description": "Rate limiter unavailable"},
    },
)
async def twinkler_transcribe(
    http_request: Request,
    file: UploadFile = File(..., description="M4A voice recording"),
    locale: str | None = Form(
        default=None,
        max_length=35,
        description="Optional BCP 47 app locale used only as a weak hint",
    ),
    api_key: bool = RequireAPIKey,
) -> CompleteResponse:
    if locale is not None and not LOCALE_PATTERN.fullmatch(locale):
        raise HTTPException(status_code=422, detail="Invalid locale")

    try:
        mime_type = _audio_mime_type(file)
        audio = await file.read(_MAX_AUDIO_BYTES + 1)
    finally:
        await file.close()

    if not audio:
        raise HTTPException(status_code=422, detail="Audio file is empty")
    if len(audio) > _MAX_AUDIO_BYTES:
        raise HTTPException(status_code=413, detail="Audio file is too large")

    client_key = resolve_client_ip(http_request)
    await _enforce_rate_limit(client_key)

    try:
        text = await transcribe(audio, mime_type, locale)
    except GeminiError as error:
        # Keep this static: provider errors must never leak recording metadata.
        logger.warning("Twinkler transcription request failed")
        raise HTTPException(status_code=502, detail="AI service unavailable") from error
    return CompleteResponse(text=text)
