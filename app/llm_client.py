"""
OpenAI-compatible chat-completions transport (ClickUp 86cbegg2f, ADR 0009).

The three chat-shaped AI stages — the guiding question
(`POST /api/ai/question`), the retrieval query rewrite and the grounded
passage rerank — used to speak Gemini's `:generateContent` protocol inline,
one hand-rolled copy per module. This module is the OTHER transport: one
OpenAI-compatible `/chat/completions` client the same three stages can use,
chosen per stage by `AI_*_PROVIDER` (see `app/config.py`). The generic
`openai_compat` profile sends the stage's validated reasoning effort as before.
The explicit `openrouter` profile is question-only and adds the fixed privacy,
routing and reasoning-off objects required by ADR 0022. It is selected by the
provider value, never inferred from the endpoint hostname. The question-only
`together` profile sends reasoning.enabled=false without OpenRouter policies.

What is deliberately NOT here: prompts, parsers and validation. A stage's
instruction, its user content and the checks applied to the answer stay in
the stage module (`query_rewrite`, `passage_rerank`, `question_prompt`) and
are shared by both providers byte for byte — this step is transport only, so
a provider switch cannot move a prompt version or loosen a validation rule.

Retry discipline is NOT re-invented either: it is `app/gemini_retry.py`, the
policy the Gemini stages already share (ClickUp 86cbbnaxn). Nothing in it is
Gemini-specific except the reader of a 429 quota body, which simply reports
"details unknown" for a provider that answers a different shape — and an
unknown quota keeps the ordinary backoff, which is the correct default. So:

- `provider_timeout()` carves ONE call's budget across httpx's four timeout
  phases (a bare number would authorise four times the budget);
- `retry_pause()` refuses to sleep unless the attempt after it still fits in
  the request's `Deadline`;
- `RETRYABLE_STATUS` is the same set of statuses.

Two client classes because the stages differ in colour, not in protocol:
`ChatClient` is synchronous (rewrite and rerank run inside a thread, like
their Gemini counterparts) and `AsyncChatClient` is awaited by the FastAPI
handler of `/api/ai/question`. Everything they share — URL, headers, payload,
answer extraction, error wording — is module-level functions above them.

Privacy and key hygiene (same policy as the Gemini stages): by default the
prayer text and the answer are never logged or embedded in an error. The
question endpoint can explicitly pass a diagnostic logger in a controlled
local/test environment; that logger receives request and response bodies but
never headers, endpoint data or the API key. Transport failures are reported
by category (exception type, HTTP status) and never by quoting the request
URL, which is why `config.validate_endpoint` also refuses an endpoint carrying
credentials or a query string — a key belongs in `AI_*_API_KEY`, never in a
URL that an exception could print.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from urllib.parse import urlsplit

import httpx

from deadline import Deadline
from gemini_retry import (
    RETRYABLE_STATUS,
    provider_timeout,
    rate_limit_of,
    retry_pause,
)

# Some models emit a reasoning block before the answer. Both production
# parsers extract JSON greedily (`\{.*\}` with DOTALL), so a brace inside the
# reasoning would swallow the real object; and for the question endpoint the
# block would simply be shown to the person. Strip it in the transport, once,
# for every stage (AI-Evaluation/evaluation/gen_rewrites.py and the verified trace-stand
# adapters do exactly this).
_THINK_BLOCK = re.compile(r"<think>.*?</think>", flags=re.DOTALL | re.IGNORECASE)

# Default output ceiling for chat stages that do not pass their own operational
# value. Rewrite and rerank retain 1024; question passes
# `AI_QUESTION_MAX_TOKENS` explicitly because a reasoning model charges hidden
# reasoning against the same allowance. On a server with context shifting an
# unreachable ceiling can turn a degenerate repetition into an endless one
# (AI-Evaluation/evaluation/gen_rewrites.DEFAULT_MAX_TOKENS says the same).
DEFAULT_MAX_TOKENS = 1024

# Linear backoff of the retry ladder, identical to the Gemini stages: 2 s
# before the second attempt, 4 s before the third — and only when the budget
# can still afford the attempt that follows.
_RETRY_BASE_SECONDS = 2.0

# Kept literal here as a transport guard as well as in config validation: the
# client is also used by tests and tooling that can construct it directly.
REASONING_EFFORTS = ("omit", "none", "low", "medium", "high")
REQUEST_PROFILE_OPENAI_COMPAT = "openai_compat"
REQUEST_PROFILE_OPENROUTER = "openrouter"
REQUEST_PROFILE_TOGETHER = "together"
REQUEST_PROFILES = (
    REQUEST_PROFILE_OPENAI_COMPAT,
    REQUEST_PROFILE_OPENROUTER,
    REQUEST_PROFILE_TOGETHER,
)
OPENROUTER_PROVIDER_ENDPOINT = "crusoe/bf16"

# Fixed in code on purpose. Letting an environment-provided JSON object reach
# this field would make the effective privacy and routing policy invisible to
# startup validation and review.
OPENROUTER_PROVIDER_POLICY = {
    "allow_fallbacks": False,
    "data_collection": "deny",
}
OPENROUTER_REASONING_POLICY = {"enabled": False}


def log_diagnostic_request(
    logger: logging.Logger,
    provider: str,
    payload: dict,
    attempt: int,
    api_key: str,
) -> str:
    """Log a content-bearing request body without headers or endpoint data."""
    call_id = uuid.uuid4().hex
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    logger.info(
        "AI question provider request: call_id=%s provider=%s attempt=%d payload=%s",
        call_id,
        provider,
        attempt,
        _redact_api_key(body, api_key),
    )
    return call_id


def log_diagnostic_response(
    logger: logging.Logger,
    provider: str,
    response: httpx.Response,
    attempt: int,
    api_key: str,
    call_id: str,
) -> None:
    """Log the raw response body with API-key redaction, never its headers."""
    body = _redact_api_key(response.text, api_key)
    logger.info(
        "AI question provider response: call_id=%s provider=%s attempt=%d "
        "status=%d body=%s",
        call_id,
        provider,
        attempt,
        response.status_code,
        json.dumps(body, ensure_ascii=False),
    )


def _redact_api_key(body: str, api_key: str) -> str:
    if not api_key:
        return body
    return body.replace(api_key, "[REDACTED_API_KEY]")


class LLMError(RuntimeError):
    """The chat backend is not configured, unreachable or returned junk.

    Stage modules catch this and re-raise their own error type, so a caller
    of `rewrite()` / `choose()` / `complete()` sees the same exception
    whichever provider served it.
    """


def strip_think(text: str) -> str:
    """Drop `<think>...</think>` blocks and surrounding whitespace."""
    return _THINK_BLOCK.sub("", text).strip()


def completions_url(endpoint: str) -> str:
    """`https://host/v1` -> `https://host/v1/chat/completions`.

    An endpoint that already names the method is left alone, so both spellings
    can be configured (the vLLM/Ollama convention is the base `/v1`).
    """
    base = endpoint.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    return f"{base}/chat/completions"


def endpoint_host(endpoint: str) -> str:
    """Host of an endpoint — never its path or query, for logs and tests."""
    return urlsplit(endpoint).hostname or ""


def auth_headers(api_key: str) -> dict[str, str]:
    """Bearer header, or none at all when the endpoint needs no key.

    An empty stage-specific `AI_*_API_KEY` is an explicit statement ("this
    endpoint is unauthenticated"), validated as such in config; sending
    `Authorization: Bearer ` would be a different, wrong request.
    """
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def build_payload(
    model: str,
    instruction: str,
    user_content: str,
    *,
    temperature: float,
    max_tokens: int,
    json_object: bool,
    reasoning_effort: str,
    request_profile: str = REQUEST_PROFILE_OPENAI_COMPAT,
    openrouter_provider_endpoint: str | None = None,
) -> dict:
    """The chat-completions body: system instruction + one user message.

    `json_object` asks for OpenAI's `response_format` — the counterpart of
    Gemini's `responseMimeType: application/json`. There is no counterpart of
    `responseSchema` in this protocol, which is why the rerank contract is
    carried by the prompt and by the server-side parser (which was the
    load-bearing half all along — the server never trusted the schema).
    """
    payload: dict = {
        "model": model,
        "messages": [
            {"role": "system", "content": instruction},
            {"role": "user", "content": user_content},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if json_object:
        payload["response_format"] = {"type": "json_object"}
    if reasoning_effort not in REASONING_EFFORTS:
        raise ValueError(
            "reasoning_effort must be one of " + ", ".join(REASONING_EFFORTS)
        )
    if request_profile not in REQUEST_PROFILES:
        raise ValueError(
            "request_profile must be one of " + ", ".join(REQUEST_PROFILES)
        )
    if request_profile == REQUEST_PROFILE_OPENROUTER:
        if reasoning_effort != "none":
            raise ValueError(
                "the openrouter request profile requires reasoning_effort=none"
            )
        if openrouter_provider_endpoint != OPENROUTER_PROVIDER_ENDPOINT:
            raise ValueError(
                "the openrouter request profile requires provider endpoint "
                f"{OPENROUTER_PROVIDER_ENDPOINT!r}"
            )
        payload["provider"] = {
            **OPENROUTER_PROVIDER_POLICY,
            "only": [openrouter_provider_endpoint],
        }
        payload["reasoning"] = dict(OPENROUTER_REASONING_POLICY)
    else:
        if openrouter_provider_endpoint is not None:
            raise ValueError(
                "openrouter_provider_endpoint belongs only to the openrouter "
                "request profile"
            )
        if request_profile == REQUEST_PROFILE_TOGETHER:
            if reasoning_effort != "none":
                raise ValueError(
                    "the together request profile requires reasoning_effort=none"
                )
            payload["reasoning"] = {"enabled": False}
        elif reasoning_effort != "omit":
            payload["reasoning_effort"] = reasoning_effort
    return payload


def content_of(data) -> str:
    """The assistant message of a chat-completions response, `<think>` gone.

    Raises LLMError for every shape that is not one usable answer — the
    caller degrades exactly as it does when Gemini answers without
    candidates.
    """
    try:
        message = data["choices"][0]["message"]
    except (KeyError, IndexError, TypeError) as exc:
        raise LLMError("response has no choices") from exc
    text = message.get("content") if isinstance(message, dict) else None
    if not isinstance(text, str) or not text.strip():
        raise LLMError("response content is empty")
    stripped = strip_think(text)
    if not stripped:
        raise LLMError("response content is empty")
    return stripped


def transport_error(exc: Exception) -> str:
    """Failure CATEGORY of a transport exception, never its message.

    An httpx error message quotes the request URL, and a rewrite/rerank
    request body is derived from the prayer context — the same policy the
    Gemini stages and `AI-Evaluation/evaluation/gen_rewrites.py` follow.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTPStatusError (HTTP {exc.response.status_code})"
    return type(exc).__name__


class _ChatBase:
    """Configuration and the retry plan shared by both client colours."""

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        model: str,
        reasoning_effort: str,
        timeout: float = 20.0,
        attempts: int = 3,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        request_profile: str = REQUEST_PROFILE_OPENAI_COMPAT,
        openrouter_provider_endpoint: str | None = None,
    ):
        self.endpoint = endpoint
        self.api_key = api_key
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout = timeout
        self.attempts = max(1, attempts)
        self.max_tokens = max_tokens
        self.request_profile = request_profile
        self.openrouter_provider_endpoint = openrouter_provider_endpoint

    def _check_configured(self) -> None:
        """Refuse to build a request out of an unconfigured stage.

        Unreachable in a started service — `config._validate` aborts the
        start when a stage on openai_compat has no endpoint or model — but a
        CLI or a test that bypasses config must fail loudly rather than post
        to `/chat/completions` with an empty model.
        """
        if not self.endpoint:
            raise LLMError("chat endpoint is not configured")
        if not self.model:
            raise LLMError("chat model is not configured")
        if self.reasoning_effort not in REASONING_EFFORTS:
            raise LLMError("chat reasoning effort is not configured")
        if self.request_profile not in REQUEST_PROFILES:
            raise LLMError("chat request profile is not configured")
        if self.request_profile in (
            REQUEST_PROFILE_OPENROUTER, REQUEST_PROFILE_TOGETHER
        ):
            if not self.api_key.strip():
                raise LLMError(f"{self.request_profile} API key is not configured")
            if self.reasoning_effort != "none":
                raise LLMError(f"{self.request_profile} reasoning must be disabled")
        if self.request_profile == REQUEST_PROFILE_OPENROUTER:
            if self.openrouter_provider_endpoint != OPENROUTER_PROVIDER_ENDPOINT:
                raise LLMError("openrouter provider endpoint is not configured")
        elif self.openrouter_provider_endpoint is not None:
            raise LLMError(
                "openrouter provider endpoint is set for another request profile"
            )

    def _request(
        self,
        instruction: str,
        user_content: str,
        *,
        json_object: bool,
        temperature: float,
    ) -> tuple[str, dict, dict]:
        self._check_configured()
        payload = build_payload(
            self.model,
            instruction,
            user_content,
            temperature=temperature,
            max_tokens=self.max_tokens,
            json_object=json_object,
            reasoning_effort=self.reasoning_effort,
            request_profile=self.request_profile,
            openrouter_provider_endpoint=self.openrouter_provider_endpoint,
        )
        return completions_url(self.endpoint), payload, auth_headers(self.api_key)

    def _plan_pause(
        self, deadline: Deadline | None, attempt: int, rate_limit=None
    ) -> float | None:
        """Seconds to wait before the next attempt, or None: give up now.

        None on the last attempt (no pointless sleep before failing), on a
        quota that cannot reopen inside this request, and whenever the pause
        plus a usable call no longer fit in the budget.
        """
        if attempt + 1 >= self.attempts:
            return None
        return retry_pause(
            deadline, _RETRY_BASE_SECONDS * (attempt + 1), rate_limit
        )


class ChatClient(_ChatBase):
    """Synchronous OpenAI-compatible chat completions for one stage.

    Same shape as `GeminiQueryRewriter`/`GeminiPassageReranker` deliberately:
    an optional injected `httpx.Client` (shared across requests by
    `scripture_select`, replaced by `httpx.MockTransport` in tests), a
    per-call timeout carved out of the request budget, and a bounded retry
    ladder.
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        model: str,
        reasoning_effort: str,
        http_client: httpx.Client | None = None,
        timeout: float = 20.0,
        attempts: int = 3,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        sleep=time.sleep,
        request_profile: str = REQUEST_PROFILE_OPENAI_COMPAT,
        openrouter_provider_endpoint: str | None = None,
    ):
        super().__init__(
            endpoint,
            api_key,
            model,
            reasoning_effort,
            timeout,
            attempts,
            max_tokens,
            request_profile,
            openrouter_provider_endpoint,
        )
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=httpx.Timeout(timeout))
        self._sleep = sleep

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "ChatClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def complete(
        self,
        instruction: str,
        user_content: str,
        deadline: Deadline | None = None,
        json_object: bool = True,
        temperature: float = 0.0,
    ) -> str:
        """One answer, or LLMError. Never logs or quotes the content."""
        url, payload, headers = self._request(
            instruction,
            user_content,
            json_object=json_object,
            temperature=temperature,
        )
        data = None
        last_error: Exception | None = None
        for attempt in range(self.attempts):
            timeout = provider_timeout(deadline, self.timeout)
            if timeout is None:
                raise LLMError("chat budget exhausted") from last_error
            try:
                response = self._client.post(
                    url, json=payload, headers=headers, timeout=timeout
                )
                if response.status_code in RETRYABLE_STATUS:
                    last_error = LLMError(
                        f"chat request failed (HTTP {response.status_code})"
                    )
                    pause = self._plan_pause(
                        deadline, attempt, rate_limit_of(response)
                    )
                    if pause is None:
                        break
                    self._sleep(pause)
                    continue
                response.raise_for_status()
                data = response.json()
                break
            except httpx.TimeoutException as exc:
                last_error = exc
                pause = self._plan_pause(deadline, attempt)
                if pause is None:
                    break
                self._sleep(pause)
            except (httpx.HTTPError, ValueError) as exc:
                # `from None`, unlike the Gemini stages' `from exc`: an httpx
                # exception carries the request URL in its message, and this
                # one is the only place a stage-specific endpoint (a value an
                # operator may have pasted a key into despite the config
                # check) could reach an error string. The category is kept.
                raise LLMError(
                    f"chat request failed: {transport_error(exc)}"
                ) from None
        if data is None:
            raise LLMError("chat request failed after retries") from last_error
        return content_of(data)


class AsyncChatClient(_ChatBase):
    """The same protocol, awaited — for the FastAPI question handler.

    A client per call (`async with httpx.AsyncClient(...)`), exactly as
    `twinkler_ai` has always created one for Gemini: the question endpoint
    makes one provider call per request and holds no shared pool.
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        model: str,
        reasoning_effort: str,
        timeout: float = 20.0,
        attempts: int = 3,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        sleep=asyncio.sleep,
        diagnostic_logger: logging.Logger | None = None,
        request_profile: str = REQUEST_PROFILE_OPENAI_COMPAT,
        openrouter_provider_endpoint: str | None = None,
    ):
        super().__init__(
            endpoint,
            api_key,
            model,
            reasoning_effort,
            timeout,
            attempts,
            max_tokens,
            request_profile,
            openrouter_provider_endpoint,
        )
        # Injectable for the same reason `ChatClient` takes one: a test of the
        # retry ladder must not spend the backoff in real seconds.
        self._sleep = sleep
        self._diagnostic_logger = diagnostic_logger

    async def complete(
        self,
        instruction: str,
        user_content: str,
        deadline: Deadline | None = None,
        json_object: bool = False,
        temperature: float = 0.0,
    ) -> str:
        url, payload, headers = self._request(
            instruction,
            user_content,
            json_object=json_object,
            temperature=temperature,
        )
        data = None
        last_error: Exception | None = None
        for attempt in range(self.attempts):
            timeout = provider_timeout(deadline, self.timeout)
            if timeout is None:
                raise LLMError("chat budget exhausted") from last_error
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    diagnostic_call_id = ""
                    if self._diagnostic_logger is not None:
                        diagnostic_call_id = log_diagnostic_request(
                            self._diagnostic_logger,
                            self.request_profile,
                            payload,
                            attempt + 1,
                            self.api_key,
                        )
                    response = await client.post(url, json=payload, headers=headers)
                    if self._diagnostic_logger is not None:
                        log_diagnostic_response(
                            self._diagnostic_logger,
                            self.request_profile,
                            response,
                            attempt + 1,
                            self.api_key,
                            diagnostic_call_id,
                        )
                    if response.status_code in RETRYABLE_STATUS:
                        last_error = LLMError(
                            f"chat request failed (HTTP {response.status_code})"
                        )
                        pause = self._plan_pause(
                            deadline, attempt, rate_limit_of(response)
                        )
                        if pause is None:
                            break
                        await self._sleep(pause)
                        continue
                    response.raise_for_status()
                    data = response.json()
                    break
            except httpx.TimeoutException as exc:
                last_error = exc
                pause = self._plan_pause(deadline, attempt)
                if pause is None:
                    break
                await self._sleep(pause)
            except (httpx.HTTPError, ValueError) as exc:
                # `from None`, unlike the Gemini stages' `from exc`: an httpx
                # exception carries the request URL in its message, and this
                # one is the only place a stage-specific endpoint (a value an
                # operator may have pasted a key into despite the config
                # check) could reach an error string. The category is kept.
                raise LLMError(
                    f"chat request failed: {transport_error(exc)}"
                ) from None
        if data is None:
            raise LLMError("chat request failed after retries") from last_error
        return content_of(data)
