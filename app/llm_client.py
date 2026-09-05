"""
OpenAI-compatible chat-completions transport (ClickUp 86cbegg2f, ADR 0009).

The three chat-shaped AI stages — the guiding question
(`POST /api/ai/question`), the retrieval query rewrite and the grounded
passage rerank — used to speak Gemini's `:generateContent` protocol inline,
one hand-rolled copy per module. This module is the OTHER transport: one
OpenAI-compatible `/chat/completions` client the same three stages can use,
chosen per stage by `AI_*_PROVIDER` (see `app/config.py`).

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

Privacy and key hygiene (same policy as the Gemini stages): the prayer text
and the answer are never logged or embedded in an error, and neither is the
API key. Transport failures are reported by category (exception type, HTTP
status) and never by quoting the request URL, which is why
`config.validate_endpoint` also refuses an endpoint carrying credentials or a
query string — a key belongs in `AI_*_API_KEY`, never in a URL that an
exception could print.
"""

from __future__ import annotations

import asyncio
import re
import time
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
# for every stage (evaluation/gen_rewrites.py and the verified trace-stand
# adapters do exactly this).
_THINK_BLOCK = re.compile(r"<think>.*?</think>", flags=re.DOTALL | re.IGNORECASE)

# Output ceiling of one call. 1024 and not the 8192 the Gemini stages ask
# for: those are "thinking" models whose hidden reasoning is charged against
# the same cap, while the instruct-tuned local models need ~250 tokens for
# six rewrite queries — and on a server with context shifting an unreachable
# ceiling turns a degenerate repetition into an endless one
# (evaluation/gen_rewrites.DEFAULT_MAX_TOKENS says the same).
DEFAULT_MAX_TOKENS = 1024

# Linear backoff of the retry ladder, identical to the Gemini stages: 2 s
# before the second attempt, 4 s before the third — and only when the budget
# can still afford the attempt that follows.
_RETRY_BASE_SECONDS = 2.0


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

    An empty `AI_OPENAI_COMPAT_API_KEY` is an explicit statement ("this
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
    Gemini stages and `evaluation/gen_rewrites.py` follow.
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
        timeout: float = 20.0,
        attempts: int = 3,
        max_tokens: int = DEFAULT_MAX_TOKENS,
    ):
        self.endpoint = endpoint
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.attempts = max(1, attempts)
        self.max_tokens = max_tokens

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
        http_client: httpx.Client | None = None,
        timeout: float = 20.0,
        attempts: int = 3,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        sleep=time.sleep,
    ):
        super().__init__(endpoint, api_key, model, timeout, attempts, max_tokens)
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
        timeout: float = 20.0,
        attempts: int = 3,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        sleep=asyncio.sleep,
    ):
        super().__init__(endpoint, api_key, model, timeout, attempts, max_tokens)
        # Injectable for the same reason `ChatClient` takes one: a test of the
        # retry ladder must not spend the backoff in real seconds.
        self._sleep = sleep

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
                    response = await client.post(url, json=payload, headers=headers)
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
