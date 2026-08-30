"""
Embedding client for the scripture-selection RAG index.

Uses the Gemini embedding API (gemini-embedding-001) — the model selected in
architect/adr/0002-embedding-model-and-vector-store.md after benchmarking
local sentence-transformers alternatives on evaluation/scenarios.json.

Key properties:

- Asymmetric retrieval task types: documents are embedded with
  RETRIEVAL_DOCUMENT, queries with RETRIEVAL_QUERY.
- Dimensionality is truncated server-side (Matryoshka) via
  outputDimensionality and re-normalised client-side, because Gemini only
  returns unit-length vectors at the full 3072 dims.
- One embedContent call per text. The batchEmbedContents endpoint is
  deliberately NOT used: on the free tier it is quota-throttled to a crawl
  (~12 chunks/min observed), while sequential single calls sustain
  ~100 chunks/min. Indexing is an offline CLI job, so simple sequential
  calls with retries are sufficient and resume-safe (the reindex skips
  already-stored chunks).
- Exponential backoff on 429/5xx/transport errors, honouring the
  server-provided RetryInfo delay when present (app/gemini_retry.py). Under
  a serve-time `Deadline` the ladder stops the moment a pause plus the
  attempt after it no longer fit in the budget, and a 429 naming an
  exhausted DAILY quota stops it outright — a daily free-tier quota cannot
  reopen inside one request, and its RetryInfo used to be slept off against
  the caller's whole remaining budget (ClickUp 86cbbnaxn).

Failure mode: raises EmbeddingUnavailable when the API is not configured or
keeps failing after retries. Callers (the future selection endpoint) must
degrade gracefully — the stored index in MySQL stays intact; only query
embedding needs the API at serve time.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass

import httpx

from config import (
    EMBEDDING_DIMENSIONS,
    EMBEDDING_MODEL,
    GEMINI_API_KEY,
)
from deadline import Deadline
from gemini_retry import (
    RETRYABLE_STATUS,
    provider_timeout,
    rate_limit_of,
    retry_pause,
)

GEMINI_EMBED_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "{model}:embedContent"
)

TASK_DOCUMENT = "RETRIEVAL_DOCUMENT"
TASK_QUERY = "RETRIEVAL_QUERY"

_MAX_RETRIES = 6
_RETRY_BASE_SECONDS = 1.0
_RETRY_MAX_SECONDS = 30.0
_RETRYABLE_STATUS = RETRYABLE_STATUS


class EmbeddingUnavailable(RuntimeError):
    """The embedding backend is not configured or not reachable.

    provider_down=True marks failures that would equally hit any other
    request right now (no API key, retries exhausted on 429/5xx/transport
    errors): callers embedding several texts fail fast instead of burning
    the full retry budget per text (retrieval m2). provider_down=False
    marks request-specific failures (non-retryable HTTP status, malformed
    response body) — other texts may still succeed.
    """

    def __init__(self, message: str, provider_down: bool = False):
        super().__init__(message)
        self.provider_down = provider_down


def normalize(vector: list[float]) -> list[float]:
    """Return the unit-length version of a vector (list in, list out)."""
    norm = math.sqrt(sum(x * x for x in vector))
    if norm == 0.0:
        return list(vector)
    return [x / norm for x in vector]


@dataclass(frozen=True)
class EmbeddingConfig:
    model: str = EMBEDDING_MODEL
    dimensions: int = EMBEDDING_DIMENSIONS
    api_key: str = GEMINI_API_KEY


class GeminiEmbeddingClient:
    """Thin synchronous wrapper over the Gemini embedding REST API."""

    def __init__(
        self,
        config: EmbeddingConfig | None = None,
        http_client: httpx.Client | None = None,
        sleep=time.sleep,
        timeout: float = 60.0,
        max_retries: int = _MAX_RETRIES,
    ):
        self.config = config or EmbeddingConfig()
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=httpx.Timeout(timeout))
        self._sleep = sleep
        # The offline indexing CLI wants a long, patient ladder; the
        # serve-time endpoint lowers both (ADR 0006) so a request cannot
        # outlive its budget.
        self.timeout = timeout
        self.max_retries = max(1, max_retries)

    def close(self) -> None:
        """Close the underlying HTTP client (only if this instance owns it)."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "GeminiEmbeddingClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_one(text, TASK_DOCUMENT) for text in texts]

    def embed_query(
        self, text: str, deadline: Deadline | None = None
    ) -> list[float]:
        return self._embed_one(text, TASK_QUERY, deadline)

    def _embed_one(
        self, text: str, task_type: str, deadline: Deadline | None = None
    ) -> list[float]:
        if not self.config.api_key:
            raise EmbeddingUnavailable(
                "GEMINI_API_KEY is not configured", provider_down=True
            )
        url = GEMINI_EMBED_URL.format(model=self.config.model)
        body = {
            # The API rejects empty content; a single space is a harmless
            # stand-in that keeps result positions aligned with the input.
            "content": {"parts": [{"text": text if text.strip() else " "}]},
            "taskType": task_type,
            "outputDimensionality": self.config.dimensions,
        }
        last_error = "unknown error"
        provider_down = False
        for attempt in range(self.max_retries):
            last_attempt = attempt + 1 == self.max_retries
            backoff = min(
                _RETRY_BASE_SECONDS * (2 ** attempt), _RETRY_MAX_SECONDS
            )
            timeout = provider_timeout(deadline, self.timeout)
            if timeout is None:
                raise EmbeddingUnavailable(
                    "embedding budget exhausted", provider_down=True
                )
            try:
                response = self._client.post(
                    url,
                    json=body,
                    headers={"x-goog-api-key": self.config.api_key},
                    timeout=timeout,
                )
            except httpx.HTTPError as exc:
                # Type only: an httpx error message can quote the request
                # URL and body, and on the serve path the body is a query
                # rewritten from the prayer context (same policy as
                # query_rewrite / passage_rerank).
                last_error = f"transport error: {type(exc).__name__}"
                provider_down = True
            else:
                if response.status_code == 200:
                    # An HTTP 200 with a broken body must surface as
                    # EmbeddingUnavailable, not json.JSONDecodeError, so
                    # retrieval degrades to its fallback (retrieval m3).
                    try:
                        payload = response.json()
                    except ValueError as exc:
                        raise EmbeddingUnavailable(
                            "invalid JSON in embedding response"
                        ) from exc
                    embedding = (
                        payload.get("embedding") if isinstance(payload, dict)
                        else None
                    )
                    values = (
                        embedding.get("values") if isinstance(embedding, dict)
                        else None
                    )
                    if not values or len(values) != self.config.dimensions:
                        raise EmbeddingUnavailable(
                            "unexpected embedding size: "
                            f"{0 if not values else len(values)}"
                        )
                    return normalize(values)
                # Status only — a provider error body can echo the request
                # (i.e. a query derived from the prayer context) and this
                # message is logged by retrieval.
                last_error = f"HTTP {response.status_code}"
                provider_down = response.status_code in _RETRYABLE_STATUS
                if response.status_code not in _RETRYABLE_STATUS or last_attempt:
                    break
                # None = retrying is pointless (daily quota) or no longer
                # affordable (the pause plus the call would outlive the
                # budget): give up NOW so the caller can degrade in time.
                pause = retry_pause(deadline, backoff, rate_limit_of(response))
                if pause is None:
                    break
                self._sleep(pause)
                continue
            if last_attempt:  # no pointless backoff before giving up
                break
            pause = retry_pause(deadline, backoff)
            if pause is None:
                break
            self._sleep(pause)
        raise EmbeddingUnavailable(
            f"Gemini embedding failed: {last_error}", provider_down=provider_down
        )
