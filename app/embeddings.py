"""
Embedding clients for the scripture-selection RAG index.

Two providers, chosen by `EMBEDDING_PROVIDER` through
`build_embedding_client()` (ADR 0010):

- `gemini` — `GeminiEmbeddingClient`, the API of
  architect/adr/0002-embedding-model-and-vector-store.md;
- `local` — `LocalEmbeddingClient`, BAAI/bge-m3 on CPU in this process
  through sentence-transformers, weights from a read-only volume.

Both expose the same three things — `embed_documents(texts)`,
`embed_query(text, deadline=None)` and the context-manager/`close()` pair —
and both signal every failure as `EmbeddingUnavailable`, so no caller knows
which one it holds.

Gemini-specific properties:

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

import logging
import math
import threading
import time
from dataclasses import dataclass

import httpx

from config import (
    EMBEDDING_DIMENSIONS,
    EMBEDDING_MODEL,
    EMBEDDING_MODEL_PATH,
    EMBEDDING_PROVIDER,
    EMBEDDING_PROVIDER_LOCAL,
    GEMINI_API_KEY,
)
from deadline import Deadline
from gemini_retry import (
    RETRYABLE_STATUS,
    provider_timeout,
    rate_limit_of,
    retry_pause,
)

logger = logging.getLogger(__name__)

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


# ---------------------------------------------------------------------------
# Local embeddings: BAAI/bge-m3 on CPU (ClickUp 86cbegg2r, ADR 0010)
# ---------------------------------------------------------------------------

# The window and batch of the measurement (ClickUp 86cbe4n7e), as code
# constants rather than environment knobs: they are properties of this model
# on a CPU host, not of a deployment. bge-m3 advertises 8192 tokens, and a
# batch of 16 of those allocates gigabytes of activations — while the longest
# chunk of this corpus is far below 512 tokens, so the cap costs nothing and
# the pair is what the published corpus pass (11 960 chunks) actually ran
# with; its memory peak is in ADR 0010, which is the one place that number
# is maintained.
LOCAL_MAX_SEQ_LENGTH = 512
LOCAL_DOCUMENT_BATCH_SIZE = 4

_model_lock = threading.Lock()
_model = None
# Serialises `encode`. Module-level and not per client on purpose: the
# weights are ONE process-wide object (`_model`), so two clients built
# around it — a second `build_embedding_client()` anywhere, a warm-up path,
# a future in-process rebuild — must queue on the same lock or they would
# run concurrent encodes on the same model after all.
_encode_lock = threading.Lock()


def load_embedding_model(
    path: str = EMBEDDING_MODEL_PATH,
    dimensions: int = EMBEDDING_DIMENSIONS,
    max_seq_length: int = LOCAL_MAX_SEQ_LENGTH,
):
    """The process-wide sentence-transformers model, loaded exactly once.

    2.3 GB of fp32 weights: a second copy does not fit on either the local
    machine or the production VM, so every client shares this one. The load
    is idempotent and thread-safe; callers that want the cost paid at
    start-up rather than inside the first request call it from there (see
    `app/main.py`).

    The directory is `EMBEDDING_MODEL_PATH`, a read-only volume — never a
    hub id, so nothing can be downloaded on a machine that has no route to
    the internet (the image also sets HF_HUB_OFFLINE=1).

    Verified on load: the model's output width must equal
    `EMBEDDING_DIMENSIONS`. That pair is half of the stored index version,
    so a directory holding another model would otherwise write vectors of
    the wrong space under the right version string — silently, and
    irreversibly for the rows it overwrote.
    """
    global _model
    with _model_lock:
        if _model is not None:
            return _model
        if not path:
            raise EmbeddingUnavailable(
                "EMBEDDING_MODEL_PATH is not configured", provider_down=True
            )
        # Imported here, not at module import time: torch costs ~1 s and
        # ~200 MB, and a deployment on `gemini` must not pay either.
        from sentence_transformers import SentenceTransformer

        started = time.time()
        try:
            model = SentenceTransformer(path, device="cpu")
        except Exception as exc:
            raise EmbeddingUnavailable(
                f"cannot load the embedding model from {path!r}: "
                f"{type(exc).__name__}",
                provider_down=True,
            ) from exc
        model.max_seq_length = min(model.max_seq_length, max_seq_length)
        # sentence-transformers 6 renamed this; the old name still works but
        # warns, and the warning would be printed once per rebuild.
        measure = getattr(
            model, "get_embedding_dimension", None
        ) or model.get_sentence_embedding_dimension
        width = measure()
        if int(width) != int(dimensions):
            raise EmbeddingUnavailable(
                f"the model at {path!r} produces {width}-dimension vectors, "
                f"but EMBEDDING_DIMENSIONS is {dimensions} — the index "
                f"version would name a space these vectors are not in",
                provider_down=True,
            )
        logger.info(
            "Local embedding model loaded from %s (%s dims, max_seq_length=%s)"
            " in %.1f s",
            path, width, model.max_seq_length, time.time() - started,
        )
        _model = model
        return _model


class LocalEmbeddingClient:
    """`GeminiEmbeddingClient`'s interface, served by bge-m3 in this process.

    Same three entry points, same `EmbeddingUnavailable` contract, same unit
    vectors — `normalize_embeddings=True` is what `vector_index` assumes
    everywhere (cosine similarity is a plain dot product over stored rows),
    so it is not optional here.

    `encode` is serialised by a PROCESS-WIDE lock (`_encode_lock`), because
    the weights it runs on are process-wide too. torch already parallelises
    one encode across the cores, so six concurrent CPU encodes on an 8-core
    box that also runs MySQL would oversubscribe it and change nothing about
    the result; the retrieval pipeline therefore runs with `embed_workers=1`
    on this provider (ADR 0010). The lock is what makes that safe rather than
    merely intended: any caller that still hands this client — or a second
    one built around the same model — to a thread pool gets correct vectors,
    just no speed-up.

    No timeout and no retry ladder: there is no network and no quota. A
    failed encode is a broken process, not a transient provider — hence
    `provider_down=True`, which makes the retrieval pipeline stop after the
    first failed variant instead of retrying five more.
    """

    def __init__(
        self,
        config: EmbeddingConfig | None = None,
        model=None,
        batch_size: int = LOCAL_DOCUMENT_BATCH_SIZE,
    ):
        self.config = config or EmbeddingConfig()
        self._model = model if model is not None else load_embedding_model()
        self._encode_lock = _encode_lock
        self.batch_size = max(1, batch_size)

    def close(self) -> None:
        """No-op: the weights are process-wide and outlive every client."""

    def __enter__(self) -> "LocalEmbeddingClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return self._encode(texts, self.batch_size)

    def embed_query(
        self, text: str, deadline: Deadline | None = None
    ) -> list[float]:
        # A local encode cannot be interrupted half-way, so the budget is
        # checked before it starts — the same answer the Gemini client gives
        # when `provider_timeout` returns None.
        if deadline is not None and deadline.expired():
            raise EmbeddingUnavailable(
                "embedding budget exhausted", provider_down=True
            )
        return self._encode([text], 1)[0]

    def _encode(self, texts: list[str], batch_size: int) -> list[list[float]]:
        if not texts:
            return []
        # An empty string is a degenerate input for a tokenizer; a single
        # space keeps result positions aligned with the input, exactly as the
        # Gemini client does for the same reason.
        prepared = [text if text.strip() else " " for text in texts]
        try:
            with self._encode_lock:
                vectors = self._model.encode(
                    prepared,
                    batch_size=batch_size,
                    show_progress_bar=False,
                    normalize_embeddings=True,
                    convert_to_numpy=True,
                )
        except Exception as exc:
            # Type only: the text is a query rewritten from the prayer
            # context, and an exception message can quote its input (same
            # policy as the Gemini client and query_rewrite).
            raise EmbeddingUnavailable(
                f"local embedding failed: {type(exc).__name__}",
                provider_down=True,
            ) from exc
        if vectors.shape[1] != self.config.dimensions:
            raise EmbeddingUnavailable(
                f"unexpected embedding size: {vectors.shape[1]}"
            )
        return [row.astype(float).tolist() for row in vectors]


def build_embedding_client(
    provider: str = EMBEDDING_PROVIDER,
    timeout: float = 60.0,
    max_retries: int = _MAX_RETRIES,
    **kwargs,
):
    """The embedding client this deployment configured (ADR 0010).

    The one place that maps `EMBEDDING_PROVIDER` onto a class, so the
    endpoint, both CLIs and the tests all get the same answer. An unknown
    value cannot reach here — `config._validate` refuses it at start-up.

    `timeout` and `max_retries` describe a network call and are ignored by
    the local client, which has neither; they stay in the signature so the
    two call sites that tune the Gemini ladder (serve-time budgets vs the
    patient offline CLI) do not have to know which provider they got.
    """
    if provider == EMBEDDING_PROVIDER_LOCAL:
        return LocalEmbeddingClient(**kwargs)
    return GeminiEmbeddingClient(
        timeout=timeout, max_retries=max_retries, **kwargs
    )
