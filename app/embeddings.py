"""
Embedding clients for the scripture-selection RAG index.

Three providers, chosen by `EMBEDDING_PROVIDER` through
`build_embedding_client()` (ADR 0010, ADR 0014):

- `gemini` — `GeminiEmbeddingClient`, the API of
  architect/adr/0002-embedding-model-and-vector-store.md;
- `local` — `LocalEmbeddingClient`, BAAI/bge-m3 on CPU in this process
  through sentence-transformers, weights from a read-only volume;
- `openai_compat` — `RemoteEmbeddingClient`, the SAME bge-m3 on the company
  server's CPU, over `POST {endpoint}/embeddings`. The production provider
  since 2026-09-05: identical vectors (verified, ADR 0014), and this process
  holds no weights at all.

All three expose the same three things — `embed_documents(texts)`,
`embed_query(text, deadline=None)` and the context-manager/`close()` pair —
and all three signal every failure as `EmbeddingUnavailable`, so no caller
knows which one it holds.

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
    EMBEDDING_PROVIDER_OPENAI_COMPAT,
    EMBEDDING_STAGE,
    GEMINI_API_KEY,
)
from deadline import Deadline
from gemini_retry import (
    RETRYABLE_STATUS,
    provider_timeout,
    rate_limit_of,
    retry_pause,
)
from llm_client import transport_error

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
# batch of 16 of those allocates gigabytes of activations; the pair is what
# the published corpus pass (11 960 chunks) actually ran with, and its memory
# peak is in ADR 0010, which is the one place that number is maintained.
#
# ADR 0010 claimed the cap costs nothing because "the longest chunk of this
# corpus is far below 512 tokens". Measured against the bge-m3 tokenizer
# while building the remote provider (ClickUp 86cbehd6h): that is wrong —
# **811 of the 11 960 indexed chunks (6.8%) are longer**, up to 1168 tokens.
# So the stored index truncates those, and the remote provider (whose server
# applies its own, larger window) answers a different — fuller — vector for
# exactly those chunks. It is the whole of the difference between the two
# providers: cut a long chunk at this window and the cosine against the
# stored row is 1.000000. Queries are never near it. See ADR 0014.
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


# ---------------------------------------------------------------------------
# Remote embeddings: bge-m3 on the company server (ClickUp 86cbehd6h, ADR 0014)
# ---------------------------------------------------------------------------

# Texts per request. The server (Infinity on CPU) accepts more, but a batch is
# also the unit of retry and of failure: 64 keeps one HTTP call under a minute
# on the measured host (their own 64x500-character batch took 26.7 s) and a
# rebuild's progress at a granularity an operator can watch.
REMOTE_MAX_BATCH_SIZE = 64

# How far a returned vector may be from unit length before we renormalise it.
# The server answers normalised vectors today (measured: |‖v‖-1| < 4e-8), and
# the stored index is normalised too — cosine search over it IS a dot product
# (`vector_index.InMemoryVectorIndex`). So this is not a preference: a vector
# of another length would silently rank against the stored ones on magnitude.
# 1e-3 is far above float32 round-trip noise and far below any real change of
# behaviour (a server switched to a non-normalising model, an averaged pooling
# change), which is exactly the event that must be loud.
REMOTE_NORM_TOLERANCE = 1e-3

_unnormalised_lock = threading.Lock()
_unnormalised_warned = False


def _warn_unnormalised_once(norm: float) -> None:
    """Say ONCE per process that the server stopped answering unit vectors.

    Once, not per vector: a server that changed its pooling answers this way
    for every request, and a per-vector warning would bury the log it is
    supposed to be found in. The vectors are still corrected (`normalize`) —
    the alternative, trusting them, would corrupt the index silently, which is
    the one outcome this check exists to prevent.
    """
    global _unnormalised_warned
    with _unnormalised_lock:
        if _unnormalised_warned:
            return
        _unnormalised_warned = True
    logger.warning(
        "The embedding server returned a vector of length %.6f, not 1.0 — "
        "normalising it here. The stored index is unit-length, so this "
        "would otherwise change ranking silently; check EMBEDDING_MODEL "
        "against the model the server actually serves. Logged once.",
        norm,
    )


class RemoteEmbeddingClient:
    """The other two clients' interface, served by bge-m3 over HTTP.

    `POST {endpoint}/embeddings` with `{"model": ..., "input": [texts]}` —
    the OpenAI embeddings shape, which Infinity, TEI and vLLM all expose —
    and the answer's `data[]` re-ordered by its own `index` field. Same
    `EmbeddingUnavailable` contract as `GeminiEmbeddingClient` and
    `LocalEmbeddingClient`, including the `provider_down` split (retries
    exhausted / transport down / not configured = True; a malformed body or a
    request-specific status = False, so the caller may still try other texts).

    Transport discipline is `RemoteTranscriber`'s, which is `llm_client`'s,
    which is `app/gemini_retry.py`: `provider_timeout` carves one call's
    ceiling across httpx's four phases (a bare number would authorise four
    times it), `retry_pause` refuses to sleep unless the attempt after it
    still fits in the request budget, and `RETRYABLE_STATUS` decides what is
    worth another attempt at all.

    Never logged and never quoted in an error: the text being embedded, the
    key, and the endpoint URL — an httpx message carries that URL, so every
    transport failure is reported by CATEGORY and raised `from None`.
    """

    def __init__(
        self,
        endpoint: str = EMBEDDING_STAGE.endpoint,
        api_key: str = EMBEDDING_STAGE.api_key,
        model: str = EMBEDDING_MODEL,
        dimensions: int = EMBEDDING_DIMENSIONS,
        http_client: httpx.Client | None = None,
        sleep=time.sleep,
        timeout: float = 60.0,
        max_retries: int = _MAX_RETRIES,
        batch_size: int = REMOTE_MAX_BATCH_SIZE,
    ):
        self.endpoint = endpoint
        self.api_key = api_key
        self.model = model
        self.dimensions = dimensions
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=httpx.Timeout(timeout))
        self._sleep = sleep
        self.timeout = timeout
        self.max_retries = max(1, max_retries)
        # Capped, not merely defaulted: a caller that asks for 500 would build
        # a request the server may refuse whole, and one refusal would then
        # cost 500 chunks instead of 64.
        self.batch_size = max(1, min(batch_size, REMOTE_MAX_BATCH_SIZE))

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "RemoteEmbeddingClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    @property
    def url(self) -> str:
        """`https://host/v1` -> `https://host/v1/embeddings`.

        An endpoint that already names the method is left alone, the way
        `llm_client.completions_url` and `transcription.transcriptions_url`
        accept both spellings of theirs.
        """
        base = self.endpoint.rstrip("/")
        if base.endswith("/embeddings"):
            return base
        return f"{base}/embeddings"

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        vectors: list[list[float]] = []
        started = time.time()
        for start in range(0, len(texts), self.batch_size):
            vectors.extend(self._embed_batch(texts[start:start + self.batch_size]))
        # Progress of a rebuild, and nothing else: how many texts and how long
        # — never the texts. `python app/index_cli.py rebuild` on this provider
        # is an hour of these lines.
        logger.info(
            "Remote embeddings: %d texts in %d call(s), %.1f s",
            len(texts),
            (len(texts) + self.batch_size - 1) // self.batch_size,
            time.time() - started,
        )
        return vectors

    def embed_query(
        self, text: str, deadline: Deadline | None = None
    ) -> list[float]:
        return self._embed_batch([text], deadline)[0]

    def _check_configured(self) -> None:
        """Unreachable in a started service (`config._validate` refuses an
        incomplete stage), but a CLI or a test that bypasses config must fail
        loudly instead of posting a prayer-derived query to an empty URL."""
        if not self.endpoint:
            raise EmbeddingUnavailable(
                "EMBEDDING_ENDPOINT is not configured", provider_down=True
            )
        if not self.model:
            raise EmbeddingUnavailable(
                "EMBEDDING_MODEL is not configured", provider_down=True
            )

    def _headers(self) -> dict[str, str]:
        """Bearer, or nothing: an empty key states "no Authorization here"."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _unit(self, values: list) -> list[float]:
        """One vector, unit length, of the configured width.

        The server normalises today and is expected to keep doing so; this
        checks rather than assumes, because the failure it guards against is
        invisible — vectors of another length rank against a normalised index
        by magnitude and simply return worse passages.
        """
        try:
            norm = math.sqrt(sum(float(x) * float(x) for x in values))
        except (TypeError, ValueError):
            raise EmbeddingUnavailable(
                "the embedding response is not a vector of numbers"
            ) from None
        if abs(norm - 1.0) < REMOTE_NORM_TOLERANCE:
            return [float(x) for x in values]
        _warn_unnormalised_once(norm)
        return normalize([float(x) for x in values])

    def _vectors_of(
        self, response: httpx.Response, expected: int
    ) -> list[list[float]]:
        """`data[]` as vectors in INPUT order, or EmbeddingUnavailable.

        The order of `data` is the server's business — the protocol says each
        item carries its own `index` — and a silently mis-ordered batch would
        attach every chunk's vector to its neighbour. So the position is read,
        never assumed, and a duplicate or out-of-range one is an error.

        Nothing here echoes the body: on a server that answers an error as a
        200 with prose, that prose is about the text being embedded.
        """
        try:
            payload = response.json()
        except ValueError as exc:
            # An HTTP 200 with a broken body must surface as
            # EmbeddingUnavailable, not json.JSONDecodeError (retrieval m3).
            raise EmbeddingUnavailable(
                "invalid JSON in embedding response"
            ) from exc
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list) or len(data) != expected:
            raise EmbeddingUnavailable(
                f"the embedding response holds "
                f"{len(data) if isinstance(data, list) else 0} vectors for "
                f"{expected} inputs"
            )
        rows: list[list[float] | None] = [None] * expected
        for position, item in enumerate(data):
            index = item.get("index", position) if isinstance(item, dict) else None
            if (
                not isinstance(index, int)
                or isinstance(index, bool)
                or not 0 <= index < expected
                or rows[index] is not None
            ):
                raise EmbeddingUnavailable(
                    "the embedding response does not carry one vector per input"
                )
            values = item.get("embedding")
            if not isinstance(values, list) or len(values) != self.dimensions:
                raise EmbeddingUnavailable(
                    "unexpected embedding size: "
                    f"{len(values) if isinstance(values, list) else 0}"
                )
            rows[index] = self._unit(values)
        return [row for row in rows if row is not None]

    def _embed_batch(
        self, texts: list[str], deadline: Deadline | None = None
    ) -> list[list[float]]:
        self._check_configured()
        body = {
            "model": self.model,
            # An empty string is a degenerate input for a tokenizer; a single
            # space keeps result positions aligned with the input, exactly as
            # both other clients do for the same reason.
            "input": [text if text.strip() else " " for text in texts],
        }
        headers = self._headers()
        url = self.url
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
                    url, json=body, headers=headers, timeout=timeout
                )
            except httpx.HTTPError as exc:
                # Category only: an httpx message quotes the request URL and
                # the body, and on the serve path that body is a query
                # rewritten from the prayer context.
                last_error = f"transport error: {transport_error(exc)}"
                provider_down = True
            else:
                if response.status_code == 200:
                    return self._vectors_of(response, len(texts))
                # Status only — a provider error body can echo the request.
                last_error = f"HTTP {response.status_code}"
                provider_down = response.status_code in _RETRYABLE_STATUS
                if response.status_code not in _RETRYABLE_STATUS or last_attempt:
                    break
                # None = retrying is pointless or no longer affordable: give
                # up NOW so the caller can degrade in time.
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
            f"remote embedding failed: {last_error}", provider_down=provider_down
        ) from None


def build_embedding_client(
    provider: str = EMBEDDING_PROVIDER,
    timeout: float = 60.0,
    max_retries: int = _MAX_RETRIES,
    **kwargs,
):
    """The embedding client this deployment configured (ADR 0010, ADR 0014).

    The one place that maps `EMBEDDING_PROVIDER` onto a class, so the
    endpoint, both CLIs and the tests all get the same answer. An unknown
    value cannot reach here — `config._validate` refuses it at start-up.

    `timeout` and `max_retries` describe a network call and are ignored by
    the local client, which has neither; they stay in the signature so the
    two call sites that tune the remote ladders (serve-time budgets vs the
    patient offline CLI) do not have to know which provider they got.
    """
    if provider == EMBEDDING_PROVIDER_LOCAL:
        return LocalEmbeddingClient(**kwargs)
    if provider == EMBEDDING_PROVIDER_OPENAI_COMPAT:
        return RemoteEmbeddingClient(
            timeout=timeout, max_retries=max_retries, **kwargs
        )
    return GeminiEmbeddingClient(
        timeout=timeout, max_retries=max_retries, **kwargs
    )
