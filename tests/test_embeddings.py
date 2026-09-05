"""Unit tests for the two embedding clients (app/embeddings.py).

No network and no model: `httpx.MockTransport` for Gemini, a stand-in encoder
for the local one. **Nothing here loads bge-m3** — 2.3 GB of weights have no
place in a suite that must stay under three minutes, so `LocalEmbeddingClient`
is always given its model, and the one test that would load a real one is
skipped unless it is asked for explicitly.

Focus: the two clients are interchangeable. Every failure surfaces as
EmbeddingUnavailable (never a raw json.JSONDecodeError — retrieval m3, never
a raw torch error), the provider_down flag lets callers fail fast when the
provider is down for everyone (retrieval m2), and no failure message carries
the text that was being embedded.
"""

import os
import threading
import time

import httpx
import numpy as np
import pytest

os.environ.setdefault("API_KEY", "test-api-key")

import config
from embeddings import (
    EmbeddingConfig,
    EmbeddingUnavailable,
    GeminiEmbeddingClient,
    LocalEmbeddingClient,
    build_embedding_client,
    normalize,
)

DIMS = 4


def make_client(handler) -> GeminiEmbeddingClient:
    return GeminiEmbeddingClient(
        config=EmbeddingConfig(
            model="embed-test", dimensions=DIMS, api_key="test-key"
        ),
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _s: None,
    )


def test_embed_query_happy_path():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"embedding": {"values": [3.0, 0.0, 4.0, 0.0]}}
        )

    vector = make_client(handler).embed_query("текст")
    assert vector == pytest.approx(normalize([3.0, 0.0, 4.0, 0.0]))


def test_http_200_with_invalid_json_raises_embedding_unavailable():
    # m3: a 200 with a broken body used to escape as json.JSONDecodeError
    # and crash select() instead of triggering its fallback.
    client = make_client(
        lambda r: httpx.Response(200, content=b"<html>not json</html>")
    )
    with pytest.raises(EmbeddingUnavailable, match="invalid JSON") as exc_info:
        client.embed_query("текст")
    assert exc_info.value.provider_down is False


def test_http_200_with_non_object_json_raises_embedding_unavailable():
    for payload in ([1, 2, 3], {"embedding": "junk"}, {"embedding": {}}):
        client = make_client(lambda r, p=payload: httpx.Response(200, json=p))
        with pytest.raises(EmbeddingUnavailable):
            client.embed_query("текст")


def test_wrong_dimension_count_is_not_provider_down():
    client = make_client(
        lambda r: httpx.Response(200, json={"embedding": {"values": [1.0]}})
    )
    with pytest.raises(EmbeddingUnavailable, match="size") as exc_info:
        client.embed_query("текст")
    assert exc_info.value.provider_down is False


def test_missing_api_key_is_provider_down():
    client = GeminiEmbeddingClient(
        config=EmbeddingConfig(model="m", dimensions=DIMS, api_key=""),
        http_client=httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200))
        ),
    )
    with pytest.raises(EmbeddingUnavailable) as exc_info:
        client.embed_query("текст")
    assert exc_info.value.provider_down is True


def test_exhausted_retries_on_5xx_is_provider_down():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    with pytest.raises(EmbeddingUnavailable) as exc_info:
        make_client(handler).embed_query("текст")
    assert exc_info.value.provider_down is True
    assert calls["n"] == 6  # full retry budget


def test_transport_errors_are_provider_down():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(EmbeddingUnavailable) as exc_info:
        make_client(handler).embed_query("текст")
    assert exc_info.value.provider_down is True


def test_non_retryable_http_error_is_not_provider_down():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, json={"error": {"message": "bad"}})

    with pytest.raises(EmbeddingUnavailable) as exc_info:
        make_client(handler).embed_query("текст")
    assert exc_info.value.provider_down is False
    assert calls["n"] == 1  # no retries on a request-specific error


def test_retry_then_success_recovers():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(429, json={"error": {}})
        return httpx.Response(
            200, json={"embedding": {"values": [1.0, 0.0, 0.0, 0.0]}}
        )

    assert make_client(handler).embed_query("текст")[0] == pytest.approx(1.0)
    assert calls["n"] == 3


def test_provider_error_bodies_never_reach_the_exception_message():
    """Serve-path privacy: the request body is a query derived from the
    prayer context, and a provider error can echo it back. Callers log
    this message (retrieval.select), so it must carry the status only."""
    secret = "переписанный запрос про тревогу"
    client = make_client(
        lambda r: httpx.Response(400, text=f'{{"error": "bad input: {secret}"}}')
    )

    with pytest.raises(EmbeddingUnavailable) as exc_info:
        client.embed_query(secret)

    message = str(exc_info.value)
    assert "HTTP 400" in message
    assert secret not in message
    assert "bad input" not in message


def test_transport_errors_report_only_the_exception_type():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"failed to POST {request.url}?q=секрет")

    with pytest.raises(EmbeddingUnavailable) as exc_info:
        make_client(handler).embed_query("секрет")

    message = str(exc_info.value)
    assert "ConnectError" in message
    assert "секрет" not in message


# ---------------------------------------------------------------------------
# The local client: bge-m3 in this process (ClickUp 86cbegg2r, ADR 0010)
# ---------------------------------------------------------------------------


class FakeSentenceTransformer:
    """The two calls `LocalEmbeddingClient` makes on a real model.

    Returns already-normalised rows, because that is what
    `normalize_embeddings=True` guarantees and what the vector index assumes;
    records how it was called, so the encoding contract (batch size, one
    encode per call, the arguments that make the vectors comparable to the
    stored ones) is asserted rather than hoped for.
    """

    max_seq_length = 8192

    def __init__(self, dims: int = DIMS, fail: bool = False, delay: float = 0.0):
        self.dims = dims
        self.fail = fail
        # Held INSIDE the concurrency counters, so a test about serialisation
        # observes an overlap when there is one instead of passing on the
        # timing luck of six instant encodes.
        self.delay = delay
        self.calls = []
        self.concurrent = 0
        self.max_concurrent = 0
        self._lock = threading.Lock()

    # The name sentence-transformers 6 uses; the loader also accepts the
    # deprecated `get_sentence_embedding_dimension` of earlier versions.
    def get_embedding_dimension(self) -> int:
        return self.dims

    def encode(self, texts, **kwargs):
        with self._lock:
            self.concurrent += 1
            self.max_concurrent = max(self.max_concurrent, self.concurrent)
        try:
            if self.delay:
                time.sleep(self.delay)
            self.calls.append((list(texts), kwargs))
            if self.fail:
                raise RuntimeError(f"torch exploded on {texts[0]!r}")
            rows = []
            for i, _text in enumerate(texts):
                row = np.zeros(self.dims, dtype=np.float32)
                row[i % self.dims] = 1.0
                rows.append(row)
            return np.vstack(rows)
        finally:
            with self._lock:
                self.concurrent -= 1


def local_client(model=None, **kwargs) -> LocalEmbeddingClient:
    return LocalEmbeddingClient(
        config=EmbeddingConfig(model="BAAI/bge-m3", dimensions=DIMS, api_key=""),
        model=model if model is not None else FakeSentenceTransformer(),
        **kwargs,
    )


def test_local_embed_query_returns_a_unit_vector_of_the_right_width():
    vector = local_client().embed_query("тревога")
    assert len(vector) == DIMS
    assert vector == pytest.approx(normalize(vector))


def test_local_encode_asks_for_normalised_vectors():
    """Cosine search over the stored matrix IS a dot product
    (`vector_index.InMemoryVectorIndex`), so unit length is a correctness
    requirement, not a preference."""
    model = FakeSentenceTransformer()
    local_client(model).embed_query("тревога")
    _texts, kwargs = model.calls[0]
    assert kwargs["normalize_embeddings"] is True
    assert kwargs["convert_to_numpy"] is True
    assert kwargs["show_progress_bar"] is False


def test_local_embed_documents_keeps_order_and_batches():
    model = FakeSentenceTransformer()
    client = local_client(model, batch_size=4)
    vectors = client.embed_documents([f"chunk {i}" for i in range(7)])
    assert len(vectors) == 7
    texts, kwargs = model.calls[0]
    # One encode for the whole list; the batch is the model's, not ours.
    assert len(model.calls) == 1
    assert texts == [f"chunk {i}" for i in range(7)]
    assert kwargs["batch_size"] == 4


def test_local_empty_text_is_replaced_the_way_gemini_replaces_it():
    model = FakeSentenceTransformer()
    local_client(model).embed_documents(["", "  ", "текст"])
    texts, _kwargs = model.calls[0]
    assert texts == [" ", " ", "текст"]


def test_local_no_texts_makes_no_call():
    model = FakeSentenceTransformer()
    assert local_client(model).embed_documents([]) == []
    assert model.calls == []


def test_local_failure_is_embedding_unavailable_and_provider_down():
    """A broken encode is not a transient provider: the retrieval pipeline
    must stop after the first failed variant, not retry five more."""
    client = local_client(FakeSentenceTransformer(fail=True))
    with pytest.raises(EmbeddingUnavailable) as exc_info:
        client.embed_query("секретная молитва")
    assert exc_info.value.provider_down is True
    message = str(exc_info.value)
    assert "RuntimeError" in message
    # Same privacy rule as the Gemini client: the query never leaves in a
    # message the caller logs.
    assert "секретная молитва" not in message
    assert "torch exploded" not in message


def test_local_wrong_width_is_reported_not_stored():
    client = LocalEmbeddingClient(
        config=EmbeddingConfig(model="m", dimensions=DIMS + 1, api_key=""),
        model=FakeSentenceTransformer(dims=DIMS),
    )
    with pytest.raises(EmbeddingUnavailable, match="size"):
        client.embed_query("q")


def test_local_encode_is_serialised():
    """The pipeline may still hand this client to a thread pool; the lock is
    what makes that safe rather than merely discouraged."""
    model = FakeSentenceTransformer(delay=0.02)
    client = local_client(model)
    threads = [
        threading.Thread(target=client.embed_query, args=(f"q{i}",))
        for i in range(6)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(model.calls) == 6
    assert model.max_concurrent == 1


def test_two_clients_over_one_model_share_the_lock():
    """The weights are process-wide, so the lock has to be too: a second
    client built around the same model (a warm-up path, an in-process
    rebuild, a second `build_embedding_client()`) must queue on it rather
    than encode beside the first one."""
    model = FakeSentenceTransformer(delay=0.02)
    clients = [local_client(model), local_client(model)]
    threads = [
        threading.Thread(target=clients[i % 2].embed_query, args=(f"q{i}",))
        for i in range(6)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(model.calls) == 6
    assert model.max_concurrent == 1


def test_local_expired_deadline_refuses_before_encoding():
    """A CPU encode cannot be interrupted half-way, so the budget is checked
    before it starts — the answer the Gemini client gives when
    `provider_timeout` returns None."""
    from deadline import Deadline

    model = FakeSentenceTransformer()
    with pytest.raises(EmbeddingUnavailable, match="budget"):
        local_client(model).embed_query("q", deadline=Deadline(0.0))
    assert model.calls == []


def test_local_client_never_opens_an_http_client():
    """Structural half of the tripwire: nothing to dial, so nothing can be
    dialled (see tests/test_llm_client.py for the behavioural half)."""
    client = local_client()
    assert not any(
        isinstance(value, httpx.Client) for value in vars(client).values()
    )
    client.close()  # idempotent no-op: the weights outlive every client


# --- loading the weights ---------------------------------------------------


@pytest.fixture
def fake_sentence_transformers(monkeypatch):
    """A stand-in `sentence_transformers` module in sys.modules.

    Injected rather than patched on the real package so the suite imports
    neither torch nor 2.3 GB of weights; what is under test is the loader's
    own contract (one load, the window cap, the width check), not the
    library's.
    """
    import sys
    import types

    import embeddings

    built = []

    class Module(types.ModuleType):
        dims = DIMS

        @staticmethod
        def SentenceTransformer(path, device):  # noqa: N802 - library name
            built.append((path, device))
            return FakeSentenceTransformer(dims=Module.dims)

    module = Module("sentence_transformers")
    monkeypatch.setitem(sys.modules, "sentence_transformers", module)
    monkeypatch.setattr(embeddings, "_model", None)
    yield module, built
    embeddings._model = None


def test_load_embedding_model_caps_the_window_and_loads_once(
    fake_sentence_transformers,
):
    import embeddings

    module, built = fake_sentence_transformers
    model = embeddings.load_embedding_model(path="/models/bge-m3", dimensions=DIMS)
    again = embeddings.load_embedding_model(path="/models/bge-m3", dimensions=DIMS)

    assert model is again  # 2.3 GB is loaded once per process, or never
    assert built == [("/models/bge-m3", "cpu")]
    assert model.max_seq_length == embeddings.LOCAL_MAX_SEQ_LENGTH


def test_load_embedding_model_refuses_a_model_of_another_width(
    fake_sentence_transformers,
):
    """The path and the model identity are separate variables, so a
    directory holding another model would write vectors of the wrong space
    under the right index version — silently, and irreversibly."""
    import embeddings

    module, _built = fake_sentence_transformers
    module.dims = DIMS
    with pytest.raises(EmbeddingUnavailable) as exc_info:
        embeddings.load_embedding_model(path="/models/other", dimensions=1024)
    message = str(exc_info.value)
    assert "1024" in message and "/models/other" in message
    assert exc_info.value.provider_down is True


def test_load_embedding_model_without_a_path_is_unavailable(monkeypatch):
    import embeddings

    monkeypatch.setattr(embeddings, "_model", None)
    with pytest.raises(EmbeddingUnavailable, match="EMBEDDING_MODEL_PATH"):
        embeddings.load_embedding_model(path="", dimensions=DIMS)


def test_load_embedding_model_reports_a_broken_directory(
    fake_sentence_transformers, monkeypatch
):
    import embeddings

    module, _built = fake_sentence_transformers

    def broken(path, device):
        raise OSError("no such file or directory: config.json")

    monkeypatch.setattr(module, "SentenceTransformer", broken)
    with pytest.raises(EmbeddingUnavailable) as exc_info:
        embeddings.load_embedding_model(path="/models/gone", dimensions=DIMS)
    assert "OSError" in str(exc_info.value)
    assert exc_info.value.provider_down is True


# --- the factory -----------------------------------------------------------


def test_factory_builds_the_gemini_client_by_default():
    client = build_embedding_client(provider=config.EMBEDDING_PROVIDER_GEMINI)
    assert isinstance(client, GeminiEmbeddingClient)
    client.close()


def test_factory_builds_the_local_client(monkeypatch):
    monkeypatch.setattr(
        "embeddings.load_embedding_model", lambda: FakeSentenceTransformer()
    )
    client = build_embedding_client(
        provider=config.EMBEDDING_PROVIDER_LOCAL
    )
    assert isinstance(client, LocalEmbeddingClient)
    assert not isinstance(client, GeminiEmbeddingClient)


def test_factory_does_not_hand_network_budgets_to_the_local_client(monkeypatch):
    """`timeout` / `max_retries` describe a call the local client does not
    make; it must accept them from a shared call site and ignore them, not
    fail on them."""
    monkeypatch.setattr(
        "embeddings.load_embedding_model", lambda: FakeSentenceTransformer()
    )
    client = build_embedding_client(
        provider=config.EMBEDDING_PROVIDER_LOCAL, timeout=8.0, max_retries=2
    )
    assert isinstance(client, LocalEmbeddingClient)


# --- the real weights, on request only -------------------------------------

@pytest.mark.skipif(
    not os.getenv("EMBEDDING_MODEL_PATH_UNDER_TEST"),
    reason=(
        "loads 2.3 GB of real weights; run it explicitly with "
        "EMBEDDING_MODEL_PATH_UNDER_TEST=/models/bge-m3"
    ),
)
def test_the_real_model_loads_and_embeds():
    """The one test that touches the weights volume. Skipped by default —
    the suite must stay a suite — and run by hand after a change to the
    image, the mount or the model directory."""
    import embeddings

    path = os.environ["EMBEDDING_MODEL_PATH_UNDER_TEST"]
    dims = int(os.environ.get("EMBEDDING_DIMENSIONS_UNDER_TEST", "1024"))
    embeddings._model = None  # this test owns the process-wide slot
    try:
        model = embeddings.load_embedding_model(path=path, dimensions=dims)
        assert model.max_seq_length == embeddings.LOCAL_MAX_SEQ_LENGTH
        client = LocalEmbeddingClient(
            config=EmbeddingConfig(model="BAAI/bge-m3", dimensions=dims,
                                   api_key=""),
            model=model,
        )
        vector = client.embed_query("Господь — Пастырь мой")
        assert len(vector) == dims
        assert sum(x * x for x in vector) == pytest.approx(1.0, abs=1e-4)
    finally:
        embeddings._model = None
