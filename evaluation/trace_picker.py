#!/usr/bin/env python3
"""
Side-by-side TRACE of the production scripture selection (ClickUp 86cbegawh,
right column on local models 86cbegcmm, umbrella 86cbe4mtq).

What it is: a hand-driven stand that runs the REAL production pipeline
(`app/retrieval.ScriptureRetriever.select_final` on Gemini) over a prayer
typed into a web page, and shows every stage it went through — the rewrite
variants, what each variant found by vector and by BM25, what the interleave
produced, what the genre blacklist and the diversity caps cut, the candidate
list the model was shown, the choice it made and why, the key verses.

The RIGHT column answers the same prayer. Two of them exist, chosen by `?r=`:

* `r=prod-local` (default, 86cbegcmm) — **the same production pipeline with
  local models instead of Gemini**: the identical `ScriptureRetriever` /
  `select_final`, the identical trace hook and the identical stage list, only
  the three providers are swapped for `QwenQueryRewriter` (prompt 8c on
  qwen3-30b), `LocalEmbedder` (bge-m3 in this process over the benchmark's
  document matrix) and `QwenPassageReranker` (the production v9 prompt on
  qwen3-30b). BM25, the genre blacklist, diversity and the safe pool are the
  production ones, shared with the left column. This is the honest comparison
  Maria asked for: same scheme, different models.
* `r=senses` — the previous right column: the LOCAL stand
  (`evaluation/local_picker.py` on port 9089) over HTTP, a *different* scheme
  (a sense index instead of a rewrite stage), kept because it is what the
  sense experiment measured.

Because `r` names the right column, the picked replies are the `replies`
parameter (they were `r` before 86cbegcmm).

    ./run_trace_picker.sh --serve          # http://0.0.0.0:9090/
    ./run_trace_picker.sh "текст молитвы"  # one run to stdout as JSON

THE LEFT COLUMN CALLS GEMINI AND IT COSTS MONEY. One request spends one
rewrite call (`AI_SCRIPTURE_REWRITE_API_KEY`, the paid key), up to six
embedding calls and one rerank call. The page says so at the top and counts
the calls made in this process since it started. The right column spends
none of that: its counter is shown next to Gemini's and must stay at zero
(the tripwire below makes it a guarantee, not a hope). Nothing here is
served to the mobile app and nothing here is on the production path.

Tripwire of the right column: the two Qwen adapters share one httpx client
whose transport refuses any host that is neither the configured local
endpoint nor loopback/private (`LocalOnlyTransport`), and the stand asserts
at construction that not one of its three providers is a Gemini client.
A global socket guard (what `local_picker.py` installs) is impossible here —
the left column legitimately calls Gemini in the same process — so the guard
is scoped to the objects the right column owns.

How the trace is collected: `ScriptureRetriever` takes an optional
`trace(stage, payload)` observer (added with this ticket). It is called after
a stage has already decided, its result is ignored and an exception in it is
swallowed — so a traced selection decides exactly what an untraced one
decides (`tests/test_retrieval.py`, "Diagnostic trace hook"). The stand
therefore watches the production code rather than re-implementing it: the
only pipeline logic in this file is presentation.

What is deliberately NOT reproduced from the endpoint: the API key, the
rate limits and the 15-second serve budget (this is a manual tool — the
budget here is 60 s), and the renderable-translation catalogue (the stand
always serves the language's primary/indexed translation, which is the
corpus itself and needs no coverage filter). Everything that decides WHICH
passage comes back is the production code, unchanged.

Configuration (environment; `run_trace_picker.sh` fills it from `.env`):

    GEMINI_API_KEY                embeddings + rerank (required)
    AI_SCRIPTURE_REWRITE_API_KEY  rewrite stage (optional; paid key)
    AI_SCRIPTURE_REWRITE_MODEL / AI_SCRIPTURE_RERANK_MODEL / EMBEDDING_MODEL
    DB_*                          cep_public, host 127.0.0.1:3306
    TRACE_PICKER_PORT             web port, default 9090
    TRACE_PICKER_LOCAL_URL        senses stand, default http://127.0.0.1:9089
    TRACE_PICKER_LOCAL_TIMEOUT    seconds to wait for it, default 150
    TRACE_PICKER_BUDGET_SECONDS   budget of one Gemini run, default 60

    TRACE_LOCAL_REWRITE_ENDPOINT / _MODEL / _API_KEY    rewrite on Qwen
    TRACE_LOCAL_RERANK_ENDPOINT  / _MODEL / _API_KEY    rerank on Qwen
    TRACE_LOCAL_BUDGET_SECONDS    budget of one local run, default 600
    TRACE_LOCAL_HTTP_TIMEOUT      one Qwen call, seconds, default 240
    TRACE_LOCAL_MAX_TOKENS        output ceiling of a Qwen call, default 1024

`run_trace_picker.sh` fills the six TRACE_LOCAL_* provider variables from
Maria's vLLM box through the SSH tunnel, exactly as
`run_local_picker_qwen.sh` does; the key never reaches a file or a log.
Without them the right column says which variable is missing instead of
quietly degrading to a rewrite-less run.

Privacy, same rule as the local stand: the prayer text is NEVER written to
the log — only its length, the timings and the canonical id that came back.
The API keys are never logged and never rendered.

Russian only (`ru`/`syn`), because the local stand it compares against is.
"""

from __future__ import annotations

import argparse
import html
import ipaddress
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "app"))

import httpx  # noqa: E402
import numpy as np  # noqa: E402

import config  # noqa: E402
import rewrite_prompts  # noqa: E402
from chunking import CHUNKING_VERSION  # noqa: E402
from database import create_connection  # noqa: E402
from deadline import Deadline  # noqa: E402
from embeddings import EmbeddingUnavailable, GeminiEmbeddingClient  # noqa: E402
from lexical_index import load_lexical_indexes  # noqa: E402
from passage_highlight import load_psalm_maps  # noqa: E402
from passage_rerank import (  # noqa: E402
    RERANK_PROMPT_VERSION,
    GeminiPassageReranker,
    PassageRerankError,
    build_rerank_instruction,
    build_rerank_user_content,
    parse_rerank_response,
)
from query_rewrite import (  # noqa: E402
    REWRITE_PROMPT_VERSION,
    REWRITE_VARIANTS,
    GeminiQueryRewriter,
    QueryRewriteError,
    build_rewrite_user_content,
    build_search_query,
)
from retrieval import (  # noqa: E402
    ScriptureRetriever,
    SelectionRequest,
    make_db_passage_loader,
    make_db_verse_loader,
    parse_canonical_id,
    prompt_passage,
)
from vector_index import InMemoryVectorIndex, load_index  # noqa: E402

# --- configuration ---------------------------------------------------------

LANGUAGE = "ru"
TOP_K = 10                      # the benchmarked candidate list (ADR 0005)
TABLE_K = 8                     # rows shown per per-variant table
DEFAULT_PORT = int(os.environ.get("TRACE_PICKER_PORT", "9090"))
LOCAL_URL = os.environ.get(
    "TRACE_PICKER_LOCAL_URL", "http://127.0.0.1:9089"
).rstrip("/")
LOCAL_TIMEOUT = float(os.environ.get("TRACE_PICKER_LOCAL_TIMEOUT", "150"))
BUDGET_SECONDS = float(os.environ.get("TRACE_PICKER_BUDGET_SECONDS", "60"))
# Serve-time budgets are the endpoint's (ADR 0006); the ladder is a little
# more patient here because a manual run would rather wait than degrade.
PROVIDER_TIMEOUT_SECONDS = 20.0
PROVIDER_ATTEMPTS = 3

# --- the local stack of the right column (86cbegcmm) -----------------------

LOCAL_REWRITE_ENDPOINT = os.environ.get("TRACE_LOCAL_REWRITE_ENDPOINT", "").strip()
LOCAL_REWRITE_MODEL = os.environ.get("TRACE_LOCAL_REWRITE_MODEL", "").strip()
LOCAL_REWRITE_API_KEY = os.environ.get("TRACE_LOCAL_REWRITE_API_KEY", "").strip()
LOCAL_RERANK_ENDPOINT = os.environ.get("TRACE_LOCAL_RERANK_ENDPOINT", "").strip()
LOCAL_RERANK_MODEL = os.environ.get("TRACE_LOCAL_RERANK_MODEL", "").strip()
LOCAL_RERANK_API_KEY = os.environ.get("TRACE_LOCAL_RERANK_API_KEY", "").strip()

# The rewrite prompt of the local column: 8c, the object-per-query variant
# measured for small models (evaluation/rewrite_prompts.py). The rerank
# prompt is the PRODUCTION one (v9) and is not versioned here at all — it is
# imported, so it cannot drift.
LOCAL_REWRITE_PROMPT = "8c"
# The embedding side is the benchmark's cached document matrix, so the model
# key and the doc-text variant must be the pair that matrix was built from.
LOCAL_EMBED_MODEL = "bge-m3"
LOCAL_EMBED_VARIANT = "title_text"
# A local run is minutes, not seconds: Qwen is reached over a tunnel and
# bge-m3 encodes on 8 CPU cores. The budget only has to be generous enough
# that no stage degrades for lack of time — this is a manual stand.
LOCAL_BUDGET_SECONDS = float(os.environ.get("TRACE_LOCAL_BUDGET_SECONDS", "600"))
LOCAL_HTTP_TIMEOUT = float(os.environ.get("TRACE_LOCAL_HTTP_TIMEOUT", "240"))
# 1024, not the production 8192: qwen3-30b-a3b-instruct is a NON-thinking
# model that needs ~250 tokens for six queries, and on a server with context
# shifting an unreachable ceiling turns a degenerate repetition into an
# endless one (gen_rewrites.DEFAULT_MAX_TOKENS says the same).
LOCAL_MAX_TOKENS = int(os.environ.get("TRACE_LOCAL_MAX_TOKENS", "1024"))
LOCAL_ATTEMPTS = 2

RIGHT_COLUMNS = ("prod-local", "senses")
DEFAULT_RIGHT = "prod-local"

# Some models emit a reasoning block before the answer; the JSON extraction
# of both production parsers is greedy, so strip it rather than let a brace
# inside the reasoning swallow the real object (gen_rewrites/gen_reranks).
_THINK_BLOCK = re.compile(r"<think>.*?</think>", flags=re.DOTALL | re.IGNORECASE)


# ---------------------------------------------------------------------------
# Provider call counting
# ---------------------------------------------------------------------------

class _Counter:
    """Provider STAGE calls, per request and for the whole session.

    One `rewrite()` / `embed_query()` / `choose()` is one count. Retries
    inside a client (a 429 ladder, say) are extra HTTP requests against the
    same count — this is what the pipeline ASKED for, not the provider's own
    bill, and it is labelled that way on the page.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.session = {"rewrite": 0, "embed": 0, "rerank": 0}

    def add(self, stage: str, request_counts: dict) -> None:
        with self.lock:
            self.session[stage] += 1
        request_counts[stage] = request_counts.get(stage, 0) + 1

    def snapshot(self) -> dict:
        with self.lock:
            return dict(self.session)


COUNTER = _Counter()
# The local column has its own session counter. Keeping them apart is what
# makes "the right column did not spend Gemini quota" a readable fact on the
# page rather than a claim (86cbegcmm).
LOCAL_COUNTER = _Counter()


class CountingRewriter:
    def __init__(self, inner, counts: dict, counter: _Counter = None):
        self.inner, self.counts = inner, counts
        self.counter = counter or COUNTER

    def rewrite(self, *args, **kwargs):
        self.counter.add("rewrite", self.counts)
        return self.inner.rewrite(*args, **kwargs)


class CountingEmbedder:
    def __init__(self, inner, counts: dict, counter: _Counter = None):
        self.inner, self.counts = inner, counts
        self.counter = counter or COUNTER

    def embed_query(self, *args, **kwargs):
        self.counter.add("embed", self.counts)
        return self.inner.embed_query(*args, **kwargs)


class CountingReranker:
    def __init__(self, inner, counts: dict, counter: _Counter = None):
        self.inner, self.counts = inner, counts
        self.counter = counter or COUNTER

    def choose(self, *args, **kwargs):
        self.counter.add("rerank", self.counts)
        return self.inner.choose(*args, **kwargs)


# ---------------------------------------------------------------------------
# The local stack of the right column: tripwire, then the three adapters
# (ClickUp 86cbegcmm)
#
# Every adapter is a DUCK of the production client it replaces — same method
# name, same arguments, same exception type — so `ScriptureRetriever` cannot
# tell the difference and no production code is touched or copied. The
# prompts and the parsers are IMPORTED from `app/` and `rewrite_prompts.py`,
# never re-typed: a copy would drift from what the service accepts.
# ---------------------------------------------------------------------------

class TripwireError(RuntimeError):
    """The right column tried to reach a host it is not allowed to."""


def _host_allowed(host: str, allowed: set[str]) -> bool:
    """Loopback, private/link-local, or an explicitly configured endpoint."""
    if not host:
        return False
    if host in allowed or host == "localhost":
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.is_loopback or address.is_private or address.is_link_local


class LocalOnlyTransport(httpx.HTTPTransport):
    """Refuses every request outside the local stack's allowlist.

    Scoped to the client the Qwen adapters share, because the LEFT column of
    the same process legitimately calls Gemini: a global socket guard (what
    `local_picker.py` installs) would break it. Placed in the transport
    rather than in the adapters so a redirect or a future call site cannot
    walk around it.
    """

    def __init__(self, allowed_hosts, **kwargs):
        super().__init__(**kwargs)
        self._allowed = set(allowed_hosts)

    def handle_request(self, request):
        if not _host_allowed(request.url.host, self._allowed):
            raise TripwireError(
                f"outbound connection to {request.url.host!r} refused: the "
                f"local column may only reach {sorted(self._allowed)}, "
                f"loopback and private addresses"
            )
        return super().handle_request(request)


def local_stack_missing() -> list[str]:
    """Which TRACE_LOCAL_* variables the right column still needs."""
    return [
        name
        for name, value in (
            ("TRACE_LOCAL_REWRITE_ENDPOINT", LOCAL_REWRITE_ENDPOINT),
            ("TRACE_LOCAL_REWRITE_MODEL", LOCAL_REWRITE_MODEL),
            ("TRACE_LOCAL_RERANK_ENDPOINT", LOCAL_RERANK_ENDPOINT),
            ("TRACE_LOCAL_RERANK_MODEL", LOCAL_RERANK_MODEL),
        )
        if not value
    ]


def _completions_url(endpoint: str) -> str:
    base = endpoint.rstrip("/")
    return base if base.endswith("/chat/completions") else f"{base}/chat/completions"


def _endpoint_host(endpoint: str) -> str:
    """Host of an endpoint — never its query string, keys live there."""
    return urlsplit(endpoint).hostname or ""


def build_local_client() -> httpx.Client:
    """The one guarded HTTP client both Qwen adapters use."""
    allowed = {
        host
        for host in (
            _endpoint_host(LOCAL_REWRITE_ENDPOINT),
            _endpoint_host(LOCAL_RERANK_ENDPOINT),
        )
        if host
    }
    return httpx.Client(
        timeout=httpx.Timeout(LOCAL_HTTP_TIMEOUT),
        transport=LocalOnlyTransport(allowed),
    )


def _chat_completion(
    client: httpx.Client, endpoint: str, api_key: str, model: str,
    instruction: str, user_content: str, error_type: type[Exception],
) -> str:
    """One OpenAI-compatible chat completion, same protocol as gen_rewrites.

    temperature 0, json_object, the caller's max_tokens ceiling. Raises
    `error_type` (the production error of the stage) so the retriever
    degrades exactly as it does when Gemini fails.
    """
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": instruction},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.0,
        "max_tokens": LOCAL_MAX_TOKENS,
        "response_format": {"type": "json_object"},
    }
    response = client.post(
        _completions_url(endpoint), json=payload, headers=headers
    )
    response.raise_for_status()
    data = response.json()
    try:
        message = data["choices"][0]["message"]
    except (KeyError, IndexError, TypeError) as exc:
        raise error_type("response has no choices") from exc
    text = message.get("content") or ""
    if not isinstance(text, str) or not text.strip():
        raise error_type("response content is empty")
    return _THINK_BLOCK.sub("", text)


def _transport_error(exc: Exception) -> str:
    """Failure category only — an httpx message can quote the URL and body."""
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTPStatusError (HTTP {exc.response.status_code})"
    return type(exc).__name__


class QwenQueryRewriter:
    """`GeminiQueryRewriter` on a local OpenAI-compatible endpoint, prompt 8c.

    Same surface: `rewrite(language, topic, user_replies, deadline=None)` ->
    list of query strings, `QueryRewriteError` on any failure. The prompt is
    `rewrite_prompts.build_instruction("8c", ...)` and the answer goes
    through `rewrite_prompts.parse_response`, which hands the queries to the
    production cleaner and throws the `ref` fields away.
    """

    def __init__(self, client: httpx.Client, variants: int = REWRITE_VARIANTS):
        self._client = client
        self.variants = variants
        self.model = LOCAL_REWRITE_MODEL
        self.prompt_version = LOCAL_REWRITE_PROMPT

    def rewrite(
        self, language: str, topic: str, user_replies: list[str],
        deadline: Deadline | None = None,
    ) -> list[str]:
        if not LOCAL_REWRITE_ENDPOINT or not self.model:
            raise QueryRewriteError(
                "TRACE_LOCAL_REWRITE_ENDPOINT/_MODEL are not configured"
            )
        instruction = rewrite_prompts.build_instruction(
            self.prompt_version, language, self.variants
        )
        user_content = build_rewrite_user_content(topic, user_replies)
        last: Exception | None = None
        for _attempt in range(LOCAL_ATTEMPTS):
            try:
                text = _chat_completion(
                    self._client, LOCAL_REWRITE_ENDPOINT, LOCAL_REWRITE_API_KEY,
                    self.model, instruction, user_content, QueryRewriteError,
                )
                queries, _refs = rewrite_prompts.parse_response(
                    self.prompt_version, text, self.variants
                )
                return queries
            except TripwireError:
                raise
            except QueryRewriteError as exc:
                last = exc
            except (httpx.HTTPError, ValueError) as exc:
                last = QueryRewriteError(
                    f"rewrite request failed: {_transport_error(exc)}"
                )
        raise QueryRewriteError(f"local rewrite failed: {last}") from last


class QwenPassageReranker:
    """`GeminiPassageReranker` on the same endpoint, PRODUCTION prompt v9.

    Same surface: `choose(topic, user_replies, candidate_texts,
    deadline=None, key_verses=True)` -> `RerankChoice`,
    `PassageRerankError` on any failure. Instruction, user content and
    validation are the production functions, so what this column accepts is
    exactly what the service accepts. The OpenAI-compatible endpoint has no
    `responseSchema`, so `json_object` plus the production parser carry the
    contract — the parser is the part that was ever load-bearing.
    """

    def __init__(self, client: httpx.Client):
        self._client = client
        self.model = LOCAL_RERANK_MODEL
        self.prompt_version = RERANK_PROMPT_VERSION

    def choose(
        self, topic: str, user_replies: list[str], candidate_texts: list[str],
        deadline: Deadline | None = None, key_verses: bool = True,
    ):
        if not candidate_texts:
            raise PassageRerankError("no candidates to rerank")
        if not LOCAL_RERANK_ENDPOINT or not self.model:
            raise PassageRerankError(
                "TRACE_LOCAL_RERANK_ENDPOINT/_MODEL are not configured"
            )
        count = len(candidate_texts)
        instruction = build_rerank_instruction(count, key_verses)
        user_content = build_rerank_user_content(
            topic, user_replies, candidate_texts
        )
        last: Exception | None = None
        for _attempt in range(LOCAL_ATTEMPTS):
            try:
                text = _chat_completion(
                    self._client, LOCAL_RERANK_ENDPOINT, LOCAL_RERANK_API_KEY,
                    self.model, instruction, user_content, PassageRerankError,
                )
                return parse_rerank_response(text, count)
            except TripwireError:
                raise
            except PassageRerankError as exc:
                last = exc
            except (httpx.HTTPError, ValueError) as exc:
                last = PassageRerankError(
                    f"rerank request failed: {_transport_error(exc)}"
                )
        raise PassageRerankError(f"local rerank failed: {last}") from last


class LocalEmbedder:
    """`GeminiEmbeddingClient.embed_query` served by bge-m3 in this process.

    The weights are loaded ONCE (2.3 GB in fp32) and shared; `encode` is
    serialised by a lock, so the retriever is created with `embed_workers=1`.
    That is not a behaviour difference — concurrency is a latency knob of the
    public endpoint (ADR 0006) and every variant is embedded either way — but
    six concurrent CPU encodes on an 8-core box that also runs MySQL would
    thrash the machine and change nothing about the result.

    The query prefix is read from the benchmark's model registry, so the
    query side is embedded exactly the way the cached document matrix was.
    """

    def __init__(self, model, query_prefix: str):
        self._model = model
        self._prefix = query_prefix
        self._lock = threading.Lock()

    def embed_query(self, text: str, deadline: Deadline | None = None):
        try:
            with self._lock:
                vectors = self._model.encode(
                    [self._prefix + (text if text.strip() else " ")],
                    batch_size=1, show_progress_bar=False,
                    normalize_embeddings=True, convert_to_numpy=True,
                )
        except Exception as exc:            # category only, never the query
            raise EmbeddingUnavailable(
                f"local embedding failed: {type(exc).__name__}",
                provider_down=True,
            ) from exc
        return vectors[0].astype(np.float32)


# ---------------------------------------------------------------------------
# The bge-m3 index: the production InMemoryVectorIndex over the benchmark's
# cached document matrix (an ADAPTER, not a second search implementation)
# ---------------------------------------------------------------------------

def load_local_index() -> InMemoryVectorIndex:
    """`bench_data/emb_bge-m3_title_text.npy` as a production vector index.

    Row i of the matrix is line i of `bench_data/chunks.jsonl` — the same
    order `retrieval_benchmark.load_chunks` returns and the same order the
    matrix was written in. So the metadata the index needs is read from that
    file, and `load_corpus_matrix` validates the matrix against it (row count
    plus the corpus fingerprint) before anything is searched: a matrix built
    from another export is a hard error, never quietly wrong scores.

    Cross-checked row by row against the benchmark's own metadata as well,
    so the two readings of the file cannot silently disagree.
    """
    import retrieval_benchmark as rb

    row_metas, _texts, _title_texts = rb.load_chunks()
    matrix = rb.load_corpus_matrix(LOCAL_EMBED_MODEL, LOCAL_EMBED_VARIANT, row_metas)
    metas = []
    with rb.CHUNKS_FILE.open() as handle:
        for row in (json.loads(line) for line in handle):
            metas.append({
                "canonical_id": row["canonical_id"],
                "translation": row["translation"],
                "alias": row["alias"],
                "language": row["language"],
                "book_number": row["book_number"],
                "chapter_number": row["chapter_number"],
                "verse_number_start": row["verse_number_start"],
                "verse_number_end": row["verse_number_end"],
                "title": row["title"],
            })
    mismatch = [
        i for i, (meta, bench) in enumerate(zip(metas, row_metas))
        if meta["canonical_id"] != bench.canonical_id
        or meta["translation"] != bench.translation
    ]
    if len(metas) != len(row_metas) or mismatch:
        raise RuntimeError(
            f"{rb.CHUNKS_FILE.name} read twice and disagreed at rows "
            f"{mismatch[:3]} ({len(metas)} vs {len(row_metas)})"
        )
    return InMemoryVectorIndex(matrix, metas)


def load_local_embedder() -> LocalEmbedder:
    """bge-m3 on CPU, window-capped exactly as the benchmark caps it."""
    import retrieval_benchmark as rb

    _kind, model_id, query_prefix, _passage_prefix, _dims = rb.MODELS[
        LOCAL_EMBED_MODEL
    ]
    limits = rb.LOCAL_ENCODE_LIMITS.get(LOCAL_EMBED_MODEL, {})
    model = rb.load_st_model(model_id, limits.get("max_seq_length", 0))
    return LocalEmbedder(model, query_prefix)


# ---------------------------------------------------------------------------
# Corpus (loaded once, exactly what the endpoint's CorpusResources holds for
# the primary path: vector index, BM25 indexes, Psalm maps)
# ---------------------------------------------------------------------------

class Corpus:
    def __init__(self) -> None:
        connection = create_connection()
        if connection is None:
            raise RuntimeError("cep_public is not reachable")
        cursor = connection.cursor(dictionary=True)
        try:
            started = time.perf_counter()
            self.index = load_index(cursor)
            self.lexical = load_lexical_indexes(cursor, CHUNKING_VERSION)
            self.psalm_maps = load_psalm_maps(cursor)
            self.book_names = _load_book_names(cursor)
            # index insertion order == the translation every candidate text
            # and every rerank prompt is rendered in (retrieval.prompt_passage)
            self.translation, self.alias = next(
                (meta["translation"], meta["alias"])
                for meta in self.index.metas
                if meta["language"] == LANGUAGE
            )
            self.previews = _load_previews(cursor, self.translation)
            self.load_seconds = time.perf_counter() - started
        finally:
            cursor.close()
            connection.close()

    def reference(self, canonical_id: str) -> str:
        _v, book, chapter, start, end = parse_canonical_id(canonical_id)
        name = self.book_names.get(book) or f"кн.{book}"
        span = f"{start}" if start == end else f"{start}-{end}"
        return f"{name} {chapter}:{span}"

    def first_words(self, canonical_id: str, limit: int = 110) -> str:
        text = " ".join((self.previews.get(canonical_id) or "").split())
        return text[:limit] + ("…" if len(text) > limit else "")


def _load_book_names(cursor) -> dict[int, str]:
    cursor.execute("SELECT number, short_name_ru FROM bible_books")
    return {row["number"]: row["short_name_ru"] for row in cursor.fetchall()}


def _load_previews(cursor, translation: int) -> dict[str, str]:
    """canonical_id -> the first characters of the chunk, for the tables.

    Only a preview is kept: the passage actually served comes from the
    pipeline's own loader, and holding the whole corpus text would cost
    megabytes for a stand that shows one line per row.
    """
    cursor.execute(
        "SELECT canonical_id, LEFT(text, 200) AS head FROM translation_chunks "
        "WHERE translation = %s AND chunking_version = %s",
        (translation, CHUNKING_VERSION),
    )
    return {row["canonical_id"]: row["head"] for row in cursor.fetchall()}


# ---------------------------------------------------------------------------
# The trace sink
# ---------------------------------------------------------------------------

class TraceSink:
    """Timestamped record of the stages the retriever reported.

    Read-only by contract (`ScriptureRetriever.trace`): the payloads carry
    live pipeline objects, so nothing here mutates them.
    """

    def __init__(self) -> None:
        self.started = time.perf_counter()
        self.events: list[tuple[float, str, dict]] = []

    def __call__(self, stage: str, payload: dict) -> None:
        self.events.append((time.perf_counter(), stage, payload))

    def first(self, stage: str) -> dict | None:
        return next((p for _t, s, p in self.events if s == stage), None)

    def all(self, stage: str) -> list[dict]:
        return [p for _t, s, p in self.events if s == stage]

    def at(self, stage: str, last: bool = False) -> float | None:
        stamps = [t for t, s, _p in self.events if s == stage]
        if not stamps:
            return None
        return stamps[-1] if last else stamps[0]


def _ms(a: float | None, b: float | None) -> int | None:
    if a is None or b is None:
        return None
    return int((b - a) * 1000)


# ---------------------------------------------------------------------------
# The production run
# ---------------------------------------------------------------------------

class Stand:
    """One retriever per request over shared, thread-safe provider clients.

    Everything that decides WHICH passage comes back — the retriever, the
    stages, the blacklist, diversity, the safe pool, the BM25 indexes — is
    the production code and is identical in both stands. A subclass supplies
    only the three providers, the vector index they search and the labels the
    page prints, which is exactly the axis Maria wanted compared: the same
    scheme on different models (86cbegcmm).
    """

    stack = "?"
    provider_label = "?"
    budget = BUDGET_SECONDS
    embed_workers = 1
    counter = None                   # set by the subclass

    def __init__(self, corpus: Corpus) -> None:
        self.corpus = corpus
        self.index = corpus.index
        self.rewriter = None
        self.embedder = None
        self.reranker = None
        self.rewrite_model = ""
        self.rewrite_prompt_version = ""
        self.embedding_model = ""
        self.rerank_model = ""
        self.rerank_prompt_version = RERANK_PROMPT_VERSION

    def run(self, topic: str, replies: list[str]) -> dict:
        counts: dict[str, int] = {}
        sink = TraceSink()
        deadline = Deadline(self.budget)
        connection = create_connection()
        if connection is None:
            return {"stack": self.stack, "error": "cep_public is not reachable"}
        cursor = connection.cursor(dictionary=True)
        started = time.perf_counter()
        try:
            retriever = ScriptureRetriever(
                index=self.index,
                embedder=CountingEmbedder(self.embedder, counts, self.counter),
                rewriter=CountingRewriter(self.rewriter, counts, self.counter),
                reranker=CountingReranker(self.reranker, counts, self.counter),
                load_passages=make_db_passage_loader(cursor),
                load_verses=make_db_verse_loader(cursor),
                lexical_indexes=self.corpus.lexical,
                embed_workers=self.embed_workers,
                trace=sink,
            )
            final = retriever.select_final(
                SelectionRequest(
                    language=LANGUAGE,
                    topic=topic,
                    user_replies=tuple(replies),
                    top_k=TOP_K,
                ),
                deadline,
            )
        except Exception as exc:                # a stand, not a service
            return {
                "stack": self.stack,
                "error": f"{type(exc).__name__}: {exc}",
                "provider_calls": counts,
                "ms": int((time.perf_counter() - started) * 1000),
            }
        finally:
            cursor.close()
            connection.close()
        total_ms = int((time.perf_counter() - started) * 1000)
        return self._describe(final, sink, counts, topic, replies, total_ms)

    # -- presentation -------------------------------------------------------

    def _rows(self, hits, limit: int) -> list[dict]:
        """(canonical_id, score) pairs -> table rows."""
        rows = []
        for rank, (canonical_id, score) in enumerate(hits[:limit], start=1):
            rows.append({
                "rank": rank,
                "canonical_id": canonical_id,
                "reference": self.corpus.reference(canonical_id),
                "score": round(float(score), 4),
                "first_words": self.corpus.first_words(canonical_id),
            })
        return rows

    def _describe(self, final, sink: TraceSink, counts: dict,
                  topic: str, replies: list[str], total_ms: int) -> dict:
        corpus = self.corpus
        selection = final.selection
        rewrite = sink.first("rewrite") or {}
        searches = sink.all("variant_search")
        skipped_variants = sink.all("variant_skipped")
        fused = (sink.first("fused") or {}).get("hits") or []
        filtered = sink.first("filtered") or {}
        diversity = sink.first("diversity") or {}
        rerank_request = sink.first("rerank_request")
        rerank_choice = sink.first("rerank_choice")
        rerank_failed = sink.first("rerank_failed")
        safe_pool = sink.first("safe_pool")

        variants = []
        for payload in searches:
            variants.append({
                "index": payload.get("variant"),
                "query": payload.get("query", ""),
                "semantic": self._rows(payload.get("semantic") or [], TABLE_K),
                "lexical": self._rows(payload.get("lexical") or [], TABLE_K),
                "merged": self._rows(payload.get("merged") or [], TABLE_K),
            })

        cut = {"excluded": [], "blacklist": [], "coverage": []}
        for hit, reason, entry in filtered.get("dropped") or []:
            row = {
                "canonical_id": hit.canonical_id,
                "reference": corpus.reference(hit.canonical_id),
                "rule": "",
            }
            if entry is not None:
                chapters = (
                    f"{entry.chapter_from}"
                    if entry.chapter_from == entry.chapter_to
                    else f"{entry.chapter_from}-{entry.chapter_to}"
                )
                verses = (
                    "" if entry.verse_from is None
                    else f":{entry.verse_from}-{entry.verse_to}"
                )
                book = corpus.book_names.get(entry.book, f"кн.{entry.book}")
                row["rule"] = f"{entry.genre} — {book} {chapters}{verses}"
            cut.setdefault(reason, []).append(row)

        diversity_cut = [
            {
                "canonical_id": hit.canonical_id,
                "reference": corpus.reference(hit.canonical_id),
                "reason": (
                    "лимит по книге (максимум 4)" if reason == "book_cap"
                    else "лимит по главе (максимум 1)"
                ),
            }
            for hit, reason in diversity.get("skipped") or []
        ]

        candidates = []
        for number, candidate in enumerate(selection.candidates, start=1):
            passage = prompt_passage(candidate)
            text = (passage.text if passage else "") or ""
            candidates.append({
                "n": number,
                "canonical_id": candidate.canonical_id,
                "reference": corpus.reference(candidate.canonical_id),
                "score": None if candidate.score is None
                else round(float(candidate.score), 4),
                "from_variant": candidate.best_variant,
                "verses": len(passage.verses) if passage else 0,
                "head": " ".join(text.split())[:200]
                + ("…" if len(text) > 200 else ""),
            })

        chosen = final.candidate
        chosen_passage = prompt_passage(chosen) if chosen is not None else None
        highlight = None
        if final.highlight and chosen_passage and chosen_passage.verses:
            start, end = final.highlight
            marked = chosen_passage.verses[start - 1:end]
            highlight = {
                "markers": [start, end],
                "chapter": chosen_passage.chapter_number,
                "verse_start": marked[0].verse_number if marked else None,
                "verse_end": marked[-1].verse_number if marked else None,
                "text": " ".join(v.text for v in marked),
            }

        t0 = sink.started
        t_rewrite = sink.at("rewrite")
        t_search = sink.at("variant_search", last=True)
        t_candidates = sink.at("candidates") or sink.at("safe_pool")
        t_rerank_start = sink.at("rerank_request")
        t_rerank_end = sink.at("rerank_choice") or sink.at("rerank_failed")

        return {
            "stack": self.stack,
            "provider_label": self.provider_label,
            "input": {
                "language": LANGUAGE,
                "topic": topic,
                "replies": replies,
                "translation": corpus.translation,
                "alias": corpus.alias,
                "raw_query": build_search_query(topic, replies),
            },
            "rewrite": {
                "model": self.rewrite_model,
                "prompt_version": self.rewrite_prompt_version,
                "asked": REWRITE_VARIANTS,
                "failed": bool(rewrite.get("rewrite_failed")),
                "variants": list(rewrite.get("variants") or []),
                "queries": list(rewrite.get("queries") or []),
                "ms": _ms(t0, t_rewrite),
            },
            "embedding_model": self.embedding_model,
            "variants": variants,
            "variants_not_embedded": [
                p.get("variant") for p in skipped_variants
            ],
            "interleave": self._rows(
                [(h.canonical_id, h.score) for h in fused], 20
            ),
            "interleave_total": len(fused),
            "interleave_from_variant": {
                h.canonical_id: h.best_variant for h in fused[:20]
            },
            "cut": cut,
            "diversity_cut": diversity_cut,
            "candidates": candidates,
            "rerank": {
                "model": self.rerank_model,
                "prompt_version": self.rerank_prompt_version,
                "asked_key_verses": bool(
                    (rerank_request or {}).get("key_verses")
                ),
                "candidates_shown": len((rerank_request or {}).get("texts") or []),
                "index": (
                    rerank_choice["choice"].index if rerank_choice else None
                ),
                "reason": (
                    rerank_choice["choice"].reason if rerank_choice else None
                ),
                "error": (rerank_failed or {}).get("error"),
                "method": final.method,
                "fallback_reason": final.fallback_reason,
                "ms": _ms(t_rerank_start, t_rerank_end),
            },
            "safe_pool": None if safe_pool is None
            else {"reason": safe_pool.get("reason")},
            "chosen": None if chosen is None else {
                "canonical_id": chosen.canonical_id,
                "reference": corpus.reference(chosen.canonical_id),
                "rank": selection.candidates.index(chosen) + 1,
                "title": chosen_passage.title if chosen_passage else None,
                "passage": chosen_passage.text if chosen_passage else "",
                "first_words": corpus.first_words(chosen.canonical_id),
                "source": selection.source,
                "fallback_reason": selection.fallback_reason,
            },
            "highlight": highlight,
            "timings_ms": {
                "rewrite": _ms(t0, t_rewrite),
                "search": _ms(t_rewrite, t_search),
                "rank": _ms(t_search, t_candidates),
                "rerank": _ms(t_rerank_start, t_rerank_end),
                "total": total_ms,
            },
            "provider_calls": counts,
        }


class GeminiStand(Stand):
    """The production pipeline on the production providers."""

    stack = "gemini"
    provider_label = "Gemini"
    budget = BUDGET_SECONDS
    # Variant embeddings are independent round trips; the public endpoint
    # embeds them concurrently and so does this column (ADR 0006).
    embed_workers = REWRITE_VARIANTS
    counter = COUNTER

    def __init__(self, corpus: Corpus) -> None:
        super().__init__(corpus)
        self.rewriter = GeminiQueryRewriter(
            timeout=PROVIDER_TIMEOUT_SECONDS, attempts=PROVIDER_ATTEMPTS
        )
        self.embedder = GeminiEmbeddingClient(
            timeout=PROVIDER_TIMEOUT_SECONDS, max_retries=PROVIDER_ATTEMPTS
        )
        self.reranker = GeminiPassageReranker(
            timeout=PROVIDER_TIMEOUT_SECONDS, attempts=PROVIDER_ATTEMPTS
        )
        self.rewrite_model = config.AI_SCRIPTURE_REWRITE_MODEL
        self.rewrite_prompt_version = REWRITE_PROMPT_VERSION
        self.embedding_model = config.EMBEDDING_MODEL
        self.rerank_model = config.AI_SCRIPTURE_RERANK_MODEL


class LocalStand(Stand):
    """The SAME production pipeline on local models (ClickUp 86cbegcmm).

    Constructed once at start-up: bge-m3 is 2.3 GB of weights and the
    document matrix is 11 960 x 1024 float32 (~49 MB), so both are loaded
    exactly once and shared by every request.
    """

    stack = "local-prod"
    provider_label = "локальные модели"
    budget = LOCAL_BUDGET_SECONDS
    # See LocalEmbedder: one CPU model behind a lock, so concurrency here
    # would only queue on that lock while thrashing the box.
    embed_workers = 1
    counter = LOCAL_COUNTER

    def __init__(self, corpus: Corpus) -> None:
        super().__init__(corpus)
        self.client = build_local_client()
        self.index = load_local_index()
        self.embedder = load_local_embedder()
        self.rewriter = QwenQueryRewriter(self.client)
        self.reranker = QwenPassageReranker(self.client)
        self.rewrite_model = LOCAL_REWRITE_MODEL
        self.rewrite_prompt_version = f"{LOCAL_REWRITE_PROMPT} (локальный)"
        self.embedding_model = f"{LOCAL_EMBED_MODEL} ({LOCAL_EMBED_VARIANT})"
        self.rerank_model = LOCAL_RERANK_MODEL
        assert_no_gemini_clients(self)

    def close(self) -> None:
        self.client.close()


def assert_no_gemini_clients(stand: Stand) -> None:
    """Tripwire, structural half: this column holds no Gemini client.

    The transport guard stops a call that is attempted; this stops the
    object that would attempt it from existing at all — including the case
    where a future edit passes the wrong provider into the local stand.
    """
    forbidden = (
        GeminiQueryRewriter, GeminiEmbeddingClient, GeminiPassageReranker
    )
    for name in ("rewriter", "embedder", "reranker"):
        provider = getattr(stand, name)
        if isinstance(provider, forbidden):
            raise TripwireError(
                f"{stand.stack} stand holds a Gemini {name}: "
                f"{type(provider).__name__}"
            )


# ---------------------------------------------------------------------------
# The senses stand (HTTP to local_picker.py on 9089)
# ---------------------------------------------------------------------------

def run_local(topic: str, replies: list[str]) -> dict:
    """Ask the local stand the same prayer and keep its answer as it is.

    The local picker takes one text field, so topic and replies are joined
    the way it joins them itself.
    """
    query = "\n".join([topic] + replies).strip()
    url = f"{LOCAL_URL}/json?" + urllib.parse.urlencode({"q": query})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=LOCAL_TIMEOUT) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        return {"stack": "local", "error": f"{LOCAL_URL} недоступен: {exc.reason}"}
    except Exception as exc:
        return {"stack": "local", "error": f"{type(exc).__name__}: {exc}"}
    payload["stack"] = "local"
    payload["wall_ms"] = int((time.perf_counter() - started) * 1000)
    return payload


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

PAGE_CSS = """
:root { color-scheme: light dark; --line: #ddd; --dim: #777; }
body { font: 15px/1.5 -apple-system, "Segoe UI", Roboto, sans-serif;
       max-width: 1600px; margin: 0 auto; padding: 20px 16px 80px; }
h1 { font-size: 20px; margin: 0 0 4px; }
h2 { font-size: 16px; margin: 22px 0 6px; }
h3 { font-size: 14px; margin: 16px 0 4px; text-transform: uppercase;
     letter-spacing: .04em; color: var(--dim); }
p.sub { color: var(--dim); font-size: 13px; margin: 0 0 14px; }
textarea { width: 100%; box-sizing: border-box; font: inherit; padding: 9px;
           border: 1px solid #bbb; border-radius: 6px; }
textarea.topic { min-height: 74px; }
textarea.replies { min-height: 52px; }
label { font-size: 13px; color: var(--dim); display: block; margin-top: 8px; }
button { font: inherit; padding: 9px 22px; border-radius: 6px; border: 0;
         background: #2f6f4f; color: #fff; cursor: pointer; margin-top: 10px; }
.bill { background: #fff6d8; border: 1px solid #e6cf7a; color: #6b5200;
        border-radius: 6px; padding: 9px 12px; font-size: 13px;
        margin: 14px 0; }
.err { background: #ffdede; border: 1px solid #e0a0a0; color: #8a1c1c;
       border-radius: 6px; padding: 9px 12px; margin: 10px 0; font-size: 14px; }
.cols { display: grid; grid-template-columns: 1fr 1fr; gap: 22px;
        margin-top: 18px; }
.col { border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px;
       min-width: 0; }
.col.gem { border-color: #b08a3a; }
.col.loc { border-color: #4c7a9a; }
.top { display: grid; grid-template-columns: 1fr 1fr; gap: 22px;
       margin-top: 16px; }
.result { border: 2px solid #2f6f4f; border-radius: 8px; padding: 12px 14px; }
.result h2 { margin: 0 0 4px; font-size: 19px; }
.meta { color: var(--dim); font-size: 12.5px; }
.passage { white-space: pre-wrap; margin: 8px 0; font-size: 14px; }
.hl { background: #ffe9a8; color: #222; padding: 1px 2px; }
table { border-collapse: collapse; width: 100%; font-size: 12.5px;
        margin: 4px 0 10px; table-layout: fixed; }
th, td { border-bottom: 1px solid var(--line); padding: 4px 5px;
         text-align: left; vertical-align: top; word-wrap: break-word; }
th { font-size: 11px; text-transform: uppercase; color: var(--dim); }
td.num, th.num { width: 2.2em; }
td.ref { width: 8.5em; }
td.sc { width: 4.2em; }
ol.variants { padding-left: 20px; margin: 4px 0; font-size: 13.5px; }
ol.variants li { margin-bottom: 3px; }
.stage { border-top: 1px dashed var(--line); padding-top: 6px; margin-top: 12px; }
.none { color: var(--dim); font-size: 13px; }
.pill { display: inline-block; background: #eee; border-radius: 10px;
        padding: 1px 8px; font-size: 12px; margin-right: 5px; }
a.pill { text-decoration: none; color: inherit; }
.pill.on { background: #2f6f4f; color: #fff; }
code { font-size: 12px; }
@media (prefers-color-scheme: dark) {
  :root { --line: #333; --dim: #999; }
  body { background: #14171a; color: #e6e6e6; }
  textarea { background: #1d2124; color: #e6e6e6; border-color: #444; }
  .bill { background: #2e2a15; border-color: #6b5c1f; color: #f0dda0; }
  .err { background: #3a1c1c; border-color: #7a3a3a; color: #ffc9c9; }
  .hl { background: #6b5c1f; color: #ffe9a8; }
  .pill { background: #262b2f; }
}
@media (max-width: 1100px) { .cols, .top { grid-template-columns: 1fr; } }
"""

EXAMPLES = [
    "Сервис падает под нагрузкой третью неделю, не могу найти причину, "
    "устала, боюсь не успеть к релизу",
    "Брат на фронте, мама в прифронтовом городе, каждую ночь просыпаюсь "
    "от новостей",
    "Поссорилась с мамой, наговорила лишнего, стыдно",
]


def esc(text) -> str:
    return html.escape("" if text is None else str(text), quote=True)


def fmt_ms(value) -> str:
    """A stage that never ran has no time — say so instead of "None"."""
    return "—" if value is None else f"{value} мс"


def _table(rows: list[dict], columns: list[tuple[str, str, str]]) -> str:
    if not rows:
        return "<p class='none'>—</p>"
    head = "".join(f"<th class='{cls}'>{esc(title)}</th>"
                   for _key, title, cls in columns)
    body = []
    for row in rows:
        cells = "".join(
            f"<td class='{cls}'>{esc(row.get(key))}</td>"
            for key, _title, cls in columns
        )
        body.append(f"<tr>{cells}</tr>")
    return f"<table><tr>{head}</tr>{''.join(body)}</table>"


HIT_COLUMNS = [
    ("rank", "#", "num"), ("reference", "ссылка", "ref"),
    ("score", "score", "sc"), ("first_words", "первые слова", ""),
]


def render_trace(trace: dict) -> str:
    """One traced run of the production pipeline, whoever the providers were.

    Both columns of the `prod-local` page go through here: the payload of
    `Stand._describe` is the same shape for Gemini and for the local models,
    because the stages that produced it are literally the same code.
    """
    if not trace:
        return "<p class='none'>колонка не запускалась</p>"
    if trace.get("error"):
        return f"<div class='err'>{esc(trace['error'])}</div>"
    out = []
    inp = trace["input"]
    out.append("<h3>Вход</h3>")
    out.append(
        f"<p class='meta'>язык {esc(inp['language'])}, перевод "
        f"{esc(inp['alias'])} ({inp['translation']}), реплик "
        f"{len(inp['replies'])}, сырой запрос — {len(inp['raw_query'])} знаков"
        f"</p>"
    )

    rw = trace["rewrite"]
    out.append("<div class='stage'><h3>1. Переписывание запроса (rewrite)</h3>")
    out.append(
        f"<p class='meta'>{esc(rw['model'])}, промпт v{rw['prompt_version']}, "
        f"просили {rw['asked']} вариантов, {fmt_ms(rw['ms'])}</p>"
    )
    if rw["failed"]:
        out.append(
            "<div class='err'>rewrite_failed — ищется сырой текст молитвы</div>"
        )
    if rw["queries"]:
        out.append("<ol class='variants'>" + "".join(
            f"<li>{esc(q)}</li>" for q in rw["queries"]) + "</ol>")
    else:
        out.append("<p class='none'>вариантов нет</p>")
    out.append("</div>")

    out.append("<div class='stage'><h3>2. Поиск по каждому варианту</h3>")
    out.append(
        f"<p class='meta'>эмбеддинги {esc(trace['embedding_model'])} + косинус "
        f"по индексу фрагментов, плюс BM25 по тому же тексту (индекс BM25 "
        f"общий у обеих колонок); {fmt_ms(trace['timings_ms']['search'])} на "
        f"все варианты</p>"
    )
    if trace["variants_not_embedded"]:
        out.append(
            "<div class='err'>не удалось векторизовать варианты: "
            f"{esc(trace['variants_not_embedded'])}</div>"
        )
    for variant in trace["variants"]:
        out.append(
            f"<p class='meta'><b>вариант {variant['index'] + 1}</b>: "
            f"{esc(variant['query'])}</p>"
        )
        out.append("<p class='meta'>вектор:</p>")
        out.append(_table(variant["semantic"], HIT_COLUMNS))
        out.append("<p class='meta'>BM25:</p>")
        out.append(_table(variant["lexical"], HIT_COLUMNS))
    out.append("</div>")

    out.append("<div class='stage'><h3>3. Слияние (interleave)</h3>")
    origin = trace["interleave_from_variant"]
    rows = [
        dict(row, first_words=f"вариант {origin.get(row['canonical_id'], 0) + 1}"
             + " · " + row["first_words"])
        for row in trace["interleave"]
    ]
    out.append(
        f"<p class='meta'>всего {trace['interleave_total']} окон, показаны "
        f"первые {len(rows)}; порядок — ранг 1 каждого варианта, потом ранг 2 "
        f"и так далее</p>"
    )
    out.append(_table(rows, HIT_COLUMNS))
    out.append("</div>")

    out.append("<div class='stage'><h3>4. Что срезано</h3>")
    cut = trace["cut"]
    out.append("<p class='meta'>чёрный список жанров:</p>")
    out.append(_table(cut.get("blacklist") or [], [
        ("reference", "ссылка", "ref"), ("rule", "правило", "")]))
    if cut.get("excluded"):
        out.append("<p class='meta'>уже показывалось (exclude_canonical_ids):</p>")
        out.append(_table(cut["excluded"], [("reference", "ссылка", "ref")]))
    out.append("<p class='meta'>diversity:</p>")
    out.append(_table(trace["diversity_cut"], [
        ("reference", "ссылка", "ref"), ("reason", "причина", "")]))
    out.append("</div>")

    out.append("<div class='stage'><h3>5. Кандидаты, показанные модели</h3>")
    out.append(_table(trace["candidates"], [
        ("n", "[n]", "num"), ("reference", "ссылка", "ref"),
        ("score", "score", "sc"), ("head", "текст", "")]))
    out.append("</div>")

    rr = trace["rerank"]
    out.append("<div class='stage'><h3>6. Выбор (rerank)</h3>")
    out.append(
        f"<p class='meta'>{esc(rr['model'])}, промпт v{rr['prompt_version']}, "
        f"кандидатов {rr['candidates_shown']}, ключевые стихи "
        f"{'просили' if rr['asked_key_verses'] else 'не просили'}, "
        f"{fmt_ms(rr['ms'])}</p>"
    )
    if rr["error"]:
        out.append(f"<div class='err'>rerank не сработал: {esc(rr['error'])} "
                   f"→ {esc(rr['fallback_reason'])}</div>")
    if rr["index"] is not None:
        out.append(
            f"<p>выбран кандидат <b>[{rr['index'] + 1}]</b><br>"
            f"<span class='meta'>reason (серверная диагностика, человеку не "
            f"показывается): {esc(rr['reason'])}</span></p>"
        )
    elif not rr["error"]:
        out.append(
            f"<p class='none'>модель не выбирала: {esc(rr['fallback_reason'])}"
            f"</p>"
        )
    out.append("</div>")

    out.append("<div class='stage'><h3>7. Подсветка ключевых стихов</h3>")
    hl = trace["highlight"]
    if hl:
        out.append(
            f"<p class='meta'>маркеры [{hl['markers'][0]}]–[{hl['markers'][1]}] "
            f"→ {hl['chapter']}:{hl['verse_start']}-{hl['verse_end']}</p>"
            f"<p class='passage'>{esc(hl['text'])}</p>"
        )
    else:
        out.append("<p class='none'>нет</p>")
    out.append("</div>")

    timings = trace["timings_ms"]
    calls = trace["provider_calls"]
    out.append("<div class='stage'><h3>8. Времена и вызовы</h3>")
    out.append(
        "<p class='meta'>" + " ".join(
            f"<span class='pill'>{esc(name)} {fmt_ms(value)}</span>"
            for name, value in timings.items() if value is not None
        ) + "</p>"
    )
    out.append(
        f"<p class='meta'>вызовы провайдера "
        f"({esc(trace.get('provider_label', '?'))}) в этом запросе: rewrite "
        f"{calls.get('rewrite', 0)}, эмбеддинги {calls.get('embed', 0)}, "
        f"rerank {calls.get('rerank', 0)}</p>"
    )
    out.append("</div>")
    return "".join(out)


def render_senses(result: dict) -> str:
    if not result:
        return "<p class='none'>колонка не запускалась</p>"
    if result.get("error"):
        return f"<div class='err'>{esc(result['error'])}</div>"
    out = []
    choice = result.get("choice") or {}
    out.append("<h3>Вход</h3>")
    out.append(
        f"<p class='meta'>язык ru, перевод syn, сырой запрос — "
        f"{len(result.get('query', ''))} знаков (тема и реплики склеены "
        f"построчно — локальный стенд принимает одно поле)</p>"
    )
    out.append("<div class='stage'><h3>1. Переписывание запроса</h3>")
    out.append(
        "<p class='none'>нет: локальная схема ищет сырой текст молитвы. "
        "Роль rewrite здесь играет индекс смыслов — у каждого фрагмента "
        "заранее записано, о чём он, и запрос сравнивается со смыслами, а не "
        "с библейским текстом.</p>"
    )
    out.append("</div>")

    candidates = result.get("candidates") or []
    out.append("<div class='stage'><h3>2. Поиск по смыслам + BM25</h3>")
    out.append(
        f"<p class='meta'>bge-m3 по смыслам, BM25 по тексту, "
        f"{result.get('search_ms', 0)} мс. Столбец «нашлось по смыслу» — та "
        f"строка смысла, которая выиграла фрагмент.</p>"
    )
    out.append(_table(
        [
            {
                "rank": i,
                "reference": c["reference"],
                "score": c["score"],
                "source": c["source"],
                "sense": c["sense"] or "—",
                "first_words": c["first_words"],
            }
            for i, c in enumerate(candidates, start=1)
        ],
        [("rank", "#", "num"), ("reference", "ссылка", "ref"),
         ("score", "score", "sc"), ("source", "откуда", "sc"),
         ("sense", "нашлось по смыслу", ""), ("first_words", "первые слова", "")],
    ))
    out.append("</div>")

    out.append("<div class='stage'><h3>3. Слияние, чёрный список, diversity</h3>")
    out.append(
        "<p class='none'>те же функции app/retrieval (interleave → чёрный "
        "список жанров → лимиты по книге и главе), но локальный стенд отдаёт "
        "наружу только результат — что именно было срезано, он не сообщает."
        "</p>"
    )
    out.append("</div>")

    out.append("<div class='stage'><h3>4. Кандидаты</h3>")
    out.append(
        f"<p class='meta'>всего {result.get('candidates_total', len(candidates))}"
        f", показаны первые {len(candidates)} (столько отдаёт локальный "
        f"стенд)</p>"
    )
    out.append("</div>")

    out.append("<div class='stage'><h3>5. Выбор (Qwen, локально)</h3>")
    if choice.get("ok"):
        rank = result.get("chosen_rank")
        out.append(
            f"<p>выбран кандидат <b>[{rank}]</b> из "
            f"{result.get('candidates_total')}, {choice.get('ms')} мс<br>"
            f"<span class='meta'>reason: {esc(choice.get('reason'))}</span></p>"
        )
    else:
        out.append(f"<div class='err'>{esc(choice.get('reason'))}</div>")
    out.append("</div>")

    out.append("<div class='stage'><h3>6. Подсветка ключевых стихов</h3>")
    hl = result.get("highlight")
    if hl:
        out.append(
            f"<p class='meta'>{hl['chapter']}:{hl['verse_start']}-"
            f"{hl['verse_end']}</p><p class='passage'>{esc(hl.get('text'))}</p>"
        )
    else:
        out.append("<p class='none'>нет</p>")
    out.append("</div>")

    out.append("<div class='stage'><h3>7. Времена и вызовы</h3>")
    out.append(
        f"<p class='meta'><span class='pill'>поиск "
        f"{result.get('search_ms', 0)} мс</span>"
        f"<span class='pill'>всего {result.get('ms', 0)} мс</span>"
        f"<span class='pill'>очередь {result.get('queue_ms', 0)} мс</span></p>"
        f"<p class='meta'>вызовов внешнего провайдера: 0 (стенд физически "
        f"не может выйти наружу — в нём стоит tripwire)</p>"
    )
    out.append("</div>")
    return "".join(out)


def _result_card(title: str, reference: str, first_words: str,
                 note: str) -> str:
    return (
        f"<div class='result'><div class='meta'>{esc(title)}</div>"
        f"<h2>{esc(reference)}</h2>"
        f"<div class='passage'>{esc(first_words)}</div>"
        f"<div class='meta'>{esc(note)}</div></div>"
    )


RIGHT_TITLES = {
    "prod-local": "Боевая схема — локальные модели",
    "senses": "Локальная схема (смыслы) — 9089",
}
RIGHT_CARD_TITLES = {
    "prod-local": "боевая схема (локальные модели)",
    "senses": "локальная схема (смыслы + Qwen, 9089)",
}


def local_stack_line() -> str:
    """The one-line description of what the local column actually runs."""
    return (
        f"rewrite: {LOCAL_REWRITE_MODEL or '—'} (промпт {LOCAL_REWRITE_PROMPT}) · "
        f"эмбеддинги: {LOCAL_EMBED_MODEL} · "
        f"rerank: {LOCAL_RERANK_MODEL or '—'} (v{RERANK_PROMPT_VERSION})"
    )


def _passage_with_highlight(chosen: dict, trace: dict) -> str:
    body = esc(chosen["passage"])
    hl = trace.get("highlight")
    if hl and hl.get("text"):
        needle = esc(hl["text"])
        if needle in body:
            body = body.replace(needle, f"<span class='hl'>{needle}</span>", 1)
    return body


def _switch(right: str, topic: str, replies_text: str) -> str:
    """The right-column switch — both columns stay one click apart."""
    options = []
    for key in RIGHT_COLUMNS:
        label = esc(RIGHT_TITLES[key])
        if key == right:
            options.append(f"<span class='pill on'>{label}</span>")
        else:
            query = urllib.parse.urlencode(
                {"q": topic, "replies": replies_text, "r": key}
            )
            options.append(f"<a class='pill' href='/?{query}'>{label}</a>")
    stack = (
        f"<br><span class='meta'>{esc(local_stack_line())}</span>"
        if right == "prod-local" else ""
    )
    return (
        "<p class='meta'>Правая колонка: " + " ".join(options)
        + " <span class='meta'>(параметр <code>r</code>; реплики — "
        "<code>replies</code>)</span>" + stack + "</p>"
    )


def render_page(topic: str, replies_text: str, right: str,
                gemini: dict | None, other: dict | None) -> str:
    session = COUNTER.snapshot()
    local_session = LOCAL_COUNTER.snapshot()
    parts = [
        "<h1>Трассировка подбора Писания: боевая схема на разных моделях</h1>",
        "<p class='sub'>Слева — тот же код, что отвечает мобильному "
        "приложению, на Gemini: rewrite → эмбеддинги вариантов → вектор + "
        "BM25 → interleave → чёрный список → diversity → кандидаты → rerank → "
        "подсветка. Справа — либо <b>та же схема на локальных моделях</b> "
        "(тот же <code>ScriptureRetriever.select_final</code>, тот же trace-хук, "
        "подменены только три провайдера), либо старая локальная схема со "
        "стенда 9089 (индекс смыслов вместо rewrite). Русский, перевод syn; "
        "BM25, чёрный список, diversity и safe pool — боевые и общие.</p>",
        f"<div class='bill'>Левая колонка ходит в <b>Gemini</b> и стоит "
        f"денег и квоты: один запрос — 1 rewrite ({esc(config.AI_SCRIPTURE_REWRITE_MODEL)}, "
        f"платный ключ), до {REWRITE_VARIANTS} эмбеддингов и 1 rerank "
        f"({esc(config.AI_SCRIPTURE_RERANK_MODEL)}). "
        f"За эту сессию: rewrite {session['rewrite']}, эмбеддинги "
        f"{session['embed']}, rerank {session['rerank']}. "
        f"Правая колонка Gemini не трогает — её счётчик отдельный: rewrite "
        f"{local_session['rewrite']}, эмбеддинги {local_session['embed']}, "
        f"rerank {local_session['rerank']}.</div>",
        _switch(right, topic, replies_text),
        "<form method='post' action='/'>",
        f"<input type='hidden' name='r' value='{esc(right)}'>",
        "<label for='q'>Молитва</label>",
        f"<textarea class='topic' id='q' name='q' autofocus "
        f"placeholder='Текст молитвы по-русски…'>{esc(topic)}</textarea>",
        "<label for='replies'>Реплики (необязательно, по одной в строке)"
        "</label>",
        f"<textarea class='replies' id='replies' name='replies'>"
        f"{esc(replies_text)}</textarea>",
        "<button type='submit'>Прогнать обе схемы</button>",
        "</form>",
    ]
    if gemini is None and other is None:
        parts.append("<p class='meta'>Примеры: " + " · ".join(
            f"<a href='/?q={urllib.parse.quote(ex)}&r={right}'>"
            f"{esc(ex[:52])}…</a>"
            for ex in EXAMPLES) + "</p>")
        return "".join(parts)

    traced_right = right == "prod-local"
    gem_chosen = (gemini or {}).get("chosen")
    other_chosen = (other or {}).get("chosen")
    parts.append("<div class='top'>")
    if gem_chosen:
        rr = gemini["rerank"]
        note = (
            f"кандидат № {gem_chosen['rank']} · источник "
            f"{gem_chosen['source']}"
            + (f" ({gem_chosen['fallback_reason']})"
               if gem_chosen["fallback_reason"] else "")
            + f" · {fmt_ms(gemini['timings_ms']['total'])} · метод {rr['method']}"
        )
        parts.append(_result_card(
            "боевая схема (Gemini)", gem_chosen["reference"],
            gem_chosen["first_words"], note))
    else:
        parts.append(_result_card(
            "боевая схема (Gemini)", "—",
            (gemini or {}).get("error", "нет результата"), ""))
    card_title = RIGHT_CARD_TITLES[right]
    if other_chosen and traced_right:
        rr = other["rerank"]
        note = (
            f"кандидат № {other_chosen['rank']} · источник "
            f"{other_chosen['source']}"
            + (f" ({other_chosen['fallback_reason']})"
               if other_chosen["fallback_reason"] else "")
            + f" · {fmt_ms(other['timings_ms']['total'])} · метод {rr['method']}"
        )
        parts.append(_result_card(
            card_title, other_chosen["reference"],
            other_chosen["first_words"], note))
    elif other_chosen:
        parts.append(_result_card(
            card_title, other_chosen["reference"], other_chosen["first_words"],
            f"кандидат № {other.get('chosen_rank', '?')} · "
            f"{other.get('ms', 0)} мс"))
    else:
        parts.append(_result_card(
            card_title, "—",
            (other or {}).get("error", "нет результата"), ""))
    parts.append("</div>")

    parts.append("<div class='cols'>")
    parts.append("<div class='col gem'><h2>Боевая схема — Gemini</h2>"
                 + render_trace(gemini or {}) + "</div>")
    if traced_right:
        parts.append(
            f"<div class='col loc'><h2>{esc(RIGHT_TITLES[right])}</h2>"
            f"<p class='meta'>{esc(local_stack_line())}</p>"
            + render_trace(other or {}) + "</div>"
        )
    else:
        parts.append(
            f"<div class='col loc'><h2>{esc(RIGHT_TITLES[right])}</h2>"
            + render_senses(other or {}) + "</div>"
        )
    parts.append("</div>")

    if gem_chosen and gem_chosen["passage"]:
        parts.append("<h2>Текст, выбранный боевой схемой (Gemini)</h2>")
        if gem_chosen["title"]:
            parts.append(f"<p class='meta'>{esc(gem_chosen['title'])}</p>")
        parts.append(
            f"<div class='passage'>"
            f"{_passage_with_highlight(gem_chosen, gemini or {})}</div>"
        )
    if traced_right and other_chosen and other_chosen["passage"]:
        parts.append(f"<h2>Текст, выбранный правой колонкой ({esc(card_title)})"
                     f"</h2>")
        if other_chosen["title"]:
            parts.append(f"<p class='meta'>{esc(other_chosen['title'])}</p>")
        parts.append(
            f"<div class='passage'>"
            f"{_passage_with_highlight(other_chosen, other or {})}</div>"
        )
    elif not traced_right and other and other.get("passage"):
        parts.append("<h2>Текст, выбранный локальной схемой</h2>")
        parts.append(f"<div class='passage'>{esc(other['passage'])}</div>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

class Stands:
    """The stands this process holds: Gemini always, the local one if it could
    be built. A local stack that is not configured is REPORTED, never
    silently replaced by the senses column or by a rewrite-less run."""

    def __init__(self, corpus: Corpus, with_local: bool = True) -> None:
        self.gemini = GeminiStand(corpus)
        self.local: LocalStand | None = None
        self.local_error = ""
        if not with_local:
            self.local_error = "локальная колонка отключена (--no-local)"
            return
        missing = local_stack_missing()
        if missing:
            self.local_error = (
                "локальная колонка не настроена: не заданы "
                + ", ".join(missing)
                + " (их заполняет run_trace_picker.sh)"
            )
            return
        started = time.perf_counter()
        try:
            self.local = LocalStand(corpus)
        except Exception as exc:            # a stand, not a service
            self.local_error = f"{type(exc).__name__}: {exc}"
            return
        print(
            f"[trace] local stack ready in {time.perf_counter() - started:.1f} s: "
            f"{len(self.local.index)} vectors, {local_stack_line()}",
            flush=True,
        )

    def right_result(self, right: str, topic: str, replies: list[str]) -> dict:
        if right == "senses":
            return run_local(topic, replies)
        if self.local is None:
            return {"stack": "local-prod", "error": self.local_error}
        return self.local.run(topic, replies)


def pick_right(raw: str) -> tuple[str, str]:
    """Normalise `?r=`; an unknown value is named, never silently ignored."""
    value = (raw or "").strip()
    if not value:
        return DEFAULT_RIGHT, ""
    if value in RIGHT_COLUMNS:
        return value, ""
    return DEFAULT_RIGHT, (
        f"неизвестное значение r={value!r} — показана колонка "
        f"{DEFAULT_RIGHT}. Допустимо: {', '.join(RIGHT_COLUMNS)}. "
        f"Реплики теперь передаются параметром replies, а не r."
    )


def run_both(stands: Stands, topic: str, replies: list[str],
             right: str) -> tuple:
    """Both stacks at once — the right one is minutes slower on this box."""
    with ThreadPoolExecutor(max_workers=2) as pool:
        gemini = pool.submit(stands.gemini.run, topic, replies)
        other = pool.submit(stands.right_result, right, topic, replies)
        return gemini.result(), other.result()


def serve(stands: Stands, port: int) -> None:
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        # The prayer text never reaches the log: the default access line
        # prints the query string, so the path is cut at the '?'.
        def log_message(self, fmt, *args):      # noqa: A003
            sys.stderr.write(
                f"{self.command} {self.path.split('?')[0]} "
                f"{args[1] if len(args) > 1 else ''}\n"
            )

        def log_run(self, topic: str, right: str, gemini: dict,
                    other: dict) -> None:
            chosen = (gemini or {}).get("chosen") or {}
            other_chosen = (other or {}).get("chosen") or {}
            other_ms = (
                (other or {}).get("timings_ms", {}).get("total")
                or (other or {}).get("ms", "-")
            )
            sys.stderr.write(
                f"[trace] q={len(topic)} chars, right={right}, gemini "
                f"{(gemini or {}).get('timings_ms', {}).get('total', '-')} ms "
                f"-> {chosen.get('canonical_id', '-')}, right "
                f"{other_ms} ms -> "
                f"{other_chosen.get('canonical_id', '-')}, session calls "
                f"gemini={COUNTER.snapshot()} local={LOCAL_COUNTER.snapshot()}\n"
            )
            sys.stderr.flush()

        def _send(self, body: str, status: int = 200) -> None:
            page = (
                "<meta charset='utf-8'>"
                "<meta name='viewport' content='width=device-width,"
                "initial-scale=1'>"
                "<title>Трассировка подбора Писания</title>"
                f"<style>{PAGE_CSS}</style>{body}"
            )
            raw = page.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_json(self, payload: dict) -> None:
            raw = json.dumps(payload, ensure_ascii=False, indent=2,
                             default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _page(self, topic, replies_text, right, note,
                  gemini=None, other=None) -> None:
            body = render_page(topic, replies_text, right, gemini, other)
            if note:
                body = f"<div class='err'>{esc(note)}</div>" + body
            self._send(body)

        def do_GET(self):                        # noqa: N802
            split = urlsplit(self.path)
            params = parse_qs(split.query)
            topic = (params.get("q", [""])[0]).strip()
            replies_text = params.get("replies", [""])[0]
            replies = [r.strip() for r in replies_text.splitlines() if r.strip()]
            right, note = pick_right(params.get("r", [""])[0])
            if split.path == "/json":
                gemini, other = run_both(stands, topic, replies, right)
                self.log_run(topic, right, gemini, other)
                self._send_json({"gemini": gemini, "right": other,
                                 "right_column": right})
                return
            if split.path not in ("/", "/index.html"):
                self._send("<h1>404</h1>", 404)
                return
            if not topic:
                self._page("", replies_text, right, note)
                return
            gemini, other = run_both(stands, topic, replies, right)
            self.log_run(topic, right, gemini, other)
            self._page(topic, replies_text, right, note, gemini, other)

        def do_POST(self):                       # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length).decode("utf-8", "replace")
            form = parse_qs(raw)
            topic = (form.get("q", [""])[0]).strip()
            replies_text = form.get("replies", [""])[0]
            replies = [r.strip() for r in replies_text.splitlines() if r.strip()]
            right, note = pick_right(form.get("r", [""])[0])
            if not topic:
                self._page("", replies_text, right, note)
                return
            gemini, other = run_both(stands, topic, replies, right)
            self.log_run(topic, right, gemini, other)
            self._page(topic, replies_text, right, note, gemini, other)

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[trace] http://0.0.0.0:{port}/  (Ctrl-C to stop)", flush=True)
    server.serve_forever()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Trace the production scripture selection next to the "
                    "local one")
    parser.add_argument("query", nargs="*", help="prayer text (Russian)")
    parser.add_argument("--serve", action="store_true",
                        help=f"run the page on port {DEFAULT_PORT}")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--reply", action="append", default=[],
                        help="one picked reply (repeatable)")
    parser.add_argument("--gemini-only", action="store_true",
                        help="skip the right column entirely")
    parser.add_argument("--right", choices=RIGHT_COLUMNS, default=DEFAULT_RIGHT,
                        help="which right column to run (default: "
                             f"{DEFAULT_RIGHT})")
    parser.add_argument("--no-local", action="store_true",
                        help="do not load bge-m3 (the senses column still "
                             "works; prod-local then reports it is off)")
    args = parser.parse_args(argv)

    if not config.GEMINI_API_KEY:
        print("GEMINI_API_KEY is not set — this stand IS the Gemini path",
              file=sys.stderr)
        return 2
    corpus = Corpus()
    print(f"[trace] corpus loaded in {corpus.load_seconds:.1f} s: "
          f"{len(corpus.index)} vectors, translation {corpus.alias}",
          flush=True)
    # Serving keeps both right columns one click apart, so the local stack is
    # loaded unless it was explicitly turned off; a one-shot CLI run only
    # pays the 2.3 GB of weights when it is the column actually asked for.
    with_local = (
        not args.no_local and not args.gemini_only
        and (args.serve or args.right == "prod-local")
    )
    stands = Stands(corpus, with_local=with_local)
    if stands.local_error and with_local:
        print(f"[trace] {stands.local_error}", file=sys.stderr, flush=True)
    if args.serve:
        serve(stands, args.port)
        return 0
    if not args.query:
        parser.error("give a prayer text, or --serve")
    topic = " ".join(args.query)
    if args.gemini_only:
        payload = {"gemini": stands.gemini.run(topic, args.reply)}
    else:
        gemini, other = run_both(stands, topic, args.reply, args.right)
        payload = {"gemini": gemini, "right": other,
                   "right_column": args.right}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
