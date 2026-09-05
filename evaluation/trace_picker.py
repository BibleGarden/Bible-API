#!/usr/bin/env python3
"""
Side-by-side TRACE of the production scripture selection (ClickUp 86cbegawh,
umbrella 86cbe4mtq).

What it is: a hand-driven stand that runs the REAL production pipeline
(`app/retrieval.ScriptureRetriever.select_final` on Gemini) over a prayer
typed into a web page, and shows every stage it went through — the rewrite
variants, what each variant found by vector and by BM25, what the interleave
produced, what the genre blacklist and the diversity caps cut, the candidate
list the model was shown, the choice it made and why, the key verses. The
same page shows, in the same stage order, what the LOCAL stack
(`evaluation/local_picker.py` on port 9089, no external provider) answered
for the same prayer, so the two can be compared line by line.

    ./run_trace_picker.sh --serve          # http://0.0.0.0:9090/
    ./run_trace_picker.sh "текст молитвы"  # one run to stdout as JSON

THIS STAND CALLS GEMINI AND IT COSTS MONEY. One request spends one rewrite
call (`AI_SCRIPTURE_REWRITE_API_KEY`, the paid key), up to six embedding
calls and one rerank call. The page says so at the top and counts the calls
made in this process since it started. Nothing here is served to the mobile
app and nothing here is on the production path.

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
    TRACE_PICKER_LOCAL_URL        local stand, default http://127.0.0.1:9089
    TRACE_PICKER_LOCAL_TIMEOUT    seconds to wait for it, default 150
    TRACE_PICKER_BUDGET_SECONDS   budget of one Gemini run, default 60

Privacy, same rule as the local stand: the prayer text is NEVER written to
the log — only its length, the timings and the canonical id that came back.
The API keys are never logged and never rendered.

Russian only (`ru`/`syn`), because the local stand it compares against is.
"""

from __future__ import annotations

import argparse
import html
import json
import os
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

import config  # noqa: E402
from chunking import CHUNKING_VERSION  # noqa: E402
from database import create_connection  # noqa: E402
from deadline import Deadline  # noqa: E402
from embeddings import GeminiEmbeddingClient  # noqa: E402
from lexical_index import load_lexical_indexes  # noqa: E402
from passage_highlight import load_psalm_maps  # noqa: E402
from passage_rerank import (  # noqa: E402
    RERANK_PROMPT_VERSION,
    GeminiPassageReranker,
)
from query_rewrite import (  # noqa: E402
    REWRITE_PROMPT_VERSION,
    REWRITE_VARIANTS,
    GeminiQueryRewriter,
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
from vector_index import load_index  # noqa: E402

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


class CountingRewriter:
    def __init__(self, inner, counts: dict):
        self.inner, self.counts = inner, counts

    def rewrite(self, *args, **kwargs):
        COUNTER.add("rewrite", self.counts)
        return self.inner.rewrite(*args, **kwargs)


class CountingEmbedder:
    def __init__(self, inner, counts: dict):
        self.inner, self.counts = inner, counts

    def embed_query(self, *args, **kwargs):
        COUNTER.add("embed", self.counts)
        return self.inner.embed_query(*args, **kwargs)


class CountingReranker:
    def __init__(self, inner, counts: dict):
        self.inner, self.counts = inner, counts

    def choose(self, *args, **kwargs):
        COUNTER.add("rerank", self.counts)
        return self.inner.choose(*args, **kwargs)


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

class GeminiStand:
    """One retriever per request over shared, thread-safe Gemini clients."""

    def __init__(self, corpus: Corpus) -> None:
        self.corpus = corpus
        self.rewriter = GeminiQueryRewriter(
            timeout=PROVIDER_TIMEOUT_SECONDS, attempts=PROVIDER_ATTEMPTS
        )
        self.embedder = GeminiEmbeddingClient(
            timeout=PROVIDER_TIMEOUT_SECONDS, max_retries=PROVIDER_ATTEMPTS
        )
        self.reranker = GeminiPassageReranker(
            timeout=PROVIDER_TIMEOUT_SECONDS, attempts=PROVIDER_ATTEMPTS
        )

    def run(self, topic: str, replies: list[str]) -> dict:
        counts: dict[str, int] = {}
        sink = TraceSink()
        deadline = Deadline(BUDGET_SECONDS)
        connection = create_connection()
        if connection is None:
            return {"error": "cep_public is not reachable"}
        cursor = connection.cursor(dictionary=True)
        started = time.perf_counter()
        try:
            retriever = ScriptureRetriever(
                index=self.corpus.index,
                embedder=CountingEmbedder(self.embedder, counts),
                rewriter=CountingRewriter(self.rewriter, counts),
                reranker=CountingReranker(self.reranker, counts),
                load_passages=make_db_passage_loader(cursor),
                load_verses=make_db_verse_loader(cursor),
                lexical_indexes=self.corpus.lexical,
                embed_workers=REWRITE_VARIANTS,
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
            "stack": "gemini",
            "input": {
                "language": LANGUAGE,
                "topic": topic,
                "replies": replies,
                "translation": corpus.translation,
                "alias": corpus.alias,
                "raw_query": build_search_query(topic, replies),
            },
            "rewrite": {
                "model": config.AI_SCRIPTURE_REWRITE_MODEL,
                "prompt_version": REWRITE_PROMPT_VERSION,
                "asked": REWRITE_VARIANTS,
                "failed": bool(rewrite.get("rewrite_failed")),
                "variants": list(rewrite.get("variants") or []),
                "queries": list(rewrite.get("queries") or []),
                "ms": _ms(t0, t_rewrite),
            },
            "embedding_model": config.EMBEDDING_MODEL,
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
                "model": config.AI_SCRIPTURE_RERANK_MODEL,
                "prompt_version": RERANK_PROMPT_VERSION,
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


# ---------------------------------------------------------------------------
# The local stand (HTTP to local_picker.py on 9089)
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


def render_gemini(trace: dict) -> str:
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
        f"<p class='meta'>эмбеддинги {esc(trace['embedding_model'])} "
        f"(параллельно) + косинус по chunk_embeddings, плюс BM25 по тому же "
        f"тексту; {fmt_ms(trace['timings_ms']['search'])} на все варианты</p>"
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
        f"<p class='meta'>вызовы Gemini в этом запросе: rewrite "
        f"{calls.get('rewrite', 0)}, эмбеддинги {calls.get('embed', 0)}, "
        f"rerank {calls.get('rerank', 0)}</p>"
    )
    out.append("</div>")
    return "".join(out)


def render_local(result: dict) -> str:
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


def render_page(topic: str, replies_text: str, gemini: dict | None,
                local: dict | None) -> str:
    session = COUNTER.snapshot()
    parts = [
        "<h1>Трассировка подбора Писания: боевая схема против локальной</h1>",
        "<p class='sub'>Слева — тот же код, что отвечает мобильному "
        "приложению: rewrite → эмбеддинги вариантов → вектор + BM25 → "
        "interleave → чёрный список → diversity → кандидаты → rerank → "
        "подсветка. Справа — локальная схема со стенда на 9089 (индекс "
        "смыслов + Qwen), разложенная по тем же этапам. Русский, перевод "
        "syn.</p>",
        f"<div class='bill'>Левая колонка ходит в <b>Gemini</b> и стоит "
        f"денег и квоты: один запрос — 1 rewrite ({esc(config.AI_SCRIPTURE_REWRITE_MODEL)}, "
        f"платный ключ), до {REWRITE_VARIANTS} эмбеддингов и 1 rerank "
        f"({esc(config.AI_SCRIPTURE_RERANK_MODEL)}). "
        f"За эту сессию: rewrite {session['rewrite']}, эмбеддинги "
        f"{session['embed']}, rerank {session['rerank']}.</div>",
        "<form method='post' action='/'>",
        "<label for='q'>Молитва</label>",
        f"<textarea class='topic' id='q' name='q' autofocus "
        f"placeholder='Текст молитвы по-русски…'>{esc(topic)}</textarea>",
        "<label for='r'>Реплики (необязательно, по одной в строке)</label>",
        f"<textarea class='replies' id='r' name='r'>{esc(replies_text)}</textarea>",
        "<button type='submit'>Прогнать обе схемы</button>",
        "</form>",
    ]
    if gemini is None and local is None:
        parts.append("<p class='meta'>Примеры: " + " · ".join(
            f"<a href='/?q={urllib.parse.quote(ex)}'>{esc(ex[:52])}…</a>"
            for ex in EXAMPLES) + "</p>")
        return "".join(parts)

    gem_chosen = (gemini or {}).get("chosen")
    loc_chosen = (local or {}).get("chosen")
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
    if loc_chosen:
        parts.append(_result_card(
            "локальная схема (Qwen, 9089)", loc_chosen["reference"],
            loc_chosen["first_words"],
            f"кандидат № {local.get('chosen_rank', '?')} · "
            f"{local.get('ms', 0)} мс"))
    else:
        parts.append(_result_card(
            "локальная схема (Qwen, 9089)", "—",
            (local or {}).get("error", "нет результата"), ""))
    parts.append("</div>")

    parts.append("<div class='cols'>")
    parts.append("<div class='col gem'><h2>Боевая схема — Gemini</h2>"
                 + render_gemini(gemini or {}) + "</div>")
    parts.append("<div class='col loc'><h2>Локальная схема — 9089</h2>"
                 + render_local(local or {}) + "</div>")
    parts.append("</div>")

    if gem_chosen and gem_chosen["passage"]:
        body = esc(gem_chosen["passage"])
        hl = (gemini or {}).get("highlight")
        if hl and hl.get("text"):
            needle = esc(hl["text"])
            if needle in body:
                body = body.replace(needle, f"<span class='hl'>{needle}</span>", 1)
        parts.append("<h2>Текст, выбранный боевой схемой</h2>")
        if gem_chosen["title"]:
            parts.append(f"<p class='meta'>{esc(gem_chosen['title'])}</p>")
        parts.append(f"<div class='passage'>{body}</div>")
    if local and local.get("passage"):
        parts.append("<h2>Текст, выбранный локальной схемой</h2>")
        parts.append(f"<div class='passage'>{esc(local['passage'])}</div>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

def run_both(stand: GeminiStand, topic: str, replies: list[str]) -> tuple:
    """Both stacks at once — the local one is minutes slower on this box."""
    with ThreadPoolExecutor(max_workers=2) as pool:
        gemini = pool.submit(stand.run, topic, replies)
        local = pool.submit(run_local, topic, replies)
        return gemini.result(), local.result()


def serve(stand: GeminiStand, port: int) -> None:
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        # The prayer text never reaches the log: the default access line
        # prints the query string, so the path is cut at the '?'.
        def log_message(self, fmt, *args):      # noqa: A003
            sys.stderr.write(
                f"{self.command} {self.path.split('?')[0]} "
                f"{args[1] if len(args) > 1 else ''}\n"
            )

        def log_run(self, topic: str, gemini: dict, local: dict) -> None:
            chosen = (gemini or {}).get("chosen") or {}
            local_chosen = (local or {}).get("chosen") or {}
            sys.stderr.write(
                f"[trace] q={len(topic)} chars, gemini "
                f"{(gemini or {}).get('timings_ms', {}).get('total', '-')} ms "
                f"-> {chosen.get('canonical_id', '-')}, local "
                f"{(local or {}).get('ms', '-')} ms -> "
                f"{local_chosen.get('canonical_id', '-')}, session calls "
                f"{COUNTER.snapshot()}\n"
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

        def do_GET(self):                        # noqa: N802
            split = urlsplit(self.path)
            params = parse_qs(split.query)
            topic = (params.get("q", [""])[0]).strip()
            replies = [r.strip() for r in params.get("r", []) if r.strip()]
            if split.path == "/json":
                gemini, local = run_both(stand, topic, replies)
                self.log_run(topic, gemini, local)
                self._send_json({"gemini": gemini, "local": local})
                return
            if split.path not in ("/", "/index.html"):
                self._send("<h1>404</h1>", 404)
                return
            if not topic:
                self._send(render_page("", "", None, None))
                return
            gemini, local = run_both(stand, topic, replies)
            self.log_run(topic, gemini, local)
            self._send(render_page(topic, "\n".join(replies), gemini, local))

        def do_POST(self):                       # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length).decode("utf-8", "replace")
            form = parse_qs(raw)
            topic = (form.get("q", [""])[0]).strip()
            replies_text = form.get("r", [""])[0]
            replies = [r.strip() for r in replies_text.splitlines() if r.strip()]
            if not topic:
                self._send(render_page("", replies_text, None, None))
                return
            gemini, local = run_both(stand, topic, replies)
            self.log_run(topic, gemini, local)
            self._send(render_page(topic, replies_text, gemini, local))

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
                        help="skip the local stand")
    args = parser.parse_args(argv)

    if not config.GEMINI_API_KEY:
        print("GEMINI_API_KEY is not set — this stand IS the Gemini path",
              file=sys.stderr)
        return 2
    corpus = Corpus()
    print(f"[trace] corpus loaded in {corpus.load_seconds:.1f} s: "
          f"{len(corpus.index)} vectors, translation {corpus.alias}",
          flush=True)
    stand = GeminiStand(corpus)
    if args.serve:
        serve(stand, args.port)
        return 0
    if not args.query:
        parser.error("give a prayer text, or --serve")
    topic = " ".join(args.query)
    if args.gemini_only:
        payload = {"gemini": stand.run(topic, args.reply)}
    else:
        gemini, local = run_both(stand, topic, args.reply)
        payload = {"gemini": gemini, "local": local}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
