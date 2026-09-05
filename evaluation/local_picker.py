#!/usr/bin/env python3
"""
Live LOCAL scripture picker over the sense index (ClickUp 86cbeeqjp,
umbrella 86cbe4mtq).

What it is: the retrieval pipeline of the benchmark, wired to a raw Russian
prayer text instead of a scenario file, so a human can type a prayer and see
which passage the local-only stack picks and — the part no metric shows —
WHICH sense of that passage matched. It exists to be judged by eye, not to
produce numbers; the numbers come from `retrieval_benchmark.py pipeline`.

Not part of the service. It runs on the host in `evaluation/.venv`, reuses
`retrieval_benchmark.py` (corpus, sense matrix, matrix fingerprint check) and
`app/` (BM25, genre blacklist, diversity, the v9 rerank prompt, key-verse
highlighting), and never talks to an external AI provider.

Stages, in order (the same order the production pipeline uses, minus the
rewrite stage which is the whole point of the sense index):

    raw prayer text
      -> bge-m3 query vector
      -> cosine over the SENSE matrix, de-duplicated per fragment
         (the winning row is the sense that matched, and it is reported)
      -> BM25 over the Scripture text of the same corpus
      -> interleave merge -> genre blacklist -> diversity caps -> top 10
      -> final choice by a local OpenAI-compatible model (rerank prompt v9,
         production builders and production parser) with key verses
      -> chosen passage + highlight + caution flag + a table of the top 5

Without a configured local model the final stage is skipped and the top 5 is
shown as-is, saying so — a silent "the first candidate is the answer" would
misrepresent what the stack did (project rule: no silent fallbacks).

Configuration (environment):

    LOCAL_PICKER_RERANK_ENDPOINT  OpenAI-compatible base URL, e.g.
                                  http://<host>:<port>/v1  (unset -> no
                                  final choice, top-5 only)
    LOCAL_PICKER_RERANK_MODEL     model id at that endpoint
    LOCAL_PICKER_RERANK_API_KEY   bearer token, optional
    LOCAL_PICKER_PORT             web port, default 9089
    LOCAL_PICKER_SENSES_FILE      sense artifact, default
                                  bench_data/codex_gpt5_senses_syn_v4.jsonl.
                                  Any `gen_descriptions.py` artifact works;
                                  the matrix is looked up (and built when
                                  absent) by ITS sha1, through the same
                                  benchmark code, so swapping the file cannot
                                  silently search the previous file's vectors.
                                  `LOCAL_PICKER_DESCRIPTIONS` is the former
                                  name of this variable and is still read.
    LOCAL_PICKER_EMBEDDER         default bge-m3

The endpoint address and key are deliberately NOT in this file: Maria's Qwen
server is not a public address and must not enter the repository.

Usage:

    python local_picker.py "сервис падает под нагрузкой третью неделю"
    python local_picker.py --serve            # http://0.0.0.0:9089/
    python local_picker.py --serve --port 9089

Limitations, all deliberate for this package: Russian only (the sense file
covers `syn`), no clarifying questions, `caution` is displayed but does NOT
filter, and the safe pool is used only for an empty query.
"""

from __future__ import annotations

import argparse
import html
import ipaddress
import json
import os
import socket
import sys
import threading
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "app"))

# ---------------------------------------------------------------------------
# Tripwire: this tool must not reach an external AI provider (86cbeeqjp)
#
# Not a promise in a docstring — a guard. Three layers, installed BEFORE the
# heavy imports so nothing can slip past during import:
#
#  1. every Gemini key is removed from this process's environment, so even a
#     code path that reads one finds nothing;
#  2. `socket.socket.connect` refuses every address that is not loopback,
#     private/link-local, or the host of the configured local rerank endpoint
#     (Maria's Qwen box may legitimately be a routable address — it is named
#     explicitly, and nothing else is allowed);
#  3. after the imports, the Gemini clients that `app/retrieval.py` pulls in
#     transitively (`embeddings`, `query_rewrite`, `passage_rerank`) get their
#     call surfaces replaced by raising stubs.
# ---------------------------------------------------------------------------

RERANK_ENDPOINT = os.environ.get("LOCAL_PICKER_RERANK_ENDPOINT", "").strip()
RERANK_MODEL = os.environ.get("LOCAL_PICKER_RERANK_MODEL", "").strip()
RERANK_API_KEY = os.environ.get("LOCAL_PICKER_RERANK_API_KEY", "").strip()

for _key in ("GEMINI_API_KEY", "AI_SCRIPTURE_REWRITE_API_KEY"):
    os.environ.pop(_key, None)
os.environ.setdefault("HF_HUB_OFFLINE", "1")


class TripwireError(RuntimeError):
    """An external AI call was attempted. The picker fails instead."""


def _allowed_hosts() -> set[str]:
    """Hostnames the socket guard lets through besides loopback/private."""
    if not RERANK_ENDPOINT:
        return set()
    host = urlsplit(RERANK_ENDPOINT).hostname
    return {host} if host else set()


_ALLOWED_HOSTS = _allowed_hosts()
_ALLOWED_IPS: set[str] = set()
for _host in _ALLOWED_HOSTS:
    try:
        _ALLOWED_IPS.update(
            info[4][0] for info in socket.getaddrinfo(_host, None)
        )
    except OSError:
        pass

_real_connect = socket.socket.connect


def _guarded_connect(self, address):
    host = address[0] if isinstance(address, tuple) else address
    text = str(host)
    if text.startswith("/") or text in _ALLOWED_HOSTS or text in _ALLOWED_IPS:
        return _real_connect(self, address)
    try:
        ip = ipaddress.ip_address(text)
        if ip.is_loopback or ip.is_private or ip.is_link_local:
            return _real_connect(self, address)
    except ValueError:
        if text == "localhost":
            return _real_connect(self, address)
    raise TripwireError(
        f"outbound connection to {address!r} refused: this tool is local-only"
    )


socket.socket.connect = _guarded_connect

import numpy as np  # noqa: E402

import retrieval_benchmark as rb  # noqa: E402
from lexical_index import LexicalIndex  # noqa: E402
from retrieval import (  # noqa: E402
    MAX_PER_BOOK,
    MAX_PER_CHAPTER,
    fuse_interleave,
    load_genre_blacklist,
    load_safe_pool,
    merge_semantic_lexical,
    parse_canonical_id,
    rotate_safe_pool,
)
from passage_rerank import (  # noqa: E402
    PassageRerankError,
    RERANK_PROMPT_VERSION,
    build_rerank_instruction,
    build_rerank_user_content,
    parse_rerank_response,
)


def _tripwire_stub(*args, **kwargs):
    raise TripwireError(f"external AI call attempted: {args!r:.120}")


def _disarm_gemini_clients() -> list[str]:
    """Replace every imported Gemini call surface with a raising stub."""
    disarmed = []
    import embeddings
    import query_rewrite
    import passage_rerank

    for module, attribute in (
        (embeddings.GeminiEmbeddingClient, "embed_query"),
        (embeddings.GeminiEmbeddingClient, "embed_documents"),
        (query_rewrite.GeminiQueryRewriter, "rewrite"),
        (passage_rerank.GeminiPassageReranker, "choose"),
        (rb, "require_api_key"),
        (rb, "require_rewrite_api_key"),
        (rb, "_key_from_env"),
        (rb, "_query_vector"),
        (rb, "embed_gemini"),
    ):
        if hasattr(module, attribute):
            setattr(module, attribute, _tripwire_stub)
            disarmed.append(f"{getattr(module, '__name__', module)}.{attribute}")
    return disarmed


DISARMED = _disarm_gemini_clients()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LANGUAGE = "ru"
TRANSLATION, ALIAS = rb.LANGUAGE_CORPUS[LANGUAGE]
DOC_TEXT = "description"
TOP_K = 10
TABLE_K = 5
DEFAULT_PORT = int(os.environ.get("LOCAL_PICKER_PORT", "9089"))
DEFAULT_SENSES_FILE = "bench_data/codex_gpt5_senses_syn_v4.jsonl"
# `LOCAL_PICKER_DESCRIPTIONS` was the first name of this setting; both are
# read so a shell that still exports the old one is not silently ignored.
SENSES_FILE = (
    os.environ.get("LOCAL_PICKER_SENSES_FILE", "").strip()
    or os.environ.get("LOCAL_PICKER_DESCRIPTIONS", "").strip()
    or DEFAULT_SENSES_FILE
)
EMBEDDER = os.environ.get("LOCAL_PICKER_EMBEDDER", "bge-m3")
RERANK_TIMEOUT = float(os.environ.get("LOCAL_PICKER_RERANK_TIMEOUT", "180"))
RERANK_MAX_TOKENS = int(os.environ.get("LOCAL_PICKER_RERANK_MAX_TOKENS", "600"))


def load_cautions(path: Path) -> dict[tuple[int, str], tuple[bool, str]]:
    """(translation, canonical_id) -> (caution, caution_note).

    `rb.load_descriptions` intentionally keeps only the senses (they are the
    vectors); the flag is what this tool has to SHOW. Same last-row-wins rule
    as there, so a `--resume` retry is read the same way by both.
    """
    out: dict[tuple[int, str], tuple[bool, str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        if not (row.get("senses") or []):
            continue
        out[(row["translation"], row["canonical_id"])] = (
            bool(row.get("caution")), row.get("caution_note") or "",
        )
    return out


# ---------------------------------------------------------------------------
# The index
# ---------------------------------------------------------------------------

class Picker:
    """Everything loaded once: model, sense matrix, BM25, verses, blacklist."""

    def __init__(self, verbose: bool = True) -> None:
        self.lock = threading.Lock()
        started = time.time()
        say = print if verbose else (lambda *a, **k: None)

        senses_path = Path(SENSES_FILE)
        if not senses_path.is_absolute():
            senses_path = HERE / senses_path
        if not senses_path.exists():
            raise SystemExit(
                f"[picker] sense file not found: {senses_path}\n"
                f"  set LOCAL_PICKER_SENSES_FILE to an existing "
                f"gen_descriptions.py artifact (default: "
                f"{DEFAULT_SENSES_FILE})"
            )
        self.senses_path = senses_path

        say(f"[picker] chunks…")
        metas, texts, title_texts = rb.load_chunks()
        self.metas = metas

        say(f"[picker] senses from {senses_path.name}…")
        descriptions, self.sense_meta, sha = rb.load_descriptions(
            str(senses_path))
        self.cautions = load_cautions(senses_path)

        doc_texts, owners, stats = rb.build_doc_texts(
            DOC_TEXT, metas, texts, title_texts, descriptions, [LANGUAGE])
        self.doc_texts = doc_texts
        self.row_metas = [metas[owner] for owner in owners]
        covered = stats["fragments_by_language"].get(LANGUAGE, 0)
        missing = stats["missing_by_language"].get(LANGUAGE, 0)
        self.coverage = (covered - missing) / covered if covered else 0.0
        say(f"[picker] {len(doc_texts)} sense rows over {covered} fragments, "
            f"coverage {self.coverage:.3f}")

        # The matrix is named after the sha1 of THIS sense file, so a
        # different LOCAL_PICKER_SENSES_FILE names a different matrix and can
        # never be searched against the previous file's vectors.
        variant = rb.doc_matrix_variant(DOC_TEXT, sha, [LANGUAGE])
        matrix_path = rb.emb_path(EMBEDDER, variant)
        self.matrix_name = matrix_path.name
        if matrix_path.exists():
            say(f"[picker] matrix {self.matrix_name} + {EMBEDDER} on CPU "
                f"(first load takes a minute)…")
        else:
            # Building is the benchmark's own code path (`_pipeline_embedder`
            # -> `_local_pipeline_corpus`), which prints its own progress and
            # writes the .sha1 sidecar. It is an HOUR of CPU for bge-m3 over
            # ~8000 sense rows, so say so before the process looks hung.
            say(f"[picker] matrix {self.matrix_name} is MISSING — "
                f"embedding {len(doc_texts)} sense rows with {EMBEDDER} on "
                f"CPU. This takes about an hour for bge-m3; the process is "
                f"not hung. It is written to bench_data/ and reused.")
        # Reuses the benchmark's embedder wiring, so the picker searches the
        # exact matrix the benchmark scored — including its fingerprint check.
        self.corpus, self.query_vector = rb._pipeline_embedder(
            EMBEDDER, self.row_metas, doc_texts, {"query_embeddings": {}},
            variant)

        # Rows of this language, and the rows of each fragment.
        self.row_idx = np.array(
            [r for r, m in enumerate(self.row_metas)
             if m.translation == TRANSLATION], dtype=int)
        self.canon_rows: dict[str, list[int]] = {}
        for r in self.row_idx:
            self.canon_rows.setdefault(
                self.row_metas[r].canonical_id, []).append(int(r))

        say("[picker] BM25 over the Scripture text…")
        self.lexical = LexicalIndex([
            (m.canonical_id, title_texts[i])
            for i, m in enumerate(metas) if m.translation == TRANSLATION
        ])

        self.blacklist = load_genre_blacklist()
        self.safe_pool = load_safe_pool()
        self.meta_by_id = {
            m.canonical_id: m for m in metas if m.translation == TRANSLATION
        }

        say("[picker] verses and book names from cep_public…")
        ru_metas = [m for m in metas if m.translation == TRANSLATION]
        self.chunk_verses = rb._load_chunk_verses(ru_metas)
        self.chunk_title, self.chunk_text = rb.load_chunk_prompt_parts()
        self.book_names = rb._load_book_names()
        self.psalm_maps = rb.load_psalm_maps()
        self.numbered = bool(self.chunk_verses)

        self.pool_cursor = 0
        self.ready_seconds = time.time() - started
        say(f"[picker] ready in {self.ready_seconds:.1f}s; rerank "
            f"{'-> ' + RERANK_MODEL + ' @ ' + _endpoint_host() if self.rerank_configured else 'NOT configured (top-5 only)'}")

    # -- helpers ------------------------------------------------------------

    @property
    def rerank_configured(self) -> bool:
        return bool(RERANK_ENDPOINT and RERANK_MODEL)

    def reference(self, canonical_id: str) -> str:
        _v, book, chapter, start, end = parse_canonical_id(canonical_id)
        name = self.book_names.get(book, {}).get(LANGUAGE) or f"кн.{book}"
        span = f"{start}" if start == end else f"{start}-{end}"
        return f"{name} {chapter}:{span}"

    def passage_text(self, canonical_id: str) -> str:
        return self.chunk_text.get((TRANSLATION, canonical_id), "")

    def first_words(self, canonical_id: str, limit: int = 90) -> str:
        text = " ".join(self.passage_text(canonical_id).split())
        return text[:limit] + ("…" if len(text) > limit else "")

    def caution_of(self, canonical_id: str) -> tuple[bool, str]:
        return self.cautions.get((TRANSLATION, canonical_id), (False, ""))

    # -- retrieval ----------------------------------------------------------

    def _diversity_with_trace(
        self, ranked: list, top_k: int,
    ) -> tuple[list, list[dict]]:
        """Same greedy cap loop as `retrieval.apply_diversity`, but also
        records WHY a hit did not make it in — the one thing the plain
        boolean-returning helper cannot show a human (Stage 6).

        Deliberately not calling `apply_diversity` and diffing the result:
        that would require re-deriving the skip reason from the OUTPUT,
        which cannot distinguish "skipped for a book cap" from "skipped for
        a chapter cap" once both hits are simply absent. This is the exact
        same traversal (same break condition, same fill-back tail), just
        also writing a reason next to a rejected hit.
        """
        selected: list = []
        skipped: list = []
        reasons: dict[str, str] = {}
        per_book: dict[int, int] = {}
        per_chapter: dict[tuple[int, int], int] = {}
        for hit in ranked:
            if len(selected) >= top_k:
                break
            _v, book, chapter, _s, _e = parse_canonical_id(hit.canonical_id)
            chapter_full = per_chapter.get((book, chapter), 0) >= MAX_PER_CHAPTER
            book_full = per_book.get(book, 0) >= MAX_PER_BOOK
            if chapter_full or book_full:
                skipped.append(hit)
                reasons[hit.canonical_id] = (
                    f"в выдаче уже есть {MAX_PER_CHAPTER} отрывок из этой главы"
                    if chapter_full else
                    f"в выдаче уже есть {MAX_PER_BOOK} отрывка из этой книги")
                continue
            per_book[book] = per_book.get(book, 0) + 1
            per_chapter[(book, chapter)] = per_chapter.get((book, chapter), 0) + 1
            selected.append(hit)
        if len(selected) < top_k and skipped:
            fill = skipped[: top_k - len(selected)]
            fill_ids = {h.canonical_id for h in fill}
            selected.extend(fill)
            skipped = [h for h in skipped if h.canonical_id not in fill_ids]
        cut = [
            {"reference": self.reference(h.canonical_id), "reason": reasons[h.canonical_id]}
            for h in skipped
        ]
        return selected, cut

    def search(self, query: str) -> tuple[list[dict], list[dict]]:
        """Top-K after senses + BM25 + blacklist + diversity.

        Each entry carries the sense row that won the fragment, which is the
        one thing a metric cannot show and a human review needs. Returns
        (candidates, stage_trace) — stage_trace holds Stages 2-6 for the
        human-readable page (ClickUp 86cbeeqjp): what each step did and what
        it removed, not just the final list.
        """
        stages: list[dict] = []

        embed_started = time.time()
        qvec = self.query_vector(query)
        embed_ms = int((time.time() - embed_started) * 1000)

        sem_started = time.time()
        sims = self.corpus[self.row_idx] @ qvec
        semantic: list[tuple[str, float]] = []
        sense_of: dict[str, tuple[str, float]] = {}
        seen: set[str] = set()
        for j in np.argsort(-sims):
            row = int(self.row_idx[j])
            canonical_id = self.row_metas[row].canonical_id
            if canonical_id in seen:
                continue
            seen.add(canonical_id)
            semantic.append((canonical_id, float(sims[j])))
            sense_of[canonical_id] = (self.doc_texts[row], float(sims[j]))
            if len(semantic) >= rb.FETCH_K_DEFAULT:
                break
        sem_ms = int((time.time() - sem_started) * 1000)

        stages.append({
            "n": 2, "title": "Поиск по смыслам", "ms": embed_ms + sem_ms,
            "kind": "candidates",
            "what": (
                f"Превращаем текст молитвы в вектор ({EMBEDDER}, считается "
                f"прямо на этой машине, без интернета) и сравниваем его с "
                f"{len(self.doc_texts)} заранее описанными «смыслами» "
                f"библейских отрывков — какой смысл ближе всего по значению."),
            "got": {
                "embed_ms": embed_ms,
                "total": len(semantic),
                "rows": [
                    {"reference": self.reference(cid),
                     "first_words": self.first_words(cid),
                     "sense": sense_of[cid][0], "score": round(score, 4)}
                    for cid, score in semantic[:10]
                ],
            },
        })

        lex_started = time.time()
        lex_hits = self.lexical.search(query, top_k=20)
        lexical = []
        for hit in lex_hits:
            rows = self.canon_rows.get(hit.canonical_id)
            if not rows:
                continue
            best_row = max(rows, key=lambda r: float(self.corpus[r] @ qvec))
            score = float(self.corpus[best_row] @ qvec)
            lexical.append((hit.canonical_id, score))
            sense_of.setdefault(
                hit.canonical_id, (self.doc_texts[best_row], score))
        lex_ms = int((time.time() - lex_started) * 1000)

        stages.append({
            "n": 3, "title": "Поиск по словам (BM25)", "ms": lex_ms,
            "kind": "candidates",
            "what": (
                "Ищем буквальные совпадения слов молитвы с текстом Писания "
                "— без понимания смысла, чисто по словоформам."),
            "got": {
                "total": len(lexical),
                "rows": [
                    {"reference": self.reference(cid),
                     "first_words": self.first_words(cid),
                     "score": round(score, 4)}
                    for cid, score in lexical[:10]
                ],
            },
        })

        merged = merge_semantic_lexical(semantic, lexical)
        lexical_ids = {cid for cid, _s in lexical}
        semantic_ids = {cid for cid, _s in semantic}
        fused = fuse_interleave([merged])

        def source_of(canonical_id: str) -> str:
            parts = []
            if canonical_id in semantic_ids:
                parts.append("смысл")
            if canonical_id in lexical_ids:
                parts.append("BM25")
            return "+".join(parts) or "смысл"

        stages.append({
            "n": 4, "title": "Объединение списков", "ms": None,
            "kind": "merge",
            "what": (
                "Смешиваем список «по смыслам» и список «по словам» по "
                "очереди (сначала лидер каждого способа, потом второе место "
                "и так далее), без повторов."),
            "got": {
                "total": len(fused),
                "rows": [
                    {"reference": self.reference(hit.canonical_id),
                     "source": source_of(hit.canonical_id)}
                    for hit in fused[:15]
                ],
            },
        })

        bl_started = time.time()
        filtered = []
        cut_blacklist = []
        for hit in fused:
            _v, book, chapter, start, end = parse_canonical_id(hit.canonical_id)
            genre = None
            for entry in self.blacklist:
                if entry.blocks(book, chapter, start, end):
                    genre = entry.genre
                    break
            if genre is not None:
                cut_blacklist.append({
                    "reference": self.reference(hit.canonical_id),
                    "reason": f"жанр «{genre}» в чёрном списке",
                })
                continue
            if hit.canonical_id not in self.meta_by_id:
                cut_blacklist.append({
                    "reference": hit.canonical_id,
                    "reason": "текста нет в индексе этого перевода",
                })
                continue
            filtered.append(hit)
        bl_ms = int((time.time() - bl_started) * 1000)

        stages.append({
            "n": 5, "title": "Чёрный список жанров", "ms": bl_ms,
            "kind": "cut",
            "what": (
                "Убираем отрывки из жанров, которые заранее решено не "
                "предлагать в утешении (например, судебные проклятия)."),
            "got": {"cut": cut_blacklist},
        })

        div_started = time.time()
        final, cut_diversity = self._diversity_with_trace(filtered, TOP_K)
        div_ms = int((time.time() - div_started) * 1000)

        stages.append({
            "n": 6, "title": "Разнообразие", "ms": div_ms,
            "kind": "cut",
            "what": (
                f"Не даём одной книге или главе занять всю выдачу (не "
                f"больше {MAX_PER_BOOK} отрывков из одной книги, "
                f"{MAX_PER_CHAPTER} из одной главы), чтобы модель видела "
                f"разные варианты."),
            "got": {"cut": cut_diversity},
        })

        out = []
        for hit in final:
            sense, score = sense_of.get(hit.canonical_id, ("", hit.score))
            caution, note = self.caution_of(hit.canonical_id)
            out.append({
                "canonical_id": hit.canonical_id,
                "reference": self.reference(hit.canonical_id),
                "first_words": self.first_words(hit.canonical_id),
                "sense": sense,
                "score": round(score, 4),
                "caution": caution,
                "caution_note": note,
                "source": source_of(hit.canonical_id),
            })
        return out, stages

    def safe_pool_top(self) -> list[dict]:
        """The empty-query answer: the safe pool, no search, no model."""
        resolved = []
        for ref in self.safe_pool:
            best = None
            for canonical_id, meta in self.meta_by_id.items():
                _v, book, chapter, start, end = parse_canonical_id(canonical_id)
                if (book == ref.book and chapter == ref.chapter
                        and end >= ref.verse_start
                        and start <= ref.verse_end):
                    if best is None or start <= ref.verse_start:
                        best = canonical_id
            resolved.append(best)
        indices = rotate_safe_pool(self.safe_pool, resolved, set(), TOP_K)
        out, seen = [], set()
        for i in indices:
            canonical_id = resolved[i]
            if not canonical_id or canonical_id in seen:
                continue
            seen.add(canonical_id)
            caution, note = self.caution_of(canonical_id)
            out.append({
                "canonical_id": canonical_id,
                "reference": self.reference(canonical_id),
                "first_words": self.first_words(canonical_id),
                "sense": "", "score": 0.0,
                "caution": caution, "caution_note": note,
                "source": "безопасный пул",
            })
        return out

    # -- final choice -------------------------------------------------------

    def candidate_prompt_text(self, canonical_id: str) -> str:
        return rb.candidate_prompt_text(
            (TRANSLATION, canonical_id), self.chunk_verses,
            self.chunk_title, self.chunk_text)

    def choose(self, topic: str, replies: list[str],
               candidates: list[dict]) -> dict:
        """Final choice by the local model, or a documented refusal.

        The prompt is built by the PRODUCTION builders and the answer is
        validated by the PRODUCTION parser, so what is shown here is what the
        service would accept — a local prompt copy would drift.
        """
        if not self.rerank_configured:
            return {"ok": False, "reason": (
                "LOCAL_PICKER_RERANK_ENDPOINT/_MODEL не заданы — "
                "выбор лучшего не делался, ниже просто топ-5 поиска")}
        import httpx

        texts = [self.candidate_prompt_text(c["canonical_id"])
                 for c in candidates]
        instruction = build_rerank_instruction(len(texts), self.numbered)
        user_content = build_rerank_user_content(topic, replies, texts)
        url = RERANK_ENDPOINT.rstrip("/")
        if not url.endswith("/chat/completions"):
            url = f"{url}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if RERANK_API_KEY:
            headers["Authorization"] = f"Bearer {RERANK_API_KEY}"
        payload = {
            "model": RERANK_MODEL,
            "messages": [
                {"role": "system", "content": instruction},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.0,
            "max_tokens": RERANK_MAX_TOKENS,
            "response_format": {"type": "json_object"},
        }
        started = time.time()
        try:
            with httpx.Client(timeout=RERANK_TIMEOUT) as client:
                response = client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
            text = (data["choices"][0]["message"].get("content") or "")
            import re as _re
            text = _re.sub(r"<think>.*?</think>", "", text, flags=_re.DOTALL)
            choice = parse_rerank_response(text, len(texts))
        except TripwireError:
            raise
        except (httpx.HTTPError, ValueError, KeyError, IndexError,
                TypeError, PassageRerankError) as exc:
            # Transport/parse failures are named, never swallowed into
            # "the first candidate wins".
            return {"ok": False, "reason": (
                f"локальная модель не ответила пригодно: "
                f"{type(exc).__name__}"), "ms": int((time.time() - started) * 1000)}
        return {
            "ok": True,
            "index": choice.index,
            "reason": choice.reason,
            "key_verse_start": choice.key_verse_start,
            "key_verse_end": choice.key_verse_end,
            "ms": int((time.time() - started) * 1000),
        }

    def highlight(self, canonical_id: str, start: int | None,
                  end: int | None) -> dict | None:
        meta = self.meta_by_id.get(canonical_id)
        verses = self.chunk_verses.get((TRANSLATION, canonical_id))
        if meta is None or not verses or start is None:
            return None
        return rb._resolve_bench_highlight(
            meta, verses, (start, end), self.psalm_maps)

    # -- stage builders (Stage 1, 7-9 — Stages 2-6 come from `search`) ------

    @staticmethod
    def _stage_raw(query: str) -> dict:
        return {
            "n": 1, "title": "Молитва как есть", "ms": None, "kind": "raw",
            "what": (
                "Берём текст молитвы буквально, без пересказа на язык "
                "Писания — переписывания в библейские фразы нет, ищем по "
                "сырому тексту."),
            "got": {"text": query, "chars": len(query), "language": "ru"},
        }

    def _stage_final_candidates(self, candidates: list[dict], n: int) -> dict:
        return {
            "n": n, "title": "Кандидаты для выбора", "ms": None,
            "kind": "final_candidates",
            "what": (
                f"После чёрного списка и разнообразия остаётся не больше "
                f"{TOP_K} отрывков — их и увидит модель, чтобы выбрать "
                f"лучший."),
            "got": {
                "total": len(candidates),
                "rows": [
                    {"index": i + 1, "reference": c["reference"],
                     "text": self.first_words(c["canonical_id"], limit=150),
                     "caution": c["caution"], "caution_note": c["caution_note"]}
                    for i, c in enumerate(candidates)
                ],
            },
        }

    @staticmethod
    def _stage_choice(choice: dict, candidates_total: int, configured: bool,
                       n: int) -> dict:
        if choice.get("ok"):
            got = {
                "done": True,
                "model": RERANK_MODEL,
                "chosen_index": choice["index"] + 1,
                "candidates_total": candidates_total,
                "key_verse_start": choice.get("key_verse_start"),
                "key_verse_end": choice.get("key_verse_end"),
                "reason": choice.get("reason", ""),
                "reason_note": "диагностика модели, не для человека",
            }
        else:
            got = {"done": False, "configured": configured,
                   "reason": choice.get("reason", "")}
        return {
            "n": n, "title": "Выбор лучшего", "ms": choice.get("ms"),
            "kind": "choice",
            "what": (
                f"Локальная модель ({RERANK_MODEL}) читает кандидатов и "
                f"выбирает один — так же, как это делает боевой сервис."
                if configured else
                "Финальный выбор моделью не настроен на этом стенде."),
            "got": got,
        }

    def _stage_final(self, chosen: dict | None, hl: dict | None,
                      note: str, n: int) -> dict:
        if chosen is None:
            return {
                "n": n, "title": "Итог", "ms": None, "kind": "final",
                "what": note or "Ничего подходящего не нашлось.",
                "got": {"reference": None, "canonical_id": None,
                        "passage": "", "highlight": None,
                        "caution": False, "caution_note": ""},
            }
        return {
            "n": n, "title": "Итог", "ms": None, "kind": "final",
            "what": note or (
                "Показываем выбранный отрывок целиком, с подсветкой "
                "ключевых стихов, если модель их назвала."),
            "got": {
                "reference": chosen["reference"],
                "canonical_id": chosen["canonical_id"],
                "passage": self.passage_text(chosen["canonical_id"]),
                "highlight": hl,
                "caution": chosen["caution"],
                "caution_note": chosen["caution_note"],
            },
        }

    # -- one request --------------------------------------------------------

    def pick(self, topic: str, replies: list[str] | None = None) -> dict:
        replies = [r.strip() for r in (replies or []) if r.strip()]
        topic = topic.strip()
        # One request at a time: the sentence-transformer is not thread-safe
        # and a second bge-m3 does not fit in this machine's memory. The wait
        # for that lock is reported SEPARATELY from the work (`queue_ms`), or
        # a request that merely queued behind a 70-second rerank would be
        # recorded as a 70-second request and the latency numbers of this
        # tool would be fiction.
        queued = time.time()
        with self.lock:
            started = time.time()
            queue_ms = int((started - queued) * 1000)
            query = "\n".join([topic] + replies).strip()
            stage1 = self._stage_raw(query)
            if not query:
                candidates = self.safe_pool_top()
                chosen = candidates[0] if candidates else None
                stage2 = {
                    "n": 2, "title": "Пустой запрос — безопасный список",
                    "ms": None, "kind": "final_candidates",
                    "what": (
                        "Молитва пустая, поэтому поиска и модели не было — "
                        "показываем заранее подобранный безопасный список "
                        "отрывков по кругу."),
                    "got": {
                        "total": len(candidates),
                        "rows": [
                            {"index": i + 1, "reference": c["reference"],
                             "text": c["first_words"], "caution": c["caution"],
                             "caution_note": c["caution_note"]}
                            for i, c in enumerate(candidates)
                        ],
                    },
                }
                stage3 = self._stage_final(
                    chosen, None,
                    "Первый отрывок из безопасного списка (не поиск, не "
                    "модель).", 3)
                return {
                    "query": "", "source": "safe_pool",
                    "candidates": candidates[:TABLE_K],
                    "chosen": chosen,
                    "choice": {"ok": False, "reason":
                               "пустой запрос — отвечает безопасный пул, "
                               "без поиска и без модели"},
                    "highlight": None,
                    "passage": (self.passage_text(chosen["canonical_id"])
                                if chosen else ""),
                    "queue_ms": queue_ms,
                    "ms": int((time.time() - started) * 1000),
                    "stages": [stage1, stage2, stage3],
                }
            search_started = time.time()
            candidates, stages_2_6 = self.search(query)
            search_ms = int((time.time() - search_started) * 1000)
            if not candidates:
                stage7 = self._stage_final_candidates([], 7)
                stage8 = self._stage_final(
                    None, None, "Ничего не нашлось после всех фильтров.", 8)
                return {"query": query, "source": "retrieval",
                        "candidates": [], "chosen": None,
                        "choice": {"ok": False, "reason": "ничего не найдено"},
                        "highlight": None, "passage": "",
                        "queue_ms": queue_ms,
                        "ms": int((time.time() - started) * 1000),
                        "stages": [stage1, *stages_2_6, stage7, stage8]}
            choice = self.choose(topic, replies, candidates)
            index = choice["index"] if choice.get("ok") else 0
            chosen = candidates[index]
            hl = (self.highlight(chosen["canonical_id"],
                                 choice.get("key_verse_start"),
                                 choice.get("key_verse_end"))
                  if choice.get("ok") else None)
            stage7 = self._stage_final_candidates(candidates, 7)
            stage8 = self._stage_choice(
                choice, len(candidates), self.rerank_configured, 8)
            stage9 = self._stage_final(chosen, hl, "", 9)
            return {
                "query": query,
                "source": "retrieval",
                "candidates": candidates[:TABLE_K],
                "candidates_total": len(candidates),
                "chosen": chosen,
                "chosen_rank": index + 1,
                "choice": choice,
                "highlight": hl,
                "passage": self.passage_text(chosen["canonical_id"]),
                "search_ms": search_ms,
                "queue_ms": queue_ms,
                "ms": int((time.time() - started) * 1000),
                "stages": [stage1, *stages_2_6, stage7, stage8, stage9],
            }


def _endpoint_host() -> str:
    parts = urlsplit(RERANK_ENDPOINT)
    return f"{parts.scheme}://{parts.netloc}" if parts.netloc else "?"


# ---------------------------------------------------------------------------
# CLI rendering
# ---------------------------------------------------------------------------

def _stage_header_text(stage: dict) -> str:
    ms = stage.get("ms")
    timing = f" — {ms} мс" if ms is not None else ""
    return f"Этап {stage['n']}. {stage['title']}{timing}"


def _stage_body_text(stage: dict) -> list[str]:
    """"Что получили" for one stage, as plain-text lines (Stage kind decides
    the shape — every stage still gets a "что делаем" / "что получили" pair,
    only the second half's layout differs)."""
    kind = stage["kind"]
    got = stage["got"]
    lines: list[str] = []
    if kind == "raw":
        lines.append(f"  «{got['text']}» — {got['chars']} знаков, "
                     f"язык: {got['language']}")
    elif kind == "candidates":
        if not got["rows"]:
            lines.append("  ничего не нашлось")
        for r in got["rows"]:
            lines.append(f"  - {r['reference']}: {r['first_words']} "
                         f"[score {r['score']}]")
            if "sense" in r:
                lines.append(f"      смысл: {r['sense'] or '—'}")
        if got["total"] > len(got["rows"]):
            lines.append(f"  … всего найдено {got['total']}")
    elif kind == "merge":
        for r in got["rows"]:
            lines.append(f"  - {r['reference']} ({r['source']})")
        if got["total"] > len(got["rows"]):
            lines.append(f"  … всего в списке {got['total']}")
    elif kind == "cut":
        if not got["cut"]:
            lines.append("  ничего не вырезано")
        for c in got["cut"]:
            lines.append(f"  - {c['reference']}: {c['reason']}")
    elif kind == "final_candidates":
        if not got["rows"]:
            lines.append("  кандидатов нет")
        for r in got["rows"]:
            flag = " ⚠ caution" if r["caution"] else ""
            lines.append(f"  [{r['index']}] {r['reference']}{flag}")
            lines.append(f"       {r['text']}")
            if r["caution"] and r["caution_note"]:
                lines.append(f"       caution: {r['caution_note']}")
    elif kind == "choice":
        if got.get("done"):
            lines.append(f"  модель: {got['model']}")
            lines.append(f"  выбран кандидат {got['chosen_index']} из "
                         f"{got['candidates_total']}")
            if got.get("key_verse_start"):
                lines.append(f"  ключевые стихи: {got['key_verse_start']}"
                             f"-{got.get('key_verse_end') or got['key_verse_start']}")
            lines.append(f"  reason ({got['reason_note']}): {got['reason']}")
        else:
            lines.append(f"  выбор не делался: {got.get('reason', '')}")
    elif kind == "final":
        if got["reference"] is None:
            lines.append("  ничего не найдено")
        else:
            mark = "  ⚠ CAUTION" if got["caution"] else ""
            lines.append(f"  {got['reference']}  [{got['canonical_id']}]{mark}")
            if got["caution"] and got["caution_note"]:
                lines.append(f"  caution: {got['caution_note']}")
            lines.append("")
            lines.append(got["passage"])
            hl = got.get("highlight")
            if hl:
                lines.append("")
                lines.append(f"  ключевые стихи {hl['chapter']}:"
                             f"{hl['verse_start']}-{hl['verse_end']}: "
                             f"{hl['text']}")
    return lines


def render_text(result: dict) -> str:
    stages = result.get("stages") or []
    chosen = result.get("chosen")
    lines = []
    if chosen is None:
        lines.append("ИТОГ: ничего не найдено")
    else:
        mark = "  ⚠ CAUTION" if chosen["caution"] else ""
        lines.append(f"ИТОГ: {chosen['reference']}  "
                     f"[{chosen['canonical_id']}]{mark}")
    lines.append("=" * 60)
    for stage in stages:
        lines.append("")
        lines.append(_stage_header_text(stage))
        lines.append(f"Что делаем: {stage['what']}")
        lines.append("Что получили:")
        lines.extend(_stage_body_text(stage))
    lines.append("")
    lines.append(f"поиск {result.get('search_ms', 0)} мс, всего "
                 f"{result['ms']} мс")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Web page (stdlib only)
# ---------------------------------------------------------------------------

PAGE_CSS = """
:root { color-scheme: light dark; }
body { font: 16px/1.55 -apple-system, "Segoe UI", Roboto, sans-serif;
       max-width: 940px; margin: 0 auto; padding: 24px 18px 80px; }
h1 { font-size: 20px; margin: 0 0 4px; }
.sub { color: #777; font-size: 13px; margin: 0 0 20px; }
textarea { width: 100%; box-sizing: border-box; font: inherit; padding: 10px;
           border: 1px solid #bbb; border-radius: 6px; min-height: 90px; }
button { font: inherit; padding: 9px 22px; border-radius: 6px; border: 0;
         background: #2f6f4f; color: #fff; cursor: pointer; margin-top: 10px; }
.chosen { border: 2px solid #2f6f4f; border-radius: 8px; padding: 14px 16px;
          margin: 24px 0 18px; }
.chosen h2 { margin: 0 0 6px; font-size: 18px; }
.passage { white-space: pre-wrap; margin: 10px 0; }
.hl { background: #ffe9a8; color: #222; padding: 1px 2px; }
.caution { background: #ffdede; color: #8a1c1c; border: 1px solid #e0a0a0;
           border-radius: 4px; padding: 2px 7px; font-size: 13px;
           font-weight: 600; }
.note { color: #8a1c1c; font-size: 13px; }
.meta { color: #777; font-size: 13px; }
.sense { font-size: 14px; color: #38553f; }
table { border-collapse: collapse; width: 100%; font-size: 14px;
        margin-top: 8px; }
th, td { border-bottom: 1px solid #ddd; padding: 7px 6px; text-align: left;
         vertical-align: top; }
th { font-size: 12px; text-transform: uppercase; color: #777; }
.warn { background: #fff6d8; border: 1px solid #e6cf7a; color: #6b5200;
        border-radius: 6px; padding: 10px 12px; font-size: 14px;
        margin: 16px 0; }
details { border: 1px solid #ddd; border-radius: 6px; margin: 8px 0;
          padding: 0; }
details > summary { cursor: pointer; padding: 10px 12px; font-weight: 600;
                     font-size: 14px; list-style: revert; }
details[open] > summary { border-bottom: 1px solid #ddd; }
.stage-body { padding: 10px 14px 14px; }
.stage-what { color: #555; font-size: 13px; margin-bottom: 8px; }
.stage-got { font-size: 14px; }
@media (prefers-color-scheme: dark) {
  body { background: #14171a; color: #e6e6e6; }
  textarea { background: #1d2124; color: #e6e6e6; border-color: #444; }
  .chosen { border-color: #4c9a72; }
  .hl { background: #6b5c1f; color: #ffe9a8; }
  .caution { background: #4a1f1f; color: #ffc9c9; border-color: #7a3a3a; }
  .note { color: #ffc9c9; }
  th, td { border-color: #333; }
  .warn { background: #2e2a15; border-color: #6b5c1f; color: #f0dda0; }
  .sense { color: #9fd0b0; }
  details { border-color: #333; }
  details[open] > summary { border-color: #333; }
  .stage-what { color: #aaa; }
}
"""

EXAMPLES = [
    "сервис падает под нагрузкой третью неделю, устала, боюсь не успеть к релизу",
    "благодарю за дочку, она сегодня первый раз улыбнулась мне",
    "брат на войне, я не сплю ночами и боюсь за него",
    "поссорилась с мамой, наговорила лишнего и теперь стыдно",
    "Помоги",
]


def esc(text: str) -> str:
    return html.escape(text or "", quote=True)


def _stage_body_html(stage: dict) -> str:
    """"Что получили" for one stage, as compact HTML (tables/lists) — the
    shape depends on the stage's `kind`, same dispatch as render_text."""
    kind = stage["kind"]
    got = stage["got"]
    if kind == "raw":
        return (f"<p>«{esc(got['text'])}» — {got['chars']} знаков, "
                f"язык: {esc(got['language'])}</p>")
    if kind == "candidates":
        if not got["rows"]:
            return "<p class='meta'>ничего не нашлось</p>"
        has_sense = "sense" in got["rows"][0]
        head = ("<tr><th>ссылка</th><th>первые слова</th>"
                + ("<th>смысл</th>" if has_sense else "")
                + "<th>score</th></tr>")
        rows = "".join(
            f"<tr><td>{esc(r['reference'])}</td>"
            f"<td>{esc(r['first_words'])}</td>"
            + (f"<td class='sense'>{esc(r.get('sense') or '—')}</td>"
               if has_sense else "")
            + f"<td>{r['score']}</td></tr>"
            for r in got["rows"])
        tail = (f"<p class='meta'>… всего найдено {got['total']}</p>"
                if got["total"] > len(got["rows"]) else "")
        return f"<table>{head}{rows}</table>{tail}"
    if kind == "merge":
        if not got["rows"]:
            return "<p class='meta'>пусто</p>"
        rows = "".join(
            f"<tr><td>{esc(r['reference'])}</td><td>{esc(r['source'])}</td>"
            f"</tr>" for r in got["rows"])
        tail = (f"<p class='meta'>… всего в списке {got['total']}</p>"
                if got["total"] > len(got["rows"]) else "")
        return (f"<table><tr><th>ссылка</th><th>откуда</th></tr>{rows}"
                f"</table>{tail}")
    if kind == "cut":
        if not got["cut"]:
            return "<p class='meta'>ничего не вырезано</p>"
        rows = "".join(
            f"<tr><td>{esc(c['reference'])}</td><td>{esc(c['reason'])}</td>"
            f"</tr>" for c in got["cut"])
        return (f"<table><tr><th>ссылка</th><th>почему убрано</th></tr>"
                f"{rows}</table>")
    if kind == "final_candidates":
        if not got["rows"]:
            return "<p class='meta'>кандидатов нет</p>"
        rows = []
        for r in got["rows"]:
            badge = ("<span class='caution'>caution</span>"
                     if r["caution"] else "")
            note = (f"<div class='note'>{esc(r['caution_note'])}</div>"
                    if r["caution"] and r["caution_note"] else "")
            rows.append(
                f"<tr><td>[{r['index']}]</td>"
                f"<td><b>{esc(r['reference'])}</b> {badge}{note}</td>"
                f"<td>{esc(r['text'])}</td></tr>")
        return ("<table><tr><th>№</th><th>ссылка</th><th>текст</th></tr>"
                + "".join(rows) + "</table>")
    if kind == "choice":
        if got.get("done"):
            key = ""
            if got.get("key_verse_start"):
                end = got.get("key_verse_end") or got["key_verse_start"]
                key = (f"<div class='meta'>ключевые стихи: "
                       f"{got['key_verse_start']}-{end}</div>")
            return (
                f"<div class='meta'>модель: {esc(got['model'])}</div>"
                f"<div class='meta'>выбран кандидат № {got['chosen_index']} "
                f"из {got['candidates_total']}</div>{key}"
                f"<div class='note'>{esc(got['reason_note'])}: "
                f"{esc(got['reason'])}</div>")
        return f"<p class='meta'>{esc(got.get('reason', ''))}</p>"
    if kind == "final":
        if got["reference"] is None:
            return "<p>Ничего не найдено.</p>"
        badge = ("<span class='caution'>caution</span>"
                 if got["caution"] else "")
        note = (f"<div class='note'>caution: {esc(got['caution_note'])}</div>"
                if got["caution"] and got["caution_note"] else "")
        hl = got.get("highlight")
        body = esc(got["passage"])
        hl_note = ""
        if hl and hl.get("text"):
            needle = esc(hl["text"])
            if needle in body:
                body = body.replace(
                    needle, f"<span class='hl'>{needle}</span>", 1)
            else:
                hl_note = (f"<div class='meta'>ключевые стихи "
                           f"{hl['chapter']}:{hl['verse_start']}-"
                           f"{hl['verse_end']}</div>")
        return (f"<h4>{esc(got['reference'])} {badge}</h4>"
                f"<div class='meta'>{esc(got['canonical_id'] or '')}</div>"
                f"{note}{hl_note}<div class='passage'>{body}</div>")
    return ""


def _render_stages_html(stages: list[dict]) -> str:
    parts = []
    for stage in stages:
        ms = stage.get("ms")
        timing = f" — {ms} мс" if ms is not None else ""
        opened = " open" if stage["n"] <= 2 else ""
        parts.append(
            f"<details{opened}><summary>Этап {stage['n']}. "
            f"{esc(stage['title'])}{timing}</summary>"
            f"<div class='stage-body'>"
            f"<div class='stage-what'><b>Что делаем:</b> "
            f"{esc(stage['what'])}</div>"
            f"<div class='stage-got'><b>Что получили:</b>"
            f"{_stage_body_html(stage)}</div>"
            f"</div></details>")
    return "".join(parts)


def render_html(picker: "Picker", query: str, result: dict | None) -> str:
    parts = [
        "<!-- local picker, ClickUp 86cbeeqjp -->",
        "<h1>Локальный подбор Писания по смыслам</h1>",
        f"<p class='sub'>{esc(EMBEDDER)} по смыслам из "
        f"<b>{esc(picker.senses_path.name)}</b> ({len(picker.doc_texts)} строк, "
        f"покрытие ru {picker.coverage:.3f}) + BM25 → чёрный список → "
        f"diversity → топ-10 → "
        f"{'выбор ' + esc(RERANK_MODEL) + ' (промпт v' + str(RERANK_PROMPT_VERSION) + ')' if picker.rerank_configured else 'выбор НЕ настроен'}"
        f". Только русский (syn), без наводящих вопросов, "
        f"caution показывается, но не фильтрует.</p>",
        "<form method='post' action='/'>",
        f"<textarea name='q' placeholder='Текст молитвы по-русски…' "
        f"autofocus>{esc(query)}</textarea>",
        "<button type='submit'>Подобрать</button>",
        "</form>",
    ]
    if result is None:
        parts.append("<p class='meta'>Примеры: " + " · ".join(
            f"<a href='/?q={esc(ex)}'>{esc(ex[:40])}…</a>"
            for ex in EXAMPLES) + "</p>")
        return "".join(parts)

    choice = result.get("choice") or {}
    if not choice.get("ok"):
        parts.append(f"<div class='warn'>{esc(choice.get('reason', ''))}"
                     f" — показан порядок поиска как есть.</div>")
    chosen = result.get("chosen")
    if chosen is None:
        parts.append("<p>Ничего не найдено.</p>")
        return "".join(parts)

    badge = ("<span class='caution'>caution</span>" if chosen["caution"] else "")
    parts.append("<div class='chosen'>")
    parts.append(f"<h2>{esc(chosen['reference'])} {badge}</h2>")
    rank_note = ""
    if choice.get("ok"):
        rank = result.get("chosen_rank", 1)
        total = result.get("candidates_total", TOP_K)
        rank_note = f" · кандидат № {rank} из {total} · модель {choice['ms']} мс"
        if rank > TABLE_K:
            rank_note += " · вне таблицы топ-5 ниже"
    parts.append(f"<div class='meta'>{esc(chosen['canonical_id'])}"
                 f"{rank_note}</div>")
    if chosen["caution"] and chosen["caution_note"]:
        parts.append(f"<div class='note'>caution: "
                     f"{esc(chosen['caution_note'])}</div>")
    if chosen["sense"]:
        parts.append(f"<div class='sense'>нашлось по смыслу "
                     f"({chosen['score']}): {esc(chosen['sense'])}</div>")
    hl = result.get("highlight")
    body = esc(result.get("passage", ""))
    if hl and hl.get("text"):
        needle = esc(hl["text"])
        if needle in body:
            body = body.replace(needle, f"<span class='hl'>{needle}</span>", 1)
        else:
            parts.append(f"<div class='meta'>ключевые стихи "
                         f"{hl['chapter']}:{hl['verse_start']}-"
                         f"{hl['verse_end']}</div>")
    parts.append(f"<div class='passage'>{body}</div>")
    if choice.get("ok") and choice.get("reason"):
        parts.append(f"<div class='meta'>reason (диагностика, не для человека): "
                     f"{esc(choice['reason'])}</div>")
    parts.append("</div>")

    stages = result.get("stages") or []
    if stages:
        parts.append("<h3>Как получился этот ответ — по этапам</h3>")
        parts.append(_render_stages_html(stages))
    parts.append(f"<p class='meta'>поиск {result.get('search_ms', 0)} мс, "
                 f"всего {result['ms']} мс</p>")
    return "".join(parts)


def serve(picker: "Picker", port: int) -> None:
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        # The prayer text is NEVER written to the log. A prayer is the most
        # private thing this tool touches, and on a GET it travels in the
        # query string — which is exactly what the default access log line
        # prints. So the path is cut at the '?', and what is recorded about
        # the request itself is its LENGTH and its timings, plus the
        # canonical id that came back (a reference, not the person's words).
        def log_message(self, fmt, *args):  # noqa: A003
            sys.stderr.write(f"{self.command} {self.path.split('?')[0]} "
                             f"{args[1] if len(args) > 1 else ''}\n")

        def log_pick(self, query: str, result: dict | None) -> None:
            if result is None:
                sys.stderr.write(f"[pick] q={len(query)} chars, no search\n")
                return
            chosen = result.get("chosen") or {}
            sys.stderr.write(
                f"[pick] q={len(query)} chars, search "
                f"{result.get('search_ms', 0)} ms, total {result['ms']} ms, "
                f"top1={chosen.get('canonical_id', '-')}\n")
            sys.stderr.flush()

        def _send(self, body: str, status: int = 200) -> None:
            page = (f"<!-- served by local_picker.py -->"
                    f"<meta charset='utf-8'>"
                    f"<meta name='viewport' content='width=device-width,"
                    f"initial-scale=1'>"
                    f"<title>Локальный подбор Писания</title>"
                    f"<style>{PAGE_CSS}</style>{body}")
            raw = page.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _send_json(self, payload: dict) -> None:
            raw = json.dumps(payload, ensure_ascii=False, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):  # noqa: N802
            path = urlsplit(self.path).path
            query = parse_qs(urlsplit(self.path).query).get("q", [""])[0]
            # /json exists for scripted checks (and for this package's own
            # report); the page itself never renders JSON.
            if path == "/json":
                result = picker.pick(query)
                self.log_pick(query, result)
                self._send_json(result)
                return
            if path not in ("/", "/index.html"):
                self._send("<h1>404</h1>", 404)
                return
            result = picker.pick(query) if query.strip() else None
            self.log_pick(query, result)
            self._send(render_html(picker, query, result))

        def do_POST(self):  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length).decode("utf-8", "replace")
            query = parse_qs(raw).get("q", [""])[0]
            result = picker.pick(query) if query.strip() else None
            self.log_pick(query, result)
            self._send(render_html(picker, query, result))

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[picker] http://0.0.0.0:{port}/  (Ctrl-C to stop)", flush=True)
    server.serve_forever()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Local scripture picker over the sense index")
    parser.add_argument("query", nargs="*", help="prayer text (Russian)")
    parser.add_argument("--serve", action="store_true",
                        help=f"run the web page on port {DEFAULT_PORT}")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--json", action="store_true",
                        help="print the raw result as JSON")
    args = parser.parse_args(argv)

    picker = Picker()
    print(f"[picker] tripwire armed: {len(DISARMED)} provider entry points "
          f"stubbed, outbound sockets restricted to local"
          + (f" + {sorted(_ALLOWED_HOSTS)}" if _ALLOWED_HOSTS else ""),
          flush=True)
    if args.serve:
        serve(picker, args.port)
        return 0
    if not args.query:
        parser.error("give a prayer text, or --serve")
    result = picker.pick(" ".join(args.query))
    print(json.dumps(result, ensure_ascii=False, indent=2)
          if args.json else render_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
