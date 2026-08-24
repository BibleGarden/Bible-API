"""
Scripture-selection retrieval pipeline (ClickUp 86cb8vw1g).

Turns a prayer context (topic + allowed user replies) into a ranked top-K of
Bible passage candidates with canonical coordinates, exact texts from the DB
and diagnostic scores. Architecture and measurements:
architect/adr/0004-retrieval-pipeline.md.

Pipeline (ScriptureRetriever.select):

1. Build the raw search query from topic + replies. An empty context goes
   straight to the safe pool (no retrieval, no Gemini calls).
2. Rewrite the context into several scripture-register query variants with
   Gemini (app/query_rewrite.py) — the main quality lever: raw embedding
   search suffers from the register gap between everyday wording and
   biblical language (ADR 0002 probe: rank 356 raw vs rank 4 reformulated).
   On rewrite failure the raw query alone is still searched.
3. Embed every variant (RETRIEVAL_QUERY) and search the vector index per
   language; fuse the per-variant rankings (max cosine per canonical chunk).
   If NO query can be embedded (Gemini down), fall back to the safe pool —
   the deterministic no-AI path (raw embedding search is impossible without
   the API embedder, so the safe pool IS the retrieval fallback).
4. Filter: already-shown canonical IDs (repeat exclusion) and the global
   genre blacklist (app/data/genre_blacklist.json) by canonical-range
   intersection.
5. Diversity: greedy selection in fused-score order with a per-book cap, so
   one book cannot flood the candidate list.
6. Resolve texts: candidates are grouped by canonical chunk ID (shared
   between translations), and every translation of the language contributes
   its own exact verse range and text from `translation_chunks`. Chunk
   boundaries ARE natural passage boundaries by construction (structural
   chunking, ADR 0001: section/paragraph aligned, never crossing chapters or
   titles), so the returned window needs no further expansion.

Privacy: prayer context and rewrite variants are never logged — only
failure categories.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

from embeddings import EmbeddingUnavailable
from query_rewrite import QueryRewriteError, build_search_query

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent / "data"
GENRE_BLACKLIST_FILE = DATA_DIR / "genre_blacklist.json"
SAFE_POOL_FILE = DATA_DIR / "safe_pool.json"

# How many hits to pull from the index per query variant before fusion.
FETCH_K = 50
# How many lexical (BM25) hits to merge into each variant's ranking.
LEXICAL_K = 20
# Diversity: at most this many candidates of one book in the final top-K
# (thresholds.json diversity: max_share_single_book_in_window = 0.4) and at
# most one candidate per chapter (near-duplicate windows are redundant).
MAX_PER_BOOK = 4
MAX_PER_CHAPTER = 1

_CANONICAL_ID_RE = re.compile(
    r"^v(?P<version>\d+):(?P<book>\d{2})\.(?P<chapter>\d{3})"
    r"\.(?P<start>\d{3})-(?P<end>\d{3})$"
)


def parse_canonical_id(canonical_id: str) -> tuple[int, int, int, int, int]:
    """Split 'v3:19.023.001-003' into (version, book, chapter, start, end).

    Coordinates are canonical (english-masoretic Psalm space, ADR 0003).
    """
    match = _CANONICAL_ID_RE.match(canonical_id)
    if not match:
        raise ValueError(f"malformed canonical chunk id: {canonical_id!r}")
    return (
        int(match.group("version")),
        int(match.group("book")),
        int(match.group("chapter")),
        int(match.group("start")),
        int(match.group("end")),
    )


# ---------------------------------------------------------------------------
# Genre blacklist (global, canonical coordinates)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BlacklistRange:
    book: int
    chapter_from: int
    chapter_to: int
    verse_from: int | None  # verse bounds apply only to single-chapter ranges
    verse_to: int | None
    genre: str

    def blocks(self, book: int, chapter: int, verse_start: int, verse_end: int) -> bool:
        """Does the canonical range (book, chapter, verses) intersect this entry?"""
        if book != self.book or not (self.chapter_from <= chapter <= self.chapter_to):
            return False
        if self.verse_from is None or self.verse_to is None:
            return True
        return verse_end >= self.verse_from and verse_start <= self.verse_to


def load_genre_blacklist(path: Path = GENRE_BLACKLIST_FILE) -> list[BlacklistRange]:
    payload = json.loads(Path(path).read_text())
    entries = []
    for row in payload["entries"]:
        chapter_from, chapter_to = row["chapters"]
        verses = row.get("verses")
        if verses is not None and chapter_from != chapter_to:
            raise ValueError(
                f"blacklist entry {row}: verse bounds require a single chapter"
            )
        entries.append(
            BlacklistRange(
                book=row["book"],
                chapter_from=chapter_from,
                chapter_to=chapter_to,
                verse_from=None if verses is None else verses[0],
                verse_to=None if verses is None else verses[1],
                genre=row["genre"],
            )
        )
    return entries


def is_blacklisted(
    blacklist: list[BlacklistRange],
    book: int,
    chapter: int,
    verse_start: int,
    verse_end: int,
) -> bool:
    return any(e.blocks(book, chapter, verse_start, verse_end) for e in blacklist)


# ---------------------------------------------------------------------------
# Safe pool (empty topic / AI-unavailable fallback)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SafePoolRef:
    book: int
    chapter: int
    verse_start: int
    verse_end: int
    note: str = ""


def load_safe_pool(path: Path = SAFE_POOL_FILE) -> list[SafePoolRef]:
    payload = json.loads(Path(path).read_text())
    return [
        SafePoolRef(
            book=row["book"],
            chapter=row["chapter"],
            verse_start=row["verse_start"],
            verse_end=row["verse_end"],
            note=row.get("note", ""),
        )
        for row in payload["entries"]
    ]


def rotate_safe_pool(
    pool: list[SafePoolRef],
    resolved_ids: list[str | None],
    exclude_canonical_ids: set[str],
    limit: int,
) -> list[int]:
    """Deterministic safe-pool rotation with repeat exclusion.

    resolved_ids[i] is the canonical chunk ID pool entry i resolves to in the
    current index (None when unresolved). Entries whose chunk was already
    shown are skipped; when every resolvable entry is excluded the rotation
    resets and ignores exclusions. Returns pool indices in file order.
    """
    resolvable = [i for i, cid in enumerate(resolved_ids) if cid is not None]
    fresh = [
        i for i in resolvable if resolved_ids[i] not in exclude_canonical_ids
    ]
    chosen = fresh if fresh else resolvable
    return chosen[:limit]


# ---------------------------------------------------------------------------
# Fusion / diversity (pure ranking helpers)
# ---------------------------------------------------------------------------

@dataclass
class FusedHit:
    canonical_id: str
    score: float                    # best cosine over variants
    best_variant: int               # variant index that produced the best score
    variant_scores: dict[int, float] = field(default_factory=dict)


def fuse_variant_hits(
    variant_hits: list[list[tuple[str, float]]],
) -> list[FusedHit]:
    """Max-cosine fusion baseline (kept for benchmark ablations).

    variant_hits[v] is [(canonical_id, cosine score), ...] for query variant
    v. Hits of the same canonical chunk from different translations collapse
    into one entry (max score wins). The production pipeline uses
    fuse_interleave instead — measured better on every metric (ADR 0004).
    """
    fused: dict[str, FusedHit] = {}
    for variant_index, hits in enumerate(variant_hits):
        for canonical_id, score in hits:
            entry = fused.get(canonical_id)
            if entry is None:
                entry = FusedHit(
                    canonical_id=canonical_id, score=score, best_variant=variant_index
                )
                fused[canonical_id] = entry
            existing = entry.variant_scores.get(variant_index)
            if existing is None or score > existing:
                entry.variant_scores[variant_index] = score
            if score > entry.score:
                entry.score = score
                entry.best_variant = variant_index
    return sorted(fused.values(), key=lambda h: -h.score)


def fuse_interleave(
    variant_hits: list[list[tuple[str, float]]],
) -> list[FusedHit]:
    """Round-robin fusion: rank-1 of every variant, then rank-2, etc.

    The rewrite variants are diverse spiritual angles by design, so a
    passage found by ONE variant must not be drowned by variants with
    generically higher cosines (max fusion) or by consensus mediocrity
    (RRF) — both measured worse on the benchmark (ADR 0004). Within one
    round, variants are taken in their original order: the rewrite prompt
    orders variants most-central-first, and honouring that order measurably
    beats cosine ordering on first-relevant rank (MRR 0.577 -> 0.650).
    Duplicate canonical IDs keep their first (best) position.
    """
    fused: dict[str, FusedHit] = {}
    order: list[str] = []
    max_len = max((len(hits) for hits in variant_hits), default=0)
    for rank in range(max_len):
        for variant_index, hits in enumerate(variant_hits):
            if rank >= len(hits):
                continue
            canonical_id, score = hits[rank]
            entry = fused.get(canonical_id)
            if entry is None:
                fused[canonical_id] = FusedHit(
                    canonical_id=canonical_id, score=score,
                    best_variant=variant_index,
                    variant_scores={variant_index: score},
                )
                order.append(canonical_id)
            else:
                existing = entry.variant_scores.get(variant_index)
                if existing is None or score > existing:
                    entry.variant_scores[variant_index] = score
    return [fused[canonical_id] for canonical_id in order]


def merge_semantic_lexical(
    semantic: list[tuple[str, float]],
    lexical: list[tuple[str, float]],
) -> list[tuple[str, float]]:
    """Merge one variant's semantic and lexical rankings (round-robin).

    Both lists are [(canonical_id, cosine score), ...] — the caller assigns
    a cosine score to every lexical hit so downstream fusion stays in one
    score space. Order: sem[0], lex[0], sem[1], lex[1], ... deduplicated
    keeping the first (best) position.
    """
    merged: list[tuple[str, float]] = []
    seen: set[str] = set()
    for rank in range(max(len(semantic), len(lexical))):
        for source in (semantic, lexical):
            if rank >= len(source):
                continue
            canonical_id, score = source[rank]
            if canonical_id not in seen:
                seen.add(canonical_id)
                merged.append((canonical_id, score))
    return merged


def apply_diversity(
    ranked: list[FusedHit],
    top_k: int,
    max_per_book: int = MAX_PER_BOOK,
    max_per_chapter: int = MAX_PER_CHAPTER,
) -> list[FusedHit]:
    """Greedy top-K selection with per-chapter and per-book caps.

    Keeps fusion order. The chapter cap (default 1) drops near-duplicate
    windows of one chapter — adjacent chunks of the same passage are
    redundant for the downstream reranker and starve the book quota
    (benchmark: the cap freed slots that recovered missed references). The
    book cap keeps one book from flooding the list. When the caps leave the
    list short of top_k, the best skipped candidates fill the remainder.
    """
    selected: list[FusedHit] = []
    skipped: list[FusedHit] = []
    per_book: dict[int, int] = {}
    per_chapter: dict[tuple[int, int], int] = {}
    for hit in ranked:
        if len(selected) >= top_k:
            break
        _v, book, chapter, _s, _e = parse_canonical_id(hit.canonical_id)
        if (
            per_book.get(book, 0) >= max_per_book
            or per_chapter.get((book, chapter), 0) >= max_per_chapter
        ):
            skipped.append(hit)
            continue
        per_book[book] = per_book.get(book, 0) + 1
        per_chapter[(book, chapter)] = per_chapter.get((book, chapter), 0) + 1
        selected.append(hit)
    if len(selected) < top_k and skipped:
        selected.extend(skipped[: top_k - len(selected)])
    return selected


# ---------------------------------------------------------------------------
# Production service
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SelectionRequest:
    language: str
    topic: str = ""
    user_replies: tuple[str, ...] = ()
    exclude_canonical_ids: frozenset[str] = frozenset()
    top_k: int = 10


@dataclass
class PassageText:
    """One translation's rendering of a candidate chunk."""
    translation: int
    alias: str
    book_number: int
    chapter_number: int          # translation's own numbering
    verse_number_start: int
    verse_number_end: int
    title: str | None
    text: str


@dataclass
class Candidate:
    canonical_id: str
    book_number: int             # canonical coordinates (english-masoretic)
    chapter_number: int
    verse_start: int
    verse_end: int
    score: float | None          # fused cosine; None for safe-pool candidates
    best_variant: int | None
    variant_scores: dict[int, float]
    passages: list[PassageText]


@dataclass
class SelectionResult:
    candidates: list[Candidate]
    source: str                  # "retrieval" | "safe_pool"
    fallback_reason: str | None  # None | "empty_topic" | "ai_unavailable"
    query_variants: list[str]    # rewrite variants actually searched (+ raw)
    rewrite_failed: bool


class ScriptureRetriever:
    """Wires the pipeline against a vector index, embedder, rewriter and DB.

    index           - vector_index.InMemoryVectorIndex (search())
    embedder        - embeddings.GeminiEmbeddingClient (embed_query())
    rewriter        - query_rewrite.GeminiQueryRewriter (rewrite())
    load_passages   - callable(translation_code: int, canonical_ids: list[str])
                      -> dict[canonical_id, PassageText-shaped dict]; the
                      production implementation reads translation_chunks.
    lexical_indexes - optional {language: lexical_index.LexicalIndex} for the
                      hybrid BM25 signal (lexical_index.load_lexical_indexes).

    include_raw_query=False by default: with working rewrites the raw query
    only wastes interleave slots (benchmark: 0.917 -> 0.958 hit@10 without
    it); it is still searched whenever the rewrite fails.
    """

    def __init__(
        self,
        index,
        embedder,
        rewriter,
        load_passages,
        lexical_indexes: dict | None = None,
        blacklist: list[BlacklistRange] | None = None,
        safe_pool: list[SafePoolRef] | None = None,
        fetch_k: int = FETCH_K,
        lexical_k: int = LEXICAL_K,
        max_per_book: int = MAX_PER_BOOK,
        include_raw_query: bool = False,
    ):
        self.index = index
        self.embedder = embedder
        self.rewriter = rewriter
        self.load_passages = load_passages
        self.lexical_indexes = lexical_indexes or {}
        self.blacklist = blacklist if blacklist is not None else load_genre_blacklist()
        self.safe_pool = safe_pool if safe_pool is not None else load_safe_pool()
        self.fetch_k = fetch_k
        self.lexical_k = lexical_k
        self.max_per_book = max_per_book
        self.include_raw_query = include_raw_query
        self._vector_rows_cache: dict[str, dict[str, list[int]]] = {}

    # -- public entry point -------------------------------------------------

    def select(self, request: SelectionRequest) -> SelectionResult:
        raw_query = build_search_query(request.topic, list(request.user_replies))
        if not raw_query:
            return self._safe_pool_result(request, "empty_topic")

        queries, rewrite_failed = self._build_queries(
            request.language, request.topic, list(request.user_replies), raw_query
        )
        variant_hits, searched_queries = self._search_variants(
            request.language, queries
        )
        if not variant_hits:
            # Gemini is down: no query could be embedded. Raw embedding
            # search is impossible without the API embedder, so the safe
            # pool is the deterministic fallback (ADR 0004).
            result = self._safe_pool_result(request, "ai_unavailable")
            result.rewrite_failed = rewrite_failed
            return result

        fused = fuse_interleave(variant_hits)
        filtered = self._filter(fused, request.exclude_canonical_ids)
        final = apply_diversity(filtered, request.top_k, self.max_per_book)
        candidates = self._resolve_candidates(request.language, final)
        return SelectionResult(
            candidates=candidates,
            source="retrieval",
            fallback_reason=None,
            query_variants=searched_queries,
            rewrite_failed=rewrite_failed,
        )

    # -- pipeline stages ----------------------------------------------------

    def _build_queries(
        self, language: str, topic: str, replies: list[str], raw_query: str
    ) -> tuple[list[str], bool]:
        variants: list[str] = []
        rewrite_failed = False
        try:
            variants = self.rewriter.rewrite(language, topic, replies)
        except QueryRewriteError as exc:
            rewrite_failed = True
            logger.warning("query rewrite failed: %s", exc)
        queries = list(variants)
        if self.include_raw_query or not queries:
            queries.append(raw_query)
        return queries, rewrite_failed

    def _search_variants(
        self, language: str, queries: list[str]
    ) -> tuple[list[list[tuple[str, float]]], list[str]]:
        variant_hits: list[list[tuple[str, float]]] = []
        searched: list[str] = []
        for query in queries:
            try:
                vector = self.embedder.embed_query(query)
            except EmbeddingUnavailable as exc:
                logger.warning("query embedding failed: %s", exc)
                continue
            hits = self.index.search(
                vector, top_k=self.fetch_k, language=language
            )
            semantic = [(h.canonical_id, h.score) for h in hits]
            variant_hits.append(
                self._merge_lexical(language, query, vector, semantic)
            )
            searched.append(query)
        return variant_hits, searched

    def _merge_lexical(
        self,
        language: str,
        query: str,
        query_vector,
        semantic: list[tuple[str, float]],
    ) -> list[tuple[str, float]]:
        lexical_index = self.lexical_indexes.get(language)
        if lexical_index is None:
            return semantic
        lexical = [
            (hit.canonical_id, self._cosine_for(language, hit.canonical_id,
                                                query_vector))
            for hit in lexical_index.search(query, top_k=self.lexical_k)
        ]
        return merge_semantic_lexical(semantic, lexical)

    def _cosine_for(self, language: str, canonical_id: str, query_vector) -> float:
        """Cosine of one chunk (best translation) against a query vector —
        keeps lexical hits in the same score space as semantic ones."""
        rows = self._vector_rows(language).get(canonical_id)
        if not rows:
            return 0.0
        return max(
            float(self.index.vectors[row] @ query_vector) for row in rows
        )

    def _vector_rows(self, language: str) -> dict[str, list[int]]:
        cached = self._vector_rows_cache.get(language)
        if cached is None:
            cached = {}
            for row, meta in enumerate(self.index.metas):
                if meta["language"] == language:
                    cached.setdefault(meta["canonical_id"], []).append(row)
            self._vector_rows_cache[language] = cached
        return cached

    def _filter(
        self, fused: list[FusedHit], exclude: frozenset[str] | set[str]
    ) -> list[FusedHit]:
        result = []
        for hit in fused:
            if hit.canonical_id in exclude:
                continue
            _v, book, chapter, start, end = parse_canonical_id(hit.canonical_id)
            if is_blacklisted(self.blacklist, book, chapter, start, end):
                continue
            result.append(hit)
        return result

    # -- candidate resolution ------------------------------------------------

    def _language_chunks(self, language: str) -> dict[str, list[dict]]:
        """canonical_id -> metas of every translation of the language."""
        chunks: dict[str, list[dict]] = {}
        for meta in self.index.metas:
            if meta["language"] == language:
                chunks.setdefault(meta["canonical_id"], []).append(meta)
        return chunks

    def _resolve_candidates(
        self, language: str, hits: list[FusedHit]
    ) -> list[Candidate]:
        by_canonical = self._language_chunks(language)
        # texts per translation in one query each
        wanted: dict[int, list[str]] = {}
        for hit in hits:
            for meta in by_canonical.get(hit.canonical_id, []):
                wanted.setdefault(meta["translation"], []).append(hit.canonical_id)
        texts: dict[int, dict[str, dict]] = {
            code: self.load_passages(code, ids) for code, ids in wanted.items()
        }

        candidates = []
        for hit in hits:
            _v, book, chapter, start, end = parse_canonical_id(hit.canonical_id)
            passages = []
            for meta in by_canonical.get(hit.canonical_id, []):
                row = texts.get(meta["translation"], {}).get(hit.canonical_id)
                if row is None:
                    continue
                passages.append(
                    PassageText(
                        translation=meta["translation"],
                        alias=meta["alias"],
                        book_number=meta["book_number"],
                        chapter_number=meta["chapter_number"],
                        verse_number_start=meta["verse_number_start"],
                        verse_number_end=meta["verse_number_end"],
                        title=row.get("title"),
                        text=row["text"],
                    )
                )
            candidates.append(
                Candidate(
                    canonical_id=hit.canonical_id,
                    book_number=book,
                    chapter_number=chapter,
                    verse_start=start,
                    verse_end=end,
                    score=hit.score,
                    best_variant=hit.best_variant,
                    variant_scores=dict(hit.variant_scores),
                    passages=passages,
                )
            )
        return candidates

    # -- safe pool -----------------------------------------------------------

    def _resolve_pool_ids(self, language: str) -> list[str | None]:
        """Resolve each pool ref to the canonical chunk ID owning it."""
        parsed = []
        for canonical_id in self._language_chunks(language):
            _v, book, chapter, start, end = parse_canonical_id(canonical_id)
            parsed.append((canonical_id, book, chapter, start, end))
        resolved: list[str | None] = []
        for ref in self.safe_pool:
            best: str | None = None
            for canonical_id, book, chapter, start, end in parsed:
                if (
                    book == ref.book and chapter == ref.chapter
                    and end >= ref.verse_start and start <= ref.verse_end
                ):
                    # prefer the chunk whose own range starts at the ref
                    if best is None or start <= ref.verse_start:
                        best = canonical_id
            resolved.append(best)
        return resolved

    def _safe_pool_result(
        self, request: SelectionRequest, reason: str
    ) -> SelectionResult:
        resolved = self._resolve_pool_ids(request.language)
        indices = rotate_safe_pool(
            self.safe_pool, resolved, set(request.exclude_canonical_ids),
            request.top_k,
        )
        hits = [
            FusedHit(canonical_id=resolved[i], score=0.0, best_variant=0)
            for i in indices
            if resolved[i] is not None
        ]
        # deduplicate chunk ids (two pool refs may share one chunk)
        seen: set[str] = set()
        unique = []
        for hit in hits:
            if hit.canonical_id not in seen:
                seen.add(hit.canonical_id)
                unique.append(hit)
        candidates = self._resolve_candidates(request.language, unique)
        for candidate in candidates:
            candidate.score = None
            candidate.best_variant = None
            candidate.variant_scores = {}
        return SelectionResult(
            candidates=candidates,
            source="safe_pool",
            fallback_reason=reason,
            query_variants=[],
            rewrite_failed=False,
        )


# ---------------------------------------------------------------------------
# Production passage loader
# ---------------------------------------------------------------------------

def make_db_passage_loader(cursor):
    """load_passages implementation over cep_public.translation_chunks."""
    from chunking import CHUNKING_VERSION

    def load(translation_code: int, canonical_ids: list[str]) -> dict[str, dict]:
        if not canonical_ids:
            return {}
        unique_ids = sorted(set(canonical_ids))
        placeholders = ", ".join(["%s"] * len(unique_ids))
        cursor.execute(
            f"""
            SELECT canonical_id, title, text
            FROM translation_chunks
            WHERE translation = %s AND chunking_version = %s
              AND canonical_id IN ({placeholders})
            """,
            (translation_code, CHUNKING_VERSION, *unique_ids),
        )
        return {row["canonical_id"]: row for row in cursor.fetchall()}

    return load
