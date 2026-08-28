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

select_final adds the grounded rerank stage on top (ClickUp 86cb8vw1h,
architect/adr/0005-grounded-passage-rerank.md): Gemini chooses the best
candidate STRICTLY from the server's list (validated index answer,
app/passage_rerank.py) and marks the 1-3 key verses inside it as a span of
the verse markers the server rendered; on any AI failure the retrieval
top-1 is served, on an invalid span only the highlight is dropped.

Both entry points accept an optional `deadline.Deadline` — the public
endpoint's time budget (ADR 0006). Stages are skipped once it is gone
(safe pool, then retrieval top-1), and it caps every provider HTTP call,
so the whole selection is bounded by the budget instead of by the sum of
the stages' retry ladders.

Privacy: prayer context and rewrite variants are never logged — only
failure categories.
"""

from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from deadline import Deadline
from embeddings import EmbeddingUnavailable
from passage_rerank import MAX_KEY_VERSES, PassageRerankError
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

# Below this many exclusions, an emptied primary ranking ("ranking_empty")
# is logged as a warning rather than info: fetch_k (50) pulls far more
# candidates than a short exclusion list could plausibly remove, so reaching
# zero here looks like a stuck blacklist or a corrupted index, not an
# exhausted repeat history — worth an operator's attention.
RANKING_EMPTY_ANOMALY_THRESHOLD = 10

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


def split_exclusions(
    canonical_ids, chunking_version: int
) -> tuple[frozenset[str], list[str]]:
    """Split already-shown IDs into usable ones and ones of another corpus.

    A canonical ID is only meaningful inside the chunking version that
    produced it (`v3:...`): after a CHUNKING_VERSION bump the boundaries
    move, so an old ID neither matches a current chunk nor describes a
    current window. Such IDs are dropped from the filter (they can no
    longer hide anything) and reported back, so the client can reset its
    "already shown" history instead of silently accumulating dead entries
    (ADR 0006).

    Unparseable IDs are classified as stale too, so a non-HTTP caller
    cannot make this raise; over HTTP they never get here — request
    validation rejects a malformed ID with 422.
    """
    current: set[str] = set()
    stale: list[str] = []
    for canonical_id in canonical_ids:
        try:
            version, *_ = parse_canonical_id(canonical_id)
        except ValueError:
            stale.append(canonical_id)
            continue
        if version == chunking_version:
            current.add(canonical_id)
        else:
            stale.append(canonical_id)
    return frozenset(current), stale


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


@dataclass(frozen=True)
class VerseText:
    """One verse of a chunk, in the translation's own numbering."""
    verse_number: int
    text: str
    start_paragraph: bool = False
    # True when a section title stands before this verse. `chunking.build_text`
    # opens a paragraph there as well, so the rendered text carries a break the
    # verse's own `start_paragraph` does not (278 `ubh` chunks of the current
    # corpus, where the chunk boundaries come from another translation's plan).
    # Display metadata only: the rerank prompt is rendered by `number_verses`,
    # which reads `start_paragraph` alone, so the prompt is unaffected.
    title_break: bool = False


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
    # The same text split back into its verses (empty when the caller did
    # not provide a verse loader). It is what the rerank prompt numbers with
    # [n] markers, and what a returned key-verse span indexes into.
    verses: list[VerseText] = field(default_factory=list)


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
    # None | "empty_topic" | "ai_unavailable" | "deadline" |
    # "coverage_empty" | "ranking_empty"
    fallback_reason: str | None
    query_variants: list[str]    # rewrite variants actually searched (+ raw)
    rewrite_failed: bool


@dataclass
class FinalSelection:
    """One passage chosen from the candidate list (grounded rerank stage).

    The chosen candidate is ALWAYS an element of selection.candidates — the
    reranker only ever returns a validated index into that list, so the
    passage text/coordinates come from the DB, never from the model.
    """

    candidate: Candidate | None  # None only when there are no candidates
    reason: str | None           # model diagnostic (rerank only); NOT for users
    method: str                  # "rerank" | "fallback_top1" | "none"
    # Why the rerank did not decide: None (it did) | "no_reranker" |
    # "safe_pool" | "rerank_failed" | "deadline" | "no_candidates"
    fallback_reason: str | None
    selection: SelectionResult
    # Validated 1-based verse marker span of the key verses inside the
    # chosen candidate's prompt passage, or None (no rerank, no verses, or
    # a span the server refused). Coordinates are resolved one layer up,
    # by app/passage_highlight.py.
    highlight: tuple[int, int] | None = None


class ScriptureRetriever:
    """Wires the pipeline against a vector index, embedder, rewriter and DB.

    index           - vector_index.InMemoryVectorIndex (search())
    embedder        - embeddings.GeminiEmbeddingClient (embed_query())
    rewriter        - query_rewrite.GeminiQueryRewriter (rewrite())
    reranker        - optional passage_rerank.GeminiPassageReranker
                      (choose()); without it select_final() degrades to the
                      retrieval top-1
    load_passages   - callable(translation_code: int, canonical_ids: list[str])
                      -> dict[canonical_id, PassageText-shaped dict]; the
                      production implementation reads translation_chunks.
    load_verses     - optional callable(translation_code: int,
                      list[(canonical_id, book, chapter, first, last)])
                      -> dict[canonical_id, list[VerseText]]; called only for
                      the passages actually shown to the reranker. Without it
                      the rerank prompt carries unnumbered chunk texts, the
                      key-verse contract is left out of the request and no
                      highlight can be produced (make_db_verse_loader is the
                      production one).
    lexical_indexes - optional {language: lexical_index.LexicalIndex} for the
                      hybrid BM25 signal (lexical_index.load_lexical_indexes).
    allowed_canonical_ids - optional set of canonical chunk IDs the selection
                      may return. Used when the passage will be RENDERED in a
                      translation that was never chunked (ADR 0007): only the
                      windows that fully exist in it survive, so the rerank
                      never chooses a passage the server cannot serve. None
                      (the default, and always the case for the primary
                      translation of a language) means no restriction — the
                      filter is then not applied at all. When the filter
                      leaves the ranking empty, the selection degrades to the
                      safe pool (fallback_reason "coverage_empty") rather
                      than to nothing at all; an unrestricted selection whose
                      ranking is emptied by the exclusions and the blacklist
                      alone degrades the same way, under the separate
                      category "ranking_empty".

    include_raw_query=False by default: with working rewrites the raw query
    only wastes interleave slots (benchmark: 0.917 -> 0.958 hit@10 without
    it); it is still searched whenever the rewrite fails.

    embed_workers=1 by default (sequential, with the provider-down
    fail-fast of ADR 0005 m2) — the CLI and the benchmark want a
    deterministic, patient path. The public endpoint passes
    embed_workers=REWRITE_VARIANTS: variant embeddings are independent
    round trips and embedding them concurrently is the single biggest
    serve-time saving (measured 1.6 s -> 0.31 s, ADR 0006). Under
    concurrency the fail-fast heuristic is moot (all calls are already in
    flight); the shared deadline bounds them instead.
    """

    def __init__(
        self,
        index,
        embedder,
        rewriter,
        load_passages,
        reranker=None,
        load_verses=None,
        lexical_indexes: dict | None = None,
        blacklist: list[BlacklistRange] | None = None,
        safe_pool: list[SafePoolRef] | None = None,
        fetch_k: int = FETCH_K,
        lexical_k: int = LEXICAL_K,
        max_per_book: int = MAX_PER_BOOK,
        include_raw_query: bool = False,
        embed_workers: int = 1,
        allowed_canonical_ids: frozenset[str] | set[str] | None = None,
    ):
        self.index = index
        self.embedder = embedder
        self.rewriter = rewriter
        self.reranker = reranker
        self.load_passages = load_passages
        self.load_verses = load_verses
        self.lexical_indexes = lexical_indexes or {}
        self.blacklist = blacklist if blacklist is not None else load_genre_blacklist()
        self.safe_pool = safe_pool if safe_pool is not None else load_safe_pool()
        self.fetch_k = fetch_k
        self.lexical_k = lexical_k
        self.max_per_book = max_per_book
        self.include_raw_query = include_raw_query
        self.embed_workers = max(1, embed_workers)
        self.allowed_canonical_ids = allowed_canonical_ids
        self._vector_rows_cache: dict[str, dict[str, list[int]]] = {}

    # -- public entry point -------------------------------------------------

    def select(
        self, request: SelectionRequest, deadline: Deadline | None = None
    ) -> SelectionResult:
        raw_query = build_search_query(request.topic, list(request.user_replies))
        if not raw_query:
            return self._safe_pool_result(request, "empty_topic")

        queries, rewrite_failed = self._build_queries(
            request.language, request.topic, list(request.user_replies),
            raw_query, deadline,
        )
        variant_hits, searched_queries = self._search_variants(
            request.language, queries, deadline
        )
        if not variant_hits:
            # No query could be embedded: Gemini is down, or the request's
            # time budget ran out. Raw embedding search is impossible
            # without the API embedder, so the safe pool is the
            # deterministic fallback (ADR 0004).
            expired = deadline is not None and deadline.expired()
            result = self._safe_pool_result(
                request, "deadline" if expired else "ai_unavailable"
            )
            result.rewrite_failed = rewrite_failed
            return result

        fused = fuse_interleave(variant_hits)
        filtered = self._filter(fused, request.exclude_canonical_ids)
        final = apply_diversity(filtered, request.top_k, self.max_per_book)
        candidates = self._resolve_candidates(request.language, final)
        if not candidates:
            # ADR 0007 fix F1, extended to the primary path: retrieval ran,
            # but nothing it ranked survived the filters. The safe pool is
            # the same deterministic no-AI answer used when the provider is
            # down, and it is resolved through the very same coverage set,
            # so what it serves is renderable by construction. Answering 503
            # here instead would fail a request the server can satisfy.
            #
            # Two categories, because they mean different things to an
            # operator and to the client: with a coverage set it is an
            # incomplete Bible that narrowed the pool ("coverage_empty"),
            # without one it is the caller's exclusion list (or the genre
            # blacklist) exhausting a fully covered corpus for this topic
            # ("ranking_empty"). Naming the latter "coverage_empty" would
            # blame a filter that was never applied.
            reason = (
                "coverage_empty" if self.allowed_canonical_ids is not None
                else "ranking_empty"
            )
            exclusion_count = len(request.exclude_canonical_ids)
            # A short exclusion list emptying the primary ranking is the
            # anomalous case (see RANKING_EMPTY_ANOMALY_THRESHOLD); a long
            # one is an ordinary exhausted repeat history.
            log = (
                logger.warning
                if reason == "ranking_empty"
                and exclusion_count < RANKING_EMPTY_ANOMALY_THRESHOLD
                else logger.info
            )
            log(
                "retrieval produced no candidate (%s, %d exclusions); "
                "serving the safe pool",
                reason, exclusion_count,
            )
            result = self._safe_pool_result(request, reason)
            result.query_variants = searched_queries
            result.rewrite_failed = rewrite_failed
            return result
        return SelectionResult(
            candidates=candidates,
            source="retrieval",
            fallback_reason=None,
            query_variants=searched_queries,
            rewrite_failed=rewrite_failed,
        )

    def select_final(
        self, request: SelectionRequest, deadline: Deadline | None = None
    ) -> FinalSelection:
        """Select candidates, then let the reranker choose the final one.

        Grounding: the model only ever answers with a validated index into
        the server's candidate list; the returned passage text always comes
        from the DB. On ANY rerank failure (timeout, HTTP error, malformed
        JSON, out-of-range/unknown candidate, empty response) the retrieval
        top-1 is served instead — never an error to the user path. An
        exhausted `deadline` degrades the same way (`fallback_reason`
        "deadline"), without starting the rerank call.

        The same call also returns the key-verse span of the chosen
        passage; an absent or invalid span only drops `highlight`.
        """
        selection = self.select(request, deadline)
        if not selection.candidates:
            return FinalSelection(
                candidate=None, reason=None, method="none",
                fallback_reason="no_candidates", selection=selection,
            )
        top1 = selection.candidates[0]
        if selection.source != "retrieval":
            # Safe pool is already the deterministic no-AI path; there is no
            # prayer context worth reranking on (empty topic) or no AI at all.
            return FinalSelection(
                candidate=top1, reason=None, method="fallback_top1",
                fallback_reason="safe_pool", selection=selection,
            )
        if self.reranker is None:
            return FinalSelection(
                candidate=top1, reason=None, method="fallback_top1",
                fallback_reason="no_reranker", selection=selection,
            )
        if deadline is not None and deadline.expired():
            # Retrieval already produced a verified list; serving its top-1
            # is the documented degraded mode (ADR 0005).
            return FinalSelection(
                candidate=top1, reason=None, method="fallback_top1",
                fallback_reason="deadline", selection=selection,
            )
        texts = [
            _candidate_prompt_text(candidate)
            for candidate in selection.candidates
        ]
        # Nothing was numbered -> no markers in the prompt: ask for the
        # candidate only, never for a key-verse span the model would have to
        # invent (it can drag the choice with it).
        key_verses = any(
            _is_numbered(candidate) for candidate in selection.candidates
        )
        try:
            choice = self.reranker.choose(
                topic=request.topic,
                user_replies=list(request.user_replies),
                candidate_texts=texts,
                deadline=deadline,
                key_verses=key_verses,
            )
        except PassageRerankError as exc:
            # failure category only — never the prayer context or model text
            logger.warning("passage rerank failed: %s", exc)
            return FinalSelection(
                candidate=top1, reason=None, method="fallback_top1",
                fallback_reason="rerank_failed", selection=selection,
            )
        except Exception as exc:  # defensive: any AI failure -> fallback
            logger.warning("passage rerank failed: %s", type(exc).__name__)
            return FinalSelection(
                candidate=top1, reason=None, method="fallback_top1",
                fallback_reason="rerank_failed", selection=selection,
            )
        if not 0 <= choice.index < len(selection.candidates):
            # The reranker validates this already; keep the belt anyway.
            logger.warning("passage rerank returned an out-of-range index")
            return FinalSelection(
                candidate=top1, reason=None, method="fallback_top1",
                fallback_reason="rerank_failed", selection=selection,
            )
        chosen = selection.candidates[choice.index]
        return FinalSelection(
            candidate=chosen,
            reason=choice.reason,
            method="rerank",
            fallback_reason=None,
            selection=selection,
            highlight=_highlight_indices(chosen, choice),
        )

    # -- pipeline stages ----------------------------------------------------

    def _build_queries(
        self,
        language: str,
        topic: str,
        replies: list[str],
        raw_query: str,
        deadline: Deadline | None = None,
    ) -> tuple[list[str], bool]:
        variants: list[str] = []
        rewrite_failed = False
        try:
            variants = self.rewriter.rewrite(
                language, topic, replies, deadline=deadline
            )
        except QueryRewriteError as exc:
            rewrite_failed = True
            logger.warning("query rewrite failed: %s", exc)
        queries = list(variants)
        if self.include_raw_query or not queries:
            queries.append(raw_query)
        return queries, rewrite_failed

    def _embed_queries(
        self, queries: list[str], deadline: Deadline | None
    ) -> list[list[float] | None]:
        """Embed every query variant, keeping their order (None = failed)."""
        if self.embed_workers > 1 and len(queries) > 1:
            with ThreadPoolExecutor(
                max_workers=min(self.embed_workers, len(queries))
            ) as pool:
                futures = [
                    pool.submit(self._embed_one_query, query, deadline)
                    for query in queries
                ]
                return [future.result() for future in futures]

        vectors: list[list[float] | None] = []
        for index, query in enumerate(queries):
            try:
                vectors.append(self.embedder.embed_query(query, deadline=deadline))
            except EmbeddingUnavailable as exc:
                logger.warning("query embedding failed: %s", exc)
                vectors.append(None)
                if getattr(exc, "provider_down", False):
                    # Fail fast: the provider is down for every request
                    # right now — do not burn the full retry budget on each
                    # remaining variant (worst case minutes before fallback).
                    logger.warning(
                        "embedding provider unavailable, "
                        "skipping remaining query variants"
                    )
                    vectors.extend([None] * (len(queries) - index - 1))
                    break
        return vectors

    def _embed_one_query(
        self, query: str, deadline: Deadline | None
    ) -> list[float] | None:
        try:
            return self.embedder.embed_query(query, deadline=deadline)
        except EmbeddingUnavailable as exc:
            logger.warning("query embedding failed: %s", exc)
            return None

    def _search_variants(
        self,
        language: str,
        queries: list[str],
        deadline: Deadline | None = None,
    ) -> tuple[list[list[tuple[str, float]]], list[str]]:
        vectors = self._embed_queries(queries, deadline)
        variant_hits: list[list[tuple[str, float]]] = []
        searched: list[str] = []
        for query, vector in zip(queries, vectors):
            if vector is None:
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

    def _allowed(self, canonical_id: str) -> bool:
        """Can the requested translation render this window? (ADR 0007)"""
        return (
            self.allowed_canonical_ids is None
            or canonical_id in self.allowed_canonical_ids
        )

    def _filter(
        self, fused: list[FusedHit], exclude: frozenset[str] | set[str]
    ) -> list[FusedHit]:
        result = []
        for hit in fused:
            if hit.canonical_id in exclude:
                continue
            if not self._allowed(hit.canonical_id):
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

    def _resolve_verses(
        self, ranges: dict[int, list[tuple]]
    ) -> dict[int, dict[str, list[VerseText]]]:
        """Verses of the chunk ranges asked for, one query per translation.

        Best effort by design: verses only power the key-verse highlight, so
        a failing verse loader degrades to "no highlight" instead of failing
        a selection that has verified passages already — and a translation
        that fails takes only its own verses down, not the ones already
        loaded for another.
        """
        if self.load_verses is None:
            return {}
        resolved: dict[int, dict[str, list[VerseText]]] = {}
        for code, chunk_ranges in ranges.items():
            try:
                resolved[code] = self.load_verses(code, chunk_ranges)
            except Exception as exc:      # category only — never the context
                logger.warning(
                    "candidate verse loading failed: %s", type(exc).__name__
                )
        return resolved

    def _attach_prompt_verses(self, candidates: list[Candidate]) -> None:
        """Number the passages the reranker will actually be shown.

        Only the prompt passage of each candidate carries `[n]` markers and
        only its verses are ever indexed by a key-verse answer, so only they
        are loaded — the other translations of a candidate would be rows
        nobody reads. Every candidate of a language normally shares one
        primary translation, which makes this the single extra
        `translation_verses` query per selection (ADR 0005).
        """
        if self.load_verses is None:
            return
        shown: list[tuple[str, PassageText]] = []
        ranges: dict[int, list[tuple]] = {}
        for candidate in candidates:
            passage = prompt_passage(candidate)
            if passage is None:
                continue
            shown.append((candidate.canonical_id, passage))
            ranges.setdefault(passage.translation, []).append(
                (
                    candidate.canonical_id,
                    passage.book_number,
                    passage.chapter_number,
                    passage.verse_number_start,
                    passage.verse_number_end,
                )
            )
        verses = self._resolve_verses(ranges)
        for canonical_id, passage in shown:
            passage.verses = list(
                verses.get(passage.translation, {}).get(canonical_id, ())
            )

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
        self._attach_prompt_verses(candidates)
        return candidates

    # -- safe pool -----------------------------------------------------------

    def _resolve_pool_ids(self, language: str) -> list[str | None]:
        """Resolve each pool ref to the canonical chunk ID owning it.

        Windows the requested translation cannot render are left out here as
        well: the safe pool is the no-AI path, not an exemption from the
        grounding rule (a pool entry that does not exist in the requested
        translation simply does not resolve for it).
        """
        parsed = []
        for canonical_id in self._language_chunks(language):
            if not self._allowed(canonical_id):
                continue
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


def prompt_passage(candidate: Candidate) -> PassageText | None:
    """The passage the reranker is shown: the language's primary corpus
    translation (the first passage — index insertion order)."""
    return candidate.passages[0] if candidate.passages else None


# A handful of verses carry a literal "[27]"-style textual variant of their
# own (27 verses of `syn`, all in four Genesis chunks; verified over the
# whole corpus, chunk titles are free of brackets entirely). Left as they
# are they would be indistinguishable from the markers this function adds,
# and a low literal number would give the model an in-bounds but wrong
# marker to answer with. They are rewritten to round brackets — PROMPT
# RENDERING ONLY; the passage text served to the client passes nowhere near
# this module.
_LITERAL_MARKER_RE = re.compile(r"\[\s*(\d+)\s*\]")


def _neutralize_literal_markers(text: str) -> str:
    """Round brackets for a "[27]" the scripture text carries of its own."""
    return _LITERAL_MARKER_RE.sub(r"(\1)", text)


def number_verses(verses: list[VerseText]) -> str:
    """Chunk text with a [n] marker before every verse.

    Paragraph structure is rebuilt from the verses' own `start_paragraph`
    flags (a blank line at every flagged verse), which reproduces the
    stored chunk text of 11678 of 11960 chunks byte for byte. 278 (all
    `ubh`) carry one paragraph break less than the stored text — same
    words in the same order, one break short. The remaining 4 (all `syn`)
    differ only because their literal "[n]" sequences are neutralised to
    round brackets, so only the server's own markers look like markers —
    a deliberate deviation from the stored text. So the reranker reads the
    passage it always did, plus the markers its key-verse answer indexes
    into. Empty verses are dropped by the loader, so marker n is always
    the n-th verse of `verses`.
    """
    paragraphs: list[list[str]] = []
    for number, verse in enumerate(verses, start=1):
        piece = f"[{number}] {_neutralize_literal_markers(verse.text.strip())}"
        if not paragraphs or verse.start_paragraph:
            paragraphs.append([piece])
        else:
            paragraphs[-1].append(piece)
    return "\n\n".join(" ".join(pieces) for pieces in paragraphs)


def _is_numbered(candidate: Candidate) -> bool:
    """Was this candidate rendered with [n] verse markers?"""
    passage = prompt_passage(candidate)
    return passage is not None and bool(passage.verses)


def _candidate_prompt_text(candidate: Candidate) -> str:
    """Candidate text as shown to the reranker: primary translation's
    title + verse-numbered text (the plain stored text when the verses of
    the chunk are not available)."""
    passage = prompt_passage(candidate)
    if passage is None:
        return ""
    body = number_verses(passage.verses) if passage.verses else passage.text
    if passage.title:
        return f"{passage.title}\n{body}"
    return body


def _highlight_indices(
    candidate: Candidate, choice
) -> tuple[int, int] | None:
    """Validate a model key-verse span against the candidate it belongs to.

    The span must be a real, in-bounds range of the verses the server put
    into the prompt for THIS candidate and at most MAX_KEY_VERSES long.
    Anything else (missing fields, out of bounds, reversed, too long, no
    verses loaded) returns None — the passage is served without a
    highlight, the choice itself is untouched.
    """
    start = getattr(choice, "key_verse_start", None)
    end = getattr(choice, "key_verse_end", None)
    if start is None or end is None:
        return None
    passage = prompt_passage(candidate)
    if passage is None or not passage.verses:
        return None
    if not 1 <= start <= end <= len(passage.verses):
        return None
    if end - start + 1 > MAX_KEY_VERSES:
        return None
    return (start, end)


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


def _range_conditions(ordered, prefix: str = "") -> tuple[str, list]:
    """SQL disjunction over chunk display ranges + its parameters.

    Never called with an empty list — both callers return early on one — and
    an empty disjunction would silently render as `AND ()`, a syntax error at
    best and a whole-table read at worst, so the invariant is stated here.
    """
    if not ordered:
        raise ValueError("no chunk ranges to build a condition for")
    conditions = " OR ".join(
        [f"({prefix}book_number = %s AND {prefix}chapter_number = %s "
         f"AND {prefix}verse_number BETWEEN %s AND %s)"] * len(ordered)
    )
    params: list = []
    for _cid, book, chapter, first, last in ordered:
        params.extend((book, chapter, first, last))
    return conditions, params


def _load_title_verses(cursor, translation_code: int, ordered) -> set[tuple]:
    """(book, chapter, verse) of the ranges' verses that carry a section title.

    `chunking.build_text` breaks the paragraph before such a verse, so this is
    the second half of the paragraph structure of a stored chunk text — the
    half `translation_verses.start_paragraph` does not carry. Same source and
    filter as `chunk_cli.load_translation_titles` (non-subtitle rows), so the
    breaks are the ones the chunk was actually built with.

    `translation_titles` is indexed by its primary key alone, so a query
    DRIVEN by it can only be a full scan of all ~12.7k rows plus one row read
    in `translation_verses` per title. Driving from the verses instead — the
    ranges a range scan on `idx_translation_verses_trans_book_chapter`, the
    titles one scan materialised into a hash — asks the same question over
    the same rows in a sixth of the time: 14-16 ms -> 2.4 ms on the
    production corpus for the 10 chunks a selection actually loads (the two
    converge around 100 ranges, which only the corpus-wide test reaches). No
    index is added: the table is small and the corpus has no migrations.

    Best effort: it only refines the paragraph flags of the PUBLIC passage,
    while the verses themselves also power the key-verse highlight — a
    failure here must not cost the caller its verses (nor, through them, its
    highlight). That promise includes leaving the shared cursor usable: a
    failure BETWEEN `execute` and the end of `fetchall` would otherwise leave
    an unread result set on it and make the next statement — the verse query
    right below — fail as well, degrading to "no verses" instead of "verses
    without title breaks". Hence the drain in the handler.
    """
    conditions, params = _range_conditions(ordered, prefix="tv.")
    try:
        cursor.execute(
            f"""
            SELECT tv.book_number, tv.chapter_number, tv.verse_number
            FROM translation_verses tv
            WHERE tv.translation = %s AND ({conditions})
              AND tv.code IN (
                  SELECT tt.before_translation_verse
                  FROM translation_titles tt
                  WHERE tt.subtitle = 0
              )
            """,
            [translation_code, *params],
        )
        return {
            (row["book_number"], row["chapter_number"], row["verse_number"])
            for row in cursor.fetchall()
        }
    except Exception as exc:          # category only — never the context
        logger.warning(
            "candidate title loading failed: %s", type(exc).__name__
        )
        _discard_result(cursor)
        return set()


def _discard_result(cursor) -> None:
    """Leave the shared cursor ready for the next statement.

    Called only on the failure path above, where it is unknown whether the
    result set was consumed at all; draining an already-consumed cursor is a
    no-op, and a drain that fails in turn changes nothing that was not
    already broken.
    """
    try:
        cursor.fetchall()
    except Exception:
        pass


def make_db_verse_loader(cursor):
    """load_verses implementation over cep_public.translation_verses.

    Takes the chunks' own display ranges (book, chapter, first/last verse of
    the chunk TEXT, overlap verses included — exactly what
    `translation_chunks` stores) and returns their verses in order. Empty
    verses are dropped, mirroring chunking.build_text, so the n-th verse
    here is the n-th verse of the rendered chunk text.

    A second, guarded query marks the verses a section title stands before
    (`title_break`), which is what makes the verse list reassemble into the
    stored chunk text through `chunking.build_text` — see `VerseText`.
    """

    def load(translation_code: int, chunk_ranges) -> dict[str, list[VerseText]]:
        unique = {
            (canonical_id, book, chapter, first, last)
            for canonical_id, book, chapter, first, last in chunk_ranges
        }
        if not unique:
            return {}
        ordered = sorted(unique)
        # Titles first so the verse query is the last statement on the cursor.
        title_verses = _load_title_verses(cursor, translation_code, ordered)
        conditions, range_params = _range_conditions(ordered)
        params: list = [translation_code, *range_params]
        cursor.execute(
            f"""
            SELECT book_number, chapter_number, verse_number, text,
                   start_paragraph
            FROM translation_verses
            WHERE translation = %s AND ({conditions})
            ORDER BY book_number, chapter_number, verse_number
            """,
            params,
        )
        by_chapter: dict[tuple[int, int], list[dict]] = {}
        for row in cursor.fetchall():
            by_chapter.setdefault(
                (row["book_number"], row["chapter_number"]), []
            ).append(row)
        loaded: dict[str, list[VerseText]] = {}
        for canonical_id, book, chapter, first, last in ordered:
            loaded[canonical_id] = [
                VerseText(
                    verse_number=row["verse_number"],
                    text=row["text"].strip(),
                    start_paragraph=bool(row["start_paragraph"]),
                    title_break=(
                        (book, chapter, row["verse_number"]) in title_verses
                    ),
                )
                for row in by_chapter.get((book, chapter), [])
                if first <= row["verse_number"] <= last and row["text"].strip()
            ]
        return loaded

    return load
