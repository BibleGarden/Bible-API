"""
Lightweight in-process BM25 index over translation chunks — the lexical half
of the hybrid retrieval signal (architect/adr/0004-retrieval-pipeline.md).

Why: the query-rewrite stage produces near-quotes of well-known passages.
Semantic search alone dilutes them — a single sought verse inside a long
aggregate chunk (Proverbs sayings, long psalms) is outranked by chunks that
are thematically close overall. Exact wording is precisely what a lexical
signal ranks first (benchmark: Prov 22:6 semantic rank 26 -> BM25 rank 1 for
the same rewrite variant). Each variant's lexical hits are merged with its
semantic hits in app/retrieval.py.

Implementation: plain BM25 (k1=1.2, b=0.75) over unicode word tokens of
`title + "\n\n" + text` (same document text the embeddings use). ~12k docs
per corpus — pure python is milliseconds per query and needs no new
dependencies or MySQL FULLTEXT indexes. Documents are grouped per language,
mirroring the vector index filters.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)

BM25_K1 = 1.2
BM25_B = 0.75


def tokenize(text: str) -> list[str]:
    return [w.lower() for w in _TOKEN_RE.findall(text)]


@dataclass(frozen=True)
class LexicalHit:
    canonical_id: str
    score: float


class LexicalIndex:
    """BM25 over one document collection (typically one language's chunks).

    Documents sharing a canonical_id (several translations of one language)
    keep separate postings; hits are deduplicated by canonical_id at query
    time keeping the best score.
    """

    def __init__(self, documents: list[tuple[str, str]]):
        """documents: [(canonical_id, text), ...]"""
        self.canonical_ids = [cid for cid, _text in documents]
        self._doc_len: list[int] = []
        self._postings: dict[str, list[tuple[int, int]]] = {}
        for doc_index, (_cid, text) in enumerate(documents):
            words = tokenize(text)
            self._doc_len.append(len(words))
            for word, count in Counter(words).items():
                self._postings.setdefault(word, []).append((doc_index, count))
        total = sum(self._doc_len)
        self._avg_len = total / len(self._doc_len) if self._doc_len else 0.0
        self._n = len(self._doc_len)

    def __len__(self) -> int:
        return self._n

    def search(self, query: str, top_k: int = 20) -> list[LexicalHit]:
        if self._n == 0:
            return []
        scores: dict[int, float] = {}
        for word in set(tokenize(query)):
            postings = self._postings.get(word)
            if not postings:
                continue
            df = len(postings)
            idf = math.log(1.0 + (self._n - df + 0.5) / (df + 0.5))
            for doc_index, freq in postings:
                norm = 1.0 - BM25_B + BM25_B * (
                    self._doc_len[doc_index] / self._avg_len
                )
                scores[doc_index] = scores.get(doc_index, 0.0) + (
                    idf * freq * (BM25_K1 + 1.0) / (freq + BM25_K1 * norm)
                )
        best: dict[str, float] = {}
        for doc_index, score in scores.items():
            cid = self.canonical_ids[doc_index]
            if score > best.get(cid, 0.0):
                best[cid] = score
        ranked = sorted(best.items(), key=lambda item: (-item[1], item[0]))
        return [LexicalHit(cid, score) for cid, score in ranked[:top_k]]


def load_lexical_indexes(cursor, chunking_version: int) -> dict[str, LexicalIndex]:
    """Build one LexicalIndex per language from translation_chunks."""
    cursor.execute(
        """
        SELECT c.canonical_id, c.title, c.text, t.language
        FROM translation_chunks c
        JOIN translations t ON t.code = c.translation
        WHERE c.chunking_version = %s
        ORDER BY c.code
        """,
        (chunking_version,),
    )
    per_language: dict[str, list[tuple[str, str]]] = {}
    for row in cursor.fetchall():
        title = (row["title"] or "").strip()
        text = f"{title}\n\n{row['text']}" if title else row["text"]
        per_language.setdefault(row["language"], []).append(
            (row["canonical_id"], text)
        )
    return {
        language: LexicalIndex(docs) for language, docs in per_language.items()
    }
