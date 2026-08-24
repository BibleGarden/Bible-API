"""
Vector index for scripture-selection RAG retrieval.

Storage decision (architect/adr/0002-embedding-model-and-vector-store.md):
embeddings live in MySQL (`cep_public.chunk_embeddings`, one row per chunk of
`translation_chunks`) and cosine search runs in-process over a numpy matrix.
MySQL stays the single canonical store — existing backups cover the vectors,
no extra service is needed on the small production VPS, and the whole index
can be rebuilt from `translation_chunks` with one CLI command
(`python app/index_cli.py rebuild`).

Versioning: every row carries `embedding_version` =
"c{chunking_version}:{model}@{dims}". A rebuild embeds only chunks that do
not yet have a row with the current version (idempotent, no duplicates —
enforced by a UNIQUE key as well) and deletes rows whose version is stale or
whose chunk no longer exists.

Embedding text: `title + "\n\n" + text` when the chunk has a title (decided
by the retrieval benchmark, see the ADR: titles improved every metric for
every model). `build_embedding_text` is the single place implementing this.

Vectors are float32 unit vectors packed little-endian into a BLOB; cosine
similarity is therefore a dot product.
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass

import numpy as np

from chunking import CHUNKING_VERSION
from config import EMBEDDING_DIMENSIONS, EMBEDDING_MODEL

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS chunk_embeddings (
    code INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    canonical_id VARCHAR(40) NOT NULL,
    translation INT NOT NULL,
    embedding_version VARCHAR(120) NOT NULL,
    dims SMALLINT UNSIGNED NOT NULL,
    vector MEDIUMBLOB NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_embeddings_chunk (translation, canonical_id, embedding_version),
    INDEX idx_embeddings_version (embedding_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


class MissingChunksError(RuntimeError):
    """Rebuild refused: embeddings exist but no chunks of the current
    CHUNKING_VERSION — a plain rebuild would silently delete the index."""


def current_embedding_version(
    model: str = EMBEDDING_MODEL,
    dims: int = EMBEDDING_DIMENSIONS,
    chunking_version: int = CHUNKING_VERSION,
) -> str:
    """Index version = chunking algorithm version + embedding model id + dims."""
    return f"c{chunking_version}:{model}@{dims}"


def build_embedding_text(title: str | None, text: str) -> str:
    """Text fed to the embedding model for one chunk (title included)."""
    title = (title or "").strip()
    return f"{title}\n\n{text}" if title else text


def pack_vector(vector: list[float] | np.ndarray) -> bytes:
    return struct.pack(f"<{len(vector)}f", *vector)


def unpack_vector(blob: bytes) -> np.ndarray:
    return np.frombuffer(blob, dtype="<f4")


def plan_reindex(
    chunk_ids: set[str],
    existing_versions: dict[str, str],
    version: str,
    force: bool = False,
) -> tuple[set[str], set[str]]:
    """Pure reindex plan for one translation.

    chunk_ids           - canonical ids present in translation_chunks now
    existing_versions   - canonical id -> embedding_version currently stored
    returns (to_embed, to_delete) canonical id sets:
      to_embed  - chunks with no row of the current version (or all with force)
      to_delete - rows that are stale: version differs, or chunk disappeared
    """
    up_to_date = {
        cid for cid, ver in existing_versions.items()
        if ver == version and cid in chunk_ids
    }
    to_embed = set(chunk_ids) if force else set(chunk_ids) - up_to_date
    to_delete = {
        cid for cid, ver in existing_versions.items()
        if ver != version or cid not in chunk_ids
    }
    return to_embed, to_delete


@dataclass(frozen=True)
class SearchHit:
    canonical_id: str
    translation: int
    alias: str
    language: str
    book_number: int
    chapter_number: int
    verse_number_start: int
    verse_number_end: int
    title: str | None
    score: float


class InMemoryVectorIndex:
    """Brute-force cosine search over unit vectors with metadata filters.

    Sized for the current corpus (~12k chunks x 768 dims ~ 35 MB): a filtered
    dot product takes single-digit milliseconds, far below the embedding API
    round-trip. Rebuild the instance (reload) after reindexing.
    """

    def __init__(self, vectors: np.ndarray, metas: list[dict]):
        if len(metas) != vectors.shape[0]:
            raise ValueError("vectors and metadata length mismatch")
        self.vectors = vectors.astype(np.float32, copy=False)
        self.metas = metas
        self._translations = np.array(
            [m["translation"] for m in metas], dtype=np.int64
        ) if metas else np.empty(0, dtype=np.int64)
        self._languages = np.array(
            [m["language"] for m in metas]
        ) if metas else np.empty(0, dtype=object)

    def __len__(self) -> int:
        return len(self.metas)

    def search(
        self,
        query_vector: list[float] | np.ndarray,
        top_k: int = 10,
        translation: int | None = None,
        language: str | None = None,
    ) -> list[SearchHit]:
        if not self.metas:
            return []
        mask = np.ones(len(self.metas), dtype=bool)
        if translation is not None:
            mask &= self._translations == translation
        if language is not None:
            mask &= self._languages == language
        candidate_idx = np.nonzero(mask)[0]
        if candidate_idx.size == 0:
            return []
        query = np.asarray(query_vector, dtype=np.float32)
        scores = self.vectors[candidate_idx] @ query
        top = min(top_k, candidate_idx.size)
        order = np.argpartition(-scores, top - 1)[:top]
        order = order[np.argsort(-scores[order])]
        hits = []
        for local_idx in order:
            meta = self.metas[candidate_idx[local_idx]]
            hits.append(SearchHit(score=float(scores[local_idx]), **meta))
        return hits


# ---------------------------------------------------------------------------
# MySQL access (used by the CLI and by index loading)
# ---------------------------------------------------------------------------

def load_index(cursor, version: str | None = None) -> InMemoryVectorIndex:
    """Load every embedding of the given version joined with chunk metadata."""
    version = version or current_embedding_version()
    cursor.execute(
        """
        SELECT e.canonical_id, e.translation, e.vector,
               t.alias, t.language,
               c.book_number, c.chapter_number,
               c.verse_number_start, c.verse_number_end, c.title
        FROM chunk_embeddings e
        JOIN translation_chunks c
          ON c.translation = e.translation AND c.canonical_id = e.canonical_id
        JOIN translations t ON t.code = e.translation
        WHERE e.embedding_version = %s
        ORDER BY e.code
        """,
        (version,),
    )
    vectors, metas = [], []
    for row in cursor.fetchall():
        vectors.append(unpack_vector(row["vector"]))
        metas.append(
            {
                "canonical_id": row["canonical_id"],
                "translation": row["translation"],
                "alias": row["alias"],
                "language": row["language"],
                "book_number": row["book_number"],
                "chapter_number": row["chapter_number"],
                "verse_number_start": row["verse_number_start"],
                "verse_number_end": row["verse_number_end"],
                "title": row["title"],
            }
        )
    matrix = np.vstack(vectors) if vectors else np.empty((0, EMBEDDING_DIMENSIONS), np.float32)
    return InMemoryVectorIndex(matrix, metas)


def reindex_translation(
    connection,
    cursor,
    embed_documents,
    translation_code: int,
    version: str | None = None,
    force: bool = False,
    batch_size: int = 50,
    log=print,
) -> dict:
    """Idempotently (re)index one translation's chunks of CHUNKING_VERSION.

    embed_documents: callable(list[str]) -> list[list[float]] (unit vectors).
    Returns counters {"embedded": n, "kept": n, "deleted": n}.

    Guard: when the translation has stored embeddings but NO chunks of the
    current CHUNKING_VERSION (typical after a version bump before the
    rechunk migration), MissingChunksError is raised instead of wiping the
    index; force=True overrides. Known limitation: on an EMBEDDING_MODEL /
    dimensions change the stale rows are still deleted (and committed)
    before the new ones are embedded — an abort mid-way leaves a partial
    index, which the next rebuild completes (cheap on a billed key).
    """
    version = version or current_embedding_version()
    cursor.execute(CREATE_TABLE_SQL)

    cursor.execute(
        """
        SELECT canonical_id, title, text FROM translation_chunks
        WHERE translation = %s AND chunking_version = %s
        """,
        (translation_code, CHUNKING_VERSION),
    )
    chunks = {row["canonical_id"]: row for row in cursor.fetchall()}

    cursor.execute(
        "SELECT canonical_id, embedding_version FROM chunk_embeddings "
        "WHERE translation = %s",
        (translation_code,),
    )
    existing = {row["canonical_id"]: row["embedding_version"] for row in cursor.fetchall()}

    if not chunks and existing and not force:
        raise MissingChunksError(
            f"translation {translation_code} has {len(existing)} stored "
            f"embeddings but no chunks of CHUNKING_VERSION {CHUNKING_VERSION} "
            f"— a plain rebuild would delete the whole index. Migrate the "
            f"chunks first (python app/versification_cli.py rechunk, then "
            f"rebuild); pass --force only to wipe and re-embed from scratch."
        )

    to_embed, to_delete = plan_reindex(set(chunks), existing, version, force=force)
    # With force, rows of the current version are re-embedded via delete+insert.
    if force:
        to_delete = to_delete | (set(existing) & to_embed)

    if to_delete:
        placeholders = ", ".join(["%s"] * len(to_delete))
        cursor.execute(
            f"DELETE FROM chunk_embeddings WHERE translation = %s "
            f"AND canonical_id IN ({placeholders})",
            (translation_code, *sorted(to_delete)),
        )
        connection.commit()

    ordered = sorted(to_embed)
    embedded = 0
    started = time.time()
    for start in range(0, len(ordered), batch_size):
        batch_ids = ordered[start:start + batch_size]
        texts = [
            build_embedding_text(chunks[cid]["title"], chunks[cid]["text"])
            for cid in batch_ids
        ]
        vectors = embed_documents(texts)
        cursor.executemany(
            """
            INSERT INTO chunk_embeddings
                (canonical_id, translation, embedding_version, dims, vector)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                dims = VALUES(dims), vector = VALUES(vector)
            """,
            [
                (cid, translation_code, version, len(vec), pack_vector(vec))
                for cid, vec in zip(batch_ids, vectors)
            ],
        )
        connection.commit()
        embedded += len(batch_ids)
        if embedded % 500 < batch_size:
            rate = embedded / max(time.time() - started, 1e-9)
            log(f"  {embedded}/{len(ordered)} embedded ({rate:.1f} chunks/s)")

    return {
        "embedded": embedded,
        "kept": len(chunks) - len(to_embed),
        "deleted": len(to_delete),
    }
