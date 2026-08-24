"""
CLI for the scripture-selection vector index (see
architect/adr/0002-embedding-model-and-vector-store.md).

Embeds `translation_chunks` rows with the configured embedding model
(EMBEDDING_MODEL / EMBEDDING_DIMENSIONS, Gemini API) and stores the vectors
in `cep_public.chunk_embeddings`. MySQL remains the canonical store; the
whole index is rebuilt by one command and the run is idempotent — re-running
embeds nothing new and never creates duplicates.

Usage (inside the bible-api container):

    # full rebuild / catch-up for all chunked translations
    python app/index_cli.py rebuild

    # specific translations, forced re-embedding
    python app/index_cli.py rebuild --translations syn,bsb --force

    # index state per translation/version
    python app/index_cli.py status

    # smoke-test search (embeds the query via the API)
    python app/index_cli.py search --query "благодарность за ребёнка" \
        --translation syn --top-k 5
"""

from __future__ import annotations

import argparse
import sys
import time

from database import create_connection
from embeddings import EmbeddingUnavailable, GeminiEmbeddingClient
from vector_index import (
    MissingChunksError,
    current_embedding_version,
    load_index,
    reindex_translation,
)


def resolve_translations(cursor, spec: str | None) -> list[dict]:
    """Resolve --translations (aliases/codes) or default to chunked ones."""
    if spec:
        result = []
        for raw in spec.split(","):
            raw = raw.strip()
            if not raw:
                continue
            query = "SELECT code, alias FROM translations WHERE alias = %s"
            params: tuple = (raw,)
            if raw.isdigit():
                query = "SELECT code, alias FROM translations WHERE code = %s"
                params = (int(raw),)
            cursor.execute(query, params)
            row = cursor.fetchone()
            if not row:
                raise SystemExit(f"Translation '{raw}' not found")
            result.append(row)
        return result
    cursor.execute(
        """
        SELECT DISTINCT t.code, t.alias
        FROM translation_chunks c JOIN translations t ON t.code = c.translation
        ORDER BY t.code
        """
    )
    return cursor.fetchall()


def cmd_rebuild(connection, cursor, args) -> int:
    version = current_embedding_version()
    translations = resolve_translations(cursor, args.translations)
    if not translations:
        print("No chunked translations found — run app/chunk_cli.py first")
        return 1
    print(f"Index version: {version}")
    totals = {"embedded": 0, "kept": 0, "deleted": 0}
    with GeminiEmbeddingClient() as client:
        for translation in translations:
            print(f"{translation['alias']} (code {translation['code']}):")
            started = time.time()
            try:
                stats = reindex_translation(
                    connection,
                    cursor,
                    client.embed_documents,
                    translation["code"],
                    version=version,
                    force=args.force,
                    batch_size=args.batch_size,
                )
            except MissingChunksError as exc:
                print(f"  REFUSED: {exc}", file=sys.stderr)
                return 1
            except EmbeddingUnavailable as exc:
                print(f"  ABORTED: {exc}", file=sys.stderr)
                print(
                    "  Already-stored embeddings are untouched; "
                    "re-run to continue from where it stopped.",
                    file=sys.stderr,
                )
                return 1
            for key in totals:
                totals[key] += stats[key]
            print(
                f"  embedded={stats['embedded']} kept={stats['kept']} "
                f"deleted={stats['deleted']} in {time.time() - started:.0f}s"
            )
    print(
        f"Total: embedded={totals['embedded']} kept={totals['kept']} "
        f"deleted={totals['deleted']}"
    )
    return 0


def cmd_status(connection, cursor, args) -> int:
    cursor.execute("SHOW TABLES LIKE 'chunk_embeddings'")
    if not cursor.fetchall():
        print("chunk_embeddings table does not exist yet — run rebuild")
        return 0
    print(f"Current index version: {current_embedding_version()}")
    cursor.execute(
        """
        SELECT t.alias, e.translation, e.embedding_version, e.dims,
               COUNT(*) AS rows_count,
               (SELECT COUNT(*) FROM translation_chunks c
                WHERE c.translation = e.translation) AS chunks_count
        FROM chunk_embeddings e JOIN translations t ON t.code = e.translation
        GROUP BY t.alias, e.translation, e.embedding_version, e.dims
        ORDER BY e.translation, e.embedding_version
        """
    )
    rows = cursor.fetchall()
    if not rows:
        print("chunk_embeddings is empty")
        return 0
    for row in rows:
        marker = "OK " if row["rows_count"] == row["chunks_count"] else "!! "
        print(
            f"{marker}{row['alias']:<6} {row['embedding_version']:<45} "
            f"dims={row['dims']} embeddings={row['rows_count']} "
            f"chunks={row['chunks_count']}"
        )
    return 0


def cmd_search(connection, cursor, args) -> int:
    try:
        with GeminiEmbeddingClient() as client:
            query_vector = client.embed_query(args.query)
    except EmbeddingUnavailable as exc:
        print(f"Embedding API unavailable: {exc}", file=sys.stderr)
        return 1
    index = load_index(cursor)
    if not len(index):
        print("Index is empty — run rebuild first")
        return 1
    translation = None
    if args.translation:
        rows = resolve_translations(cursor, args.translation)
        translation = rows[0]["code"]
    hits = index.search(
        query_vector,
        top_k=args.top_k,
        translation=translation,
        language=args.language or None,
    )
    for rank, hit in enumerate(hits, start=1):
        title = f" — {hit.title}" if hit.title else ""
        print(
            f"{rank:>2}. {hit.score:.4f} {hit.alias} {hit.canonical_id} "
            f"[{hit.book_number} {hit.chapter_number}:"
            f"{hit.verse_number_start}-{hit.verse_number_end}]{title}"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Vector index CLI for scripture-selection RAG"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("rebuild", help="(re)build the embedding index")
    p.add_argument(
        "--translations",
        help="Comma-separated aliases or codes (default: all chunked)",
    )
    p.add_argument("--force", action="store_true", help="re-embed everything")
    p.add_argument("--batch-size", type=int, default=50)
    p.set_defaults(func=cmd_rebuild)

    p = sub.add_parser("status", help="show index state per translation")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("search", help="smoke-test cosine search")
    p.add_argument("--query", required=True)
    p.add_argument("--translation", help="alias or code filter")
    p.add_argument("--language", help="language filter, e.g. ru")
    p.add_argument("--top-k", type=int, default=10)
    p.set_defaults(func=cmd_search)

    args = parser.parse_args(argv)

    connection = create_connection()
    if connection is None:
        print("Cannot connect to the database", file=sys.stderr)
        return 1
    cursor = connection.cursor(dictionary=True)
    try:
        return args.func(connection, cursor, args)
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
