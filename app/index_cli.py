"""
CLI for the scripture-selection vector index (see
architect/adr/0002-embedding-model-and-vector-store.md).

Embeds `translation_chunks` rows with the configured embedding model
(EMBEDDING_MODEL / EMBEDDING_DIMENSIONS) through the configured provider
(EMBEDDING_PROVIDER: the Gemini API, bge-m3 in this process — ADR 0010 — or
bge-m3 on the company server over `POST {endpoint}/embeddings` — ADR 0014)
and stores the vectors in `cep_public.chunk_embeddings`. MySQL remains the
canonical store; the whole index is rebuilt by one command and the run is
idempotent — re-running embeds nothing new and never creates duplicates.

Usage (inside the bible-api container):

    # full rebuild / catch-up for all chunked translations
    python app/index_cli.py rebuild

    # specific translations, forced re-embedding
    python app/index_cli.py rebuild --translations syn,bsb --force

    # index state per translation/version
    python app/index_cli.py status

    # smoke-test search (embeds the query through the configured provider)
    python app/index_cli.py search --query "благодарность за ребёнка" \
        --translation syn --top-k 5

A rebuild KEEPS the rows of every other index version, so the migration to
another embedding model builds its index beside the live one and the switch
is an `.env` edit plus a restart. Drop the old rows afterwards, deliberately,
once the new version has been verified:

    python app/index_cli.py rebuild --drop-other-versions
"""

from __future__ import annotations

import argparse
import sys
import time

from config import (
    EMBEDDING_PROVIDER,
    EMBEDDING_PROVIDER_GEMINI,
    EMBEDDING_PROVIDER_LOCAL,
    EMBEDDING_PROVIDER_OPENAI_COMPAT,
    GEMINI_API_KEY,
)
from database import create_connection
from embeddings import (
    REMOTE_MAX_BATCH_SIZE,
    EmbeddingUnavailable,
    build_embedding_client,
)
from vector_index import (
    IndexVersionUnavailable,
    MissingChunksError,
    current_embedding_version,
    load_index,
    reindex_translation,
)


# How many chunks are handed to the embedder — and committed — at once.
#
# On Gemini every text is its own HTTP call, so this is only the size of the
# INSERT batch and 50 has always been right. On the local model it is the
# size of ONE `encode()` call, and sentence-transformers sorts a call's texts
# by length before batching them: the longer the list, the less padding is
# computed. Measured on this corpus, same machine, same encode batch:
# 0.75 chunks/s at 50, **2.8 chunks/s at 512** — a two-hour rebuild instead
# of a five-hour one, for a list of 512 vectors (2 MB) held in memory.
#
# On `openai_compat` it is the size of one HTTP request, and the client caps
# it at `REMOTE_MAX_BATCH_SIZE` anyway (ADR 0014): asking for more here would
# only make the commit coarser than the retry unit.
DEFAULT_BATCH_SIZE = 50
LOCAL_DEFAULT_BATCH_SIZE = 512
REMOTE_DEFAULT_BATCH_SIZE = REMOTE_MAX_BATCH_SIZE


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
    # Refuse BEFORE any row is touched. A rebuild can delete rows (a chunk
    # that disappeared, or every other version with --drop-other-versions),
    # so an unresolvable target version — or an embedder that cannot produce
    # a single vector — must stop the run here rather than on the first call.
    if EMBEDDING_PROVIDER == EMBEDDING_PROVIDER_GEMINI and not GEMINI_API_KEY:
        print(
            "GEMINI_API_KEY is not configured — a rebuild on "
            f"EMBEDDING_PROVIDER={EMBEDDING_PROVIDER} has to embed every "
            "chunk through the Gemini API. Nothing was changed.",
            file=sys.stderr,
        )
        return 1
    try:
        version = current_embedding_version()
    except IndexVersionUnavailable as exc:
        print(f"REFUSED: {exc}. Nothing was changed.", file=sys.stderr)
        return 1
    translations = resolve_translations(cursor, args.translations)
    if not translations:
        print("No chunked translations found — run app/chunk_cli.py first")
        return 1
    # The local model is loaded here — after resolving the translations
    # (read-only SELECTs, and a typo in --translations should not cost a
    # 2.3 GB load first) and before the first write: a missing or unreadable
    # weights volume is the local provider's "no API key", and it must not
    # surface half-way through a rebuild.
    try:
        client = build_embedding_client()
    except EmbeddingUnavailable as exc:
        print(f"REFUSED: {exc}. Nothing was changed.", file=sys.stderr)
        return 1
    print(f"Index version: {version}")
    if EMBEDDING_PROVIDER == EMBEDDING_PROVIDER_OPENAI_COMPAT:
        # The two bge-m3 providers do NOT write byte-identical rows, and the
        # index version cannot say so (it names the model, and it is the same
        # model). The local client caps the input at 512 tokens; the server
        # applies its own, larger window, so the 6.8% of chunks that are
        # longer get a fuller vector here (ClickUp 86cbehd6h, ADR 0014).
        # Harmless in itself — but a catch-up rebuild (without --force) would
        # leave those chunks half from one provider and half from the other,
        # which is why this is said out loud rather than left to be noticed.
        print(
            "Provider is openai_compat: chunks over 512 tokens (811 of "
            "11 960 today) get a FULLER vector than the local provider "
            "wrote. Use --force to rebuild a translation whole rather than "
            "mixing the two inside one index version."
        )
    if args.drop_other_versions:
        print(
            "Rows of every OTHER index version will be DELETED "
            "(--drop-other-versions)"
        )
    totals = {"embedded": 0, "kept": 0, "deleted": 0}
    with client:
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
                    drop_other_versions=args.drop_other_versions,
                )
            except MissingChunksError as exc:
                print(f"  REFUSED: {exc}", file=sys.stderr)
                return 1
            except EmbeddingUnavailable as exc:
                print(f"  ABORTED: {exc}", file=sys.stderr)
                print(
                    "  Embeddings of the current index version are kept "
                    "(rows of an outdated version may already have been "
                    "removed); re-run to continue from where it stopped.",
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
        with build_embedding_client() as client:
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


def build_parser() -> argparse.ArgumentParser:
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
    p.add_argument(
        "--batch-size",
        type=int,
        default=(
            LOCAL_DEFAULT_BATCH_SIZE
            if EMBEDDING_PROVIDER == EMBEDDING_PROVIDER_LOCAL
            else REMOTE_DEFAULT_BATCH_SIZE
            if EMBEDDING_PROVIDER == EMBEDDING_PROVIDER_OPENAI_COMPAT
            else DEFAULT_BATCH_SIZE
        ),
        help="chunks per embedder call and per commit (see the constants)",
    )
    p.add_argument(
        "--drop-other-versions",
        action="store_true",
        help=(
            "also DELETE every row of another index version (the cleanup "
            "after a model migration; without it they are kept, so the old "
            "index keeps serving while the new one is built)"
        ),
    )
    p.set_defaults(func=cmd_rebuild)

    p = sub.add_parser("status", help="show index state per translation")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("search", help="smoke-test cosine search")
    p.add_argument("--query", required=True)
    p.add_argument("--translation", help="alias or code filter")
    p.add_argument("--language", help="language filter, e.g. ru")
    p.add_argument("--top-k", type=int, default=10)
    p.set_defaults(func=cmd_search)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    connection = create_connection()
    if connection is None:
        print("Cannot connect to the database", file=sys.stderr)
        return 1
    cursor = connection.cursor(dictionary=True)
    try:
        return args.func(connection, cursor, args)
    except IndexVersionUnavailable as exc:
        # Only reachable for the read-only commands; cmd_rebuild refuses
        # earlier, before it can touch a row.
        print(f"{exc}", file=sys.stderr)
        return 1
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
