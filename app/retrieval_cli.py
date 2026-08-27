"""
CLI smoke-test for the scripture-selection retrieval pipeline
(architect/adr/0004-retrieval-pipeline.md).

Runs the full production path against the live DB and the Gemini API:
query rewrite -> per-variant hybrid search (vector index + BM25) ->
interleave fusion -> blacklist/exclusions -> diversity -> texts from
translation_chunks.

Usage (inside the bible-api container):

    python app/retrieval_cli.py select --language ru \
        --topic "Благодарность за рождение дочки" \
        --reply "Мы ждали её несколько лет" \
        [--exclude v3:19.127.001-005,...] [--top-k 10] [--show-text]

    # empty topic -> safe pool, no Gemini calls
    python app/retrieval_cli.py select --language ru --topic ""
"""

from __future__ import annotations

import argparse

from chunking import CHUNKING_VERSION
from database import create_connection
from embeddings import GeminiEmbeddingClient
from lexical_index import load_lexical_indexes
from passage_rerank import GeminiPassageReranker
from query_rewrite import GeminiQueryRewriter
from retrieval import (
    ScriptureRetriever,
    SelectionRequest,
    make_db_passage_loader,
    make_db_verse_loader,
    prompt_passage,
)
from vector_index import load_index


def cmd_select(args) -> int:
    connection = create_connection()
    if connection is None:
        print("Cannot connect to the database")
        return 1
    cursor = connection.cursor(dictionary=True)
    index = load_index(cursor)
    if not len(index):
        print("Vector index is empty — run app/index_cli.py rebuild first")
        return 1
    lexical = load_lexical_indexes(cursor, CHUNKING_VERSION)
    final = None
    with GeminiEmbeddingClient() as embedder, \
            GeminiQueryRewriter() as rewriter, \
            GeminiPassageReranker() as reranker:
        retriever = ScriptureRetriever(
            index=index,
            embedder=embedder,
            rewriter=rewriter,
            reranker=reranker if args.final else None,
            load_passages=make_db_passage_loader(cursor),
            load_verses=make_db_verse_loader(cursor),
            lexical_indexes=lexical,
        )
        request = SelectionRequest(
            language=args.language,
            topic=args.topic,
            user_replies=tuple(args.reply),
            exclude_canonical_ids=frozenset(
                x.strip() for x in args.exclude.split(",") if x.strip()
            ),
            top_k=args.top_k,
        )
        if args.final:
            final = retriever.select_final(request)
            result = final.selection
        else:
            result = retriever.select(request)

    if final is not None:
        chosen = (
            final.candidate.canonical_id if final.candidate else "(none)"
        )
        print(f"final: {chosen}  method={final.method}"
              + (f" fallback={final.fallback_reason}"
                 if final.fallback_reason else ""))
        if final.reason:
            print(f"final reason (diagnostic): {final.reason}")
        shown = (
            prompt_passage(final.candidate) if final.candidate else None
        )
        if final.highlight and shown is not None:
            start, end = final.highlight
            verses = shown.verses[start - 1:end]
            print(
                f"key verses: {shown.book_number} {shown.chapter_number}:"
                f"{verses[0].verse_number}-{verses[-1].verse_number} "
                f"(markers {start}-{end})"
            )
            print("  " + " ".join(verse.text for verse in verses))
    print(f"source: {result.source}"
          + (f" (fallback: {result.fallback_reason})"
             if result.fallback_reason else ""))
    if result.rewrite_failed:
        print("note: query rewrite failed, searched the raw query")
    if result.query_variants:
        print("query variants:")
        for i, query in enumerate(result.query_variants):
            print(f"  v{i}: {query}")
    print(f"candidates ({len(result.candidates)}):")
    for rank, candidate in enumerate(result.candidates, start=1):
        score = "-" if candidate.score is None else f"{candidate.score:.3f}"
        variant = (
            "-" if candidate.best_variant is None
            else f"v{candidate.best_variant}"
        )
        print(f"{rank:3d}. {candidate.canonical_id}  score={score} {variant}"
              f"  canonical {candidate.book_number} "
              f"{candidate.chapter_number}:{candidate.verse_start}"
              f"-{candidate.verse_end}")
        for passage in candidate.passages:
            line = (f"       [{passage.alias}] {passage.book_number} "
                    f"{passage.chapter_number}:{passage.verse_number_start}"
                    f"-{passage.verse_number_end}")
            if passage.title:
                line += f"  «{passage.title}»"
            print(line)
            if args.show_text:
                text = passage.text
                print("       " + (text[:300] + "…" if len(text) > 300 else text))
    cursor.close()
    connection.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("select", help="run one selection")
    p.add_argument("--language", required=True, choices=("ru", "en", "uk"))
    p.add_argument("--topic", default="")
    p.add_argument("--reply", action="append", default=[])
    p.add_argument("--exclude", default="",
                   help="comma-separated canonical IDs already shown")
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--final", action="store_true",
                   help="also run the grounded rerank (select_final)")
    p.add_argument("--show-text", action="store_true")
    p.set_defaults(func=cmd_select)

    args = parser.parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
