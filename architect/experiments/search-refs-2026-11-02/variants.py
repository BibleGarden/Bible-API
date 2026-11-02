"""Offline, no-LLM replay: how should expansion references enter retrieval?

Reads the recorded expansion references of a measure.py output (2026-11-02
paraphrase run, free-text `ref` such as "Мф 6:34"), maps them to windows with
the production `_reference_windows` and compares the candidate rank of every
anchor for three ways to search them, plus the recorded candidates:

- text: the windows' database text as extra queries (embedding + BM25 each);
- pinned: the windows alone, ahead of the query's hybrid ranking;
- vectors: each window's stored vector as a semantic query (production).

Embeddings only (no Gemini calls). Prints one JSON line per positive run.
"""

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import scripture_search as search
import scripture_select as select
from canon import CANONICAL_BOOK_CODES
from database import create_connection
from deadline import Deadline
from embeddings import build_embedding_client
from retrieval import (
    ScriptureRetriever,
    apply_diversity,
    fuse_interleave,
    make_db_passage_loader,
    parse_canonical_id,
)

NAMES = {
    "1 Kings": "1ki", "1 Peter": "1pe", "1 Samuel": "1sa", "1 Петра": "1pe",
    "1 Царств": "1sa", "1Петра": "1pe", "4 Царств": "2ki", "Ecclesiastes": "ecc",
    "Exodus": "exo", "Genesis": "gen", "Hebrews": "heb", "Jonah": "jon",
    "Joshua": "jos", "Leviticus": "lev", "Luke": "luk", "Matthew": "mat",
    "Philippians": "php", "Romans": "rom", "Бытие": "gen", "Екклесиаст": "ecc",
    "Иеремия": "jer", "Иисус Навин": "jos", "Иона": "jon", "Исход": "exo",
    "Левит": "lev", "Луки": "luk", "Марка": "mrk", "Матфея": "mat", "Мф": "mat",
    "Псалмы": "psa", "Римлянам": "rom", "Филиппийцам": "php", "Флп": "php",
}
NUMBERS = {code: number for number, code in CANONICAL_BOOK_CODES.items()}


def parse(ref):
    name, chapter, first, last = re.match(
        r"^(.*?)\s*(\d+):(\d+)(?:[-–](\d+))?$", ref
    ).groups()
    return NUMBERS[NAMES[name]], int(chapter), int(first), int(last or first)


def rank_of(candidates, anchor):
    book, chapter, first, last = anchor
    for position, canonical_id in enumerate(candidates, 1):
        _v, b, c, start, end = parse_canonical_id(canonical_id)
        if (b, c) == (book, chapter) and end >= first and start <= last:
            return position
    return None


def main(records, queries):
    cases = {case["id"]: case for case in json.load(open(queries))}
    resources = select.get_resources(allow_stale=False)
    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    load = make_db_passage_loader(cursor)
    with build_embedding_client(timeout=20, max_retries=2) as embedder:
        for line in open(records):
            record = json.loads(line)
            case = cases[record["id"]]
            if record["status"] != "ok" or not case["anchors"]:
                continue
            language, query = case["language"], case["query"]
            answer = next(
                c["answer"] for c in record["calls"] if c["stage"] == "scripture_rewrite"
            )
            references = [parse(p["ref"]) for p in json.loads(answer)["passages"]]
            allowed = select.coverage_filter(resources, language, case["translation"])
            windows = search._reference_windows(
                resources.windows[language], allowed, references
            )
            retriever = ScriptureRetriever(
                index=resources.index, embedder=embedder, rewriter=None,
                load_passages=load, lexical_indexes=resources.lexical,
                blacklist=[], safe_pool=[], allowed_canonical_ids=allowed,
            )

            def hybrid(text, vector):
                semantic = [
                    (hit.canonical_id, hit.score)
                    for hit in resources.index.search(
                        vector, top_k=retriever.fetch_k, language=language
                    )
                ]
                return retriever._merge_lexical(language, text, vector, semantic)

            def finish(rankings):
                fused = [
                    hit for hit in fuse_interleave(rankings)
                    if retriever._allowed(hit.canonical_id)
                ]
                return [h.canonical_id for h in apply_diversity(fused, 20)]

            seconds = {}
            started = time.monotonic()
            texts = load(resources.indexed[language][0][0], windows)
            texts = [texts[w]["text"] for w in windows] + [query]
            with ThreadPoolExecutor(len(texts)) as pool:
                vectors = list(pool.map(embedder.embed_query, texts))
            text = finish([hybrid(t, v) for t, v in zip(texts, vectors)])
            seconds["text"] = time.monotonic() - started
            started = time.monotonic()
            query_ranking = hybrid(query, embedder.embed_query(query))
            pinned = finish([[(w, 1.0)] for w in windows] + [query_ranking])
            seconds["pinned"] = time.monotonic() - started
            started = time.monotonic()
            vectors = [
                c.canonical_id
                for c in retriever.search(language, query, windows, Deadline(30))
            ]
            seconds["vectors"] = time.monotonic() - started
            recorded = [c[0] for c in record["candidates"]]
            print(json.dumps({
                "id": record["id"],
                "run": record["run"],
                "windows": windows,
                **{
                    name: [rank_of(candidates, anchor) for anchor in case["anchors"]]
                    for name, candidates in (
                        ("recorded", recorded), ("text", text),
                        ("pinned", pinned), ("vectors", vectors),
                    )
                },
                "seconds": {k: round(v, 3) for k, v in seconds.items()},
            }), flush=True)


if __name__ == "__main__":
    main(*sys.argv[1:])
