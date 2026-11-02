"""Manual, billable search ablation against the development container only.

Run inside the container with /code/app on PYTHONPATH. No retries. Existing
output files are never overwritten. Normal tests must not invoke this script.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import scripture_search as search
from config import EMBEDDING_MODEL
from deadline import Deadline
from retrieval import parse_canonical_id


def hits(coordinates, anchors):
    return any(
        book == target_book
        and chapter == target_chapter
        and end >= first
        and start <= last
        for book, chapter, start, end in coordinates
        for target_book, target_chapter, first, last in anchors
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("queries", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    if not hasattr(search, "_rewrites"):
        raise RuntimeError(
            "This historical experiment requires Bible-API baseline 81a6c94"
        )
    queries = json.loads(args.queries.read_text())
    resources = search.get_resources(allow_stale=False)
    # Pin a warm corpus snapshot; both arms still read/render real DB verses.
    search.get_resources = lambda **kwargs: resources
    original_rewrite = search._rewrites
    original_retrieve = search.ScriptureRetriever.search
    original_factory = search.build_query_rewriter
    embed_factory = search.build_embedding_client
    record = {}

    def instrument(client, stage):
        transport = client._chat._client if hasattr(client, "_chat") else client._client

        def observe(response):
            response.read()
            event = {"stage": stage, "http_status": response.status_code}
            if response.status_code == 200:
                payload = response.json()
                event["usage"] = payload.get("usage", payload.get("usageMetadata", {}))
            record["calls"].append(event)

        transport.event_hooks["response"].append(observe)
        return client

    def factory(**kwargs):
        return instrument(original_factory(**kwargs), kwargs["stage"].stage)

    search.build_query_rewriter = factory
    search.build_embedding_client = lambda **kwargs: instrument(
        embed_factory(**kwargs), "embedding"
    )

    def rewrite(generator, query, language, deadline):
        variants = (
            [query]
            if record["mode"] == "raw"
            else original_rewrite(generator, query, language, deadline)
        )
        record["variants"] = variants
        return variants

    def retrieve(self, *args, **kwargs):
        candidates = original_retrieve(self, *args, **kwargs)
        record["candidates"] = [candidate.canonical_id for candidate in candidates]
        return candidates

    search._rewrites = rewrite
    search.ScriptureRetriever.search = retrieve
    providers = {
        "rewrite": search.SCRIPTURE_REWRITE_PROVIDER.model,
        "rerank": search.SCRIPTURE_RERANK_PROVIDER.model,
        "embedding": EMBEDDING_MODEL,
    }
    # Alternate the first arm to reduce systematic warm-up/time-order bias.
    with args.output.open("x") as output:
        for position, case in enumerate(queries):
            for mode in (
                ("rewrite", "raw") if position % 2 == 0 else ("raw", "rewrite")
            ):
                record = {
                    "id": case["id"],
                    "mode": mode,
                    "models": providers,
                    "started_utc": datetime.now(timezone.utc).isoformat(),
                    "calls": [],
                }
                started = time.monotonic()
                try:
                    response = search._run_search(
                        search.SearchRequest(
                            query=case["query"],
                            language=case["language"],
                            translation=case["translation"],
                            limit=5,
                        ),
                        Deadline(search.AI_SCRIPTURE_TIMEOUT_SECONDS),
                    )
                    record["status"] = "ok"
                    record["results"] = [
                        {
                            "canonical": result.canonical.model_dump(),
                            "highlight": result.highlight.model_dump(),
                        }
                        for result in response.results
                    ]
                    final = [
                        (
                            r.canonical.book_number,
                            r.canonical.chapter_number,
                            r.canonical.verse_start,
                            r.canonical.verse_end,
                        )
                        for r in response.results
                    ]
                    record["final_anchor_hit"] = hits(final, case["anchors"])
                    record["empty"] = not response.results
                except Exception as error:
                    record["status"] = "error"
                    record["error_type"] = type(error).__name__
                record["elapsed_seconds"] = round(time.monotonic() - started, 4)
                candidates = [
                    parse_canonical_id(cid)[1:] for cid in record.get("candidates", [])
                ]
                record["candidate_anchor_hit"] = hits(candidates, case["anchors"])
                output.write(json.dumps(record, ensure_ascii=False) + "\n")
                output.flush()
                print(
                    case["id"],
                    mode,
                    record["status"],
                    record["elapsed_seconds"],
                    "candidate_hit",
                    record["candidate_anchor_hit"],
                    "final_hit",
                    record.get("final_anchor_hit"),
                    flush=True,
                )


if __name__ == "__main__":
    main()
