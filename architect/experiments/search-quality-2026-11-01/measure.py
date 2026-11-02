"""Manual, billable live measurement of general search quality.

`run` executes `scripture_search._run_search` directly (no HTTP, auth or rate
limit) inside a disposable container built from the development API image and
records, per query and pass: retrieval windows, candidate canonical ids, every
model call (stage, model, HTTP statuses, raw answer), final results, seconds
and errors. No retries; existing output files are never overwritten.
`summary` needs no application code and can run on the host.
Normal tests must not invoke this script.
"""

import argparse
import json
import statistics
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

TOP = 5


def overlaps(coordinates, anchor):
    book, chapter, first, last = anchor
    return any(
        b == book and c == chapter and end >= first and start <= last
        for b, c, start, end in coordinates
    )


def error_chain(error):
    chain = []
    while error is not None:
        chain.append(f"{type(error).__name__}: {error}")
        error = error.__cause__ or error.__context__
    return chain


def run(args):
    import scripture_search as search
    from deadline import Deadline
    from retrieval import parse_canonical_id

    cases = json.loads(args.queries.read_text())
    resources = search.get_resources(allow_stale=False)
    # Pin one warm corpus snapshot; every pass still reads real DB verses.
    search.get_resources = lambda **kwargs: resources
    original_search = search.ScriptureRetriever.search
    original_factory = search.build_query_rewriter
    record = {}

    def retrieve(self, language, query, windows, deadline, top_k=20):
        started = time.monotonic()
        record["retrieval_windows"] = list(windows)
        candidates = original_search(self, language, query, windows, deadline, top_k)
        record["retrieval_seconds"] = round(time.monotonic() - started, 3)
        record["candidates"] = [
            [c.canonical_id, *parse_canonical_id(c.canonical_id)[1:]]
            for c in candidates
        ]
        return candidates

    def factory(**kwargs):
        client = original_factory(**kwargs)
        stage = kwargs["stage"]
        transport = (
            client._chat._client if hasattr(client, "_chat") else client._client
        )
        statuses = []
        transport.event_hooks["response"].append(
            lambda response: statuses.append(response.status_code)
        )
        complete = client.complete

        def observed(instruction, user_content, deadline=None, **kwargs):
            call = {"stage": stage.stage, "model": stage.model, "http": statuses}
            record["calls"].append(call)
            started = time.monotonic()
            try:
                call["answer"] = complete(
                    instruction, user_content, deadline=deadline, **kwargs
                )
                return call["answer"]
            finally:
                call["seconds"] = round(time.monotonic() - started, 3)

        client.complete = observed
        return client

    search.ScriptureRetriever.search = retrieve
    search.build_query_rewriter = factory
    with args.output.open("x") as output:
        for run_index in range(1, args.runs + 1):
            for case in cases:
                record = {
                    "id": case["id"],
                    "run": run_index,
                    "label": args.label,
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
                            limit=TOP,
                        ),
                        Deadline(search.AI_SCRIPTURE_TIMEOUT_SECONDS),
                    )
                    record["status"] = "ok"
                    record["results"] = [
                        [
                            r.canonical.canonical_id,
                            r.canonical.book_number,
                            r.canonical.chapter_number,
                            r.canonical.verse_start,
                            r.canonical.verse_end,
                            [r.highlight.canonical.verse_start,
                             r.highlight.canonical.verse_end],
                        ]
                        for r in response.results
                    ]
                except Exception as error:
                    record["status"] = "error"
                    record["error"] = error_chain(error)
                record["seconds"] = round(time.monotonic() - started, 3)
                output.write(json.dumps(record, ensure_ascii=False) + "\n")
                output.flush()
                print(
                    case["id"], run_index, record["status"], record["seconds"],
                    len(record.get("results", [])), record.get("error", [""])[0],
                    flush=True,
                )
                time.sleep(args.pause)


def summary(args):
    cases = {c["id"]: c for c in json.loads(args.queries.read_text())}
    records = [json.loads(line) for line in args.output.read_text().splitlines()]
    totals = Counter()
    grades = Counter()
    seconds = []
    rows = []
    for r in records:
        case = cases[r["id"]]
        anchors = case["anchors"]
        candidates = [tuple(c[1:5]) for c in r.get("candidates", [])]
        cand = "-"
        if anchors:
            cand = "".join("C" if overlaps(candidates, a) else "." for a in anchors)
        totals["runs"] += 1
        notes = ""
        for call in r["calls"]:
            try:
                answer = json.loads(call.get("answer") or "null")
            except json.JSONDecodeError:
                answer = None
            if call["stage"] == "scripture_rewrite" and answer:
                # Intermediate runs used the key "meanings"; reference
                # answers (2026-11-02) have no query.
                expansions = answer.get("passages", answer.get("meanings"))
                notes += " expansions=" + str([
                    m.get("query") or "{book} {chapter}:{verse_start}-{verse_end}".format(**m)
                    for m in expansions
                ])
            if call["stage"] == "scripture_rerank" and isinstance(answer, dict) and "matches" not in answer:
                if isinstance(answer.get("direct"), list):  # intermediate list format
                    count = Counter({k: len(answer[k]) for k in ("direct", "related")})
                else:
                    count = Counter(g["relevance"] for g in answer.values())
                # Omitted candidates are unrelated (approximate: before range dedup).
                count["unrelated"] = len(r["candidates"]) - sum(count.values())
                grades.update({(case["category"] == "unrelated", k): v for k, v in count.items()})
                notes += " grades=" + "/".join(
                    str(count[k]) for k in ("direct", "related", "unrelated")
                )
        if r["status"] != "ok":
            totals["errors"] += 1
            http = [s for call in r["calls"] for s in call["http"] if s != 200]
            totals[f"error {r['error'][0].split(':')[0]} {http or ''}"] += 1
            rows.append((r["id"], r["run"], cand,
                         "ERROR " + " <- ".join(r["error"]) + notes, r["seconds"]))
            continue
        seconds.append(r["seconds"])
        final = [tuple(x[1:5]) for x in r["results"]]
        n = len(final)
        if anchors:
            hits = [overlaps(final, a) for a in anchors]
            totals["positive"] += 1
            totals["primary_hit"] += hits[0]
            totals["any_hit"] += any(hits)
            totals["anchors"] += len(hits)
            totals["anchors_hit"] += sum(hits)
            totals["primary_candidate"] += cand[0] == "C"
            outcome = "".join("F" if h else "." for h in hits)
        else:
            totals["unrelated"] += 1
            totals["unrelated_empty"] += n == 0
            outcome = "empty" if n == 0 else "NOT EMPTY"
        if case.get("broad"):
            totals["broad"] += 1
            totals["broad_3plus"] += n >= 3
        totals["results"] += n
        ok = ", ".join(f"{b}.{c}:{v1}-{v2}" for b, c, v1, v2 in final)
        rows.append(
            (r["id"], r["run"], cand, f"{outcome} n={n} [{ok}]{notes}", r["seconds"])
        )
    t = totals
    print(f"# {args.output.name}: {t['runs']} runs, {t['errors']} errors")
    print(
        f"primary hit@{TOP} {t['primary_hit']}/{t['positive']}; "
        f"any anchor hit@{TOP} {t['any_hit']}/{t['positive']}; "
        f"anchors hit {t['anchors_hit']}/{t['anchors']}; "
        f"primary among candidates {t['primary_candidate']}/{t['positive']}; "
        f"broad >=3 results {t['broad_3plus']}/{t['broad']}; "
        f"unrelated empty {t['unrelated_empty']}/{t['unrelated']}; "
        f"mean results {t['results'] / max(1, t['runs'] - t['errors']):.2f}; "
        f"median seconds {round(statistics.median(seconds), 3) if seconds else '-'}"
    )
    for key in sorted(k for k in t if k.startswith("error ")):
        print(f"{key}: {t[key]}")
    for unrelated in (False, True):
        if any(k[0] == unrelated for k in grades):
            print(
                ("unrelated" if unrelated else "biblical") + " queries, grades "
                "direct/related/unrelated: "
                + "/".join(str(grades[(unrelated, k)]) for k in ("direct", "related", "unrelated"))
            )
    print("\n| case | run | candidates | output | seconds |\n|---|---|---|---|---|")
    for row in sorted(rows, key=lambda x: (x[0], x[1])):
        print("| " + " | ".join(str(v) for v in row) + " |")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "summary"):
        command = sub.add_parser(name)
        command.add_argument("queries", type=Path)
        command.add_argument("output", type=Path)
    sub.choices["run"].add_argument("--runs", type=int, default=2)
    sub.choices["run"].add_argument("--pause", type=float, default=5.0)
    sub.choices["run"].add_argument("--label", required=True)
    args = parser.parse_args()
    (run if args.command == "run" else summary)(args)


if __name__ == "__main__":
    main()
