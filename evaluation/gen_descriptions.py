#!/usr/bin/env python3
"""
Annotate the chunk corpus with the situations each fragment can serve
(ClickUp 86cbeef7h, umbrella 86cbe4mtq).

What this is for. Vector search over the RAW prayer fails — hit@10 0.286 for
Gemini and 0.238 for bge-m3 (README, 86cbe4n7e, "Главная поправка") — because
the prayer and the Scripture fragment use different words. Production bridges
that gap per request, in the rewrite stage, which a local model does not carry
(86cbea05x). The hypothesis this tool exists to test: move the bridge into the
INDEX. Annotate each fragment once, in the register of human situations ("who
this passage can serve, and in what state"), embed those annotations, and
search them with the raw prayer. If that works, the rewrite stage is not
needed at all.

Each fragment gets a LIST of 2-5 short senses (one situation each) plus a
structured `caution` flag — see `description_prompts.py` for why the senses
are a list and why the caution is a field rather than a sentence.

Three subcommands, ONE prompt and ONE validator
-----------------------------------------------
* `generate` — the API mode: talk to any OpenAI-compatible chat-completions
  endpoint, batch by batch, and write the artifact.
* `prepare` — write the batches to a file instead of sending them, so an
  agent with a model subscription can answer them by hand (no API, no key).
  Every batch line carries the SAME versioned instruction and user content
  the API mode would have sent.
* `ingest` — read those answers back, validate them with the SAME parser the
  API mode uses, and write the SAME artifact. Batches that are missing or
  unusable are reported by `batch_id` so they can be filled in and ingested
  again (`--resume`).

Because the prompt and the validator are shared, an artifact made offline and
one made through an API are comparable measurements rather than two different
experiments.

Output (`generate` and `ingest`) is a JSONL file — one line per fragment —
consumed by `retrieval_benchmark.py pipeline --descriptions-file … --doc-text
description`, which indexes every sense as its own vector under the
fragment's `canonical_id`.

API protocol (deliberately the same as `gen_rewrites.py` / `gen_reranks.py`,
so the artifacts read alike): temperature 0, `response_format=json_object`, an
unusable answer retried once, transport failures up to three attempts, the
wall time of every call recorded. Two differences follow from the corpus being
12 000 fragments rather than 24 scenarios: calls are BATCHED (`--batch-size`,
default 8) and an answer must carry every id of its batch; and the file is
written incrementally and can be resumed (`--resume`), because a full-corpus
run is hours long and must survive being interrupted.

The key never reaches the artifact: `meta.endpoint` keeps scheme+host only,
and a transport failure is recorded as its category (httpx puts the full
request URL, query string included, into an HTTPStatusError message).

Examples:

    # probe: 20 fragments per translation through a local Ollama
    python gen_descriptions.py generate --endpoint http://localhost:11434/v1 \\
        --model qwen3:4b-instruct-2507-q4_K_M --limit 20 \\
        --out bench_data/qwen3-4b_senses_probe.jsonl

    # one whole translation (~4k fragments) on any OpenAI-compatible model
    python gen_descriptions.py generate --endpoint <base URL> --model <id> \\
        --only-translation bsb --batch-size 8 --resume \\
        --out bench_data/senses_bsb.jsonl

    # offline: batches out, answers in
    python gen_descriptions.py prepare --only-translation bsb \\
        --out bench_data/desc_batches_bsb.jsonl
    python gen_descriptions.py ingest --model <name for meta> \\
        --batches bench_data/desc_batches_bsb.jsonl \\
        --answers bench_data/desc_answers_bsb.jsonl \\
        --out bench_data/senses_bsb.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

import httpx

HERE = Path(__file__).resolve().parent
# Unlike gen_rewrites.py / gen_reranks.py this tool imports nothing from
# `app/`: its prompt is not a variant of a production one, so there is no
# sys.path surgery and no config stand-ins here.

from description_prompts import (  # noqa: E402
    DESCRIPTION_PROMPT_VERSION,
    MIN_SENSES,
    DescriptionError,
    build_description_instruction,
    build_description_user_content,
    find_reference_leaks,
    parse_description_response,
)

CHUNKS_FILE = HERE / "bench_data" / "chunks.jsonl"

# 8 fragments of ~850 characters each is ~3-4k tokens of prompt. Larger
# batches are cheaper per fragment but lose more work when one answer is
# broken, and they overflow a small local context window (see --batch-size).
DEFAULT_BATCH_SIZE = 8
# A batch of eight annotations is a few hundred tokens; the room above that
# is for models that think out loud.
DEFAULT_MAX_TOKENS = 2048
# One batch may legitimately take minutes on a CPU-only local model.
DEFAULT_TIMEOUT_SECONDS = 600.0
TRANSPORT_ATTEMPTS = 3
# A broken OR incomplete answer is retried once — the same allowance the
# other two generators give.
PARSE_ATTEMPTS = 2

# Some small models emit a reasoning block before the answer; the JSON
# extraction is greedy, so strip it rather than let a brace inside the
# reasoning swallow the real object.
_THINK_BLOCK = re.compile(r"<think>.*?</think>", flags=re.DOTALL | re.IGNORECASE)

PARTIAL_NOTE = (
    "PARTIAL run: fewer fragments annotated than bench_data/chunks.jsonl "
    "holds (--limit, --only-translation, --ids, or the run was interrupted). "
    "`pipeline --doc-text description` falls back to title_text for every "
    "fragment missing here and says how many; a LANGUAGE with no coverage at "
    "all is refused outright rather than degraded, so the numbers of such a "
    "run are NOT comparable with a full-corpus one."
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def endpoint_host(endpoint: str) -> str:
    """Host of the endpoint, never its query string (keys live there)."""
    parts = urlsplit(endpoint)
    return f"{parts.scheme}://{parts.netloc}" if parts.netloc else endpoint


def completions_url(endpoint: str) -> str:
    base = endpoint.rstrip("/")
    if base.endswith("/chat/completions"):
        return base
    return f"{base}/chat/completions"


def transport_error(exc: Exception) -> str:
    """Failure category of a transport error — never the provider's message.

    httpx renders an HTTPStatusError as "... for url '<full url>'", and that
    URL carries whatever the endpoint puts in its query string, `?key=…`
    included. Only the exception type and — for an HTTP error — the status
    code are recorded.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return f"transport: HTTPStatusError (HTTP {exc.response.status_code})"
    return f"transport: {type(exc).__name__}"


def resolve_path(value: str) -> Path:
    """Relative paths are relative to evaluation/, not to the caller's cwd."""
    path = Path(value)
    return path if path.is_absolute() else HERE / path


def meta_path(out: Path) -> Path:
    """Sidecar holding the run's metadata.

    A sidecar rather than a first line of the JSONL: the data file is
    APPENDED to (a full-corpus run writes 12 000 lines and must be resumable),
    and rewriting it after every batch just to refresh a header would be both
    quadratic and a chance to truncate hours of work.
    """
    return out.with_name(out.name + ".meta.json")


def load_jsonl(path: Path) -> list[dict]:
    """Every complete JSON line of a file, in order.

    A truncated last line — the signature of a crash mid-append — is skipped
    rather than fatal; anything else malformed raises, because a corrupted
    answer file must not quietly shrink a measurement.
    """
    rows = []
    lines = path.read_text(encoding="utf-8").splitlines()
    for number, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            if number == len(lines):
                break
            raise
    return rows


def load_corpus(path: Path) -> list[dict]:
    """The chunk corpus, in file order (the order the matrices are built in)."""
    return load_jsonl(path)


def load_existing(out: Path) -> set[str]:
    """Canonical keys already annotated in `out` (for --resume).

    A row with no senses (an error row) is NOT counted as done: a resumed run
    retries exactly the fragments that failed.
    """
    done: set[str] = set()
    if not out.exists():
        return done
    for row in load_jsonl(out):
        if row.get("senses"):
            done.add(f"{row.get('translation')}:{row.get('canonical_id')}")
    return done


def append_records(out: Path, records: list[dict]) -> None:
    """Append one batch and get it onto the disk.

    The whole batch is one `write` of complete lines followed by fsync, so an
    interrupted run leaves whole records behind it (and at worst one partial
    trailing line, which `load_jsonl` skips).
    """
    if not records:
        return
    payload = "".join(
        json.dumps(record, ensure_ascii=False) + "\n" for record in records
    )
    with out.open("a", encoding="utf-8") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())


def write_meta(out: Path, meta: dict) -> None:
    """Write the sidecar atomically: a crash must not truncate a good one."""
    path = meta_path(out)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    tmp.replace(path)


def parse_ids(value: str) -> set[str]:
    """`--ids`: a comma-separated list, or a file with one id per line."""
    if not value:
        return set()
    path = resolve_path(value)
    if path.exists():
        return {
            line.strip() for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        }
    return {part.strip() for part in value.split(",") if part.strip()}


def select_chunks(
    corpus: list[dict], only_translation: str, limit: int, ids: set[str],
) -> list[dict]:
    """The fragments this run will annotate, grouped by translation.

    `--limit` is PER TRANSLATION, not per run: a probe is only useful if it
    covers every language, and `--limit 20` over a corpus ordered by
    translation would otherwise stay inside the first one. `--ids` selects
    named fragments (a hand-picked hard set) and ignores `--limit`.
    """
    wanted = {w.strip() for w in only_translation.split(",") if w.strip()}
    groups: dict[int, list[dict]] = {}
    for chunk in corpus:
        if wanted and not (
            str(chunk["translation"]) in wanted or chunk.get("alias") in wanted
        ):
            continue
        if ids and chunk["canonical_id"] not in ids:
            continue
        groups.setdefault(chunk["translation"], []).append(chunk)
    selected = []
    for code in sorted(groups):
        rows = groups[code]
        selected.extend(rows if ids or not limit else rows[:limit])
    return selected


def make_batches(chunks: list[dict], batch_size: int) -> list[list[dict]]:
    """Slice the selection into batches that never cross a translation.

    A batch is ONE prompt with ONE language fixed in its instruction, so a
    batch holding two translations asks the model to answer part of it in the
    wrong language — and `account()` would file the whole batch under the
    first translation. `select_chunks` returns the fragments grouped by
    translation, and the slicing has to respect those groups instead of
    cutting the flat list every `batch_size` rows (the bug this function
    exists to prevent: batches b00003 and b00006 of the first probe file held
    ru+en and en+uk).
    """
    batches: list[list[dict]] = []
    group: list[dict] = []
    for chunk in chunks:
        if group and chunk["translation"] != group[0]["translation"]:
            batches.extend(
                group[i:i + batch_size] for i in range(0, len(group), batch_size)
            )
            group = []
        group.append(chunk)
    if group:
        batches.extend(
            group[i:i + batch_size] for i in range(0, len(group), batch_size)
        )
    return batches


def batch_language(chunks: list[dict]) -> str:
    """The one language of a batch; a mixed batch is a bug, not a warning."""
    languages = {chunk["language"] for chunk in chunks}
    if len(languages) != 1:
        raise AssertionError(
            f"a batch must hold one language, got {sorted(languages)} — the "
            f"instruction fixes the answer language per batch"
        )
    return languages.pop()


def batch_items(chunks: list[dict]) -> list[dict]:
    """Prompt items of one batch: small ids local to the batch.

    Small integers rather than canonical ids because a weak model copies a
    short number reliably and mangles `v3:19.127.001-005`. The canonical id
    travels beside them (`prepare` writes it into every item) so an offline
    answer can still be traced to its fragment.
    """
    return [
        {"id": i, "title": chunk.get("title") or "", "text": chunk["text"]}
        for i, chunk in enumerate(chunks, start=1)
    ]


def build_records(
    chunks: list[dict],
    items: list[dict],
    found: dict[int, dict],
    latency_ms: int,
    attempts: int,
    last_error: str,
) -> list[dict]:
    """Artifact rows of one batch — the annotated ones and the missing ones.

    Shared by the API mode and by `ingest`, so an offline artifact is the
    same document as an online one, field for field.
    """
    records = []
    for item, chunk in zip(items, chunks):
        entry = found.get(item["id"])
        record = {
            "canonical_id": chunk["canonical_id"],
            "translation": chunk["translation"],
            "alias": chunk.get("alias", ""),
            "language": chunk["language"],
            "senses": [] if entry is None else entry["senses"],
            "caution": False if entry is None else entry["caution"],
            "caution_note": (
                None if entry is None or not entry["caution_note"]
                else entry["caution_note"]
            ),
            # Batch-level values, repeated on each of its rows: the call is
            # the unit of work, and hiding that would make the latency table
            # of a run look like per-fragment cost.
            "latency_ms": latency_ms,
            "attempts": attempts,
            "error": None if entry is not None else (
                last_error or "missing from the answer"
            ),
        }
        if entry is not None:
            if len(entry["senses"]) < MIN_SENSES:
                # A warning, NOT an error: one honest sense is usable, and
                # `--descriptions-file` must still index it. A run full of
                # these means the contract is not landing.
                record["warning"] = (
                    f"short answer: {len(entry['senses'])} of {MIN_SENSES}+ "
                    f"senses"
                )
            leaks = sorted({
                leak
                for sense in entry["senses"]
                for leak in find_reference_leaks(sense)
            })
            if leaks:
                # Not an error either: the senses are still usable, but the
                # prompt forbids coordinates and a run full of these means
                # the model is retelling instead of describing.
                record["reference_leaks"] = leaks
        records.append(record)
    return records


def empty_stats() -> dict:
    return {
        "annotated": 0, "errors": 0, "senses": 0, "caution": 0,
        "batches": 0, "per_translation": {},
    }


def account(stats: dict, chunks: list[dict], records: list[dict]) -> tuple[int, int]:
    """Fold one batch's records into the run statistics."""
    ok = sum(1 for r in records if r["error"] is None)
    senses = sum(len(r["senses"]) for r in records)
    cautions = sum(1 for r in records if r["caution"])
    stats["batches"] += 1
    stats["annotated"] += ok
    stats["errors"] += len(records) - ok
    stats["senses"] += senses
    stats["caution"] += cautions
    per = stats["per_translation"].setdefault(
        chunks[0].get("alias") or str(chunks[0]["translation"]),
        {"annotated": 0, "errors": 0, "senses": 0, "caution": 0},
    )
    per["annotated"] += ok
    per["errors"] += len(records) - ok
    per["senses"] += senses
    per["caution"] += cautions
    return ok, cautions


def build_meta(
    args: argparse.Namespace,
    corpus_size: int,
    chunking_version: int | None,
    stats: dict,
    mode: str,
    extra: dict | None = None,
) -> dict:
    """The metadata sidecar, including whether this run is partial.

    `partial` is derived, never passed in by hand: a run is full only when
    every fragment of `chunks.jsonl` ended up annotated.
    """
    partial = stats["annotated"] < corpus_size
    meta = {
        "model": args.model,
        "mode": mode,
        "date": date.today().isoformat(),
        "prompt_version": DESCRIPTION_PROMPT_VERSION,
        "chunking_version": chunking_version,
        "corpus": str(args.chunks),
        "corpus_size": corpus_size,
        "partial": partial,
        "annotated": stats["annotated"],
        "errors": stats["errors"],
        "senses": stats["senses"],
        "caution": stats["caution"],
        "batches": stats["batches"],
        "per_translation": stats["per_translation"],
    }
    if mode == "api":
        meta["endpoint"] = endpoint_host(args.endpoint)
        meta["sampling"] = {
            "temperature": args.temperature,
            "max_tokens": args.max_tokens,
            "response_format": "json_object",
            "batch_size": args.batch_size,
        }
    if extra:
        meta.update(extra)
    if partial:
        meta["note"] = PARTIAL_NOTE
    return meta


def report_written(out: Path, corpus_size: int, stats: dict, partial: bool) -> None:
    state = "PARTIAL" if partial else "full"
    digest = hashlib.sha1(out.read_bytes()).hexdigest()[:12]
    print(
        f"\nwrote {out} — {state}: {stats['annotated']} of {corpus_size} "
        f"corpus fragments annotated ({stats['senses']} senses, "
        f"{stats['caution']} flagged caution), {stats['errors']} failed; "
        f"file sha1 {digest}… (that prefix names the embedding matrix)"
    )


# ---------------------------------------------------------------------------
# API mode
# ---------------------------------------------------------------------------

def call_model(
    client: httpx.Client,
    url: str,
    api_key: str,
    model: str,
    instruction: str,
    user_content: str,
    temperature: float,
    max_tokens: int,
) -> str:
    """One chat completion; raises httpx errors for the transport ladder."""
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": instruction},
            {"role": "user", "content": user_content},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    response = client.post(url, json=payload, headers=headers)
    response.raise_for_status()
    data = response.json()
    try:
        message = data["choices"][0]["message"]
    except (KeyError, IndexError, TypeError) as exc:
        raise DescriptionError("response has no choices") from exc
    text = message.get("content") or ""
    if not isinstance(text, str) or not text.strip():
        # Some providers put the answer in a reasoning field when the visible
        # content came back empty; that is a failure, not something to guess.
        raise DescriptionError("response content is empty")
    return _THINK_BLOCK.sub("", text)


def generate_batch(
    client: httpx.Client,
    url: str,
    args: argparse.Namespace,
    chunks: list[dict],
) -> list[dict]:
    """Annotations for one batch of fragments, in the batch's own order.

    Every fragment of the batch gets a record: the ones the model annotated,
    and the ones it silently dropped (with `error`). An INCOMPLETE answer is
    retried once as a whole — the same allowance a broken one gets — and what
    the first attempt did return is kept, so a retry can only add.
    """
    language = batch_language(chunks)
    items = batch_items(chunks)
    expected_ids = [item["id"] for item in items]
    instruction = build_description_instruction(language, len(items))
    user_content = build_description_user_content(items)

    started = time.monotonic()
    attempts = 0
    parse_failures = 0
    transport_failures = 0
    last_error = ""
    found: dict[int, dict] = {}

    while attempts < TRANSPORT_ATTEMPTS + PARSE_ATTEMPTS:
        attempts += 1
        try:
            text = call_model(
                client, url, args.api_key, args.model,
                instruction, user_content, args.temperature, args.max_tokens,
            )
        except (httpx.HTTPError, ValueError) as exc:
            transport_failures += 1
            last_error = transport_error(exc)
            if transport_failures >= TRANSPORT_ATTEMPTS:
                break
            time.sleep(2.0 * transport_failures)
            continue
        except DescriptionError as exc:
            parse_failures += 1
            last_error = f"empty: {exc}"
            if parse_failures >= PARSE_ATTEMPTS:
                break
            continue
        try:
            answered = parse_description_response(text, expected_ids)
        except DescriptionError as exc:
            parse_failures += 1
            last_error = f"parse: {exc}"
            if parse_failures >= PARSE_ATTEMPTS:
                break
            continue
        for item_id, entry in answered.items():
            found.setdefault(item_id, entry)
        missing = [i for i in expected_ids if i not in found]
        if not missing:
            last_error = ""
            break
        parse_failures += 1
        last_error = (
            f"incomplete: {len(missing)} of {len(expected_ids)} fragments "
            f"missing from the answer"
        )
        if parse_failures >= PARSE_ATTEMPTS:
            break

    latency_ms = int((time.monotonic() - started) * 1000)
    return build_records(chunks, items, found, latency_ms, attempts, last_error)


def cmd_generate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if not args.api_key:
        args.api_key = (
            os.environ.get("DESCRIPTION_BENCH_API_KEY")
            or os.environ.get("REWRITE_BENCH_API_KEY")
            or os.environ.get("OPENROUTER_API_KEY")
            or ""
        )
    corpus = load_corpus(resolve_path(args.chunks))
    if not corpus:
        parser.error(f"{args.chunks} holds no chunks")
    chunking_version = corpus[0].get("chunking_version")
    selected = select_chunks(
        corpus, args.only_translation, args.limit, parse_ids(args.ids))
    if not selected:
        parser.error("no fragments selected")

    out = resolve_path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    done: set[str] = set()
    if args.resume:
        done = load_existing(out)
        selected = [
            c for c in selected
            if f"{c['translation']}:{c['canonical_id']}" not in done
        ]
    else:
        out.write_text("", encoding="utf-8")
    if not selected:
        print(f"nothing to do: {len(done)} fragments already annotated in {out}")
        return 0

    url = completions_url(args.endpoint)
    batches = make_batches(selected, args.batch_size)
    stats = empty_stats()
    stats["annotated"] = len(done)
    print(
        f"{args.model} @ {endpoint_host(args.endpoint)} — description prompt "
        f"v{DESCRIPTION_PROMPT_VERSION}, {len(selected)} fragments in "
        f"{len(batches)} batches of {args.batch_size}"
        + (f" (+{len(done)} already annotated)" if done else ""),
        flush=True,
    )

    def snapshot() -> None:
        write_meta(out, build_meta(
            args, len(corpus), chunking_version, stats, "api",
            {"selection": {
                "only_translation": args.only_translation,
                "limit_per_translation": args.limit,
                "ids": bool(args.ids),
                "resume": bool(args.resume),
            }},
        ))

    # The file grows after EVERY batch and the sidecar is refreshed with it:
    # a full-corpus run is hours long, and a partial file that says it is
    # partial beats losing the lot (the lesson gen_rewrites.py learned on
    # 2026-09-04).
    try:
        with httpx.Client(timeout=httpx.Timeout(args.timeout)) as client:
            for number, batch in enumerate(batches, start=1):
                records = generate_batch(client, url, args, batch)
                append_records(out, records)
                ok, cautions = account(stats, batch, records)
                mark = "ok " if ok == len(records) else "ERR"
                note = "" if ok == len(records) else (
                    records[0]["error"] or "incomplete"
                )
                print(
                    f"  {mark} batch {number}/{len(batches)} "
                    f"[{batch[0].get('alias', '')}] {ok}/{len(records)} "
                    f"annotated, {cautions} caution "
                    f"{records[0]['latency_ms'] / 1000:6.1f}s "
                    f"attempts={records[0]['attempts']}"
                    + (f" — {note}" if note else ""),
                    flush=True,
                )
                snapshot()
    finally:
        snapshot()

    meta = build_meta(args, len(corpus), chunking_version, stats, "api")
    report_written(out, len(corpus), stats, meta["partial"])
    return 0


# ---------------------------------------------------------------------------
# Offline mode: prepare -> (an agent answers) -> ingest
# ---------------------------------------------------------------------------

def cmd_prepare(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Write the batches an agent is to answer, one JSON object per line.

    Each line carries the same `instruction` and `user_content` the API mode
    would have sent, so the two modes measure the same prompt. `items[].id`
    is the SMALL id the instruction numbers the fragments with (that is what
    the answer must carry back); `items[].canonical_id` travels beside it so
    a line can be traced to its fragment by eye.
    """
    corpus = load_corpus(resolve_path(args.chunks))
    if not corpus:
        parser.error(f"{args.chunks} holds no chunks")
    selected = select_chunks(
        corpus, args.only_translation, args.limit, parse_ids(args.ids))
    if not selected:
        parser.error("no fragments selected")

    out = resolve_path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    batches = make_batches(selected, args.batch_size)
    lines = []
    for number, chunks in enumerate(batches, start=1):
        items = batch_items(chunks)
        language = batch_language(chunks)
        lines.append(json.dumps({
            "batch_id": f"b{number:05d}",
            "prompt_version": DESCRIPTION_PROMPT_VERSION,
            "language": language,
            "translation": chunks[0]["translation"],
            "alias": chunks[0].get("alias", ""),
            "instruction": build_description_instruction(language, len(items)),
            "user_content": build_description_user_content(items),
            "expected_ids": [item["id"] for item in items],
            "items": [
                {
                    "id": item["id"],
                    "canonical_id": chunk["canonical_id"],
                    "translation": chunk["translation"],
                    "alias": chunk.get("alias", ""),
                    "language": chunk["language"],
                    "title": item["title"],
                    "text": item["text"],
                }
                for item, chunk in zip(items, chunks)
            ],
        }, ensure_ascii=False))
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        f"wrote {out} — {len(batches)} batches of up to {args.batch_size}, "
        f"{len(selected)} fragments, description prompt "
        f"v{DESCRIPTION_PROMPT_VERSION}\n"
        f"Answer each batch as one line "
        f'{{"batch_id": ..., "response": {{"descriptions": [...]}}}} and feed '
        f"the file to `ingest`."
    )
    return 0


def _answer_object(raw: object) -> str:
    """The answer of one batch as text the shared parser can read.

    Accepted shapes: the JSON object itself (an agent that wrote structured
    output), or a string holding it (an agent that pasted a model reply,
    reasoning block and all).
    """
    if isinstance(raw, str):
        return _THINK_BLOCK.sub("", raw)
    return json.dumps(raw, ensure_ascii=False)


def cmd_ingest(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    corpus = load_corpus(resolve_path(args.chunks))
    if not corpus:
        parser.error(f"{args.chunks} holds no chunks")
    chunking_version = corpus[0].get("chunking_version")
    by_key = {(c["translation"], c["canonical_id"]): c for c in corpus}

    batches = load_jsonl(resolve_path(args.batches))
    if not batches:
        parser.error(f"{args.batches} holds no batches")
    answers: dict[str, object] = {}
    answers_path = resolve_path(args.answers)
    if not answers_path.exists():
        parser.error(f"{args.answers} does not exist")
    for row in load_jsonl(answers_path):
        batch_id = row.get("batch_id")
        if batch_id is None:
            parser.error(f"{args.answers}: a row has no batch_id")
        # The LAST answer for a batch wins: that is how a re-answered batch
        # is delivered without editing the file in place.
        if "response" in row:
            answers[str(batch_id)] = row["response"]
        elif "descriptions" in row:
            # Tolerated shorthand: the annotation list written straight into
            # the answer row. Wrapped here, so the shared parser sees the
            # object it expects — a bare list is not valid input for it.
            answers[str(batch_id)] = {"descriptions": row["descriptions"]}
        else:
            parser.error(
                f"{args.answers}: batch {batch_id} has neither 'response' nor "
                f"'descriptions'"
            )

    stale = [
        row["prompt_version"] for row in batches
        if row.get("prompt_version") not in (None, DESCRIPTION_PROMPT_VERSION)
    ]
    if stale:
        print(
            f"  [warn] batches were prepared with description prompt "
            f"v{stale[0]}, this checkout is v{DESCRIPTION_PROMPT_VERSION} — "
            f"the artifact will not be comparable with runs of the current "
            f"prompt",
            flush=True,
        )

    out = resolve_path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    done: set[str] = set()
    if args.resume:
        done = load_existing(out)
    else:
        out.write_text("", encoding="utf-8")

    stats = empty_stats()
    stats["annotated"] = len(done)
    missing_batches: list[str] = []
    broken_batches: list[str] = []
    incomplete_batches: list[str] = []
    for batch in batches:
        batch_id = str(batch["batch_id"])
        chunks = []
        for item in batch["items"]:
            key = (item["translation"], item["canonical_id"])
            chunk = by_key.get(key)
            if chunk is None:
                parser.error(
                    f"{args.batches}: batch {batch_id} names a fragment that "
                    f"is not in {args.chunks}: {key}"
                )
            chunks.append(chunk)
        items = batch_items(chunks)
        if args.resume and all(
            f"{c['translation']}:{c['canonical_id']}" in done for c in chunks
        ):
            continue
        expected_ids = batch.get("expected_ids") or [i["id"] for i in items]
        raw = answers.get(batch_id)
        found: dict[int, dict] = {}
        last_error = ""
        if raw is None:
            missing_batches.append(batch_id)
            last_error = "no answer for this batch"
        else:
            try:
                found = parse_description_response(
                    _answer_object(raw), expected_ids)
            except DescriptionError as exc:
                broken_batches.append(batch_id)
                last_error = f"parse: {exc}"
            else:
                gap = [i for i in expected_ids if i not in found]
                if gap:
                    incomplete_batches.append(batch_id)
                    last_error = (
                        f"incomplete: {len(gap)} of {len(expected_ids)} "
                        f"fragments missing from the answer"
                    )
        records = build_records(chunks, items, found, 0, 0, last_error)
        append_records(out, records)
        account(stats, chunks, records)

    extra = {
        "batches_file": args.batches,
        "answers_file": args.answers,
        "batches_total": len(batches),
        "batches_missing": missing_batches,
        "batches_broken": broken_batches,
        "batches_incomplete": incomplete_batches,
    }
    meta = build_meta(
        args, len(corpus), chunking_version, stats, "offline", extra)
    write_meta(out, meta)
    todo = missing_batches + broken_batches + incomplete_batches
    print(
        f"ingested {len(batches)} batches: {stats['annotated'] - len(done)} "
        f"fragments annotated, {stats['errors']} failed "
        f"({len(missing_batches)} batches unanswered, "
        f"{len(broken_batches)} unparseable, "
        f"{len(incomplete_batches)} incomplete)"
    )
    if todo:
        print(
            "  fill these batch_ids in and run `ingest --resume` again:\n"
            "  " + ",".join(todo[:200])
            + (" …" if len(todo) > 200 else "")
        )
    report_written(out, len(corpus), stats, meta["partial"])
    return 0


# ---------------------------------------------------------------------------

def _add_selection_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--chunks", default=str(CHUNKS_FILE),
        help="chunk corpus (JSONL). A relative path is resolved against the "
             "evaluation/ directory, like --out.",
    )
    p.add_argument(
        "--only-translation", default="",
        help="comma-separated translation codes or aliases (e.g. `bsb` or "
             "`1,16`) — annotate only those",
    )
    p.add_argument(
        "--limit", type=int, default=0,
        help="at most N fragments PER TRANSLATION (0 = no limit). Per "
             "translation and not per run, so a probe covers every language",
    )
    p.add_argument(
        "--ids", default="",
        help="comma-separated canonical ids, or a file with one per line — "
             "annotate exactly those fragments (a hand-picked hard set). "
             "Overrides --limit",
    )
    p.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"fragments per call (default {DEFAULT_BATCH_SIZE}). Larger is "
             f"cheaper per fragment but loses more work on a broken answer, "
             f"and the prompt must still fit the model's context window — on "
             f"Ollama's default num_ctx 4096 even this default overflows",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Annotate the chunk corpus with the situations each fragment can "
            "serve: `generate` through an OpenAI-compatible endpoint, or "
            "`prepare`/`ingest` for an agent answering the batches offline."
        )
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("generate", help="annotate through an API endpoint")
    p.add_argument(
        "--endpoint", required=True,
        help="base URL of the OpenAI-compatible API, e.g. "
             "http://localhost:11434/v1 (…/chat/completions is appended)",
    )
    p.add_argument("--model", required=True, help="model id at that endpoint")
    p.add_argument(
        "--api-key", default="",
        help="bearer token; falls back to $DESCRIPTION_BENCH_API_KEY, then "
             "$REWRITE_BENCH_API_KEY, then $OPENROUTER_API_KEY. Local "
             "endpoints need none.",
    )
    p.add_argument(
        "--out", required=True,
        help="artifact path (JSONL, one line per fragment) for `pipeline "
             "--descriptions-file`. A relative path is resolved against the "
             "evaluation/ directory. The metadata goes to <out>.meta.json",
    )
    _add_selection_args(p)
    p.add_argument("--temperature", type=float, default=0.0)
    p.add_argument(
        "--max-tokens", type=int, default=DEFAULT_MAX_TOKENS,
        help="output ceiling per call (a batch of eight annotations is a few "
             "hundred tokens; the room is for models that think out loud)",
    )
    p.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    p.add_argument(
        "--resume", action="store_true",
        help="append to an existing --out, skipping the fragments it already "
             "annotates. Without it the file is REWRITTEN from scratch. Rows "
             "that carry an error are retried either way",
    )
    p.set_defaults(func=cmd_generate)

    p = sub.add_parser(
        "prepare",
        help="write the batches for an agent to answer offline (no API)",
    )
    p.add_argument(
        "--out", required=True,
        help="batches path (JSONL, one line per batch). A relative path is "
             "resolved against the evaluation/ directory",
    )
    _add_selection_args(p)
    p.set_defaults(func=cmd_prepare)

    p = sub.add_parser(
        "ingest",
        help="turn answered batches into the same artifact `generate` writes",
    )
    p.add_argument("--batches", required=True,
                   help="the `prepare` file those answers belong to")
    p.add_argument("--answers", required=True,
                   help='JSONL, one line per batch: {"batch_id": ..., '
                        '"response": {"descriptions": [...]}}. The response '
                        "may also be the raw model text holding that object")
    p.add_argument("--out", required=True,
                   help="artifact path (JSONL) for `pipeline "
                        "--descriptions-file`")
    p.add_argument("--model", required=True,
                   help="name of the model that answered — recorded in meta, "
                        "since nothing in an offline answer says who wrote it")
    p.add_argument(
        "--chunks", default=str(CHUNKS_FILE),
        help="chunk corpus (JSONL) the batches were prepared from",
    )
    p.add_argument(
        "--resume", action="store_true",
        help="append to an existing --out, skipping the batches whose "
             "fragments it already annotates — how a re-answered batch is "
             "added without redoing the rest",
    )
    p.set_defaults(func=cmd_ingest)

    args = parser.parse_args(argv)
    if getattr(args, "batch_size", 1) < 1:
        parser.error("--batch-size must be >= 1")
    if getattr(args, "max_tokens", 1) < 1:
        parser.error("--max-tokens must be >= 1")
    if getattr(args, "limit", 0) < 0:
        parser.error("--limit must be >= 0")
    return args.func(args, parser)


if __name__ == "__main__":
    raise SystemExit(main())
