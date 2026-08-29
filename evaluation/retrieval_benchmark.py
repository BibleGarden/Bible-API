"""
Retrieval mini-benchmark for the scripture-selection RAG (ClickUp 86cb8vw14).

Compares embedding models (local sentence-transformers and the Gemini
embedding API) on the draft evaluation set (evaluation/scenarios.json) against
the retrieval thresholds (evaluation/thresholds.json).

Not part of the production service: it runs on the host in a dedicated venv
(sentence-transformers + torch-cpu), because the bible-api container stays
slim. Corpus data is exported from MySQL to bench_data/chunks.jsonl first
(see evaluation/README-embeddings section "benchmark").

Usage:
    python retrieval_benchmark.py embed  --model e5-small --variant title_text
    python retrieval_benchmark.py run    --model e5-small --variant title_text
    python retrieval_benchmark.py run-all           # every cached config
    python retrieval_benchmark.py stores --model X --variant Y   # numpy/qdrant/chroma parity+latency
    python retrieval_benchmark.py pipeline [...]    # full retrieval pipeline
                                                    # (rewrite+fusion+blacklist+pool),
                                                    # ablations via flags — see -h

Scenario query = prayer topic + allowed user replies, embedded with the
model's query mode; the corpus is the language's translation chunks
(ru -> syn, en -> bsb, uk -> ubh) embedded in document mode.

Psalm coordinates: scenario references use the canonical english-masoretic
numbering (see coordinate_system in scenarios.json); they are converted to
each translation's own numbering before matching with the project's
versification module (app/versification.py, ADR 0003) built from the verse
counts in bench_data/psalm_verse_counts.tsv — the same rules that define the
canonical chunk IDs and the cep_public.psalm_verse_mappings table.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "app"))

from versification import (  # noqa: E402
    PsalmMap,
    build_psalm_map,
    canonical_counts_with_extras,
)
DATA = HERE / "bench_data"
CHUNKS_FILE = DATA / "chunks.jsonl"
PSALM_COUNTS_FILE = DATA / "psalm_verse_counts.tsv"
SCENARIOS_FILE = HERE / "scenarios.json"
THRESHOLDS_FILE = HERE / "thresholds.json"

TOP_K = 10
PSALMS_BOOK = 19

# language -> (translation code, alias) used as the retrieval corpus
LANGUAGE_CORPUS = {"ru": (1, "syn"), "en": (16, "bsb"), "uk": (20, "ubh")}

# ---------------------------------------------------------------------------
# Model registry
# ---------------------------------------------------------------------------

MODELS = {
    # key: (kind, model id, query prefix, passage prefix, dims)
    "minilm": ("local", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", "", "", 384),
    "e5-small": ("local", "intfloat/multilingual-e5-small", "query: ", "passage: ", 384),
    "e5-base": ("local", "intfloat/multilingual-e5-base", "query: ", "passage: ", 768),
    "labse": ("local", "sentence-transformers/LaBSE", "", "", 768),
    # Gemini is embedded once at 3072 dims; gemini-768 is evaluated by MRL
    # truncation to the first 768 dims + re-normalisation (what the API's
    # outputDimensionality=768 does server-side).
    "gemini": ("gemini", "gemini-embedding-001", "", "", 3072),
}
VARIANTS = ("text", "title_text")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@dataclass
class ChunkMeta:
    canonical_id: str
    translation: int
    book: int
    chapter: int
    vstart: int
    vend: int


def load_chunks() -> tuple[list[ChunkMeta], list[str], list[str]]:
    """Return metadata, plain texts and title+texts, in stable file order."""
    metas, texts, title_texts = [], [], []
    with CHUNKS_FILE.open() as fh:
        for line in fh:
            row = json.loads(line)
            metas.append(
                ChunkMeta(
                    canonical_id=row["canonical_id"],
                    translation=row["translation"],
                    book=row["book_number"],
                    chapter=row["chapter_number"],
                    vstart=row["verse_number_start"],
                    vend=row["verse_number_end"],
                )
            )
            texts.append(row["text"])
            title = (row.get("title") or "").strip()
            title_texts.append(f"{title}\n\n{row['text']}" if title else row["text"])
    return metas, texts, title_texts


def load_psalm_maps() -> dict[int, PsalmMap]:
    """Per-translation PsalmMap built from the exported verse counts."""
    counts: dict[int, dict[int, int]] = {}
    for line in PSALM_COUNTS_FILE.read_text().splitlines():
        translation, chapter, max_verse = line.split("\t")
        counts.setdefault(int(translation), {})[int(chapter)] = int(max_verse)
    canonical = canonical_counts_with_extras(counts[16])  # bsb defines the canon
    aliases = {code: alias for code, alias in LANGUAGE_CORPUS.values()}
    return {
        code: PsalmMap(build_psalm_map(aliases[code], counts[code], canonical))
        for code in counts
        if code in aliases
    }


# ---------------------------------------------------------------------------
# Psalm mapping layer (canonical english-masoretic -> translation coords)
# ---------------------------------------------------------------------------

def map_reference(
    ref: dict, translation: int, psalm_maps: dict[int, PsalmMap]
) -> list[tuple[int, int, int, int]]:
    """Map one canonical reference to (book, chapter, vstart, vend) targets
    in the given translation's own numbering."""
    book, m = ref["book_number"], ref["chapter"]
    vs, ve = ref["verse_start"], ref["verse_end"]
    if book != PSALMS_BOOK:
        return [(book, m, vs, ve)]

    per_chapter: dict[int, list[int]] = {}
    psalm_map = psalm_maps[translation]
    for verse in range(vs, ve + 1):
        located = psalm_map.from_canonical(m, verse)
        if located is None:  # verse absent from this translation
            continue
        chapter, own_verse = located
        per_chapter.setdefault(chapter, []).append(own_verse)
    return [
        (book, chapter, min(verses), max(verses))
        for chapter, verses in sorted(per_chapter.items())
    ]


def chunk_matches(chunk: ChunkMeta, targets: list[tuple[int, int, int, int]]) -> bool:
    for book, chapter, vs, ve in targets:
        if chunk.book == book and chunk.chapter == chapter \
                and chunk.vend >= vs and chunk.vstart <= ve:
            return True
    return False


# ---------------------------------------------------------------------------
# Embedding backends
# ---------------------------------------------------------------------------

def emb_path(model_key: str, variant: str) -> Path:
    return DATA / f"emb_{model_key}_{variant}.npy"


def embed_local(model_id: str, texts: list[str], prefix: str) -> np.ndarray:
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_id, device="cpu")
    vecs = model.encode(
        [prefix + t for t in texts],
        batch_size=16,
        show_progress_bar=True,
        normalize_embeddings=True,
        convert_to_numpy=True,
    )
    return vecs.astype(np.float32)


def _gemini_retry_delay(resp) -> float:
    """Extract the server-suggested retry delay from a 429 body, if any."""
    try:
        for detail in resp.json()["error"]["details"]:
            if detail.get("@type", "").endswith("RetryInfo"):
                raw = detail.get("retryDelay", "")
                return float(raw.rstrip("s") or 0) or 30.0
    except Exception:
        pass
    return 30.0


def embed_gemini(
    texts: list[str], task_type: str, api_key: str,
    checkpoint_dir: Path | None = None,
) -> np.ndarray:
    """Embed with gemini-embedding-001 at full 3072 dims.

    Uses SINGLE embedContent calls, client-side paced just under the
    free-tier quota (~100 RPM / 30k TPM): on the free tier the
    batchEmbedContents endpoint crawls (~12 chunks/min observed), a thread
    pool of unpaced single calls drowns in 429s with long server-suggested
    delays, while a paced sequential loop sustains its rate cleanly.
    On a 429 the pace temporarily slows. With a checkpoint_dir every
    finished shard of 200 texts is persisted, so an interrupted run resumes
    without re-embedding.
    """
    import requests

    url = ("https://generativelanguage.googleapis.com/v1beta/models/"
           "gemini-embedding-001:embedContent")
    session = requests.Session()
    # AIMD pacing: the effective cap is tokens-per-minute, which depends on
    # chunk length and language (Cyrillic tokenises heavily), so the interval
    # self-tunes: multiplicative increase on 429, slow additive decrease on
    # success streaks.
    state = {"interval": 0.8, "streak": 0}

    def embed_one(text: str) -> list[float]:
        body = {
            "content": {"parts": [{"text": text if text.strip() else " "}]},
            "taskType": task_type,
        }
        while True:
            time.sleep(state["interval"])
            try:
                resp = session.post(
                    url, json=body, timeout=60,
                    headers={"x-goog-api-key": api_key},
                )
            except requests.RequestException:
                time.sleep(10)
                continue
            if resp.status_code == 200:
                state["streak"] += 1
                if state["streak"] >= 30:
                    state["streak"] = 0
                    state["interval"] = max(state["interval"] - 0.05, 0.3)
                return resp.json()["embedding"]["values"]
            if resp.status_code == 429:
                state["streak"] = 0
                state["interval"] = min(state["interval"] * 1.5, 10.0)
                time.sleep(5)
                continue
            if resp.status_code in (500, 502, 503, 504):
                time.sleep(10)
                continue
            raise RuntimeError(
                f"gemini embedding failed ({resp.status_code}): {resp.text[:300]}")

    shard_size = 200
    if checkpoint_dir:
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
    shards: list[np.ndarray] = []
    started = time.time()
    fresh = 0
    for start in range(0, len(texts), shard_size):
        shard_file = (
            checkpoint_dir / f"single_{start:06d}.npy" if checkpoint_dir else None
        )
        if shard_file is not None and shard_file.exists():
            shards.append(np.load(shard_file))
            continue
        batch = texts[start:start + shard_size]
        shard = np.asarray([embed_one(t) for t in batch], dtype=np.float32)
        if shard_file is not None:
            np.save(shard_file, shard)
        shards.append(shard)
        fresh += len(batch)
        rate = fresh / max(time.time() - started, 1e-9) * 60
        print(f"  {start + len(batch)}/{len(texts)} ({rate:.0f} chunks/min)",
              flush=True)
    vecs = np.vstack(shards)
    return vecs / np.linalg.norm(vecs, axis=1, keepdims=True)


def truncate_mrl(vecs: np.ndarray, dims: int) -> np.ndarray:
    cut = vecs[:, :dims]
    return cut / np.linalg.norm(cut, axis=1, keepdims=True)


def cmd_embed(args) -> None:
    kind, model_id, _qpref, ppref, _dims = MODELS[args.model]
    metas, texts, title_texts = load_chunks()
    corpus = title_texts if args.variant == "title_text" else texts
    path = emb_path(args.model, args.variant)
    if path.exists() and not args.force:
        print(f"{path} exists, skip (use --force)")
        return
    started = time.time()
    if kind == "local":
        vecs = embed_local(model_id, corpus, ppref)
    else:
        vecs = embed_gemini(
            corpus, "RETRIEVAL_DOCUMENT", require_api_key(),
            checkpoint_dir=DATA / f"gemini_ckpt_{args.variant}",
        )
    np.save(path, vecs)
    print(f"saved {path} shape={vecs.shape} in {time.time() - started:.0f}s")


ENV_FILE = HERE.parent / ".env"


def _key_from_env(name: str, env_file: Path | None = None) -> str:
    """Value of `name` from the environment, else from Bible-API/.env.

    The `.env` file is optional and must never turn into a crash: it is not
    part of the container image, so a run inside `bible-api` (where the
    variables come from compose) would otherwise die with FileNotFoundError
    — after minutes of corpus loading, and even in the case where the
    environment can answer the question perfectly well. Returns "" when
    neither source has the name.
    """
    key = os.environ.get(name, "").strip()
    if key:
        return key
    path = ENV_FILE if env_file is None else env_file
    if not path.exists():
        return ""
    for line in path.read_text().splitlines():
        if line.startswith(f"{name}="):
            key = line.split("=", 1)[1].strip()
    return key


def require_api_key() -> str:
    key = _key_from_env("GEMINI_API_KEY")
    if not key:
        raise SystemExit("GEMINI_API_KEY not found (env or Bible-API/.env)")
    return key


def require_rewrite_api_key() -> str:
    """Key for the rewrite stage — the same split as production.

    Mirrors `config.resolve_rewrite_api_key`: a benchmark run must bill (and
    hit the quota of) the same key the serving path uses for rewrites, or its
    numbers describe a configuration nobody runs.
    """
    return _key_from_env("RETRIEVAL_REWRITE_API_KEY") or require_api_key()


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

@dataclass
class ScenarioResult:
    scenario_id: str
    language: str
    category: str
    hit: bool
    recall: float
    rr: float
    unacceptable_share: float
    query_ms: float


@dataclass
class Aggregate:
    label: str
    n: int
    hit_rate: float
    recall: float
    mrr: float
    unacceptable_share: float


def build_query(scenario: dict) -> str:
    ctx = scenario["prayer_context"]
    parts = [ctx["topic"].strip()] + [r.strip() for r in ctx["user_replies"]]
    return "\n".join(p for p in parts if p)


def evaluate_config(
    model_key: str,
    variant: str,
    dims_override: int | None = None,
) -> tuple[list[ScenarioResult], dict]:
    kind, model_id, qpref, _ppref, dims = MODELS[model_key]
    metas, _texts, _tt = load_chunks()
    psalm_maps = load_psalm_maps()
    scenarios = json.loads(SCENARIOS_FILE.read_text())["scenarios"]

    corpus = np.load(emb_path(model_key, variant))
    if dims_override:
        corpus = truncate_mrl(corpus, dims_override)

    # per-translation views
    trans_idx = {
        code: np.array([i for i, m in enumerate(metas) if m.translation == code])
        for code, _alias in LANGUAGE_CORPUS.values()
    }

    queries = [build_query(s) for s in scenarios]
    t0 = time.time()
    if kind == "local":
        qvecs = embed_local(model_id, queries, qpref)
    else:
        qvecs = embed_gemini(queries, "RETRIEVAL_QUERY", require_api_key())
    query_embed_ms = (time.time() - t0) * 1000 / len(queries)
    if dims_override:
        qvecs = truncate_mrl(qvecs, dims_override)

    results = []
    for scenario, qvec in zip(scenarios, qvecs):
        code, _alias = LANGUAGE_CORPUS[scenario["language"]]
        idx = trans_idx[code]
        t0 = time.time()
        sims = corpus[idx] @ qvec
        top_local = np.argsort(-sims)[:TOP_K]
        top = idx[top_local]
        search_ms = (time.time() - t0) * 1000

        relevant = [r for r in scenario["references"] if r["grade"] == "relevant"]
        unacceptable = [r for r in scenario["references"] if r["grade"] == "unacceptable"]
        rel_targets = [map_reference(r, code, psalm_maps) for r in relevant]
        una_targets = [map_reference(r, code, psalm_maps) for r in unacceptable]

        top_metas = [metas[i] for i in top]
        hit_rank = 0
        for rank, cm in enumerate(top_metas, start=1):
            if any(chunk_matches(cm, t) for t in rel_targets):
                hit_rank = rank
                break
        matched_refs = sum(
            1 for t in rel_targets if any(chunk_matches(cm, t) for cm in top_metas)
        )
        una_hits = sum(
            1 for cm in top_metas if any(chunk_matches(cm, t) for t in una_targets)
        )
        results.append(
            ScenarioResult(
                scenario_id=scenario["id"],
                language=scenario["language"],
                category=scenario["category"],
                hit=hit_rank > 0,
                recall=matched_refs / len(relevant) if relevant else 1.0,
                rr=1.0 / hit_rank if hit_rank else 0.0,
                unacceptable_share=una_hits / TOP_K,
                query_ms=search_ms,
            )
        )
    info = {"query_embed_ms_avg": round(query_embed_ms, 1)}
    return results, info


def aggregate(results: list[ScenarioResult], label: str) -> Aggregate:
    n = len(results)
    return Aggregate(
        label=label,
        n=n,
        hit_rate=sum(r.hit for r in results) / n,
        recall=sum(r.recall for r in results) / n,
        mrr=sum(r.rr for r in results) / n,
        unacceptable_share=sum(r.unacceptable_share for r in results) / n,
    )


def print_report(config: str, results: list[ScenarioResult], info: dict, thresholds: dict) -> None:
    t = thresholds["retrieval_top_k"]
    groups = [
        ("ALL", results),
        ("no-empty", [r for r in results if r.category != "empty"]),
        ("ru", [r for r in results if r.language == "ru"]),
        ("en", [r for r in results if r.language == "en"]),
        ("uk", [r for r in results if r.language == "uk"]),
    ]
    print(f"\n=== {config} (query embed avg {info.get('query_embed_ms_avg', '?')} ms) ===")
    print(f"{'group':<10}{'n':>4}{'hit@10':>9}{'recall@10':>11}{'MRR':>8}{'unacc@10':>10}")
    for label, subset in groups:
        if not subset:
            continue
        a = aggregate(subset, label)
        print(f"{a.label:<10}{a.n:>4}{a.hit_rate:>9.3f}{a.recall:>11.3f}"
              f"{a.mrr:>8.3f}{a.unacceptable_share:>10.3f}")
    a = aggregate(results, "ALL")
    checks = [
        ("hit_rate@10", a.hit_rate, ">=", t["hit_rate_at_k_min"]),
        ("recall@10", a.recall, ">=", t["recall_at_k_min"]),
        ("MRR", a.mrr, ">=", t["mrr_min"]),
        ("unacceptable@10", a.unacceptable_share, "<=", t["unacceptable_share_in_top_k_max"]),
    ]
    for name, value, op, limit in checks:
        ok = value >= limit if op == ">=" else value <= limit
        print(f"  threshold {name}: {value:.3f} {op} {limit} -> {'PASS' if ok else 'FAIL'}")
    misses = [r.scenario_id for r in results if not r.hit]
    if misses:
        print(f"  scenarios without relevant in top-10: {', '.join(misses)}")


def cmd_run(args) -> None:
    thresholds = json.loads(THRESHOLDS_FILE.read_text())
    dims = args.dims if args.dims else None
    label = f"{args.model}/{args.variant}" + (f"/dims={dims}" if dims else "")
    results, info = evaluate_config(args.model, args.variant, dims_override=dims)
    print_report(label, results, info, thresholds)
    if args.json_out:
        payload = {
            "config": label,
            "aggregate": aggregate(results, "ALL").__dict__,
            "info": info,
            "scenarios": [r.__dict__ for r in results],
        }
        Path(args.json_out).write_text(json.dumps(payload, ensure_ascii=False, indent=1))


def cmd_run_all(args) -> None:
    thresholds = json.loads(THRESHOLDS_FILE.read_text())
    for model_key in MODELS:
        for variant in VARIANTS:
            if not emb_path(model_key, variant).exists():
                continue
            configs = [(None, "")]
            if model_key == "gemini":
                configs = [(None, ""), (768, "/dims=768")]
            for dims, suffix in configs:
                results, info = evaluate_config(model_key, variant, dims_override=dims)
                print_report(f"{model_key}/{variant}{suffix}", results, info, thresholds)


# ---------------------------------------------------------------------------
# Vector store parity / latency (numpy vs qdrant vs chroma)
# ---------------------------------------------------------------------------

def cmd_stores(args) -> None:
    """Load one cached embedding config into qdrant + chroma, verify that the
    top-10 matches brute-force cosine, and measure query latency."""
    metas, _texts, _tt = load_chunks()
    corpus = np.load(emb_path(args.model, args.variant))
    if args.dims:
        corpus = truncate_mrl(corpus, args.dims)
    n, dims = corpus.shape
    rng = np.random.default_rng(7)
    sample = rng.choice(n, size=100, replace=False)
    translations = np.array([m.translation for m in metas])

    def brute(qv, code, k):
        idx = np.nonzero(translations == code)[0]
        sims = corpus[idx] @ qv
        return idx[np.argsort(-sims)[:k]]

    # --- numpy latency
    lat = []
    for i in sample:
        t0 = time.perf_counter()
        brute(corpus[i], translations[i], TOP_K)
        lat.append((time.perf_counter() - t0) * 1000)
    report = {"numpy": (statistics.median(lat), max(lat))}

    # --- qdrant
    try:
        from qdrant_client import QdrantClient, models as qm

        client = QdrantClient(url=args.qdrant_url, timeout=30)
        coll = f"bench_{args.model}_{args.variant}_{dims}"
        client.delete_collection(coll)
        client.create_collection(
            coll, vectors_config=qm.VectorParams(size=dims, distance=qm.Distance.COSINE)
        )
        batch = 256
        for start in range(0, n, batch):
            end = min(start + batch, n)
            client.upsert(
                coll,
                points=qm.Batch(
                    ids=list(range(start, end)),
                    vectors=corpus[start:end].tolist(),
                    payloads=[{"translation": int(translations[i])} for i in range(start, end)],
                ),
            )
        mismatches, lat = 0, []
        for i in sample:
            flt = qm.Filter(must=[qm.FieldCondition(
                key="translation", match=qm.MatchValue(value=int(translations[i])))])
            t0 = time.perf_counter()
            res = client.query_points(coll, query=corpus[i].tolist(),
                                      query_filter=flt, limit=TOP_K)
            lat.append((time.perf_counter() - t0) * 1000)
            got = [p.id for p in res.points]
            if set(got) != set(brute(corpus[i], translations[i], TOP_K).tolist()):
                mismatches += 1
        report["qdrant"] = (statistics.median(lat), max(lat))
        print(f"qdrant top-10 set mismatches vs brute force: {mismatches}/100")
    except Exception as exc:  # pragma: no cover - depends on local qdrant
        print(f"qdrant skipped: {exc}")

    # --- chroma (embedded, persistent)
    try:
        import chromadb

        cclient = chromadb.PersistentClient(path=str(DATA / "chroma"))
        cname = f"bench_{args.model.replace('/', '_')}_{args.variant}_{dims}"
        try:
            cclient.delete_collection(cname)
        except Exception:
            pass
        ccoll = cclient.create_collection(cname, metadata={"hnsw:space": "cosine"})
        batch = 2000
        for start in range(0, n, batch):
            end = min(start + batch, n)
            ccoll.add(
                ids=[str(i) for i in range(start, end)],
                embeddings=corpus[start:end].tolist(),
                metadatas=[{"translation": int(translations[i])} for i in range(start, end)],
            )
        mismatches, lat = 0, []
        for i in sample:
            t0 = time.perf_counter()
            res = ccoll.query(
                query_embeddings=[corpus[i].tolist()],
                n_results=TOP_K,
                where={"translation": int(translations[i])},
            )
            lat.append((time.perf_counter() - t0) * 1000)
            got = [int(x) for x in res["ids"][0]]
            if set(got) != set(brute(corpus[i], translations[i], TOP_K).tolist()):
                mismatches += 1
        report["chroma"] = (statistics.median(lat), max(lat))
        print(f"chroma top-10 set mismatches vs brute force: {mismatches}/100")
    except Exception as exc:  # pragma: no cover
        print(f"chroma skipped: {exc}")

    print(f"\nquery latency over 100 sampled vectors, corpus n={n} dims={dims}:")
    for store, (p50, worst) in report.items():
        print(f"  {store:<8} p50={p50:.2f} ms  max={worst:.2f} ms")


# ---------------------------------------------------------------------------
# Full retrieval pipeline (ClickUp 86cb8vw1g): LLM query reformulation +
# multi-variant fusion + genre blacklist + diversity + safe pool.
#
# Reuses the production modules from app/ (query_rewrite, retrieval) over the
# cached gemini corpus embeddings (truncated to 768 dims — MRL-identical to
# the production index c3:gemini-embedding-001@768). Gemini calls (rewrites,
# query embeddings) are cached on disk, so re-runs with different ablation
# flags cost nothing.
# ---------------------------------------------------------------------------

os.environ.setdefault("API_KEY", "benchmark")  # app/config.py requires it;
# DB_*/EMBEDDING_* are not defaulted here — they come from the container's
# real .env, which the benchmark relies on for its DB connection and index
# version.

PIPELINE_CACHE_FILE = DATA / "pipeline_cache.json"
PIPELINE_DIMS = 768
FETCH_K_DEFAULT = 50


def _load_pipeline_cache() -> dict:
    cache = (
        json.loads(PIPELINE_CACHE_FILE.read_text())
        if PIPELINE_CACHE_FILE.exists() else {}
    )
    cache.setdefault("rewrites", {})
    cache.setdefault("query_embeddings", {})
    cache.setdefault("reranks", {})
    return cache


def _save_pipeline_cache(cache: dict) -> None:
    PIPELINE_CACHE_FILE.write_text(json.dumps(cache))


def _dotenv_value(name: str, default: str = "") -> str:
    value = os.environ.get(name, "")
    if value:
        return value
    env = Path(__file__).resolve().parents[1] / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.startswith(f"{name}="):
                return line.split("=", 1)[1].strip()
    return default


def _query_vector(text: str, api_key: str, cache: dict) -> np.ndarray:
    """Embed one query at 768 dims (RETRIEVAL_QUERY), disk-cached."""
    import hashlib

    key = hashlib.sha1(f"q768:{text}".encode()).hexdigest()
    hit = cache["query_embeddings"].get(key)
    if hit is not None:
        return np.asarray(hit, dtype=np.float32)
    import requests

    url = ("https://generativelanguage.googleapis.com/v1beta/models/"
           "gemini-embedding-001:embedContent")
    body = {
        "content": {"parts": [{"text": text if text.strip() else " "}]},
        "taskType": "RETRIEVAL_QUERY",
        "outputDimensionality": PIPELINE_DIMS,
    }
    for attempt in range(6):
        resp = requests.post(url, json=body, timeout=60,
                             headers={"x-goog-api-key": api_key})
        if resp.status_code == 200:
            values = np.asarray(resp.json()["embedding"]["values"],
                                dtype=np.float32)
            values = values / np.linalg.norm(values)
            cache["query_embeddings"][key] = [round(float(x), 7) for x in values]
            return values
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(2 ** attempt * 2.0, 30.0))
            continue
        raise RuntimeError(f"query embedding failed ({resp.status_code}): "
                           f"{resp.text[:200]}")
    raise RuntimeError("query embedding failed: retries exhausted")


def _scenario_rewrites(
    scenario: dict, rewriter, model: str, variants: int, cache: dict,
    tag: str = "",
) -> list[str]:
    from query_rewrite import REWRITE_PROMPT_VERSION

    key = f"{model}|p{REWRITE_PROMPT_VERSION}|n{variants}|{scenario['id']}"
    if tag:
        key += f"|{tag}"
    hit = cache["rewrites"].get(key)
    if hit is not None:
        return list(hit)
    ctx = scenario["prayer_context"]
    queries = rewriter.rewrite(scenario["language"], ctx["topic"],
                               ctx["user_replies"])
    cache["rewrites"][key] = queries
    return queries


def _load_external_rewrites(path: str) -> tuple[dict[str, list[str]], dict]:
    """Rewrite variants produced OUTSIDE this benchmark (external model).

    File format (see bench_data/qwen_rewrites_v070.json):
        {"meta": {"model": ..., "rewrite_prompt_version": 7, ...},
         "scenarios": [{"id": ..., "variants": [...], "error": null}, ...]}

    A scenario whose entry is missing, errored, or carries an empty variant
    list is treated exactly like a live rewrite failure (the caller degrades
    to the raw query, as production does on `rewrite_failed`). Using this
    option makes the rewrite stage fully offline: no rewrite provider is
    constructed and no rewrite call is issued.
    """
    payload = json.loads(Path(path).read_text())
    meta = payload.get("meta", {})
    by_id: dict[str, list[str]] = {}
    for row in payload.get("scenarios", []):
        variants = [q for q in (row.get("variants") or []) if q and q.strip()]
        if row.get("error") or not variants:
            continue
        by_id[row["id"]] = variants
    return by_id, meta


def _evaluate_topk(
    scenario: dict,
    top_metas: list[ChunkMeta],
    translation: int,
    psalm_maps: dict[int, PsalmMap],
    top_k: int,
) -> ScenarioResult:
    """Metrics of one ranked candidate list against the scenario references
    (same matching rules as evaluate_config)."""
    relevant = [r for r in scenario["references"] if r["grade"] == "relevant"]
    unacceptable = [
        r for r in scenario["references"] if r["grade"] == "unacceptable"
    ]
    rel_targets = [map_reference(r, translation, psalm_maps) for r in relevant]
    una_targets = [map_reference(r, translation, psalm_maps) for r in unacceptable]
    hit_rank = 0
    for rank, cm in enumerate(top_metas, start=1):
        if any(chunk_matches(cm, t) for t in rel_targets):
            hit_rank = rank
            break
    matched = sum(
        1 for t in rel_targets if any(chunk_matches(cm, t) for cm in top_metas)
    )
    una_hits = sum(
        1 for cm in top_metas if any(chunk_matches(cm, t) for t in una_targets)
    )
    return ScenarioResult(
        scenario_id=scenario["id"],
        language=scenario["language"],
        category=scenario["category"],
        hit=hit_rank > 0,
        recall=matched / len(relevant) if relevant else 1.0,
        rr=1.0 / hit_rank if hit_rank else 0.0,
        unacceptable_share=una_hits / top_k,
        query_ms=0.0,
    )


# ---------------------------------------------------------------------------
# Final top-1 stage (ClickUp 86cb8vw1h): grounded rerank + fallback metrics
# against thresholds.json final_top1.
# ---------------------------------------------------------------------------

# Safety-first: a chunk intersecting an unacceptable reference is graded
# unacceptable even if it also touches a relevant range.
GRADE_PRIORITY = ("unacceptable", "relevant", "acceptable")


def _grade_chunk(
    scenario: dict, cm: ChunkMeta, translation: int,
    psalm_maps: dict[int, PsalmMap],
) -> str:
    """Grade of one chosen chunk against the scenario references
    (thresholds matching rule); "ungraded" when it matches no reference."""
    for grade in GRADE_PRIORITY:
        refs = [r for r in scenario["references"] if r["grade"] == grade]
        targets = [map_reference(r, translation, psalm_maps) for r in refs]
        if any(chunk_matches(cm, t) for t in targets):
            return grade
    return "ungraded"


def _rerank_cached(
    scenario: dict, candidate_ids: list[str], candidate_texts: list[str],
    reranker, model: str, cache: dict, stats: dict,
    key_verses: bool = True,
):
    """Reranker choice for one scenario, disk-cached like rewrites.

    Returns (0-based index, reason, key_verse_start, key_verse_end) or None
    when the rerank failed (the caller falls back to top-1, mirroring
    production select_final). The key verses are the 1-based verse markers
    of the highlight inside the chosen candidate (prompt v9)."""
    import hashlib

    from passage_rerank import RERANK_PROMPT_VERSION, PassageRerankError

    ids_hash = hashlib.sha1("|".join(candidate_ids).encode()).hexdigest()[:16]
    # key_verses toggles the prompt variant (build_rerank_instruction) for
    # the same RERANK_PROMPT_VERSION, so it must be part of the cache key
    # too. Only the False case gets a suffix, so existing cache entries for
    # the benchmarked key_verses=True path (the vast majority) keep hitting
    # unchanged.
    key = (
        f"{model}|p{RERANK_PROMPT_VERSION}|{scenario['id']}|{ids_hash}"
        if key_verses else
        f"{model}|p{RERANK_PROMPT_VERSION}|{scenario['id']}|{ids_hash}|nomarkers"
    )
    hit = cache["reranks"].get(key)
    if hit is not None:
        return (
            hit["index"], hit.get("reason", ""),
            hit.get("key_verse_start"), hit.get("key_verse_end"),
        )
    ctx = scenario["prayer_context"]
    stats["calls"] += 1
    try:
        choice = reranker.choose(
            topic=ctx["topic"],
            user_replies=list(ctx["user_replies"]),
            candidate_texts=candidate_texts,
            key_verses=key_verses,
        )
    except PassageRerankError as exc:
        stats["failures"] += 1
        print(f"  [warn] rerank failed for {scenario['id']}: {exc}")
        return None
    cache["reranks"][key] = {
        "index": choice.index,
        "reason": choice.reason,
        "key_verse_start": choice.key_verse_start,
        "key_verse_end": choice.key_verse_end,
    }
    return (
        choice.index, choice.reason,
        choice.key_verse_start, choice.key_verse_end,
    )


# ---------------------------------------------------------------------------
# Key-verse highlight (rerank prompt v9): the reranker answers with verse
# markers of the candidate the server rendered, so the benchmark needs the
# same per-verse view of the corpus that production reads from MySQL.
# Without a database the pipeline still runs — unnumbered candidates, no
# highlights (exactly the production degradation).
# ---------------------------------------------------------------------------

def _load_chunk_verses(metas: list[ChunkMeta]) -> dict[tuple[int, str], list]:
    """(translation, canonical_id) -> verses of that chunk, from MySQL."""
    try:
        from database import create_connection
        from retrieval import VerseText
    except Exception as exc:                      # pragma: no cover
        print(f"  [warn] verse loading unavailable: {exc}")
        return {}
    try:
        connection = create_connection()
    except Exception as exc:
        print(f"  [warn] verse loading unavailable: {type(exc).__name__} {exc}")
        return {}
    if connection is None:
        print("  [warn] no database: candidates stay unnumbered, "
              "no key-verse highlights")
        return {}
    cursor = connection.cursor(dictionary=True)
    verses: dict[tuple[int, str], list] = {}
    try:
        by_translation: dict[int, list[ChunkMeta]] = {}
        for meta in metas:
            by_translation.setdefault(meta.translation, []).append(meta)
        for code, chunk_metas in by_translation.items():
            cursor.execute(
                """
                SELECT book_number, chapter_number, verse_number, text,
                       start_paragraph
                FROM translation_verses WHERE translation = %s
                ORDER BY book_number, chapter_number, verse_number
                """,
                (code,),
            )
            by_chapter: dict[tuple[int, int], list[dict]] = {}
            for row in cursor.fetchall():
                by_chapter.setdefault(
                    (row["book_number"], row["chapter_number"]), []
                ).append(row)
            for meta in chunk_metas:
                verses[(code, meta.canonical_id)] = [
                    VerseText(
                        verse_number=row["verse_number"],
                        text=row["text"].strip(),
                        start_paragraph=bool(row["start_paragraph"]),
                    )
                    for row in by_chapter.get((meta.book, meta.chapter), ())
                    if meta.vstart <= row["verse_number"] <= meta.vend
                    and row["text"].strip()
                ]
    finally:
        cursor.close()
        connection.close()
    return verses


def _load_book_names() -> dict[int, dict[str, str]]:
    """book number -> {language: short name}, for the review table."""
    try:
        from database import create_connection

        connection = create_connection()
    except Exception:
        return {}
    if connection is None:
        return {}
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT number, short_name_ru, short_name_en, short_name_uk "
            "FROM bible_books"
        )
        return {
            row["number"]: {
                "ru": row["short_name_ru"],
                "en": row["short_name_en"],
                "uk": row["short_name_uk"],
            }
            for row in cursor.fetchall()
        }
    finally:
        cursor.close()
        connection.close()


def _resolve_bench_highlight(
    meta: ChunkMeta, verses: list, indices: tuple[int, int],
    psalm_maps: dict[int, PsalmMap],
) -> dict | None:
    """Verse-marker span -> reference + verse text, or None (as production).

    Mirrors retrieval._highlight_indices + passage_highlight.resolve_highlight
    so the benchmark reports exactly what the endpoint would return.
    """
    from passage_highlight import VerseSpan, to_canonical_span
    from passage_rerank import MAX_KEY_VERSES

    start, end = indices
    if start is None or end is None or not verses:
        return None
    if not 1 <= start <= end <= len(verses):
        return None
    if end - start + 1 > MAX_KEY_VERSES:
        return None
    chosen = verses[start - 1:end]
    span = VerseSpan(
        chapter=meta.chapter,
        verse_start=chosen[0].verse_number,
        verse_end=chosen[-1].verse_number,
    )
    canonical = to_canonical_span(
        meta.book, span, psalm_maps.get(meta.translation)
    )
    if canonical is None:
        return None
    return {
        "book": meta.book,
        "chapter": span.chapter,
        "verse_start": span.verse_start,
        "verse_end": span.verse_end,
        "canonical_chapter": canonical.chapter,
        "canonical_verse_start": canonical.verse_start,
        "canonical_verse_end": canonical.verse_end,
        "text": " ".join(verse.text for verse in chosen),
    }


def _highlight_review_table(
    rows: list[dict], scenarios: list[dict], books: dict[int, dict[str, str]]
) -> None:
    """Full per-scenario listing for the editor's manual review."""
    by_id = {s["id"]: s for s in scenarios}
    print("\n=== key-verse highlights (manual theological review) ===")
    missing = [r["scenario_id"] for r in rows if not r.get("highlight")]
    print(f"{len(rows) - len(missing)}/{len(rows)} scenarios carry a "
          f"highlight" + (f"; without one: {', '.join(missing)}" if missing
                          else ""))
    for row in rows:
        scenario = by_id.get(row["scenario_id"], {})
        language = scenario.get("language", "")
        topic = (scenario.get("prayer_context") or {}).get("topic", "")
        print(f"\n  {row['scenario_id']} [{scenario.get('category', '?')}] "
              f"{topic!r}")
        print(f"    top-1 : {row['chosen']}  ({row['method']}, "
              f"{row['grade']})")
        highlight = row.get("highlight")
        if not highlight:
            print("    key   : —")
            continue
        book = books.get(highlight["book"], {}).get(
            language, str(highlight["book"])
        )
        span = (
            f"{highlight['verse_start']}"
            if highlight["verse_start"] == highlight["verse_end"]
            else f"{highlight['verse_start']}-{highlight['verse_end']}"
        )
        canonical_span = (
            f"{highlight['canonical_verse_start']}"
            if highlight["canonical_verse_start"]
            == highlight["canonical_verse_end"]
            else f"{highlight['canonical_verse_start']}"
                 f"-{highlight['canonical_verse_end']}"
        )
        print(f"    key   : {book} {highlight['chapter']}:{span} "
              f"(canonical {highlight['book']} "
              f"{highlight['canonical_chapter']}:{canonical_span})")
        print(f"    text  : {highlight['text']}")


def _final_top1_report(label: str, rows: list[dict], thresholds: dict) -> None:
    """Evaluate one top-1 policy against thresholds.json final_top1.

    Ungraded top-1s count neither as success nor failure — they are listed
    separately for manual review (Maria)."""
    t = thresholds["final_top1"]
    graded = [r for r in rows if r["grade"] != "ungraded"]
    ungraded = [r for r in rows if r["grade"] == "ungraded"]
    n = len(graded)
    rel = sum(r["grade"] == "relevant" for r in graded) / n if n else 0.0
    rel_acc = sum(
        r["grade"] in ("relevant", "acceptable") for r in graded
    ) / n if n else 0.0
    una_all = sum(r["grade"] == "unacceptable" for r in rows)
    sens = [r for r in graded if r["category"] == "sensitive"]
    sens_rel = (
        sum(r["grade"] == "relevant" for r in sens) / len(sens)
        if sens else 1.0
    )
    sens_una = sum(
        r["grade"] == "unacceptable" for r in rows
        if r["category"] == "sensitive"
    )

    print(f"\n=== final top-1: {label} ===")
    print(f"graded {n}/{len(rows)} scenarios "
          f"(ungraded listed for manual review, not counted)")
    checks = [
        ("relevant_share", rel, ">=", t["relevant_share_min"]),
        ("relevant_or_acceptable", rel_acc, ">=",
         t["relevant_or_acceptable_share_min"]),
        ("unacceptable_share", una_all / len(rows) if rows else 0.0, "<=",
         t["unacceptable_share_max"]),
        ("sensitive_relevant_share", sens_rel, ">=",
         t["sensitive_relevant_share_min"]),
        ("sensitive_unacceptable", sens_una / len(rows) if rows else 0.0,
         "<=", t["sensitive_unacceptable_share_max"]),
    ]
    for name, value, op, limit in checks:
        ok = value >= limit if op == ">=" else value <= limit
        print(f"  threshold {name}: {value:.3f} {op} {limit} "
              f"-> {'PASS' if ok else 'FAIL'}")
    by_grade = {}
    for r in rows:
        by_grade[r["grade"]] = by_grade.get(r["grade"], 0) + 1
    print(f"  grades: {by_grade}")
    bad = [r for r in graded if r["grade"] != "relevant"]
    if bad:
        print("  non-relevant graded top-1:")
        for r in bad:
            print(f"    {r['scenario_id']:<8} [{r['category']}] "
                  f"{r['grade']:<12} {r['chosen']}  {r.get('reason', '')}")
    if ungraded:
        if t.get("ungraded_review_required"):
            print(f"  [action required] {len(ungraded)} top-1 match no "
                  f"reference — grade them and merge into scenarios.json "
                  f"(thresholds.ungraded_review_required=true); the shares "
                  f"above are computed WITHOUT them")
        print("  ungraded top-1 (manual review):")
        for r in ungraded:
            print(f"    {r['scenario_id']:<8} [{r['category']}] "
                  f"{r['chosen']}  reason: {r.get('reason', '')}")


def _coverage_allowed(
    translation_code: int, metas: list[ChunkMeta]
) -> tuple[int, frozenset[str]]:
    """Canonical windows a NON-INDEXED translation can render (ADR 0007).

    Simulates the production candidate filter: the corpus stays the indexed
    translation of the language (the rerank prompt is unchanged), but only
    windows that fully exist in `translation_code` may be chosen — which is
    what `POST /api/ai/scripture` does when the passage will be
    rendered in a translation that was never chunked.

    Returns (corpus translation code of that language, allowed canonical
    IDs). Needs the database: coverage is a fact about the verses, not about
    the exported chunk corpus.
    """
    from database import create_connection
    from passage_highlight import load_psalm_maps
    from passage_render import (
        build_coverage,
        load_chunk_ranges,
        reference_faithful_windows,
    )
    from retrieval import parse_canonical_id

    connection = create_connection()
    if connection is None:
        raise SystemExit("--coverage-translation needs the cep_public database")
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT alias, language FROM translations WHERE code = %s",
            (translation_code,),
        )
        row = cursor.fetchone()
        if row is None:
            raise SystemExit(f"translation {translation_code} not found")
        corpus_code, _alias = LANGUAGE_CORPUS[row["language"]]
        windows = []
        seen = set()
        for meta in metas:
            if meta.translation != corpus_code or meta.canonical_id in seen:
                continue
            seen.add(meta.canonical_id)
            _v, book, chapter, start, end = parse_canonical_id(meta.canonical_id)
            windows.append((meta.canonical_id, book, chapter, start, end))
        maps = load_psalm_maps(cursor)
        # Same two steps as production (`scripture_select._build_catalogue`):
        # windows whose stored reference chunk really is the window's own
        # range, then the ones this translation carries in full.
        offered = reference_faithful_windows(
            windows,
            load_chunk_ranges(cursor, corpus_code),
            maps.get(corpus_code),
        )
        covered = frozenset(
            build_coverage(
                cursor, translation_code, offered, maps.get(translation_code)
            )
        )
    finally:
        cursor.close()
        connection.close()
    print(
        f"coverage filter: {row['alias']} (code {translation_code}, "
        f"{row['language']}) renders {len(covered)} of {len(windows)} "
        f"canonical windows of the {row['language']} corpus"
    )
    return corpus_code, covered


def _rrf_fuse(variant_hits: list[list[tuple[str, float]]], k: int = 60):
    """Reciprocal-rank fusion alternative to retrieval.fuse_variant_hits."""
    from retrieval import FusedHit

    fused: dict[str, FusedHit] = {}
    for variant_index, hits in enumerate(variant_hits):
        for rank, (canonical_id, score) in enumerate(hits, start=1):
            entry = fused.get(canonical_id)
            if entry is None:
                entry = FusedHit(canonical_id=canonical_id, score=0.0,
                                 best_variant=variant_index)
                fused[canonical_id] = entry
            entry.variant_scores[variant_index] = score
            entry.score += 1.0 / (k + rank)
    return sorted(fused.values(), key=lambda h: -h.score)


def cmd_pipeline(args) -> None:
    from retrieval import (
        apply_diversity,
        fuse_interleave,
        fuse_variant_hits,
        is_blacklisted,
        load_genre_blacklist,
        load_safe_pool,
        merge_semantic_lexical,
        parse_canonical_id,
        rotate_safe_pool,
    )
    from lexical_index import LexicalIndex
    from query_rewrite import GeminiQueryRewriter, QueryRewriteError

    thresholds = json.loads(THRESHOLDS_FILE.read_text())
    scenarios = json.loads(SCENARIOS_FILE.read_text())["scenarios"]
    metas, _texts, _tt = load_chunks()
    psalm_maps = load_psalm_maps()
    corpus = truncate_mrl(np.load(emb_path("gemini", "title_text")), PIPELINE_DIMS)
    api_key = require_api_key()
    cache = _load_pipeline_cache()
    blacklist = load_genre_blacklist() if not args.no_blacklist else []
    safe_pool = load_safe_pool()
    # ADR 0007 simulation: restrict one language's candidates to the windows
    # a non-indexed translation can render. Other languages are untouched, so
    # their cached rewrites/embeddings/reranks stay valid.
    coverage_code, coverage_allowed = (
        _coverage_allowed(args.coverage_translation, metas)
        if args.coverage_translation else (None, frozenset())
    )

    def allowed(code: int, canonical_id: str) -> bool:
        return code != coverage_code or canonical_id in coverage_allowed

    # per-translation corpus views + canonical-id lookups
    trans_idx = {
        code: np.array([i for i, m in enumerate(metas) if m.translation == code])
        for code, _alias in LANGUAGE_CORPUS.values()
    }
    meta_by_key = {(m.translation, m.canonical_id): m for m in metas}
    canon_parsed = {
        code: [
            (metas[i].canonical_id, *parse_canonical_id(metas[i].canonical_id)[1:])
            for i in idx
        ]
        for code, idx in trans_idx.items()
    }

    from config import RETRIEVAL_REWRITE_MODEL

    # External rewrites (--rewrites-file): the rewrite stage runs entirely
    # from disk. No rewriter is constructed at all, so the run cannot reach a
    # rewrite provider even by accident.
    external_rewrites: dict[str, list[str]] = {}
    rewriter = None
    if args.rewrites_file:
        external_rewrites, ext_meta = _load_external_rewrites(args.rewrites_file)
        rewrite_model = args.rewrite_model or (
            f"file:{ext_meta.get('model', Path(args.rewrites_file).stem)}"
        )
    else:
        rewrite_model = args.rewrite_model or RETRIEVAL_REWRITE_MODEL
        if not args.no_rewrite:
            rewriter = GeminiQueryRewriter(
                api_key=require_rewrite_api_key(),
                model=rewrite_model,
                variants=args.variants,
            )

    # --- final top-1 stage: rerank (optional) + fallback policies (free)
    reranker = None
    rerank_model = ""
    rerank_rows: list[dict] | None = None
    rerank_stats = {"calls": 0, "failures": 0}
    if args.rerank:
        from config import RETRIEVAL_RERANK_MODEL
        from passage_rerank import GeminiPassageReranker

        rerank_model = args.rerank_model or RETRIEVAL_RERANK_MODEL
        # Patient offline settings: a 429 burst in the middle of a run used
        # to leave whole languages un-reranked (and re-running costs calls).
        reranker = GeminiPassageReranker(
            api_key=api_key, model=rerank_model, timeout=60.0, attempts=6
        )
        rerank_rows = []
    fb_rank_rows: list[dict] = []    # retrieval rank-1 (interleave order)
    fb_score_rows: list[dict] = []   # best fused cosine

    # candidate text exactly as production shows it to the reranker
    # (retrieval._candidate_prompt_text: "title\ntext" of the primary
    # translation, every verse prefixed with its [n] marker when the verses
    # are available)
    chunk_title: dict[tuple[int, str], str] = {}
    chunk_text: dict[tuple[int, str], str] = {}
    with CHUNKS_FILE.open() as fh:
        for line in fh:
            row = json.loads(line)
            key = (row["translation"], row["canonical_id"])
            chunk_title[key] = (row.get("title") or "").strip()
            chunk_text[key] = row["text"]

    chunk_verses = _load_chunk_verses(metas) if args.rerank else {}
    book_names = _load_book_names() if args.rerank else {}

    def prompt_text(code: int, canonical_id: str) -> str:
        from retrieval import number_verses

        key = (code, canonical_id)
        verses = chunk_verses.get(key)
        body = number_verses(verses) if verses else chunk_text[key]
        title = chunk_title[key]
        return f"{title}\n{body}" if title else body

    def top1_row(scenario: dict, cm: ChunkMeta, method: str,
                 reason: str = "", highlight: dict | None = None) -> dict:
        _v, book, chapter, vs, ve = parse_canonical_id(cm.canonical_id)
        return {
            "scenario_id": scenario["id"],
            "category": scenario["category"],
            "grade": _grade_chunk(scenario, cm, cm.translation, psalm_maps),
            "chosen": f"{cm.canonical_id} "
                      f"(canonical {book} {chapter}:{vs}-{ve})",
            "method": method,
            "reason": reason,
            "highlight": highlight,
        }

    lexical: dict[int, LexicalIndex] = {}
    if not args.no_lexical:
        _m, _texts2, title_texts = load_chunks()
        for code, _alias in LANGUAGE_CORPUS.values():
            docs = [
                (metas[i].canonical_id, title_texts[i])
                for i in trans_idx[code]
            ]
            lexical[code] = LexicalIndex(docs)

    canon_row = {
        code: {metas[i].canonical_id: i for i in idx}
        for code, idx in trans_idx.items()
    }

    def search_variant(
        code: int, query: str, qvec: np.ndarray
    ) -> list[tuple[str, float]]:
        idx = trans_idx[code]
        sims = corpus[idx] @ qvec
        top_local = np.argsort(-sims)[: args.fetch_k]
        semantic = [
            (metas[idx[j]].canonical_id, float(sims[j])) for j in top_local
        ]
        lex_index = lexical.get(code)
        if lex_index is None:
            return semantic
        lex = [
            (h.canonical_id,
             float(corpus[canon_row[code][h.canonical_id]] @ qvec))
            for h in lex_index.search(query, top_k=args.lex_k)
        ]
        return merge_semantic_lexical(semantic, lex)

    results, per_scenario = [], []
    rewrite_failures = 0
    try:
        for scenario in scenarios:
            code, _alias = LANGUAGE_CORPUS[scenario["language"]]
            ctx = scenario["prayer_context"]
            raw_query = "\n".join(
                p for p in [ctx["topic"].strip()]
                + [r.strip() for r in ctx["user_replies"]] if p
            )

            # --- empty topic: safe pool (no retrieval, no Gemini)
            if not raw_query and not args.no_pool:
                resolved = []
                for ref in safe_pool:
                    best = None
                    for cid, book, chapter, start, end in canon_parsed[code]:
                        if not allowed(code, cid):
                            continue
                        if (book == ref.book and chapter == ref.chapter
                                and end >= ref.verse_start
                                and start <= ref.verse_end):
                            if best is None or start <= ref.verse_start:
                                best = cid
                    resolved.append(best)
                indices = rotate_safe_pool(safe_pool, resolved, set(), TOP_K)
                seen, pool_ids = set(), []
                for i in indices:
                    if resolved[i] is not None and resolved[i] not in seen:
                        seen.add(resolved[i])
                        pool_ids.append(resolved[i])
                top_metas = [meta_by_key[(code, cid)] for cid in pool_ids]
                results.append(_evaluate_topk(
                    scenario, top_metas, code, psalm_maps, TOP_K))
                per_scenario.append({
                    "scenario_id": scenario["id"], "source": "safe_pool",
                    "queries": [], "top": pool_ids,
                })
                if top_metas:
                    # production select_final: safe pool -> top-1, no rerank
                    row = top1_row(scenario, top_metas[0], "safe_pool")
                    fb_rank_rows.append(row)
                    fb_score_rows.append(dict(row))
                    if rerank_rows is not None:
                        rerank_rows.append(dict(row))
                continue

            # --- rewrite (unless ablated)
            queries: list[str] = []
            if args.rewrites_file and not args.no_rewrite:
                queries = list(external_rewrites.get(scenario["id"], []))
                if not queries:
                    # same degradation path as a live QueryRewriteError
                    rewrite_failures += 1
                    print(f"  [warn] rewrite missing/failed for "
                          f"{scenario['id']} in {args.rewrites_file}")
            elif not args.no_rewrite:
                try:
                    queries = _scenario_rewrites(
                        scenario, rewriter, rewrite_model, args.variants,
                        cache, args.cache_tag)
                except QueryRewriteError as exc:
                    rewrite_failures += 1
                    print(f"  [warn] rewrite failed for {scenario['id']}: {exc}")
            if not args.no_raw or not queries:
                queries = queries + [raw_query]

            # --- embed + search + fuse
            variant_hits = [
                search_variant(code, q, _query_vector(q, api_key, cache))
                for q in queries
            ]
            if args.fusion == "rrf":
                fused = _rrf_fuse(variant_hits)
            elif args.fusion == "interleave":
                fused = fuse_interleave(variant_hits)
            else:
                fused = fuse_variant_hits(variant_hits)

            # --- blacklist + diversity
            filtered = []
            for hit in fused:
                if not allowed(code, hit.canonical_id):
                    continue
                _v, book, chapter, start, end = parse_canonical_id(hit.canonical_id)
                if is_blacklisted(blacklist, book, chapter, start, end):
                    continue
                filtered.append(hit)
            final = apply_diversity(
                filtered, TOP_K, args.max_per_book, args.max_per_chapter)

            top_metas = [meta_by_key[(code, h.canonical_id)] for h in final]
            results.append(_evaluate_topk(
                scenario, top_metas, code, psalm_maps, TOP_K))
            per_scenario.append({
                "scenario_id": scenario["id"], "source": "retrieval",
                "queries": queries,
                "top": [
                    {"id": h.canonical_id, "score": round(h.score, 4),
                     "variant": h.best_variant}
                    for h in final
                ],
            })
            if not top_metas:
                continue
            fb_rank_rows.append(
                top1_row(scenario, top_metas[0], "fallback_rank1"))
            best = max(range(len(final)), key=lambda i: final[i].score)
            fb_score_rows.append(
                top1_row(scenario, top_metas[best], "fallback_score"))
            if rerank_rows is not None:
                texts_for_prompt = [
                    prompt_text(code, h.canonical_id) for h in final
                ]
                got = _rerank_cached(
                    scenario, [h.canonical_id for h in final],
                    texts_for_prompt, reranker, rerank_model, cache,
                    rerank_stats,
                    # mirrors production: without a database the candidates
                    # carry no [n] markers, so the key-verse contract is not
                    # asked for at all (retrieval.select_final does the same)
                    key_verses=bool(chunk_verses),
                )
                if got is None or not 0 <= got[0] < len(top_metas):
                    # production fallback: any rerank failure -> rank-1
                    rerank_rows.append(
                        top1_row(scenario, top_metas[0], "fallback_rank1"))
                else:
                    chosen_meta = top_metas[got[0]]
                    rerank_rows.append(top1_row(
                        scenario, chosen_meta, "rerank", got[1],
                        highlight=_resolve_bench_highlight(
                            chosen_meta,
                            chunk_verses.get(
                                (code, chosen_meta.canonical_id), []
                            ),
                            (got[2], got[3]),
                            psalm_maps,
                        ),
                    ))
    finally:
        _save_pipeline_cache(cache)
        if rewriter is not None:
            rewriter.close()
        if reranker is not None:
            reranker.close()

    label = (
        f"pipeline rewrite={'off' if args.no_rewrite else rewrite_model}"
        f" variants={args.variants} raw={'no' if args.no_raw else 'yes'}"
        f" fusion={args.fusion} blacklist={'off' if args.no_blacklist else 'on'}"
        f" pool={'off' if args.no_pool else 'on'}"
        f" lexical={'off' if args.no_lexical else f'k{args.lex_k}'}"
        f" fetch_k={args.fetch_k} max_per_book={args.max_per_book}"
        + (f" coverage={args.coverage_translation}"
           if args.coverage_translation else "")
    )
    print_report(label, results, {"rewrite_failures": rewrite_failures},
                 thresholds)
    if rerank_rows is not None:
        print(f"\nrerank: model={rerank_model} fresh_calls="
              f"{rerank_stats['calls']} failures={rerank_stats['failures']}")
        _final_top1_report(f"rerank {rerank_model}", rerank_rows, thresholds)
        _highlight_review_table(rerank_rows, scenarios, book_names)
    _final_top1_report(
        "fallback rank-1 (retrieval order, no AI rerank)",
        fb_rank_rows, thresholds)
    _final_top1_report(
        "fallback max-score (best fused cosine, no AI rerank)",
        fb_score_rows, thresholds)
    if args.json_out:
        payload = {
            "config": label,
            "aggregate": aggregate(results, "ALL").__dict__,
            "scenarios": [r.__dict__ for r in results],
            "details": per_scenario,
            "final_top1": {
                "rerank_model": rerank_model,
                "rerank": rerank_rows,
                "fallback_rank1": fb_rank_rows,
                "fallback_max_score": fb_score_rows,
            },
        }
        Path(args.json_out).write_text(
            json.dumps(payload, ensure_ascii=False, indent=1))


# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("embed", help="embed the chunk corpus for one config")
    p.add_argument("--model", choices=MODELS, required=True)
    p.add_argument("--variant", choices=VARIANTS, required=True)
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_embed)

    p = sub.add_parser("run", help="evaluate one cached config")
    p.add_argument("--model", choices=MODELS, required=True)
    p.add_argument("--variant", choices=VARIANTS, required=True)
    p.add_argument("--dims", type=int, default=0, help="MRL truncation (gemini)")
    p.add_argument("--json-out", default="")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("run-all", help="evaluate every cached config")
    p.set_defaults(func=cmd_run_all)

    p = sub.add_parser(
        "pipeline",
        help="full retrieval pipeline (rewrite + fusion + blacklist + pool)",
    )
    p.add_argument("--no-rewrite", action="store_true",
                   help="ablation: search the raw query only")
    p.add_argument("--with-raw", dest="no_raw", action="store_false",
                   help="ablation: search the raw query alongside variants")
    p.set_defaults(no_raw=True)
    p.add_argument("--no-blacklist", action="store_true",
                   help="ablation: disable the genre blacklist")
    p.add_argument("--no-pool", action="store_true",
                   help="ablation: search empty topics instead of the safe pool")
    p.add_argument("--fusion", choices=("max", "rrf", "interleave"),
                   default="interleave")
    p.add_argument("--no-lexical", action="store_true",
                   help="ablation: disable the hybrid BM25 signal")
    p.add_argument("--lex-k", type=int, default=20,
                   help="lexical hits merged into each variant's ranking")
    p.add_argument("--variants", type=int, default=6,
                   help="rewrite variants per scenario")
    p.add_argument("--fetch-k", type=int, default=FETCH_K_DEFAULT)
    p.add_argument("--max-per-book", type=int, default=4)
    p.add_argument("--max-per-chapter", type=int, default=1)
    p.add_argument("--rewrite-model", default="",
                   help="Gemini model for rewriting "
                        "(default: production RETRIEVAL_REWRITE_MODEL)")
    p.add_argument("--rerank", action="store_true",
                   help="run the grounded final-choice stage (Gemini) and "
                        "evaluate final_top1 thresholds")
    p.add_argument("--rerank-model", default="",
                   help="Gemini model for the final choice "
                        "(default: production RETRIEVAL_RERANK_MODEL)")
    p.add_argument("--cache-tag", default="",
                   help="extra rewrite-cache key part (stability re-sampling)")
    p.add_argument("--rewrites-file", default="",
                   help="JSON with rewrite variants produced by an EXTERNAL "
                        "model ({meta, scenarios:[{id, variants, error}]}); "
                        "the rewrite stage then runs fully offline — no "
                        "rewrite provider is constructed or called. Scenarios "
                        "with an error/empty variants degrade to the raw "
                        "query, exactly like a live rewrite failure")
    p.add_argument("--coverage-translation", type=int, default=0,
                   help="ADR 0007: restrict the candidates of that "
                        "translation's language to the canonical windows it "
                        "can render (e.g. 21 for npu); other languages are "
                        "unaffected")
    p.add_argument("--json-out", default="")
    p.set_defaults(func=cmd_pipeline)

    p = sub.add_parser("stores", help="store parity + latency for one config")
    p.add_argument("--model", choices=MODELS, required=True)
    p.add_argument("--variant", choices=VARIANTS, required=True)
    p.add_argument("--dims", type=int, default=0)
    p.add_argument("--qdrant-url", default="http://localhost:6333")
    p.set_defaults(func=cmd_stores)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
