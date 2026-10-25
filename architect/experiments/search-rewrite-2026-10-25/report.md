# General-search rewrite ablation — 2026-10-25

## Scope and method

Bible-API baseline `81a6c94d632a2be25b839bdcf742e3aab0b35239`, on the development server. The live endpoint and production were not changed during measurement. The experiment invoked the internal pipeline directly, bypassing HTTP rate limiting, sequentially with no retries.

Inputs were frozen before generation: six intents translated into ru/en/uk (18 cases), including five positive intents and one unrelated Wi-Fi request. They are six intent families, not 18 independent observations. References are non-exhaustive chapter/verse anchors, not an exhaustive relevance judgement. Existing AI-Evaluation scenarios and thresholds were not changed.

Both arms used the same warm corpus snapshot, real DB rendering, hybrid search (semantic top-50 + BM25 top-20, diversity, 20 candidates), Gemini 3.5 Flash-Lite ranking with highlights, limit 5, and 30-second request budget. The rewrite arm used the original query plus up to three Cerebras Qwen 3.8 paraphrases. The raw arm used only the original query. First-arm order alternated between cases. The raw arm retained construction/closing of an unused rewrite client for this comparison; it made no rewrite request. This small overhead is removed from the selected implementation.

Runtime model identities and per-call token usage/status are preserved in `results.jsonl`. Model prices and account billing tiers were not verified; USD cost was not calculated. The run was explicitly authorized as a live comparison; ordinary CI remains offline.

## Results

| Measure | With rewrite | Original query only |
|---|---:|---:|
| Reference found among 20 candidates (15 positive cases) | 12 | 13 |
| Reference found in output (12 paired successful positive cases) | 9 | 10 |
| Median seconds (same 12 paired positive cases) | 2.423 | 1.970 |
| Completed pipelines (18 cases) | 13 | 14 |
| Rewrite API requests (18 cases) | 18 | 0 |
| Embedding API requests (18 cases) | 65 | 18 |
| Gemini HTTP 429 responses (18 cases) | 4 | 4 |

One additional rewrite-arm pipeline returned a structurally invalid ranking/highlight response (`ValueError`, provider HTTP 200). Eight other failures were Gemini HTTP 429. They are failures, never empty successful answers. Neither failed calls nor their shorter latencies are included in the paired output/latency comparison. All 36 candidate retrieval stages completed, so the candidate anchor comparison is complete.

Only one unrelated-query pipeline completed (ru with rewrite, empty output). The remaining five unrelated pipelines received 429. This run cannot establish unrelated-query rejection equivalence. No model calls were retried.

## Decision for the development API

Remove query rewriting from general search. On this small set it added no measured anchor-coverage advantage, displaced a relevant Ukrainian enemies/forgiveness result, and increased model calls and latency. Direct search found anchors in 13/15 candidate lists versus 12/15; among 12 paired successful positive cases, final reference hits were 10 versus 9 and median latency was 1.970 s versus 2.423 s. This supports a simpler development implementation, not a statistically established general quality improvement.

The selected search path makes one embedding request and one ranking request when candidates exist. It makes no Cerebras rewrite request. The accepted prayer-selection pipeline, prompts, models and regression tests remain unchanged; this experiment is not evidence for removing prayer rewriting.

## Unresolved cases

The Russian `распад государства` and English `collapse of a state` miss the Israel/Judah split anchors in BOTH arms. Removing rewriting does not fix that limitation. The Ukrainian equivalent reaches the split references with direct search; the rewrite arm retrieved an anchor but returned an invalid ranking/highlight payload. A broader evaluation, especially abstract historical concepts and unrelated queries, is still needed. Do not add fixed query-to-reference mappings based on this set.

Human review is pending. Anchors do not capture every possibly relevant returned passage; an anchor miss does not automatically mean all returned passages were irrelevant.

## Per-case evidence

`C` means an anchor reached the candidate list; `F` means an anchor reached the output. References are canonical book.chapter ranges. Errors are shown explicitly.

| Case | Rewrite: C / F / seconds | Raw: C / F / seconds |
|---|---|---|
| ru-split | False / False / 2.689 | False / False / 1.851 |
| en-split | False / False / 2.353 | False / False / 1.936 |
| uk-split | True / ERROR ValueError / 2.165 | True / True / 1.895 |
| ru-sea | True / True / 2.700 | True / True / 1.944 |
| en-sea | True / True / 2.404 | True / True / 1.781 |
| uk-sea | True / True / 2.480 | True / True / 2.475 |
| ru-enemies | True / True / 2.345 | True / True / 2.469 |
| en-enemies | True / True / 2.442 | True / True / 1.995 |
| uk-enemies | False / False / 1.984 | True / True / 1.847 |
| ru-jonah | True / True / 2.208 | True / True / 2.163 |
| en-jonah | True / True / 2.989 | True / True / 2.002 |
| uk-jonah | True / True / 2.354 | True / True / 2.121 |
| ru-season | True / True / 2.491 | True / True / 1.709 |
| en-season | True / ERROR 429 / 1.706 | True / True / 1.871 |
| uk-season | True / ERROR 429 / 1.895 | True / ERROR 429 / 1.153 |
| ru-router | False / empty / 2.503 | False / ERROR 429 / 1.153 |
| en-router | False / ERROR 429 / 1.885 | False / ERROR 429 / 1.361 |
| uk-router | False / ERROR 429 / 1.899 | False / ERROR 429 / 1.316 |

## Reproduction and preservation

`compare.py` requires the historical baseline implementation (81a6c94) inside a dedicated development container with the same provider/model identities and corpus. It fails explicitly on the simplified implementation; it does not substitute another checkout. It must never run as an ordinary unit check. Inputs and output are research artifacts, not a replacement for AI-Evaluation datasets.

The measurement command used a baseline-bound `bible-api` development container:

```bash
docker cp queries.json bible-api:/tmp/search-rewrite-queries-2026-10-25.json
docker exec -i -e PYTHONPATH=/code/app bible-api python - /tmp/search-rewrite-queries-2026-10-25.json /tmp/search-rewrite-results-2026-10-25.jsonl < compare.py
```

The script refuses to overwrite output, writes each completed observation immediately and makes no retries. Do not run this recipe against a production container. Corpus content was not modified. The command exited 0; individual pipeline failures are recorded separately above.

Artifact SHA-256:

- `queries.json`: `268960d5a95d8799cdb29e77eac222d7feebf631773fa2a38bc28095d0e0e42b`
- `results.jsonl`: `f992ec286e481d19f74212e5df8beaff1b22332b656924d4de076df116f0437b`
