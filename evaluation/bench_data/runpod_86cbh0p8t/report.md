# Qwen / Gemma Runpod experiment

Task: https://app.clickup.com/t/86cbh0p8t. Started 2026-09-12 UTC.

## Scope and method

The baseline is the shared `https://llm.ai2.ru/v1` API serving
`qwen3-30b-a3b-instruct-2507`. The candidate is the pinned Gemma model and
runtime described in [preflight.md](preflight.md). The live local Bible-API
question endpoint uses OpenRouter and is deliberately excluded from load
generation. Existing production services and model settings are not modified.

The load client sends synthetic fixtures directly to the model APIs. Each
fixture is warmed once before recorded requests. Question, rewrite and rerank
results are reported separately. Concurrency 1, 2, 4, 8 and 16 denotes in-flight
requests from this test; other clients of the shared Qwen service can still
contribute load. Therefore concurrency 1 is not a verified idle GPU baseline.
Repeated fixtures exercise a warm prompt/cache workload rather than unique
production conversations.
The timing fixture contains only six distinct inputs (two per stage); 20
measurements per stage are repetitions, not 20 different conversation contexts.

Metrics include time to first content token, completion latency, generated
tokens, decode rate, throughput, transport/timeout failures, truncation and
format validity. A usable response accepted by production parsing is distinct
from strict adherence to the requested JSON contract. Percentiles from a small
sample are exploratory and do not establish a production capacity limit.
Different model tokenizers also limit direct interpretation of tokens/second.

The GPU budget is capped at $15 for the experiment. The first rental has a
two-hour operational deadline; actual identifiers, timestamps and teardown
evidence belong in the deployment record. No persistent paid storage is to be
left behind after results are retrieved.

## Results

### Qwen warm-cache baseline

Measured on 2026-09-12 with `load_benchmark.py`: 60 requests per concurrency
level (20 per stage), 180 recorded requests plus six excluded warmups. All
completed with `finish_reason=stop`, without transport failures, unusable
responses or strict-contract violations. The payload uses application prompts
and stage temperatures, but a uniform 1024-token output cap; it is not an exact
replay of every production generation setting. The historical `production`
filename does not imply that the harness offers a production payload profile;
the artifact explicitly identifies this as an application-prompt workload.

| Stage | Concurrent requests | Median completion, s | p95 completion, s |
| --- | ---: | ---: | ---: |
| Question | 1 | 0.365 | 0.447 |
| Question | 2 | 0.453 | 0.549 |
| Question | 4 | 0.561 | 0.692 |
| Question | 8 | 0.708 | 0.831 |
| Question | 16 | 0.872 | 1.119 |
| Rewrite | 1 | 2.557 | 2.771 |
| Rewrite | 2 | 3.255 | 3.723 |
| Rewrite | 4 | 4.029 | 4.660 |
| Rewrite | 8 | 5.172 | 5.912 |
| Rewrite | 16 | 6.334 | 7.253 |
| Rerank | 1 | 0.381 | 0.405 |
| Rerank | 2 | 0.462 | 0.530 |
| Rerank | 4 | 0.642 | 0.700 |
| Rerank | 8 | 0.817 | 0.866 |
| Rerank | 16 | 1.067 | 1.192 |

Mixed-workload throughput increased from 0.902 to 1.410 to 2.216 completed
requests/second. These figures describe the six repeated fixtures, not a
capacity guarantee for arbitrary conversations. Each stage has only 20 samples
per concurrency level, so the p95 values are exploratory.

Source: [qwen_production_load_2026-09-12.json](qwen_production_load_2026-09-12.json).
Maria subsequently authorized concurrency 8 and 16. Each added 60 measured
requests and six excluded warmups, with no errors or invalid responses. Level
8 was checked against the saved level-4 stage p95 values before proceeding to
16. Mixed-workload throughput was 3.341 and 4.579 requests/second, respectively.
Sources: [level 8](qwen_load_c8_2026-09-12.json) and
[level 16](qwen_load_c16_2026-09-12.json). In total the Qwen timing experiment
contains 300 measured requests and 18 excluded warmups. These are finite
60-request closed-loop batches, including ramp-up and drain, not a sustained
throughput limit measurement.
Gemma measurements are pending. No replacement decision has been made.

### Question quality sample

The [quality fixture](quality_cases.json) contains 20 synthetic multilingual
scenarios, including accumulated-skips series. Three samples per scenario/turn
produce 78 responses. The Qwen run completed with zero failed generations;
transport attempts were limited to one. Outputs are stored in
[qwen_quality_2026-09-12.jsonl](qwen_quality_2026-09-12.jsonl), with its adjacent
metadata file. Generation success is not a quality acceptance verdict.

For the blind review, exclude sample 1 for both models and retain samples 2–3.
For single questions and the first step of each series this excludes the first
pass over identical prompt text. Later accumulated-skips steps contain the
model's own stochastic previous answers, so their prompts are not identical
between samples or models: they test the resulting dialogue behavior. Do not
use quality-run latency for speed comparisons or claim deterministic pairing
of those later turns. The load benchmark is the separate timing evidence.

Quality acceptance requires Maria's review of blinded model outputs. This
experiment does not change the reference scenarios, retrieval thresholds, or
production model selection.

## Verification

The focused harness and question-generator tests passed: 69 tests, exit code 0,
run by the implementation agent in the documented container environment.
Independent review also checked the streaming metrics,
bounded load, per-stage aggregation, artifact structure and absence of secrets.
