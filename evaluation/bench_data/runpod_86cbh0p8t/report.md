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

The GPU budget is capped at $15 for the experiment. The first rental's initial
two-hour operational deadline was extended to four hours after the slow image
pull and Maria's question about losing downloads on deletion. The revised
estimate is about $4.40 including disk, still within the authorized budget.
Actual identifiers, timestamps and shutdown evidence belong in the deployment
record. Maria subsequently chose stopping the Pod to retain downloaded weights,
rather than deleting it. The stopped 60 GB Pod volume costs approximately
$12/month ($0.20/GB/month); GPU and container disk are not billed while stopped.
This ongoing storage charge is separate from the active test's compute cost.
Container disk contents are erased on stop; only the `/workspace` volume is
retained. A restart does not guarantee the same available GPU or a cached image.

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
### Gemma performance

On the L40S, the first 60 measured requests (20 per stage) completed without
errors, unusable responses, contract violations or truncation.

| Stage | Qwen median/p95, s | Gemma median/p95, s |
| --- | ---: | ---: |
| Question | 0.365 / 0.447 | 1.949 / 2.252 |
| Rewrite | 2.557 / 2.771 | 14.279 / 14.670 |
| Rerank | 0.381 / 0.405 | 3.212 / 3.776 |

Gemma's median decode rate was approximately 18–19 tokens/second versus
122–136 for Qwen. Median completion tokens for question/rewrite/rerank were
30/247/52.5 for Gemma and 36/300/41 for Qwen. Tokenizers differ; these counts
are not equal-length output controls. The measurements include different API
network paths as well as different models, GPUs, and server configurations.

Source: [gemma_load_c1_2026-09-12.json](gemma_load_c1_2026-09-12.json).
All four higher levels also completed 60 requests each, without errors,
unusable responses, contract violations or truncation (300 measured Gemma
requests in total).

| Concurrency | Question p95, s | Rewrite p95, s | Rerank p95, s | Mixed requests/s |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2.252 | 14.670 | 3.776 | 0.156 |
| 2 | 2.790 | 15.358 | 3.814 | 0.302 |
| 4 | 2.391 | 16.314 | 3.775 | 0.573 |
| 8 | 3.494 | 16.172 | 4.530 | 0.993 |
| 16 | 13.967 | 31.082 | 24.062 | 0.955 |

Source: [gemma_load_c2_c16_2026-09-12.json](gemma_load_c2_c16_2026-09-12.json).
Moving from 8 to 16 in-flight requests did not increase throughput on this
fixture. Tail latency rose substantially, with overall first-token p95 about
15 seconds. This is evidence of saturation for the measured mixed workload,
not a universal eight-user limit. The startup KV cache capacity was 14,668
tokens; long distinct prompts can behave differently from these repeated short
inputs.

Quality review is pending. No replacement decision has been made. The
single-request rewrite already consumes almost
the entire 15-second end-to-end retrieval target before embedding/reranking,
so this configuration must not be assumed suitable for a full pipeline swap.

### Measurement incident

The combined Gemma levels 2/4/8/16 completed and saved their result at
09:08:07 UTC. A polling race was incorrectly interpreted as process loss, and
the implementation agent started an unnecessary repeat of level 2. That repeat
was explicitly stopped and independently checked for remaining client processes;
its request count is unknown and it is excluded from the benchmark artifacts.
It started after the completed main run, so it does not overlap the recorded
levels. It does add a small amount of unrecorded traffic/cost. The mistake was
disclosed to Maria. The harness was then changed to flush progress output and
checkpoint completed levels, with focused tests, to avoid the same ambiguity.

### Question quality sample

The [quality fixture](quality_cases.json) contains 20 synthetic multilingual
scenarios, including accumulated-skips series. Three samples per scenario/turn
produce 78 responses for each model. Both runs completed with zero failed
generations; transport attempts were limited to one. Qwen outputs are stored in
[qwen_quality_2026-09-12.jsonl](qwen_quality_2026-09-12.jsonl), with its adjacent
metadata file; Gemma outputs are in
[gemma_quality_2026-09-12.jsonl](gemma_quality_2026-09-12.jsonl) with its metadata.
Generation success is not a quality acceptance verdict. The Gemma runner's
numeric process exit code was not retained by the tool wrapper; the completion
message, 78 rows, zero failures and `partial=false` metadata establish output
completion, not a separately recorded process exit-code assertion.

Open [blind_review.html](blind_review.html) locally to review 40 A/B pairs and
export decisions as JSON. It uses no external assets or network requests. Model
orientation is kept in a separate mapping file and should not be consulted
before completing the blind review.

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

The focused harness and question-generator tests passed: 71 tests, exit code 0,
run by the implementation agent in the documented container environment.
Independent review also ran the blind-pack tests, checked the streaming
metrics, bounded load, per-stage aggregation, artifact structure, and absence
of secrets. A local headless Chromium check opened all 40 review sections,
selected an answer, entered a note and successfully exported decisions as JSON.

## Operational outcome

The Pod was stopped after saving all results. Independent readback at
09:19:53 UTC returned `EXITED` and a retained 60 GB `/workspace` mount.
The approximately 110-minute active interval is estimated at $2.02 using
the quoted compute/storage rates. A billing read shortly after shutdown
contained only two hourly buckets ending at 09:00 UTC, totaling $1.0053;
the 09:00–shutdown interval was absent, so this is not the final charged total.
Stopped storage continues at approximately $12/month until the Pod is deleted.
See [deployment-result.md](deployment-result.md) and
[deployment-telemetry.md](deployment-telemetry.md) for the startup timeline,
measured disk/GPU use and monitoring limitations.
