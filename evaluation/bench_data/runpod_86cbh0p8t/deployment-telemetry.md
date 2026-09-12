# Runpod Gemma 4 runtime telemetry

Collected on 2026-09-12 through authenticated localhost metrics and read-only GPU/disk commands while the controlled benchmark was running. No extra inference requests were generated for these samples.

## Low-concurrency samples

Four complete samples between `08:54:02Z` and `08:54:42Z`, at ten-second intervals, observed:

- GPU memory: 41,561 MiB used of 46,068 MiB on the L40S.
- GPU utilization: 100% in every sample.
- Requests running: 1.
- Requests waiting: 0, including zero capacity waits and zero deferred waits.
- KV-cache usage: 0.1453 to 0.1675.
- Total preemptions: 0.

Additional samples during the main load run:

- `09:03:42Z` and `09:04:12Z`, concurrency 2: 2 running, 0 waiting, GPU 100%, 41,561 MiB of 46,068 MiB used, KV-cache usage up to 0.3391, and 0 preemptions.
- `09:04:22Z`, concurrency 4: 4 running, 0 waiting, GPU 100%, 41,561 MiB of 46,068 MiB used, KV-cache usage 0.5180, and 0 preemptions.

No synchronized telemetry sample was captured for concurrency 8 or 16. Their latency, throughput, and error results belong to the benchmark artifact; this telemetry file does not infer queue or cache behavior for those levels.

The container filesystem used about 300 MB of its 30 GB writable disk. `/workspace` used 32 GB of 60 GB after the checkpoint download, leaving 29 GB free. At startup vLLM reported a 14,668-token GPU KV cache (5.95 GiB) and an estimated maximum concurrency of 1.79 only for requests that each consume the full 8,192-token configured context.

The Pod was stopped after the load and quality artifacts were copied locally. Independent control-plane readback at `09:19:53Z` returned `EXITED`; no post-stop GPU or request sample was claimed.
