# Runpod Gemma 4 deployment result

## Resource record

- Pod ID: `8q0uiy9j7lon07`
- Name: `bible-gemma4-31b-fp8-86cbh0p8t`
- Created: `2026-09-12T07:29:55.668Z` (`2026-09-12 10:29:55` Europe/Moscow)
- Initial two-hour deadline: `2026-09-12T09:29:55.668Z`. At the one-hour checkpoint, the user asked whether stopping would lose the completed download. The orchestrator extended its initial self-imposed cap to four hours within the user's authorized `$15` experiment budget.
- Revised mandatory compute-stop deadline: before `2026-09-12T11:29:55.668Z` (`14:29:55` Europe/Moscow), four hours after creation. The user explicitly requested stop rather than delete after testing to preserve `/workspace`.
- One Secure Cloud `NVIDIA L40S`, requested in `EUR-IS-2`
- Live compute quote immediately before creation: `$1.09/hour`
- Storage: 30 GB container disk plus 60 GB persistent Pod volume. The current Runpod quote is about `$0.004/hour` for the running container disk and `$0.008/hour` for the running persistent volume, making the running total about `$1.10/hour`. When stopped, the container disk is erased and no longer charged; the 60 GB persistent volume remains at `$12/month` (about `$0.0167/hour`) until the Pod is deleted.
- Revised expected maximum for four hours at these rates: about `$4.41`.
- Initial API state returned by create-pod: `RUNNING`. This is container state, not model readiness.

The model API credential was supplied through `VLLM_API_KEY` and is intentionally absent from this artifact. See `deployment-plan.md` and `preflight.md` for the pinned image, model revision, and launch arguments.

## Verification

Cold-start evidence:

- The pinned Linux amd64 image manifest contains 34 layers totaling 9,203,858,315 compressed bytes (8.572 GiB). This was read directly from Docker Registry using manifest digest `sha256:f9de5cd9fa907fbf6dbba691eb7db095d48ad58ea283e3eba7142f9a91e186e8`.
- At `07:36:25Z`, six image layer IDs were still downloading.
- At `07:41:41Z`, three remained. Their compressed sizes from the manifest were 3.600 GiB (`761033aa3ab9`), 1.609 GiB (`4b26248a25ba`), and 1.435 GiB (`8d698a4b10d2`).
- At `08:06:32Z`, only the 3.600 GiB layer remained.
- At the one-hour checkpoint, `08:29:40Z`, that last layer was still actively downloading. No container log existed, the SSH proxy returned `container not found`, and model weights had not started downloading. A live Secure L40S catalog read then returned availability `NONE`, so another equivalent Pod could not be started.
- At `08:36:04Z`, the final layer was still emitting active `Downloading` events. Cold-start elapsed time was 66 minutes. The log API exposes layer state but no byte count or percentage within a layer.
- The image pull completed at `08:42:49Z`, 72 minutes 54 seconds after Pod creation, and Docker verified the pinned manifest digest. The container began starting at `08:42:52Z`.
- vLLM 0.24.0 resolved `Gemma4ForConditionalGeneration`, selected `CutlassFP8ScaledMMLinearKernel` for the compressed FP8 checkpoint on Ada, and began loading the model at `08:43:50Z`. Gemma 4's heterogeneous attention head dimensions forced the `TRITON_ATTN` backend. This is expected runtime selection, not an error.
- At `08:45:11Z`, the L40S reported 46,068 MiB total VRAM, 36,785 MiB used, and 0% utilization during checkpoint loading. `/workspace/huggingface` occupied 22 GB; the 60 GB volume had 39 GB free. The live process arguments matched the recorded plan and did not contain the API secret.
- vLLM reported a 30.98 GiB checkpoint, 206.47 seconds to download weights, 3.45 seconds to load the two shards, and 31.71 GiB of model memory. Engine initialization took 155.75 seconds, including 87.46 seconds of compilation and 15 seconds of CUDA graph capture.
- The resulting GPU KV cache holds 14,668 tokens (5.95 GiB); vLLM estimated maximum concurrency of 1.79 at the full 8,192-token request length. Benchmark prompts are substantially shorter, so higher request concurrency remains measurable but must be checked for errors and queueing rather than inferred from this full-context estimate.
- At `08:48:38Z`, the complete cache occupied 32 GB and the 60 GB volume had 29 GB free. The L40S reported 34,015 MiB used and 97% utilization while processing the checkpoint.
- By `08:51:59Z`, an authenticated external `GET /v1/models` returned `gemma-4-31b-it-fp8`. An authenticated external chat completion then returned exactly `ready` with `finish_reason=stop`, 20 prompt tokens, 2 completion tokens, and 2.468 seconds total request time. This is the first real serving proof; it is not a benchmark result.

The controlled load and quality runs completed, and their artifacts were copied to the local repository before teardown. The credential remains only in the mode-0600 temporary file and is not recorded here.

## Teardown

- Stop was issued at about `2026-09-12T09:19:45Z`.
- Independent readback at `2026-09-12T09:19:53Z` returned status `EXITED`, exposed only `start` and `terminate` as subsequent actions, and retained the 60 GB persistent mount at `/workspace`. GPU compute had stopped and the Pod was not deleted.
- Running duration was about 1 hour 50 minutes. At the quoted `$1.09/hour` compute plus about `$0.012/hour` running storage, the estimated run cost is `$2.02`. The post-stop [partial billing snapshot](billing_partial_2026-09-12.json) contains only the two hourly buckets ending at 09:00 UTC, totaling `$1.0053`. The 09:00-to-stop interval is absent; neither that partial sum nor the estimate is a confirmed final charged total.
- The retained 60 GB persistent volume costs `$12/month` while stopped. The 30 GB container disk is erased on stop. The model weights under `/workspace/huggingface` remain; the vLLM image and compilation cache outside `/workspace` may need to be fetched or rebuilt on a later start.
