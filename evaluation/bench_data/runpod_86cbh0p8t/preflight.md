# Runpod Gemma 4 preflight

Checked on 2026-09-12 for ClickUp 86cbh0p8t. This is a read-only preflight based on the live Hugging Face API and model files, the live vLLM documentation and image registry, and the published Runpod Terms of Service. No Pod was created and no inference was run during this check.

## Reproducible artifacts

- Model: [`RedHatAI/gemma-4-31B-it-FP8-dynamic`](https://huggingface.co/RedHatAI/gemma-4-31B-it-FP8-dynamic), pinned to commit `d4ab4f579dd3516f97d8a6a4c98d0653480bad15`. The [Hugging Face API record](https://huggingface.co/api/models/RedHatAI/gemma-4-31B-it-FP8-dynamic) was public, ungated, and enabled when checked.
- The two safetensors shards reported by the [repository tree API](https://huggingface.co/api/models/RedHatAI/gemma-4-31B-it-FP8-dynamic/tree/main?recursive=true&expand=false) total 33,268,122,536 bytes, about 31.0 GiB.
- Runtime: `docker.io/vllm/vllm-openai:v0.24.0@sha256:251eba5cc7c12fed0b75da22a9240e582b1c9e39f6fbc064f86781b963bd814f`. Its Linux amd64 manifest is `sha256:f9de5cd9fa907fbf6dbba691eb7db095d48ad58ea283e3eba7142f9a91e186e8`.
- The [quantized model card](https://huggingface.co/RedHatAI/gemma-4-31B-it-FP8-dynamic/blob/d4ab4f579dd3516f97d8a6a4c98d0653480bad15/README.md) states that this checkpoint was validated with vLLM 0.24.0. The [vLLM quantization support matrix](https://docs.vllm.ai/en/latest/features/quantization/) lists `llm-compressor FP8 (W8A8)` as supported on Ada (SM 8.9), which covers L40S, RTX 6000 Ada, and RTX 4090.

The experiment is text-only. Disabling image and audio inputs avoids reserving memory for multimodal profiling. Use a 30 GB container disk and a 60 GB Pod volume mounted at `/workspace`, with the Hugging Face cache under `/workspace/huggingface`. A network volume is unnecessary for this disposable experiment. The Pod volume survives a stop but is deleted on termination.

## Startup arguments

Use the image's existing `vllm serve` entrypoint with these Docker arguments:

```text
--model RedHatAI/gemma-4-31B-it-FP8-dynamic --revision d4ab4f579dd3516f97d8a6a4c98d0653480bad15 --served-model-name gemma-4-31b-it-fp8 --download-dir /workspace/huggingface --max-model-len 8192 --gpu-memory-utilization 0.90 --limit-mm-per-prompt '{"image":0,"audio":0}' --default-chat-template-kwargs '{"enable_thinking":false}' --enable-prefix-caching --host 0.0.0.0 --port 8000
```

For a structured MCP `create-pod` argument, the literal JSON field is:

```json
{
  "args": "--model RedHatAI/gemma-4-31B-it-FP8-dynamic --revision d4ab4f579dd3516f97d8a6a4c98d0653480bad15 --served-model-name gemma-4-31b-it-fp8 --download-dir /workspace/huggingface --max-model-len 8192 --gpu-memory-utilization 0.90 --limit-mm-per-prompt '{\"image\":0,\"audio\":0}' --default-chat-template-kwargs '{\"enable_thinking\":false}' --enable-prefix-caching --host 0.0.0.0 --port 8000",
  "env": {"VLLM_API_KEY": "<generated-secret>"}
}
```

The outer JSON decoder removes the backslashes and leaves the single-quoted JSON value intact for argument parsing. vLLM 0.24.0 reads `VLLM_API_KEY` directly in `api_server.py`, so the secret stays out of the process arguments. Insert the generated secret into the create request in memory and do not print or commit it.

`8192` is the initial context cap because this benchmark uses representative Bible-API requests below that size. Reserving 32K context would reduce the available concurrency without improving these measurements. A later capacity check can raise the cap separately if production conversations require it. This benchmark does not infer the company Qwen server's configured maximum context.

Do not add Gemma reasoning or tool-call parsers to the first comparison: Bible-API's question path uses ordinary chat completion, and enabling thinking would change both output length and latency. Thinking is therefore disabled explicitly in the server's default chat-template arguments; record this setting with every result. The public HTTP proxy URL requires the vLLM API key. The live create-pod schema has no timed-termination field: record the creation deadline, warn the orchestrator at one hour, and actively terminate before two hours.

## License and service terms

- Both the [original model record](https://huggingface.co/api/models/google/gemma-4-31B-it) and the Red Hat quantized model record declare Apache-2.0. Google's linked [Gemma 4 license](https://ai.google.dev/gemma/docs/gemma_4_license) is the Apache License 2.0. No model-license age or end-user territory restriction was found in the published text checked on 2026-09-12.
- The [Runpod Terms of Service](https://www.runpod.io/legal/terms-of-service), marked “Last Updated: March 24, 2026” when checked, contained no general requirement that a customer's end users be 18 or older. They prohibit illegal and harmful content and restrict use by sanctioned parties and from listed embargoed territories. These are Runpod account and service-use obligations; this temporary internal benchmark does not make Runpod the eventual production inference provider.

This section records a technical reading of the published terms, not legal advice or a guarantee that the terms will remain unchanged. Recheck the live terms before a production deployment or a material change in audience or hosting geography.

## Remaining runtime gates

The first Pod boot must still prove that the pinned vLLM image loads this checkpoint on the selected L40S, report actual VRAM use, and answer an authenticated request through the external URL. A successful Runpod “Running” state alone is not readiness. If the model fails to fit at the representative 8K cap, stop and diagnose the recorded startup error rather than silently changing quantization or offloading.
