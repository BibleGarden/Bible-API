# Gemma Runpod deployment plan

Prepared on 2026-09-12 for ClickUp 86cbh0p8t. Creation is gated on the orchestrator's explicit `GO`.

## Billable resource

- One Secure Cloud `NVIDIA L40S` (48 GB), one GPU, preferred data center `EUR-IS-2`.
- Catalog compute rate observed before preparation: $1.09/hour. Re-read stock and price immediately before creation.
- Container disk: 30 GB. Persistent Pod volume: 60 GB at `/workspace`.
- Initial budget guard: warn at one hour and terminate before two hours. At the one-hour checkpoint, the user asked whether stopping would lose the completed download. The orchestrator extended its initial self-imposed cap to four hours within the user's authorized `$15` experiment budget. The revised maximum is about `$4.41` at the quoted compute and disk rates, with the Pod stopped before `2026-09-12T11:29:55.668Z`. The user explicitly chose stop rather than delete after testing so the model cache on `/workspace` remains available. MCP create-pod has no timed-stop field, so teardown is an active-session responsibility. Do not invent one.

## Create body

The API key placeholder must be replaced in memory from the mode-0600 temporary key file. Never print or commit the value.

```json
{
  "name": "bible-gemma4-31b-fp8-86cbh0p8t",
  "cloud": "SECURE",
  "dataCenterIds": ["EUR-IS-2"],
  "gpu": {
    "id": "NVIDIA L40S",
    "count": 1
  },
  "image": "docker.io/vllm/vllm-openai:v0.24.0@sha256:251eba5cc7c12fed0b75da22a9240e582b1c9e39f6fbc064f86781b963bd814f",
  "args": "--model RedHatAI/gemma-4-31B-it-FP8-dynamic --revision d4ab4f579dd3516f97d8a6a4c98d0653480bad15 --served-model-name gemma-4-31b-it-fp8 --download-dir /workspace/huggingface --max-model-len 8192 --gpu-memory-utilization 0.90 --limit-mm-per-prompt '{\"image\":0,\"audio\":0}' --default-chat-template-kwargs '{\"enable_thinking\":false}' --enable-prefix-caching --host 0.0.0.0 --port 8000",
  "disk": 30,
  "ports": ["8000/http", "22/tcp"],
  "env": {
    "HF_HOME": "/workspace/huggingface",
    "VLLM_API_KEY": "<generated-secret>"
  },
  "mounts": {
    "persistent": {
      "path": "/workspace",
      "size": 60
    }
  },
  "startSsh": true,
  "startJupyter": false
}
```

After creation, immediately record the Pod ID, creation time, selected data center, returned hourly rate, and the current deadline in `deployment-result.md`. Read logs without exposing `args` or environment values. Readiness requires both an authenticated external `GET /v1/models` and an authenticated external `POST /v1/chat/completions`; Pod state `RUNNING` is insufficient. After results are copied locally, stop the Pod and verify the control-plane state is `EXITED`. Finalize charges when complete billing data is available. Do not delete the Pod or its persistent volume.

If `EUR-IS-2` stock changes, do not silently move to another GPU. A Secure L40S in another listed data center is an equivalent infrastructure choice, but record the location and live rate before creating it. Do not fall back to a different GPU model, unpinned image, reduced precision, CPU offload, or a smaller context cap.
