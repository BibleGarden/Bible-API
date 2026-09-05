#!/bin/bash
# Start the local picker with Maria's Qwen3-30B as the final-choice model.
# The vLLM server is reached over direct HTTPS at https://llm.ai2.ru (the
# admins added this machine's IP to the allow-list on 2026-09-05; llm.ai2.ru
# resolves through public DNS, no /etc/hosts override needed). Before that
# date this went through a persistent SSH tunnel (systemd unit
# qwen-tunnel.service, 127.0.0.1:8443 -> llm.ai2 nginx :443, with
# llm.ai2.ru forced to 127.0.0.1 in /etc/hosts) — now retired. The bearer
# key is read over SSH at start-up and lives only in this process's
# environment: never in a file, never in a log, never in git.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEY="$(ssh -o BatchMode=yes root@193.39.168.166 'sed -n "s/^VLLM_SECONDARY_API_KEY=//p" /etc/vllm/api-secondary.env')"
[[ -n "$KEY" ]] || { echo "empty VLLM key" >&2; exit 1; }
export LOCAL_PICKER_RERANK_ENDPOINT="https://llm.ai2.ru/v1"
export LOCAL_PICKER_RERANK_MODEL="qwen3-30b-a3b-instruct-2507"
export LOCAL_PICKER_RERANK_API_KEY="$KEY"
export LOCAL_PICKER_RERANK_TIMEOUT=60
exec "$HERE/run_local_picker.sh" "$@"
