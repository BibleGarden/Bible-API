#!/bin/bash
# Start the local picker with Maria's Qwen3-30B as the final-choice model.
# The vLLM server is reached through the persistent SSH tunnel
# (systemd unit qwen-tunnel: 127.0.0.1:8443 -> llm.ai2 nginx :443, so the
# host name llm.ai2.ru must resolve to 127.0.0.1 in /etc/hosts). The bearer
# key is read over SSH at start-up and lives only in this process's
# environment: never in a file, never in a log, never in git.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
systemctl is-active --quiet qwen-tunnel.service || { echo "qwen-tunnel.service is not active" >&2; exit 1; }
KEY="$(ssh -o BatchMode=yes root@193.39.168.166 'sed -n "s/^VLLM_SECONDARY_API_KEY=//p" /etc/vllm/api-secondary.env')"
[[ -n "$KEY" ]] || { echo "empty VLLM key" >&2; exit 1; }
export LOCAL_PICKER_RERANK_ENDPOINT="https://llm.ai2.ru:8443/v1"
export LOCAL_PICKER_RERANK_MODEL="qwen3-30b-a3b-instruct-2507"
export LOCAL_PICKER_RERANK_API_KEY="$KEY"
export LOCAL_PICKER_RERANK_TIMEOUT=60
exec "$HERE/run_local_picker.sh" "$@"
