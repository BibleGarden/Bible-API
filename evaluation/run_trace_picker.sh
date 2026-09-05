#!/bin/bash
# Start the side-by-side trace stand (ClickUp 86cbegawh; the right column on
# local models — 86cbegcmm).
#
#   ./run_trace_picker.sh --serve              # http://0.0.0.0:9090/
#   ./run_trace_picker.sh "текст молитвы"       # one run, JSON to stdout
#
# THE LEFT COLUMN CALLS GEMINI. It is the production scripture-selection
# pipeline, so it uses the production keys and models from ../.env and every
# request costs quota and money: 1 rewrite call on
# AI_SCRIPTURE_REWRITE_API_KEY (the paid key), up to 6 embeddings and 1
# rerank on GEMINI_API_KEY. The page says so and counts the calls.
#
# THE RIGHT COLUMN COSTS NOTHING. By default (`?r=prod-local`) it is the SAME
# production pipeline with local models: rewrite and rerank on Maria's
# qwen3-30b through the persistent SSH tunnel (systemd unit qwen-tunnel:
# 127.0.0.1:8443 -> llm.ai2 nginx :443, so llm.ai2.ru must resolve to
# 127.0.0.1 in /etc/hosts), embeddings on bge-m3 loaded into this process.
# The bearer key is read over SSH at start-up and lives only in this
# process's environment: never in a file, never in a log, never in git.
# `?r=senses` keeps the previous right column — the local stand on 9089
# (evaluation/run_local_picker_qwen.sh), fetched over HTTP. Override its
# address with TRACE_PICKER_LOCAL_URL; TRACE_PICKER_LOCAL_TIMEOUT bounds the
# wait for it (default 150 s).
#
# Memory: bge-m3 is ~2.3 GB of fp32 weights plus a 11 960 x 1024 float32
# document matrix (~49 MB), loaded ONCE at start-up. Run with --no-local on a
# box that cannot afford it — the senses column still works and the
# prod-local one says it is off instead of quietly degrading.
#
# Reads ../.env, never writes it. Keys stay in this process's environment.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$HERE/../.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "no $ENV_FILE — this stand needs the production keys and models" >&2
  exit 1
fi

set -a
# Only the variables the pipeline needs; nothing is written back.
eval "$(grep -E '^(DB_USER|DB_PASSWORD|DB_NAME|API_KEY|GEMINI_API_KEY|AI_SCRIPTURE_REWRITE_API_KEY|AI_SCRIPTURE_REWRITE_MODEL|AI_SCRIPTURE_RERANK_MODEL|AI_QUESTION_MODEL|AI_TRANSCRIBE_MODEL|EMBEDDING_MODEL|EMBEDDING_DIMENSIONS)=' "$ENV_FILE")"
set +a

# cep-mysql publishes 127.0.0.1:3306; the container hostname does not resolve
# from the host, so the value from .env would fail here.
export DB_HOST=127.0.0.1
export DB_PORT=3306

# --- the local column's providers (86cbegcmm) ------------------------------
# Same mechanics as run_local_picker_qwen.sh: the tunnel must be up and the
# key is fetched over SSH into the environment only. Missing -> the local
# column reports which variable is unset; the stand still starts.
if systemctl is-active --quiet qwen-tunnel.service; then
  QWEN_KEY="$(ssh -o BatchMode=yes root@193.39.168.166 \
    'sed -n "s/^VLLM_SECONDARY_API_KEY=//p" /etc/vllm/api-secondary.env' || true)"
  if [[ -n "${QWEN_KEY:-}" ]]; then
    export TRACE_LOCAL_REWRITE_ENDPOINT="https://llm.ai2.ru:8443/v1"
    export TRACE_LOCAL_REWRITE_MODEL="qwen3-30b-a3b-instruct-2507"
    export TRACE_LOCAL_REWRITE_API_KEY="$QWEN_KEY"
    export TRACE_LOCAL_RERANK_ENDPOINT="$TRACE_LOCAL_REWRITE_ENDPOINT"
    export TRACE_LOCAL_RERANK_MODEL="$TRACE_LOCAL_REWRITE_MODEL"
    export TRACE_LOCAL_RERANK_API_KEY="$QWEN_KEY"
    unset QWEN_KEY
  else
    echo "warn: empty VLLM key — the prod-local column will report it" >&2
  fi
else
  echo "warn: qwen-tunnel.service is not active — the prod-local column will report it" >&2
fi

# bge-m3 comes from the local HF cache; the stand must never fetch weights.
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1

exec "$HERE/.venv/bin/python" "$HERE/trace_picker.py" "$@"
