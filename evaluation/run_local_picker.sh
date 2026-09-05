#!/bin/bash
# Start the local scripture picker (ClickUp 86cbeeqjp).
#
# Reads (never writes) Bible-API/.env for the MySQL credentials, forces the
# DB host to the loopback port the cep-mysql container publishes (the picker
# runs on the HOST, not inside the docker network), and drops every Gemini
# key from the environment before python starts.
#
#   ./run_local_picker.sh --serve                 # http://0.0.0.0:9089/
#   ./run_local_picker.sh "текст молитвы"          # CLI
#
# Point the final-choice stage at a local OpenAI-compatible server:
#   export LOCAL_PICKER_RERANK_ENDPOINT=http://<host>:<port>/v1
#   export LOCAL_PICKER_RERANK_MODEL=<model id>
#   export LOCAL_PICKER_RERANK_API_KEY=<token>     # optional
# Unset -> no final choice, the top-5 is shown as-is and the page says so.
#
# Search a different sense artifact (its matrix is found — or built — by the
# sha1 of that file, so nothing can be searched against the wrong vectors):
#   export LOCAL_PICKER_SENSES_FILE=bench_data/<other>.jsonl
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$HERE/../.env"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC2046
  set -a
  # Only the variables the picker needs; nothing is written back.
  eval "$(grep -E '^(DB_USER|DB_PASSWORD|DB_NAME|API_KEY|EMBEDDING_MODEL|EMBEDDING_DIMENSIONS|EMBEDDING_PROVIDER)=' "$ENV_FILE")"
  set +a
fi

# cep-mysql publishes 127.0.0.1:3306; the container hostname does not resolve
# from the host, so the value from .env would fail here.
export DB_HOST=127.0.0.1
export DB_PORT=3306
# .env's EMBEDDING_MODEL_PATH is the IN-CONTAINER mount (/models/bge-m3); this
# stand runs on the host, so it needs the real host path directly.
export EMBEDDING_MODEL_PATH=/root/models/bge-m3
unset GEMINI_API_KEY AI_SCRIPTURE_REWRITE_API_KEY || true
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1

exec "$HERE/.venv/bin/python" "$HERE/local_picker.py" "$@"
