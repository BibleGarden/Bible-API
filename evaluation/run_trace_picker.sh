#!/bin/bash
# Start the side-by-side trace stand (ClickUp 86cbegawh).
#
#   ./run_trace_picker.sh --serve              # http://0.0.0.0:9090/
#   ./run_trace_picker.sh "текст молитвы"       # one run, JSON to stdout
#
# UNLIKE run_local_picker.sh THIS ONE CALLS GEMINI. It is the production
# scripture-selection pipeline, so it uses the production keys and models
# from ../.env and every request costs quota and money: 1 rewrite call on
# AI_SCRIPTURE_REWRITE_API_KEY (the paid key), up to 6 embeddings and 1
# rerank on GEMINI_API_KEY. The page says so and counts the calls.
#
# The right-hand column is fetched over HTTP from the LOCAL stand, which
# must already be running on 9089 (evaluation/run_local_picker_qwen.sh).
# Override with TRACE_PICKER_LOCAL_URL; TRACE_PICKER_LOCAL_TIMEOUT bounds
# the wait for it (default 150 s — the local rerank is slow on this box).
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

exec "$HERE/.venv/bin/python" "$HERE/trace_picker.py" "$@"
