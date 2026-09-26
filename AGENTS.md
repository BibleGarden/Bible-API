# Bible-API

Public read-only FastAPI API for Bible Garden clients. It reads finalized
content from `cep_public`; Dashboard-API is the export source and
`GET /api/import` is the only publication path.

Workspace-wide rules, including production-data safety, are in `../AGENTS.md`.

## Local work

For first setup, create the local configuration only if `.env` does not already
exist. Set `CLIENT_HMAC_KEY` to the existing `AI_CLIENT_HMAC_KEY` value when
upgrading, or to a new random secret for a fresh installation.

```bash
test -f .env || cp .env.example .env
# Fill CLIENT_HMAC_KEY in .env before starting the container.
docker compose up -d --build
docker logs bible-api -f
docker compose down
```

Export the credentials required by the selected AI providers before creating or
recreating the container; `docker-compose.yml` lists the shell-passed keys.
Never put them in `.env`.
Both Dockerfile and Compose disable Uvicorn access logging; do not remove
`--no-access-log`, since its default records client addresses and query strings.

Generate the OpenAPI document inside the container:

```bash
docker exec bible-api bash -c \
  "cd /code && PYTHONPATH=app python3 extract-openapi.py app.main:app"
```

## Tests

Run tests in the container. Only `app/` is bind-mounted, so copy the contents
of `tests/` first; the trailing `/.` prevents an unwanted nested directory.
Re-copy after recreating the container.
In a worktree, use an isolated test container with that worktree's `app/` and
`tests/` mounted read-only; the usual `bible-api` may bind-mount another checkout.

```bash
docker cp tests/. bible-api:/code/tests
docker exec -e API_KEY=test-api-key -e CLIENT_HMAC_KEY=test-hmac-key \
  bible-api pytest -q
```

The normal suite is offline and injects provider stand-ins. Do not enable the
two real-weight tests unless that is the task:

```bash
docker exec -e EMBEDDING_MODEL_PATH_UNDER_TEST=/models/bge-m3 \
  bible-api pytest -q tests/test_embeddings.py -k real_model
docker exec -e AI_TRANSCRIBE_MODEL_PATH_UNDER_TEST=/models/whisper/small \
  bible-api pytest -q tests/test_transcription.py -k real_model
```

To convert existing raw request-log addresses after release, first run
`docker exec bible-api bash -c 'cd /code && PYTHONPATH=app python -m pseudonymize_request_log --dry-run'`,
then repeat without `--dry-run`. Both commands use `CLIENT_HMAC_KEY` from the
container environment and print counts without printing addresses.

## Non-negotiable rules

- Critical configuration, including providers, models and index identity, must
  fail at startup when missing or invalid. Do not introduce silent defaults or
  fallback behaviour; see `architect/adr/0008-fail-fast-configuration.md`.
- Treat model and retrieval-pipeline changes as architectural decisions. Read
  `architect/adr/0004-retrieval-pipeline.md`,
  `architect/adr/0005-grounded-passage-rerank.md`,
  `architect/adr/0009-provider-independent-llm-client.md`,
  `architect/adr/0010-local-embeddings-bge-m3.md`,
  `architect/adr/0012-speech-transcription-providers.md` and
  `architect/adr/0014-remote-embeddings-openai-compat.md` before changing the
  corresponding code or configuration.
- Publication, backup, removal and RAG-index ordering are controlled by
  `../Deploy/data-flow.md` and `../Deploy/runbook.md`. Do not bypass them with
  direct production SQL.
- Configure trusted reverse proxies through the documented production procedure
  in `../Deploy/runbook.md`; do not pin ephemeral container addresses in code.

## Documentation map

- `README.md` — public API overview, current request contracts and configuration
  examples.
- `architect/twinkler-ai.md` — question and transcription contracts and prompt
  behaviour.
- `architect/scripture-select.md` — scripture-selection API and data flow.
- `architect/adding-a-language.md` — required cross-repository work for a new
  human language, including safety validation.
- `architect/adr/` — accepted repository decisions. In particular, ADR 0015–0018
  cover question history, novelty, structured responses and offline language
  detection.
- `../AI-Evaluation/README.md` — benchmarks and evaluation tools; do not alter
  scenarios or thresholds without the workspace process.
- `../Deploy/runbook.md`, `../Deploy/data-flow.md` and `../Deploy/operations.md` — private current
  deployment and data-operation procedures.
