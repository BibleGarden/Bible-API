# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Public API — a read-only FastAPI service for the Bible Garden iOS app. Works with the `cep_public` database, which contains only finalized data. Data is imported from Dashboard-API via `GET /api/import`.

## Common Commands

### Run / Build
```bash
docker compose up -d --build
docker logs bible-api -f
docker compose down
```
Local AI (all five stages on the company server since 2026-09-05) needs
`AI_OPENAI_COMPAT_API_KEY`, `AI_TRANSCRIBE_API_KEY` and `EMBEDDING_API_KEY`
exported in the shell before `docker compose up -d` (never written to
`.env`; see the comment block at the top of the AI section in `.env`) —
after a host reboot the `restart: always` container comes back with
`key=none` until the export + `up -d` is repeated.

### OpenAPI Spec
```bash
docker exec bible-api bash -c "cd /code && PYTHONPATH=app python3 extract-openapi.py app.main:app"
```

### Tests
Only `app/` is bind-mounted, so `tests/` and `evaluation/` must be copied into
the container (trailing `/.` — plain `docker cp tests` nests it into
`/code/tests/tests`; recreating the container resets both directories to the
image contents):
```bash
docker cp tests/. bible-api:/code/tests
docker cp evaluation/. bible-api:/code/evaluation
docker exec -e API_KEY=test-api-key -e AI_CLIENT_HMAC_KEY=test-hmac-key \
  bible-api pytest -q
```
The suite never loads a model: `conftest` pins `EMBEDDING_PROVIDER=gemini`
and `AI_TRANSCRIBE_PROVIDER=gemini`, and the local-client tests inject
stand-ins (a fake encoder; a fake `faster_whisper` module), so no test
imports 2.3 GB of weights — or faster-whisper at all, which is why the suite
also passes on an image built before ADR 0012. The two tests that load real
weights are skipped unless asked for:
```bash
docker exec -e EMBEDDING_MODEL_PATH_UNDER_TEST=/models/bge-m3 \
  bible-api pytest -q tests/test_embeddings.py -k real_model
docker exec -e AI_TRANSCRIBE_MODEL_PATH_UNDER_TEST=/models/whisper/small \
  bible-api pytest -q tests/test_transcription.py -k real_model
```

`tests/conftest.py` supplies the model and DB variables the fail-fast config
now requires (via `setdefault`, so real values still win), which is why no
extra `-e` overrides are needed. The two that remain are *not* redundant:
`conftest` uses `setdefault`, so the container's real `.env` values for
`API_KEY` and `AI_CLIENT_HMAC_KEY` would win and the auth/pseudonym
assertions (which expect `test-api-key` / `test-hmac-key`) would fail with
403. The `TWINKLER_SYSTEM_PROMPT` override was dropped on 2026-08-30: the
system prompt is a code constant now, not an environment value.

## Architecture

- **`architect/adding-a-language.md`** — cross-cutting checklist: every place a human language is named, enumerated or spelled into code across all repositories (the `languages` table and the per-language `bible_books.short_name_*` columns, bible-parser's closed `ru`/`uk`/`en` branches, the `Language` enum of `POST /api/ai/scripture`, the rewrite prompt's per-language examples, **the despair rule of `app/safety.py`**, transcription, `evaluation/`, ops), with a per-layer verification step, the procedure for validating the despair detector in a new language, a dry run on `uk`, and the full table of hardcoded language literals with file:line (ClickUp 86cbegn16)

### Application Structure (`app/`)

- **`main.py`** — FastAPI app entry point, languages/translations/books endpoints, `timed_cache` decorator
- **`excerpt.py`** — Core content endpoint: `excerpt_with_alignment`. No COALESCE, no voice_manual_fixes (manual fixes already applied during import). Also owns `prev_excerpt`/`next_excerpt` navigation, which walks the books of *this* translation and steps over the ones it ships no text for (see "Navigation across books without text" below). The `excerpt` value is `<book alias> <chapter>[:<verse>[-<verse>]]`; the book is the **catalogue alias** of `GET /api/translations/{code}/books` (`bible_books.code1..code5`), Latin, **case-insensitive** — `EXCERPT_PATTERN` takes the whole token and casefolds it, so `Gen 1:1` no longer matches the substring `en`, an unparseable value is a `422` naming the format and an unknown alias a `404`. `short_name_en`/`short_name_ru` are display names and are **not** matched by the lookup (ClickUp 86cbehfqx, Maria's decision of 2026-09-05; `architect/adding-a-language.md` 3.5)
- **`canon.py`** — chapter structure of the 66-book canon (`CANONICAL_BOOKS`, 1189 chapters) plus the per-translation exceptions; the single source of "how many chapters a book is expected to have" for `/translations/{code}/books` and for excerpt navigation (see "Chapter coverage" below)
- **`audio.py`** — MP3 file serving with HTTP Range request support
- **`about.py`** — About page content, per application: `GET /api/about?app=` selects `bible-garden` (the default, byte-for-byte the pre-2026-09-05 response for released clients) or `lampada` (own website URL/subtitles and description, shared Telegram and GitHub). Unknown values are a 422; the response model and the API key are unchanged, and the selector is **not** an authorization boundary (PR #3, `architect/adr/0011-application-specific-about-content.md`)
- **`version_check.py`** — App version check, per application: `GET /api/version-check?app=` selects `bible-garden` (the default — same fields and same `update_type` for every version a released client can send, plus the additive `app`) or `lampada` (own `LAMPADA_MIN_SUPPORTED_VERSION` / `LAMPADA_LATEST_VERSION` / `LAMPADA_STORE_URL`, and its messages name Lampada). Unknown values are a 422, exactly as in `about.py`. `app_version` is now constrained to one to three numeric components (`^[0-9]+(?:\.[0-9]+){0,2}$`, missing ones read as zero), so a malformed version is a 422 instead of the 500 it used to raise. The **activation switch is a code constant**, `LAMPADA_UPDATES_ENABLED = False`: until its App Store page is public Lampada always gets `update_type=none`, whatever its thresholds say. Not an environment variable on purpose — forcing an update is a release decision, so it lives in a reviewed commit and ADR 0008 does not apply (PR #4, `architect/adr/0013-application-version-policies.md`, ClickUp 86cbbt978)
- **`import_data.py`** — Import data from Dashboard-API
- **`twinkler_ai.py`** — Server-prompted AI integration with in-memory rate limiting: `POST /api/ai/question` (Gemini or an OpenAI-compatible endpoint, per `AI_QUESTION_PROVIDER`) and `POST /api/ai/transcribe` (Whisper on a remote audio server, Whisper in this process, or Gemini — per `AI_TRANSCRIBE_PROVIDER`; the seam is `transcribe()`, everything around it is unchanged). The question handler owns one request `Deadline`, both despair tiers and the novelty retry of `question_novelty.py` — see `architect/twinkler-ai.md`
- **`transcription.py`** — the two Whisper transports of `POST /api/ai/transcribe`: `RemoteTranscriber` (multipart `POST {endpoint}/audio/transcriptions`, the OpenAI **audio** API — the production provider) and `LocalTranscriber` (faster-whisper/CTranslate2 on this CPU, weights from a read-only volume, loaded once at start-up, run on a worker thread). Same contract as the Gemini path — verbatim, no translation, the locale a weak `language=` hint — and the same `502` on failure (`architect/adr/0012-speech-transcription-providers.md`)
- **`llm_client.py`** — the OpenAI-compatible chat-completions transport (`ChatClient`, `AsyncChatClient`): payload, `<think>` stripping, answer extraction, and the shared `gemini_retry` budget/retry policy. Prompts and parsers stay in the stage modules, so both transports send the same bytes (ADR 0009)
- **`question_prompt.py`** — the system prompt of `POST /api/ai/question` as four complete localized texts (`build_question_prompt(language)` for ru/uk/en plus the universal one; `QUESTION_PROMPT_TEMPLATE` is the universal one, kept for diagnostics) with `QUESTION_PROMPT_VERSION` — **6** since 2026-09-06, ClickUp 86cbejvt2 — plus `clarification_angle(step, language)` and `build_user_message(topic, stage, messages, skipped_questions=(), language=None, gender=None, used_subjects=())`, which assembles the per-stage instructions the mobile app used to build itself. Versioned the way `query_rewrite`/`passage_rerank` version theirs; moved out of `TWINKLER_SYSTEM_PROMPT` on 2026-08-30. v2 named the answer language in the prompt (resolved by `safety.detect_language`) and banned interpreting the person's feelings back at them; v3 removed the one sentence about the incoming layout — the stage blocks say it structurally — and moved those blocks here; the skipped-questions block of 86cbehyfe is **additive** (rendered only when the field is non-empty), so it moved no version by itself; v4 is the anti-loop revision chosen by measuring candidate wordings (86cbehyf8); v5 localized every prompt and every stage block (86cbejq55); **v6** changes the answer contract (see the `QUESTION_PROMPT_VERSION = 6` section below). The module still imports nothing from the application — language, gender, angle and used subjects are all resolved by the caller and handed in
- **`question_format.py`** — the answer contract of `POST /api/ai/question` since prompt v6 (ClickUp 86cbejvt2): `parse_question` reads `{"subject": …, "question": …}` through four rungs (`json` → `repaired` via `json_repair.repair_json_object` → `regex`, forgiving about the key's spelling and quoting → `raw`, the answer's first line, salvaged so that a person is never shown a brace), and `SubjectMemory` remembers what each question we have shown was about (`normalize(question) -> subject`, two hours, 2000 entries, oldest evicted) so the next message can list the subjects already used. Dependency-free like `question_prompt.py`, so the evaluation stand parses production answers with the production parser (`architect/adr/0017-structured-question-response.md`)
- **`person_gender.py`** — the person's grammatical gender from a **reviewed list** of first-person ru/uk forms (`detect_gender`): `f`, `m`, or `None` for no match, a contradiction, or English. Read from their own words only — a Twinkler question may carry the very error this replaces. Also dependency-free (ClickUp 86cbejvt2)
- **`json_repair.py`** — the bounded JSON repair of `query_rewrite` (`repair_json_object`), moved here unchanged on 2026-09-06 so `question_format.py` can import the same rule without dragging `config`/`httpx` in; `query_rewrite` re-exports it and every caller and test is unaffected
- **`question_novelty.py`** — the repeat filter of `POST /api/ai/question` (ClickUp 86cbehyg0): `normalize` + character-trigram Jaccard, `is_repeat(candidate, shown)` against the `assistant` turns and `skipped_questions`. Dependency-free like `question_prompt.py`, and the metric is `evaluation/check_questions.py`'s own, so the benchmark number and the production filter are one measurement. Thresholds are reviewed constants with their table in the docstring, never environment (ADR 0008). Lexical only — a reworded return to the same thought is 86cbehyg8 (`architect/adr/0016-question-novelty-check.md`)
- **`question_filters.py`** — the three post-filters of ClickUp 86cbejvra, **not wired into any endpoint**: `detect_gender` (the person's grammatical gender from their own words — reviewed ru/uk form lists, never a `-ла` rule, because «колега звільнився» and «сосед помог» sit inside the same requests), `gender_mismatch` (a question addressing the other gender; with no evidence, *any* explicit gendered address — the gender was imposed), `is_menu` («X или Y»), `has_tail` (a clause appended after a dash, «не просто … а …»). Dependency-free like `question_prompt.py`, constants reviewed in the docstring (ADR 0008). It exists because prompt wording did not fix these: v5 asked Qwen not to default to the masculine and made it worse (15 masculine addresses to a woman against v4's 1 — the independent assessment of 86cbejtt2), and sampling does not either — **six measured runs of 86cbejvra sit in one cloud of 15-22 gender errors**, whatever temperature, `min_p` or `presence_penalty` says, while `min_p` 0.05 at temperature 1.0 is the one lever that moved verbatim repeats (0-2 against the baseline's 4-10 over three runs). Full table, ranges and the honest caveats: `evaluation/README.md`, «Промпт наводящего вопроса v6-A». Verified against that hand count on all 396 benchmark answers: three combinations reproduce it exactly, the fourth by one row, whose false-positive class (a third-person noun subject between the pronoun and the gendered form) is documented rather than tuned away (`tests/test_question_filters.py`)
- **`safety.py`** — the despair / self-harm rule of `POST /api/ai/question` in code rather than in the prompt (ru/uk/en dictionary + regex, no model, no network): tier 1 answers the versioned fixed reply (`SAFETY_REPLIES`, `SAFETY_REPLY_VERSION`) without calling the provider, tier 2 replaces a model reply that came back as a question for a weaker despair signal. Reason: Qwen3-30B answered the explicit despair input with a question 3/3 while Gemini obeyed the prompt (ClickUp 86cbegctz/86cbegg23) — see `architect/twinkler-ai.md`, "The despair rule is code". Since 86cbegmzz, and since Maria's 2026-09-05 decision, **both tiers** read the person's **last reply** (the topic at `stage: first`, nothing at `next`/`reflect` with no history): a phrase that already got the fixed reply must not answer every later question of that prayer with it. Tier 2's fixed reply takes its **language** from the same source the prompt uses (`language_source`), not from the matched text, so it speaks the prayer's language rather than the tier-2 pattern's
- **`scripture_select.py`** — Public scripture-selection endpoint `POST /api/ai/scripture` over `retrieval.select_final`; owns the process-local corpus cache: vector + BM25 indexes, Psalm maps, catalogue, coverage sets (see `architect/scripture-select.md`, `architect/adr/0006-scripture-select-api.md`, `architect/adr/0007-reference-translation-rendering.md`)
- **`passage_render.py`** — renders a canonical passage window in a translation that has no chunk corpus (coordinates through `psalm_verse_mappings`, text from `translation_verses` with `chunking.build_text` semantics) and builds the per-translation coverage sets used to filter candidates before the rerank (ADR 0007)
- **`rate_limit.py`** — Shared in-memory rolling-window limiter (Twinkler + scripture selection)
- **`deadline.py`** — Per-request time budget threaded through the AI stages
- **`gemini_retry.py`** — the retry policy the three Gemini stages share: an
  `httpx.Timeout` whose four phases are carved out of the budget (a bare
  number is applied to each phase separately and would authorise four times
  it), a pause planner that refuses to sleep unless the attempt after it
  still fits, and the reader of a 429 body's `QuotaFailure` / `RetryInfo`
  details (see "The time budget is a ceiling now" below)
- **`prompt_safety.py`** — Neutralizes forged prompt data-block delimiters (invisible characters and angle-bracket look-alikes) in user text
- **`trusted_proxies.py`** — which peers may speak for their clients through `X-Forwarded-For`: container names resolved through docker DNS with a TTL (plus literal IPs and CIDR networks), and the loud diagnostics around them (see "Trusted proxies survive a reboot" below)
- **`client_ip.py`** — the client address of a request (`resolve_client_ip`, through `trusted_proxies`; `X-Forwarded-For` is read **right to left**, see "Which element of `X-Forwarded-For`" below) and its HMAC pseudonym for the stats table and the per-client limiter
- **`auth.py`** — Only API Key authentication (no JWT)
- **`models.py`** — Pydantic response models (no admin models)
- **`chunking.py`** — Pure structural chunking algorithm for RAG (see `architect/adr/0001-structural-chunking.md`)
- **`chunk_cli.py`** — CLI that materializes chunks into `translation_chunks`
- **`versification.py`** — Pure Psalm versification mapping to the canonical english-masoretic numbering (see `architect/adr/0003-psalm-versification-canon.md`)
- **`versification_cli.py`** — CLI: builds/verifies `psalm_verse_mappings`, migrates the chunk corpus to the current CHUNKING_VERSION carrying embeddings over by text (`build`/`verify`/`rechunk`)
- **`embeddings.py`** — the embedding clients of the RAG index and the factory that picks one: `GeminiEmbeddingClient` (the API of `architect/adr/0002-embedding-model-and-vector-store.md`), `LocalEmbeddingClient` (BAAI/bge-m3 on CPU in this process, weights from a read-only volume) and `RemoteEmbeddingClient` (**the production one** — the same bge-m3 on the company server, `POST {endpoint}/embeddings`, batches ≤ 64, unit length verified per vector), chosen by `EMBEDDING_PROVIDER` in `build_embedding_client` — same interface, same `EmbeddingUnavailable` contract, so no caller knows which it holds (`architect/adr/0010-local-embeddings-bge-m3.md`, `architect/adr/0014-remote-embeddings-openai-compat.md`)
- **`vector_index.py`** — `chunk_embeddings` storage + in-process cosine search with language/translation filters
- **`index_cli.py`** — CLI that (re)builds the vector index idempotently (`rebuild`/`status`/`search`); a rebuild **keeps** the rows of every other index version unless `--drop-other-versions` is given, so a model migration builds the new index beside the live one
- **`query_rewrite.py`** — LLM rewrite of prayer context into scripture-register query variants (see `architect/adr/0004-retrieval-pipeline.md`); shared prompt **v8** and parser, a transport per provider (`GeminiQueryRewriter` / `OpenAICompatQueryRewriter`, chosen by `build_query_rewriter`). v8 (86cbegg36) is the benchmark prompt "8c": the model names the passage in a `ref` field before writing each `query` (the parser reads `ref` and drops it), the instruction carries six de-fingerprinted worked examples, and its last line repeats the answer language. The parser also repairs the bounded JSON breakage small models produce (`repair_json_object`: a closer of the wrong type, a truncation at a clean boundary, a trailing comma — never invented content)
- **`lexical_index.py`** — in-process BM25 over chunks, the hybrid lexical signal
- **`retrieval.py`** — retrieval pipeline: interleave fusion, global genre blacklist (`data/genre_blacklist.json`), safe pool (`data/safe_pool.json`), diversity, `ScriptureRetriever` service; `select_final` adds the grounded rerank with top-1 fallback; both entry points accept an optional per-request `Deadline` and can embed query variants concurrently
- **`passage_rerank.py`** — grounded AI choice of the final passage among retrieval candidates plus its 1-3 key verses: validated index-only answer (candidate number + verse-marker span), JSON schema, injection-hardened prompt (see `architect/adr/0005-grounded-passage-rerank.md`); same split as `query_rewrite` (`GeminiPassageReranker` / `OpenAICompatPassageReranker`, `build_passage_reranker`)
- **`passage_highlight.py`** — turns the validated key-verse marker span into canonical and translation coordinates through `psalm_verse_mappings`; the optional `highlight` field of the selection response
- **`retrieval_cli.py`** — end-to-end retrieval smoke CLI (`select`)
- **`database.py`** — MySQL connection factory
- **`config.py`** — Environment variable loading

### Key Differences from Dashboard-API

- No JWT authentication, no admin endpoints
- No `voice_manual_fixes` table — fixes are baked into `voice_alignments` during import
- No `voice_anomalies`, `voice_chapters`, `bible_stat` tables
- No `chapter_with_alignment` endpoint
- `get_translations` always returns only active translations/voices
- `clear_cache` uses API Key instead of JWT

### Database: `cep_public`

Content tables: `languages`, `bible_books`, `translations`, `translation_books`,
`translation_verses`, `translation_titles`, `translation_notes`, `voices`, and
`voice_alignments`. Operational tables include request statistics,
`translation_chunks` (RAG chunks, produced by `app/chunk_cli.py`),
`chunk_embeddings` (chunk vectors, produced by `app/index_cli.py rebuild`;
versioned by chunking version + embedding model) and `psalm_verse_mappings`
(Psalm versification: translation verse → canonical english-masoretic
coordinates, produced by `app/versification_cli.py build`).

The three index tables are **built on the local machine and shipped by the
import** since 2026-09-05 (ClickUp 86cbegwr9): `GET /api/import` writes each
translation's index rows in the same transaction as its text and creates the
tables if they are missing. So this deployment's `cep_public` is the source of
truth for the index while `cep_admin` is the source of truth for the text under
it — the CLIs above are run against the local `cep_public` *after* a local
import, never on production. See "§ Import" below and `Deploy/data-flow.md`.

### Environment

**No silent defaults.** `app/config.py` fails fast: a missing required
variable, or a non-numeric value in any numeric variable, aborts startup with
a single `ConfigError` listing *every* problem at once (not one per restart).
An unset *operational* parameter still falls back to its documented default —
those are tuning knobs. Model names never do.

**Enforced always** (startup fails when unset or blank): `API_KEY`,
`DB_HOST`, `DB_USER`, `DB_NAME`, `EMBEDDING_MODEL`, `EMBEDDING_DIMENSIONS`
(must be ≥ 1), `EMBEDDING_PROVIDER` (`gemini` | `local` | `openai_compat`),
plus `EMBEDDING_MODEL_PATH` when the provider is `local` and an endpoint
(`EMBEDDING_ENDPOINT`, else the shared `AI_OPENAI_COMPAT_ENDPOINT`) plus the
presence of a key when it is `openai_compat` — checked **before** the
"AI is configured" gate below, because embeddings are not part of that
surface: a deployment with no chat provider and no key still reads the index.
`DB_PASSWORD` must be
*present* but may be empty (`DB_PASSWORD=` is an explicit statement; MySQL
accepts a passwordless user — both the local and the production `.env` set a
real password today). `DB_PORT` keeps its default 3306; a non-numeric value
is an error.

**Enforced once the AI surface is configured** — that is, `GEMINI_API_KEY`
is set **or** any provider variable is named (ADR 0009): the four
`AI_*_PROVIDER` variables below (the three chat stages and
`AI_TRANSCRIBE_PROVIDER`, ADR 0012), plus `AI_TRANSCRIBE_MODEL_PATH` when
transcription is `local`. **Enforced when `GEMINI_API_KEY` is set**
(no defaults in code): `AI_QUESTION_MODEL`, `AI_TRANSCRIBE_MODEL`,
`AI_SCRIPTURE_REWRITE_MODEL`, `AI_SCRIPTURE_RERANK_MODEL` — and a stage's
model is required whether or not there is a Gemini key once that stage runs
on `openai_compat` (or, for transcription, on `local`). (Reason: on 2026-08-29 a default
`AI_SCRIPTURE_REWRITE_MODEL=gemini-3.7-flash` sent the rewrite stage to a model
the key could not reach, while `.env` said flash-lite everywhere.)

`GEMINI_API_KEY` itself stays optional — without it the AI surface is "not
configured", the AI endpoints answer with their own error and the rest of the
API works as before, including `POST /api/ai/scripture`, which
degrades to the safe pool with `fallback_reason=ai_unavailable`. That answer
still reads the vector index, which is why `EMBEDDING_MODEL` /
`EMBEDDING_DIMENSIONS` are required with or without a key: they name the
index version (`c{chunking}:{model}@{dims}`), not a provider call.

**Operational, keep their defaults** (a malformed value is still an error):
`AUDIO_FILES_PATH` (`audio`), `AUDIO_BASE_URL` (`http://localhost:8000`),
`ADMIN_API_URL`, `ADMIN_API_KEY` (for import), `IMPORT_MAX_PAYLOAD_MB` (48),
`IMPORT_HTTP_TIMEOUT_SECONDS` (300), `AI_REQUESTS_PER_MINUTE`,
`AI_REQUESTS_PER_CLIENT_PER_MINUTE`, `AI_CLIENT_HMAC_KEY`,
`AI_QUESTION_TIMEOUT_SECONDS` (20),
`AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS` (8),
`TRUSTED_PROXY_HOSTS` / `TRUSTED_PROXY_IPS` /
`TRUSTED_PROXY_DNS_TTL_SECONDS` (30) and the `AI_SCRIPTURE_*` knobs below.
`AUDIO_DIR` is read by docker-compose, not by the application.

For a stage on Gemini, two variables decide whether the AI surface works:
`GEMINI_API_KEY` (unset → that stage's endpoint answers `502`; since ADR 0012
`/api/ai/transcribe` is affected only while **its own** provider is `gemini`)
and `AI_CLIENT_HMAC_KEY` (unset → `503`, the per-client limiter fails
closed rather than dropping the limit). A stage on `openai_compat` needs its
endpoint and model instead, and start-up has already refused an incomplete
one. `AI_SCRIPTURE_REWRITE_API_KEY` is optional and affects the rewrite stage
only (see below). The limiters are process-local, so production uses a single
API worker.

### AI providers per stage (ClickUp 86cbegg2f, 2026-09-05)

Each chat stage names its transport, so moving one to another model is an
`.env` edit plus a benchmark — see
`architect/adr/0009-provider-independent-llm-client.md` and
`app/llm_client.py`.

| Variable | Values | Stage |
| --- | --- | --- |
| `AI_QUESTION_PROVIDER` | `gemini`, `openai_compat` | `POST /api/ai/question` |
| `AI_SCRIPTURE_REWRITE_PROVIDER` | `gemini`, `openai_compat` | retrieval rewrite (ADR 0004) |
| `AI_SCRIPTURE_RERANK_PROVIDER` | `gemini`, `openai_compat` | grounded rerank (ADR 0005) |
| `AI_TRANSCRIBE_PROVIDER` | `gemini`, `openai_compat`, `local` | `POST /api/ai/transcribe` (ADR 0012 — `openai_compat` is the **audio** API here, see the next section) |
| `AI_OPENAI_COMPAT_ENDPOINT` | base URL, e.g. `https://host:8443/v1` | shared by every `openai_compat` stage |
| `AI_OPENAI_COMPAT_API_KEY` | key; **may be empty** = send no `Authorization` header | same |
| `AI_QUESTION_ENDPOINT` / `AI_QUESTION_API_KEY` | optional overrides | that stage alone |
| `AI_SCRIPTURE_REWRITE_ENDPOINT` / `AI_SCRIPTURE_REWRITE_API_KEY` | optional overrides | that stage alone (the key is ADR 0004's paid-key split, generalised) |
| `AI_SCRIPTURE_RERANK_ENDPOINT` / `AI_SCRIPTURE_RERANK_API_KEY` | optional overrides | that stage alone |

- **All four provider variables are required together** once the AI surface
  is configured at all. An `.env` with only `GEMINI_API_KEY` (every
  deployment before this change) does **not** start and names them: which
  provider answers a request is exactly what ADR 0008 forbids defaulting in
  code, and the alternative — starting with AI silently switched off — is the
  same information without the error message.
- An endpoint carrying credentials or a query string is refused: a key
  belongs in `AI_*_API_KEY`, never in a URL that httpx logs.
- **Transcription has its own value set and its own protocol** (ADR 0012, the
  next section but one): `openai_compat` there means
  `POST {endpoint}/audio/transcriptions`, and `local` means Whisper in this
  process. It has the per-stage `AI_TRANSCRIBE_ENDPOINT` /
  `AI_TRANSCRIBE_API_KEY` overrides for the same reason the chat stages do,
  and needs them more: the audio server is a different process from the chat
  one.
- Embeddings have their own provider variable (steps 3 and 10, ClickUp
  86cbegg2r / 86cbehd6h — see the next section): `EMBEDDING_MODEL` names the
  stored index, not only a call, so its provider is required in every
  deployment rather than only when the AI surface is configured. On
  `openai_compat` it uses the same shared pair, with `EMBEDDING_ENDPOINT` /
  `EMBEDDING_API_KEY` as its per-stage override.
- `AI_SCRIPTURE_PROVIDER_TIMEOUT_SECONDS` (8) caps ONE call inside a
  selection and `AI_QUESTION_TIMEOUT_SECONDS` (20) the question endpoint's
  single call. Both defaults are the values those stages always ran with;
  they became variables because `provider_timeout` takes the *minimum* of the
  per-call ceiling and the remaining request budget, so a slower self-hosted
  model cannot be given more time by raising `AI_SCRIPTURE_TIMEOUT_SECONDS`
  alone.

### Embeddings: bge-m3, remote or local (86cbegg2r + 86cbehd6h, 2026-09-05)

`EMBEDDING_PROVIDER` chooses who computes the vectors — full rationale in
`architect/adr/0010-local-embeddings-bge-m3.md` (the model) and
`architect/adr/0014-remote-embeddings-openai-compat.md` (who runs it).

| Variable | `openai_compat` (**production**) | `local` (fallback, rebuild path) | `gemini` |
| --- | --- | --- | --- |
| `EMBEDDING_PROVIDER` | `openai_compat` | `local` | `gemini` |
| `EMBEDDING_MODEL` | `BAAI/bge-m3` | `BAAI/bge-m3` | `gemini-embedding-001` |
| `EMBEDDING_DIMENSIONS` | `1024` | `1024` | `768` |
| `EMBEDDING_MODEL_PATH` | must NOT be set | required, e.g. `/models/bge-m3` | must NOT be set |
| endpoint / key | `EMBEDDING_ENDPOINT` / `EMBEDDING_API_KEY`, else the shared `AI_OPENAI_COMPAT_*` pair | must NOT be set | `GEMINI_API_KEY` |
| index version | `c3:BAAI/bge-m3@1024` | `c3:BAAI/bge-m3@1024` | `c3:gemini-embedding-001@768` |

- **The first two columns are the SAME index version and the same vectors.**
  Switching between them is an `.env` edit and a restart — never a rebuild,
  never an import. Verified on 40 random rows of the live index (ADR 0014):
  cosine median and max **1.000000**, min 0.936332, and the two exceptions are
  the local client's 512-token window (below), which queries never reach.
- **Production is `openai_compat` since 2026-09-05** (Maria's decision, same
  server as Whisper): `POST https://llm.ai2.ru/v1/embeddings`, server
  **Infinity** on CPU, model `BAAI/bge-m3`, `Authorization: Bearer`, direct
  HTTPS. The API process holds **no weights**: 72 MB of application, ~280 MB
  once the corpus cache is loaded (that cache is provider-independent — 208 MB
  at `@1024`, 189 MB at `@768`, measured with no network call), 344 MB peak
  through the live acceptance. Query latency 137 ms median (six variants
  through the pool: 592 ms; sequential 842 ms). **The 8 GB VM is no longer a
  prerequisite** — that was ADR 0010's blocker and it is closed; production
  reads this index on the VM it has.
- **The index still arrives by `GET /api/import`** and is still BUILT locally
  (`EMBEDDING_PROVIDER=local` on this machine, `index_cli rebuild`). A rebuild
  through the remote provider is allowed and needs no weights, but the two
  providers do not write byte-identical rows: the local client caps input at
  512 tokens and **811 of 11 960 chunks (6.8%) are longer**, so those get a
  fuller vector remotely. `index_cli rebuild` prints this and points at
  `--force`; do not mix the two inside one index version.
- **Required in every deployment**, with or without any AI key — the same
  rule as the model/dimensions pair, and for the same reason: the three
  together name the index this service *reads*. An `.env` predating this
  change does not start and the error names the variable.
- **A variable that could never be read is a startup error**, in both
  directions: `EMBEDDING_MODEL_PATH` beside a remote provider, and
  `EMBEDDING_ENDPOINT` / `EMBEDDING_API_KEY` beside `gemini` or `local`. The
  message names the provider that IS configured.
- **On `local`, weights are a read-only volume, never an image layer and
  never a download.** `docker-compose.yml` mounts
  `${EMBEDDING_MODELS_DIR:-./models}` at `/models:ro`; the image sets
  `HF_HUB_OFFLINE=1`, so a missing directory is a loud startup failure.
  Materialise the directory once with
  `huggingface-cli download BAAI/bge-m3 --local-dir <dir>/bge-m3` (2.2 GB).
  On this machine: `/root/models/bge-m3`. On `openai_compat` nothing is
  mounted and nothing is loaded — the banner is all `main.py` does.
- **Memory was the constraint on `local`.** The weights are loaded once at
  start-up (`app/main.py`, fatal on failure — never lazily, or a missing
  volume would look like a provider outage) and stay in the process:
  **2.13 GiB RSS** after warm-up (`ps`; `docker stats` shows ~1.0 GiB because
  the mmapped weight pages are file-backed — size the VM by the 2.13). A
  rebuild peaks at 3.09 GiB. That is what made the 8 GB VM a prerequisite for
  `local`, and what `openai_compat` removes. Image: 1.4 GB → 2.46 GB (torch
  from the CPU-only index — from PyPI it drags ~2.5 GB of CUDA wheels; `-c
  requirements-torch.txt` on the second pip install is what keeps it from
  doing so); the image is unchanged by ADR 0014, since `local` stays
  supported.
- **Two index versions coexist during a migration.** `index_cli rebuild` no
  longer deletes rows of other versions; `--drop-other-versions` is the
  explicit cleanup afterwards. So the switch is an `.env` edit plus a
  restart, and the rollback is the edit back.
- A rebuild is ~1 h of CPU for 11 960 chunks on 8 cores (3.1 chunks/s) and
  needs no network and no key at all. Query embedding: median **39 ms** on
  `local`, **137 ms** through the company server, ~350 ms on Gemini.
- Retrieval quality (86cbe4n7e, full pipeline): hit@10 0.875, recall@10
  0.688, MRR 0.524 against Gemini's 1.000 / 0.789 / 0.664 — recall passes,
  ranking is worse and the grounded rerank absorbs it. Maria lowered the
  retrieval-stage MRR threshold to 0.50 on 2026-09-05 (`thresholds.json`
  0.4.0); the `final_top1` thresholds are unchanged — see ADR 0010's open
  question 1. **The remote provider changes none of these numbers**: the same
  benchmark with `--embedder bge-m3 --embedder-provider openai_compat` (the
  new flag — remote query embeddings against the cached document matrix)
  reproduces the local run to six decimals, every scenario's top-10 identical
  including the scores (86cbehd6h).
- Tests: `tests/test_embeddings.py` (the three clients side by side; the
  remote ones on `httpx.MockTransport`, the local one on a stand-in encoder)
  and the five-stage tripwire
  `test_no_google_host_is_dialled_on_any_of_the_five_stages` in
  `tests/test_twinkler_ai.py`.

### Transcription: Whisper, remote or local (ClickUp 86cbegg3m, 2026-09-05)

`AI_TRANSCRIBE_PROVIDER` chooses who hears the voice message — full rationale
in `architect/adr/0012-speech-transcription-providers.md`, the contract in
`architect/twinkler-ai.md`.

| Variable | `openai_compat` (production) | `local` (fallback) | `gemini` |
| --- | --- | --- | --- |
| `AI_TRANSCRIBE_MODEL` | the name the audio server expects — today `deepdml/faster-whisper-large-v3-turbo-ct2` | `small` \| `medium` | `gemini-3.5-flash-lite` |
| `AI_TRANSCRIBE_MODEL_PATH` | must NOT be set | required, e.g. `/models/whisper/small` | must NOT be set |
| endpoint / key | `AI_TRANSCRIBE_ENDPOINT` / `AI_TRANSCRIBE_API_KEY`, else the shared `AI_OPENAI_COMPAT_*` pair | none — nothing leaves the process | `GEMINI_API_KEY` |

- **Maria's decision of 2026-09-05**: production transcribes on the **CPU of
  the company's Qwen server** (24 cores, ~127 GB RAM, idle beside a GPU fully
  taken by Qwen3-30B), through the OpenAI **audio** API — a multipart
  `POST {endpoint}/audio/transcriptions`, the shape vLLM, speaches and
  faster-whisper-server all expose. The recording leaves this VM but stays
  inside the company, and the model there can be as large as quality wants.
  **Live since 2026-09-05**: `https://llm.ai2.ru/whisper/v1` — direct HTTPS
  from this machine since the same-day IP allow-list (before that, through
  the SSH tunnel `qwen-tunnel.service`, port 8443, now retired), server
  **speaches** on CPU, model `deepdml/faster-whisper-large-v3-turbo-ct2`,
  `Authorization: Bearer`, 14 MB limit at their nginx. Still open for the
  deploy: the production key (`bible-api-prod`, Passbolt) and the production
  VM's place in that server's IP allow-list.
- **Measured on that live endpoint** (review, 15 excerpts, driven through
  `RemoteTranscriber` itself — `evaluation/transcribe_bench.py remote`, which
  is also how to re-measure when the admins change the model): WER ru/uk/en
  **0.037 / 0.059 / 0.002**, CER ru **0.003** (Gemini's own), 3.9-13.9 s per
  excerpt = 0.20x the audio on average (0.42x worst, network included),
  15/15 answered. Better than any local option on Russian by 2.7x; still
  behind Gemini on ru/uk WER (0.019 / 0.051) on this studio corpus. Our side
  spends **no memory at all**: 75 MB RSS for the whole API container.
- **`local` is the fallback and the measured one**: faster-whisper
  (CTranslate2) in the API process, weights from the same read-only `/models`
  volume as bge-m3, `HF_HUB_OFFLINE=1`, loaded once at start-up and **fatal**
  when the directory is missing or unreadable. Materialise it once with
  `hf download Systran/faster-whisper-small --local-dir <dir>/whisper/small`
  (464 MB; `medium` is 1.5 GB). On this machine: `/root/models/whisper/small`.
- **The contract does not change with the provider**: verbatim, in the
  recording's own language (`task="transcribe"`, never `translate`), the
  locale a **weak hint** — its primary subtag becomes `language=` when Whisper
  knows that language and is dropped otherwise. Same 200 shape, same
  413/415/422/429/502/503, same 14 MiB cap, and no error ever names the
  provider, the recording or the transcript.
- **Measured** (8 cores, int8, 15 ru/uk/en excerpts of 17-53 s, full table and
  the side-by-side transcripts in `evaluation/README.md`): WER ru/uk/en
  `small` 0.153 / 0.129 / 0.003, `medium` 0.100 / 0.097 / 0.000, Gemini
  0.019 / 0.051 / 0.000; time 0.07-0.22x the audio (target ≤ 1.5x) and threads
  saturate at ~4. **Speed is not the constraint** — memory and Russian
  quality are: `medium` peaks at 2.1 GB beside bge-m3's 2.13 GiB, and even
  `medium` is 5x Gemini's word error rate on Russian. That is the case for the
  remote provider, where `large-v3` costs this VM nothing.
- Operational knobs, defaults are the measured operating point:
  `AI_TRANSCRIBE_COMPUTE_TYPE` (`int8`, validated against a reviewed list),
  `AI_TRANSCRIBE_THREADS` (0 = all cores), `AI_TRANSCRIBE_BEAM_SIZE` (1 —
  beam 5 buys 0.5-0.7 WER points for ~1.5x the time),
  `AI_TRANSCRIBE_TIMEOUT_SECONDS` (60) and `AI_TRANSCRIBE_MAX_AUDIO_SECONDS`
  (600). The last two split honestly: the timeout bounds the HTTP call on the
  two remote providers, while a local run cannot be cancelled at all
  (CTranslate2 has no cancellation and anyio's pool waits for its thread), so
  locally the **duration cap** is what bounds the work and an over-long run is
  logged as "this machine is too slow for this model".
- **The timeout bounds the whole call, retries included** (86cbegg3w,
  2026-09-05). It used to bound each attempt, so an audio server that accepted
  the connection and answered nothing was waited out twice with a backoff
  between: `/api/ai/transcribe` answered its 502 after a measured **116.1 s**
  against the 60 s ceiling. `RemoteTranscriber` now builds a per-call
  `deadline.Deadline` (the scripture endpoint's mechanism), and the same
  failure answers in **57.0 s**. The retry that buys a recovery from a
  restarting server is untouched — a fast failure leaves the budget intact.
  `/api/ai/question` got the same explicit budget: it held its 20 s only
  because its client is built with `attempts=1`.
- Live acceptance (throwaway container from the new image, `small`, this
  8-core host): four excerpts (2 ru, 1 uk, 1 en, 27-53 s) answered `200` in
  1.2-2.5 s; API process 859 MB RSS with the weights, **68 MB** on the remote
  provider.
- Live acceptance of the **remote** provider against the real company server
  (review, throwaway instance on 127.0.0.1:9097, `GEMINI_API_KEY` empty):
  the same four excerpts answered `200` in 3.9-8.4 s with `large-v3-turbo`.
  It also pins the production wiring: the shared `AI_OPENAI_COMPAT_ENDPOINT`
  pointed at the **chat** URL (`https://llm.ai2.ru/v1`, which has no
  `/audio/transcriptions`) while `AI_TRANSCRIBE_ENDPOINT` pointed at
  `https://llm.ai2.ru/whisper/v1` — a `200` is only possible if the
  per-stage override is the one used. (Measured through the SSH tunnel,
  port 8443, the pre-allow-list path; same host, direct today.)
- Tests: `tests/test_transcription.py` (44 + 1 gated on
  `AI_TRANSCRIBE_MODEL_PATH_UNDER_TEST=/models/whisper/small`, which is the
  only one that touches real weights) and the provider-seam and no-Google
  tripwire tests in `tests/test_twinkler_ai.py`.

RAG / scripture selection:

- `EMBEDDING_MODEL` and `EMBEDDING_DIMENSIONS` configure the vector index —
  required as a pair in every deployment; changing them changes the index
  version and requires `python app/index_cli.py rebuild`. On the Gemini
  provider `index_cli rebuild` also refuses to run without `GEMINI_API_KEY`
  (it could delete rows before discovering it cannot embed anything); on the
  local provider it refuses just as early if the weights cannot be loaded.
- `AI_SCRIPTURE_REWRITE_MODEL` (`gemini-3.7-flash`) — LLM query
  reformulation, the dominant quality lever; value pinned by the benchmark,
  deliberately independent of `AI_QUESTION_MODEL`, and required (ADR 0004).
- `AI_SCRIPTURE_REWRITE_API_KEY` (optional, no default) — API key for the
  **rewrite stage only**. Set: rewrites bill this key; unset or blank:
  rewrites bill `GEMINI_API_KEY` (one shared key — the previous behaviour and
  an operational default, not a hidden fallback: the *configured* behaviour
  is identical either way — same model, same prompt, same request — what
  differs is the quota and the bill, and therefore how often the stage is
  available rather than rejected with 429). Reason for the split: rewrite is
  pinned to `gemini-3.7-flash`, whose free daily quota the retrieval traffic
  exhausts, while embeddings and the rerank run on free lite-model quotas —
  so only this stage needs a paid key. Embeddings, `passage_rerank` and
  Twinkler keep reading `GEMINI_API_KEY`. Setting it while `GEMINI_API_KEY`
  is empty is a startup error (it would pay for one stage of a pipeline
  whose other stages have no key at all). Resolved once in
  `config.resolve_rewrite_api_key` → `config.REWRITE_API_KEY`, which is the
  default argument of `GeminiQueryRewriter`.
- `AI_SCRIPTURE_RERANK_MODEL` (`gemini-3.5-flash-lite`) — grounded choice
  of the final passage among candidates; value pinned by the benchmark,
  required (ADR 0005).
- `AI_SCRIPTURE_REQUESTS_PER_MINUTE` (10),
  `AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE` (3) — the selection
  endpoint's own rate-limit budget (separate from the Twinkler one; it reuses
  `AI_CLIENT_HMAC_KEY` for pseudonyms).
- `AI_SCRIPTURE_TIMEOUT_SECONDS` (15) — total budget of one selection;
  `AI_SCRIPTURE_INDEX_CACHE_SECONDS` (3600) — TTL of the in-process corpus cache,
  also dropped by `POST /api/cache/clear` (ADR 0006).
Trusted reverse proxies — **name the proxy, do not pin its address**
(`app/trusted_proxies.py`, ClickUp 86cbbq6vz):

- `TRUSTED_PROXY_HOSTS` (empty) — comma-separated **container / DNS names**
  of the reverse proxies whose `X-Forwarded-For` is believed, e.g.
  `bible-web`. Resolved through docker's embedded DNS at startup and
  re-resolved on a TTL, so an address change (reboot, resize, recreation) is
  picked up on its own. **This is the production setting.**
- `TRUSTED_PROXY_IPS` (empty) — literal addresses **and/or CIDR networks**
  (`127.0.0.1`, `172.18.0.0/16`). Still supported, still additive with the
  above. Networks are parsed strictly: `172.18.0.5/16` is a startup error,
  not a silent widening to the whole subnet.
- `TRUSTED_PROXY_DNS_TTL_SECONDS` (30) — how stale a resolved proxy address
  may get. One DNS call per TTL, never per request, and never **in** a
  request: the lookup runs on a background thread while the request path
  keeps answering from the previous snapshot. Floored at 1 s.
- Everything unset means "no proxy in front of this API": the peer address is
  the client and `X-Forwarded-For` is ignored. That is the local machine's
  configuration and a supported deployment, not a missing setting.
- Malformed entries abort startup naming the variable and the token, through
  the same aggregated `ConfigError` as everything else (a host name in
  `TRUSTED_PROXY_IPS` and an IP in `TRUSTED_PROXY_HOSTS` each point at the
  other variable).

- `AI_SCRIPTURE_PRIMARY_TRANSLATIONS` (empty) — per-language default translation
  of the selection endpoint, e.g. `ru=syn,en=bsb,uk=ubh` (`language=alias` or
  `language=code`, comma separated). Must name an INDEXED translation;
  entries that do not are ignored with a warning. Empty means the indexed
  translation with the lowest code (ADR 0007).

### Chapter coverage of a translation (ClickUp 86cbb2xxp, 2026-08-30)

`GET /api/translations/{code}/books` reports `chapters_count`,
`chapters_without_text` and (new) `has_text` per book. The expected structure
comes from **`app/canon.py`**, never from `translation_verses`:

- the base is the literal 66-book canon table (1189 chapters), verified
  against the three structurally complete translations in `cep_public`
  (`bsb`, `webus`, `webbe`);
- a translation that carries **more** than the canon keeps its own chapters:
  `chapters_count = max(canonical, this translation's last chapter)`, so
  `syn` still exposes Ps 151, Dan 13-14, 2 Chr 37 and `ubh` its Esth 11-12;
- a translation that divides the same text into **fewer** chapters needs an
  explicit entry in `TRANSLATION_CHAPTER_COUNTS` — today `("ubh", 39) = 3`
  (its Mal 3 holds canonical 3:1-18 + 4:1-6, so Mal 4 is not a hole);
- a book the translation declares in `translation_books` but ships **no text**
  for is still returned, with every canonical chapter in
  `chapters_without_text` and `has_text=false`. This is normal data, not an
  import failure: `npu` declares all 66 books and publishes the Psalms and
  the New Testament only (38 books without text, 779 chapters).

Reason for the table: the previous code took `SELECT max(chapter_number) FROM
translation_verses WHERE book_number = tb.book_number` — a subquery naming no
translation, so the maximum came from any translation in the shared table.
`bti` was reported to lack 26 chapters instead of 20 (the extras were
deuterocanonical chapters of `syn` and `ubh`), `syn` was reported to lack
Esth 11-12 and `ubh` to lack 2 Chr 37 and Ps 151. The same subquery fed
`chapters_count` in `app/excerpt.py`, which drives `prev_excerpt`/
`next_excerpt`: `bti` Ps 150 pointed at a non-existent Ps 151, `ubh` Mal 3 at
a non-existent Mal 4. `cep_public` cannot answer the question itself —
`bible_books` stores only `verse_count` — hence a reviewed constant in code.

`has_text` is the one additive field of the fix (default `true`); everything
else in `TranslationBookModel` is unchanged. Tests: `tests/test_translation_books.py`
(SQLite stand-in for `cep_public` holding several canons at once).

### Navigation across books without text (ClickUp 86cbbpc6v, 2026-08-30)

`prev_excerpt`/`next_excerpt` used to step into the neighbouring **book
number** whenever a chapter boundary was crossed, checking only its
`chapters_count`. For a translation that publishes part of the canon that
meant dead ends: `npu mat 1` offered `mal 4` and `npu psa 150` offered
`pro 1`, both answered 422 "No verses found".

Navigation now walks to the nearest book **that has text in this
translation** (`app/excerpt.py`, `get_adjacent_book_with_text`):
`npu psa 150` → `mat 1`, `npu mat 1` → `psa 150`. When no published book
remains on that side the field is empty, exactly as at the ends of the Bible
(`gen 1` prev, `rev 22` next) — so `npu psa 1` prev is `''`.

- "Has text" is not a second definition: `get_books_info` marks each book
  `has_text` from the `translation_max_chapter` it already selects, the same
  predicate `GET /api/translations/{code}/books` reports.
- **One query per boundary crossing**, whatever the distance: the book list
  is read once and searched in memory, so skipping npu's 38 text-less Old
  Testament books costs what stepping into the next book costs (fewer queries
  than before, in fact: the old path spent 2-3). No walk of one query per
  book. Guarded by a query-counting test.
- Translations that publish the whole canon are unaffected — verified by
  replaying every book boundary of every active translation against the
  pre-fix code on live `cep_public`: `syn`, `bti`, `bsb`, `webus`, `ubh`,
  `webbe` answer identically; only `npu` changes.
- `get_book_number`/`get_book_alias` are gone: the alias and number of both
  the current and the target book come from `get_books_info`, which the
  navigation already needs.

### Import: a full resync is a sequence of point imports (ClickUp 86cbbq5zp, 2026-08-30)

`GET /api/import` without parameters used to be one request: `GET /api/data`
returned the whole export as a single **147 MB** JSON document, `response.json()`
turned it into Python objects, and the importer then built insert tuples for
197 614 verses and 259 663 alignments — in the one-worker container that shares
the production VM with MySQL. On 2026-08-30 the VM stopped answering during a
full resync. Measured on the local machine before the fix: peak RSS of the
importing worker **972 MB** against 82 MB idle. Point imports
(`?translation=`, 18-29 MB) always passed.

So the full resync now *is* a sequence of point imports:

1. `GET /api/data/manifest` (Dashboard-API, new) — a few kilobytes: the
   reference tables in full, `code`+`alias` of every active translation, and
   the expected row count per table. This is the work list.
2. `languages` / `bible_books` — `REPLACE INTO`, one transaction.
3. every translation, **one at a time**: fetch its export, then
   `delete_translation_data` + `INSERT` inside **its own transaction**. Peak
   memory is set by the largest translation (`syn`, 29 MB), not by the corpus
   — on both sides, because the 147 MB document was materialised inside
   admin-api too, on the same VM.
4. translations `cep_public` still holds that admin-api no longer publishes —
   dropped, one transaction each, and named in the report. **Only with
   `?allow_removals=1`** (see below).
5. an orphan sweep (rows whose parent is gone) and stale reference rows.
   `TRUNCATE` used to remove these implicitly; a resync built out of point
   imports has to say it.
6. `SELECT COUNT(*)` against the manifest — per table **and per translation**,
   reported in the response.

**There is no `TRUNCATE` anywhere any more.** That was the second half of the
incident: `TRUNCATE` is DDL, it commits implicitly, so "truncate everything,
then insert" left every table empty the moment the process died, with nothing
to roll back. Now an abort at any point leaves every other translation exactly
as it was and the interrupted one at its previous contents. Proved two ways:
`tests/test_import_data.py` injects a failure at an exact statement against a
recording stand-in connection, and a live `kill -9` of the worker mid-resync
left `cep_public` complete (7 translations, 197 614 verses, 256 492
alignments).

Measured after the fix, same machine, same data: peak RSS **215 MB** (idle
87 MB), wall time 149 s (was 172 s — the per-translation queries are cheaper
on the admin side than one giant `IN`-list plus a 147 MB serialization).

Safety valve: `IMPORT_MAX_PAYLOAD_MB` (**48**) caps the body of **one**
`GET /api/data` response, enforced while streaming (`Content-Length` when the
server sends one, and always against the bytes received). A trip is a loud
`507` naming the variable, raised **before** that translation's rows are
touched — the fetch happens outside the transaction on purpose. 48 and not 96
(review of this ticket): the largest real payload is `syn` at 29.3 MB, parsing
costs several times the body in RSS, and on the 2-4 GB production VM that also
runs MySQL a 96 MB body could OOM-kill the worker *before* it answered 507 —
the very failure the valve replaces. And a manifest declaring **zero** active
translations is refused with `502`: an empty source is a broken source, never
an instruction to publish an empty Bible (the old code would have truncated
everything and inserted nothing).

**Verification is per translation, not only in total.** Global totals pass on
compensating errors — a hundred verses too many in one translation and a
hundred too few in another sum to the expected number, while a real
translation lost real text. The manifest carries `counts.per_translation`, so
each translation is counted in each of its seven tables and the
*disagreements* are reported in `translation_mismatches`
(`{alias: {table: {expected, actual, ok}}}`, empty when everything matched).
A manifest that lists a translation it has no counts for is a `502` before any
write: an unverifiable translation is a broken source.

**Removing a translation takes `?allow_removals=1`.** Step 4 above deletes
from `cep_public` every translation the manifest does not list — and a
translation goes missing from the manifest far more often by accident
(`active = 0` clicked in the dashboard, a filter bug on the admin side) than
by decision. A resync that is about to remove **at least one** translation
therefore removes **none** without the flag: it answers `200` with
`status="removals_rejected"`, the aliases in `removals_rejected`, and `detail`
naming them and the parameter. The import itself (step 2) is *not* undone —
the translations that were imported are correct and current, which is why this
is an honest report rather than an exception. Re-run with `?allow_removals=1`
to actually drop them; then it is the previous behaviour, with the aliases in
`translations_removed`. **Operationally:** a production resync answering
`removals_rejected` is the signal to check `cep_admin` before doing anything
else — never to add the flag reflexively.

Response contract of `/api/import` — additive, `status`/`translation`/`tables`
unchanged: `translations_imported`, `translations_removed`,
`removals_rejected`, `orphans_removed`, `verification`
(`{table: {expected, actual, ok}}`), `translation_mismatches`, `detail`,
`duration_seconds` and — since 2026-09-05 — `index` (the next section).
`status` is `"ok"` only when every count matches, per
table and per translation; `"removals_rejected"` when the gate above stopped a
removal; otherwise `"mismatch"`, with the data written and `verification` /
`translation_mismatches` saying what disagrees. Callers that tested
`status == "ok"` keep working and now catch more.

`?translation=` is unchanged in behaviour — same delete+insert, same
`REPLACE INTO` for the reference tables — and gains the size valve, one
explicit transaction, and a translation-scoped count check of **all seven**
tables it writes (it checked three until the review of this ticket, leaving
`voice_alignments` — the largest table, and the one the manual fixes are
delivered in — unverified). `allow_removals` is meaningless for a point import
and ignored there.

### The import carries the RAG index (ClickUp 86cbegwr9, 2026-09-05)

`translation_chunks`, `psalm_verse_mappings` and `chunk_embeddings` used to
reach production as a hand-made MySQL dump over an SSH tunnel, on a different
schedule from the text. That let production hold **new text under an index
built for the old text**, and nothing detected it. They now travel with
`GET /api/import` — both paths, `?translation=` included — and the guarantee
is stronger than "in the same request": a translation's index rows are written
**in the same transaction as its text**, so an interrupted import leaves that
translation whole, old text with old index.

- **Everything is fetched before the transaction opens.** The index of one
  translation is several pages of `GET /api/data/index` (Dashboard-API,
  86cbegwqg: the corpus in full on the first page, ~600 embeddings there and
  2000 afterwards, `vector` as base64 of the stored BLOB). Every page passes
  the `IMPORT_MAX_PAYLOAD_MB` valve, so an oversized page is a `507` **before**
  that translation is touched. The walk follows the source's `next_offset`,
  and an export that delivers fewer rows than it declared is a `502`.
- **The version is checked before the first write.** `want =
  c{CHUNKING_VERSION}:{EMBEDDING_MODEL}@{EMBEDDING_DIMENSIONS}`, from *this*
  deployment's `.env`. Not in the manifest's `index.available_versions` → `502`
  naming the version asked for, the versions that exist, and both variables.
  A source whose `chunking_version` differs from ours, or that reports several
  at once (`null` in the manifest, a half-finished rechunk), is also a `502`:
  `canonical_id` carries the chunking version, so that is another corpus, not
  an older copy of this one. A manifest with no `index` block at all (an older
  Dashboard-API) or with `index.error` set is named as such. **Never a silently
  empty index** — that reads exactly like a translation nobody has indexed.
- **`?drop_other_index_versions=1`** (default off, the shape of
  `?allow_removals=1` and `index_cli rebuild --drop-other-versions`): without
  it the `chunk_embeddings` rows of *other* versions survive every import, so
  a model migration keeps its rollback (an `.env` edit plus a restart). With
  it, they are deleted and counted per translation in
  `index.other_versions_removed`. The one place a routine import drops another
  version anyway is a translation being **removed** with `?allow_removals=1`:
  its index leaves with its text, every version of it. There is nothing to
  roll back to once the text is gone, and rows left there would be owned by
  nobody — no later import writes them (the translation is out of the
  manifest), `index_cli` rebuilds only what `translations` lists, and the
  orphan sweep does not reach the index tables.
- **Verified against the source, per translation:** the three row counts and
  the chunk-set digest from the manifest (`chunks_digest`, an
  order-independent `BIT_XOR` of per-chunk MD5s — **unsigned 64-bit**, syn =
  18030424974330788968, never to be read into a signed type; the importer runs
  admin-api's statement verbatim over its own `cep_public`), plus the number of
  embeddings whose chunk is missing, which must be zero. That last check is
  scoped to the embedding version this deployment reads, deliberately: the
  rows of an older version are kept precisely so that they can be rolled back
  to, and after a rechunk at the source they may well point at chunks that no
  longer exist — reporting them as orphans would fail every import until the
  old version is dropped, which is what `?drop_other_index_versions=1` is for.
  Disagreements join the text ones in `translation_mismatches` (`chunks_digest`
  and `chunk_embeddings_orphans` are reported there as `{expected, actual, ok}`
  like a count; `chunks_digest` is the one line whose `expected`/`actual` may
  be `null` — "no chunks at all", which `0` could not say, since a total XOR
  cancellation can genuinely produce zero), and `status="ok"` still means
  everything matched. A translation with chunks but **zero** embeddings of our
  version is a named mismatch even when the source declares the same zero; a
  translation with no chunks at all (`bti`, `npu`, `webbe`, `webus`) is normal.
- **A foreign `mapping_version` is a warning, not a refusal** — the one
  version disagreement that does not abort the import. `psalm_verse_mappings`
  carries its version in a column and `passage_highlight` selects by
  `VERSIFICATION_VERSION`, so a map of another version is imported as stored
  and simply not read; refusing the text over it would be a worse trade than
  the two versions coexisting. It is logged at `WARNING`, and the version that
  arrived is in the report as `index.mapping_version` (`null` when the source
  holds more than one). `chunking_version` is the opposite case and *is* a
  502: `canonical_id` carries it, so a foreign chunking version is a different
  corpus in the same table, not an unread column.
- **DDL is outside every transaction.** `CREATE TABLE IF NOT EXISTS` for the
  three tables, imported verbatim from `chunk_cli` / `versification_cli` /
  `vector_index`, so the first production import creates them; DDL commits
  implicitly and inside a transaction would split a translation's write.
- **The in-process index cache is dropped afterwards** — the same
  `scripture_select.clear_cached_resources()` that `POST /api/cache/clear`
  calls, in the same single worker — on `mismatch` as well as on `ok`, because
  the rows on disk changed either way. Reported as `index.index_cache_cleared`;
  `false` means a restart is needed before the new index is served.
- **Report:** the additive `index` block (`embedding_version`,
  `chunking_version`, `mapping_version`, `translations_indexed`, `tables`,
  `other_versions_removed`, `drop_other_index_versions`,
  `index_cache_cleared`). Everything else in the response is unchanged.
- **Order of operations, and it is not a preference** (`Deploy/runbook.md`):
  local `/api/import` → `chunk_cli` → `versification_cli` → `index_cli` →
  production `/api/import`. The index is built from the local `cep_public`, so
  one built *before* the local import describes text that no longer exists
  there — and that is what production would receive.

Measured on the local machine, 2026-09-05, against the local Dashboard-API
(gemini@768 index, 11 960 chunks / 17 490 Psalm mappings / 11 960 embeddings
on top of the 197 614 verses and 256 492 alignments): full resync **peak RSS
210 MiB** (215 232 kB from `/proc`, 212 MiB by `docker stats`), wall time
**159 s** — against 215 MB / 149 s for the text alone, i.e. the index costs
seconds and no measurable memory. The reason it is not more: one translation's
index is fetched, decoded and released like its text, and the vectors are
inserted 500 rows at a time (`INDEX_BATCH_SIZE` — 4 KB of BLOB per row would
make a 5000-row `executemany` a ~20 MB packet). A point import of `syn` (3963
chunks, 3963 embeddings, 2532 Psalm mappings) takes 53 s.

Tests: `tests/test_import_data.py` — atomicity (a failed embedding insert
rolls the text of that translation back), the `502` before any write for a
missing version / a foreign or mixed chunking version / no `index` block, the
`507` on an index page before the transaction, other-version rows kept without
the flag and deleted with it, the count / digest / orphan verification, the
cache drop, and the point-import path.

Proved live on 2026-09-05 as well: a `docker kill -9` fired while
`information_schema.innodb_trx` showed 34 762 rows modified in the open
transaction (mid-`syn`) left `syn`'s verses, chunks, Psalm map and embeddings
identical **down to their AUTO_INCREMENT codes** — the transaction rolled
back, it did not half-commit — every other translation intact, and the repeat
import answered `status="ok"`. The same run confirmed that a `@768` import
leaves the `@1024` rows at 11 960 and vice versa, that
`EMBEDDING_MODEL=nonexistent` answers `502` naming both variables and both
real versions with the three tables' counts unchanged, and that
`POST /api/ai/scripture` answers `source: rerank` right after an import in the
same process — no restart.

### The time budget is a ceiling now (ClickUp 86cbbnaxn, 2026-08-31)

`POST /api/ai/scripture` answered in 16.3 s, 21.4 s (production) and 13.9 s
against `AI_SCRIPTURE_TIMEOUT_SECONDS` = 15 while the free-tier provider was
returning 429. The budget object existed and was threaded through every
stage; four things let requests walk past it anyway.

1. **`timeout=<number>` is per phase in httpx**, not per request:
   `Timeout(connect=t, read=t, write=t, pool=t)`, each bounded separately.
   Handing it `remaining` authorised ~4x `remaining` for one call.
   `gemini_retry.provider_timeout` splits the budget across the four phases
   instead (a twelfth each for connect/write/pool, capped at 1 s; read keeps
   the remaining three quarters), so their worst case sums to the budget.
   The split is sized, not symmetric: `:generateContent` is not streamed, so
   the model's whole thinking time lands in the first **read**, and the
   rewrite call measures 3.7-4.7 s (ADR 0006) against the 8 s base — a
   half-and-half split would leave read 4.0 s and time out the median
   healthy request, buying the budget back with a permanent quality loss.
2. **The backoff was clipped to the budget and then slept.** The old
   `deadline.sleep_budget` returned `min(delay, remaining)`, so a Gemini
   `RetryInfo` of 30-55 s slept out every second the request had left and
   the attempt it waited for then had nothing to run in.
   `gemini_retry.retry_pause` returns `None` ("degrade now") when the pause
   plus a minimally useful call (1 s) no longer fit. `sleep_budget` is
   **deleted** rather than fixed — it invited exactly this misuse.
3. **Every 429 was treated alike.** Gemini's body says which quota
   rejected the call (`google.rpc.QuotaFailure`, `quotaId` such as
   `GenerateRequestsPerDayPerProjectPerModel-FreeTier`) and how long it
   wants us to wait (`google.rpc.RetryInfo.retryDelay`, e.g. `"14s"`). A
   per-**day** quota cannot reopen inside a 15 s request, so it now ends the
   ladder immediately; a per-**minute** one is retried after
   `max(our backoff, retryDelay)` when that still fits in the budget;
   anything unrecognised (or a 5xx) keeps the previous ladder. The delay
   alone does not distinguish them — the same daily violation answers
   `"0s"`, `"14s"` and `"55s"` — so the quota id is what is read. Parsing
   never raises: a body of another shape is "details unknown".
4. **The budget started too late.** `Deadline` was created just before the
   pipeline, so a cold corpus load (`get_resources`, a full index read, also
   on every TTL refresh) was outside it. It is now created at the top of the
   handler: the budget is the endpoint's promise about its own latency, and
   the stages get what is left.

Unchanged on purpose: the 15 s value, the attempt counts, the retry policy
for 5xx and for unrecognised 429s, and every `fallback_reason` — the stages
degrade through the same ladder, only sooner. Still outside the budget by
design: reading the chosen candidates' texts from MySQL and building the
response (local, bounded, and required for any answer at all). No new
environment variables: the three new numbers (the 1 s handshake ceiling, the
one-twelfth handshake share and the 1 s minimum attempt) are code constants
beside the existing `_PROVIDER_TIMEOUT_SECONDS`, not deployment knobs.

Tests: `tests/test_gemini_retry.py` (the 429 shapes, including malformed
ones, and the pause planner) and `tests/test_deadline.py` (per-stage and
whole-pipeline wall-clock ceilings against a provider that burns every phase
of every timeout, on a fake clock — no real waiting). Details and the
verbatim 429 body: `architect/scripture-select.md`, "Time budget".

### Trusted proxies survive a reboot, and stop failing silently (ClickUp 86cbbq6vz, 2026-08-30)

`TRUSTED_PROXY_IPS` used to be one hardcoded container address. Docker hands
addresses out in start order and they do **not** survive a VM reboot or
resize: after the 2026-08-30 hard reboot the production containers came back
in a different order, `172.18.0.5` became MySQL instead of nginx, and
Bible-API silently stopped believing `X-Forwarded-For` from the only proxy in
front of it. Measured consequences: `api_requests` recorded every caller under
the nginx container's address (one client, statistically), and
`AI_REQUESTS_PER_CLIENT_PER_MINUTE` became a **global** limit, because every
caller hashed to the same pseudonym. **No errors in the logs** — the failure
was completely silent, and the fix was a manual one-line `.env` edit after
every reboot.

**Trust a name, not an address.** `app/trusted_proxies.py` owns a
`TrustedProxies` object built from the three variables above and answers one
question: `is_trusted(peer)`. Names in `TRUSTED_PROXY_HOSTS` are resolved
through docker's embedded DNS at startup and re-resolved whenever the cached
answer is older than `TRUSTED_PROXY_DNS_TTL_SECONDS`, so the address the proxy
holds *right now* is the address that is trusted — a reboot costs at most one
TTL of misattributed statistics and **no `.env` edit at all**.

**Resolution is stale-while-revalidate, and that is correctness, not speed.**
`is_trusted` is called in the request path of an async application and
`socket.getaddrinfo` is a blocking call: a name that falls outside docker's
DNS and reaches an unresponsive upstream would park the **whole event loop**
— every request in flight, not just this one — for the length of the lookup,
once per TTL. So the request path never resolves. It reads the current
snapshot and returns; when that snapshot is older than the TTL it hands the
work to a single background daemon thread (at most one in flight, whatever
the request rate) whose result atomically replaces the snapshot. Trust
follows a moved proxy within one TTL plus one lookup, and a hung resolver
costs staleness only. The one synchronous resolution is the cold one, at
startup, in `log_startup_state()` — there is no previous snapshot to serve
then, and answering "trust nobody" while a lookup is pending is the failure
this module exists to prevent.

Why not the alternatives (both were considered, both remain possible):

- **Static IPs in the prod compose (`ipam`)** — makes the address a declared
  fact rather than an accident, and would also fix nginx's cached upstreams.
  But it is a prod-compose change that has to be kept in sync by hand, it
  pins the subnet layout, and above all it keeps the failure *silent*: the
  day the declaration and reality disagree, nothing says so. Not chosen as
  the primary mechanism; it composes with this one if it is ever wanted.
- **Trusting the whole docker subnet** (`TRUSTED_PROXY_IPS=172.18.0.0/16`) —
  supported by the CIDR parsing, and it does survive reboots. Honest
  downside: it trusts *every* container on that network (MySQL, admin-api,
  anything added later), so a single compromised container could forge
  `X-Forwarded-For` and both poison the statistics and evade the per-client
  AI rate limit by minting a new client address per request. A name resolves
  to the proxy alone and costs nothing extra, so the subnet is documented as
  an escape hatch, not the recommendation.

**And the failure is loud now.** Four places, none of them per-request:

1. **Startup banner**, always: `Trusted proxies: hosts=[bible-web->172.18.0.4]`
   or `Trusted proxies: none configured — ...`. It is emitted through a
   handler this module installs when nothing else has configured logging:
   uvicorn leaves the root logger without handlers, so an application `INFO`
   record would otherwise be swallowed and only `WARNING`+ would reach
   `docker logs` (which is why the banner is worth grepping for after a
   deploy: `docker logs bible-api | grep 'Trusted prox'`).
2. **Address change**, `WARNING`:
   `Trusted proxy host 'bible-web' changed address: 172.18.0.5 -> 172.18.0.4`.
   The reboot, narrated.
3. **A host that stops resolving**, `ERROR`, once per state change (not once
   per TTL — it keeps retrying quietly and shouts again only when the state
   changes).
4. **`X-Forwarded-For` from an untrusted peer**, `ERROR`, once per peer per
   5 minutes (`client_ip.FORWARDING_LOG_INTERVAL_SECONDS`, table capped at 64
   peers, oldest evicted). That header from an unexpected peer *is* the
   signature of the incident. With no proxy configured at all the same event
   is a `WARNING` instead — locally it means someone sent a forged header to a
   directly exposed API, which is information, not a misconfiguration.
   Because the trigger is a request header, the *volume* of this log would
   otherwise be chosen by whoever sends the requests: a second, global ceiling
   of **10 lines per 5 minutes** (`client_ip._MAX_REPORTS_PER_INTERVAL`)
   caps it however many distinct peers appear. Peers silenced by that ceiling
   are not recorded as "already reported", so the real misconfigured proxy is
   reported again as soon as there is budget.

**Which element of `X-Forwarded-For` is the client: the RIGHTMOST one.**
Production nginx sets `X-Forwarded-For $proxy_add_x_forwarded_for` in every
`location`, and that variable **keeps the header the client sent** and appends
`$remote_addr` to it. Reading the leftmost element (as the first cut of this
ticket did) therefore let any caller simply *declare* its address: `curl -H
'X-Forwarded-For: 1.2.3.4'` was recorded as 1.2.3.4 in `api_requests`, and a
fresh value per request walked straight past
`AI_REQUESTS_PER_CLIENT_PER_MINUTE`, whose bucket is keyed by that address.
Only the part *our own* proxies appended can be believed.

`client_ip.client_from_forwarded` implements the standard rule: walk the list
right to left, skip addresses that are themselves trusted proxies, and the
first address that is not one is the client. With today's single hop that is
just the rightmost element, but written as a walk it stays correct if a second
trusted hop is ever added. A malformed or empty element (nginx never produces
one) means the header cannot be read as a hop chain, so it is not read at all
and the direct peer is the client — the same conservative answer as an
untrusted peer.

**Pseudonyms change.** Requests whose forged left element used to be recorded
now hash from the address nginx actually saw, so `api_requests.client` values
differ before and after this change for any caller that sent its own
`X-Forwarded-For`. That is the point of the fix; a report crossing the deploy
should not treat the two as the same client identity.

Startup is deliberately **not** aborted when a configured host does not
resolve: the proxy may simply be slower to come up, and taking the whole
public API down over statistics accuracy is the wrong trade. The error is
logged and every TTL retries.

Tests: `tests/test_trusted_proxies.py` (64) — parsing and its startup errors,
matching by address/CIDR/name, the reboot itself (a fake resolver whose answer
changes, trust follows within one TTL), each log line firing exactly once, the
spoofing cases (`X-Forwarded-For` from an untrusted peer is ignored and the
peer is the client; a forged left element is ignored and the right one wins),
the request path answering instantly while a deliberately hung resolver is
blocked on a background thread, and the two bounds on the untrusted-peer log
(oldest peer evicted, global ceiling per interval).

### All API routes are under `/api` prefix

### AI routes renamed on 2026-08-30 (ClickUp 86cbbmwjk)

`POST /api/twinkler/v1/complete` → `POST /api/ai/question`,
`POST /api/twinkler/v1/transcribe` → `POST /api/ai/transcribe`,
`POST /api/scripture/v1/select` → `POST /api/ai/scripture`. Paths only:
bodies, responses, headers, authentication and rate limits are unchanged.
The old paths return 404 — there are no aliases, because the only client is
one unpublished app (Twinkler-Mobile, renamed in a paired ticket).
`GET /api/scripture/v1/translations` was deleted outright: the app chooses
its translation once and the selection renders any accepted code (ADR 0007),
so the catalogue had no consumer.

Everything the client sees in `/docs` was renamed with the paths: the three
methods share one tag **`AI`** (the `Twinkler` and `Scripture` tags are
gone), and their `operationId`s are `ai_question`, `ai_transcribe` and
`ai_scripture`. The word "twinkler" no longer appears anywhere in the
generated spec. No generated clients depended on the old IDs — the mobile
client is handwritten.

Deliberately NOT renamed (out of scope): the modules `twinkler_ai.py` and
`scripture_select.py` and the handler functions — these are internal and
invisible to API consumers. The environment variables were renamed the
following day (below).

### Env variables renamed on 2026-08-30 (ClickUp 86cbbmy8d)

Names now mirror the method they configure: `AI_*` for the whole AI surface,
`AI_SCRIPTURE_*` for the selection pipeline only.

| Old | New |
| --- | --- |
| `GEMINI_MODEL` | `AI_QUESTION_MODEL` |
| `GEMINI_TRANSCRIPTION_MODEL` | `AI_TRANSCRIBE_MODEL` |
| `GEMINI_REQUESTS_PER_MINUTE` | `AI_REQUESTS_PER_MINUTE` |
| `GEMINI_REQUESTS_PER_CLIENT_PER_MINUTE` | `AI_REQUESTS_PER_CLIENT_PER_MINUTE` |
| `TWINKLER_CLIENT_HMAC_KEY` | `AI_CLIENT_HMAC_KEY` |
| `RETRIEVAL_REWRITE_MODEL` | `AI_SCRIPTURE_REWRITE_MODEL` |
| `RETRIEVAL_RERANK_MODEL` | `AI_SCRIPTURE_RERANK_MODEL` |
| `RETRIEVAL_REWRITE_API_KEY` | `AI_SCRIPTURE_REWRITE_API_KEY` |
| `SCRIPTURE_SELECT_REQUESTS_PER_MINUTE` | `AI_SCRIPTURE_REQUESTS_PER_MINUTE` |
| `SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE` | `AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE` |
| `SCRIPTURE_SELECT_TIMEOUT_SECONDS` | `AI_SCRIPTURE_TIMEOUT_SECONDS` |
| `SCRIPTURE_INDEX_CACHE_SECONDS` | `AI_SCRIPTURE_INDEX_CACHE_SECONDS` |
| `SCRIPTURE_PRIMARY_TRANSLATIONS` | `AI_SCRIPTURE_PRIMARY_TRANSLATIONS` |
| `MP3_FILES_PATH` | `AUDIO_FILES_PATH` |
| `TWINKLER_SYSTEM_PROMPT` | **deleted** — now `app/question_prompt.py` |

Unchanged: `API_KEY`, `DB_*`, `ADMIN_API_URL/KEY`, `GEMINI_API_KEY`,
`EMBEDDING_MODEL`, `EMBEDDING_DIMENSIONS`, `TRUSTED_PROXY_IPS`,
`AUDIO_BASE_URL`, `AUDIO_DIR` (docker-compose host path), `OPENROUTER_API_KEY`
(benchmark only).

Values were **not** touched, only keys — in particular the HMAC key, so
client pseudonyms in `api_requests` stay stable across the rename (the digest
is keyed by the value; the variable name never enters it). There are no
aliases for the old names on purpose: a place the rename missed aborts
startup naming the variable it wants, which is ADR 0008's aggregated error
doing its job. Production `.env` is rebuilt with the new names at deploy time
(checklist 86cb8wdp4), not by this change.

**`TWINKLER_SYSTEM_PROMPT` is gone.** The system prompt of
`/api/ai/question` is versioned code in `app/question_prompt.py`
(`QUESTION_PROMPT_VERSION`), carried over from the local `.env` byte for byte
as v1. A prompt is product behaviour, not a
deployment knob: as a variable it could silently differ between local and
production and every test run had to inject a stand-in. It is public from
this date (public repository, owner-approved: never a secret, no key
material). The old runtime guards "prompt is not configured" / "too long"
were removed from `complete()` — a reviewed literal cannot violate them —
and their invariants are asserted in `tests/test_twinkler_ai.py` instead.

**`QUESTION_PROMPT_VERSION = 2` since 2026-09-05** (ClickUp 86cbegg3f): the
constant became `QUESTION_PROMPT_TEMPLATE` + `build_question_prompt(language)`
with one placeholder — the language to answer in, resolved per request by
`safety.detect_language` (the detector the despair rule already runs) and
named twice, including as the last sentence. v2 also bans naming a feeling
the person did not name and asking for facts instead of an open question, and
drops the despair sentence (that rule is `app/safety.py`). Measured: Qwen's
language violations 6/81 → 0/81, interpretations 5/81 → 0/81, clean answers
65/81 → 81/81, and Gemini 75/81 → 81/81 on the same prompt
(`evaluation/README.md`, "Промпт наводящего вопроса v2").

**`QUESTION_PROMPT_VERSION = 3`, the same day** (ClickUp 86cbegmzz): the
request became `topic` + `stage` + `messages` and the stage instructions
became the server's (`build_user_message`), so the prompt lost its one
sentence about the incoming layout and kept everything else byte for byte.
Measured the next day: 96 clean answers of 99 on Qwen3-30B, language 0/99.

**`QUESTION_PROMPT_VERSION = 4` since 2026-09-06** (ClickUp 86cbehyf8, bug
86cbehtkh): the first revision **chosen between candidate wordings** rather
than written against a named violation. Two edits. The `next` instruction no
longer asks for a question that «смотрит на ситуацию с другой стороны» — the
model read that as permission to contest what the person said and answered the
journal case with the same contestation six replacements running; it now asks
to develop the person's last answer, not to restate an earlier question's
thought, and not to doubt what they said unless they doubted it themselves. And
one sentence of the system prompt takes the person's grammatical gender and
number from their own words, never defaulting to the masculine. Measured on
Qwen3-30B against the v3 baseline (`series-scale-ru`, 6 samples, identical
body): distinct openings 0.17 → 0.33, mean max-similarity 0.98 → 0.93, verbatim
duplicate pairs 11 → 5; the Ukrainian series' gender mismatch **30/30 → 0/30**;
probes and scenarios 96/99 → **99/99** clean, language 0/99. Rejected in the
same run, and that is a result too: both rewordings of the `skipped_questions`
block (one of them sent the question off to an invented third party) and Qwen's
own `top_p=0.8`/`top_k=20`. The loop is weakened, not closed — with an
identical body the model cannot know what it already offered; the ADR 0015
field is what breaks it, and the repetition filter is 86cbehyg0. Candidate
texts and the table: `evaluation/question_prompts.py`,
`evaluation/README.md` «Промпт наводящего вопроса v4».

**`QUESTION_PROMPT_VERSION = 6` since 2026-09-06** (ClickUp 86cbejvt2, umbrella
86cbejvq1) — **on the branch, not measured**: whether it beats v5 on Qwen3-30B
is the next step of the umbrella. The first revision that changes the *contract*
rather than the wording, because the wording levers are spent: the independent
assessment of v5 (86cbejtt2, `FABLE_ASSESSMENT.md`, 396 answers read by hand)
found Qwen addressing a woman in the masculine in **15 answers of 99**, six
verbatim duplicates inside one Ukrainian replacement series, one subject
reworded through a whole series, «X или Y» menus and hidden advice — all of them
already forbidden in v5's words. So v6: the model returns
`{"subject": …, "question": …}` (parsed by `app/question_format.py`, one extra
generation when it cannot be read at all — logged as
`question format: parsed=json|repaired|regex|retry_ok|retry_failed|raw` — the
`response_format: json_object` the openai_compat transport now asks for, and the
additive `QuestionResponse.subject`); the `next` message names **which of the goal's five
angles** this step is about, rotating by `len(skipped_questions)`; the person's
**grammatical gender is computed in code** (`app/person_gender.py`) and stated,
and the prompt's «рада»/«рад» examples are gone; the **subjects already used**
are listed from the server's own `SubjectMemory`, the client contract untouched;
and a dash-tail and a self-named two-option menu are forbidden by name. Each
prompt also gains two worked examples from a secular domain. **The criterion is
depth** (Maria, 2026-09-06) — a question worth stopping over — and nothing in
the handler rejects an answer for its gender, menus or dashes: the only
regenerations are the one format retry and the novelty retry of ADR 0016, both
inside the one request budget. `architect/adr/0017-structured-question-response.md`,
`architect/twinkler-ai.md` «v6»; the stand's variant is `--prompt-variant v6`.

### `POST /api/ai/question` takes topic + stage + messages (ClickUp 86cbegmzz, 2026-09-05)

`{"topic": "…", "stage": "first|next|reflect", "messages": [{"role":
"assistant|user", "text": "…"}]}`; the response is unchanged. The old `user`
string is gone with **no** transitional support (both ends changed at once and
the app is unpublished), and a body carrying `user` or `last_user_message`
answers `422` naming the field and what to send instead. Limits mirror the
client's: topic ≤ 2000, ≤ 40 messages, topic + texts ≤ 16 000. Two shape rules
are `422` as well: `first` takes no history, and a non-empty history must end
with a `user` turn. The history may *start* with a `user` turn (the client
trims the old head whole), and `messages: []` with `next`/`reflect` is normal
— no answers, or the person forbade sending them.

Two parts of the request are read by rule, on purpose: the model gets the
whole assembled message; **both tiers** of the despair rule (since Maria's
2026-09-05 decision) get the last `user` turn (the topic at `first`, and
*nothing* at `next`/`reflect` with no history — the topic is never
substituted for a reply, or one phrase would answer every later question of
that prayer with the fixed reply, which is the bug this ticket closed). A
despair phrase in an **older** turn is therefore no longer refused by tier 2
either — the request is split into turns precisely so this rule looks at the
last one, and the companion must not keep answering the fixed text for the
rest of the prayer. Tier 2's own fixed reply still needs a language, and that
is resolved separately, the same way the prompt's is: the last `user` turn,
falling back to the topic, then to their earlier replies newest first, then to
the last `assistant` turn, then to English — a chain walked by *decidability*
rather than presence, since `detect_language` returns `None` for a line like
«Помоги» that does not say which language it is and the same person usually
said it elsewhere in the same request (undetermined on the evaluation set: 9
of 33 → 6). The blocks the server assembles are Russian whatever the prayer's
language is — that is what the client always sent — which is exactly why the
language is resolved separately: our own wrapper must not outvote an English
prayer. Verbatim blocks, the 422 wordings and the reasoning:
`architect/twinkler-ai.md`.

**`skipped_questions` — the "replace question" button (ClickUp 86cbehyfe,
2026-09-05).** Optional fourth field, `[string]`, default `[]`: the questions
already shown to the person and left unanswered, chronological, all of the
current prayer and **never one that is already in `messages`** (an answered
question is an `assistant` turn, a replaced one is a skipped question — the two
lists never overlap). Before it, "replace" resent an identical body, so the
model could not know its question had been declined and looped on the same
thought. Limits: ≤ 10 entries, ≤ 300 characters each after stripping, counted
with `topic` and the turns against the same 16 000 total; blank entries are
**dropped, not refused** (our own text coming back to us — unlike a `messages`
turn, which is the person's words and must be non-empty); a non-empty list with
`first` is a `422`. Rendered at `next` only — one block, «Человек попросил
другой вопрос вместо этих:», plus one sentence of the instruction asking for
another *direction* — and accepted-but-not-rendered at `reflect`, which by the
86cbegmzz contract never shows our questions at all. It states what the person
**did**, never that they disagreed with the thought: pressing "replace" is not
an argument. The field moved no version — a request without it renders
byte-identical bytes — and its minimal wording **survived the v4 prompt
work by measurement** (86cbehyf8: both rewordings tried were worse); the
repetition filter is 86cbehyg0. The field
reaches the **model and nothing else**: it votes on neither the answer's
language nor either tier of the despair rule, because it is our own generated
text in a Russian block whatever language the prayer is in
(`architect/adr/0015-skipped-questions-in-question-request.md`).

**`novel` — the answer is checked against what was already shown (ClickUp
86cbehyg0, 2026-09-06).** Telling the model which questions were declined does
not stop it offering them again: the replacement-series baseline has Qwen
answering six "replace" presses with six variants of one sentence, the last
five differing only in the tail. So the handler compares the generated text
with the `assistant` turns plus `skipped_questions` (`app/question_novelty.py`
— normalize + character-trigram Jaccard ≥ **0.60**, or a shared opening of ≥ 4
words covering ≥ 0.7 of the shorter question; the same metric
`evaluation/check_questions.py` reports for those series, pinned by a test).
On a repeat it generates **exactly once more**, with the rejected question
appended to the skipped list for that call only, and never a third time.

The response gains an additive `novel: bool` (`QuestionResponse`, a subclass —
`/api/ai/transcribe` keeps the plain `CompleteResponse`). `true` = the text
returned repeats nothing shown, including when there was nothing to compare
with and when a safety tier replaced it; `false` = it repeats something and the
second generation repeated too, was unaffordable, or failed. A `false` answer
still carries the best text obtained (the less similar of the two) — a repeat
is never `true`, an answer is never withheld, and a failing *second* generation
is never a `502`. The whole request, both generations, runs under **one**
`Deadline` of `AI_QUESTION_TIMEOUT_SECONDS`; the second starts only with
`MIN_SECOND_ATTEMPT_SECONDS` (3.0) left. That budget also reached the Gemini
transport, which handed httpx a bare number until now — per *phase*, so a 20 s
ceiling authorised 80 s for one call. Both despair tiers are untouched: tier 1
before any call, tier 2 on **every** reply including the second. One `INFO`
line per answered request carries the fact and no text:
`question novelty: attempts=2 repeat=near score=0.78 novel=false stage=next`.
It is a lexical check and **not** a measure of semantic diversity — a reworded
return to the same thought passes; that is 86cbehyg8
(`architect/adr/0016-question-novelty-check.md`).

**State on 2026-09-06 (86cbehxm2, bug 86cbehtkh).** What is shipped: prompt
**v4**, the `skipped_questions` field (ADR 0015), the lexical novelty check
with one extra generation and the additive `novel` field (ADR 0016), all under
one request budget. What was measured and deliberately **not** shipped, with
nothing in `app/` changed by either: N candidates per call (86cbehyg4 — worse,
because the retry is sent different bytes while N candidates share one input)
and the bge-m3 semantic check (86cbehyg8 — better than the lexical filter,
+0.5-0.6 s median, threshold 0.78-0.80 recommended, **Maria decides**). Verified
live on the local 9084 on 2026-09-06 (86cbehygb): 25 answered requests over four
replacement series, 200 everywhere, 0.3-1.1 s, three `novel: false`, one retry
that escaped the repeat — and the one finding of that check, that the endpoint's
`question novelty:` `INFO` line never reached `docker logs` until `main.py` was
given `ensure_visible_handler(logging.getLogger("twinkler_ai"))`. Protocol:
`evaluation/bench_data/live_9084_2026-09-06.md`; before/after transcripts for
Maria: `evaluation/bench_data/before_after_2026-09-06.md`; the measurement
sections are indexed at the top of `evaluation/README.md`'s question block. The
**client contract** (accumulate every replaced question, do not duplicate
`messages`, what to do on `novel: false`) is in `architect/twinkler-ai.md` and
in the two ADRs.

The request statistics store the path verbatim, so `api_requests` and
`api_request_daily_stats` carry the old names before the rename and the new
ones after it; the rows were not rewritten. A report crossing 2026-08-30
must union both names (see `architect/scripture-select.md`).
