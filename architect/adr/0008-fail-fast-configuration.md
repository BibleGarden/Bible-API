# ADR 0008: Fail-fast configuration validation

Status: accepted (2026-08-29).
Ticket: none — owner-directed policy change following the incident below.

> Note (2026-08-30, ClickUp 86cbbmwjk): the AI routes named below were
> renamed — `/api/twinkler/v1/complete` → `/api/ai/question`,
> `/api/twinkler/v1/transcribe` → `/api/ai/transcribe`,
> `/api/scripture/v1/select` → `/api/ai/scripture`. Old path names are kept
> in the historical text.

> Note (2026-08-30, ClickUp 86cbbmy8d): the AI environment variables were
> renamed to mirror the method each one configures (`GEMINI_MODEL` →
> `AI_QUESTION_MODEL`, `GEMINI_TRANSCRIPTION_MODEL` → `AI_TRANSCRIBE_MODEL`,
> `RETRIEVAL_*`/`SCRIPTURE_SELECT_*`/`SCRIPTURE_*` → `AI_SCRIPTURE_*`,
> `GEMINI_REQUESTS_PER_*` → `AI_REQUESTS_PER_*`, `TWINKLER_CLIENT_HMAC_KEY` →
> `AI_CLIENT_HMAC_KEY`, `MP3_FILES_PATH` → `AUDIO_FILES_PATH`). **The rules
> and the three classes below are unchanged; only the names in them are new**,
> and the incident that produced this ADR was reported against the former
> `RETRIEVAL_REWRITE_MODEL`. Deliberately, no old name is accepted as an
> alias — under this ADR that is a feature, not an omission: any place the
> rename missed aborts the start naming the variable it wants, which is the
> aggregated error this document specifies.
>
> One variable left the classification entirely rather than being renamed:
> `TWINKLER_SYSTEM_PROMPT`, previously an operational parameter with an empty
> default. A prompt is product behaviour, so an *environment* default for it
> was the very thing this ADR objects to — an unset value silently produced a
> different answer (a 502 instead of a reply), and two deployments could run
> different prompts with identical, valid-looking configuration. It is now the
> reviewed, versioned code constant `question_prompt.QUESTION_PROMPT`. Moving
> it out of the environment removes a whole class of misconfiguration instead
> of validating it: see `architect/twinkler-ai.md`.

## Context

Incident, 2026-08-29: `.env` set `AI_QUESTION_MODEL=gemini-3.5-flash-lite`, and
the owner believed the whole application ran on that model. The retrieval
rewrite stage (ADR 0004) reads a separate variable,
`AI_SCRIPTURE_REWRITE_MODEL`, which was unset; `app/config.py` defaulted it in
code to `gemini-3.7-flash`. The key could not reach that model, so the
rewrite stage started failing while `AI_QUESTION_MODEL`-driven calls (Twinkler)
kept working on flash-lite. Nothing in the deployment declared "the rewrite
stage runs on a different model than you think" — the degradation had to be
found by reading code, not configuration.

The general failure is not this one variable. A code-level default for a
setting that changes *behaviour* — which model answers a request, which
database is written to — turns a missing environment variable into a silent
choice the deployer never made and cannot see by reading `.env`. The owner's
rule going forward: **no fallback may hide a configuration problem.** A
fallback is fine when it names the one behaviour the service intentionally
runs with when a value is absent (an operational default); it is not fine
when it silently substitutes a different, unreviewed behaviour for a value
that was supposed to be set.

## Decision

### Validate everything at import, fail with one aggregated error

`app/config.py` validates the environment as it is imported (`_validate()`,
called at module load). If anything is wrong, the module raises `ConfigError`
— a `RuntimeError` subclass, so it also aborts application startup — with
every problem it found, not just the first:

```
Invalid configuration (3 problems):
  - AI_SCRIPTURE_REWRITE_MODEL is required when GEMINI_API_KEY is set (no default: the model must be named explicitly)
  - EMBEDDING_DIMENSIONS is required
  - AI_REQUESTS_PER_MINUTE: expected an integer, got 'many'
```

One error listing everything means a broken deployment is fixed in a single
edit-and-restart cycle instead of one variable discovered per restart.

### Three classes of variable

- **`ALWAYS_REQUIRED_VARS`** — `API_KEY`, `DB_HOST`, `DB_USER`, `DB_NAME`,
  `EMBEDDING_MODEL`, `EMBEDDING_DIMENSIONS`. Must be set and non-blank in
  every deployment; a blank value counts as unset. `localhost` / `root` /
  `cep_public` were exactly the kind of guessable-but-wrong defaults this
  rule exists to remove — a misconfigured deployment used to silently point
  at whatever database happened to answer.
- **`PRESENCE_REQUIRED_VARS`** — `DB_PASSWORD`. Must be *present* in the
  environment, but is allowed to be empty: MySQL accepts a passwordless
  user, so `DB_PASSWORD=` is a legitimate, explicit statement. A variable
  that is simply absent is the silence this ADR forbids; both the local and
  the production `.env` set a real password today regardless.
- **`AI_REQUIRED_VARS`** — `AI_QUESTION_MODEL`, `AI_TRANSCRIBE_MODEL`,
  `AI_SCRIPTURE_REWRITE_MODEL`, `AI_SCRIPTURE_RERANK_MODEL`. Required only when
  `GEMINI_API_KEY` is set. Without a key the whole AI surface is "not
  configured": the AI endpoints already answer with their own error and the
  rest of the API must keep working, so demanding these model names would
  turn a supported deployment (Bible API without AI) into a startup
  failure. With a key, every model these calls can reach must be spelled
  out — none of them defaults in code, because a default here is exactly
  what hid the 2026-08-29 model mismatch.

  `EMBEDDING_MODEL` / `EMBEDDING_DIMENSIONS` are deliberately **not** in
  this conditional class, even though they also name a Gemini concept. They
  do not name a live provider call; they name the vector index this service
  *reads* (`c{chunking}:{model}@{dims}`, ADR 0002) and are required in
  `ALWAYS_REQUIRED_VARS`. The documented no-AI contract of
  `POST /api/scripture/v1/select` is a 200 from the safe pool with
  `fallback_reason=ai_unavailable` (ADR 0004/0006), and even that answer is
  resolved through the loaded corpus — making the pair conditional on the
  key would silently address the version `c3:@0`, an index nobody ever
  wrote, and turn the documented 200 into a 503 ("vector index is empty").
  That is the same class of bug this whole ADR exists to prevent, just one
  hop removed from a provider call.

### Addendum (2026-08-29): `AI_SCRIPTURE_REWRITE_API_KEY`

The retrieval rewrite stage may bill its own key
(`AI_SCRIPTURE_REWRITE_API_KEY`, optional; ADR 0004): it is pinned to
gemini-3.7-flash, whose free daily quota the traffic exhausts, while the
embedding and rerank stages live on free lite-model quotas. Unset or blank
means the stage uses `GEMINI_API_KEY` — the behaviour every deployment had
before the variable existed.

That fallback is deliberately allowed under this ADR, because it does not
hide a configuration problem: the absent value has exactly one intended
meaning ("one key pays for everything"), and it selects no unreviewed
behaviour — the *configured* behaviour is identical either way (same model,
same prompt, byte-identical request); what differs is the quota and the
invoice. That difference is not invisible: an exhausted free quota answers
429 and the stage degrades with `rewrite_failed`, which is a logged,
observable outcome rather than a silent substitution. This is the same trade
already made for
`GEMINI_API_KEY` itself, which is optional so that "deploy without AI" stays
supported; requiring a second key would break the far more common
single-key deployment for no observability gain. The resolution lives in one
pure function, `config.resolve_rewrite_api_key()`, feeding the single
constant `config.REWRITE_API_KEY` that `GeminiQueryRewriter` defaults to.

The asymmetric configuration *is* rejected, and joins the aggregated list
from `invalid_required_values()`: `AI_SCRIPTURE_REWRITE_API_KEY` set while
`GEMINI_API_KEY` is empty buys the first stage of a pipeline whose
embeddings and rerank have no key at all — a state no deployer means to be
in, and one that would otherwise surface as a puzzling half-working
endpoint. It is a value that is set and cannot be used, which is precisely
what that function reports.

### Operational parameters keep their defaults — but not their typos

Limits, TTLs, timeouts and `DB_PORT` are tuning knobs, not identity
decisions: `AI_SCRIPTURE_TIMEOUT_SECONDS=15` unset is a deliberate,
reviewed operating point, not a guess. `parse_int` / `parse_float` return the
documented default when a variable is unset or blank. But a value that *is*
set and cannot be parsed is a `ConfigError` naming the variable and the raw
value, never a silent fallback to the default — a typo like
`AI_SCRIPTURE_INDEX_CACHE_SECONDS=3600s` used to be swallowed and the service
silently ran on the default anyway, exactly the invisibility this ADR
targets. `invalid_required_values()` adds a second layer for values that
parse but are out of range: `EMBEDDING_DIMENSIONS=0` parses as a valid `int`
and would otherwise reach `current_embedding_version()` and produce an
`outputDimensionality: 0` request.

### Defense in depth: `IndexVersionUnavailable`

Even with `EMBEDDING_MODEL` / `EMBEDDING_DIMENSIONS` in
`ALWAYS_REQUIRED_VARS`, `app/vector_index.py` does not trust callers to have
gone through `config.py` — `current_embedding_version()` re-checks its
`model`/`dims` arguments and raises `IndexVersionUnavailable` rather than
build the string `c3:@0` from an empty model or non-positive dimensions.
`c3:@0` reads as a legitimate-looking version that simply has no rows: a
read against it looks like "the index is empty", and a *rebuild* against it
would mark every stored row of every real version stale and delete them
before failing to embed a single new one. `python app/index_cli.py rebuild`
therefore refuses before touching any row — both when `GEMINI_API_KEY` is
absent (a rebuild must embed every chunk through the API) and when
`IndexVersionUnavailable` is raised — and the read paths (`status`, `search`,
`scripture_select._load_resources`) let the exception surface as a clear
refusal instead of an empty-index diagnosis. In production this branch is
unreachable — config guarantees the pair is valid before either module runs
— but a CLI or an import path that ever bypasses `config.py` must still fail
loudly instead of quietly corrupting the index.

## Alternatives considered

**(B) Always start; answer 503 from the AI endpoints only.** Keep the
service startable with an incomplete AI configuration and have
`/api/twinkler/v1/*` and `/api/scripture/v1/select` report the missing
variable names in a 503, without touching the read/audio endpoints that
need no AI. Rejected by the owner on 2026-08-29: a booted-but-degraded
container is easy not to notice, especially for a variable nobody is
actively testing (the incident variable is only exercised by the rewrite
stage of one endpoint). Failing at import makes the problem visible at the
moment it is introduced — at deploy time, in the deploy log — rather than
at the moment a user happens to hit the affected endpoint. A crashed
container cannot be missed; a healthy-looking container quietly returning
502/503 to a fraction of requests can be, and was.

This has a real cost, stated plainly: with `restart: always` (the
production compose policy), an incomplete production `.env` puts the
*entire* API into a restart-crash loop, not just the AI surface, until the
missing variable is fixed — worse than option B's partial degradation for
that window. The mitigation is procedural, not architectural: production
deploys go through a checklist of the required variables (see the deploy
ticket) before `docker compose up -d` runs against a changed `.env`. The
owner's judgment is that an unmissable, fully-down failure with a clear
aggregated error beats a partially-working service whose broken part has
to be discovered by using it.

## Consequences

- A deploy with an incomplete or malformed `.env` never starts; the
  container log shows every problem at once instead of one crash-and-fix
  cycle per missing variable.
- Production deploys that touch `.env` require the variable checklist in
  the deploy ticket to be re-checked first — this ADR trades runtime
  invisibility for a deploy-time precondition. `restart: always` means a
  bad `.env` restart-loops the whole API, not only the AI surface, until
  fixed.
- "Deploy without AI" remains a fully supported configuration: omitting
  `GEMINI_API_KEY` starts the service normally with the AI endpoints
  reporting their own "not configured" error and everything else — reading,
  audio, scripture selection via the safe pool — working as documented.
  Nothing in this ADR requires a Gemini key to exist.
- `EMBEDDING_MODEL` / `EMBEDDING_DIMENSIONS` are required in every
  deployment, key or no key — a stricter bar than the other AI-shaped
  variables, justified above and covered by
  `tests/test_config.py::test_embedding_pair_is_required_even_without_a_key`.
- `tests/conftest.py` sets every required variable via `os.environ.setdefault`
  before any test module imports `config`, so the suite runs regardless of
  what the container's real `.env` contains; a real value already present
  still wins.
- Tooling that imports `app/config.py` (CLIs, the benchmark) inherits the
  same fail-fast behaviour and must set the required variables the same way
  tests do — see `evaluation/retrieval_benchmark.py`'s `API_KEY` default and
  the nit fixed alongside this ADR in `app/retrieval_cli.py`.
- Extends ADR 0002 (`EMBEDDING_MODEL`/`EMBEDDING_DIMENSIONS` already had no
  code default, for the same "names an index, not a knob" reason), ADR 0004
  (`AI_SCRIPTURE_REWRITE_MODEL`, the variable at the center of the incident)
  and ADR 0005 (`AI_SCRIPTURE_RERANK_MODEL`) by making the requirement a single
  enforced policy instead of three independent conventions.

## Open questions

1. The deploy checklist mitigating the restart-loop cost lives in the
   deploy ticket, outside this repo — consider moving it into
   `infrastructure.md` (the `cep` monorepo) so it survives independently of
   any single ticket.
2. `AI_REQUIRED_VARS` is gated on `GEMINI_API_KEY` alone. If a future
   deployment ever wants AI partially configured (e.g. transcription but not
   rewrite), the current all-or-nothing gate would need to be split — no
   such deployment exists today.
