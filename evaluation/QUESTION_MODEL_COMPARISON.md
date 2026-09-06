# Comparing the quality of prayer questions

Ticket: https://app.clickup.com/t/86cbejhh9

The comparison measures **raw model answers** to the same prompt, before
novelty selection, retries or safety-response replacement. It does not change
application configuration or deploy a model. The production prompt and
provider transports come from `gen_questions.py`; existing benchmarks and
`scenarios.json` remain unchanged.

## Inputs and interpretation

`question_quality_inputs.json` contains 12 synthetic single-turn cases and
four existing replacement series, RU/UK/EN. Three samples produce **99 answers
per model** (plus one separately recorded warm-up), displayed as 48 review
cards. Every answer appears; the page never picks the "best" sample.

Initial topic/history and prompt are identical across models. Within a series,
each model's own earlier questions accumulate as skipped questions. These
later requests necessarily differ: that is the behaviour being compared.
These are replacements after fixed human answers, **not** simulated evolving
conversations. Do not infer conversation quality from replacement-only runs.

The synthetic cases contain no crisis scenarios. The separate production
safety tests remain necessary and are not replaced by this quality benchmark.
One warm-up is not a claim that every subsequent prompt is warm. Transport
failures are recorded and abort the run; reporting rejects partial runs
instead of ranking the successful subset. Failed warm-ups are recorded in the
model metadata. To repeat a failed run, preserve its directory as a diagnostic
artifact and start a new comparison directory. There is no implicit resume.

## Run locally

Copy evaluation code into the existing container (only `app/` is mounted):

```bash
docker cp evaluation/. bible-api:/code/evaluation
# No network calls, credentials or quota needed:
docker exec -w /code/evaluation bible-api python compare_question_models.py run \
  --dry-run --out /tmp/question-comparison

# Reads AI_OPENAI_COMPAT_API_KEY from the container's environment:
docker exec -w /code/evaluation bible-api python compare_question_models.py run \
  --models qwen --out /tmp/question-comparison

# Supply GEMINI_API_KEY through your secret manager / shell environment.
# docker exec -e NAME forwards the caller's value; do not put the value in argv.
docker exec -e GEMINI_API_KEY -w /code/evaluation bible-api \
  python compare_question_models.py run --models gemini --out /tmp/question-comparison

docker exec -w /code/evaluation bible-api python compare_question_models.py report \
  --out /tmp/question-comparison
docker cp bible-api:/tmp/question-comparison /tmp/question-comparison
```

Never paste a key into the command line, config or report. Model config names
an **environment variable**, not a value. Models must be named explicitly;
missing access is an error, never a switch to another provider. Use `--config`
for a different registry. Provider type `openai_compat` also works with a proxy
that serves Gemini via `/chat/completions`; record that transport explicitly
instead of pretending it is a direct Gemini API call.

The default pause for direct Gemini is 4.5 seconds. Actual provider limits
vary. Change the registry's operational pause if needed; do not hide a quota
failure or repeatedly spend quota on retries. Each request makes one attempt.

## Add a model

Add an entry to `question_models.json`, for example:

```json
{
  "new_model": {
    "provider": "openai_compat",
    "model": "EXACT_PROVIDER_MODEL_ID",
    "endpoint": "https://your-provider.example/v1",
    "api_key_env": "NEW_MODEL_API_KEY",
    "pause_seconds": 1
  }
}
```

Then run `--models new_model --out /tmp/question-comparison`, with the same
input file and sample count, and rebuild `report`. Other models are not called
again and their results are not overwritten. A changed protocol is rejected:
use a **new directory** and rerun the comparison when changing prompts,
scenarios or sample count. Model capability limitations are errors to resolve,
not silently ignored settings. For a provider with a different wire protocol,
add a transport explicitly; a registry entry alone cannot adapt arbitrary APIs.

`protocol.json` snapshots exact system prompts, input scenarios and rendered
stage/skip templates. Each JSONL row includes the actual sent user message,
input, sample, step, text, latency and heuristic flags. Model metadata records
the model id, safe endpoint, expected/actual counts and completion state.

## Human review

Open `review.html` in a desktop browser. It is a standalone offline file with
no external assets or requests. Letter order is shuffled per card with a
recorded deterministic seed. Initial histories and complete replacement series
are visible, model names and automatic flags are hidden. Blindness is a UI
convenience, not a secrecy boundary: source inspection can reveal names.

Mark the preferred option, "equivalent" or "all poor", plus optional flags:
thoughtful, superficial, invented feelings/facts, same thought, awkward.
Add comments or a suggested question. Navigation preserves choices; the page
uses browser storage and **Download ratings** exports JSON for handoff. Import
accepts only the same comparison identity. Export before closing; browsers may
restrict local-file storage. Model names can be revealed explicitly and a
summary of **your** votes appears. Unrated cards never count as ties or losses.
Adding a model changes comparison identity; old ratings are not silently
applied to different option sets.

`report.md` contains heuristic counts, latency and **all** responses. Heuristics
measure shape and selected wording, not insight, depth, faithfulness to a
person's experience or the correctness of religious claims. Their flags need
reading. Report the assessor and sample scope for any qualitative conclusion;
Maria's acceptance is separate from an agent's assessment. Multiple samples
of one situation are correlated, not independent votes about model quality.

## Verification

```bash
docker cp tests/. bible-api:/code/tests
docker exec -e API_KEY=test-api-key -e AI_CLIENT_HMAC_KEY=test-hmac-key \
  bible-api pytest -q /code/tests/test_compare_question_models.py /code/tests/test_gen_questions.py
# Optional, on a host with Playwright and Chromium installed:
python3 tests/browser_question_review.py
```

The browser check uses clearly synthetic options, exercises both narrow and
wide layouts, persistence/export/import/reveal, and verifies model text cannot
execute HTML. It writes diagnostic screenshots and rating files to `/tmp`.

## Inspect an exact prompt and answer

`report` also writes `prompts.html`. Select a model, input, sample and step
to see its saved system prompt, full sent user message (including skips),
answer and generation parameters. The URL updates to link that exact record.
Changing the model retains the selected scenario/sample/step. This view reveals
model names and is separate from blind review; it makes no provider calls.

## Compare prompt variants (86cbejq55)

Pass `--prompt-variant v4`, `--prompt-variant v5-structured`, or the default
`production` to select frozen v4, the structured English-instruction ablation,
or the current localized prompt. Each variant needs its own output directory.
The protocol records that name, localized and universal system prompts, and
all actual sent system/user texts. Old v4 artifacts remain untouched.

After both models finish for each variant, use:

```bash
python compare_question_prompts.py /tmp/prompts-v4 /tmp/prompts-structured /tmp/prompts-localized \
  --out /tmp/prompts-before-after
```

This deliberately permits different prompts but requires the same scenarios,
sample count, models and generation settings. Each blind card compares prompt
variants within one model, not different models at once. Model-generated skips
accumulate independently for each option. The resulting `prompts.html` shows
exact saved prompts for every variant, including the universal prompt when the
language detector abstained. Do not mistake one sample per scenario in a
short diagnostic ablation for a robust multi-sample quality evaluation.
