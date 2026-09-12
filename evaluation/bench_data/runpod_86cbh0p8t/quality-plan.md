# Blind question-quality comparison

Prepared on 2026-09-12 for ClickUp 86cbh0p8t. This plan compares the production
Bible-API question stage on Qwen3-30B and Gemma 4 31B. It does not change or
reinterpret `evaluation/scenarios.json` or `evaluation/thresholds.json`.

## Inputs and execution

`quality_cases.json` contains 20 synthetic, non-golden inputs in Russian,
English, and Ukrainian. They cover `first`, `next`, and `reflect`, ordinary
life, grief, sparse or ambiguous context, explicit grammatical gender, and
three replacement series. The series must run with accumulated skipped
questions so the second and third generations exercise the production novelty
contract rather than repeat an identical prompt.

Use the production prompt builder and parser through the existing
`evaluation/gen_questions.py` series-input path. Run three samples per input on
each model with the production question settings: prompt v6, temperature 0.7,
`max_tokens=1024`, JSON response format, and no provider retry. Keep the input,
sample number, generation settings, model revision, vLLM image, and parse method
in the raw artifact. Generate three samples for both models, retain all of them
as provenance, and exclude sample 1 from the blind pack as the warm-up. Samples
2 and 3 are the compared answers.

For a single input, and for step 1 of a replacement series, this exclusion
warms the exact prompt bytes. Steps 2 and 3 of an accumulated-skips series
contain the preceding stochastic answer, so their prompt branches cannot be
replayed exactly without changing the experiment. Apply the same sample-1
exclusion to both models, preserve this limitation in the report, and do not
use latency from the quality run for a speed comparison. The separate load
benchmark is the performance evidence.

The same input and sample identifiers must be present for both models. A failed,
timed-out, empty, truncated, or raw-fallback response remains in the comparison
as a failure; do not replace it with another generation. Automated checks may
report format, language, gender, menu, tail, and lexical-repeat defects, but
these checks are supporting observations rather than a qualitative verdict.

## Blind review pack

Build one row per input and sample. Show the reviewer the input context and two
answers labelled only `A` and `B`. Randomize model orientation independently per
row with a recorded seed, keep the identity map outside the review document,
and reveal it only after the review is complete. Preserve series order inside a
row so novelty can be judged across replacements.

Maria records one of `A better`, `B better`, `tie`, or `both unacceptable`, plus
an optional short reason. The review prompt asks her to consider whether the
question is worth pausing over, grounded in the person's words, natural in the
requested language, gentle without inventing feelings, free of hidden advice,
and meaningfully different from earlier or skipped questions. Structural
failures are displayed but do not decide a preference automatically.

Do not let the agent that generated, assembled, or reviewed the infrastructure
assign the qualitative winner. No ungraded retrieval top-1 is part of this
fixture. The final report separates Maria's blind preferences from automated
defect counts and performance measurements.

## Comparability and evidence

Run both models from the same committed fixture and code revision. Record GPU,
model and container revisions, context cap, start and finish time, and the exact
command with exit code. Store sanitized raw outputs, the blind pack, Maria's
completed review, and the sealed orientation map together under this experiment
directory. Remove API keys, authorization headers, endpoint credentials, and
any provider error body before committing artifacts.

The sample is intentionally diagnostic rather than a statistical claim about
all user conversations. Report counts with their denominators and do not infer
that a small preference difference establishes superiority. A production model
change requires Maria's qualitative acceptance and a separate successful check
on the company server hardware.
