#!/usr/bin/env python3
"""
Generate answers of the "leading question" feature (`POST /api/ai/question`)
for the evaluation inputs, on either provider (ClickUp 86cbegctz).

The endpoint is a single provider call: `system_instruction` =
`app/question_prompt.build_question_prompt(safety.detect_language(<the
person's last words>))`, `contents` = the message
`app/question_prompt.build_user_message(topic, stage, messages)` assembles
from the request, temperature 0.7, maxOutputTokens 1024.
This tool reproduces exactly that call against two providers so the answers
are comparable:

* `--provider gemini` — the production path, byte for byte: the same REST URL
  shape, the same payload, the same `x-goog-api-key` header as
  `app/twinkler_ai.complete`. Model and key come from the environment
  (`AI_QUESTION_MODEL`, `GEMINI_API_KEY`); when they are not exported, `.env`
  of the repository is parsed INTO MEMORY only (never copied, never logged).
* `--provider qwen` — the same system/user pair through an OpenAI-compatible
  `/chat/completions` endpoint (Maria's Qwen3-30B behind the SSH tunnel), the
  protocol `gen_rewrites.py` / `gen_reranks.py` already use.

The prompt and the stage blocks are NOT re-typed here: both are imported from
`app/question_prompt.py`, so a change of the production wording changes this
measurement too.

**Since ClickUp 86cbegmzz the request is structured** (`topic` + `stage` +
`messages`) and the stage instructions are the server's. So an input is a
request now, not a string:

* `question_probe_inputs.json` (v2.0.0) carries `stage`, `topic` and
  `messages` per input, covering what the scripture set does not — despair,
  one word, pure joy, a conversation that already holds a question, both
  stages the app added, a language switch mid-conversation, and an explicit
  despair phrase in an older reply.
* `scenarios.json` is the approved benchmark set and is NOT edited for this:
  its `prayer_context` maps to a request the same way the app would send one
  — the topic is the topic, every recorded reply is a `user` turn, and the
  stage is `next` when there are replies and `first` when there are none.
  (The scenarios record no questions of ours, so those histories carry no
  `assistant` turns; the "already asked" block is simply absent.)
* `empty` scenarios are still SKIPPED, for a new reason: the endpoint now
  *accepts* them (an empty topic with an empty history is a legal `first`
  request that asks for a generic opening question), but with no words of the
  person in the request at all the prompt names English by contract, and the
  scenario's declared language stops being an expectation anything can be
  graded against.
* `question_series_inputs.json` (v1.0.0, ClickUp 86cbehyez) is the third
  source and the only one that is off by default: `--series <file>`. Its
  `series` inputs are **N sequential requests**, which is what pressing
  «заменить вопрос» N times is today — the client re-sends the SAME body every
  time, so the only thing that differs between two replacements is sampling.
  `--accumulate-skipped` runs the same series as the client of ADR 0015 does
  — every replacement carries the questions already skipped — and
  `--prompt-variant` runs any of them on a candidate wording of ClickUp
  86cbehyf8 (`question_prompts.py`) instead of the shipped text.

**Several candidates per call, or one more call (ClickUp 86cbehyg4).** Two
ways to answer a repeat, measured against each other rather than argued about:

* `--retry-on-repeat` replays **production** (ADR 0016): one generation, and
  when `app/question_novelty.is_repeat` flags it, one more with the rejected
  text appended to `skipped_questions` for that call only. Unlike
  `check_questions.py --novelty-sim`, which can only count how often the retry
  *would* fire, this one actually makes the second call — so the artifact
  carries its real latency and its real tokens.
* `--candidates N` asks the model for N answers in ONE call (`n` of the
  OpenAI-compatible API; vLLM shares the prefill across them) and picks one
  server-side with `choose_candidate` below. Qwen only — Gemini's analogue is
  `generationConfig.candidateCount`, which is **not** implemented here.

Neither is wired into `app/twinkler_ai.py`: this tool produces the numbers the
decision is taken on, and the endpoint keeps doing exactly what ADR 0016 says
until that decision is made.

Artifacts: one JSONL row per (input, sample) — and per **step** for a series,
carrying `series_id` and `step` — plus a `<out>.meta.json` sidecar. Neither
ever carries a key: transport failures are recorded as a category and, for an
HTTP error, a status code — never the URL.

Examples:

    # what every input turns into — no provider, no key, no quota
    python gen_questions.py --dry-run
    python gen_questions.py --dry-run --only probe-dialog,probe-reflect

    # Maria's 30B over the tunnel (key read by the caller, see
    # run_local_picker_qwen.sh — it must never land in a file)
    python gen_questions.py --provider qwen \\
        --endpoint https://llm.ai2.ru:8443/v1 \\
        --model qwen3-30b-a3b-instruct-2507 --api-key "$KEY" \\
        --out bench_data/questions_qwen30b_v1.jsonl

    # production Gemini, free tier: 15 requests per minute
    python gen_questions.py --provider gemini --sleep 4.5 \\
        --out bench_data/questions_gemini35lite_v1.jsonl

    # the replacement series alone (86cbehyez) — the other two sources off
    QWEN_API_KEY="$(ssh -o BatchMode=yes root@193.39.168.166 \\
        'sed -n "s/^VLLM_SECONDARY_API_KEY=//p" /etc/vllm/api-secondary.env')" \\
    python gen_questions.py --provider qwen \\
        --endpoint https://llm.ai2.ru/v1 --model qwen3-30b-a3b-instruct-2507 \\
        --scenarios '' --probes '' --series question_series_inputs.json \\
        --samples 6 --out bench_data/questions_qwen30b_v3_series.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

import httpx

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "app"))

# `question_prompt` imports nothing — not even `config` — which is exactly why
# it was split out of `twinkler_ai.py` (see its module docstring). So the
# production prompt can be imported here without the fail-fast environment
# dance the other generators need. `safety` imports only `prompt_safety`, both
# of them from `app/`, for the same reason: the language the prompt names is
# resolved by THE production detector, never by a copy of it living here.
from question_prompt import QUESTION_PROMPT_VERSION  # noqa: E402
from safety import check_input, check_reply, detect_language  # noqa: E402

# The production repeat filter itself (ClickUp 86cbehyg0), imported for the
# same reason the prompt is: the selection rule of 86cbehyg4 must drop exactly
# what the endpoint would call a repeat, not what a copy of it would.
from question_novelty import KIND_NONE, is_repeat  # noqa: E402

# The candidate wordings of ClickUp 86cbehyf8 (v4). `PRODUCTION` — the default
# — returns `build_question_prompt` / `build_user_message` themselves, so a run
# without `--prompt-variant` is the endpoint byte for byte, exactly as before
# this flag existed.
sys.path.insert(0, str(HERE))
import question_prompts  # noqa: E402

SCENARIOS_FILE = HERE / "scenarios.json"
PROBE_FILE = HERE / "question_probe_inputs.json"
SERIES_FILE = HERE / "question_series_inputs.json"
ENV_FILE = HERE.parent / ".env"

# `build_user_message` takes `(role, text)` pairs; this is the role a question
# of ours carries in a history.
ROLE_ASSISTANT = "assistant"

# `CompleteRequest.skipped_questions` takes at most this many entries
# (`app/twinkler_ai.MAX_SKIPPED_QUESTIONS`). Re-typed rather than imported:
# `twinkler_ai` drags in FastAPI and the fail-fast config, which this tool
# deliberately does without — `tests/test_gen_questions.py` pins the two
# numbers to each other instead, so a change on the endpoint side fails here.
MAX_SKIPPED_QUESTIONS = 10

# The production values of `app/twinkler_ai.complete`. They are the
# measurement, not a knob: changing one makes the run incomparable with the
# endpoint, so they are constants with CLI overrides only where a provider
# forces one.
TEMPERATURE = 0.7
MAX_OUTPUT_TOKENS = 1024
GEMINI_URL_TEMPLATE = (
    "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
)
# `complete()` uses 20 s. A CPU/GPU-shared local model over an SSH tunnel is
# slower than the production provider, so the default is generous and the
# value actually used is recorded in the sidecar.
DEFAULT_TIMEOUT_SECONDS = 180.0
TRANSPORT_ATTEMPTS = 3
# Free-tier Gemini is 15 requests per minute; a 429 wants more than the
# ordinary backoff, and burning the daily quota on retries helps nobody.
RATE_LIMIT_PAUSE_SECONDS = 65.0


def endpoint_host(endpoint: str) -> str:
    """Host of the endpoint, never its query string (keys can live there)."""
    parts = urlsplit(endpoint)
    return f"{parts.scheme}://{parts.netloc}" if parts.netloc else endpoint


def transport_error(exc: Exception) -> str:
    """Failure category — never the provider's message and never a URL.

    httpx renders an HTTPStatusError as "... for url '<full url>'"; these
    artifacts are committed to a PUBLIC repository, so only the exception
    type and, for an HTTP error, the status code are recorded.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return f"transport: HTTPStatusError (HTTP {exc.response.status_code})"
    return f"transport: {type(exc).__name__}"


def is_rate_limited(exc: Exception) -> bool:
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code == 429
    )


def resolve_path(value: str) -> Path:
    """Relative paths are relative to evaluation/, not to the caller's cwd."""
    path = Path(value)
    return path if path.is_absolute() else HERE / path


def meta_path(out: Path) -> Path:
    """Sidecar holding the run's metadata (same convention as gen_descriptions)."""
    return out.with_name(out.name + ".meta.json")


def write_meta(out: Path, meta: dict) -> None:
    """Write the sidecar atomically: a crash must not truncate a good one."""
    path = meta_path(out)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    tmp.replace(path)


def append_record(out: Path, record: dict) -> None:
    """Append one row and get it onto the disk.

    One `write` of a complete line followed by fsync, so an interrupted run
    leaves whole records behind it — a 90-call run against a metered provider
    must never be lost to a crash at call 89.
    """
    with out.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def read_env_file(path: Path) -> dict[str, str]:
    """`.env` as a dict, in memory only.

    Read, never written; the caller pulls out the two values it needs and
    nothing is echoed. Deliberately minimal parsing (KEY=VALUE, `#` comments,
    optional surrounding quotes) — this is not a dotenv implementation, it is
    a way to run the production call outside the container.
    """
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def gemini_settings() -> tuple[str, str]:
    """(model, key) for the production call — environment first, then `.env`.

    Not a silent fallback: both places are named in the error when the value
    is missing, and the run aborts rather than quietly measuring a different
    model (the rule that came out of 2026-08-29).
    """
    env_file = read_env_file(ENV_FILE)
    model = os.environ.get("AI_QUESTION_MODEL") or env_file.get("AI_QUESTION_MODEL", "")
    key = os.environ.get("GEMINI_API_KEY") or env_file.get("GEMINI_API_KEY", "")
    missing = [
        name
        for name, value in (("AI_QUESTION_MODEL", model), ("GEMINI_API_KEY", key))
        if not value
    ]
    if missing:
        raise SystemExit(
            f"{', '.join(missing)} not set: export it, or put it in {ENV_FILE}"
        )
    return model, key


def turns(entry: dict) -> list[tuple[str, str]]:
    """The history as `(role, text)` pairs — what `build_user_message` takes."""
    return [
        (message["role"], message["text"]) for message in entry.get("messages", [])
    ]


def user_message(
    entry: dict,
    skipped_questions: list[str] | None = None,
    variant: str = question_prompts.PRODUCTION,
) -> str:
    """The bytes `POST /api/ai/question` sends as the user content.

    `skipped_questions` is `--accumulate-skipped`. It is empty in the mode that
    reproduces **today's client**, which re-sends an identical body for every
    replacement — that is what the baseline of 86cbehyez measured. With the
    flag on, the questions already replaced are passed in the request's own
    `skipped_questions` field (ADR 0015, merged 2026-09-05), so the block and
    the extra instruction sentence are rendered by the production
    `build_user_message` itself. Until that field existed this argument folded
    them in as extra `assistant` turns instead — a preview that measured a
    different prompt from the one the endpoint now sends, hence the change
    (ClickUp 86cbehyf8).

    `variant` names a candidate wording of 86cbehyf8; the default is the live
    production text.
    """
    source = language_source(entry)
    language = detect_language(source) if source.strip() else "en"
    return question_prompts.user_message(
        variant,
        entry["topic"],
        entry["stage"],
        turns(entry),
        list(skipped_questions or ()),
        language,
    )


def last_reply(entry: dict) -> str | None:
    """The person's last `user` turn, or `None`.

    Mirrors `twinkler_ai.safety_input_text` / `language_source`, which take the
    FastAPI request model this file must not import (it would drag `config`
    and the whole fail-fast environment in). `tests/test_gen_questions.py`
    pins the two against each other on every probe, so the mirror cannot
    drift.
    """
    replies = [text for role, text in turns(entry) if role == "user"]
    return replies[-1] if replies else None


def safety_input(entry: dict) -> str | None:
    """What tier 1 reads: the last reply, or the topic at `first`."""
    reply = last_reply(entry)
    if reply is not None:
        return reply
    return entry["topic"] if entry["stage"] == "first" else None


def language_source(entry: dict) -> str:
    """What the prompt's language is detected on.

    Mirrors `twinkler_ai.person_language_candidates` / `language_source`: the
    person's own words, best evidence first (last reply, topic, earlier
    replies newest first), walked by **decidability** rather than by presence
    — `detect_language` returns `None` for a line that does not say which
    language it is, and the same person usually said it elsewhere in the same
    request. The last assistant question answers only when they wrote nothing
    at all.
    """
    replies = [text for role, text in turns(entry) if role == "user"]
    candidates = [
        text
        for text in replies[-1:] + [entry["topic"]] + list(reversed(replies[:-1]))
        if text.strip()
    ]
    for text in candidates:
        if detect_language(text) is not None:
            return text
    if candidates:
        return candidates[0]
    questions = [text for role, text in turns(entry) if role == "assistant"]
    return questions[-1] if questions else ""


def scenario_request(scenario: dict) -> dict:
    """`prayer_context` as the request the app would send for it.

    The scenarios record a topic and the person's replies, never the questions
    that were asked, so the history is `user` turns only and the stage is
    `next` exactly when there is one. That is a legal request of the contract
    (a history that starts with `user` is what a trimmed conversation looks
    like), and it keeps the approved set unedited — `scenarios.json` and
    `thresholds.json` are frozen reference data.
    """
    context = scenario["prayer_context"]
    replies = [
        reply for reply in (context.get("user_replies") or []) if reply.strip()
    ]
    return {
        "topic": context.get("topic") or "",
        "stage": "next" if replies else "first",
        "messages": [{"role": "user", "text": reply} for reply in replies],
    }


def asked_questions(entry: dict) -> list[str]:
    """Every question already in this history — what `no_repeat` grades against.

    Deliberately NOT the questions a series produced at steps 1..k-1: today's
    request cannot carry them (that is the bug 86cbehtkh is about), so grading
    a replacement against them would grade the model on information it was
    never given. `check_questions.py` measures the step-to-step repetition
    separately, as a series metric.
    """
    explicit = entry.get("avoid_question") or []
    if isinstance(explicit, str):
        explicit = [explicit]
    asked = [text for role, text in turns(entry) if role == ROLE_ASSISTANT]
    return list(dict.fromkeys(explicit + asked))


def load_series(path: Path) -> list[dict]:
    """`question_series_inputs.json` as generator inputs.

    A `single` becomes the same kind of entry a probe does; a `series` carries
    `replacements` — how many times the identical body is sent — and is
    executed as that many sequential calls per sample.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries: list[dict] = []
    for item in payload["inputs"]:
        kind = item.get("kind", "single")
        if kind not in ("single", "series"):
            raise SystemExit(f"{path}: {item['id']} has unknown kind {kind!r}")
        entry = {
            "id": item["id"],
            "source": "series",
            "language": item["language"],
            "category": item["category"],
            "expect_question": bool(item.get("expect_question", True)),
            "topic": item["topic"],
            "stage": item["stage"],
            "messages": item["messages"],
            "steps": int(item["replacements"]) if kind == "series" else 1,
            "is_series": kind == "series",
        }
        if entry["steps"] < 1:
            raise SystemExit(f"{path}: {item['id']} has replacements < 1")
        avoid = asked_questions(item)
        if avoid:
            entry["avoid_question"] = avoid
        # The person's own words, carried into every row of this input so the
        # gender-agreement heuristic in check_questions.py works on the
        # artifact alone — an artifact that needs its input file to stay
        # unchanged to be readable is not a baseline.
        person = [
            text for role, text in turns(entry) if role == "user" and text.strip()
        ]
        if entry["topic"].strip():
            person = [entry["topic"]] + person
        entry["person_words"] = person
        entries.append(entry)
    return entries


def load_inputs(
    scenarios_path: Path | None,
    probes_path: Path | None,
    series_path: Path | None = None,
) -> tuple[list[dict], list[dict]]:
    """(inputs to send, inputs skipped with a reason).

    Any of the three sources may be `None` — "this run does not use that set".
    The series file is `None` unless `--series` names it; the other two are
    switched off with `--scenarios ''` / `--probes ''`, which is how the
    86cbehyez baseline runs the series alone.

    Two kinds of input are skipped, both because sending them would measure
    something the endpoint does not do:

    * `empty` scenarios — an empty topic with an empty history. The endpoint
      accepts it since ClickUp 86cbegmzz (it is a legal `first` request asking
      for a generic opening question), but the request then contains no words
      of the person at all: the prompt names English by contract, so the
      scenario's declared language is no longer an expectation the checker can
      grade, and the answer would say nothing about the prompt.
    * anything `safety.check_input` catches on the text tier 1 actually reads
      (the last reply, or the topic at `first`): since ClickUp 86cbegg23 the
      despair rule is code, so `probe-despair` is answered by `app/safety.py`
      with the fixed reply and the model is never called. Note the input this
      test runs on — `probe-next-despair-older` carries the same phrase two
      turns back and is NOT skipped, because the endpoint does call the model
      for it. Since Maria's 2026-09-05 decision tier 2 also reads that same
      last reply (not the whole history), so it no longer guards the answer
      here either: the generator and the endpoint now agree on what comes
      back for this input, where they used to potentially disagree.
    """
    dataset = (
        json.loads(scenarios_path.read_text(encoding="utf-8"))
        if scenarios_path
        else {"scenarios": []}
    )
    probes = (
        json.loads(probes_path.read_text(encoding="utf-8"))
        if probes_path
        else {"inputs": []}
    )
    inputs: list[dict] = []
    skipped: list[dict] = []

    def answered_in_code(entry: dict) -> bool:
        checked = safety_input(entry)
        finding = check_input(checked) if checked else None
        if finding is None or not finding.matched:
            return False
        skipped.append({
            "id": entry["id"],
            "reason": (
                f"app/safety.py answers this in code (tier {finding.tier}, "
                f"pattern {finding.pattern_id}): POST /api/ai/question returns "
                "the fixed reply and never calls a provider, so there is no "
                "model answer to measure"
            ),
        })
        return True

    for scenario in dataset["scenarios"]:
        request = scenario_request(scenario)
        entry = {
            "id": scenario["id"],
            "source": "scenarios",
            "language": scenario["language"],
            "category": scenario["category"],
            "expect_question": True,
            **request,
        }
        if scenario["category"] == "empty" or not (
            entry["topic"].strip() or entry["messages"]
        ):
            skipped.append({
                "id": scenario["id"],
                "reason": (
                    "empty request: no topic and no history, so it carries "
                    "none of the person's words. The endpoint answers it (a "
                    "generic first question) but names English by contract, "
                    "and the scenario's language can no longer be graded"
                ),
            })
            continue
        if answered_in_code(entry):
            continue
        inputs.append(entry)

    for probe in probes["inputs"]:
        entry = {
            "id": probe["id"],
            "source": "probes",
            "language": probe["language"],
            "category": probe["category"],
            "expect_question": bool(probe.get("expect_question", True)),
            "topic": probe["topic"],
            "stage": probe["stage"],
            "messages": probe["messages"],
        }
        # What `check_questions.no_repeat` compares the answer against: every
        # question already in this history, plus anything the probe names
        # explicitly (a question asked before the trimmed head, say).
        avoid = asked_questions(
            {**entry, "avoid_question": probe.get("avoid_question")}
        )
        if avoid:
            entry["avoid_question"] = avoid
        if answered_in_code(entry):
            continue
        inputs.append(entry)

    for entry in load_series(series_path) if series_path else []:
        if answered_in_code(entry):
            continue
        inputs.append(entry)

    return inputs, skipped


def chat_completion(
    client: httpx.Client,
    url: str,
    api_key: str,
    model: str,
    user: str,
    prompt: str,
    sampling: dict | None = None,
    candidates: int = 1,
) -> tuple[list[str], dict]:
    """N OpenAI-compatible chat completions of the production pair, and `usage`.

    `candidates` becomes the `n` of the chat-completions API (ClickUp
    86cbehyg4) and is **omitted entirely** when it is 1, so a single-candidate
    call is byte for byte the request every artifact before that ticket was
    produced by. vLLM answers `n` choices from one prefill of the shared
    prompt, which is the whole point of measuring it: the 872 prompt tokens of
    a series step are paid once instead of twice.

    `sampling` is the fourth lever of ClickUp 86cbehyf8, widened by 86cbejvra,
    and is empty in every other run: production sends temperature and
    max_tokens and nothing else, so the server's own defaults for `top_p`,
    `top_k`, `min_p` and `presence_penalty` are part of what is being measured.
    When a sampling flag is given its key is added here — `temperature` last,
    so it overrides the constant above — and named in the sidecar, so an
    artifact can never be mistaken for a wording result.

    The returned `usage` is the server's own — `completion_tokens` is the SUM
    over the choices, `prompt_tokens` is counted once — and is `{}` when the
    server sends none.
    """
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": user},
        ],
        "temperature": TEMPERATURE,
        "max_tokens": MAX_OUTPUT_TOKENS,
        **({"n": candidates} if candidates > 1 else {}),
        **(sampling or {}),
    }
    response = client.post(url, json=payload, headers=headers)
    response.raise_for_status()
    data = response.json()
    try:
        choices = data["choices"]
        messages = [choice["message"] for choice in choices]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("response has no choices") from exc
    texts = [
        message.get("content").strip()
        for message in messages
        if isinstance(message.get("content"), str) and message["content"].strip()
    ]
    if not texts:
        raise ValueError("response content is empty")
    usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    return texts, {
        key: usage.get(key)
        for key in ("prompt_tokens", "completion_tokens", "total_tokens")
        if isinstance(usage.get(key), int)
    }


def call_qwen(
    client: httpx.Client,
    url: str,
    api_key: str,
    model: str,
    user: str,
    prompt: str,
    sampling: dict | None = None,
) -> str:
    """One answer — the historical single-candidate path, unchanged.

    Kept as the entry point of every run that does not ask for candidates or
    for the production retry: it is the call the v1..v4 artifacts were produced
    by and the one `tests/test_gen_questions.py` stands in for. A partial
    answer is a `ValueError` here exactly as before, so the transport ladder
    around it behaves identically.
    """
    texts, _usage = chat_completion(
        client, url, api_key, model, user, prompt, sampling, 1
    )
    return texts[0]


def call_gemini(
    client: httpx.Client, url: str, api_key: str, user: str, prompt: str
) -> str:
    """One `:generateContent` call — the payload of `twinkler_ai.complete`."""
    payload = {
        "system_instruction": {"parts": [{"text": prompt}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": {
            "maxOutputTokens": MAX_OUTPUT_TOKENS,
            "temperature": TEMPERATURE,
        },
    }
    response = client.post(url, json=payload, headers={"x-goog-api-key": api_key})
    response.raise_for_status()
    data = response.json()
    # `twinkler_ai._extract_text`, kept in sync deliberately: a run must fail
    # the same way production fails, not more forgivingly.
    try:
        parts = data["candidates"][0]["content"]["parts"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("response has no candidates") from exc
    if not isinstance(parts, list):
        raise ValueError("response parts are not a list")
    text = "".join(
        part.get("text", "")
        for part in parts
        if isinstance(part, dict) and isinstance(part.get("text"), str)
    ).strip()
    if not text:
        raise ValueError("response text is empty")
    return text


def sampling(args: argparse.Namespace) -> dict:
    """The optional sampling overrides, empty unless a flag named one.

    Every key here is added to the payload **only when its flag was given**, so
    a run without any of them sends exactly the production request and stays
    comparable with every artifact produced before these flags existed. That is
    why `--temperature` is `None` by default rather than `TEMPERATURE`: an
    explicit flag overrides the constant (the payload builder spreads this dict
    last), and no flag leaves the constant alone.

    `presence_penalty` and `min_p` are the diversity levers of ClickUp
    86cbejvra, measured on Qwen3-30B; `top_p`/`top_k` are 86cbehyf8's. All of
    them are qwen-only — the Gemini path sends `generationConfig` and is
    refused in `main` when any of these is set.
    """
    extra: dict = {}
    if getattr(args, "temperature", None) is not None:
        extra["temperature"] = args.temperature
    if getattr(args, "top_p", None) is not None:
        extra["top_p"] = args.top_p
    if getattr(args, "top_k", None) is not None:
        extra["top_k"] = args.top_k
    if getattr(args, "presence_penalty", None) is not None:
        extra["presence_penalty"] = args.presence_penalty
    if getattr(args, "min_p", None) is not None:
        extra["min_p"] = args.min_p
    return extra


def effective_temperature(args: argparse.Namespace) -> float:
    """The temperature this run actually sends — the constant, or the flag."""
    override = getattr(args, "temperature", None)
    return TEMPERATURE if override is None else override


# ---------------------------------------------------------------------------
# Several candidates per call, or one more call (ClickUp 86cbehyg4)
# ---------------------------------------------------------------------------
# `shown` below is what the PERSON has read in this prayer: the questions
# already in the journal (`avoid_question` — the `assistant` turns of the
# request) plus every earlier step of this series. That is deliberately the
# same list `check_questions.py --novelty-sim` builds, in BOTH client modes:
# with the accumulating client of ADR 0015 it is literally the request's
# `skipped_questions`, and with today's identical-body client the person has
# read those questions all the same, even though the request no longer carries
# them. Two different lists for the two modes would make the two halves of the
# result table incomparable — and would measure, in the identical-body half, a
# filter that is blind by construction rather than the loop the ticket is
# about.


def shown_questions(entry: dict, produced: Sequence[str]) -> list[str]:
    """Every question this person has already been shown in this prayer."""
    return list(entry.get("avoid_question") or []) + [
        text for text in produced if text.strip()
    ]


def tier2_replaces(checked: str | None, text: str) -> bool:
    """Would tier 2 of the despair rule replace this answer with a fixed text?

    `twinkler_ai._safety_guarded_reply` on one candidate. It runs on EVERY
    reply a provider returns — including the second generation of ADR 0016 —
    so a candidate it would replace is not an answer this endpoint can offer
    at all, whichever mechanism produced it.
    """
    return check_reply(checked or "", text).matched


def choose_candidate(
    candidates: Sequence[str], shown: Sequence[str], checked: str | None
) -> dict:
    """Pick one of the N answers of a single call — the rule of 86cbehyg4.

    1. Drop the blanks and everything **tier 2 of the despair rule** would
       replace (`tier2_replaces`): those are not answers this endpoint may
       show, and the mechanism that produced them changes nothing about that.
    2. Score what is left with the production filter, `is_repeat`, against
       `shown`.
    3. Among the candidates it did **not** flag, take the **first in the
       model's own order**. Not the most distant one: the order is the model's
       own preference, and the most distant candidate is frequently the one
       that fits the conversation least — it is distant because it changed the
       subject. Distance decides nothing while anything survives; where the
       two rules WOULD disagree is recorded (`least_similar_index`,
       `disagreement`) so the difference can be read rather than assumed.
    4. If nothing survives, take the **least similar** of them (lowest
       `is_repeat` score, first on a tie) and mark the step `novel: false` —
       exactly what production returns when its second generation repeats too
       (ADR 0016: the answer is never withheld).
    5. If tier 2 would replace **every** candidate there is no model answer at
       all, and production sends the fixed safety text: `selection: "safety"`,
       no chosen index.

    Returns the fields the artifact records; the caller adds the texts.
    """
    texts = [text for text in candidates if text and text.strip()]
    safety_dropped = [
        index for index, text in enumerate(texts) if tier2_replaces(checked, text)
    ]
    kept = [index for index in range(len(texts)) if index not in set(safety_dropped)]
    verdicts = {index: is_repeat(texts[index], list(shown)) for index in kept}
    result = {
        "candidate_scores": [
            round(verdicts[index].score, 4) if index in verdicts else None
            for index in range(len(texts))
        ],
        "candidate_kinds": [
            verdicts[index].kind if index in verdicts else "safety"
            for index in range(len(texts))
        ],
        "safety_dropped": safety_dropped,
    }
    if not kept:
        return {
            **result,
            "chosen_index": None,
            "least_similar_index": None,
            "novel": True,
            # "safety" is a claim about WHY there is no answer, so an empty
            # candidate list — which the caller never produces, it only calls
            # this with a non-empty response — must not borrow it.
            "selection": "safety" if texts else "none",
            "disagreement": False,
        }
    survivors = [index for index in kept if not verdicts[index].repeat]
    pool = survivors or kept
    least_similar = min(pool, key=lambda index: (verdicts[index].score, index))
    # The model's order while anything survives; distance only when nothing
    # does, and then it is the least bad repeat rather than a preference.
    chosen = pool[0] if survivors else least_similar
    return {
        **result,
        "chosen_index": chosen,
        "least_similar_index": least_similar,
        "novel": bool(survivors),
        "selection": "first_survivor" if survivors else "least_similar",
        "disagreement": bool(survivors) and least_similar != chosen,
    }


def _call_row(
    latency_ms: int, usage: dict, asked: int, attempts: int, error: str
) -> dict:
    """One provider call as the artifact records it — no text, just the cost."""
    return {
        "latency_ms": latency_ms,
        "n": asked,
        "attempts": attempts,
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "error": error or None,
    }


def experiment(args: argparse.Namespace) -> bool:
    """Is this a run of ClickUp 86cbehyg4 (candidates, or the replayed retry)?"""
    return (
        getattr(args, "candidates", 1) > 1
        or bool(getattr(args, "retry_on_repeat", False))
    )


def _provider_call(
    client: httpx.Client,
    args: argparse.Namespace,
    url: str,
    api_key: str,
    model: str,
    sent: str,
    prompt: str,
    candidates: int,
) -> tuple[list[str], dict, int, str, int]:
    """ONE provider call behind the transport ladder.

    `(texts, usage, attempts, error, latency_ms)`; `texts` is empty when every
    attempt failed. The ladder — three attempts, a longer pause on a 429 — is
    the one every run before this ticket used, moved out of `generate_one`
    unchanged, so that a step made of two calls retries each of them the way
    one call was always retried.
    """
    started = time.monotonic()
    attempts = 0
    last_error = ""
    texts: list[str] = []
    usage: dict = {}
    while attempts < TRANSPORT_ATTEMPTS:
        attempts += 1
        try:
            if args.provider == "gemini":
                texts = [call_gemini(client, url, api_key, sent, prompt)]
            elif experiment(args):
                # The only path that asks for `n` and reads `usage` (ClickUp
                # 86cbehyg4). Every other run keeps going through `call_qwen`
                # — the same bytes, one answer — because that is the call the
                # v1..v4 artifacts were produced by.
                texts, usage = chat_completion(
                    client, url, api_key, model, sent, prompt,
                    sampling(args), candidates,
                )
            else:
                texts = [
                    call_qwen(
                        client, url, api_key, model, sent, prompt, sampling(args)
                    )
                ]
            break
        except (httpx.HTTPError, ValueError) as exc:
            last_error = transport_error(exc) if isinstance(exc, httpx.HTTPError) \
                else f"empty: {exc}"
            if attempts >= TRANSPORT_ATTEMPTS:
                break
            time.sleep(
                RATE_LIMIT_PAUSE_SECONDS if is_rate_limited(exc) else 2.0 * attempts
            )
    return (
        texts, usage, attempts, last_error,
        int((time.monotonic() - started) * 1000),
    )


def _replay_production_retry(
    client: httpx.Client,
    args: argparse.Namespace,
    url: str,
    api_key: str,
    model: str,
    entry: dict,
    variant: str,
    prompt: str,
    skipped_questions: list[str] | None,
    first: str,
    shown: Sequence[str],
    checked: str | None,
) -> tuple[list[str], dict, list[dict]]:
    """`twinkler_ai.twinkler_complete` on one step: one answer, maybe a second.

    The handler, line for line, minus the parts a benchmark has no business
    imitating (the rate limiter, the HTTP status codes) and minus the request
    budget, which cannot be replayed honestly here: this tool's timeout is 180 s
    against the endpoint's 20 s, so `MIN_SECOND_ATTEMPT_SECONDS` would never
    bind and pretending it did would fake a degradation. Every measured second
    call is therefore one production would also have made, provided the first
    one left 3 s of the budget — which the measured latencies say it does.

    Returns `(texts, selection, extra_calls)`: `texts[0]` is the first answer
    and `texts[1]`, when present, the second generation.
    """
    if tier2_replaces(checked, first):
        # Production returns the fixed reply here and never retries.
        return [first], choose_candidate([first], shown, checked), []
    verdict = is_repeat(first, list(shown))
    if not verdict.repeat:
        return [first], {
            "candidate_scores": [round(verdict.score, 4)],
            "candidate_kinds": [verdict.kind],
            "safety_dropped": [],
            "chosen_index": 0,
            "least_similar_index": 0,
            "novel": True,
            "selection": "no_repeat",
            "disagreement": False,
        }, []
    # The rejected question joins the skipped list for this call only, newest
    # kept — `twinkler_ai` trims by count, never by characters.
    retry_message = user_message(
        entry,
        (list(skipped_questions or []) + [first])[-MAX_SKIPPED_QUESTIONS:],
        variant,
    )
    texts, usage, attempts, error, latency = _provider_call(
        client, args, url, api_key, model, retry_message, prompt, 1
    )
    extra = [_call_row(latency, usage, 1, attempts, error)]
    if not texts:
        # A failing SECOND generation is never an error for the person: the
        # first answer stands, repeat and all.
        return [first], {
            "candidate_scores": [round(verdict.score, 4)],
            "candidate_kinds": [verdict.kind],
            "safety_dropped": [],
            "chosen_index": 0,
            "least_similar_index": 0,
            "novel": False,
            "selection": "retry_failed",
            "disagreement": False,
        }, extra
    second = texts[0]
    if tier2_replaces(checked, second):
        return [first, second], {
            "candidate_scores": [round(verdict.score, 4), None],
            "candidate_kinds": [verdict.kind, "safety"],
            "safety_dropped": [1],
            "chosen_index": None,
            "least_similar_index": None,
            "novel": True,
            "selection": "safety",
            "disagreement": False,
        }, extra
    # Both verdicts against the SAME `shown`, never against each other.
    second_verdict = is_repeat(second, list(shown))
    take_second = (
        not second_verdict.repeat or second_verdict.score < verdict.score
    )
    chosen = 1 if take_second else 0
    return [first, second], {
        "candidate_scores": [
            round(verdict.score, 4), round(second_verdict.score, 4)
        ],
        "candidate_kinds": [verdict.kind, second_verdict.kind],
        "safety_dropped": [],
        "chosen_index": chosen,
        "least_similar_index": min(
            (0, 1), key=lambda index: (
                (verdict, second_verdict)[index].score, index
            )
        ),
        "novel": (
            (second_verdict.kind == KIND_NONE)
            if take_second
            else (verdict.kind == KIND_NONE)
        ),
        "selection": "retry_took_second" if take_second else "retry_kept_first",
        "disagreement": False,
    }, extra


def generate_one(
    client: httpx.Client,
    args: argparse.Namespace,
    url: str,
    api_key: str,
    model: str,
    entry: dict,
    sample: int,
    step: int = 1,
    skipped_questions: list[str] | None = None,
    shown: Sequence[str] = (),
) -> dict:
    """One STEP of one input: one provider call, or two (ClickUp 86cbehyg4).

    The prompt is built per input, exactly as `twinkler_ai.complete` builds
    it: since v2 it names the language the detector resolved for THIS message
    (`None` — the detector has no evidence — is a case the prompt itself
    handles, see `question_prompt.UNDETERMINED_LANGUAGE`).

    `step` and `skipped_questions` belong to a series (86cbehyez). In the
    default mode `skipped_questions` is empty at every step, because that is
    what the client sends: the same body, again.

    `shown` is what the person has already read (`shown_questions`) and is
    read only by the two experiment modes: `--candidates N` picks among the N
    answers of one call (`choose_candidate`) and `--retry-on-repeat` replays
    production. Without either flag this is the single call it always was, and
    the record carries exactly the fields it always carried.
    """
    started = time.monotonic()
    text = ""
    source = language_source(entry)
    # `twinkler_ai.question_prompt_for`: an empty source (no topic, no
    # history) is the one case the detector cannot speak for, and English is
    # the documented answer there rather than v1's "detect it yourself".
    prompt_language = detect_language(source) if source.strip() else "en"
    variant = getattr(args, "prompt_variant", question_prompts.PRODUCTION)
    prompt = question_prompts.system_prompt(variant, prompt_language)
    sent = user_message(entry, skipped_questions, variant)
    wanted = max(1, getattr(args, "candidates", 1))

    texts, usage, attempts, last_error, latency = _provider_call(
        client, args, url, api_key, model, sent, prompt, wanted
    )
    calls = [_call_row(latency, usage, wanted, attempts, last_error)]
    if texts:
        text = texts[0]

    selection: dict = {}
    if texts and experiment(args):
        checked = safety_input(entry)
        if getattr(args, "retry_on_repeat", False):
            texts, selection, extra = _replay_production_retry(
                client, args, url, api_key, model, entry, variant, prompt,
                skipped_questions, texts[0], shown, checked,
            )
            attempts += sum(call["attempts"] for call in extra)
            calls += extra
        else:
            selection = choose_candidate(texts, shown, checked)
        index = selection["chosen_index"]
        # A step whose every candidate tier 2 would replace has no model answer
        # to record: production sends the fixed text of `app/safety.py`, which
        # is a constant, so the artifact records the fact and not the reply.
        # `error` then says so rather than "no text", which reads as a
        # transport failure and would be counted as one in the sidecar.
        text = texts[index] if index is not None else ""
        if index is None:
            last_error = "safety: tier 2 would replace every candidate"

    record = {
        "id": entry["id"],
        "sample": sample,
        "source": entry["source"],
        "language": entry["language"],
        "category": entry["category"],
        # The stage decides which instructions the message carries, so an
        # artifact without it cannot be read back (ClickUp 86cbegmzz).
        "stage": entry["stage"],
        "expect_question": entry["expect_question"],
        "provider": args.provider,
        "model": model,
        # What the prompt was told to answer in, `null` when the detector had
        # no evidence — the one number that says whether a language violation
        # is the model's or the detector's.
        "prompt_language": prompt_language,
        "prompt_version": question_prompts.prompt_version(variant),
        # Which wording produced this answer. `production` is the shipped text
        # of `app/question_prompt.py`; a candidate name belongs to 86cbehyf8.
        "prompt_variant": variant,
        "text": text,
        "latency_ms": int((time.monotonic() - started) * 1000),
        "attempts": attempts,
        "error": None if text else (last_error or "no text"),
    }
    if entry.get("avoid_question"):
        record["avoid_question"] = entry["avoid_question"]
    if entry.get("is_series"):
        # A series row is read as a sequence, so it names the sequence it
        # belongs to and its place in it. `sample` stays the repetition of the
        # WHOLE series, exactly as it is the repetition of a single call
        # elsewhere.
        record["series_id"] = entry["id"]
        record["step"] = step
        record["series_steps"] = entry["steps"]
        record["skipped_questions"] = list(skipped_questions or [])
    if entry.get("person_words"):
        record["person_words"] = entry["person_words"]
    if experiment(args):
        # ClickUp 86cbehyg4, and ONLY in a run of it: a default run writes the
        # row it always wrote, so the v1..v4 artifacts and this one are read by
        # the same code. `latency_ms` above is the whole step either way — what
        # the person waits — and `calls` is where the two of a retry show up
        # separately.
        record["mode"] = (
            "retry" if getattr(args, "retry_on_repeat", False) else "candidates"
        )
        record["candidates"] = list(texts)
        record["calls"] = calls
        record["prompt_tokens"] = sum(
            call["prompt_tokens"] or 0 for call in calls
        )
        record["completion_tokens"] = sum(
            call["completion_tokens"] or 0 for call in calls
        )
        record["shown_count"] = len(list(shown))
        record.update({
            key: selection.get(key)
            for key in (
                "chosen_index", "least_similar_index", "novel", "selection",
                "disagreement", "candidate_scores", "candidate_kinds",
                "safety_dropped",
            )
        })
    return record


def build_meta(
    args: argparse.Namespace,
    model: str,
    url_host: str,
    inputs: list[dict],
    skipped: list[dict],
    records: list[dict],
) -> dict:
    expected = sum(entry.get("steps", 1) for entry in inputs) * args.samples
    return {
        "provider": args.provider,
        "model": model,
        "endpoint": url_host,
        "date": date.today().isoformat(),
        "ticket": (
            "ClickUp 86cbegctz, 86cbegg3f, 86cbegmzz, 86cbehyez, 86cbehyf8, "
            "86cbehyg4"
        ),
        "prompt": (
            "app/question_prompt.build_question_prompt(detect_language(last "
            "words of the person)) over "
            "app/question_prompt.build_user_message(topic, stage, messages)"
        ),
        "prompt_version": question_prompts.prompt_version(args.prompt_variant),
        "prompt_variant": args.prompt_variant,
        "prompt_variant_note": question_prompts.describe(args.prompt_variant),
        "scenarios_file": args.scenarios,
        "probes_file": args.probes,
        "series_file": args.series,
        # Off = the released client: every replacement re-sends the same body.
        # On = the client of ADR 0015, which accumulates them, so an artifact
        # says which of the two it measured (see `user_message`).
        "accumulate_skipped": bool(args.accumulate_skipped),
        # ClickUp 86cbehyg4. `candidates` > 1 = N answers from ONE call, picked
        # by `choose_candidate`; `retry_on_repeat` = production replayed (ADR
        # 0016). Both off = every run before that ticket.
        "candidates": getattr(args, "candidates", 1),
        "retry_on_repeat": bool(getattr(args, "retry_on_repeat", False)),
        "selection_rule": (
            "drop what despair tier 2 would replace, drop what "
            "question_novelty.is_repeat flags against the questions already "
            "shown, take the first survivor in the model's order; if none "
            "survives take the least similar and mark the step novel=false"
            if getattr(args, "candidates", 1) > 1
            else None
        ),
        "series": {
            entry["id"]: entry["steps"]
            for entry in inputs
            if entry.get("is_series")
        },
        "sampling": {
            # The temperature actually sent: `TEMPERATURE` unless
            # `--temperature` named another one (ClickUp 86cbejvra).
            "temperature": effective_temperature(args),
            "max_output_tokens": MAX_OUTPUT_TOKENS,
            # Empty = production: temperature 0.7 and the server's own
            # top_p/top_k/min_p/presence_penalty. Non-empty is the separate
            # sampling lever (86cbehyf8, 86cbejvra) and never a wording run.
            # Everything listed here was in the payload verbatim, so the run is
            # reproducible from the sidecar alone.
            "overrides": sampling(args),
            "samples_per_input": args.samples,
            "timeout_seconds": args.timeout,
            "sleep_between_calls_seconds": args.sleep,
        },
        "inputs_sent": len(inputs),
        "inputs_skipped": skipped,
        "records_expected": expected,
        "records_written": len(records),
        "records_failed": sum(1 for r in records if r["error"]),
        "partial": len(records) < expected,
    }


def select_inputs(args: argparse.Namespace, parser: argparse.ArgumentParser):
    """(inputs, skipped) after `--only`, or a parser error naming what is off."""
    inputs, skipped = load_inputs(
        resolve_path(args.scenarios) if args.scenarios else None,
        resolve_path(args.probes) if args.probes else None,
        resolve_path(args.series) if args.series else None,
    )
    wanted = {value.strip() for value in args.only.split(",") if value.strip()}
    if wanted:
        inputs = [entry for entry in inputs if entry["id"] in wanted]
        missing = wanted - {entry["id"] for entry in inputs}
        if missing:
            parser.error(f"--only names unknown or skipped ids: {sorted(missing)}")
    if not inputs:
        parser.error("no inputs selected")
    if getattr(args, "accumulate_skipped", False):
        # At step N the request carries the N-1 questions already replaced, so
        # a series of more than MAX_SKIPPED_QUESTIONS + 1 steps would build a
        # body the endpoint answers with 422 — the run would be measuring a
        # request nobody can send. Refused rather than trimmed on purpose:
        # which end a real client would drop is a client decision nobody has
        # made, and inventing one here would measure a prompt no endpoint
        # renders. Unreachable with today's inputs (6 replacements at most).
        too_long = sorted(
            entry["id"]
            for entry in inputs
            if entry.get("steps", 1) - 1 > MAX_SKIPPED_QUESTIONS
        )
        if too_long:
            parser.error(
                "--accumulate-skipped cannot run these series: "
                f"{too_long} — after {MAX_SKIPPED_QUESTIONS + 1} replacements "
                f"the request would carry more than {MAX_SKIPPED_QUESTIONS} "
                "skipped_questions, which POST /api/ai/question refuses with "
                "422"
            )
    return inputs, skipped


def dry_run(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Print the request every input becomes; contact nothing.

    The point of the flag is that the *bytes* are reviewable without a
    provider, a key or a quota: since ClickUp 86cbegmzz this tool assembles
    the message the endpoint assembles (`build_user_message`) instead of
    sending a string somebody wrote by hand, and a run that costs money is a
    bad place to discover that the assembly is wrong.
    """
    inputs, skipped = select_inputs(args, parser)
    print(
        f"dry run — {len(inputs)} inputs, {len(skipped)} skipped, "
        f"prompt v{question_prompts.prompt_version(args.prompt_variant)}, "
        f"{question_prompts.describe(args.prompt_variant)}, "
        "no provider contacted\n"
    )
    for entry in inputs:
        source = language_source(entry)
        language = detect_language(source) if source.strip() else "en"
        checked = safety_input(entry)
        series = (
            f", series of {entry['steps']} replacements"
            if entry.get("is_series")
            else ""
        )
        print(
            f"=== {entry['id']} ({entry['language']}, stage {entry['stage']}{series})"
        )
        print(f"    prompt language: {language or 'undetermined'}")
        print(
            "    despair rule reads: "
            + (repr(checked) if checked else "nothing (no reply of theirs)")
        )
        for line in user_message(
            entry, variant=args.prompt_variant
        ).splitlines():
            print(f"    | {line}")
        if entry.get("is_series"):
            print(
                "    every replacement sends exactly these bytes again"
                if not args.accumulate_skipped
                else "    --accumulate-skipped: each replacement sends the "
                     "previous questions in `skipped_questions` (ADR 0015 — "
                     "the client that accumulates them, not today's)"
            )
        print()
    for entry in skipped:
        print(f"--- {entry['id']} skipped: {entry['reason']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate `POST /api/ai/question` answers for the evaluation "
            "inputs on Gemini (production) or an OpenAI-compatible endpoint."
        )
    )
    # Required for a real run, checked below rather than by argparse: a
    # --dry-run needs no provider at all (both send the same bytes).
    parser.add_argument("--provider", default="", choices=("qwen", "gemini", ""))
    parser.add_argument(
        "--endpoint", default="",
        help="qwen only: base URL of the OpenAI-compatible API "
             "(…/chat/completions is appended)",
    )
    parser.add_argument(
        "--model", default="",
        help="qwen only: model id at that endpoint. For gemini the model is "
             "AI_QUESTION_MODEL, exactly as production reads it.",
    )
    parser.add_argument(
        "--api-key", default="",
        help="qwen only: bearer token; falls back to $QWEN_API_KEY. For "
             "gemini the key is GEMINI_API_KEY (environment or .env).",
    )
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument(
        "--sleep", type=float, default=0.0,
        help="pause between calls, seconds. Free-tier Gemini allows 15 "
             "requests per minute, so use 4.5 there.",
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--out", default="",
        help="JSONL artifact; metadata goes to <out>.meta.json. A relative "
             "path is resolved against the evaluation/ directory. Required "
             "for a real run, meaningless with --dry-run.",
    )
    parser.add_argument(
        "--scenarios", default="scenarios.json",
        help="the approved benchmark set; '' switches this source off",
    )
    parser.add_argument(
        "--probes", default="question_probe_inputs.json",
        help="the single-request probes; '' switches this source off",
    )
    parser.add_argument(
        "--series", default="",
        help="question_series_inputs.json — the replacement series of ClickUp "
             "86cbehyez. OFF by default: a series costs `replacements` calls "
             "per sample, so it is opted into rather than inherited by every "
             "existing run.",
    )
    parser.add_argument(
        "--accumulate-skipped", action="store_true",
        help="send the questions already replaced in the request's own "
             "`skipped_questions` field (ADR 0015), so each replacement of a "
             "series knows what the person skipped. Inert by default: TODAY'S "
             "client re-sends the identical body, which is what the 86cbehyez "
             "baseline measured, so a run with this flag measures the client "
             "that accumulates them rather than the one that ships.",
    )
    parser.add_argument(
        "--candidates", type=int, default=1,
        help="qwen only (ClickUp 86cbehyg4): ask for N answers in ONE call "
             "(`n` of the chat-completions API — vLLM shares the prefill) and "
             "pick one with the rule in `choose_candidate`. 1 — the default — "
             "is the single call every earlier run made, byte for byte. "
             "Gemini's analogue is generationConfig.candidateCount and is NOT "
             "implemented.",
    )
    parser.add_argument(
        "--retry-on-repeat", action="store_true",
        help="replay PRODUCTION (ADR 0016): when the answer repeats one the "
             "person has already been shown, generate once more with the "
             "rejected text appended to `skipped_questions` for that call. "
             "Unlike `check_questions.py --novelty-sim`, this makes the second "
             "call, so its latency and its tokens are measured rather than "
             "guessed. ClickUp 86cbehyg4.",
    )
    parser.add_argument(
        "--prompt-variant", default=question_prompts.PRODUCTION,
        choices=(*question_prompts.VARIANTS, question_prompts.PRODUCTION),
        help="candidate wording of ClickUp 86cbehyf8 "
             "(evaluation/question_prompts.py). The default is the shipped "
             "text of app/question_prompt.py, byte for byte.",
    )
    parser.add_argument(
        "--top-p", type=float, default=None,
        help="qwen only, and NEVER together with a wording comparison: "
             "override the server's own top_p (the Qwen3 model card asks for "
             "0.8). Production sends neither this nor --top-k, so a run that "
             "sets one measures the sampling lever alone (86cbehyf8).",
    )
    parser.add_argument(
        "--top-k", type=int, default=None,
        help="qwen only: same as --top-p (the model card asks for 20).",
    )
    parser.add_argument(
        "--temperature", type=float, default=None,
        help="qwen only (ClickUp 86cbejvra): send this temperature instead of "
             f"the production {TEMPERATURE}. Unset — the default — leaves the "
             "constant alone, so a run without this flag is byte for byte the "
             "production call. The value used is recorded in the sidecar.",
    )
    parser.add_argument(
        "--presence-penalty", type=float, default=None,
        help="qwen only (ClickUp 86cbejvra): OpenAI-compatible "
             "`presence_penalty`, the second diversity lever after "
             "temperature. Omitted from the payload entirely when unset.",
    )
    parser.add_argument(
        "--min-p", type=float, default=None,
        help="qwen only (ClickUp 86cbejvra): vLLM's `min_p` — cut the tail "
             "relative to the top token, which is what makes a high "
             "temperature usable. Omitted from the payload when unset; a "
             "server that does not know the field answers 400 and the run "
             "stops rather than silently measuring the default.",
    )
    parser.add_argument(
        "--only", default="",
        help="comma-separated input ids — a probe run over a subset",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="print the request each input turns into (the assembled user "
             "message, the language the prompt will name, and what the "
             "despair rule reads) and exit — no provider is contacted, no "
             "artifact is written, no key is needed",
    )
    args = parser.parse_args(argv)

    if args.samples < 1:
        parser.error("--samples must be >= 1")
    if args.candidates < 1:
        parser.error("--candidates must be >= 1")
    # Ranges of the OpenAI-compatible API. A value outside them is a typo the
    # server would answer with a 400 after the first call of a long run, so it
    # is refused here instead — and never clamped, which would measure a
    # configuration nobody asked for.
    if args.temperature is not None and not 0.0 <= args.temperature <= 2.0:
        parser.error("--temperature must be between 0 and 2")
    if args.presence_penalty is not None and not -2.0 <= args.presence_penalty <= 2.0:
        parser.error("--presence-penalty must be between -2 and 2")
    if args.min_p is not None and not 0.0 <= args.min_p <= 1.0:
        parser.error("--min-p must be between 0 and 1")
    if args.candidates > 1 and args.provider == "gemini":
        parser.error(
            "--candidates is a qwen option: Gemini's analogue is "
            "generationConfig.candidateCount and is not implemented here"
        )
    if args.candidates > 1 and args.retry_on_repeat:
        # The two mechanisms this ticket compares. Running both at once would
        # measure a third thing nobody proposed, and would make the token and
        # latency columns of the result table unreadable.
        parser.error(
            "--candidates and --retry-on-repeat are the two alternatives of "
            "ClickUp 86cbehyg4 — measure one per run"
        )

    if args.dry_run:
        return dry_run(args, parser)
    if not args.provider:
        parser.error("--provider is required (qwen or gemini)")
    if not args.out:
        parser.error("--out is required")

    api_key = ""
    if args.provider == "gemini":
        if sampling(args):
            parser.error(
                "--temperature/--top-p/--top-k/--presence-penalty/--min-p are "
                "qwen options: the gemini run must send the production "
                "generationConfig or it measures something else"
            )
        if args.endpoint or args.model or args.api_key:
            parser.error(
                "--endpoint/--model/--api-key are qwen options; the gemini run "
                "must use the production model and key (AI_QUESTION_MODEL, "
                "GEMINI_API_KEY) or it measures something else"
            )
        model, api_key = gemini_settings()
        url = GEMINI_URL_TEMPLATE.format(model=model)
        url_host = "https://generativelanguage.googleapis.com"
    else:
        if not args.endpoint or not args.model:
            parser.error("--endpoint and --model are required for --provider qwen")
        model = args.model
        api_key = args.api_key or os.environ.get("QWEN_API_KEY", "")
        base = args.endpoint.rstrip("/")
        url = base if base.endswith("/chat/completions") else f"{base}/chat/completions"
        url_host = endpoint_host(args.endpoint)

    inputs, skipped = select_inputs(args, parser)

    out = resolve_path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        parser.error(f"{out} exists — move it aside rather than mixing two runs")

    records: list[dict] = []
    calls = sum(entry.get("steps", 1) for entry in inputs) * args.samples
    print(
        f"{args.provider}: {model} @ {url_host} — {len(inputs)} inputs x "
        f"{args.samples} samples = {calls} calls, {len(skipped)} inputs "
        f"skipped, {question_prompts.describe(args.prompt_variant)}"
        + (f", sampling overrides {sampling(args)}" if sampling(args) else "")
        + (" — --accumulate-skipped IS ON (the client of ADR 0015)"
           if args.accumulate_skipped else "")
        + (f" — {args.candidates} candidates per call, one picked"
           if args.candidates > 1 else "")
        + (" — --retry-on-repeat IS ON (production replayed, ADR 0016)"
           if args.retry_on_repeat else ""),
        flush=True,
    )

    try:
        with httpx.Client(timeout=httpx.Timeout(args.timeout)) as client:
            for entry in inputs:
                for sample in range(1, args.samples + 1):
                    # One series = one sample. The steps are sequential
                    # because that is what the person does — press, read,
                    # press again — and because --accumulate-skipped feeds
                    # each step from the ones before it.
                    replaced: list[str] = []
                    for step in range(1, entry.get("steps", 1) + 1):
                        record = generate_one(
                            client, args, url, api_key, model, entry, sample,
                            step=step,
                            skipped_questions=(
                                list(replaced) if args.accumulate_skipped else None
                            ),
                            # What the person has READ by now, which is the
                            # same list in both client modes (86cbehyg4).
                            shown=shown_questions(entry, replaced),
                        )
                        if record["text"]:
                            replaced.append(record["text"])
                        records.append(record)
                        append_record(out, record)
                        mark = "ok " if record["error"] is None else "ERR"
                        preview = record["text"].replace("\n", " ⏎ ")[:70]
                        place = (
                            f"#{sample}.{step}" if entry.get("is_series")
                            else f"#{sample}"
                        )
                        print(
                            f"  {mark} {record['id']:<22} {place:<7} "
                            f"{record['latency_ms'] / 1000:5.1f}s  "
                            f"{preview or record['error']}",
                            flush=True,
                        )
                        write_meta(
                            out,
                            build_meta(
                                args, model, url_host, inputs, skipped, records
                            ),
                        )
                        if args.sleep > 0:
                            time.sleep(args.sleep)
    finally:
        write_meta(out, build_meta(args, model, url_host, inputs, skipped, records))

    failed = sum(1 for record in records if record["error"])
    print(f"\nwrote {out} — {len(records)} records, {failed} failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
