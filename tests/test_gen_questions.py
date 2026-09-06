"""Tests for evaluation/gen_questions.py after the structured request.

Scope is the one thing a run cannot check for itself: that the tool builds
**the request the endpoint builds** (ClickUp 86cbegmzz). Until this ticket the
tool sent a string somebody had written by hand, so the only way it could
diverge from production was the prompt — which it imports. Now it assembles a
message, picks the text the language is detected on and picks the text the
despair rule reads, and each of those is a place where a measurement can
quietly stop measuring the endpoint.

The tool cannot import `twinkler_ai` (that would drag `config` and the whole
fail-fast environment into a script that runs outside the container), so it
carries a small mirror of those two selections. This file is what keeps the
mirror honest: every probe is put through both.

No network: `--dry-run` contacts nothing, and no other path is exercised.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

EVALUATION = Path(__file__).resolve().parents[1] / "evaluation"
TOOL = EVALUATION / "gen_questions.py"
PROBES = EVALUATION / "question_probe_inputs.json"
SCENARIOS = EVALUATION / "scenarios.json"
SERIES = EVALUATION / "question_series_inputs.json"

pytestmark = pytest.mark.skipif(
    not TOOL.exists(), reason="evaluation/ is not present in this container copy"
)


def _load_tool():
    if str(EVALUATION) not in sys.path:
        sys.path.insert(0, str(EVALUATION))
    spec = importlib.util.spec_from_file_location("gen_questions", TOOL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


tool = _load_tool()


def probes() -> list[dict]:
    return json.loads(PROBES.read_text(encoding="utf-8"))["inputs"]


def as_request(probe: dict):
    """The probe as the FastAPI request model, so both sides can be compared."""
    import twinkler_ai

    return twinkler_ai.CompleteRequest(
        topic=probe["topic"], stage=probe["stage"], messages=probe["messages"]
    )


# ---------------------------------------------------------------------------
# The probe file is a set of requests now
# ---------------------------------------------------------------------------

def test_every_probe_is_a_request_the_endpoint_accepts():
    """Schema v2.0.0, and the contract's own rules — validated by the model."""
    payload = json.loads(PROBES.read_text(encoding="utf-8"))

    assert payload["version"] == "2.0.0"
    for probe in payload["inputs"]:
        assert probe["stage"] in ("first", "next", "reflect"), probe["id"]
        assert isinstance(probe["topic"], str)
        assert "text" not in probe, f"{probe['id']} still carries the v1 field"
        as_request(probe)  # raises if the history breaks a rule of the contract


def test_the_ids_of_the_v1_and_v2_runs_all_survived():
    """The artifacts of the earlier measurements stay comparable input by
    input, so the v3 numbers can be read against them."""
    assert {probe["id"] for probe in probes()} >= {
        "probe-tech",
        "probe-short",
        "probe-despair",
        "probe-tired-work",
        "probe-joy",
        "probe-family-uk",
        "probe-dialog",
    }


def test_the_new_stages_are_actually_covered():
    stages = [probe["stage"] for probe in probes()]

    assert stages.count("next") >= 4
    assert stages.count("reflect") >= 2
    # The two cases this ticket is about, by name.
    ids = {probe["id"] for probe in probes()}
    assert {"probe-next-language-switch", "probe-next-despair-older"} <= ids


# ---------------------------------------------------------------------------
# The tool builds what the endpoint builds
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("probe", probes(), ids=lambda probe: probe["id"])
def test_the_tool_assembles_the_production_message(probe):
    import question_prompt

    assert tool.user_message(probe) == question_prompt.build_user_message(
        probe["topic"],
        probe["stage"],
        [(message["role"], message["text"]) for message in probe["messages"]],
        language=(
            tool.detect_language(tool.language_source(probe))
            if tool.language_source(probe).strip()
            else "en"
        ),
    )


@pytest.mark.parametrize("probe", probes(), ids=lambda probe: probe["id"])
def test_the_tool_picks_the_same_texts_as_the_endpoint(probe):
    """The mirror of `twinkler_ai.language_source` / `safety_input_text`."""
    import twinkler_ai

    request = as_request(probe)
    assert tool.language_source(probe) == twinkler_ai.language_source(request)
    assert tool.safety_input(probe) == twinkler_ai.safety_input_text(request)


def test_a_scenario_becomes_the_request_the_app_would_send():
    """`scenarios.json` is frozen reference data — the mapping lives here."""
    scenarios = {
        scenario["id"]: scenario
        for scenario in json.loads(SCENARIOS.read_text(encoding="utf-8"))["scenarios"]
    }

    with_replies = tool.scenario_request(scenarios["ru-003"])
    assert with_replies["topic"] == "Умерла мама"
    assert with_replies["stage"] == "next"
    assert [message["role"] for message in with_replies["messages"]] == ["user", "user"]

    # A topic and nothing else is the opening question, not an empty `next`.
    assert tool.scenario_request(scenarios["ru-008"]) == {
        "topic": "Помоги",
        "stage": "first",
        "messages": [],
    }


def test_the_despair_input_is_skipped_and_the_older_one_is_not():
    """The split this ticket introduced, at the level of what is measured.

    `probe-despair` is answered in code, so there is no model answer to grade.
    `probe-next-despair-older` carries the same phrase two turns back, the
    endpoint DOES call the model for it, and so does the tool.
    """
    inputs, skipped = tool.load_inputs(SCENARIOS, PROBES)
    sent = {entry["id"] for entry in inputs}

    assert "probe-despair" in {entry["id"] for entry in skipped}
    assert "probe-next-despair-older" in sent
    # And the empty scenarios, for the reason the tool states.
    assert {"ru-009", "en-006", "uk-006"} <= {entry["id"] for entry in skipped}
    assert not sent & {"ru-009", "en-006", "uk-006"}


def test_every_asked_question_is_what_no_repeat_will_be_checked_against():
    inputs, _ = tool.load_inputs(SCENARIOS, PROBES)
    dialog = next(entry for entry in inputs if entry["id"] == "probe-dialog")
    older = next(
        entry for entry in inputs if entry["id"] == "probe-next-despair-older"
    )

    assert dialog["avoid_question"] == ["Что для тебя самое трудное в этом молчании?"]
    assert older["avoid_question"] == [
        "Что сейчас тяжелее всего?",
        "Что помогло тебе продержаться сегодня?",
    ]


# ---------------------------------------------------------------------------
# --dry-run
# ---------------------------------------------------------------------------

def test_dry_run_contacts_nothing_and_writes_nothing(monkeypatch, capsys, tmp_path):
    """The flag exists so the assembly can be reviewed before a paid run."""
    def refuse(*args, **kwargs):
        raise AssertionError("a dry run must not open an HTTP client")

    monkeypatch.setattr(tool.httpx, "Client", refuse)
    monkeypatch.chdir(tmp_path)

    assert tool.main(["--dry-run"]) == 0

    printed = capsys.readouterr().out
    assert "no provider contacted" in printed
    assert "Задай первый вопрос" in printed
    assert "Задай итоговый вопрос" in printed
    assert list(tmp_path.iterdir()) == []


def test_a_real_run_still_needs_a_provider_and_an_output(capsys):
    for argv in (["--out", "x.jsonl"], ["--provider", "gemini"]):
        with pytest.raises(SystemExit):
            tool.main(argv)


def test_the_dry_run_shows_what_the_despair_rule_reads(capsys):
    tool.main(["--dry-run", "--only", "probe-next-no-history"])

    printed = capsys.readouterr().out
    # `next` with no history: the topic is NOT substituted for a reply.
    assert "despair rule reads: nothing (no reply of theirs)" in printed


# ---------------------------------------------------------------------------
# Replacement series (ClickUp 86cbehyez)
# ---------------------------------------------------------------------------

def series_inputs() -> list[dict]:
    return json.loads(SERIES.read_text(encoding="utf-8"))["inputs"]


def series_only(*extra: str) -> list[str]:
    return ["--scenarios", "", "--probes", "", "--series", str(SERIES), *extra]


def test_every_series_input_is_a_request_the_endpoint_accepts():
    """The same guarantee the probe file has: a run must not measure a body
    `POST /api/ai/question` would answer with a 422."""
    payload = json.loads(SERIES.read_text(encoding="utf-8"))

    assert payload["version"] == "1.0.0"
    for item in payload["inputs"]:
        assert item["kind"] in ("single", "series"), item["id"]
        if item["kind"] == "series":
            assert item["replacements"] >= 2, item["id"]
        as_request(item)  # raises if the history breaks a rule of the contract


def test_the_journal_case_is_the_observed_one_verbatim():
    """`series-scale-ru` is quoted from Maria's prayer journal (86cbehtkh).

    Pinned word for word: the point of the input is that it is the case she
    reported, and an "improved" wording would make the baseline describe some
    other conversation.
    """
    case = next(item for item in series_inputs() if item["id"] == "series-scale-ru")

    assert case["stage"] == "next"
    assert case["replacements"] == 6
    assert case["topic"] == "Понять масштаб целей на завтра"
    assert [message["role"] for message in case["messages"]] == [
        "assistant", "user", "assistant", "user"
    ]
    texts = [message["text"] for message in case["messages"]]
    assert texts[0] == (
        "Что сейчас внутри тебя, когда ты только начинаешь молитву?"
    )
    assert texts[1].startswith("Я рада тому, что сегодня немало сделано.")
    assert texts[1].endswith("а то путаница.")
    assert texts[2] == (
        "А что, если завтра окажется, что всё, что ты сегодня считал готовым, "
        "всё ещё не совсем то, что нужно?"
    )
    assert texts[3] == (
        "Ну буду доделывать. Я все делаю для Господа, стараюсь сделать очень "
        "качественно"
    )


def test_a_series_becomes_that_many_calls_and_carries_the_person_s_words():
    inputs, skipped = tool.load_inputs(None, None, SERIES)

    assert skipped == []
    by_id = {entry["id"]: entry for entry in inputs}
    assert by_id["series-scale-ru"]["steps"] == 6
    assert by_id["series-scale-ru"]["is_series"] is True
    assert by_id["gratitude-ru-first"]["steps"] == 1
    assert by_id["gratitude-ru-first"]["is_series"] is False
    # The gender heuristic reads these from the artifact alone.
    assert by_id["series-exhaustion-uk"]["person_words"] == [
        "Немає сил, прошу відпочинку",
        "Третій місяць працюю без вихідних, бо колега звільнився. Учора "
        "заснула просто в одязі, навіть не вимкнула світло.",
    ]


def _run_series(monkeypatch, tmp_path, *extra: str):
    """Run the tool over one series with the provider call stubbed out."""
    sent: list[str] = []

    def fake_call(client, url, api_key, model, user, prompt, sampling=None):
        # `sampling` is the optional top_p/top_k of ClickUp 86cbehyf8: empty in
        # every run that measures a wording, which is every run here.
        assert not sampling
        sent.append(user)
        return f"Ответ номер {len(sent)}?"

    monkeypatch.setattr(tool, "call_qwen", fake_call)
    monkeypatch.setattr(
        tool, "call_gemini",
        lambda *a, **k: pytest.fail("the gemini path must not be used here"),
    )
    out = tmp_path / "run.jsonl"
    argv = series_only(
        "--only", "series-gratitude-ru", "--samples", "1",
        "--provider", "qwen", "--endpoint", "https://example.invalid/v1",
        "--model", "test-model", "--out", str(out), *extra,
    )
    assert tool.main(argv) == 0
    records = [
        json.loads(line)
        for line in out.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    meta = json.loads(
        (tmp_path / "run.jsonl.meta.json").read_text(encoding="utf-8")
    )
    return sent, records, meta


def test_every_replacement_sends_the_identical_body(monkeypatch, tmp_path):
    """The mechanism the ticket rests on: today a replacement is the SAME
    request, so the only thing that can differ between two of them is
    sampling. Asserted from the calls, not from the documentation."""
    sent, records, meta = _run_series(monkeypatch, tmp_path)

    assert len(sent) == 5 == len(records)
    assert len(set(sent)) == 1
    assert [record["step"] for record in records] == [1, 2, 3, 4, 5]
    assert {record["series_id"] for record in records} == {"series-gratitude-ru"}
    assert {record["sample"] for record in records} == {1}
    assert all(record["series_steps"] == 5 for record in records)
    assert all(record["skipped_questions"] == [] for record in records)
    assert meta["accumulate_skipped"] is False
    assert meta["records_expected"] == meta["records_written"] == 5
    assert meta["series"] == {"series-gratitude-ru": 5}
    # The sidecar names the host and never a key, a query string or a path.
    assert meta["endpoint"] == "https://example.invalid"
    assert "test-model" == meta["model"]


def test_accumulate_skipped_sends_them_in_the_request_field(monkeypatch, tmp_path):
    """The client of ADR 0015 — off by default, and visible when on.

    Until the field existed (ClickUp 86cbehyfe) this flag folded the replaced
    questions in as extra `assistant` turns, which put them under «Уже
    прозвучали вопросы:» — a different prompt from the one the endpoint sends
    now. It passes them as `skipped_questions`, so the block and the extra
    instruction sentence are the production ones (86cbehyf8).
    """
    sent, records, meta = _run_series(monkeypatch, tmp_path, "--accumulate-skipped")

    assert len(set(sent)) == 5  # every step now sends a different body
    assert "Ответ номер 1?" not in sent[0]
    assert "Ответ номер 1?" in sent[1]
    assert "Ответ номер 4?" in sent[4]
    # Their own block, not the "already asked" one: that list means a question
    # that was asked AND answered, and the two must stay distinguishable.
    asked, _, skipped = sent[4].partition("попросил заменить")
    assert "Разговор до этого:" in asked
    assert "Ответ номер 4?" in skipped and "Ответ номер 4?" not in asked
    assert "другой предмет размышления" in skipped
    assert records[4]["skipped_questions"] == [f"Ответ номер {n}?" for n in (1, 2, 3, 4)]
    assert meta["accumulate_skipped"] is True


def test_accumulating_stops_where_the_request_field_stops(monkeypatch, capsys):
    """A series longer than the field allows is refused, not silently sent.

    At step N the accumulating client carries N-1 skipped questions, and
    `CompleteRequest` accepts at most `MAX_SKIPPED_QUESTIONS` of them. This
    tool talks to the provider directly, so nothing else would notice: the run
    would quietly measure a body `POST /api/ai/question` answers with a 422.
    Today's inputs stop at 6 replacements, which is why this is a guard rather
    than a bug — and why the two constants are pinned to each other here.
    """
    import twinkler_ai

    assert tool.MAX_SKIPPED_QUESTIONS == twinkler_ai.MAX_SKIPPED_QUESTIONS

    # The real file first: 6 replacements carry 5 skipped questions, so nothing
    # in the repository trips the guard.
    assert tool.main(["--dry-run", *series_only("--accumulate-skipped")]) == 0

    def one_long_series(*_args, **_kwargs):
        entry = dict(
            next(e for e in tool.load_series(SERIES) if e["id"] == "series-scale-ru")
        )
        entry["steps"] = tool.MAX_SKIPPED_QUESTIONS + 2
        return [entry], []

    monkeypatch.setattr(tool, "load_inputs", one_long_series)
    with pytest.raises(SystemExit):
        tool.main(["--dry-run", *series_only("--accumulate-skipped")])
    assert "skipped_questions" in capsys.readouterr().err
    # Without the flag the same series is a legitimate run: every replacement
    # re-sends the identical body and the field is never populated.
    monkeypatch.setattr(tool, "load_inputs", one_long_series)
    assert tool.main(["--dry-run", *series_only()]) == 0


def test_a_candidate_variant_is_measured_and_named(monkeypatch, tmp_path):
    """`--prompt-variant` changes the bytes and says so in the artifact.

    The default is the production wording byte for byte — that is what every
    run before ClickUp 86cbehyf8 measured and what the flag must not disturb.
    """
    default_sent, default_records, default_meta = _run_series(monkeypatch, tmp_path)
    sent, records, meta = _run_series(
        monkeypatch, tmp_path / "variant", "--prompt-variant", "v3"
    )

    assert default_meta["prompt_variant"] == "production"
    assert all(r["prompt_variant"] == "production" for r in default_records)
    assert meta["prompt_variant"] == "v3"
    assert all(record["prompt_variant"] == "v3" for record in records)
    # v3 is the wording v4 replaced, so the two differ in exactly that sentence.
    assert "смотрит на ситуацию с другой стороны" in sent[0]
    assert "смотрит на ситуацию с другой стороны" not in default_sent[0]


def test_the_dry_run_says_which_bytes_a_replacement_repeats(capsys):
    assert tool.main(["--dry-run", *series_only("--only", "series-scale-ru")]) == 0

    printed = capsys.readouterr().out
    assert "series of 6 replacements" in printed
    assert "every replacement sends exactly these bytes again" in printed
    assert "Разговор до этого:" in printed


# ---------------------------------------------------------------------------
# Several candidates per call, or one more call (ClickUp 86cbehyg4)
# ---------------------------------------------------------------------------
# The pure halves of the experiment: how N answers are parsed out of one
# response, and which of them is shown to the person. Nothing here contacts a
# provider — the transport is `httpx.MockTransport` and the retry replay runs
# against a stubbed `chat_completion`.

import httpx  # noqa: E402

# A weak despair signal (`app/safety.py` tier 2): a question-shaped answer to
# it is replaced by the fixed reply, so a candidate carrying one is not an
# answer this endpoint may show.
WEAK_DESPAIR = "Я так устала жить"


def mock_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def completion_body(*texts: str, usage: dict | None = None) -> dict:
    return {
        "choices": [
            {"index": index, "message": {"role": "assistant", "content": text}}
            for index, text in enumerate(texts)
        ],
        "usage": usage
        or {"prompt_tokens": 872, "completion_tokens": 57, "total_tokens": 929},
    }


def test_one_candidate_sends_the_request_it_always_sent():
    """`n` is omitted at 1, so an ordinary run is byte for byte the old one."""
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200, json=completion_body("Вопрос?"))

    with mock_client(handler) as client:
        texts, usage = tool.chat_completion(
            client, "https://example.invalid/v1/chat/completions", "k", "m",
            "user", "prompt",
        )

    assert texts == ["Вопрос?"]
    assert usage == {
        "prompt_tokens": 872, "completion_tokens": 57, "total_tokens": 929
    }
    assert "n" not in seen[0]
    assert seen[0]["temperature"] == tool.TEMPERATURE
    assert seen[0]["max_tokens"] == tool.MAX_OUTPUT_TOKENS
    # And the historical single-answer entry point is the same call.
    with mock_client(handler) as client:
        assert tool.call_qwen(
            client, "https://example.invalid/v1/chat/completions", "k", "m",
            "user", "prompt",
        ) == "Вопрос?"


def test_n_candidates_are_asked_for_and_all_of_them_come_back():
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(
            200, json=completion_body("Первый?", "  ", "Третий?")
        )

    with mock_client(handler) as client:
        texts, _usage = tool.chat_completion(
            client, "https://example.invalid/v1/chat/completions", "", "m",
            "user", "prompt", None, 3,
        )

    assert seen[0]["n"] == 3
    # A blank choice is not an answer and never reaches the selection rule.
    assert texts == ["Первый?", "Третий?"]


@pytest.mark.parametrize(
    "body",
    [
        {"choices": []},
        {"object": "list"},
        {"choices": [{"index": 0, "message": {"content": "   "}}]},
    ],
)
def test_a_response_without_a_usable_answer_is_a_value_error(body):
    """The transport ladder retries on `ValueError`, exactly as it always did."""
    with mock_client(lambda request: httpx.Response(200, json=body)) as client:
        with pytest.raises(ValueError):
            tool.chat_completion(
                client, "https://example.invalid/v1/chat/completions", "", "m",
                "user", "prompt", None, 2,
            )


def test_shown_is_the_journal_plus_every_earlier_step():
    entry = {"avoid_question": ["Что сейчас внутри тебя?"]}

    assert tool.shown_questions(entry, ["Шаг один?", "  ", "Шаг два?"]) == [
        "Что сейчас внутри тебя?", "Шаг один?", "Шаг два?",
    ]
    assert tool.shown_questions({}, []) == []


def test_the_first_survivor_wins_and_the_disagreement_is_recorded():
    """The rule, and the one number that says what it cost.

    Candidate 0 repeats nothing and is taken. Candidate 2 is further from
    everything shown — that is exactly the choice this rule declines to make,
    so it is reported (`least_similar_index`, `disagreement`) instead.
    """
    shown = ["Что сейчас внутри тебя, когда ты начинаешь молитву?"]
    choice = tool.choose_candidate(
        [
            "Что тебе сейчас хочется сказать про сегодняшний день?",
            "Что сейчас внутри тебя, когда ты начинаешь молитву?",
            "Where did the day feel lightest?",
        ],
        shown,
        None,
    )

    assert choice["chosen_index"] == 0
    assert choice["selection"] == "first_survivor"
    assert choice["novel"] is True
    assert choice["least_similar_index"] == 2
    assert choice["disagreement"] is True
    # Candidate 1 is the shown question verbatim, so the filter flagged it.
    assert choice["candidate_kinds"][1] == "exact"
    assert choice["candidate_scores"][1] == 1.0


def test_when_every_candidate_repeats_the_least_similar_is_returned():
    """Production never withholds an answer (ADR 0016), and neither does this."""
    shown = ["А что значит для тебя «делать всё для Господа» в этой работе?"]
    choice = tool.choose_candidate(
        [
            "А что значит для тебя «делать всё для Господа» в этой работе?",
            "А что значит для тебя «делать всё для Господа» в этом деле?",
        ],
        shown,
        None,
    )

    assert choice["selection"] == "least_similar"
    assert choice["novel"] is False
    assert choice["chosen_index"] == 1
    assert choice["least_similar_index"] == 1
    # "Disagreement" is only meaningful while something survives.
    assert choice["disagreement"] is False


def test_a_candidate_the_despair_rule_would_replace_is_dropped():
    """Tier 2 runs on every answer, whichever mechanism produced it."""
    assert tool.tier2_replaces(WEAK_DESPAIR, "Что ты чувствуешь сейчас?")
    assert not tool.tier2_replaces(WEAK_DESPAIR, "Ты не одна в этом.")

    choice = tool.choose_candidate(
        ["Что ты чувствуешь сейчас?", "Ты не одна в этом."], [], WEAK_DESPAIR
    )

    assert choice["safety_dropped"] == [0]
    assert choice["chosen_index"] == 1
    assert choice["candidate_kinds"] == ["safety", "none"]


def test_when_the_despair_rule_would_replace_all_of_them_there_is_no_answer():
    choice = tool.choose_candidate(
        ["Что ты чувствуешь?", "А что было сегодня?"], [], WEAK_DESPAIR
    )

    assert choice["selection"] == "safety"
    assert choice["chosen_index"] is None
    assert choice["novel"] is True


def test_no_candidate_at_all_is_not_reported_as_a_safety_replacement():
    """`generate_one` never calls this with nothing, and the label says so."""
    choice = tool.choose_candidate([], [], None)

    assert choice["chosen_index"] is None
    assert choice["selection"] == "none"
    assert choice["candidate_scores"] == []
    assert choice["safety_dropped"] == []


def test_nothing_shown_means_the_model_s_own_first_answer():
    choice = tool.choose_candidate(["Первый?", "Второй?"], [], None)

    assert choice["chosen_index"] == 0
    assert choice["selection"] == "first_survivor"
    assert choice["disagreement"] is False


def _run_candidates(monkeypatch, tmp_path, answers, *extra: str):
    """Run one series with `chat_completion` stubbed: no provider, no key."""
    sent: list[str] = []
    served = list(answers)

    def fake(client, url, api_key, model, user, prompt, sampling=None, candidates=1):
        sent.append(user)
        texts = served.pop(0) if served else ["Запасной вопрос?"]
        return list(texts)[:candidates] or ["Запасной вопрос?"], {
            "prompt_tokens": 800, "completion_tokens": 30 * candidates,
        }

    monkeypatch.setattr(tool, "chat_completion", fake)
    monkeypatch.setattr(
        tool, "call_gemini",
        lambda *a, **k: pytest.fail("the gemini path must not be used here"),
    )
    out = tmp_path / "run.jsonl"
    argv = series_only(
        "--only", "series-gratitude-ru", "--samples", "1",
        "--provider", "qwen", "--endpoint", "https://example.invalid/v1",
        "--model", "test-model", "--out", str(out), *extra,
    )
    assert tool.main(argv) == 0
    records = [
        json.loads(line)
        for line in out.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    meta = json.loads(
        (tmp_path / "run.jsonl.meta.json").read_text(encoding="utf-8")
    )
    return sent, records, meta


def test_a_candidates_run_records_every_candidate_and_the_chosen_one(
    monkeypatch, tmp_path
):
    repeated = "Что сегодня было таким, за что хочется сказать спасибо?"
    sent, records, meta = _run_candidates(
        monkeypatch,
        tmp_path,
        # Step 1: the first candidate repeats the journal question verbatim.
        [[repeated, "А что из сегодняшнего хочется унести с собой?"]],
        "--candidates", "2",
    )

    assert len(sent) == 5  # one call per step, five steps
    first = records[0]
    assert first["mode"] == "candidates"
    assert first["candidates"] == [
        repeated, "А что из сегодняшнего хочется унести с собой?"
    ]
    assert first["chosen_index"] == 1
    assert first["text"] == "А что из сегодняшнего хочется унести с собой?"
    assert first["novel"] is True
    assert first["prompt_tokens"] == 800
    assert first["completion_tokens"] == 60
    assert [call["n"] for call in first["calls"]] == [2]
    assert meta["candidates"] == 2
    assert meta["retry_on_repeat"] is False
    assert meta["selection_rule"]
    # Step 2 sees step 1 as well: the person has read it.
    assert records[1]["shown_count"] == first["shown_count"] + 1


def test_the_retry_replay_makes_a_second_call_with_the_rejected_question(
    monkeypatch, tmp_path
):
    """Production (ADR 0016) replayed: the second call, and its cost."""
    repeated = "Что сегодня было таким, за что хочется сказать спасибо?"
    sent, records, meta = _run_candidates(
        monkeypatch,
        tmp_path,
        [[repeated], ["А что из сегодняшнего хочется унести с собой?"]],
        "--retry-on-repeat",
    )

    first = records[0]
    assert first["mode"] == "retry"
    assert first["selection"] == "retry_took_second"
    assert first["text"] == "А что из сегодняшнего хочется унести с собой?"
    assert first["novel"] is True
    assert len(first["calls"]) == 2
    # The rejected question reaches the model in the SECOND request only, and
    # in the skipped block — never in the "already asked" one, which means a
    # question that was asked AND answered. (The text itself also appears in
    # the journal history of this input, which is why the block is what is
    # asserted rather than the string.)
    assert "попросил заменить" not in sent[0]
    _asked, _, skipped_block = sent[1].partition("попросил заменить")
    assert repeated in skipped_block
    # A step that did not repeat costs one call and says so.
    assert records[1]["selection"] == "no_repeat"
    assert len(records[1]["calls"]) == 1
    assert meta["retry_on_repeat"] is True
    assert meta["candidates"] == 1


def test_the_retry_keeps_the_first_answer_when_the_second_repeats_too(
    monkeypatch, tmp_path
):
    """The other half of ADR 0016: the answer is never withheld.

    Production keeps the first text unless the second is novel or strictly less
    similar, and reports `novel: false`. This branch fired 10 times across the
    two measured retry runs, so it is not a hypothetical.
    """
    repeated = "Что сегодня было таким, за что хочется сказать спасибо?"
    _sent, records, _meta = _run_candidates(
        monkeypatch, tmp_path, [[repeated], [repeated]], "--retry-on-repeat",
    )

    first = records[0]
    assert first["selection"] == "retry_kept_first"
    assert first["chosen_index"] == 0
    assert first["text"] == repeated
    assert first["novel"] is False
    assert len(first["calls"]) == 2


def test_the_two_mechanisms_are_measured_one_per_run(capsys):
    for argv in (
        ["--candidates", "0"],
        ["--candidates", "2", "--retry-on-repeat"],
        ["--candidates", "2", "--provider", "gemini"],
    ):
        with pytest.raises(SystemExit):
            tool.main([*argv, "--out", "x.jsonl"])
