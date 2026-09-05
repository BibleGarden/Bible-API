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
    assert "Задай первый наводящий вопрос" in printed
    assert "Молитва закончилась" in printed
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

    def fake_call(client, url, api_key, model, user, prompt):
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


def test_accumulate_skipped_folds_the_previous_questions_in(monkeypatch, tmp_path):
    """The preview of subtask 86cbehyfe — off by default, and visible when on."""
    sent, records, meta = _run_series(monkeypatch, tmp_path, "--accumulate-skipped")

    assert len(set(sent)) == 5  # every step now sends a different body
    assert "Ответ номер 1?" not in sent[0]
    assert "Ответ номер 1?" in sent[1]
    assert "Ответ номер 4?" in sent[4]
    # They land in the block the production message builder renders them in.
    asked, _, answered = sent[4].partition("Что человек ответил")
    assert "Уже прозвучали вопросы:" in asked
    assert "Ответ номер 4?" in asked and "Ответ номер 4?" not in answered
    assert records[4]["skipped_questions"] == [f"Ответ номер {n}?" for n in (1, 2, 3, 4)]
    assert meta["accumulate_skipped"] is True


def test_the_dry_run_says_which_bytes_a_replacement_repeats(capsys):
    assert tool.main(["--dry-run", *series_only("--only", "series-scale-ru")]) == 0

    printed = capsys.readouterr().out
    assert "series of 6 replacements" in printed
    assert "every replacement sends exactly these bytes again" in printed
    assert "Уже прозвучали вопросы:" in printed
