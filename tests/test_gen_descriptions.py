"""Tests for evaluation/gen_descriptions.py and description_prompts.py
(ClickUp 86cbeef7h, umbrella 86cbe4mtq).

The tool annotates the chunk corpus with the situations each fragment can
serve, either through an API (`generate`) or through an agent answering
prepared batches (`prepare` + `ingest`). Four properties are worth guarding,
because no successful run would reveal a break in any of them:

* a batch answer is accepted only when it carries EVERY id of that batch;
  what is missing becomes an error row, never a guess — the benchmark
  degrades exactly those fragments to `title_text` and counts them;
* the two modes share the prompt AND the validator, so an artifact made
  offline is the same document as one made through an API. A round trip
  proves it rather than a comment claiming it;
* a failed request must not write the endpoint URL into the artifact
  (`?key=…` lives there on several of the endpoints this tool is pointed at);
* the prompt must not name a state, situation or passage of `scenarios.json`
  — a prompt that knows the answers measures nothing.

No network: the httpx client is a stand-in. The module is loaded by path
because `evaluation/` is deliberately not on pytest's pythonpath.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import httpx
import pytest

EVALUATION = Path(__file__).resolve().parents[1] / "evaluation"
TOOL = EVALUATION / "gen_descriptions.py"

pytestmark = pytest.mark.skipif(
    not TOOL.exists(), reason="evaluation/ is not present in this container copy"
)


def _load_tool():
    # `evaluation/` must be importable: the tool imports description_prompts.
    if str(EVALUATION) not in sys.path:
        sys.path.insert(0, str(EVALUATION))
    spec = importlib.util.spec_from_file_location("gen_descriptions", TOOL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def tool():
    return _load_tool()


@pytest.fixture(scope="module")
def prompts(tool):
    import description_prompts

    return description_prompts


CHUNKS = [
    {"canonical_id": "v3:01.001.001-008", "chunking_version": 3,
     "translation": 16, "alias": "bsb", "language": "en", "book_number": 1,
     "chapter_number": 1, "verse_number_start": 1, "verse_number_end": 8,
     "title": "Creation", "text": "In the beginning God created…"},
    {"canonical_id": "v3:01.001.009-019", "chunking_version": 3,
     "translation": 16, "alias": "bsb", "language": "en", "book_number": 1,
     "chapter_number": 1, "verse_number_start": 9, "verse_number_end": 19,
     "title": "", "text": "And God said, Let there be lights…"},
    {"canonical_id": "v3:19.023.001-006", "chunking_version": 3,
     "translation": 1, "alias": "syn", "language": "ru", "book_number": 19,
     "chapter_number": 23, "verse_number_start": 1, "verse_number_end": 6,
     "title": "Псалом Давида", "text": "Господь — Пастырь мой…"},
]


def _answer(ids, senses=("для того, кто ищет опоры", "для того, кто благодарит")):
    return json.dumps({"descriptions": [
        {"id": i, "senses": list(senses), "caution": False, "caution_note": ""}
        for i in ids
    ]}, ensure_ascii=False)


class FakeResponse:
    def __init__(self, text: str):
        self._text = text

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"choices": [{"message": {"content": self._text}}]}


class FakeClient:
    """Returns the queued answers in order; records what was sent."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.requests = []

    def post(self, url, json=None, headers=None, **_kwargs):
        self.requests.append({"url": url, "payload": json, "headers": headers})
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return FakeResponse(answer)


def _args(tool, **overrides):
    defaults = dict(
        endpoint="http://localhost:11434/v1", model="qwen3-4b", api_key="",
        temperature=0.0, max_tokens=tool.DEFAULT_MAX_TOKENS,
        batch_size=tool.DEFAULT_BATCH_SIZE, out="out.jsonl",
        chunks="bench_data/chunks.jsonl", only_translation="", limit=0,
        ids="", timeout=1.0, resume=False,
    )
    return argparse.Namespace(**{**defaults, **overrides})


# ---------------------------------------------------------------------------
# The batch contract: every id back, or an error row
# ---------------------------------------------------------------------------

def test_a_complete_answer_annotates_every_fragment_of_the_batch(tool):
    batch = CHUNKS[:2]
    client = FakeClient([_answer([1, 2])])
    records = tool.generate_batch(client, "http://x/chat/completions",
                                  _args(tool), batch)
    assert [r["canonical_id"] for r in records] == \
        [c["canonical_id"] for c in batch]
    assert all(r["error"] is None for r in records)
    assert all(len(r["senses"]) == 2 for r in records)
    assert records[0]["attempts"] == 1
    assert "warning" not in records[0]


def test_an_incomplete_answer_is_retried_once_and_the_rest_errors(tool):
    # Both attempts answer only fragment 1: fragment 2 becomes an error row
    # rather than a guess, and the benchmark degrades exactly it.
    client = FakeClient([_answer([1]), _answer([1])])
    records = tool.generate_batch(client, "http://x/chat/completions",
                                  _args(tool), CHUNKS[:2])
    assert records[0]["error"] is None
    assert records[1]["senses"] == [] and records[1]["error"].startswith(
        "incomplete:")
    assert records[0]["attempts"] == tool.PARSE_ATTEMPTS == 2


def test_a_second_attempt_completes_the_batch(tool):
    # A retry can only ADD: what the first answer got right is kept.
    client = FakeClient([_answer([1]), _answer([2])])
    records = tool.generate_batch(client, "http://x/chat/completions",
                                  _args(tool), CHUNKS[:2])
    assert [r["error"] for r in records] == [None, None]
    assert records[0]["attempts"] == 2


def test_a_broken_answer_is_retried_once_and_then_errors(tool):
    client = FakeClient(["not json at all", "still not json"])
    records = tool.generate_batch(client, "http://x/chat/completions",
                                  _args(tool), CHUNKS[:2])
    assert all(r["senses"] == [] for r in records)
    assert all(r["error"].startswith("parse:") for r in records)


def test_an_id_from_another_batch_is_ignored(tool):
    # Attaching senses to the wrong fragment is worse than missing ones.
    client = FakeClient([_answer([7, 8]), _answer([7, 8])])
    records = tool.generate_batch(client, "http://x/chat/completions",
                                  _args(tool), CHUNKS[:2])
    assert all(r["senses"] == [] for r in records)


def test_the_request_carries_the_versioned_prompt_and_json_mode(tool, prompts):
    client = FakeClient([_answer([1, 2])])
    tool.generate_batch(client, "http://x/chat/completions",
                        _args(tool, api_key="secret"), CHUNKS[:2])
    payload = client.requests[0]["payload"]
    assert payload["messages"][0]["content"] == \
        prompts.build_description_instruction("en", 2)
    assert "In the beginning God created" in payload["messages"][1]["content"]
    assert payload["temperature"] == 0.0
    assert payload["response_format"] == {"type": "json_object"}
    assert client.requests[0]["headers"]["Authorization"] == "Bearer secret"


def test_a_reasoning_block_before_the_json_is_stripped(tool):
    client = FakeClient(['<think>{"descriptions": []}</think>' + _answer([1])])
    records = tool.generate_batch(client, "http://x/chat/completions",
                                  _args(tool), CHUNKS[:1])
    assert records[0]["error"] is None


def test_a_single_sense_is_a_warning_not_an_error(tool, prompts):
    client = FakeClient([_answer([1], senses=("для одинокого вечера",))])
    records = tool.generate_batch(client, "http://x/chat/completions",
                                  _args(tool), CHUNKS[:1])
    assert records[0]["error"] is None
    assert f"of {prompts.MIN_SENSES}+" in records[0]["warning"]


def test_a_coordinate_inside_a_sense_is_recorded_as_a_leak(tool):
    client = FakeClient([_answer([1], senses=("см. 23:1 для скорбящего", "и ещё"))])
    records = tool.generate_batch(client, "http://x/chat/completions",
                                  _args(tool), CHUNKS[:1])
    assert records[0]["error"] is None
    assert records[0]["reference_leaks"] == ["23:1"]


# ---------------------------------------------------------------------------
# caution is a structured field
# ---------------------------------------------------------------------------

def test_caution_is_parsed_as_a_flag_with_its_note(prompts):
    answer = json.dumps({"descriptions": [{
        "id": 1, "senses": ["для того, кто держится"],
        "caution": True, "caution_note": "звучит как суд",
    }]}, ensure_ascii=False)
    found = prompts.parse_description_response(answer, [1])
    assert found[1]["caution"] is True
    assert found[1]["caution_note"] == "звучит как суд"


def test_a_string_caution_and_a_note_without_a_flag_are_both_read(prompts):
    string_flag = json.dumps({"descriptions": [
        {"id": 1, "senses": ["a"], "caution": "true", "caution_note": "хлёстко"},
        {"id": 2, "senses": ["b"], "caution_note": "образы смерти"},
    ]}, ensure_ascii=False)
    found = prompts.parse_description_response(string_flag, [1, 2])
    assert found[1]["caution"] is True and found[2]["caution"] is True


def test_no_caution_leaves_the_note_empty(prompts):
    answer = json.dumps({"descriptions": [{
        "id": 1, "senses": ["a"], "caution": False,
        "caution_note": "должно быть пусто",
    }]}, ensure_ascii=False)
    found = prompts.parse_description_response(answer, [1])
    assert found[1]["caution"] is False and found[1]["caution_note"] == ""


def test_structural_garbage_raises_but_a_gap_does_not(prompts):
    with pytest.raises(prompts.DescriptionError):
        prompts.parse_description_response("no json here", [1])
    with pytest.raises(prompts.DescriptionError):
        prompts.parse_description_response('{"other": []}', [1])
    # An incomplete answer is NOT an exception: the caller retries and then
    # records the gap.
    found = prompts.parse_description_response(_answer([1]), [1, 2])
    assert set(found) == {1}


def test_more_senses_than_the_contract_allows_are_trimmed(prompts):
    many = [f"смысл {i}" for i in range(prompts.MAX_SENSES + 3)]
    answer = json.dumps({"descriptions": [
        {"id": 1, "senses": many, "caution": False, "caution_note": ""}
    ]}, ensure_ascii=False)
    found = prompts.parse_description_response(answer, [1])
    assert len(found[1]["senses"]) == prompts.MAX_SENSES


# ---------------------------------------------------------------------------
# The artifact: no key, no URL, derived `partial`, resume
# ---------------------------------------------------------------------------

def _status_error(url: str, status: int = 400) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", url)
    return httpx.HTTPStatusError(
        f"Client error '{status}' for url '{url}'",
        request=request, response=httpx.Response(status, request=request),
    )


def test_an_http_error_never_leaks_the_endpoint_url(tool, monkeypatch):
    monkeypatch.setattr(tool.time, "sleep", lambda _s: None)
    url = "https://llm.example.com/v1/chat/completions?key=SECRET-TOKEN"
    client = FakeClient([_status_error(url)] * tool.TRANSPORT_ATTEMPTS)
    records = tool.generate_batch(client, url, _args(tool), CHUNKS[:1])

    assert records[0]["error"] == "transport: HTTPStatusError (HTTP 400)"
    assert records[0]["attempts"] == tool.TRANSPORT_ATTEMPTS == 3
    meta = tool.build_meta(
        _args(tool, endpoint=url, api_key="SECRET-TOKEN"), 11960, 3,
        tool.empty_stats(), "api")
    dumped = json.dumps({"meta": meta, "rows": records}, ensure_ascii=False)
    assert "SECRET-TOKEN" not in dumped
    assert "llm.example.com/v1" not in dumped


def test_transport_error_keeps_the_status_and_drops_the_message(tool):
    assert tool.transport_error(_status_error("http://x?key=S", 429)) == \
        "transport: HTTPStatusError (HTTP 429)"
    assert tool.transport_error(httpx.ConnectError("boom to 10.0.0.1")) == \
        "transport: ConnectError"


def test_partial_is_derived_from_the_corpus_size(tool):
    stats = tool.empty_stats()
    stats["annotated"] = 11960
    full = tool.build_meta(_args(tool), 11960, 3, stats, "api")
    assert full["partial"] is False and "note" not in full
    stats["annotated"] = 60
    partial = tool.build_meta(_args(tool), 11960, 3, stats, "api")
    assert partial["partial"] is True
    assert partial["note"].startswith("PARTIAL run")
    assert partial["prompt_version"] == \
        __import__("description_prompts").DESCRIPTION_PROMPT_VERSION


def test_resume_skips_annotated_fragments_and_retries_failed_ones(
    tool, tmp_path
):
    out = tmp_path / "senses.jsonl"
    tool.append_records(out, [
        {"canonical_id": "v3:01.001.001-008", "translation": 16,
         "senses": ["ok"], "error": None},
        {"canonical_id": "v3:01.001.009-019", "translation": 16,
         "senses": [], "error": "incomplete: …"},
    ])
    done = tool.load_existing(out)
    assert done == {"16:v3:01.001.001-008"}


def test_a_truncated_last_line_is_skipped_not_fatal(tool, tmp_path):
    out = tmp_path / "senses.jsonl"
    out.write_text(
        json.dumps({"canonical_id": "a", "translation": 1, "senses": ["x"]})
        + '\n{"canonical_id": "b", "transl',
        encoding="utf-8",
    )
    assert tool.load_existing(out) == {"1:a"}


def test_the_meta_sidecar_is_written_atomically(tool, tmp_path):
    out = tmp_path / "senses.jsonl"
    tool.write_meta(out, {"model": "x"})
    assert json.loads(tool.meta_path(out).read_text()) == {"model": "x"}
    assert not list(tmp_path.glob("*.tmp"))


def test_batches_never_cross_a_translation(tool):
    """A batch is ONE prompt with ONE language fixed in its instruction.

    Regression: the slicing ran over the flat selection, so a group boundary
    inside a batch handed English fragments to a Russian instruction (batches
    b00003 and b00006 of the first probe file).
    """
    corpus = CHUNKS[:2] + [dict(CHUNKS[2]), dict(CHUNKS[2])]
    corpus[3]["canonical_id"] = "v3:19.023.007-012"
    # selection order is grouped by translation: 2 ru, then 2 en
    selected = tool.select_chunks(corpus, "", 0, set())
    assert [c["translation"] for c in selected] == [1, 1, 16, 16]
    # a batch size that would straddle the boundary on a flat slice
    batches = tool.make_batches(selected, 3)
    assert [[c["translation"] for c in b] for b in batches] == [[1, 1], [16, 16]]
    for batch in batches:
        assert tool.batch_language(batch) in ("ru", "en")


def test_a_batch_of_two_languages_is_an_assertion_not_a_warning(tool):
    with pytest.raises(AssertionError):
        tool.batch_language([CHUNKS[0], CHUNKS[2]])


def test_prepared_batches_hold_one_language_each(tool, corpus_file, tmp_path):
    batches = tmp_path / "batches.jsonl"
    tool.main(["prepare", "--chunks", str(corpus_file), "--batch-size", "2",
               "--out", str(batches)])
    rows = [json.loads(line) for line in batches.read_text().splitlines()]
    for row in rows:
        languages = {item["language"] for item in row["items"]}
        assert languages == {row["language"]}
        assert len({item["translation"] for item in row["items"]}) == 1


def test_limit_is_per_translation_and_ids_override_it(tool):
    picked = tool.select_chunks(CHUNKS, "", 1, set())
    assert {c["translation"] for c in picked} == {1, 16}
    assert len(picked) == 2
    named = tool.select_chunks(CHUNKS, "", 1, {"v3:19.023.001-006"})
    assert [c["canonical_id"] for c in named] == ["v3:19.023.001-006"]


# ---------------------------------------------------------------------------
# prepare -> ingest is the same artifact as the API mode
# ---------------------------------------------------------------------------

@pytest.fixture
def corpus_file(tmp_path):
    path = tmp_path / "chunks.jsonl"
    path.write_text(
        "".join(json.dumps(c, ensure_ascii=False) + "\n" for c in CHUNKS),
        encoding="utf-8",
    )
    return path


def test_prepare_writes_the_same_prompt_the_api_mode_sends(
    tool, prompts, corpus_file, tmp_path
):
    batches = tmp_path / "batches.jsonl"
    tool.main(["prepare", "--chunks", str(corpus_file),
               "--only-translation", "bsb", "--batch-size", "2",
               "--out", str(batches)])
    rows = [json.loads(line) for line in batches.read_text().splitlines()]
    assert len(rows) == 1
    row = rows[0]
    assert row["instruction"] == prompts.build_description_instruction("en", 2)
    assert row["expected_ids"] == [1, 2]
    assert row["prompt_version"] == prompts.DESCRIPTION_PROMPT_VERSION
    # the local id is what the answer must carry; the canonical id travels
    # beside it so a batch line can be traced to its fragment
    assert [i["id"] for i in row["items"]] == [1, 2]
    assert [i["canonical_id"] for i in row["items"]] == \
        [c["canonical_id"] for c in CHUNKS[:2]]


def test_prepare_ingest_round_trip_equals_the_api_mode(
    tool, corpus_file, tmp_path
):
    """The whole point of the offline mode: the same document, either way."""
    batches = tmp_path / "batches.jsonl"
    tool.main(["prepare", "--chunks", str(corpus_file),
               "--only-translation", "bsb", "--batch-size", "2",
               "--out", str(batches)])
    batch = json.loads(batches.read_text().splitlines()[0])
    answer_text = _answer([1, 2])

    answers = tmp_path / "answers.jsonl"
    answers.write_text(json.dumps({
        "batch_id": batch["batch_id"], "response": json.loads(answer_text),
    }, ensure_ascii=False) + "\n", encoding="utf-8")
    out = tmp_path / "senses.jsonl"
    tool.main(["ingest", "--chunks", str(corpus_file),
               "--batches", str(batches), "--answers", str(answers),
               "--out", str(out), "--model", "agent-model"])
    offline = [json.loads(line) for line in out.read_text().splitlines()]

    online = tool.generate_batch(
        FakeClient([answer_text]), "http://x/chat/completions",
        _args(tool), CHUNKS[:2])

    # latency and attempts are properties of HOW the answer was obtained
    volatile = ("latency_ms", "attempts")
    assert [{k: v for k, v in r.items() if k not in volatile} for r in offline] \
        == [{k: v for k, v in r.items() if k not in volatile} for r in online]
    meta = json.loads(tool.meta_path(out).read_text())
    assert meta["mode"] == "offline" and meta["model"] == "agent-model"


def test_ingest_accepts_a_bare_descriptions_list_as_shorthand(
    tool, corpus_file, tmp_path
):
    """The shorthand row shape README does not advertise, but agents write."""
    batches = tmp_path / "batches.jsonl"
    tool.main(["prepare", "--chunks", str(corpus_file),
               "--only-translation", "bsb", "--batch-size", "2",
               "--out", str(batches)])
    batch = json.loads(batches.read_text().splitlines()[0])
    answers = tmp_path / "answers.jsonl"
    answers.write_text(json.dumps({
        "batch_id": batch["batch_id"],
        "descriptions": json.loads(_answer([1, 2]))["descriptions"],
    }, ensure_ascii=False) + "\n", encoding="utf-8")
    out = tmp_path / "senses.jsonl"
    tool.main(["ingest", "--chunks", str(corpus_file),
               "--batches", str(batches), "--answers", str(answers),
               "--out", str(out), "--model", "agent-model"])
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    assert [r["error"] for r in rows] == [None, None]


def test_ingest_refuses_an_answer_row_with_neither_key(
    tool, corpus_file, tmp_path
):
    batches = tmp_path / "batches.jsonl"
    tool.main(["prepare", "--chunks", str(corpus_file), "--batch-size", "2",
               "--out", str(batches)])
    batch = json.loads(batches.read_text().splitlines()[0])
    answers = tmp_path / "answers.jsonl"
    answers.write_text(
        json.dumps({"batch_id": batch["batch_id"], "senses": []}) + "\n",
        encoding="utf-8")
    with pytest.raises(SystemExit):
        tool.main(["ingest", "--chunks", str(corpus_file),
                   "--batches", str(batches), "--answers", str(answers),
                   "--out", str(tmp_path / "s.jsonl"), "--model", "m"])


def test_ingest_reports_missing_broken_and_incomplete_batches(
    tool, corpus_file, tmp_path, capsys
):
    batches = tmp_path / "batches.jsonl"
    tool.main(["prepare", "--chunks", str(corpus_file), "--batch-size", "1",
               "--out", str(batches)])
    rows = [json.loads(line) for line in batches.read_text().splitlines()]
    assert len(rows) == 3
    answers = tmp_path / "answers.jsonl"
    answers.write_text(
        json.dumps({"batch_id": rows[0]["batch_id"],
                    "response": json.loads(_answer([1]))}) + "\n"
        + json.dumps({"batch_id": rows[1]["batch_id"],
                      "response": "not json at all"}) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "senses.jsonl"
    tool.main(["ingest", "--chunks", str(corpus_file),
               "--batches", str(batches), "--answers", str(answers),
               "--out", str(out), "--model", "agent-model"])
    meta = json.loads(tool.meta_path(out).read_text())
    assert meta["batches_broken"] == [rows[1]["batch_id"]]
    assert meta["batches_missing"] == [rows[2]["batch_id"]]
    assert meta["annotated"] == 1 and meta["errors"] == 2
    assert "ingest --resume" in capsys.readouterr().out


def test_ingest_resume_adds_a_re_answered_batch_only(
    tool, corpus_file, tmp_path
):
    batches = tmp_path / "batches.jsonl"
    tool.main(["prepare", "--chunks", str(corpus_file), "--batch-size", "1",
               "--out", str(batches)])
    rows = [json.loads(line) for line in batches.read_text().splitlines()]
    # batches come out grouped by translation code, so b00001 is the Russian
    # fragment and b00002/b00003 are the two English ones
    ordered = [row["items"][0]["canonical_id"] for row in rows]
    assert ordered == [CHUNKS[2]["canonical_id"], CHUNKS[0]["canonical_id"],
                       CHUNKS[1]["canonical_id"]]
    answers = tmp_path / "answers.jsonl"
    answers.write_text(json.dumps({
        "batch_id": rows[0]["batch_id"], "response": json.loads(_answer([1])),
    }) + "\n", encoding="utf-8")
    out = tmp_path / "senses.jsonl"
    common = ["--chunks", str(corpus_file), "--batches", str(batches),
              "--answers", str(answers), "--out", str(out),
              "--model", "agent-model"]
    tool.main(["ingest"] + common)
    assert json.loads(tool.meta_path(out).read_text())["annotated"] == 1

    with answers.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "batch_id": rows[1]["batch_id"],
            "response": json.loads(_answer([1])),
        }) + "\n")
    tool.main(["ingest", "--resume"] + common)
    written = [json.loads(line) for line in out.read_text().splitlines()]
    # the already-annotated batch is not rewritten, the newly answered one is
    # appended, and the still-unanswered one is retried (an error row again)
    assert [r["canonical_id"] for r in written] == ordered + [
        CHUNKS[0]["canonical_id"], CHUNKS[1]["canonical_id"],
    ]
    assert [r["error"] is None for r in written] == \
        [True, False, False, True, False]
    assert json.loads(tool.meta_path(out).read_text())["annotated"] == 2


# ---------------------------------------------------------------------------
# De-fingerprinting: the prompt must not know the evaluation set
# ---------------------------------------------------------------------------

def test_the_instruction_names_no_topic_of_the_evaluation_set(prompts):
    dataset = json.loads(
        (EVALUATION / "scenarios.json").read_text(encoding="utf-8"))
    topics = [
        s["prayer_context"]["topic"].strip().lower()
        for s in dataset["scenarios"] if s["prayer_context"]["topic"].strip()
    ]
    for language in prompts.LANGUAGE_NAMES:
        text = prompts.build_description_instruction(language, 8).lower()
        hits = [topic for topic in topics if topic in text]
        assert not hits, f"[{language}] evaluation topics leaked: {hits}"


def test_the_instruction_names_no_book_and_no_coordinate(prompts):
    from canon import CANONICAL_BOOKS

    for language in prompts.LANGUAGE_NAMES:
        text = prompts.build_description_instruction(language, 8)
        assert not prompts.find_reference_leaks(text)
        lowered = text.lower()
        hits = [
            code for _number, code, _chapters in CANONICAL_BOOKS
            # three-letter book codes are too short to match on their own;
            # what would leak is a full reference, and that is the check above
            if f" {code} " in lowered
        ]
        assert not hits, f"[{language}] book codes leaked: {hits}"


def test_the_instruction_forbids_the_first_person_and_repetition(prompts):
    """Prompt v2: the two failures the 4B probe of 2026-09-04 showed."""
    text = prompts.build_description_instruction("ru", 8)
    assert "THIRD person" in text
    assert "first person" in text
    assert "same situation in different words are a wrong answer" in text
    assert prompts.DESCRIPTION_PROMPT_VERSION >= 2


def test_prompt_v3_requires_present_day_prayer_situations(prompts):
    """Pilot regression: generic plot actors are still a plot retelling."""
    text = prompts.build_description_instruction("ru", 8)
    assert "At least two senses" in text
    assert "PRESENT-DAY prayer situation" in text
    assert "use only two when it honestly supports no more" in text
    assert "give fewer when the passage honestly meets fewer" not in text
    assert "People and animals suffering from an insect plague" in text
    assert "a young man thrown into a pit and sold into slavery" in text
    assert "A person betrayed by people close to them out of envy" in text
    assert prompts.DESCRIPTION_PROMPT_VERSION >= 3


def test_prompt_v3_calibrates_caution_to_reader_danger(prompts):
    """Pilot regression: historical harm alone must not flag caution."""
    text = prompts.build_description_instruction("en", 8)
    assert "ONLY when a person already suffering" in text
    assert "dangerous words directed at them" in text
    assert "punishment presented as the explanation of their suffering" in text
    assert "hopeless lament without consolation" in text
    assert "historical violence involving third parties" in text
    assert "rituals or sacrifices" in text
    assert "blessing that conditionally mentions a curse on enemies" in text
    assert '"Shepherds are despised here" remains false' in text


def test_prompt_v4_keeps_direct_threats_inside_ritual_laws_cautious(prompts):
    """Control-shard regression: ritual context must not hide a threat."""
    text = prompts.build_description_instruction("en", 8)
    assert "A ritual or sacrifice by itself is not caution" in text
    assert "if a ritual law separately addresses its participant" in text
    assert "direct threat of death, being destroyed or punished" in text
    assert "directly accuses them" in text
    assert 'still requires "caution": true' in text
    assert "Do not let the ritual context hide it" in text
    assert prompts.DESCRIPTION_PROMPT_VERSION == 4


def test_an_unsupported_language_is_refused(prompts):
    with pytest.raises(ValueError):
        prompts.build_description_instruction("de", 4)
