"""The v6 answer contract: `{"subject": …, "question": …}` (ClickUp 86cbejvt2).

Four rungs, and what is pinned here is which one reads which answer — the
`question format: parsed=…` line the endpoint logs is only worth reading if
`json` really means the model obeyed and `raw` really means it did not.
"""

import json

import pytest

import question_format
from question_format import JSON, RAW, REGEX, REPAIRED, parse_question


def test_the_contract_answer_is_read_as_json():
    answer = json.dumps(
        {"subject": "масштаб завтрашних дел", "question": "Что для тебя главное завтра?"},
        ensure_ascii=False,
    )

    parsed = parse_question(answer)

    assert parsed.kind == JSON
    assert parsed.parsed
    assert parsed.question == "Что для тебя главное завтра?"
    assert parsed.subject == "масштаб завтрашних дел"


def test_the_fields_may_arrive_in_either_order_and_inside_prose():
    """A fence or a sentence around the object is not a broken answer."""
    answer = (
        "Here you go:\n```json\n"
        '{"question": "What matters most tomorrow?", "subject": "tomorrow plan"}\n'
        "```\n"
    )

    parsed = parse_question(answer)

    assert parsed.kind == JSON
    assert parsed.question == "What matters most tomorrow?"
    assert parsed.subject == "tomorrow plan"


def test_a_missing_subject_is_none_and_the_question_still_stands():
    parsed = parse_question('{"question": "Что ты хочешь успеть?"}')

    assert parsed.kind == JSON
    assert parsed.question == "Что ты хочешь успеть?"
    assert parsed.subject is None


@pytest.mark.parametrize(
    "answer",
    [
        # A trailing comma — the malformation every model emits sooner or later.
        '{"subject": "план", "question": "Что ты хочешь успеть?",}',
        # A closer of the wrong type.
        '{"subject": "план", "question": "Что ты хочешь успеть?"]',
    ],
)
def test_bounded_breakage_is_repaired_by_the_shared_repairer(answer):
    """`json_repair.repair_json_object`, the rewrite stage's own (86cbe4nd3).

    Imported rather than re-typed, so the endpoint and the retrieval pipeline
    accept exactly the same repairs.
    """
    parsed = parse_question(answer)

    assert parsed.kind == REPAIRED
    assert parsed.question == "Что ты хочешь успеть?"
    assert parsed.subject == "план"


def test_a_truncated_object_is_closed_rather_than_refused():
    parsed = parse_question('{"subject": "план", "question": "Что ты хочешь успеть?"')

    assert parsed.kind == REPAIRED
    assert parsed.question == "Что ты хочешь успеть?"


def test_an_unrepairable_object_still_yields_the_question_by_regex():
    """The third rung: no object, but the field is visible.

    A cut inside a string is what `repair_json_object` refuses to touch (it
    would invent the end of a sentence), and yet the *question* here is
    complete — so it is read rather than thrown away.
    """
    answer = (
        '{"subject": "план", "question": "Что ты хочешь успеть?", "note": "не закрыт'
    )

    parsed = parse_question(answer)

    assert parsed.kind == REGEX
    assert parsed.question == "Что ты хочешь успеть?"
    assert parsed.subject == "план"


def test_the_regex_rung_honours_json_escapes():
    answer = 'blah "question": "Что значит \\"готово\\" для тебя?" blah'

    parsed = parse_question(answer)

    assert parsed.kind == REGEX
    assert parsed.question == 'Что значит "готово" для тебя?'


@pytest.mark.parametrize(
    ("answer", "question"),
    [
        # The v5 answer shape: a bare line. Still an answer, still shown.
        ("Что для тебя главное завтра?", "Что для тебя главное завтра?"),
        # Wrapping quotes the prompt has always had to ask against.
        ('«Что для тебя главное завтра?»', "Что для тебя главное завтра?"),
        ('"Что для тебя главное завтра?"', "Что для тебя главное завтра?"),
        # Only the first line: an explanation below the question is the shape
        # the format section forbids, and showing it is worse than dropping it.
        (
            "Что для тебя главное завтра?\nЯ спрашиваю, потому что…",
            "Что для тебя главное завтра?",
        ),
    ],
)
def test_an_answer_that_ignores_the_contract_is_raw_but_never_lost(answer, question):
    parsed = parse_question(answer)

    assert parsed.kind == RAW
    assert not parsed.parsed
    assert parsed.question == question
    assert parsed.subject is None


@pytest.mark.parametrize(
    ("answer", "kind", "question", "subject"),
    [
        # The five shapes the review of 86cbejvt2 collected. Three of them are
        # perfectly good questions wearing broken clothes and must reach the
        # person; two carry no question at all and must reach nobody.
        ('{"quesiton": "Что для тебя главное завтра?"}', REGEX,
         "Что для тебя главное завтра?", None),
        ("{'question': 'Что для тебя главное завтра?'}", REGEX,
         "Что для тебя главное завтра?", None),
        ("**subject**: завтрашние дела\n**question**: Что для тебя главное завтра?",
         REGEX, "Что для тебя главное завтра?", "завтрашние дела"),
        ('{"question": ""}', RAW, "", None),
        ('{"subject": "тема", "question": null}', RAW, "", None),
    ],
)
def test_the_shapes_the_review_collected(answer, kind, question, subject):
    parsed = parse_question(answer)

    assert parsed.kind == kind
    assert parsed.question == question
    assert parsed.subject == subject
    assert "{" not in parsed.question


def test_a_question_about_a_question_is_not_mistaken_for_machinery():
    """The narrow rule: a field name counts only when a colon follows it.

    An English question may legitimately be about a question or a subject, and
    the first version of this guard quietly deleted the word from the text.
    """
    answer = "What question do you want to bring to God?"

    parsed = parse_question(answer)

    assert parsed.question == answer
    assert not question_format.looks_like_machinery(answer)
    assert question_format.looks_like_machinery('"question": "x"')
    assert question_format.looks_like_machinery("**question**: x")
    assert question_format.looks_like_machinery('{"a": 1}')


def test_an_envelope_around_a_real_question_is_salvaged_without_braces():
    """If the retry also fails, what is shown must still be a sentence."""
    answer = '{"subject": "план", "oops": Что для тебя главное завтра?'

    parsed = parse_question(answer)

    assert not parsed.parsed
    assert parsed.question == "Что для тебя главное завтра?"
    assert "{" not in parsed.question


def test_an_object_without_a_usable_question_falls_through_to_raw():
    """`subject` alone is not an answer: there is nothing to show the person."""
    parsed = parse_question('{"subject": "план"}')

    assert parsed.kind == RAW
    assert parsed.subject is None


def test_an_empty_answer_never_raises():
    parsed = parse_question("   ")

    assert parsed.kind == RAW
    assert parsed.question == ""


def test_a_subject_is_normalised_and_bounded():
    long_subject = "слово " * 60
    parsed = parse_question(
        json.dumps({"subject": long_subject, "question": "Что дальше?"})
    )

    assert parsed.subject is not None
    assert len(parsed.subject) <= question_format.MAX_SUBJECT_CHARS
    assert "  " not in parsed.subject


# --- what each shown question was about (ClickUp 86cbejvt2) ----------------


class FakeClock:
    """A clock the TTL tests move by hand."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def test_a_remembered_subject_comes_back_for_the_same_question():
    memory = question_format.SubjectMemory()
    memory.remember("Что для тебя главное завтра?", "завтрашние дела")

    assert memory.recall("Что для тебя главное завтра?") == "завтрашние дела"
    assert memory.subject_of("Что для тебя главное завтра?") == "завтрашние дела"


def test_the_key_is_the_novelty_filter_s_own_normalisation():
    """Punctuation, case and spacing must not lose a subject.

    The same `normalize` the repeat filter uses, so "the question the person
    was shown" means one thing in both places.
    """
    memory = question_format.SubjectMemory()
    memory.remember("Что для тебя главное завтра?", "завтрашние дела")

    assert memory.recall("  что   для тебя главное завтра  ") == "завтрашние дела"


def test_an_unknown_question_falls_back_to_an_excerpt_of_itself():
    memory = question_format.SubjectMemory()
    long_question = "Что именно " + "очень " * 40 + "важно?"

    assert memory.recall(long_question) is None
    excerpt = memory.subject_of(long_question)
    assert excerpt.startswith("Что именно очень")
    assert len(excerpt) <= question_format.MAX_SUBJECT_EXCERPT_CHARS + 1
    assert excerpt.endswith("…")


def test_a_short_question_is_its_own_excerpt():
    assert question_format.subject_excerpt("  Что дальше?  ") == "Что дальше?"


def test_nothing_is_remembered_without_a_subject():
    """A `raw` answer named none, and inventing one would be a guess."""
    memory = question_format.SubjectMemory()
    memory.remember("Что дальше?", None)
    memory.remember("Что дальше?", "   ")
    memory.remember("", "тема")

    assert len(memory) == 0


def test_an_entry_expires_and_the_question_degrades_to_its_excerpt():
    clock = FakeClock()
    memory = question_format.SubjectMemory(ttl_seconds=100, clock=clock)
    memory.remember("Что дальше?", "тема")

    clock.advance(99)
    assert memory.recall("Что дальше?") == "тема"
    clock.advance(2)
    assert memory.recall("Что дальше?") is None
    assert memory.subject_of("Что дальше?") == "Что дальше?"


def test_the_oldest_entry_is_dropped_first_at_the_ceiling():
    memory = question_format.SubjectMemory(max_entries=3)
    for index in range(5):
        memory.remember(f"Вопрос {index}?", f"тема {index}")

    assert len(memory) == 3
    assert memory.recall("Вопрос 0?") is None
    assert memory.recall("Вопрос 1?") is None
    assert memory.recall("Вопрос 4?") == "тема 4"


def test_writing_the_same_question_again_refreshes_it():
    memory = question_format.SubjectMemory(max_entries=2)
    memory.remember("Первый?", "один")
    memory.remember("Второй?", "два")
    memory.remember("Первый?", "один, уточнённый")
    memory.remember("Третий?", "три")

    # «Второй» is the oldest now, so it is the one that left.
    assert memory.recall("Первый?") == "один, уточнённый"
    assert memory.recall("Второй?") is None
    assert memory.recall("Третий?") == "три"


def test_the_defaults_are_the_reviewed_ones():
    memory = question_format.SubjectMemory()

    assert memory.ttl_seconds == question_format.SUBJECT_MEMORY_TTL_SECONDS == 7200
    assert memory.max_entries == question_format.SUBJECT_MEMORY_MAX_ENTRIES == 2000


def test_the_module_pulls_in_no_configuration():
    """Both production and `evaluation/gen_questions.py` import it.

    The tool runs outside the container with no `.env` at all, so a `config`
    import here would make the stand unable to parse what the endpoint parses.
    """
    with open(question_format.__file__, encoding="utf-8") as handle:
        text = handle.read()
    for forbidden in (
        "import config",
        "from config",
        "fastapi",
        "import query_rewrite",
        "from query_rewrite",
    ):
        assert forbidden not in text
