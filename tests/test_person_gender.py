"""The grammatical gender the message states (ClickUp 86cbejvt2).

The rule is deliberately narrow: a reviewed list of first-person forms, and
`None` for everything else — including a contradiction. What is pinned here is
that narrowness, because the failure mode it guards against is the one the
assessment of v5 measured: a *wrong* gender puts an error into every sentence,
while `None` tells the model to word the question without gendered forms at
all.
"""

import pytest

import person_gender
from person_gender import FEMININE, MASCULINE, detect_gender


@pytest.mark.parametrize(
    ("texts", "expected"),
    [
        # The journal case of 86cbehtkh, verbatim from Maria's prayer.
        (["Я рада тому, что сегодня немало сделано."], FEMININE),
        (["Я рад, что успел."], MASCULINE),
        # The Ukrainian series, where v5 addressed a woman as «зробив» in
        # 30 answers of 30.
        (["Я так втомилася, заснула в одязі."], FEMININE),
        (["Я втомився за цей тиждень."], MASCULINE),
        (["Я зробила все, що могла."], FEMININE),
        (["Я зробив усе, що міг."], MASCULINE),
        (["Я молилась об этом."], FEMININE),
        (["Я молился об этом."], MASCULINE),
        # The topic alone, which is all a `first` request carries.
        (["Успела закончить отчёт"], FEMININE),
        (["Отправил письмо и жду"], MASCULINE),
    ],
)
def test_a_listed_form_decides(texts, expected):
    assert detect_gender(texts) == expected


@pytest.mark.parametrize(
    "texts",
    [
        [],
        [""],
        ["   "],
        # Nothing from the list: a person can write a whole prayer without one.
        ["Мне одиноко. Хочу восстановить общение."],
        # English has no gendered second person, so there is nothing to state.
        ["I am glad I finished the report today."],
        ["I was tired and fell asleep in my clothes."],
    ],
)
def test_nothing_to_decide_is_none(texts):
    assert detect_gender(texts) is None


def test_a_contradiction_is_none_not_a_majority_vote():
    """Two forms of different genders are two people or a quotation.

    Counting them would let one stray word decide the address for the whole
    prayer; `None` is the honest answer and its own instruction.
    """
    assert detect_gender(["Я рада.", "Он сказал: я сделал."]) is None
    assert detect_gender(["Я сделала это.", "Я сделал это."]) is None
    # Whichever text carries which form, and in either order.
    assert detect_gender(["Я сделал это.", "Я сделала это."]) is None


def test_a_form_is_matched_as_a_whole_word():
    """`рад` is a prefix of `рада`: a substring search would sex every woman.

    The other direction matters as much — a longer word that merely contains a
    listed form is not that form.
    """
    assert detect_gender(["Я рада."]) == FEMININE
    assert detect_gender(["Я рад."]) == MASCULINE
    assert detect_gender(["Городская рада приняла решение."]) == FEMININE
    assert detect_gender(["Радар не работает."]) is None
    assert detect_gender(["Понялa"]) is None  # a Latin `a`: not the form


def test_case_and_punctuation_do_not_hide_a_form():
    assert detect_gender(["РАДА!"]) == FEMININE
    assert detect_gender(["...ответила, наконец."]) == FEMININE
    assert detect_gender(["«Успел», — думаю я."]) == MASCULINE


def test_the_lists_are_reviewed_pairs():
    """The list may only grow through review, and only in pairs.

    A list that leans one way would make the endpoint state that gender more
    often, which is exactly the bias the code replaces. `рада` is the one
    feminine form with two counterparts (Russian «рад», Ukrainian «радий»),
    which is why the sizes differ by exactly that one.
    """
    assert person_gender.FEMININE_FORMS == tuple(person_gender.FORM_PAIRS)
    for feminine, masculine in person_gender.FORM_PAIRS.items():
        assert masculine, f"{feminine} has no masculine counterpart"
    assert len(set(person_gender.FEMININE_FORMS)) == len(person_gender.FEMININE_FORMS)
    assert len(set(person_gender.MASCULINE_FORMS)) == len(person_gender.MASCULINE_FORMS)
    assert not set(person_gender.FEMININE_FORMS) & set(person_gender.MASCULINE_FORMS)
    for forms in (person_gender.FEMININE_FORMS, person_gender.MASCULINE_FORMS):
        for form in forms:
            assert form == form.casefold() != ""
            assert " " not in form
    assert "радий" in person_gender.MASCULINE_FORMS


@pytest.mark.parametrize(
    ("text", "expected", "why"),
    [
        # Known FALSE POSITIVES — documented in the module docstring rather
        # than fixed, because the fix (requiring a first-person pronoun nearby)
        # was measured against the 16 reference scenarios and lost five of the
        # eight decided cases. See the docstring's table.
        ("Мама успела закончить до вечера.", FEMININE, "past tense, third person"),
        ("Брат отправил письмо вчера.", MASCULINE, "the same, masculine"),
        ("Она рада за нас.", FEMININE, "a short adjective about someone else"),
        # And the cases the narrowness costs us: a form nobody reviewed.
        ("Я очень устала за неделю.", None, "`устала` is not on the list"),
        ("Я заснул прямо в одежде.", None, "Russian `заснул` is not on the list"),
    ],
)
def test_the_known_limits_are_the_documented_ones(text, expected, why):
    assert detect_gender([text]) == expected, why


def test_the_reference_scenarios_are_unchanged():
    """The 16 inputs of `question_quality_inputs.json`, which v6-C will use.

    Pinned here so that any future widening of the list has to look at what it
    does to the very set the measurement runs on.
    """
    import json
    from pathlib import Path

    inputs = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "evaluation/question_quality_inputs.json"
        ).read_text(encoding="utf-8")
    )["inputs"]
    decided = {
        entry["id"]: detect_gender(
            [
                message["text"]
                for message in entry["messages"]
                if message["role"] == "user"
            ]
            + [entry["topic"]]
        )
        for entry in inputs
    }

    assert decided == {
        "series-scale-ru": FEMININE,
        "series-gratitude-ru": FEMININE,
        "series-choice-en": None,
        "series-exhaustion-uk": FEMININE,
        "ordinary-ru": None,
        "joy-ru": FEMININE,
        "gratitude-ru": None,
        "plans-ru": None,
        "conflict-ru": FEMININE,
        "waiting-ru": MASCULINE,
        "choice-uk": None,
        "joy-uk": FEMININE,
        "ordinary-en": None,
        "uncertainty-en": None,
        "reflect-ru": FEMININE,
        "reflect-en": None,
    }


def test_the_module_imports_nothing_from_the_application():
    """The same rule `question_prompt` follows, and for the same reason."""
    source = (
        person_gender.__file__
    )
    with open(source, encoding="utf-8") as handle:
        text = handle.read()
    for forbidden in ("import config", "from config", "fastapi", "httpx"):
        assert forbidden not in text
