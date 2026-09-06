"""Unit tests and corpus tests for the post-filters of ClickUp 86cbejvra.

Two layers, for the same reason `tests/test_question_novelty.py` has two:

1. Hand-written cases that pin the *rules* — what a gender word is, what the
   window around it decides, what a menu and a tail are.
2. A run over **all 396 answers** of the four measured artifacts
   (`question_comparison_2026-09-06` = prompt v4 and
   `question_comparison_prompt_v5_final` = prompt v5, Qwen and Gemini), whose
   totals are compared with the manual count in the same directory's
   `FABLE_ASSESSMENT.md` («Род и язык») and then frozen. The frozen numbers are
   what makes a later change to the lists visible instead of silent.

The artifacts are read, never quoted, so a rebuilt benchmark is measured
against these rules rather than against a copy of itself.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from question_filters import (
    AMBIGUOUS_LOOKBACK,
    FEMININE,
    FEMININE_ADDRESS,
    FEMININE_SELF,
    MASCULINE,
    MASCULINE_ADDRESS,
    MASCULINE_SELF,
    addressed_genders,
    detect_gender,
    gender_mismatch,
    has_tail,
    is_menu,
    words,
)

BENCH = Path(__file__).resolve().parents[1] / "evaluation" / "bench_data"
ARTIFACTS = {
    ("qwen", 4): BENCH / "question_comparison_2026-09-06" / "qwen.jsonl",
    ("gemini", 4): BENCH / "question_comparison_2026-09-06" / "gemini.jsonl",
    ("qwen", 5): BENCH / "question_comparison_prompt_v5_final" / "qwen.jsonl",
    ("gemini", 5): BENCH / "question_comparison_prompt_v5_final" / "gemini.jsonl",
}


def _rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _person_words(row: dict) -> list[str]:
    """The person's own words of this request — topic plus their `user` turns.

    `person_words` is written by `gen_questions.py` for the series inputs; the
    single ones carry the same information in `input`, and both are the same
    list, so the artifact alone is enough (the point of that field).
    """
    if row.get("person_words"):
        return list(row["person_words"])
    request = row["input"]
    return [request["topic"]] + [
        message["text"] for message in request["messages"] if message["role"] == "user"
    ]


# --- 1. the rules ----------------------------------------------------------


def test_the_lists_are_pairs_and_never_claim_both_genders():
    """No form may be feminine in one list and masculine in the other."""
    assert not FEMININE_SELF & MASCULINE_SELF
    assert not FEMININE_ADDRESS & MASCULINE_ADDRESS


@pytest.mark.parametrize(
    "texts, expected",
    [
        (["Я рада тому, что сегодня немало сделано."], FEMININE),
        (["Отправил заявку на работу и жду ответа."], MASCULINE),
        (["Учора заснула просто в одязі, навіть не вимкнула світло."], FEMININE),
        (["Я втомився, третій місяць без вихідних."], MASCULINE),
        (["Пока молилась, поняла, что хочу позвонить сестре."], FEMININE),
        # English carries no such forms at all.
        (["I keep putting the answer off.", "My mother is here alone."], None),
        # Nothing gendered said.
        (["День прошёл обычно: работа, магазин, ужин."], None),
        # Both genders in the same request: one of them is about someone else
        # and there is no way to tell which.
        (["Я рада", "Я сделал это"], None),
    ],
)
def test_detect_gender_reads_the_person_own_words(texts, expected):
    assert detect_gender(texts) == expected


def test_gender_of_someone_else_is_not_the_person_gender():
    """The two inputs a morphological `-ла` rule gets wrong.

    «колега звільнився» and «сосед помог» are masculine forms about a third
    person, in requests whose author is a woman / unknown. Neither verb is in
    the reviewed list, so neither contributes.
    """
    assert detect_gender([
        "Третій місяць працюю без вихідних, бо колега звільнився. "
        "Учора заснула просто в одязі, навіть не вимкнула світло."
    ]) == FEMININE
    assert detect_gender([
        "Утром сосед помог донести тяжёлые сумки. "
        "Для него мелочь, а мне было очень приятно."
    ]) is None


def test_mismatch_is_the_other_gender_and_none_means_any():
    feminine_question = "Что ты почувствовала, когда закончила отчёт?"
    masculine_question = "Что ты почувствовал, когда закончил отчёт?"
    assert gender_mismatch(masculine_question, FEMININE)
    assert not gender_mismatch(feminine_question, FEMININE)
    assert gender_mismatch(feminine_question, MASCULINE)
    # No evidence: any explicit gendered address is a gender that was imposed.
    assert gender_mismatch(masculine_question, None)
    assert gender_mismatch(feminine_question, None)
    # …and a question that needs no gendered form is fine in every case.
    genderless = "Что для тебя сейчас самое важное в этом разговоре?"
    assert not gender_mismatch(genderless, None)
    assert not gender_mismatch(genderless, FEMININE)
    assert not gender_mismatch(genderless, MASCULINE)


def test_english_question_never_has_a_gender():
    assert addressed_genders("What would it mean to say yes to this?") == set()
    assert not gender_mismatch("What weighs most on you in this choice?", FEMININE)


def test_a_form_about_a_third_person_is_not_an_address():
    """«что он хотел» is about the brother, not about the person."""
    assert addressed_genders("Что именно он хотел сказать тебе тогда?") == set()
    # The same verb, addressed: the nearest marker is second person.
    assert addressed_genders("Что ты хотел сказать ему тогда?") == {MASCULINE}


def test_a_second_person_verb_is_a_marker_too():
    """Many questions carry no pronoun at all — «Що відчуваєш … що хотів»."""
    assert addressed_genders(
        "Що відчуваєш, коли засинаєш, бо вже не встигаєш зробити те, "
        "що хотів зробити до сну?"
    ) == {MASCULINE}


def test_ambiguous_words_need_the_pronoun_beside_them():
    """«сам акт», «один день» are not addresses; «ты сам», «ти сама» are."""
    assert addressed_genders(
        "что в этом процессе особенно важно для тебя — не просто результат, "
        "а сам акт делать?"
    ) == set()
    assert addressed_genders("Что ты сам хочешь сказать об этом?") == {MASCULINE}
    assert addressed_genders("Что помогает тебе помнить, что ты не одна?") == {FEMININE}
    assert AMBIGUOUS_LOOKBACK < 3


@pytest.mark.parametrize(
    "question, menu",
    [
        ("Что важнее — то, что вы смеялись, или то, что вы снова рядом?", True),
        ("Що саме у цьому виборі найважливіше — рости в роботі чи час для рідних?", True),
        ("What matters more, the money or the years with her?", True),
        # A conjunction that opens the question is not a menu.
        ("Или ты уже знаешь ответ?", False),
        # One word on a side is a phrase, not an alternative.
        ("Что ты чувствуешь сейчас?", False),
        ("Что осталось после этого дня или вечера?", False),
    ],
)
def test_is_menu(question, menu):
    assert is_menu(question) is menu


@pytest.mark.parametrize(
    "question, tail",
    [
        ("Что для тебя главное сейчас — как ты будешь узнавать это?", True),
        ("Що б ти зробив, якби міг відпочити – навіть один день без провини?", True),
        ("Что важно в этом деле - и что с этим делать дальше?", True),
        ("Что делает это для тебя не просто делом, а служением?", True),
        ("Що робить це для тебе не лише роботою, а покликанням?", True),
        ("What makes it not just a job but a calling for you?", True),
        # A dash with less than three words after it is an aside, not a tail.
        ("Что для тебя главное сейчас — почему?", False),
        ("Что осталось с тобой после этого разговора?", False),
        # A hyphen inside a word is not a dash.
        ("Что-то из сегодняшнего дня осталось с тобой?", False),
    ],
)
def test_has_tail(question, tail):
    assert has_tail(question) is tail


def test_words_folds_the_two_spellings_of_yo_and_keeps_ukrainian_letters():
    assert words("Что «ещё» — её?") == ["что", "еще", "ее"]
    assert words("Її ґанок, і є час") == ["її", "ґанок", "і", "є", "час"]


# --- 2. the corpus ---------------------------------------------------------


def _corpus_counts(path: Path) -> dict[str, int]:
    """Every verdict of this module over one artifact."""
    counts = {"rows": 0, "mismatch": 0, "imposed": 0, "menu": 0, "tail": 0}
    for row in _rows(path):
        counts["rows"] += 1
        text = row["text"]
        if row["language"] in ("ru", "uk"):
            gender = detect_gender(_person_words(row))
            if gender_mismatch(text, gender):
                counts["mismatch" if gender is not None else "imposed"] += 1
        counts["menu"] += is_menu(text)
        counts["tail"] += has_tail(text)
    return counts


# Frozen verdicts of this module on the four measured artifacts. `mismatch` and
# `imposed` are compared with the manual count of FABLE_ASSESSMENT.md in the
# test below; `menu` and `tail` are this module's own numbers — the assessment
# lists menus by scenario rather than by row, and counts no tails at all.
CORPUS = {
    ("qwen", 4): {"rows": 99, "mismatch": 1, "imposed": 3, "menu": 1, "tail": 17},
    ("gemini", 4): {"rows": 99, "mismatch": 1, "imposed": 2, "menu": 5, "tail": 7},
    ("qwen", 5): {"rows": 99, "mismatch": 15, "imposed": 4, "menu": 12, "tail": 26},
    ("gemini", 5): {"rows": 99, "mismatch": 0, "imposed": 0, "menu": 3, "tail": 0},
}
# `FABLE_ASSESSMENT.md`, section «Род и язык»: (wrong gender to a woman,
# gender imposed where the request gave none).
MANUAL_GENDER = {
    ("qwen", 4): (1, 3),
    ("gemini", 4): (0, 2),
    ("qwen", 5): (15, 4),
    ("gemini", 5): (0, 0),
}


@pytest.mark.parametrize("key", sorted(ARTIFACTS, key=str))
def test_corpus_verdicts_are_frozen(key):
    assert _corpus_counts(ARTIFACTS[key]) == CORPUS[key]


def test_the_gender_detector_agrees_with_the_manual_count():
    """Where it does not, the disagreement is named rather than tuned away.

    Three of the four combinations reproduce the hand count exactly, on the
    same rows. The fourth is one extra flag on Gemini/v4, documented in the
    module docstring and in `evaluation/README.md`: «Что ты чувствовала, когда
    отчёт был наконец готов?», where «готов» agrees with «отчёт» and the
    window rule attaches it to «ты».
    """
    for key, (manual_mismatch, manual_imposed) in MANUAL_GENDER.items():
        measured = CORPUS[key]
        assert measured["imposed"] == manual_imposed, key
        if key == ("gemini", 4):
            assert measured["mismatch"] == manual_mismatch + 1
        else:
            assert measured["mismatch"] == manual_mismatch, key


def test_the_known_false_positive_is_exactly_one_row():
    """The Gemini/v4 disagreement above is that row and no other."""
    extra = [
        row
        for row in _rows(ARTIFACTS[("gemini", 4)])
        if row["language"] in ("ru", "uk")
        and detect_gender(_person_words(row)) is not None
        and gender_mismatch(row["text"], detect_gender(_person_words(row)))
    ]
    assert len(extra) == 1
    only = extra[0]
    assert (only["id"], only["sample"], only["step"]) == ("series-gratitude-ru", 2, 4)
    assert "был наконец готов" in only["text"]


def test_qwen_v5_gender_regression_is_where_the_assessment_said_it_is():
    """The 15 come from the three scenarios the assessment names."""
    per_input: dict[str, int] = {}
    for row in _rows(ARTIFACTS[("qwen", 5)]):
        if row["language"] not in ("ru", "uk"):
            continue
        gender = detect_gender(_person_words(row))
        if gender is not None and gender_mismatch(row["text"], gender):
            per_input[row["id"]] = per_input.get(row["id"], 0) + 1
    assert per_input == {
        "series-exhaustion-uk": 10,
        "series-scale-ru": 3,
        "conflict-ru": 2,
    }


def test_every_benchmark_input_gets_the_gender_its_words_carry():
    """The detector's reading of the 16 inputs, all four artifacts agreeing."""
    seen: dict[str, set[str | None]] = {}
    for path in ARTIFACTS.values():
        for row in _rows(path):
            seen.setdefault(row["id"], set()).add(detect_gender(_person_words(row)))
    assert {name: values.pop() for name, values in sorted(seen.items())} == {
        "choice-uk": None,
        "conflict-ru": FEMININE,
        "gratitude-ru": None,
        "joy-ru": FEMININE,
        "joy-uk": FEMININE,
        "ordinary-en": None,
        "ordinary-ru": None,
        "plans-ru": None,
        "reflect-en": None,
        "reflect-ru": FEMININE,
        "series-choice-en": None,
        "series-exhaustion-uk": FEMININE,
        "series-gratitude-ru": FEMININE,
        "series-scale-ru": FEMININE,
        "uncertainty-en": None,
        "waiting-ru": MASCULINE,
    }


def test_the_menus_of_qwen_v5_cover_the_ones_counted_by_hand():
    """joy-ru ×3, waiting-ru ×2, choice-uk ×2, gratitude ×2, reflect ×1, scale ×1.

    Plus one the assessment did not list — `plans-ru` «Что из двух дел — для
    работы или сестры» — which is a menu by the same reading. The detector is
    a superset here, not a different set.
    """
    per_input: dict[str, int] = {}
    for row in _rows(ARTIFACTS[("qwen", 5)]):
        if is_menu(row["text"]):
            per_input[row["id"]] = per_input.get(row["id"], 0) + 1
    manual = {
        "joy-ru": 3,
        "waiting-ru": 2,
        "choice-uk": 2,
        "series-gratitude-ru": 2,
        "reflect-ru": 1,
        "series-scale-ru": 1,
    }
    for name, count in manual.items():
        assert per_input.get(name) == count, name
    assert set(per_input) - set(manual) == {"plans-ru"}
