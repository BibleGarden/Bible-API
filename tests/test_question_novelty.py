"""Unit tests for the repeat filter of `POST /api/ai/question`.

ClickUp 86cbehyg0. Two things are pinned here: the verdicts on the examples
the constants were chosen from (`app/question_novelty.py` carries the table),
and the property that makes the table trustworthy — the metric is the one
`evaluation/check_questions.py` reports, character for character, so a number
in `evaluation/README.md` describes what the endpoint actually does.

The positive and negative sets are read from the artifact rather than quoted,
so a rebuilt benchmark is measured against the same constants instead of
against a copy of itself.
"""

from __future__ import annotations

import importlib.util
import itertools
import json
import sys
from pathlib import Path

import pytest

from question_novelty import (
    KIND_EXACT,
    KIND_NEAR,
    KIND_NONE,
    MIN_PREFIX_WORDS,
    NEAR_REPEAT_THRESHOLD,
    PREFIX_SHARE,
    is_repeat,
    normalize,
    similarity,
)

EVALUATION = Path(__file__).resolve().parents[1] / "evaluation"
SERIES = EVALUATION / "bench_data" / "questions_qwen30b_v3_series.jsonl"


def _series() -> dict[tuple[str, int], list[str]]:
    """The artifact grouped into series, each in step order."""
    grouped: dict[tuple[str, int], list[dict]] = {}
    for line in SERIES.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record.get("series_id") and record.get("text"):
            grouped.setdefault(
                (record["series_id"], record["sample"]), []
            ).append(record)
    return {
        key: [row["text"] for row in sorted(rows, key=lambda r: r["step"])]
        for key, rows in grouped.items()
    }


def _check_questions():
    """`evaluation/check_questions.py`, loaded the way test_gen_questions does.

    It is a script outside the package, so it is imported by path rather than
    by name; nothing in it is executed on import.
    """
    spec = importlib.util.spec_from_file_location(
        "check_questions_under_test", EVALUATION / "check_questions.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


# --- the metric is the benchmark's metric ----------------------------------


def test_the_metric_is_the_one_the_benchmark_reports():
    """`normalize`/`similarity` == `normalise_series_text`/`trigram_similarity`.

    Not a style preference: the filter that runs in production and the number
    `evaluation/check_questions.py` prints about a replacement series have to
    be the same measurement, or the benchmark stops describing the endpoint.
    """
    benchmark = _check_questions()
    texts = [text for texts in _series().values() for text in texts]
    assert len(texts) > 100
    for text in texts:
        assert normalize(text) == benchmark.normalise_series_text(text)
    for left, right in itertools.combinations(texts[:40], 2):
        assert similarity(left, right) == benchmark.trigram_similarity(
            left, right
        )


# --- exact repeats ---------------------------------------------------------


def test_the_same_question_twice_is_an_exact_repeat():
    question = "Что сейчас важнее всего для тебя в этом дне?"
    verdict = is_repeat(question, ["Что помогло тебе сегодня?", question])

    assert verdict.kind == KIND_EXACT
    assert verdict.repeat and verdict.score == 1.0
    assert verdict.index == 1
    assert verdict.matched == question


@pytest.mark.parametrize(
    "variant",
    [
        "что сейчас важнее всего для тебя в этом дне?",
        "  Что   сейчас важнее всего  для тебя в этом дне?  ",
        "Что сейчас важнее всего для тебя в этом дне",
        "«Что сейчас важнее всего для тебя — в этом дне?»",
        "ЧТО СЕЙЧАС ВАЖНЕЕ ВСЕГО ДЛЯ ТЕБЯ В ЭТОМ ДНЕ?!",
    ],
)
def test_case_spacing_and_punctuation_do_not_make_a_new_question(variant):
    shown = "Что сейчас важнее всего для тебя в этом дне?"
    assert is_repeat(variant, [shown]).kind == KIND_EXACT


def test_the_two_spellings_of_yo_are_one_question():
    """`ё`/`е` is a keyboard habit, not a different question. Ukrainian `є`,
    `і`, `ї` are different letters and are left alone by `normalize`."""
    assert is_repeat(
        "Что ещё сегодня осталось несделанным?",
        ["Что еще сегодня осталось несделанным?"],
    ).kind == KIND_EXACT
    assert normalize("Чиї слова сьогодні тебе підтримали?") == (
        "чиї слова сьогодні тебе підтримали"
    )


# --- near repeats: the observed loop ---------------------------------------

STEM = (
    "А если завтра окажется, что то, что ты считаешь готовым, на самом деле "
    "ещё не то, что нужно Господу"
)


def test_the_same_question_with_a_new_tail_is_a_near_repeat():
    """The pair the bug was reported on (86cbehtkh): one sentence, one tail."""
    shown = f"{STEM}?"
    candidate = f"{STEM} — как ты будешь узнавать это?"

    verdict = is_repeat(candidate, [shown])

    assert verdict.kind == KIND_NEAR
    assert verdict.score >= NEAR_REPEAT_THRESHOLD
    assert verdict.index == 0


def test_a_repeated_stem_survives_a_tail_long_enough_to_hide_it():
    """What the prefix branch is FOR: the score alone no longer sees it.

    A fresh fifteen-word tail drags the trigram similarity below the
    threshold while the person is still reading the same sentence.
    """
    candidate = (
        f"{STEM} — а что ты хочешь услышать от близкого человека завтра "
        "вечером, когда всё это уже будет позади и можно будет просто "
        "помолчать вместе?"
    )
    shown = f"{STEM}?"

    assert similarity(candidate, shown) < NEAR_REPEAT_THRESHOLD
    assert is_repeat(candidate, [shown]).kind == KIND_NEAR


def test_a_shared_frame_is_not_a_shared_question():
    """The reason the prefix rule is a share and not "the first four words".

    Qwen opens every Ukrainian question with «А що б ти зробив, якби …» — six
    words, more than `MIN_PREFIX_WORDS` — and these two ask plainly different
    things. Same for the English frame.
    """
    assert MIN_PREFIX_WORDS == 4 and PREFIX_SHARE == 0.7
    pairs = [
        (
            "А що б ти зробив, якби міг відчути, що вже досить?",
            "А що б ти зробив, якби цього вихідного просто вирішив не працювати?",
        ),
        (
            "What would it feel like to stay, knowing you might regret not "
            "taking the chance?",
            "What would it feel like to say yes, knowing she'd still be here, "
            "just not with you?",
        ),
    ]
    for candidate, shown in pairs:
        assert is_repeat(candidate, [shown]).kind == KIND_NONE


# --- negatives: different questions about the same prayer ------------------

DIFFERENT_QUESTIONS = [
    (
        "Что из сделанного сегодня тебе хочется принести Богу первым?",
        "Что завтра будет самым трудным для тебя?",
    ),
    (
        "Что сейчас важнее всего для тебя в этом дне?",
        "Что сейчас тебе труднее всего отпустить?",
    ),
    (
        "Що з того, що ти зробив сьогодні, хочеться подякувати Богу?",
        "Що завтра буде для тебе найважчим?",
    ),
    (
        "What from today would you most want to bring to God first?",
        "What will be hardest for you tomorrow?",
    ),
    (
        "What feels most alive in you right now?",
        "What would you like to leave behind today?",
    ),
]


@pytest.mark.parametrize(("candidate", "shown"), DIFFERENT_QUESTIONS)
def test_a_different_question_on_the_same_topic_is_not_a_repeat(
    candidate, shown
):
    verdict = is_repeat(candidate, [shown])

    assert verdict.kind == KIND_NONE
    assert not verdict.repeat
    assert similarity(candidate, shown) <= 0.35


def test_the_hand_written_negatives_stay_far_from_the_threshold():
    """The margin, not just the verdict: max 0.345 against a 0.60 line."""
    scores = [similarity(a, b) for a, b in DIFFERENT_QUESTIONS]
    assert max(scores) == pytest.approx(0.345, abs=0.005)


# --- the measured sets of the module docstring -----------------------------


def test_every_pair_of_the_observed_loop_is_flagged():
    """`series-scale-ru` sample 1: six replacements of one question.

    All 15 pairs, not only the consecutive ones — the person may press
    "replace" past a variant and come back to it.
    """
    texts = _series()[("series-scale-ru", 1)]
    assert len(texts) == 6
    scores = [similarity(a, b) for a, b in itertools.combinations(texts, 2)]
    assert min(scores) == pytest.approx(0.610, abs=0.001)
    assert max(scores) == pytest.approx(0.910, abs=0.001)
    for candidate, shown in itertools.permutations(texts, 2):
        assert is_repeat(candidate, [shown]).repeat

    # And in the shape the endpoint uses it: each answer against everything
    # already shown.
    for step in range(1, len(texts)):
        assert is_repeat(texts[step], texts[:step]).repeat


def test_the_frame_never_decides_on_the_other_three_series():
    """The negative material, pair by pair — and what the prefix branch costs.

    18 of the 180 pairs are flagged: six byte-identical ones, nine over the
    threshold, and **three** that only the opening rule sees — all three the
    same «А що б ти зробив, якби міг відпочити хоч на годинку …» with the tail
    swapped, which is a repeat by eye. The remaining 162, the material the
    ticket offered as negatives, are untouched, and the highest of them scores
    0.500 against a 0.60 line.

    The bound on the prefix branch is the point of the number three: with the
    "first four words are equal" rule the ticket proposed it would be most of
    `series-exhaustion-uk`, because every question there opens «А що б ти
    зробив, якби» (six words).
    """
    flagged = 0
    prefix_only = 0
    pairs = 0
    highest_unflagged = 0.0
    for key, texts in _series().items():
        if key[0] == "series-scale-ru":
            continue
        for left, right in itertools.combinations(texts, 2):
            pairs += 1
            score = similarity(left, right)
            verdict = is_repeat(left, [right])
            by_score = (
                score >= NEAR_REPEAT_THRESHOLD
                or normalize(left) == normalize(right)
            )
            if by_score:
                assert verdict.repeat
            if verdict.repeat:
                flagged += 1
                prefix_only += not by_score
            else:
                highest_unflagged = max(highest_unflagged, score)

    assert (pairs, flagged, prefix_only) == (180, 18, 3)
    assert highest_unflagged == pytest.approx(0.500, abs=0.001)


# --- the verdict object ----------------------------------------------------


def test_nothing_to_compare_is_not_a_repeat():
    assert not is_repeat("Что сейчас важнее всего?", []).repeat
    assert not is_repeat("", ["Что сейчас важнее всего?"]).repeat
    assert not is_repeat("   ", ["Что сейчас важнее всего?"]).repeat
    assert not is_repeat("Что сейчас важнее всего?", ["", "   "]).repeat


def test_the_verdict_names_the_closest_question_it_matched():
    loop = f"{STEM}?"
    shown = [
        "Что помогло тебе сегодня?",
        f"{STEM} — как ты будешь узнавать разницу?",
        "Что завтра будет самым трудным?",
    ]

    verdict = is_repeat(loop, shown)

    assert verdict.index == 1
    assert verdict.matched == shown[1]
    assert verdict.score == pytest.approx(similarity(loop, shown[1]))


def test_a_question_that_is_not_a_repeat_still_reports_its_closest_match():
    """The handler compares two candidates by how close each came, so the
    score is reported whether or not it crossed the line."""
    verdict = is_repeat(
        "Что завтра будет самым трудным для тебя?",
        ["Что из сделанного сегодня тебе хочется принести Богу первым?"],
    )

    assert verdict.kind == KIND_NONE
    assert verdict.index == 0
    assert 0.0 < verdict.score < NEAR_REPEAT_THRESHOLD
