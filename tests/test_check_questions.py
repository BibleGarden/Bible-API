"""Tests for the replacement-series metrics of evaluation/check_questions.py.

ClickUp 86cbehyez (bug 86cbehtkh). The metrics are helper numbers with no
thresholds, which is exactly why they need pinning: nothing else notices when
a normalisation change quietly moves «макс. похожесть» in the README table, and
the numbers in that table are the baseline the fix will be compared against.

Three things are covered here and nowhere else:

* the normalisation and the similarity are what the README says they are
  (casefold, ё→е, no punctuation, collapsed whitespace; a symmetric, bounded
  Jaccard over character trigrams);
* «точных повторов» counts PAIRS of steps, not texts — the README says pairs,
  and three identical answers are three pairs, not three;
* the gender heuristic fires where the baseline says it fires, does not fire
  on a masculine form the PERSON wrote, and has the one false positive its
  docstring admits to (a third-person past tense beside a «тебе»).

No network, no provider, no artifact required except the committed baseline,
which is skipped when this container copy has no `evaluation/bench_data`.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

EVALUATION = Path(__file__).resolve().parents[1] / "evaluation"
TOOL = EVALUATION / "check_questions.py"
BASELINE = EVALUATION / "bench_data" / "questions_qwen30b_v3_series.jsonl"

pytestmark = pytest.mark.skipif(
    not TOOL.exists(), reason="evaluation/ is not present in this container copy"
)


def _load_tool():
    spec = importlib.util.spec_from_file_location("check_questions", TOOL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


tool = _load_tool()


def row(text: str, *, series: str = "s", sample: int = 1, step: int = 1, **extra):
    record = {
        "id": series,
        "series_id": series,
        "sample": sample,
        "step": step,
        "language": "ru",
        "category": "series",
        "stage": "next",
        "text": text,
    }
    record.update(extra)
    return record


# ---------------------------------------------------------------------------
# Normalisation and similarity
# ---------------------------------------------------------------------------

def test_normalisation_is_what_the_readme_promises():
    assert tool.normalise_series_text("  А ЕЩЁ  «что-то» — вот?  ") == (
        "а еще что то вот"
    )
    # ё→е, so the two spellings of one word are one word.
    assert tool.normalise_series_text("Всё") == tool.normalise_series_text("Все")


def test_similarity_is_symmetric_and_bounded():
    pairs = [
        ("Что сейчас внутри тебя?", "Что сейчас внутри тебя?"),
        ("Что сейчас внутри тебя?", "Что ты чувствуешь сегодня?"),
        ("What weighs on you?", "Що зараз забирає сили?"),
        ("", "что-то"),
    ]
    for left, right in pairs:
        value = tool.trigram_similarity(left, right)
        assert 0.0 <= value <= 1.0
        assert value == tool.trigram_similarity(right, left)

    assert tool.trigram_similarity("Что сейчас внутри тебя?", "Что сейчас внутри тебя!") == 1.0
    assert tool.trigram_similarity("аааа", "щщщщ") == 0.0


def test_the_opening_is_the_first_three_words_normalised():
    assert tool.opening("А если завтра окажется, что…") == "а если завтра"
    assert tool.opening("А ЕСЛИ, завтра, окажется") == "а если завтра"


# ---------------------------------------------------------------------------
# The series numbers
# ---------------------------------------------------------------------------

def test_duplicates_count_pairs_of_steps_not_texts():
    """Three identical answers are three pairs — what the README table says."""
    rows = [row("Один и тот же вопрос?", step=step) for step in (1, 2, 3)]

    metrics = tool.series_metrics(rows)

    assert metrics["duplicate_pairs"] == 3
    assert metrics["steps"] == 3
    assert metrics["unique_openings"] == 1
    assert metrics["opening_share"] == pytest.approx(1 / 3)
    assert metrics["max_similarity"] == 1.0


def test_a_varied_series_scores_low_and_a_failed_step_is_not_an_answer():
    rows = [
        row("Что сейчас внутри тебя?", step=1),
        row("Куда тебе хочется пойти завтра утром?", step=2),
        row("", step=3, error="transport: ConnectError"),
    ]

    metrics = tool.series_metrics(rows)

    assert metrics["steps"] == 2  # the failed call is not counted as an answer
    assert metrics["duplicate_pairs"] == 0
    assert metrics["max_similarity"] < 0.5
    assert metrics["opening_share"] == 1.0


def test_series_are_grouped_by_sample_and_ordered_by_step():
    records = [
        row("третий?", sample=1, step=3),
        row("первый?", sample=1, step=1),
        row("второй?", sample=1, step=2),
        row("другой сэмпл?", sample=2, step=1),
        {"id": "single", "sample": 1, "language": "ru", "category": "x",
         "stage": "next", "text": "не серия?"},
    ]

    grouped = tool.series_runs(records)

    assert set(grouped) == {("s", 1), ("s", 2)}
    assert [record["step"] for record in grouped[("s", 1)]] == [1, 2, 3]
    assert [record["text"] for record in grouped[("s", 1)]] == [
        "первый?", "второй?", "третий?"
    ]


def test_the_report_sums_duplicates_and_averages_the_shares():
    records = [
        row("Один и тот же вопрос?", sample=1, step=1),
        row("Один и тот же вопрос?", sample=1, step=2),
        row("Первый?", sample=2, step=1),
        row("Совсем другой вопрос про завтра?", sample=2, step=2),
    ]

    report = tool.series_report(records)["s"]

    assert report["samples"] == 2
    assert report["answers"] == 4
    assert report["duplicate_pairs"] == 1  # one pair, in sample 1 only
    assert report["opening_share"] == pytest.approx((0.5 + 1.0) / 2)
    assert report["max_similarity_worst"] == 1.0


def test_samples_of_a_single_input_are_read_as_a_series_too():
    """`--samples-as-series`: N samples of one body is what a replacement is."""
    records = [
        {"id": "one", "sample": 1, "text": "Что сейчас внутри тебя?"},
        {"id": "one", "sample": 2, "text": "Что сейчас внутри тебя?"},
        {"id": "lonely", "sample": 1, "text": "Единственный сэмпл?"},
        row("серия, её сюда не берём?", series="s", sample=1, step=1),
    ]

    report = tool.sample_series(records)

    assert set(report) == {"one"}  # a one-sample input says nothing; a series
    assert report["one"]["duplicate_pairs"] == 1  # is measured by series_report
    assert report["one"]["steps"] == 2


# ---------------------------------------------------------------------------
# The gender heuristic
# ---------------------------------------------------------------------------

def test_gender_fires_on_the_baseline_shape_in_both_languages():
    assert tool.gender_mismatch(
        "А що б ти зробив, якби сьогодні міг відпочити?",
        ["Учора заснула просто в одязі, навіть не вимкнула світло."],
    )
    assert tool.gender_mismatch(
        "А что бы ты сделал, если бы завтра всё получилось?",
        ["Я рада тому, что сегодня немало сделано."],
    )


def test_gender_stays_quiet_where_it_should():
    # Feminine person, feminine address.
    assert not tool.gender_mismatch(
        "А что бы ты сделала, если бы завтра всё получилось?",
        ["Я рада тому, что сегодня немало сделано."],
    )
    # No feminine self-report at all: nothing says the person is a woman.
    assert not tool.gender_mismatch(
        "А что бы ты сделал, если бы завтра всё получилось?",
        ["Мы с мужем просто гуляли и ни о чём не говорили."],
    )
    # A masculine form the PERSON wrote (quoting someone) is never read as an
    # address to her — only the question is scanned for masculine forms.
    assert not tool.gender_mismatch(
        "Что в этом дне осталось внутри?",
        ["Я рада, хотя муж сказал, что успел не всё."],
    )
    # A masculine past tense with nobody addressed.
    assert not tool.gender_mismatch(
        "Что изменилось, когда сын сказал это?",
        ["Я рада тому, что сегодня немало сделано."],
    )


def test_the_documented_false_positive_is_still_the_documented_one():
    """A third person beside a «тебе» fires — the docstring says so.

    Pinned rather than fixed: telling the two apart needs syntax, and the
    number is reported as a flag to look at, never as a counted violation.
    """
    assert tool.gender_mismatch(
        "А что тебе сказал сын в тот вечер?",
        ["Я рада тому, что сегодня немало сделано."],
    )


# ---------------------------------------------------------------------------
# The committed baseline
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not BASELINE.exists(), reason="baseline artifact not copied in")
def test_the_readme_numbers_are_the_numbers_in_the_artifact():
    records = [
        json.loads(line)
        for line in BASELINE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    report = tool.series_report(records)
    expected = {
        # series_id: (samples, steps, openings, mean sim, duplicates, gender)
        "series-scale-ru": (6, 6, 0.17, 0.98, 11, 0),
        "series-exhaustion-uk": (6, 5, 0.20, 0.84, 4, 30),
        "series-gratitude-ru": (6, 5, 0.43, 0.57, 1, 0),
        "series-choice-en": (6, 5, 0.43, 0.56, 1, 0),
    }
    assert set(report) == set(expected)
    for series_id, (samples, steps, share, similarity, dups, gender) in expected.items():
        item = report[series_id]
        assert (item["samples"], item["steps"]) == (samples, steps), series_id
        assert round(item["opening_share"], 2) == share, series_id
        assert round(item["max_similarity_mean"], 2) == similarity, series_id
        assert item["duplicate_pairs"] == dups, series_id
        assert item["gender_flags"] == gender, series_id

    singles = tool.sample_series(records)
    assert {
        input_id: (round(item["opening_share"], 2), round(item["max_similarity"], 2),
                   item["gender_flags"])
        for input_id, item in singles.items()
    } == {
        "gratitude-ru-first": (0.17, 1.00, 0),
        "gratitude-ru-next": (0.33, 0.68, 0),
        "choice-en-first": (0.50, 1.00, 0),
        "choice-en-next": (0.50, 0.53, 0),
        "exhaustion-uk-first": (0.33, 1.00, 0),
        "exhaustion-uk-next": (0.17, 0.66, 6),
    }


@pytest.mark.skipif(not BASELINE.exists(), reason="baseline artifact not copied in")
def test_a_mistyped_series_id_is_an_error_not_silence(capsys):
    assert tool.main([str(BASELINE), "--transcript", "series-scale-r"]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "series-scale-ru" in captured.err  # the ids it does hold
