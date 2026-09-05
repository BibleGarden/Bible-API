"""Tests for the pure parts of evaluation/question_semantic_bench.py.

ClickUp 86cbehyg8 (child of 86cbehxm2, bug 86cbehtkh). The tool answers one
question — does bge-m3 cosine separate "the same thought" where the lexical
filter cannot — and the answer is a set of precision/recall numbers that end
up in `evaluation/README.md` and in ADR 0016. So the arithmetic under those
numbers is pinned here: a confusion matrix that counts a false positive as a
true one, or a loader that silently drops a mislabelled pair, would move every
number in that table with nothing to notice it.

**No network.** The one test that touches the embedding client drives it
through `httpx.MockTransport`, exactly as `tests/test_embeddings.py` does, so
nothing here contacts the company server and nothing needs a key.

Covered:

* `load_pairs` accepts the committed set and refuses every shape it cannot
  score (missing field, unknown label, duplicate id) rather than skipping it;
* `evaluate` / `Metrics` — the confusion counts and the two rates, including
  the empty-denominator cases;
* the three decision rules, and in particular that `combined_flag` is an OR
  and never weakens the lexical verdict;
* `separation`'s gap, whose sign is the whole claim of the README table;
* `best_threshold` picks by F1 and breaks ties towards precision;
* `cosine` on hand-computable vectors, and `percentile` returning a value
  that was actually measured;
* `embed_all` deduplicates before calling the client — the artifacts repeat
  questions across pairs, and a duplicate is a wasted call on a CPU server.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import httpx
import pytest

EVALUATION = Path(__file__).resolve().parents[1] / "evaluation"
TOOL = EVALUATION / "question_semantic_bench.py"
PAIRS = EVALUATION / "question_pairs_labelled.json"

pytestmark = pytest.mark.skipif(
    not TOOL.exists(), reason="evaluation/ is not present in this container copy"
)


def _load_tool():
    spec = importlib.util.spec_from_file_location("question_semantic_bench", TOOL)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


tool = _load_tool()


def scored(label, *, trigram=0.1, cosine=0.5, lexical=False, language="ru", pid="p"):
    return {
        "id": pid,
        "language": language,
        "label": label,
        "trigram": trigram,
        "cosine": cosine,
        "lexical_repeat": lexical,
        "lexical_kind": "near" if lexical else "none",
    }


# ---------------------------------------------------------------------------
# load_pairs
# ---------------------------------------------------------------------------


def test_the_committed_pair_set_loads_and_is_labelled():
    if not PAIRS.exists():
        pytest.skip("the labelled pair set is not in this container copy")
    pairs = tool.load_pairs(PAIRS)
    assert len(pairs) >= 120
    languages = {pair.language for pair in pairs}
    assert {"ru", "uk", "en"} <= languages
    for language in ("ru", "uk", "en"):
        assert sum(1 for p in pairs if p.language == language) >= 25
    assert all(pair.label in ("repeat", "different") for pair in pairs)
    # Both classes are present in every language, or a per-language
    # precision/recall row would be meaningless.
    for language in ("ru", "uk", "en"):
        labels = {p.label for p in pairs if p.language == language}
        assert labels == {"repeat", "different"}


def test_load_pairs_refuses_a_missing_field(tmp_path):
    path = tmp_path / "pairs.json"
    path.write_text(
        json.dumps([{"id": "x", "language": "ru", "a": "А?", "label": "repeat"}]),
        encoding="utf-8",
    )
    with pytest.raises(ValueError) as exc_info:
        tool.load_pairs(path)
    assert "b" in str(exc_info.value)


def test_load_pairs_refuses_an_unknown_label(tmp_path):
    path = tmp_path / "pairs.json"
    path.write_text(
        json.dumps(
            [{"id": "x", "language": "ru", "a": "А?", "b": "Б?", "label": "maybe"}]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError) as exc_info:
        tool.load_pairs(path)
    assert "maybe" in str(exc_info.value)


def test_load_pairs_refuses_a_duplicate_id(tmp_path):
    one = {"id": "x", "language": "ru", "a": "А?", "b": "Б?", "label": "repeat"}
    path = tmp_path / "pairs.json"
    path.write_text(json.dumps([one, dict(one)]), encoding="utf-8")
    with pytest.raises(ValueError) as exc_info:
        tool.load_pairs(path)
    assert "duplicate" in str(exc_info.value)


def test_ambiguous_defaults_to_false_and_survives_when_set(tmp_path):
    path = tmp_path / "pairs.json"
    path.write_text(
        json.dumps(
            [
                {"id": "a", "language": "ru", "a": "А?", "b": "Б?", "label": "repeat"},
                {
                    "id": "b",
                    "language": "uk",
                    "a": "А?",
                    "b": "Б?",
                    "label": "different",
                    "ambiguous": True,
                    "kind": "same-topic",
                },
            ]
        ),
        encoding="utf-8",
    )
    first, second = tool.load_pairs(path)
    assert first.ambiguous is False and first.is_repeat is True
    assert second.ambiguous is True and second.is_repeat is False
    # Anything the benchmark does not itself use is kept, not dropped: the
    # `kind` of a hand-written pair is what makes a failure list readable.
    assert second.extra == {"kind": "same-topic"}


# ---------------------------------------------------------------------------
# The confusion matrix
# ---------------------------------------------------------------------------


def test_evaluate_counts_all_four_cells():
    rows = [
        scored("repeat", cosine=0.9),      # flagged, positive -> tp
        scored("repeat", cosine=0.1),      # missed         -> fn
        scored("different", cosine=0.9),   # flagged, negative -> fp
        scored("different", cosine=0.1),   # correctly quiet   -> tn
    ]
    metrics = tool.evaluate(rows, lambda row: tool.cosine_flag(row, 0.5))
    assert (metrics.tp, metrics.fp, metrics.fn, metrics.tn) == (1, 1, 1, 1)
    assert metrics.precision == pytest.approx(0.5)
    assert metrics.recall == pytest.approx(0.5)
    assert metrics.f1 == pytest.approx(0.5)


def test_rates_of_an_empty_denominator_are_one_not_a_crash():
    # Nothing flagged: precision has no denominator. Reporting 1.0 keeps the
    # sweep table renderable; the tp/fp/fn columns beside it say it is empty.
    metrics = tool.evaluate(
        [scored("repeat", cosine=0.1)], lambda row: tool.cosine_flag(row, 0.9)
    )
    assert (metrics.tp, metrics.fp) == (0, 0)
    assert metrics.precision == 1.0
    assert metrics.recall == 0.0
    assert metrics.f1 == 0.0


# ---------------------------------------------------------------------------
# The three rules
# ---------------------------------------------------------------------------


def test_cosine_flag_is_inclusive_at_the_threshold():
    assert tool.cosine_flag(scored("repeat", cosine=0.80), 0.80) is True
    assert tool.cosine_flag(scored("repeat", cosine=0.7999), 0.80) is False


def test_combined_is_an_or_and_never_unsays_the_lexical_verdict():
    lexical_only = scored("repeat", cosine=0.1, lexical=True)
    cosine_only = scored("repeat", cosine=0.95, lexical=False)
    neither = scored("different", cosine=0.1, lexical=False)
    assert tool.combined_flag(lexical_only, 0.80) is True
    assert tool.combined_flag(cosine_only, 0.80) is True
    assert tool.combined_flag(neither, 0.80) is False
    # And the OR can only ever add: at every threshold its recall is at least
    # the cosine's, which is the property the README table leans on.
    rows = [lexical_only, cosine_only, neither]
    for threshold in (0.0, 0.5, 0.8, 0.99, 1.0):
        cos = tool.evaluate(rows, lambda r, t=threshold: tool.cosine_flag(r, t))
        both = tool.evaluate(rows, lambda r, t=threshold: tool.combined_flag(r, t))
        assert both.tp >= cos.tp
        assert both.fn <= cos.fn


def test_lexical_flag_reads_the_production_verdict_not_the_score():
    # A pair whose trigram score is high but which `is_repeat` did not flag
    # must stay unflagged: the column is the endpoint's answer, not a
    # re-derivation of it from the score beside it.
    row = scored("repeat", trigram=0.99, cosine=0.1, lexical=False)
    assert tool.lexical_flag(row) is False


# ---------------------------------------------------------------------------
# separation, best_threshold
# ---------------------------------------------------------------------------


def test_separation_gap_is_positive_when_the_classes_do_not_overlap():
    rows = [
        scored("repeat", cosine=0.90),
        scored("repeat", cosine=0.85),
        scored("different", cosine=0.60),
    ]
    report = tool.separation(rows, "cosine")
    assert report["gap"] == pytest.approx(0.25)
    assert report["positive_min"] == pytest.approx(0.85)
    assert report["negative_max"] == pytest.approx(0.60)


def test_separation_gap_is_negative_when_they_interleave():
    rows = [
        scored("repeat", cosine=0.70),
        scored("different", cosine=0.80),
    ]
    assert tool.separation(rows, "cosine")["gap"] == pytest.approx(-0.10)


def test_separation_of_a_one_class_set_reports_no_gap():
    report = tool.separation([scored("repeat", cosine=0.9)], "cosine")
    assert report["gap"] is None


def test_best_threshold_maximises_f1_then_precision():
    rows = [
        scored("repeat", cosine=0.90),
        scored("repeat", cosine=0.80),
        scored("different", cosine=0.70),
    ]
    threshold, metrics = tool.best_threshold(
        rows, tool.cosine_flag, [0.70, 0.75, 0.80, 0.85]
    )
    # 0.75 and 0.80 give the same perfect matrix; the higher one is reported,
    # because it stands further from the different pair at 0.70.
    assert threshold == pytest.approx(0.80)
    assert (metrics.tp, metrics.fp, metrics.fn) == (2, 0, 0)


def test_best_threshold_prefers_precision_over_recall_at_equal_f1():
    # Both thresholds score F1 = 2/3: 0.60 flags 2 of the 4 repeats and
    # nothing else (P 1.00, R 0.50), 0.40 flags 3 repeats and both different
    # pairs (P 0.60, R 0.75). The precise one wins.
    rows = [
        scored("repeat", cosine=0.90),
        scored("repeat", cosine=0.80),
        scored("repeat", cosine=0.50),
        scored("repeat", cosine=0.10),
        scored("different", cosine=0.50),
        scored("different", cosine=0.50),
    ]
    threshold, metrics = tool.best_threshold(rows, tool.cosine_flag, [0.40, 0.60])
    assert threshold == pytest.approx(0.60)
    assert (metrics.tp, metrics.fp, metrics.fn) == (2, 0, 2)
    assert metrics.f1 == pytest.approx(2 / 3)


def test_threshold_grid_covers_the_range_inclusively():
    grid = tool.threshold_grid(0.50, 0.96, 0.01)
    assert grid[0] == pytest.approx(0.50)
    assert grid[-1] == pytest.approx(0.96)
    assert len(grid) == 47


# ---------------------------------------------------------------------------
# cosine, percentile
# ---------------------------------------------------------------------------


def test_cosine_of_hand_computable_vectors():
    assert tool.cosine([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
    assert tool.cosine([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)
    assert tool.cosine([1.0, 0.0], [-1.0, 0.0]) == pytest.approx(-1.0)
    # Not unit length: the function normalises rather than returning a dot.
    assert tool.cosine([3.0, 4.0], [3.0, 4.0]) == pytest.approx(1.0)


def test_cosine_of_a_zero_vector_is_zero_not_a_division_error():
    assert tool.cosine([0.0, 0.0], [1.0, 0.0]) == 0.0


def test_cosine_refuses_vectors_of_different_width():
    with pytest.raises(ValueError):
        tool.cosine([1.0, 0.0], [1.0, 0.0, 0.0])


def test_percentile_returns_a_measured_value():
    values = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    assert tool.percentile(values, 0.90) == pytest.approx(0.9)
    assert tool.percentile(values, 0.50) == pytest.approx(0.5)
    assert tool.percentile([], 0.9) == 0.0


# ---------------------------------------------------------------------------
# embed_all, against a mocked server (no network)
# ---------------------------------------------------------------------------


def test_embed_all_deduplicates_before_calling_the_client():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
    from embeddings import RemoteEmbeddingClient

    seen: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        seen.append(payload["input"])
        return httpx.Response(
            200,
            json={
                "data": [
                    {"index": index, "embedding": [1.0, 0.0]}
                    for index in range(len(payload["input"]))
                ]
            },
        )

    client = RemoteEmbeddingClient(
        endpoint="https://example.invalid/v1",
        api_key="unit-test",
        model="BAAI/bge-m3",
        dimensions=2,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda _s: None,
    )
    cache = tool.embed_all(client, ["А?", "Б?", "А?", "Б?", "В?"])
    assert seen == [["А?", "Б?", "В?"]]
    assert set(cache) == {"А?", "Б?", "В?"}
    assert cache["А?"] == [1.0, 0.0]
