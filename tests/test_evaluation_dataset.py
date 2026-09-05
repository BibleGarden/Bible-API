"""Schema and consistency tests for the scripture selection benchmark.

Validates ``evaluation/scenarios.json`` and ``evaluation/thresholds.json``
(structure, enums, coverage requirements). Pydantic is used instead of
jsonschema because it is already a runtime dependency of the service.

These tests are DB-independent. Verifying that every canonical reference
exists in ``cep_public`` is a separate, DB-dependent step:
``python3 evaluation/check_refs_db.py`` (see evaluation/README.md).
"""

import json
import re
from collections import Counter
from pathlib import Path
from typing import Literal, Optional

import pytest
from pydantic import BaseModel, ConfigDict, Field, model_validator

EVALUATION_DIR = Path(__file__).resolve().parent.parent / "evaluation"
SCENARIOS_PATH = EVALUATION_DIR / "scenarios.json"
THRESHOLDS_PATH = EVALUATION_DIR / "thresholds.json"

SEMVER_PATTERN = r"^\d+\.\d+\.\d+$"
SCENARIO_ID_PATTERN = r"^(ru|en|uk)-\d{3}$"

Language = Literal["ru", "en", "uk"]
Category = Literal["regular", "ambiguous", "short", "empty", "sensitive"]
Grade = Literal["relevant", "acceptable", "unacceptable"]


class Reference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    book_number: int = Field(ge=1, le=66)
    chapter: int = Field(ge=1, le=176)
    verse_start: int = Field(ge=1)
    verse_end: int = Field(ge=1)
    grade: Grade
    reason: str = Field(min_length=5)

    @model_validator(mode="after")
    def check_verse_range(self) -> "Reference":
        if self.verse_end < self.verse_start:
            raise ValueError("verse_end must be >= verse_start")
        return self


class PrayerContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str = Field(max_length=500)
    user_replies: list[str] = Field(max_length=5)


class Scenario(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=SCENARIO_ID_PATTERN)
    language: Language
    category: Category
    review_status: Literal["draft", "approved"]
    prayer_context: PrayerContext
    notes: str = ""
    references: list[Reference] = Field(min_length=3)

    @model_validator(mode="after")
    def check_consistency(self) -> "Scenario":
        if not self.id.startswith(f"{self.language}-"):
            raise ValueError("scenario id prefix must match its language")
        grades = {reference.grade for reference in self.references}
        if "relevant" not in grades:
            raise ValueError("each scenario needs at least one relevant reference")
        if "unacceptable" not in grades:
            raise ValueError("each scenario needs at least one unacceptable reference")
        seen = set()
        for reference in self.references:
            key = (
                reference.book_number,
                reference.chapter,
                reference.verse_start,
                reference.verse_end,
            )
            if key in seen:
                raise ValueError(f"duplicate reference in scenario: {key}")
            seen.add(key)
        if self.category == "empty":
            if self.prayer_context.topic != "":
                raise ValueError("empty-category scenarios must have an empty topic")
        elif self.prayer_context.topic == "":
            raise ValueError("only empty-category scenarios may have an empty topic")
        return self


class CoordinateSystem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    book_numbering: str
    psalm_numbering: Literal["english-masoretic"]
    psalm_numbering_note: str
    psalm_mapping_required: dict[str, str]


class Dataset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    version: str = Field(pattern=SEMVER_PATTERN)
    status: Literal["draft", "approved"]
    approved_by: Optional[str]
    created: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    ticket: str
    coordinate_system: CoordinateSystem
    grades: dict[Grade, str]
    scenarios: list[Scenario] = Field(min_length=20)

    @model_validator(mode="after")
    def check_grade_definitions(self) -> "Dataset":
        if set(self.grades) != {"relevant", "acceptable", "unacceptable"}:
            raise ValueError("grades must define exactly relevant/acceptable/unacceptable")
        return self


class RetrievalThresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    k: int = Field(ge=1)
    hit_rate_at_k_min: float = Field(ge=0.0, le=1.0)
    recall_at_k_min: float = Field(ge=0.0, le=1.0)
    mrr_min: float = Field(ge=0.0, le=1.0)
    unacceptable_share_in_top_k_max: float = Field(ge=0.0, le=1.0)


class FinalTop1Thresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relevant_share_min: float = Field(ge=0.0, le=1.0)
    relevant_or_acceptable_share_min: float = Field(ge=0.0, le=1.0)
    unacceptable_share_max: float = Field(ge=0.0, le=1.0)
    sensitive_unacceptable_share_max: float = Field(ge=0.0, le=1.0)
    sensitive_relevant_share_min: float = Field(ge=0.0, le=1.0)
    ungraded_review_required: bool


class FinalTop1CoverageRestrictedThresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: str = Field(min_length=5)
    relevant_share_min: float = Field(ge=0.0, le=1.0)
    relevant_or_acceptable_share_min: float = Field(ge=0.0, le=1.0)
    unacceptable_share_max: float = Field(ge=0.0, le=1.0)
    sensitive_unacceptable_share_max: float = Field(ge=0.0, le=1.0)
    sensitive_relevant_share_min: float = Field(ge=0.0, le=1.0)
    sensitive_relevant_or_acceptable_share_min: float = Field(ge=0.0, le=1.0)
    ungraded_review_required: bool


class DiversityThresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    no_repeat_window_selections: int = Field(ge=1)
    max_share_single_book_in_window: float = Field(gt=0.0, le=1.0)
    min_distinct_books_in_window: int = Field(ge=1)


class ThresholdChange(BaseModel):
    """One entry of the thresholds changelog: who changed which value, when, why."""

    model_config = ConfigDict(extra="forbid")

    version: str = Field(pattern=SEMVER_PATTERN)
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    approved_by: str = Field(min_length=1)
    change: str = Field(min_length=1)


class Thresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    version: str = Field(pattern=SEMVER_PATTERN)
    status: Literal["draft", "approved"]
    approved_by: Optional[str]
    approved_date: Optional[str] = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    changelog: list[ThresholdChange] = Field(default_factory=list)
    applies_to_dataset: str
    matching_rule: str
    retrieval_top_k: RetrievalThresholds
    final_top1: FinalTop1Thresholds
    final_top1_coverage_restricted: Optional[FinalTop1CoverageRestrictedThresholds] = None
    diversity: DiversityThresholds

    @model_validator(mode="after")
    def check_approval(self) -> "Thresholds":
        if self.status == "approved" and not self.approved_by:
            raise ValueError("approved thresholds must record the approver")
        return self


@pytest.fixture(scope="module")
def dataset() -> Dataset:
    raw = json.loads(SCENARIOS_PATH.read_text(encoding="utf-8"))
    return Dataset.model_validate(raw)


@pytest.fixture(scope="module")
def thresholds() -> Thresholds:
    raw = json.loads(THRESHOLDS_PATH.read_text(encoding="utf-8"))
    return Thresholds.model_validate(raw)


def test_scenarios_file_matches_schema(dataset):
    assert dataset.name == "scripture-selection-benchmark"


def test_scenario_ids_are_unique(dataset):
    ids = [scenario.id for scenario in dataset.scenarios]
    assert len(ids) == len(set(ids)), "scenario ids must be unique"


def test_language_coverage(dataset):
    counts = Counter(scenario.language for scenario in dataset.scenarios)
    assert set(counts) == {"ru", "en", "uk"}, "all three languages must be covered"
    for language, count in counts.items():
        assert count >= 5, f"language {language} needs at least 5 scenarios, got {count}"


def test_category_coverage(dataset):
    counts = Counter(scenario.category for scenario in dataset.scenarios)
    expected = {"regular", "ambiguous", "short", "empty", "sensitive"}
    assert set(counts) == expected, "all five categories must be covered"
    for category in expected:
        assert counts[category] >= 3, f"category {category} needs at least 3 scenarios"
    assert counts["sensitive"] >= 5, "sensitive states are mandatory coverage"


def test_empty_category_present_in_every_language(dataset):
    languages = {
        scenario.language
        for scenario in dataset.scenarios
        if scenario.category == "empty"
    }
    assert languages == {"ru", "en", "uk"}


def test_draft_dataset_is_marked_unapproved(dataset):
    if dataset.status == "draft":
        assert dataset.approved_by is None
    else:
        assert dataset.approved_by, "approved dataset must record the approver"


def test_thresholds_file_matches_schema(thresholds):
    assert thresholds.name == "scripture-selection-quality-thresholds"


def test_top1_forbids_unacceptable(thresholds):
    # Ticket acceptance criterion: the share of clearly inappropriate
    # top-1 selections must be zero, and stricter-than-zero is impossible.
    assert thresholds.final_top1.unacceptable_share_max == 0.0
    assert thresholds.final_top1.sensitive_unacceptable_share_max == 0.0


def test_sensitive_top1_requires_relevant(thresholds):
    # Agreed tightening (2026-08-24): for sensitive scenarios the top-1
    # must be graded relevant — acceptable is not enough.
    assert thresholds.final_top1.sensitive_relevant_share_min == 1.0


def test_coverage_restricted_thresholds_present(thresholds):
    # Agreed 2026-08-28 (ADR 0007, delegated to the orchestrator by Мария):
    # coverage-restricted (narrowed-pool) runs get their own, slightly
    # relaxed final_top1 section instead of being silently exempt.
    assert thresholds.final_top1_coverage_restricted is not None


def test_coverage_restricted_forbids_unacceptable(thresholds):
    restricted = thresholds.final_top1_coverage_restricted
    assert restricted.unacceptable_share_max == 0.0
    assert restricted.sensitive_unacceptable_share_max == 0.0


def test_coverage_restricted_sensitive_relaxation(thresholds):
    # Sensitive top-1 may drop from "must be relevant" (1.0) to 0.8 relevant,
    # but every sensitive top-1 must still be at least acceptable (1.0).
    restricted = thresholds.final_top1_coverage_restricted
    assert restricted.sensitive_relevant_share_min == 0.8
    assert restricted.sensitive_relevant_or_acceptable_share_min == 1.0


def test_coverage_restricted_matches_main_elsewhere(thresholds):
    # Everything other than the sensitive-relevant relaxation stays aligned
    # with the main final_top1 section.
    final = thresholds.final_top1
    restricted = thresholds.final_top1_coverage_restricted
    assert restricted.relevant_share_min == final.relevant_share_min
    assert restricted.relevant_or_acceptable_share_min == final.relevant_or_acceptable_share_min
    assert restricted.ungraded_review_required == final.ungraded_review_required


def test_threshold_ordering_is_consistent(thresholds):
    final = thresholds.final_top1
    assert final.relevant_share_min <= final.relevant_or_acceptable_share_min
    assert final.relevant_share_min <= final.sensitive_relevant_share_min
    retrieval = thresholds.retrieval_top_k
    assert retrieval.recall_at_k_min <= retrieval.hit_rate_at_k_min
    diversity = thresholds.diversity
    assert (
        diversity.min_distinct_books_in_window
        <= diversity.no_repeat_window_selections
    )
