"""
Contract, privacy and integration tests for the public scripture-selection
endpoint (ClickUp 86cb8vw1m, architect/adr/0006-scripture-select-api.md).

Gemini is never called here: the AI pipeline is either mocked, or exercised
through the no-AI safe-pool path of the live integration test.
"""

import hashlib
import hmac
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("TWINKLER_SYSTEM_PROMPT", "Серверная система")
os.environ.setdefault("TWINKLER_CLIENT_HMAC_KEY", "test-hmac-key")

from fastapi.testclient import TestClient

import client_ip
import middleware
import rate_limit
import scripture_select
from chunking import CHUNKING_VERSION
from main import app
from query_rewrite import SUPPORTED_LANGUAGES
from retrieval import (
    Candidate,
    FinalSelection,
    PassageText,
    SelectionResult,
    VerseText,
    split_exclusions,
)
from versification import (
    PsalmMap,
    build_psalm_map,
    canonical_counts_with_extras,
)

client = TestClient(app)
# Captured before the autouse fixture replaces it, for the live test below.
REAL_RUN_SELECTION = scripture_select._run_selection
REAL_GET_RESOURCES = scripture_select.get_resources

# Real Psalm versification maps of the indexed corpus, built from the same
# fixture the versification tests use: the highlight of a Psalm is only
# correct if the translation's own numbering is converted (ADR 0003).
_MAX_VERSES = {
    alias: {int(chapter): mx for chapter, mx in chapters.items()}
    for alias, chapters in json.loads(
        (Path(__file__).parent / "data" / "psalm_max_verses.json").read_text()
    ).items()
}
_CANONICAL_COUNTS = canonical_counts_with_extras(_MAX_VERSES["bsb"])
PSALM_MAPS = {
    code: PsalmMap(
        build_psalm_map(alias, _MAX_VERSES[alias], _CANONICAL_COUNTS)
    )
    # 1 syn (Septuagint numbering), 16 bsb (canonical), 20 ubh (Masoretic
    # chapters, Hebrew verse numbering) — the production corpus
    for code, alias in ((1, "syn"), (16, "bsb"), (20, "ubh"))
}

TOPIC = "Тревога перед операцией"
REPLY = "Боюсь за исход"
PASSAGE_TEXT = "Господь — Пастырь мой; я ни в чем не буду нуждаться."
PRIVATE_STRINGS = (TOPIC, REPLY, PASSAGE_TEXT)


def psalm_verses(numbers=range(1, 7)) -> list[VerseText]:
    return [VerseText(verse_number=n, text=f"стих {n}") for n in numbers]


def make_candidate(canonical_id: str = "v3:19.023.001-006") -> Candidate:
    return Candidate(
        canonical_id=canonical_id,
        book_number=19,
        chapter_number=23,
        verse_start=1,
        verse_end=6,
        score=0.71,
        best_variant=0,
        variant_scores={0: 0.71},
        passages=[
            PassageText(
                translation=1,
                alias="syn",
                book_number=19,
                chapter_number=22,
                verse_number_start=1,
                verse_number_end=6,
                title="Псалом Давида",
                text=PASSAGE_TEXT,
                verses=psalm_verses(),
            ),
            PassageText(
                translation=2,
                alias="rst",
                book_number=19,
                chapter_number=22,
                verse_number_start=1,
                verse_number_end=6,
                title=None,
                text="Другой перевод",
                verses=psalm_verses(),
            ),
        ],
    )


def make_final(
    method: str = "rerank",
    fallback_reason: str | None = None,
    source: str = "retrieval",
    selection_reason: str | None = None,
    candidate: Candidate | None = -1,  # sentinel: default candidate
    reason: str = "Speaks to fear before surgery.",
    highlight: tuple[int, int] | None = None,
) -> FinalSelection:
    if candidate == -1:
        candidate = make_candidate()
    selection = SelectionResult(
        candidates=[candidate] if candidate else [],
        source=source,
        fallback_reason=selection_reason,
        query_variants=["вариант"],
        rewrite_failed=False,
    )
    return FinalSelection(
        candidate=candidate,
        reason=reason,
        method=method,
        fallback_reason=fallback_reason,
        selection=selection,
        highlight=highlight,
    )


@pytest.fixture(autouse=True)
def selection_environment(monkeypatch):
    """Fake corpus resources + a mocked pipeline; no DB and no Gemini."""
    scripture_select._limiter.reset()
    monkeypatch.setattr(middleware, "_insert_request_log", Mock())
    resources = scripture_select.CorpusResources(
        index=SimpleNamespace(metas=[]),
        lexical={},
        translations={"ru": [(1, "syn"), (2, "rst")], "en": [(16, "bsb")]},
        loaded_at=0.0,
        psalm_maps=PSALM_MAPS,
    )
    monkeypatch.setattr(scripture_select, "get_resources", lambda: resources)
    runner = Mock(return_value=make_final())
    monkeypatch.setattr(scripture_select, "_run_selection", runner)
    return runner


def post(payload: dict, api_key: str | None = "test-api-key", **kwargs):
    headers = {} if api_key is None else {"X-API-Key": api_key}
    headers.update(kwargs.pop("headers", {}))
    return client.post(
        "/api/scripture/v1/select", headers=headers, json=payload, **kwargs
    )


# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------

def test_requires_api_key():
    assert post({"language": "ru", "topic": TOPIC}, api_key=None).status_code == 403
    assert post({"language": "ru"}, api_key="wrong").status_code == 403


def test_returns_canonical_coordinates_title_and_translation_text():
    response = post({
        "language": "ru", "topic": TOPIC, "user_replies": [REPLY],
    })

    assert response.status_code == 200
    body = response.json()
    assert body["language"] == "ru"
    assert body["canonical"] == {
        "canonical_id": "v3:19.023.001-006",
        "book_number": 19,
        "chapter_number": 23,
        "verse_start": 1,
        "verse_end": 6,
    }
    assert body["passage"] == {
        "translation": 1,
        "translation_alias": "syn",
        "book_number": 19,
        "chapter_number": 22,
        "verse_start": 1,
        "verse_end": 6,
        "title": "Псалом Давида",
        "text": PASSAGE_TEXT,
    }
    assert body["source"] == "rerank"
    assert body["fallback_reason"] is None
    assert body["history_reset"] is False


def test_model_reason_is_never_returned_to_the_client():
    response = post({"language": "ru", "topic": TOPIC})

    assert response.status_code == 200
    assert "reason" not in response.json()
    assert "Speaks to fear" not in response.text
    assert "reason" not in scripture_select.SelectResponse.model_fields


def test_passes_prayer_context_and_top_k_to_the_pipeline(selection_environment):
    post({
        "language": "ru", "topic": f"  {TOPIC}  ", "user_replies": [REPLY, "   "],
    })

    request = selection_environment.call_args.args[1]
    assert request.language == "ru"
    assert request.topic == TOPIC
    assert request.user_replies == (REPLY,)
    assert request.top_k == scripture_select.TOP_K


def test_renders_the_requested_translation():
    response = post({"language": "ru", "topic": TOPIC, "translation": 2})

    assert response.status_code == 200
    assert response.json()["passage"]["translation"] == 2
    assert response.json()["passage"]["text"] == "Другой перевод"


def test_rejects_a_translation_of_another_language(selection_environment):
    response = post({"language": "ru", "topic": TOPIC, "translation": 16})

    assert response.status_code == 422
    assert "not available" in response.json()["detail"]
    selection_environment.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        {"language": "ru", "topic": TOPIC, "unknown": 1},
        {"language": "de", "topic": TOPIC},
        {"language": "ru", "topic": "x" * (scripture_select.MAX_TOPIC_CHARS + 1)},
        {"language": "ru", "user_replies": ["x"] * (scripture_select.MAX_REPLIES + 1)},
        {
            "language": "ru",
            "user_replies": ["x" * (scripture_select.MAX_REPLY_CHARS + 1)],
        },
        {
            "language": "ru",
            "exclude_canonical_ids": ["v3:19.023.001"],
        },
        {"language": "ru", "exclude_canonical_ids": ["'; DROP TABLE --"]},
        {
            "language": "ru",
            "exclude_canonical_ids": ["v3:19.023.001-006"]
            * (scripture_select.MAX_EXCLUSIONS + 1),
        },
        {"language": "ru", "translation": 0},
        {"topic": TOPIC},
    ],
)
def test_rejects_invalid_requests(payload, selection_environment):
    assert post(payload).status_code == 422
    selection_environment.assert_not_called()


def test_validation_errors_never_echo_the_prayer_text():
    """The default FastAPI 422 body would return `input` verbatim."""
    response = post({
        "language": "ru",
        "topic": TOPIC,
        "user_replies": [REPLY],
        "unknown": TOPIC,
    })

    assert response.status_code == 422
    body = response.json()
    assert list(body) == ["detail"], "flat ErrorResponse shape, not an array"
    assert isinstance(body["detail"], str)
    assert body["detail"] == "unknown field: unknown"
    assert TOPIC not in response.text
    assert REPLY not in response.text


@pytest.mark.parametrize(
    ("payload", "detail"),
    [
        ({"language": "ru", "extra": 1}, "unknown field: extra"),
        ({}, "language is required"),
        (
            {"language": "ru", "topic": "x" * 501},
            "topic is too long",
        ),
        (
            {"language": "ru", "user_replies": ["x" * 1001]},
            "user_replies[0] is too long",
        ),
        (
            {"language": "ru", "user_replies": ["x"] * 11},
            "user_replies has too many items",
        ),
        (
            {"language": "ru", "exclude_canonical_ids": ["nope"]},
            "exclude_canonical_ids[0] has an invalid format",
        ),
        ({"language": "de"}, "language has an unsupported value"),
        ({"language": "ru", "translation": 0}, "translation is out of range"),
        ({"language": "ru", "topic": 5}, "topic has a wrong type"),
    ],
)
def test_validation_details_name_the_category_and_the_field(payload, detail):
    response = post(payload)

    assert response.status_code == 422
    assert response.json() == {"detail": detail}


def test_a_field_name_that_is_itself_prayer_text_is_not_echoed():
    response = post({"language": "ru", TOPIC: "1"})

    assert response.status_code == 422
    assert response.json() == {"detail": "unknown field: field"}
    assert TOPIC not in response.text


def test_malformed_json_body_is_reported_without_the_body():
    response = client.post(
        "/api/scripture/v1/select",
        headers={"X-API-Key": "test-api-key", "Content-Type": "application/json"},
        content=f'{{"language": "ru", "topic": "{TOPIC}"'.encode(),
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "request body is not valid JSON"}
    assert TOPIC not in response.text


def test_validation_summary_is_capped():
    payload = {"language": "de", "topic": "x" * 501, "a": 1, "b": 2, "c": 3}

    detail = post(payload).json()["detail"]

    assert detail.count(";") < scripture_select._MAX_REPORTED_ERRORS


def test_twinkler_validation_body_is_unchanged():
    """Only this endpoint gets the sanitised body; Twinkler keeps its own."""
    response = client.post(
        "/api/twinkler/v1/complete",
        headers={"X-API-Key": "test-api-key"},
        json={"user": "Запрос", "system": "клиентская система"},
    )

    assert response.status_code == 422
    assert isinstance(response.json()["detail"], list)


def test_rejects_replies_over_the_total_budget(selection_environment):
    chunk = "x" * scripture_select.MAX_REPLY_CHARS
    payload = {"language": "ru", "user_replies": [chunk] * 5}

    response = post(payload)

    assert response.status_code == 422
    assert response.json()["detail"] == "Replies are too long"
    selection_environment.assert_not_called()


def test_empty_topic_is_valid_and_served_from_the_safe_pool(monkeypatch):
    monkeypatch.setattr(
        scripture_select,
        "_run_selection",
        Mock(return_value=make_final(
            method="fallback_top1",
            fallback_reason="safe_pool",
            source="safe_pool",
            selection_reason="empty_topic",
        )),
    )

    response = post({"language": "ru", "topic": ""})

    assert response.status_code == 200
    assert response.json()["source"] == "safe_pool"
    assert response.json()["fallback_reason"] == "empty_topic"


@pytest.mark.parametrize(
    ("method", "fallback_reason", "source", "selection_reason", "expected"),
    [
        ("rerank", None, "retrieval", None, ("rerank", None)),
        (
            "fallback_top1", "rerank_failed", "retrieval", None,
            ("retrieval_fallback", "rerank_failed"),
        ),
        (
            "fallback_top1", "deadline", "retrieval", None,
            ("retrieval_fallback", "deadline"),
        ),
        (
            "fallback_top1", "no_reranker", "retrieval", None,
            ("retrieval_fallback", "no_reranker"),
        ),
        (
            "fallback_top1", "safe_pool", "safe_pool", "ai_unavailable",
            ("safe_pool", "ai_unavailable"),
        ),
        (
            "fallback_top1", "safe_pool", "safe_pool", "deadline",
            ("safe_pool", "deadline"),
        ),
    ],
)
def test_reports_the_selection_source_and_fallback_category(
    monkeypatch, method, fallback_reason, source, selection_reason, expected
):
    monkeypatch.setattr(
        scripture_select,
        "_run_selection",
        Mock(return_value=make_final(
            method=method,
            fallback_reason=fallback_reason,
            source=source,
            selection_reason=selection_reason,
        )),
    )

    body = post({"language": "ru", "topic": TOPIC}).json()

    assert (body["source"], body["fallback_reason"]) == expected


def test_unknown_fallback_category_degrades_to_null_not_to_an_error(monkeypatch):
    monkeypatch.setattr(
        scripture_select,
        "_run_selection",
        Mock(return_value=make_final(
            method="fallback_top1", fallback_reason="something_new",
        )),
    )

    response = post({"language": "ru", "topic": TOPIC})

    assert response.status_code == 200
    assert response.json()["source"] == "retrieval_fallback"
    assert response.json()["fallback_reason"] is None


def test_no_candidate_is_a_service_error_not_an_empty_passage(monkeypatch):
    monkeypatch.setattr(
        scripture_select,
        "_run_selection",
        Mock(return_value=make_final(
            method="none", fallback_reason="no_candidates", candidate=None,
        )),
    )

    response = post({"language": "ru", "topic": TOPIC})

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Scripture selection temporarily unavailable"
    }


def test_unavailable_corpus_is_reported_as_503(monkeypatch, selection_environment):
    def unavailable():
        raise scripture_select.ScriptureSelectUnavailable("vector index is empty")

    monkeypatch.setattr(scripture_select, "get_resources", unavailable)

    response = post({"language": "ru", "topic": TOPIC})

    assert response.status_code == 503
    assert "vector index" not in response.text
    selection_environment.assert_not_called()


# ---------------------------------------------------------------------------
# Key-verse highlight (optional, additive)
# ---------------------------------------------------------------------------

def select_with_highlight(monkeypatch, highlight, payload=None, **final_kwargs):
    monkeypatch.setattr(
        scripture_select, "_run_selection",
        Mock(return_value=make_final(highlight=highlight, **final_kwargs)),
    )
    return post(payload or {"language": "ru", "topic": TOPIC})


def test_highlight_carries_both_coordinate_systems(monkeypatch):
    """The chosen chunk is canonical Psalm 23 rendered as syn 22; a
    highlight of markers 4-4 is verse 4 in both — the Septuagint chapter
    shift moves the chapter, not the verse."""
    response = select_with_highlight(monkeypatch, (4, 4))

    assert response.status_code == 200
    body = response.json()
    assert body["highlight"] == {
        "canonical": {
            "book_number": 19, "chapter_number": 23,
            "verse_start": 4, "verse_end": 4,
        },
        "passage": {"chapter_number": 22, "verse_start": 4, "verse_end": 4},
    }


def test_highlight_is_always_inside_the_returned_passage(monkeypatch):
    body = select_with_highlight(monkeypatch, (2, 4)).json()

    passage, highlight = body["passage"], body["highlight"]["passage"]
    assert passage["chapter_number"] == highlight["chapter_number"]
    assert passage["verse_start"] <= highlight["verse_start"]
    assert highlight["verse_end"] <= passage["verse_end"]
    assert highlight["verse_end"] - highlight["verse_start"] + 1 <= 3


def test_a_septuagint_chunk_starting_mid_chapter_keeps_its_verse_numbers(
    monkeypatch,
):
    """Markers are positions in the chunk, verse numbers are not: a chunk
    that starts at syn 22:3 must highlight verse 4 for marker 2."""
    candidate = make_candidate()
    candidate.passages[0].verses = psalm_verses(range(3, 7))
    candidate.passages[0].verse_number_start = 3

    body = select_with_highlight(
        monkeypatch, (2, 2), candidate=candidate
    ).json()

    assert body["highlight"]["passage"]["verse_start"] == 4
    assert body["highlight"]["canonical"]["verse_start"] == 4


def test_a_counted_superscription_shifts_the_canonical_numbers(monkeypatch):
    """syn 3 counts the inscription as verse 1, the canon does not: syn 3:2
    is canonical 3:1."""
    candidate = make_candidate("v3:19.003.001-004")
    candidate.chapter_number = 3
    candidate.verse_start, candidate.verse_end = 1, 4
    passage = candidate.passages[0]
    passage.chapter_number = 3
    passage.verse_number_start, passage.verse_number_end = 1, 5
    passage.verses = psalm_verses(range(1, 6))

    body = select_with_highlight(
        monkeypatch, (2, 3), candidate=candidate
    ).json()

    assert body["highlight"]["passage"] == {
        "chapter_number": 3, "verse_start": 2, "verse_end": 3,
    }
    assert body["highlight"]["canonical"] == {
        "book_number": 19, "chapter_number": 3,
        "verse_start": 1, "verse_end": 2,
    }


def test_a_ukrainian_psalm_uses_the_masoretic_chapter_and_shifted_verses(
    monkeypatch, selection_environment
):
    """ubh keeps Masoretic chapter numbers but counts the superscription,
    so only the verse numbers shift."""
    candidate = make_candidate("v3:19.003.001-004")
    candidate.chapter_number = 3
    candidate.verse_start, candidate.verse_end = 1, 4
    candidate.passages = [
        PassageText(
            translation=20, alias="ubh", book_number=19, chapter_number=3,
            verse_number_start=1, verse_number_end=5, title=None,
            text="Псалом", verses=psalm_verses(range(1, 6)),
        )
    ]
    resources = scripture_select.CorpusResources(
        index=SimpleNamespace(metas=[]), lexical={},
        translations={"uk": [(20, "ubh")]}, loaded_at=0.0,
        psalm_maps=PSALM_MAPS,
    )
    monkeypatch.setattr(scripture_select, "get_resources", lambda: resources)

    body = select_with_highlight(
        monkeypatch, (2, 2), payload={"language": "uk", "topic": TOPIC},
        candidate=candidate,
    ).json()

    assert body["highlight"]["passage"] == {
        "chapter_number": 3, "verse_start": 2, "verse_end": 2,
    }
    assert body["highlight"]["canonical"]["chapter_number"] == 3
    assert body["highlight"]["canonical"]["verse_start"] == 1


def test_outside_the_psalms_both_coordinate_systems_agree(monkeypatch):
    candidate = make_candidate("v3:43.014.027-027")
    candidate.book_number, candidate.chapter_number = 43, 14
    candidate.verse_start, candidate.verse_end = 27, 27
    candidate.passages = [
        PassageText(
            translation=1, alias="syn", book_number=43, chapter_number=14,
            verse_number_start=27, verse_number_end=27, title=None,
            text="Мир оставляю вам",
            verses=[VerseText(27, "Мир оставляю вам")],
        )
    ]

    body = select_with_highlight(
        monkeypatch, (1, 1), candidate=candidate
    ).json()

    assert body["highlight"] == {
        "canonical": {
            "book_number": 43, "chapter_number": 14,
            "verse_start": 27, "verse_end": 27,
        },
        "passage": {"chapter_number": 14, "verse_start": 27, "verse_end": 27},
    }


def test_a_psalm_without_a_stored_mapping_is_served_without_a_highlight(
    monkeypatch,
):
    resources = scripture_select.CorpusResources(
        index=SimpleNamespace(metas=[]), lexical={},
        translations={"ru": [(1, "syn")]}, loaded_at=0.0, psalm_maps={},
    )
    monkeypatch.setattr(scripture_select, "get_resources", lambda: resources)

    body = select_with_highlight(monkeypatch, (4, 4)).json()

    assert "highlight" not in body
    assert body["passage"]["text"] == PASSAGE_TEXT


def test_a_span_the_server_refuses_is_dropped_not_clamped(monkeypatch):
    """`build_response` re-checks the bounds: whatever the pipeline hands
    over, the contract never grows a range out of it."""
    for indices in ((0, 1), (5, 9), (3, 2)):
        assert "highlight" not in select_with_highlight(
            monkeypatch, indices
        ).json()


def test_without_a_highlight_the_response_is_byte_for_byte_the_old_one():
    """Backward compatibility: an absent highlight is an absent KEY."""
    body = post({"language": "ru", "topic": TOPIC}).json()

    assert "highlight" not in body
    assert set(body) == {
        "language", "canonical", "passage", "source",
        "fallback_reason", "history_reset",
    }


def test_openapi_does_not_advertise_a_null_highlight():
    """n9: `null` is not a value this endpoint can return — the key is
    omitted — so the published schema must not offer it either."""
    schema = client.get("/openapi.json").json()
    field = schema["components"]["schemas"]["SelectResponse"]["properties"][
        "highlight"
    ]

    assert "anyOf" not in field
    refs = json.dumps(field.get("allOf", field.get("$ref", "")))
    assert "HighlightModel" in refs and "null" not in refs
    # optional all the same: absent, never null
    assert "highlight" not in schema["components"]["schemas"][
        "SelectResponse"
    ].get("required", [])
    assert "KEY IS ABSENT ENTIRELY" in field.get("description", "")


@pytest.mark.parametrize(
    "final_kwargs",
    [
        {"method": "fallback_top1", "fallback_reason": "rerank_failed"},
        {"method": "fallback_top1", "fallback_reason": "deadline"},
        {
            "method": "fallback_top1", "fallback_reason": "safe_pool",
            "source": "safe_pool", "selection_reason": "empty_topic",
        },
    ],
)
def test_fallback_answers_carry_no_highlight(monkeypatch, final_kwargs):
    """Nothing but a live AI choice may produce one — the fallbacks never
    set it, and the response must not invent one."""
    body = select_with_highlight(monkeypatch, None, **final_kwargs).json()

    assert "highlight" not in body
    assert body["passage"]["text"] == PASSAGE_TEXT


def test_the_highlight_never_leaks_the_prayer_context(monkeypatch, caplog):
    with caplog.at_level("DEBUG"):
        response = select_with_highlight(
            monkeypatch, (4, 4),
            payload={"language": "ru", "topic": TOPIC, "user_replies": [REPLY]},
        )

    assert response.status_code == 200
    assert "highlight" in response.json()
    assert TOPIC not in response.text and REPLY not in response.text
    for secret in PRIVATE_STRINGS:
        assert secret not in caplog.text


# ---------------------------------------------------------------------------
# Exclusions and the chunking-version reset
# ---------------------------------------------------------------------------

def test_excludes_already_shown_passages(selection_environment):
    shown = ["v3:19.023.001-006", "v3:20.003.005-006"]

    response = post({
        "language": "ru", "topic": TOPIC, "exclude_canonical_ids": shown,
    })

    assert response.status_code == 200
    assert response.json()["history_reset"] is False
    request = selection_environment.call_args.args[1]
    assert request.exclude_canonical_ids == frozenset(shown)


def test_exclusions_of_another_chunking_version_reset_the_history(
    selection_environment,
):
    stale = f"v{CHUNKING_VERSION + 1}:19.023.001-006"

    response = post({
        "language": "ru",
        "topic": TOPIC,
        "exclude_canonical_ids": [stale, "v3:20.003.005-006"],
    })

    assert response.status_code == 200
    assert response.json()["history_reset"] is True
    request = selection_environment.call_args.args[1]
    assert request.exclude_canonical_ids == frozenset({"v3:20.003.005-006"})


def test_split_exclusions_keeps_only_the_current_corpus():
    current, stale = split_exclusions(
        ["v3:19.023.001-006", "v2:19.023.001-006", "nonsense"], 3
    )

    assert current == frozenset({"v3:19.023.001-006"})
    assert stale == ["v2:19.023.001-006", "nonsense"]


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

def test_rate_limits_per_client(monkeypatch):
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(
        scripture_select, "SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE", 1
    )

    first = post({"language": "ru", "topic": TOPIC})
    second = post({"language": "ru", "topic": TOPIC})

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.headers["Retry-After"] == "60"
    assert second.json()["detail"] == "Scripture selection request limit exceeded"


def test_rate_limits_globally(monkeypatch):
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(scripture_select, "SCRIPTURE_SELECT_REQUESTS_PER_MINUTE", 1)

    post({"language": "ru", "topic": TOPIC})
    second = post({"language": "ru", "topic": TOPIC})

    assert second.status_code == 429


def test_rate_limit_budget_is_independent_from_twinkler(monkeypatch):
    import twinkler_ai

    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(scripture_select, "SCRIPTURE_SELECT_REQUESTS_PER_MINUTE", 1)

    post({"language": "ru", "topic": TOPIC})

    assert len(twinkler_ai._request_times) == 0


def test_rate_limiter_fails_closed_without_the_hmac_key(monkeypatch):
    monkeypatch.setattr(client_ip, "TWINKLER_CLIENT_HMAC_KEY", "")

    response = post({"language": "ru", "topic": TOPIC})

    assert response.status_code == 503


def test_client_identity_comes_from_the_peer_unless_the_proxy_is_trusted(
    monkeypatch, selection_environment
):
    reserve = Mock()
    monkeypatch.setattr(scripture_select, "_reserve_rate_limit", reserve)

    post(
        {"language": "ru", "topic": TOPIC},
        headers={"X-Forwarded-For": "203.0.113.7"},
    )
    assert reserve.call_args.args == ("testclient",)

    monkeypatch.setattr(client_ip, "TRUSTED_PROXY_IPS", frozenset({"testclient"}))
    post(
        {"language": "ru", "topic": TOPIC},
        headers={"X-Forwarded-For": "203.0.113.7"},
    )
    assert reserve.call_args.args == ("203.0.113.7",)


# ---------------------------------------------------------------------------
# Privacy
# ---------------------------------------------------------------------------

def test_statistics_store_no_prayer_context_no_passage_and_no_raw_client(
    monkeypatch,
):
    started_threads = []

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            started_threads.append((self.args, self.kwargs))

    monkeypatch.setattr(middleware, "threading", SimpleNamespace(Thread=FakeThread))

    response = post(
        {"language": "ru", "topic": TOPIC, "user_replies": [REPLY]},
        headers={"User-Agent": "private-device-details"},
    )

    assert response.status_code == 200
    assert len(started_threads) == 1
    _args, kwargs = started_threads[0]
    logged = kwargs["args"]
    expected_client = hmac.new(
        b"test-hmac-key", b"testclient", hashlib.sha256
    ).hexdigest()[:40]
    assert logged[:3] == ("/api/scripture/v1/select", "POST", 200)
    assert logged[4:] == (expected_client, "")
    recorded = repr(logged)
    for secret in PRIVATE_STRINGS + ("v3:19.023.001-006", "testclient",
                                     "private-device-details"):
        assert secret not in recorded


def degradation_paths():
    """Every way a request can end: success, each fallback, each error."""
    payload = {"language": "ru", "topic": TOPIC, "user_replies": [REPLY]}
    return [
        pytest.param(
            "rerank", payload, {}, 200, id="rerank",
        ),
        pytest.param(
            "retrieval_fallback", payload,
            {"method": "fallback_top1", "fallback_reason": "rerank_failed"},
            200, id="rerank_failed",
        ),
        pytest.param(
            "retrieval_fallback", payload,
            {"method": "fallback_top1", "fallback_reason": "deadline"},
            200, id="deadline",
        ),
        pytest.param(
            "safe_pool", payload,
            {
                "method": "fallback_top1", "fallback_reason": "safe_pool",
                "source": "safe_pool", "selection_reason": "ai_unavailable",
            },
            200, id="ai_unavailable",
        ),
        pytest.param("unavailable", payload, {}, 503, id="unavailable"),
        pytest.param("rate_limited", payload, {}, 429, id="rate_limited"),
        pytest.param(
            "validation",
            {
                "language": "ru",
                "topic": TOPIC,
                "user_replies": [REPLY],
                TOPIC: REPLY,
            },
            {}, 422, id="validation",
        ),
    ]


@pytest.mark.parametrize(
    ("mode", "payload", "final_kwargs", "expected_status"), degradation_paths()
)
def test_no_path_leaks_the_prayer_context_into_body_or_logs(
    monkeypatch, caplog, mode, payload, final_kwargs, expected_status
):
    if mode == "unavailable":
        monkeypatch.setattr(
            scripture_select,
            "_run_selection",
            Mock(side_effect=scripture_select.ScriptureSelectUnavailable(
                "database is not available"
            )),
        )
    elif mode == "rate_limited":
        monkeypatch.setattr(
            scripture_select,
            "_reserve_rate_limit",
            Mock(side_effect=rate_limit.RateLimitError("limit exceeded", 42)),
        )
    elif final_kwargs:
        monkeypatch.setattr(
            scripture_select, "_run_selection",
            Mock(return_value=make_final(**final_kwargs)),
        )

    with caplog.at_level("DEBUG"):
        response = post(payload)

    assert response.status_code == expected_status
    # The prayer context never comes back, on any path; the passage comes
    # back only as the successful answer itself.
    assert TOPIC not in response.text
    assert REPLY not in response.text
    if expected_status != 200:
        assert PASSAGE_TEXT not in response.text
    # Nothing of it is ever logged, on any path.
    for secret in PRIVATE_STRINGS:
        assert secret not in caplog.text


def test_rate_limited_response_carries_no_prayer_context():
    body = {"language": "ru", "topic": TOPIC, "user_replies": [REPLY]}
    scripture_select._limiter.reset()

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            scripture_select, "SCRIPTURE_SELECT_REQUESTS_PER_CLIENT_PER_MINUTE", 1
        )
        post(body)
        response = post(body)

    assert response.status_code == 429
    assert response.json() == {
        "detail": "Scripture selection request limit exceeded"
    }
    assert TOPIC not in response.text and REPLY not in response.text


# ---------------------------------------------------------------------------
# Corpus cache
# ---------------------------------------------------------------------------

def test_a_failed_refresh_keeps_serving_the_cached_corpus(monkeypatch, caplog):
    resources = scripture_select.CorpusResources(
        index=SimpleNamespace(metas=[]), lexical={},
        translations={"ru": [(1, "syn")]}, loaded_at=0.0,
    )
    monkeypatch.setattr(scripture_select, "_resources", resources)
    monkeypatch.setattr(scripture_select, "SCRIPTURE_INDEX_CACHE_SECONDS", 0)
    monkeypatch.setattr(
        scripture_select, "_load_resources",
        Mock(side_effect=scripture_select.ScriptureSelectUnavailable("db down")),
    )

    with caplog.at_level("WARNING"):
        served = REAL_GET_RESOURCES()

    assert served is resources
    assert "db down" not in caplog.text
    assert served.loaded_at > 0.0, "the refresh timer is reset, not retried hot"


def test_a_cold_cache_propagates_the_failure(monkeypatch):
    monkeypatch.setattr(scripture_select, "_resources", None)
    monkeypatch.setattr(
        scripture_select, "_load_resources",
        Mock(side_effect=scripture_select.ScriptureSelectUnavailable("db down")),
    )

    with pytest.raises(scripture_select.ScriptureSelectUnavailable):
        REAL_GET_RESOURCES()


def test_middleware_pseudonymizes_every_prayer_endpoint():
    assert "/api/scripture/v1/select" in middleware.PRIVATE_PATHS
    assert "/api/twinkler/v1/complete" in middleware.PRIVATE_PATHS


# ---------------------------------------------------------------------------
# OpenAPI
# ---------------------------------------------------------------------------

def test_openapi_documents_the_public_contract():
    schema = app.openapi()
    operation = schema["paths"]["/api/scripture/v1/select"]["post"]

    assert operation["tags"] == ["Scripture"]
    assert operation["summary"]
    assert "retrieval_fallback" in operation["description"]
    assert {"200", "403", "422", "429", "503"} <= set(operation["responses"])
    assert "Retry-After" in operation["responses"]["429"]["headers"]

    request_schema = schema["components"]["schemas"]["SelectRequest"]
    assert request_schema["additionalProperties"] is False
    assert "example" in request_schema
    assert request_schema["properties"]["topic"]["maxLength"] == (
        scripture_select.MAX_TOPIC_CHARS
    )
    assert request_schema["properties"]["exclude_canonical_ids"]["maxItems"] == (
        scripture_select.MAX_EXCLUSIONS
    )

    response_schema = schema["components"]["schemas"]["SelectResponse"]
    assert "reason" not in response_schema["properties"]
    assert set(response_schema["properties"]) == {
        "language", "canonical", "passage", "highlight", "source",
        "fallback_reason", "history_reset",
    }
    # optional and additive: no client written against the previous
    # contract has to change
    assert "highlight" not in response_schema.get("required", [])
    highlight = schema["components"]["schemas"]["HighlightModel"]
    assert set(highlight["properties"]) == {"canonical", "passage"}
    canonical = schema["components"]["schemas"]["HighlightCanonical"]
    assert set(canonical["properties"]) == {
        "book_number", "chapter_number", "verse_start", "verse_end",
    }
    assert set(schema["components"]["schemas"]["HighlightPassage"]["properties"]) == {
        "chapter_number", "verse_start", "verse_end",
    }


def test_twinkler_contract_is_unchanged():
    schema = app.openapi()

    for path in ("/api/twinkler/v1/complete", "/api/twinkler/v1/transcribe"):
        assert path in schema["paths"]
    complete = schema["paths"]["/api/twinkler/v1/complete"]["post"]
    assert {"200", "403", "422", "429", "502", "503"} <= set(complete["responses"])


def test_language_enum_mirrors_the_rewrite_stage():
    assert tuple(item.value for item in scripture_select.Language) == tuple(
        SUPPORTED_LANGUAGES
    )


# ---------------------------------------------------------------------------
# Live integration (real DB + real corpus, no Gemini: safe-pool path)
# ---------------------------------------------------------------------------

def _database_available() -> bool:
    from database import create_connection

    try:
        connection = create_connection()
    except Exception:
        return False
    if connection is None:
        return False
    connection.close()
    return True


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
def test_empty_topic_returns_a_real_verse_range_of_the_translation(monkeypatch):
    """Acceptance: the endpoint only ever returns verses that exist in the DB.

    Uses the empty-topic path, which is served from the curated safe pool
    without a single Gemini call.
    """
    from database import create_connection

    resources = scripture_select._load_resources()
    monkeypatch.setattr(scripture_select, "get_resources", lambda: resources)
    monkeypatch.setattr(scripture_select, "_run_selection", REAL_RUN_SELECTION)

    response = post({"language": "ru", "topic": ""})

    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "safe_pool"
    assert body["fallback_reason"] == "empty_topic"
    assert body["canonical"]["canonical_id"].startswith(f"v{CHUNKING_VERSION}:")
    passage = body["passage"]
    assert passage["text"].strip()

    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT verse_number FROM translation_verses
            WHERE translation = %s AND book_number = %s AND chapter_number = %s
              AND verse_number BETWEEN %s AND %s
            """,
            (
                passage["translation"], passage["book_number"],
                passage["chapter_number"], passage["verse_start"],
                passage["verse_end"],
            ),
        )
        verses = {row["verse_number"] for row in cursor.fetchall()}
    finally:
        cursor.close()
        connection.close()

    expected = set(range(passage["verse_start"], passage["verse_end"] + 1))
    assert verses == expected, "returned range must exist in translation_verses"
