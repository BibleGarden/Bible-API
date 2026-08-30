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
os.environ.setdefault("AI_CLIENT_HMAC_KEY", "test-hmac-key")

from fastapi import HTTPException
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
from trusted_proxies import TrustedProxies
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
        "/api/ai/scripture", headers=headers, json=payload, **kwargs
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
        # additive (ClickUp 86cbb1mq7): the passage's own verse boundaries,
        # in the numbering of the translation the passage is returned in
        "verses": [
            {"number": n, "text": f"стих {n}", "paragraph_start": n == 1}
            for n in range(1, 7)
        ],
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


def test_rejects_a_translation_missing_from_the_renderable_catalogue(
    monkeypatch, selection_environment
):
    """422 now means exactly one thing: not in this language's catalogue —
    another language's, inactive, unknown, or unresolvable against the
    canonical corpus."""
    resources = scripture_select.CorpusResources(
        index=SimpleNamespace(metas=[]), lexical={},
        translations={"ru": [(1, "syn")]}, loaded_at=0.0,
        psalm_maps=PSALM_MAPS, indexed={"ru": [(1, "syn")]}, primary={"ru": 1},
    )
    monkeypatch.setattr(scripture_select, "get_resources", lambda: resources)

    response = post({"language": "ru", "topic": TOPIC, "translation": 11})

    assert response.status_code == 422
    assert response.json() == {
        "detail": "Translation 11 is not available for ru"
    }
    selection_environment.assert_not_called()


def test_a_422_repeats_nothing_but_the_code_the_caller_sent():
    response = post({
        "language": "ru", "topic": TOPIC, "user_replies": [REPLY],
        "translation": 16,
    })

    assert response.status_code == 422
    assert TOPIC not in response.text and REPLY not in response.text


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
        "/api/ai/scripture",
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
        "/api/ai/question",
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


def test_the_primary_path_is_byte_for_byte_the_pre_catalogue_one(monkeypatch):
    """Backward compatibility (ADR 0007): a request that names no
    translation — or names the primary — goes through the same code as
    before, so its body must equal the one the previous `build_response`
    call produced (no `passage` argument, no filter)."""
    final = make_final(highlight=(4, 4))
    legacy = scripture_select.build_response(
        final, "ru", 1, history_reset=False, psalm_maps=PSALM_MAPS
    )
    monkeypatch.setattr(
        scripture_select, "_run_selection", Mock(return_value=final)
    )

    for payload in (
        {"language": "ru", "topic": TOPIC},
        {"language": "ru", "topic": TOPIC, "translation": 1},
    ):
        response = post(payload)
        assert response.status_code == 200
        assert response.json() == json.loads(legacy.model_dump_json())


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
# Verse boundaries of the passage (ClickUp 86cbb1mq7, additive)
# ---------------------------------------------------------------------------
# The client places `highlight.passage` on the text by verse NUMBER; without
# the boundaries it would have to guess where a verse begins. The rule tested
# throughout: reassembling `passage.verses` with `chunking.build_text` gives
# `passage.text` back, byte for byte, on every path.

# A passage whose text really is the rendering of its verses: verse 3 carries
# a section title, which `build_text` breaks the paragraph at even though the
# verse's own `start_paragraph` is false (the `ubh` case of the live corpus).
CONSISTENT_VERSES = [
    VerseText(1, "Не заботьтесь ни о чем", start_paragraph=True),
    VerseText(2, "но всегда в молитве"),
    VerseText(3, "и мир Божий", title_break=True),
    VerseText(4, "соблюдет сердца ваши", start_paragraph=True),
]
CONSISTENT_TEXT = (
    "Не заботьтесь ни о чем но всегда в молитве\n\n"
    "и мир Божий\n\nсоблюдет сердца ваши"
)


def reassemble(verses: list[dict]) -> str:
    """`passage.verses` -> text, by exactly the rules that produced it."""
    from chunking import Verse, build_text

    return build_text(
        [
            Verse(
                verse_number=verse["number"],
                text=verse["text"],
                start_paragraph=verse["paragraph_start"],
            )
            for verse in verses
        ],
        set(),
    )


def consistent_candidate(translation: int = 1, alias: str = "syn"):
    candidate = make_candidate("v3:50.004.006-009")
    candidate.book_number, candidate.chapter_number = 50, 4
    candidate.verse_start, candidate.verse_end = 6, 9
    candidate.passages = [
        PassageText(
            translation=translation, alias=alias, book_number=50,
            chapter_number=4, verse_number_start=1, verse_number_end=4,
            title="Радуйтесь", text=CONSISTENT_TEXT,
            verses=list(CONSISTENT_VERSES),
        )
    ]
    return candidate


def test_the_passage_carries_its_verse_boundaries():
    body = post({"language": "ru", "topic": TOPIC}).json()

    assert [verse["number"] for verse in body["passage"]["verses"]] == [
        1, 2, 3, 4, 5, 6
    ]
    assert body["passage"]["verses"][0]["text"] == "стих 1"


def test_the_verses_reassemble_into_the_passage_text_on_the_primary_path(
    monkeypatch,
):
    """Contract: `build_text` over the returned verses IS `passage.text` —
    the two are the same database rows seen twice, never two renderings."""
    monkeypatch.setattr(
        scripture_select, "_run_selection",
        Mock(return_value=make_final(candidate=consistent_candidate())),
    )

    passage = post({"language": "ru", "topic": TOPIC}).json()["passage"]

    assert passage["text"] == CONSISTENT_TEXT
    assert reassemble(passage["verses"]) == passage["text"]
    # the paragraph flags are the two rules build_text uses, plus the opening
    # verse: own flag, section title, first verse
    assert [verse["paragraph_start"] for verse in passage["verses"]] == [
        True, False, True, True
    ]


def test_the_verses_reassemble_into_the_passage_text_on_the_render_path(
    monkeypatch,
):
    """ADR 0007: a translation with no chunk is served from `render_passage`,
    and its verses must come from that same rendering — the BTI reader gets
    BTI verses, not the reference translation's."""
    rendered = PassageText(
        translation=11, alias="bti", book_number=50, chapter_number=4,
        verse_number_start=1, verse_number_end=4, title="Радуйтесь",
        text=CONSISTENT_TEXT, verses=list(CONSISTENT_VERSES),
    )
    monkeypatch.setattr(
        scripture_select, "get_resources", lambda: catalogue_resources()
    )
    monkeypatch.setattr(
        scripture_select, "_render_target_passage", Mock(return_value=rendered)
    )

    passage = post(
        {"language": "ru", "topic": TOPIC, "translation": 11}
    ).json()["passage"]

    assert passage["translation_alias"] == "bti"
    assert reassemble(passage["verses"]) == passage["text"]


def test_the_safe_pool_passage_carries_its_verses_too(monkeypatch):
    """The no-AI path answers with a passage like any other, so it answers
    with its verse boundaries like any other."""
    body = select_with_highlight(
        monkeypatch, None, method="fallback_top1",
        fallback_reason="safe_pool", source="safe_pool",
        selection_reason="empty_topic",
        candidate=consistent_candidate(),
    ).json()

    assert body["source"] == "safe_pool"
    assert "highlight" not in body
    assert reassemble(body["passage"]["verses"]) == body["passage"]["text"]


def test_the_verse_numbers_are_the_ones_the_highlight_speaks(monkeypatch):
    """The whole point: `highlight.passage` is applied by matching `number`.

    The chunk starts at syn 22:3, so markers 2-3 are verses 4-5 — and those
    numbers must be findable in `verses`, not inferred from list positions.
    """
    candidate = make_candidate()
    candidate.passages[0].verses = psalm_verses(range(3, 7))
    candidate.passages[0].verse_number_start = 3

    body = select_with_highlight(
        monkeypatch, (2, 3), candidate=candidate
    ).json()

    numbers = [verse["number"] for verse in body["passage"]["verses"]]
    highlight = body["highlight"]["passage"]
    assert numbers == [3, 4, 5, 6]
    assert (highlight["verse_start"], highlight["verse_end"]) == (4, 5)
    assert {highlight["verse_start"], highlight["verse_end"]} <= set(numbers)


def test_a_counted_superscription_is_verse_one_of_the_verse_list(monkeypatch):
    """Psalter: `syn` counts the inscription as verse 1 while the canon does
    not, so the verse list and the highlight both stay in the translation's
    numbering — canonical 3:1-2 is `syn` 3:2-3."""
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

    assert [v["number"] for v in body["passage"]["verses"]] == [1, 2, 3, 4, 5]
    assert body["highlight"]["passage"]["verse_start"] == 2
    assert body["highlight"]["canonical"]["verse_start"] == 1


def test_a_ukrainian_psalm_numbers_its_verses_the_ubh_way(
    monkeypatch, selection_environment
):
    """`ubh` keeps the Masoretic chapter but counts the superscription as
    verse 1 — the verse list says so, and the highlight agrees with it."""
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

    assert body["passage"]["verses"][0]["number"] == 1
    assert body["highlight"]["passage"]["verse_start"] == 2
    assert body["highlight"]["passage"]["chapter_number"] == 3


def test_verses_are_omitted_entirely_when_the_breakdown_is_unavailable(
    monkeypatch,
):
    """Degradation (verse loader down, or a chunk of an indexed translation
    the candidates were not rendered in): an absent breakdown is an absent
    KEY — never null, never an empty list, and never a failed request."""
    candidate = make_candidate()
    for passage in candidate.passages:
        passage.verses = []
    monkeypatch.setattr(
        scripture_select, "_run_selection",
        Mock(return_value=make_final(candidate=candidate)),
    )

    response = post({"language": "ru", "topic": TOPIC})

    assert response.status_code == 200
    passage = response.json()["passage"]
    assert "verses" not in passage
    assert passage["text"] == PASSAGE_TEXT


def test_the_verse_list_is_additive_and_changes_nothing_else(monkeypatch):
    """Every previously published field of `passage` keeps its value; the
    only difference is the added key."""
    candidate = consistent_candidate()
    monkeypatch.setattr(
        scripture_select, "_run_selection",
        Mock(return_value=make_final(candidate=candidate)),
    )
    passage = post({"language": "ru", "topic": TOPIC}).json()["passage"]
    source = candidate.passages[0]

    assert set(passage) == {
        "translation", "translation_alias", "book_number", "chapter_number",
        "verse_start", "verse_end", "title", "text", "verses",
    }
    assert (
        passage["translation"], passage["translation_alias"],
        passage["book_number"], passage["chapter_number"],
        passage["verse_start"], passage["verse_end"],
        passage["title"], passage["text"],
    ) == (
        source.translation, source.alias, source.book_number,
        source.chapter_number, source.verse_number_start,
        source.verse_number_end, source.title, source.text,
    )


def test_the_verse_list_never_reaches_the_logs(monkeypatch, caplog):
    """Privacy is unchanged: the passage is in the body and nowhere else."""
    candidate = consistent_candidate()
    monkeypatch.setattr(
        scripture_select, "_run_selection",
        Mock(return_value=make_final(candidate=candidate)),
    )

    with caplog.at_level("DEBUG"):
        response = post({
            "language": "ru", "topic": TOPIC, "user_replies": [REPLY],
        })

    assert response.json()["passage"]["verses"]
    for secret in (*PRIVATE_STRINGS, "Не заботьтесь ни о чем"):
        assert secret not in caplog.text


def test_the_openapi_schema_publishes_the_verse_boundaries():
    schema = client.get("/openapi.json").json()
    passage = schema["components"]["schemas"]["PassageModel"]
    field = passage["properties"]["verses"]

    # optional and additive: absent, never null
    assert "verses" not in passage.get("required", [])
    assert "anyOf" not in field
    assert field["type"] == "array"
    assert field["items"] == {"$ref": "#/components/schemas/VerseModel"}
    assert "KEY IS ABSENT ENTIRELY" in field["description"]
    verse = schema["components"]["schemas"]["VerseModel"]
    assert set(verse["properties"]) == {"number", "text", "paragraph_start"}
    assert set(verse["required"]) == {"number", "text", "paragraph_start"}
    assert "highlight.passage" in verse["properties"]["number"]["description"]


# ---------------------------------------------------------------------------
# Renderable catalogue, primary translation and the coverage filter (ADR 0007)
# ---------------------------------------------------------------------------

INDEXED = {"ru": [(1, "syn")], "en": [(16, "bsb"), (17, "webus")]}


def catalogue_resources(**overrides):
    defaults = dict(
        index=SimpleNamespace(metas=[]),
        lexical={},
        translations={"ru": [(1, "syn"), (11, "bti")], "en": [(16, "bsb")]},
        loaded_at=0.0,
        psalm_maps=PSALM_MAPS,
        indexed={"ru": [(1, "syn")], "en": [(16, "bsb")]},
        primary={"ru": 1, "en": 16},
        coverage={11: frozenset({"v3:19.023.001-006"})},
    )
    defaults.update(overrides)
    return scripture_select.CorpusResources(**defaults)


def test_primary_config_is_parsed_into_language_pairs():
    assert scripture_select.parse_primary_config(" ru=syn , en=16 ") == {
        "ru": "syn", "en": "16",
    }
    assert scripture_select.parse_primary_config("") == {}


def test_a_malformed_primary_entry_is_ignored_not_fatal(caplog):
    with caplog.at_level("WARNING"):
        parsed = scripture_select.parse_primary_config("ru=syn,nonsense,=x")

    assert parsed == {"ru": "syn"}
    assert "malformed" in caplog.text


def test_the_default_primary_is_deterministic():
    """No configuration: the indexed translation with the lowest code —
    identical to the previous 'first in index order' while each language has
    a single indexed translation."""
    assert scripture_select.resolve_primary_translations(INDEXED, "") == {
        "ru": 1, "en": 16,
    }


@pytest.mark.parametrize("value", ["en=webus", "en=17"])
def test_the_primary_can_be_configured_by_alias_or_by_code(value):
    resolved = scripture_select.resolve_primary_translations(INDEXED, value)

    assert resolved["en"] == 17
    assert resolved["ru"] == 1, "unconfigured languages keep the default"


def test_a_primary_that_is_not_indexed_falls_back_to_the_default(caplog):
    """The primary IS the corpus: an unindexed translation cannot be one."""
    with caplog.at_level("WARNING"):
        resolved = scripture_select.resolve_primary_translations(
            INDEXED, "ru=bti,de=xyz"
        )

    assert resolved == {"ru": 1, "en": 16}
    assert "not indexed" in caplog.text
    assert "no index" in caplog.text


def test_the_coverage_filter_is_never_applied_to_the_primary():
    """Backward compatibility: the primary path is the pre-ADR-0007 path,
    with no filter in it at all."""
    resources = catalogue_resources()

    assert scripture_select.coverage_filter(resources, "ru", 1) is None
    assert scripture_select.coverage_filter(resources, "ru", 11) == frozenset(
        {"v3:19.023.001-006"}
    )


def test_a_selection_of_the_primary_passes_no_filter_to_the_pipeline(
    monkeypatch, selection_environment
):
    monkeypatch.setattr(
        scripture_select, "get_resources", lambda: catalogue_resources()
    )

    post({"language": "ru", "topic": TOPIC})
    assert selection_environment.call_args.args[3] is None

    post({"language": "ru", "topic": TOPIC, "translation": 1})
    assert selection_environment.call_args.args[3] is None


def test_a_selection_of_another_translation_is_restricted_to_its_coverage(
    monkeypatch, selection_environment
):
    monkeypatch.setattr(
        scripture_select, "get_resources", lambda: catalogue_resources()
    )

    post({"language": "ru", "topic": TOPIC, "translation": 11})

    assert selection_environment.call_args.args[3] == frozenset(
        {"v3:19.023.001-006"}
    )


def test_a_missing_coverage_set_refuses_every_window(caplog):
    """Fix F4: a non-primary translation whose coverage set is missing is
    fail-CLOSED. `None` would mean "no restriction" for exactly the
    translation whose renderability was never established."""
    resources = catalogue_resources(coverage={})

    with caplog.at_level("WARNING"):
        allowed = scripture_select.coverage_filter(resources, "ru", 11)

    assert allowed == frozenset()
    assert "No coverage set" in caplog.text
    # ...and the primary is still unfiltered
    assert scripture_select.coverage_filter(resources, "ru", 1) is None


def test_an_empty_coverage_result_is_reported_as_coverage_empty(monkeypatch):
    """Fix F1 through the public contract: the safe pool answered because
    nothing retrieved exists in the requested translation."""
    monkeypatch.setattr(
        scripture_select, "get_resources", lambda: catalogue_resources()
    )
    monkeypatch.setattr(
        scripture_select, "_run_selection",
        Mock(return_value=make_final(
            method="fallback_top1", fallback_reason="safe_pool",
            source="safe_pool", selection_reason="coverage_empty",
        )),
    )
    monkeypatch.setattr(
        scripture_select, "_render_target_passage",
        Mock(return_value=PassageText(
            translation=11, alias="bti", book_number=19, chapter_number=22,
            verse_number_start=1, verse_number_end=6, title=None,
            text="Господь — Пастырь мой", verses=psalm_verses(),
        )),
    )

    body = post({
        "language": "ru", "topic": TOPIC, "translation": 11
    }).json()

    assert body["source"] == "safe_pool"
    assert body["fallback_reason"] == "coverage_empty"


def test_an_empty_primary_ranking_is_reported_as_ranking_empty(monkeypatch):
    """The symmetric case through the public contract: no coverage filter is
    involved, so the category names the emptied ranking, not the coverage."""
    monkeypatch.setattr(
        scripture_select, "get_resources", lambda: catalogue_resources()
    )
    monkeypatch.setattr(
        scripture_select, "_run_selection",
        Mock(return_value=make_final(
            method="fallback_top1", fallback_reason="safe_pool",
            source="safe_pool", selection_reason="ranking_empty",
        )),
    )

    body = post({"language": "ru", "topic": TOPIC}).json()

    assert body["source"] == "safe_pool"
    assert body["fallback_reason"] == "ranking_empty"


def test_the_openapi_schema_publishes_the_empty_pool_fallbacks():
    schema = client.get("/openapi.json").json()
    reason = schema["components"]["schemas"]["FallbackReason"]

    assert "coverage_empty" in reason["enum"]
    assert "ranking_empty" in reason["enum"]


def test_a_primary_that_is_not_the_rerank_translation_is_reported(caplog):
    """Fix F7 / ADR 0007 OQ2: the rerank prompt is `passages[0]` — index
    insertion order. A configured primary that is a DIFFERENT indexed
    translation means the AI reads one book and the client is served
    another; it is accepted (it is indexed, so it needs no rendering) but
    never silently."""
    with caplog.at_level("WARNING"):
        resolved = scripture_select.resolve_primary_translations(
            INDEXED, "en=webus"
        )

    assert resolved["en"] == 17
    assert "rerank prompt" in caplog.text
    assert scripture_select.reference_translation(INDEXED, "en") == 16
    assert scripture_select.reference_translation(INDEXED, "de") is None


def test_rendering_is_refused_when_the_candidate_was_judged_elsewhere(caplog):
    """Fix F7: the own-range rendering is only equivalent to the judged text
    because the judged text is the reference translation's chunk. A candidate
    whose prompt passage is another translation is refused, not rendered."""
    with caplog.at_level("WARNING"):
        with pytest.raises(scripture_select.ScriptureSelectUnavailable):
            scripture_select._render_target_passage(
                make_candidate(), 11, "bti", PSALM_MAPS, reference=99,
            )

    assert "not the language's reference" in caplog.text
    assert PASSAGE_TEXT not in caplog.text


def test_rendering_is_refused_when_the_candidate_has_no_prompt_passage(caplog):
    """Fail-closed guard: `prompt_passage` returns None for a candidate with
    no passages at all. That must refuse exactly like a mismatched reference
    rather than fall through the `shown is not None` check and render the
    own range unverified. Unreachable today (every candidate carries at
    least the primary's passage), but the guard must not depend on that."""
    candidate = make_candidate()
    candidate.passages = []

    with caplog.at_level("WARNING"):
        with pytest.raises(scripture_select.ScriptureSelectUnavailable):
            scripture_select._render_target_passage(
                candidate, 11, "bti", PSALM_MAPS, reference=1,
            )

    assert "not the language's reference" in caplog.text
    assert PASSAGE_TEXT not in caplog.text


def test_rendering_is_skipped_when_the_time_budget_is_gone(monkeypatch):
    """Fix F6: no extra DB round trip for a request that is already over
    budget — the documented 503 instead."""
    connect = Mock()
    monkeypatch.setattr(scripture_select, "create_connection", connect)
    expired = scripture_select.Deadline(0.0)

    with pytest.raises(scripture_select.ScriptureSelectUnavailable):
        scripture_select._render_target_passage(
            make_candidate(), 11, "bti", PSALM_MAPS, reference=1,
            deadline=expired,
        )

    connect.assert_not_called()


def test_a_database_failure_while_rendering_is_a_503(monkeypatch, caplog):
    """Fix F6: a MySQL error in the rendering is the same documented 503 as
    every other database failure of this endpoint, not a bare 500."""
    monkeypatch.setattr(
        scripture_select, "get_resources", lambda: catalogue_resources()
    )
    monkeypatch.setattr(
        scripture_select, "render_passage",
        Mock(side_effect=RuntimeError("MySQL connection lost")),
    )
    monkeypatch.setattr(
        scripture_select, "create_connection",
        Mock(return_value=Mock()),
    )

    with caplog.at_level("WARNING"):
        response = post({
            "language": "ru", "topic": TOPIC, "translation": 11
        })

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Scripture selection temporarily unavailable"
    }
    assert "MySQL connection lost" not in caplog.text
    for secret in PRIVATE_STRINGS:
        assert secret not in caplog.text


def test_a_window_without_a_chunk_is_rendered_from_the_database(monkeypatch):
    """A renderable translation that was never chunked gets its passage from
    `translation_verses` for the same canonical window."""
    rendered = PassageText(
        translation=11, alias="bti", book_number=19, chapter_number=22,
        verse_number_start=1, verse_number_end=6, title="Псалом",
        text="Господь — Пастырь мой: ни в чем я нуждаться не буду.",
        verses=psalm_verses(),
    )
    renderer = Mock(return_value=rendered)
    monkeypatch.setattr(
        scripture_select, "get_resources", lambda: catalogue_resources()
    )
    monkeypatch.setattr(scripture_select, "_render_target_passage", renderer)

    body = post(
        {"language": "ru", "topic": TOPIC, "translation": 11}
    ).json()

    assert body["passage"]["translation"] == 11
    assert body["passage"]["translation_alias"] == "bti"
    assert body["passage"]["text"] == rendered.text
    assert body["canonical"]["canonical_id"] == "v3:19.023.001-006"
    # the canonical window is the same one the pipeline chose
    candidate = renderer.call_args.args[0]
    assert candidate.canonical_id == "v3:19.023.001-006"
    assert renderer.call_args.args[1:3] == (11, "bti")


def test_an_unrenderable_window_is_an_error_not_another_translation(
    monkeypatch
):
    """Grounding: the response carries the translation that was asked for or
    no response at all."""
    monkeypatch.setattr(
        scripture_select, "get_resources", lambda: catalogue_resources()
    )
    monkeypatch.setattr(
        scripture_select, "_render_target_passage", Mock(return_value=None)
    )

    response = post({"language": "ru", "topic": TOPIC, "translation": 11})

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Scripture selection temporarily unavailable"
    }
    assert PASSAGE_TEXT not in response.text


def test_an_indexed_translation_is_served_from_its_own_chunk(monkeypatch):
    """No re-rendering when the candidate already carries the translation:
    the chunk text stays the source of truth."""
    renderer = Mock()
    monkeypatch.setattr(scripture_select, "_render_target_passage", renderer)

    body = post({"language": "ru", "topic": TOPIC, "translation": 2}).json()

    assert body["passage"]["text"] == "Другой перевод"
    renderer.assert_not_called()


# ---------------------------------------------------------------------------
# Removed and renamed routes (ClickUp 86cbbmwjk, 2026-08-30)
# ---------------------------------------------------------------------------
# The renderable-translation catalogue was deleted outright: the translation
# is chosen once in the app and the selection serves any active translation
# of an indexed language (ADR 0007). The three AI routes moved under
# `/api/ai/*`. The client is a single unpublished app, so there are no
# aliases and no deprecation window — the old paths are simply gone.

@pytest.mark.parametrize("method,path", [
    ("GET", "/api/scripture/v1/translations"),
    ("POST", "/api/scripture/v1/select"),
    ("POST", "/api/twinkler/v1/complete"),
    ("POST", "/api/twinkler/v1/transcribe"),
])
def test_the_retired_paths_are_gone(method, path):
    response = client.request(
        method, path, headers={"X-API-Key": "test-api-key"}
    )

    assert response.status_code == 404


def test_the_translation_catalogue_is_absent_from_openapi():
    schema = app.openapi()

    assert "/api/scripture/v1/translations" not in schema["paths"]
    assert "ScriptureTranslationModel" not in schema["components"]["schemas"]


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
        scripture_select, "AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE", 1
    )

    first = post({"language": "ru", "topic": TOPIC})
    second = post({"language": "ru", "topic": TOPIC})

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.headers["Retry-After"] == "60"
    assert second.json()["detail"] == "Scripture selection request limit exceeded"


def test_rate_limits_globally(monkeypatch):
    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(scripture_select, "AI_SCRIPTURE_REQUESTS_PER_MINUTE", 1)

    post({"language": "ru", "topic": TOPIC})
    second = post({"language": "ru", "topic": TOPIC})

    assert second.status_code == 429


def test_rate_limit_budget_is_independent_from_twinkler(monkeypatch):
    import twinkler_ai

    monkeypatch.setattr(rate_limit.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(scripture_select, "AI_SCRIPTURE_REQUESTS_PER_MINUTE", 1)

    post({"language": "ru", "topic": TOPIC})

    assert len(twinkler_ai._request_times) == 0


def test_rate_limiter_fails_closed_without_the_hmac_key(monkeypatch):
    monkeypatch.setattr(client_ip, "AI_CLIENT_HMAC_KEY", "")

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

    monkeypatch.setattr(
        client_ip, "TRUSTED_PROXIES", TrustedProxies(addresses={"testclient"})
    )
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
    assert logged[:3] == ("/api/ai/scripture", "POST", 200)
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
            scripture_select, "AI_SCRIPTURE_REQUESTS_PER_CLIENT_PER_MINUTE", 1
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
    monkeypatch.setattr(scripture_select, "AI_SCRIPTURE_INDEX_CACHE_SECONDS", 0)
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
    assert "/api/ai/scripture" in middleware.PRIVATE_PATHS
    assert "/api/ai/question" in middleware.PRIVATE_PATHS


# ---------------------------------------------------------------------------
# OpenAPI
# ---------------------------------------------------------------------------

def test_openapi_documents_the_public_contract():
    schema = app.openapi()
    operation = schema["paths"]["/api/ai/scripture"]["post"]

    assert operation["tags"] == ["AI"]
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

    for path in ("/api/ai/question", "/api/ai/transcribe"):
        assert path in schema["paths"]
    complete = schema["paths"]["/api/ai/question"]["post"]
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


_LIVE_RESOURCES = None


@pytest.fixture
def live_resources(monkeypatch):
    """The real corpus (vector index, BM25, Psalm maps, catalogue).

    Loaded once for the whole module — it costs ~1.4 s and is exactly the
    prayer-independent data the production process caches.
    """
    global _LIVE_RESOURCES
    if _LIVE_RESOURCES is None:
        _LIVE_RESOURCES = scripture_select._load_resources()
    monkeypatch.setattr(
        scripture_select, "get_resources", lambda: _LIVE_RESOURCES
    )
    return _LIVE_RESOURCES


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
def test_the_live_catalogue_covers_every_active_translation(live_resources):
    """Every active translation of an indexed language is servable today.

    Read from the cached corpus directly: the catalogue endpoint that used to
    publish it was removed with ClickUp 86cbbmwjk, the invariant was not.
    """
    catalogue = {
        language: [
            (
                code,
                alias,
                code == scripture_select.primary_translation(
                    live_resources, language
                ),
            )
            for code, alias in entries
        ]
        for language, entries in live_resources.translations.items()
    }

    assert catalogue["ru"] == [(1, "syn", True), (11, "bti", False)]
    assert catalogue["en"] == [
        (16, "bsb", True), (17, "webus", False), (779, "webbe", False),
    ]
    assert catalogue["uk"] == [(20, "ubh", True), (21, "npu", False)]


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
def test_the_live_coverage_sets_are_the_documented_ones(live_resources):
    """The numbers ADR 0007 publishes, after the reference-chunk filter of
    fix F2 (which drops 17 en and 38 uk windows from what any non-indexed
    translation may be offered; ru has none).

    bti's figure was updated from 3830 to 3899 after the 2026-08-30 BTI
    backfill (ClickUp 86cbb1reb) completed its remaining canonical chapters;
    see the matching note in ADR 0007."""
    sizes = {
        code: len(covered)
        for code, covered in live_resources.coverage.items()
    }

    assert sizes == {11: 3899, 17: 3995, 779: 3995, 21: 1163}
    # the primary of a language is never given a coverage set at all
    assert set(sizes) & {1, 16, 20} == set()


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
@pytest.mark.parametrize(
    ("language", "translation", "alias"),
    [("ru", 11, "bti"), ("en", 17, "webus"), ("en", 779, "webbe"),
     ("uk", 21, "npu")],
)
def test_a_translation_without_an_index_is_served_from_its_own_verses(
    monkeypatch, live_resources, language, translation, alias
):
    """Acceptance of the whole feature, on live data and without Gemini: the
    empty-topic (safe pool) path in a translation that was never chunked."""
    from database import create_connection

    monkeypatch.setattr(scripture_select, "_run_selection", REAL_RUN_SELECTION)

    response = post({
        "language": language, "topic": "", "translation": translation,
    })

    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "safe_pool"
    passage = body["passage"]
    assert (passage["translation"], passage["translation_alias"]) == (
        translation, alias
    )
    assert passage["text"].strip()
    assert "highlight" not in body

    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT verse_number FROM translation_verses
            WHERE translation = %s AND book_number = %s AND chapter_number = %s
              AND verse_number BETWEEN %s AND %s AND TRIM(text) <> ''
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

    assert passage["verse_start"] in verses and passage["verse_end"] in verses


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
def test_a_narrowed_translation_still_has_a_safe_pool(live_resources):
    """npu has neither the Old Testament outside the Psalms nor Lamentations,
    so its pool is smaller — but not empty, which would make it unservable.

    Pool 1.1.0 (Мария, 2026-08-28): the three added places are Psalms and New
    Testament, i.e. inside npu, so it resolves 8 of the 9 — only Lamentations
    3:22-23 is out of reach for it.
    """
    from retrieval import (
        ScriptureRetriever, load_safe_pool, make_db_passage_loader,
    )
    from database import create_connection

    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        for language, code, expected in (
            ("ru", 1, 9), ("ru", 11, 9), ("en", 16, 9), ("en", 17, 9),
            ("en", 779, 9), ("uk", 21, 8), ("uk", 20, 9),
        ):
            retriever = ScriptureRetriever(
                index=live_resources.index, embedder=None, rewriter=None,
                load_passages=make_db_passage_loader(cursor),
                allowed_canonical_ids=scripture_select.coverage_filter(
                    live_resources, language, code
                ),
            )
            resolved = retriever._resolve_pool_ids(language)
            assert sum(1 for cid in resolved if cid) == expected, (
                language, code
            )
        assert len(load_safe_pool()) == 9
    finally:
        cursor.close()
        connection.close()


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
def test_a_psalm_highlight_is_carried_into_another_translation(
    monkeypatch, live_resources
):
    """The cross-translation branch of `resolve_highlight` on live data.

    Canonical Psalm 116:1-8 is chapter 114 in both `syn` and `bti`, but syn
    merges canonical 116:8 and 116:9 into its verse 8 (versification
    EXCEPTIONS). A highlight of syn marker 7 is canonical 116:7 and comes
    back as bti 114:7; marker 8 expands to canonical 116:8-9, which reaches
    past the window bti renders — and is then DROPPED rather than clamped.
    """
    from database import create_connection
    from retrieval import make_db_verse_loader

    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        loader = make_db_verse_loader(cursor)
        canonical_id = "v3:19.116.001-008"
        verses = loader(1, [(canonical_id, 19, 114, 1, 8)])[canonical_id]
    finally:
        cursor.close()
        connection.close()
    assert len(verses) == 8

    candidate = Candidate(
        canonical_id=canonical_id, book_number=19, chapter_number=116,
        verse_start=1, verse_end=8, score=0.7, best_variant=0,
        variant_scores={0: 0.7},
        passages=[
            PassageText(
                translation=1, alias="syn", book_number=19,
                chapter_number=114, verse_number_start=1, verse_number_end=8,
                title=None, text="", verses=verses,
            )
        ],
    )

    def select_with(indices):
        monkeypatch.setattr(
            scripture_select, "_run_selection",
            Mock(return_value=make_final(candidate=candidate, highlight=indices)),
        )
        return post({
            "language": "ru", "topic": TOPIC, "translation": 11,
        }).json()

    body = select_with((7, 7))
    assert body["passage"]["translation_alias"] == "bti"
    assert body["passage"]["chapter_number"] == 114
    assert body["passage"]["text"].startswith("Любовью к Господу")
    assert body["highlight"] == {
        "canonical": {
            "book_number": 19, "chapter_number": 116,
            "verse_start": 7, "verse_end": 7,
        },
        "passage": {"chapter_number": 114, "verse_start": 7, "verse_end": 7},
    }
    # the bti verse list is bti's own, and the highlight is findable in it
    numbers = [verse["number"] for verse in body["passage"]["verses"]]
    assert body["highlight"]["passage"]["verse_start"] in numbers
    assert reassemble(body["passage"]["verses"]) == body["passage"]["text"]

    # the merged verse maps onto a canonical range the bti window ends before
    assert "highlight" not in select_with((8, 8))


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
def test_a_highlight_on_a_verse_the_served_translation_merges_is_dropped(
    monkeypatch, live_resources
):
    """F1, outside the Psalter, where the coordinate conversion is the
    identity and only the served passage's own verse list can object.

    `bti` carries Genesis 35:9 and 35:10 in one verse and numbers it 9, so
    its verses of this window run 9, 11, 12… A `syn` highlight of 35:10 is
    inside the bti range (9-15) and yet not a number bti has: the response
    would name a verse the client cannot find among `passage.verses`. It is
    dropped instead — the neighbouring verse, which bti does number, still
    resolves.
    """
    from database import create_connection
    from retrieval import make_db_verse_loader

    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        loader = make_db_verse_loader(cursor)
        canonical_id = "v3:01.035.009-015"
        verses = loader(1, [(canonical_id, 1, 35, 8, 15)])[canonical_id]
    finally:
        cursor.close()
        connection.close()
    assert [v.verse_number for v in verses] == [8, 9, 10, 11, 12, 13, 14, 15]

    candidate = Candidate(
        canonical_id=canonical_id, book_number=1, chapter_number=35,
        verse_start=9, verse_end=15, score=0.7, best_variant=0,
        variant_scores={0: 0.7},
        passages=[
            PassageText(
                translation=1, alias="syn", book_number=1,
                chapter_number=35, verse_number_start=8, verse_number_end=15,
                title=None, text="", verses=verses,
            )
        ],
    )

    def select_with(indices):
        monkeypatch.setattr(
            scripture_select, "_run_selection",
            Mock(return_value=make_final(candidate=candidate, highlight=indices)),
        )
        return post({
            "language": "ru", "topic": TOPIC, "translation": 11,
        }).json()

    body = select_with((4, 4))                 # marker 4 = syn 35:11
    numbers = [verse["number"] for verse in body["passage"]["verses"]]
    assert body["passage"]["translation_alias"] == "bti"
    assert 10 not in numbers and numbers[:2] == [9, 11]
    assert body["highlight"]["passage"] == {
        "chapter_number": 35, "verse_start": 11, "verse_end": 11,
    }

    # marker 3 = syn 35:10, a number bti does not use at all
    assert "highlight" not in select_with((3, 3))


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
@pytest.mark.parametrize(
    ("language", "translation"),
    [("ru", None), ("ru", 11), ("en", None), ("en", 17), ("en", 779),
     ("uk", None), ("uk", 21)],
)
def test_the_live_verses_reassemble_into_the_served_passage(
    monkeypatch, live_resources, language, translation
):
    """Acceptance on live data, without Gemini (empty topic = safe pool), for
    the chunk path (`translation` omitted -> the primary) and the render path
    (ADR 0007) of every language."""
    monkeypatch.setattr(scripture_select, "_run_selection", REAL_RUN_SELECTION)
    payload = {"language": language, "topic": ""}
    if translation is not None:
        payload["translation"] = translation

    body = post(payload).json()

    passage = body["passage"]
    assert body["source"] == "safe_pool"
    verses = passage["verses"]
    assert verses and reassemble(verses) == passage["text"]
    numbers = [verse["number"] for verse in verses]
    assert numbers == sorted(numbers)
    assert numbers[0] == passage["verse_start"]
    assert numbers[-1] == passage["verse_end"]
    assert verses[0]["paragraph_start"] is True


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
@pytest.mark.parametrize("code", [1, 16, 20])
def test_every_chunk_reassembles_from_its_verses(code):
    """The chunk path against `translation_chunks` itself — the WHOLE corpus
    of every indexed translation, not a sample: this is the test that catches
    a re-import of the texts without a re-chunking, where the stored chunk
    text and the verses a client would rebuild it from drift apart.

    The counted subset is the chunks whose stored text has a paragraph break
    that only a section title explains (`ubh` inherits the pivot's
    boundaries, so 278 of its chunks carry a title inside them where the
    verse's own `start_paragraph` is 0). Without `VerseText.title_break`
    those are exactly the chunks a client could not rebuild.
    """
    from database import create_connection
    from retrieval import make_db_verse_loader

    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT DISTINCT tc.canonical_id, tc.book_number, tc.chapter_number,
                   tc.verse_number_start, tc.verse_number_end, tc.text
            FROM translation_chunks tc
            JOIN translation_verses tv
              ON tv.translation = tc.translation
             AND tv.book_number = tc.book_number
             AND tv.chapter_number = tc.chapter_number
             AND tv.start_paragraph = 0
             AND tv.verse_number > tc.verse_number_start
             AND tv.verse_number <= tc.verse_number_end
            JOIN translation_titles tt
              ON tt.before_translation_verse = tv.code AND tt.subtitle = 0
            WHERE tc.translation = %s AND tc.chunking_version = %s
            """,
            (code, CHUNKING_VERSION),
        )
        titled = cursor.fetchall()
        cursor.execute(
            """
            SELECT canonical_id, book_number, chapter_number,
                   verse_number_start, verse_number_end, text
            FROM translation_chunks
            WHERE translation = %s AND chunking_version = %s
            ORDER BY canonical_id
            """,
            (code, CHUNKING_VERSION),
        )
        rows = {row["canonical_id"]: row for row in cursor.fetchall()}
        loader = make_db_verse_loader(cursor)
        sample = list(rows.values())
        loaded: dict = {}
        for start in range(0, len(sample), 100):
            loaded.update(loader(code, [
                (row["canonical_id"], row["book_number"],
                 row["chapter_number"], row["verse_number_start"],
                 row["verse_number_end"])
                for row in sample[start:start + 100]
            ]))
    finally:
        cursor.close()
        connection.close()

    assert len(titled) == (278 if code == 20 else 0)
    assert {row["canonical_id"] for row in titled} <= set(rows)
    assert len(rows) > 2000, code
    for canonical_id, row in rows.items():
        verses = scripture_select.build_passage_verses(
            SimpleNamespace(verses=loaded[canonical_id])
        )
        assert verses is not None, canonical_id
        assert reassemble(
            [verse.model_dump() for verse in verses]
        ) == row["text"], canonical_id


@pytest.mark.skipif(
    not _database_available(), reason="needs the cep_public database"
)
def test_a_septuagint_shift_is_visible_in_the_response(
    monkeypatch, live_resources
):
    """Canonical Psalm 23 is chapter 22 in bti and 23 in webus — the same
    words under different numbers, both read from the database."""
    monkeypatch.setattr(scripture_select, "_run_selection", REAL_RUN_SELECTION)

    russian = post({"language": "ru", "topic": "", "translation": 11}).json()
    english = post({"language": "en", "topic": "", "translation": 17}).json()

    assert russian["canonical"]["canonical_id"] == "v3:19.023.001-006"
    assert russian["canonical"]["chapter_number"] == 23
    assert russian["passage"]["chapter_number"] == 22
    assert "Пастырь" in russian["passage"]["text"]
    assert english["canonical"]["chapter_number"] == 23
    assert english["passage"]["chapter_number"] == 23
    assert "shepherd" in english["passage"]["text"]
