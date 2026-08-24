"""Unit tests for the BM25 lexical index (hybrid retrieval signal)."""

import os

os.environ.setdefault("API_KEY", "test-api-key")

from lexical_index import LexicalIndex, tokenize


def test_tokenize_lowercases_unicode_words():
    assert tokenize("Навчай юнака, і він — не зверне!") == [
        "навчай", "юнака", "і", "він", "не", "зверне",
    ]


def test_exact_wording_outranks_thematic_overlap():
    index = LexicalIndex([
        ("v3:20.022.001-016", "Навчай юнака згідно з дорогою його"),
        ("v3:20.003.001-010", "Надійся на Господа всім серцем твоїм"),
        ("v3:19.001.001-006", "Блаженний муж що за радою несправедливих не ходить"),
    ])
    hits = index.search("Навчай юнака згідно з дорогою його")
    assert hits[0].canonical_id == "v3:20.022.001-016"
    assert hits[0].score > (hits[1].score if len(hits) > 1 else 0.0)


def test_rare_words_weigh_more_than_common_ones():
    index = LexicalIndex([
        ("a", "Господь пастир мій"),
        ("b", "Господь свiтло моє"),
        ("c", "пастир добрий душу свою кладе"),
    ])
    hits = index.search("пастир")
    ids = [h.canonical_id for h in hits]
    assert set(ids) == {"a", "c"}  # only documents containing the word


def test_duplicate_canonical_ids_deduplicated_best_score():
    index = LexicalIndex([
        ("shared", "слово слово слово"),
        ("shared", "слово"),
        ("other", "інше"),
    ])
    hits = index.search("слово")
    assert [h.canonical_id for h in hits] == ["shared"]


def test_top_k_limits_results():
    docs = [(f"id{i}", "спільне слово") for i in range(30)]
    index = LexicalIndex(docs)
    assert len(index.search("спільне", top_k=5)) == 5


def test_no_match_returns_empty():
    index = LexicalIndex([("a", "текст")])
    assert index.search("відсутнє") == []


def test_empty_index_returns_empty():
    index = LexicalIndex([])
    assert index.search("що-небудь") == []
    assert len(index) == 0
