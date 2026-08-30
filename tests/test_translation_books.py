"""
Chapter structure of a translation: `GET /api/translations/{code}/books`,
`app/canon.py` and the navigation counts of `app/excerpt.py`
(ClickUp 86cbb2xxp).

The tests run against a SQLite stand-in for `cep_public` rather than the live
database: the fixture holds several translations in one `translation_verses`
table — exactly the situation that produced the bug — and the production SQL
is executed verbatim, so an unscoped `max(chapter_number)` would fail here the
same way it failed in production.

Fixture canons (a miniature of the real data):

- `syn`   — the whole canon plus the deuterocanonical 2 Chr 37, Ps 151,
            Dan 13-14;
- `ubh`   — the whole canon plus Esth 11-12 and Dan 13-14, with Malachi
            closed at chapter 3 (Hebrew chapter division);
- `bti`   — the whole canon minus the 20 chapters really absent from the live
            BTI (Num 35-36, Deut 32-34, 1 Sam 25-31, 2 Kgs 25, 1 Chr 23-29);
- `npu`   — all 66 books declared, text for the Psalms and Matthew only.
"""

import re
import sqlite3
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

import canon
import excerpt
import main
import middleware
from canon import (
    CANONICAL_BOOK_CODES,
    CANONICAL_BOOKS,
    CANONICAL_CHAPTER_COUNTS,
    CANONICAL_CHAPTERS_TOTAL,
    chapter_coverage,
    expected_chapters,
)
from main import app

client = TestClient(app)
HEADERS = {"X-API-Key": "test-api-key"}

PSALMS = 19
CHRONICLES_2 = 14
ESTHER = 17
DANIEL = 27
MALACHI = 39
GENESIS = 1
MATTHEW = 40

# The chapters the live BTI has no text for (ClickUp 86cbb1reb).
BTI_MISSING = {
    4: [35, 36],
    5: [32, 33, 34],
    9: [25, 26, 27, 28, 29, 30, 31],
    12: [25],
    13: [23, 24, 25, 26, 27, 28, 29],
}

# Chapters a translation carries beyond the 66-book canon.
SYN_EXTRA = {CHRONICLES_2: [37], PSALMS: [151], DANIEL: [13, 14]}
UBH_EXTRA = {ESTHER: [11, 12], DANIEL: [13, 14]}

NPU_BOOKS_WITH_TEXT = {PSALMS, MATTHEW}


# --------------------------------------------------------------------------
# The canon table itself
# --------------------------------------------------------------------------


def test_the_canon_table_covers_66_books_and_1189_chapters():
    assert len(CANONICAL_BOOKS) == 66
    assert sum(CANONICAL_CHAPTER_COUNTS.values()) == CANONICAL_CHAPTERS_TOTAL == 1189


@pytest.mark.parametrize(
    "book_number, code, chapters",
    [
        (GENESIS, "gen", 50),
        (CHRONICLES_2, "2ch", 36),
        (ESTHER, "est", 10),
        (PSALMS, "psa", 150),
        (DANIEL, "dan", 12),
        (MALACHI, "mal", 4),
        (31, "oba", 1),
        (66, "rev", 22),
    ],
)
def test_the_canon_table_states_the_known_counts(book_number, code, chapters):
    """Spot check against the canon itself, independent of the table's own sum."""
    number, table_code, table_chapters = CANONICAL_BOOKS[book_number - 1]
    assert (number, table_code, table_chapters) == (book_number, code, chapters)


def test_a_translation_specific_count_overrides_the_canon():
    # ubh Mal 3 holds canonical 3:1-18 + 4:1-6; chapter 4 is not a hole.
    assert expected_chapters(MALACHI) == 4
    assert expected_chapters(MALACHI, "ubh") == 3
    assert expected_chapters(MALACHI, "syn") == 4


def test_coverage_reports_the_canonical_gaps_only():
    present = set(range(1, 151)) - {42}
    assert chapter_coverage(PSALMS, present, "bti") == (150, [42])


def test_coverage_keeps_a_deuterocanonical_chapter_of_this_translation():
    count, missing = chapter_coverage(PSALMS, set(range(1, 152)), "syn")
    assert (count, missing) == (151, [])


def test_coverage_never_expects_another_translation_s_extra_chapter():
    count, missing = chapter_coverage(PSALMS, set(range(1, 151)), "bti")
    assert count == 150
    assert 151 not in missing


def test_coverage_of_a_book_without_any_text():
    count, missing = chapter_coverage(GENESIS, set(), "npu")
    assert count == 50
    assert missing == list(range(1, 51))


def test_coverage_of_a_book_outside_the_canon_follows_its_own_text():
    """No canonical structure to compare against: only real gaps are missing."""
    count, missing = chapter_coverage(200, {1, 2, 4}, "syn")
    assert (count, missing) == (4, [3])
    assert chapter_coverage(200, set(), "syn") == (0, [])


def test_an_unknown_book_number_logs_a_warning_before_falling_back(caplog):
    """MINOR 3: the fallback to a translation's own text is a notable event
    (an unrecognised book_number), not silent behaviour."""
    with caplog.at_level("WARNING", logger="canon"):
        assert expected_chapters(200, "syn") is None
    assert "200" in caplog.text
    assert "outside the 66-book canon" in caplog.text


# --------------------------------------------------------------------------
# Self-validation of the canon table (MINOR 2)
# --------------------------------------------------------------------------


def test_validator_accepts_the_real_table():
    """Sanity check: the production table itself must still pass."""
    canon._validate_table()


def test_validator_rejects_a_chapter_count_above_the_canonical_maximum(monkeypatch):
    """TRANSLATION_CHAPTER_COUNTS documents a translation with FEWER chapters
    than the canon; a value above the canonical maximum is a mistyped table,
    not a real exception, and must fail as loudly as the rest of the
    self-check."""
    monkeypatch.setattr(canon, "TRANSLATION_CHAPTER_COUNTS", {("ubh", MALACHI): 999})
    with pytest.raises(ValueError, match="must be between 1 and the canonical"):
        canon._validate_table()
    # monkeypatch restores the real table automatically once this test ends


def test_validator_rejects_a_non_positive_chapter_count(monkeypatch):
    monkeypatch.setattr(canon, "TRANSLATION_CHAPTER_COUNTS", {("ubh", MALACHI): 0})
    with pytest.raises(ValueError, match="must be between 1 and the canonical"):
        canon._validate_table()


def test_validator_rejects_a_non_lowercase_alias(monkeypatch):
    monkeypatch.setattr(canon, "TRANSLATION_CHAPTER_COUNTS", {("UBH", MALACHI): 3})
    with pytest.raises(ValueError, match="lowercase"):
        canon._validate_table()


def test_validator_rejects_an_empty_alias(monkeypatch):
    monkeypatch.setattr(canon, "TRANSLATION_CHAPTER_COUNTS", {("", MALACHI): 3})
    with pytest.raises(ValueError, match="lowercase"):
        canon._validate_table()


def test_validator_rejects_an_unknown_book_number(monkeypatch):
    monkeypatch.setattr(canon, "TRANSLATION_CHAPTER_COUNTS", {("ubh", 200): 3})
    with pytest.raises(ValueError, match="unknown book"):
        canon._validate_table()


# --------------------------------------------------------------------------
# Live cross-check: the canon table against the real bible_books (MINOR 1)
# --------------------------------------------------------------------------


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


@pytest.mark.skipif(not _database_available(), reason="needs the cep_public database")
def test_the_canon_table_matches_bible_books_in_the_live_database():
    """The module docstring (and the comment above CANONICAL_BOOKS) promise a
    cross-check of (book_number, code1) against the live `bible_books` table;
    this is that check. All 66 canonical books must match by number and code."""
    from database import create_connection

    connection = create_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute("SELECT number, code1 FROM bible_books ORDER BY number")
        rows = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()

    live = {row["number"]: row["code1"] for row in rows}
    assert live == CANONICAL_BOOK_CODES


# --------------------------------------------------------------------------
# A SQLite stand-in for cep_public
# --------------------------------------------------------------------------


class _Cursor:
    """MySQL-connector-shaped cursor over SQLite (pyformat -> qmark/named)."""

    def __init__(self, connection: sqlite3.Connection):
        self._cursor = connection.cursor()

    def execute(self, sql: str, params=None):
        if isinstance(params, dict):
            # %(name)s -> :name. A plain .replace("%(", ":").replace(")s", "")
            # also mangles a literal ")s" that happens to follow unrelated text
            # elsewhere in the SQL; the regex only touches genuine pyformat
            # placeholders (MINOR 5, ClickUp 86cbb2xxp).
            sql = re.sub(r"%\((\w+)\)s", r":\1", sql)
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql.replace("%s", "?"), tuple(params or ()))

    def fetchall(self):
        return [dict(row) for row in self._cursor.fetchall()]

    def fetchone(self):
        row = self._cursor.fetchone()
        return dict(row) if row is not None else None

    def close(self):
        self._cursor.close()


class _Connection:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def cursor(self, dictionary: bool = False):
        return _Cursor(self._connection)

    def close(self):
        """The fixture database outlives the request, so this is a no-op."""


def _canonical_chapters(book_number: int) -> list[int]:
    return list(range(1, CANONICAL_CHAPTER_COUNTS[book_number] + 1))


def _build_database() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE translations (code INTEGER PRIMARY KEY, alias TEXT,
                                   name TEXT, language TEXT, active INTEGER);
        CREATE TABLE bible_books (number INTEGER PRIMARY KEY, code1 TEXT,
                                  code2 TEXT, code3 TEXT, code4 TEXT,
                                  code5 TEXT, code6 TEXT, code7 TEXT,
                                  code8 TEXT, code9 TEXT,
                                  short_name_en TEXT, short_name_ru TEXT);
        CREATE TABLE translation_books (code INTEGER PRIMARY KEY,
                                        translation INTEGER,
                                        book_number INTEGER, name TEXT);
        CREATE TABLE translation_verses (code INTEGER PRIMARY KEY,
                                         translation INTEGER,
                                         book_number INTEGER,
                                         chapter_number INTEGER,
                                         verse_number INTEGER, text TEXT);
        """
    )
    connection.executemany(
        "INSERT INTO translations VALUES (?, ?, ?, ?, 1)",
        [
            (1, "syn", "SYNO", "ru"),
            (11, "bti", "BTI", "ru"),
            (20, "ubh", "UBH", "uk"),
            (21, "npu", "NPU", "uk"),
        ],
    )
    connection.executemany(
        "INSERT INTO bible_books VALUES (?, ?, '', '', '', '', '', '', '', '', ?, ?)",
        [(number, code, code, code) for number, code, _chapters in CANONICAL_BOOKS],
    )

    book_rows, verse_rows = [], []
    book_code, verse_code = 0, 0
    for translation in (1, 11, 20, 21):
        for book_number, code, _chapters in CANONICAL_BOOKS:
            book_code += 1
            book_rows.append((book_code, translation, book_number, code))

            if translation == 21 and book_number not in NPU_BOOKS_WITH_TEXT:
                continue  # declared by the publisher, no text shipped

            chapters = _canonical_chapters(book_number)
            if translation == 1:
                chapters += SYN_EXTRA.get(book_number, [])
            elif translation == 20:
                chapters += UBH_EXTRA.get(book_number, [])
                if book_number == MALACHI:
                    chapters = [1, 2, 3]  # Hebrew chapter division
            elif translation == 11:
                missing = set(BTI_MISSING.get(book_number, []))
                chapters = [c for c in chapters if c not in missing]

            for chapter in chapters:
                verse_code += 1
                verse_rows.append(
                    (verse_code, translation, book_number, chapter, 1, "verse")
                )

    connection.executemany(
        "INSERT INTO translation_books VALUES (?, ?, ?, ?)", book_rows
    )
    connection.executemany(
        "INSERT INTO translation_verses VALUES (?, ?, ?, ?, ?, ?)", verse_rows
    )
    connection.commit()
    return connection


@pytest.fixture(autouse=True)
def no_request_stats(monkeypatch):
    """Keep the request-stats writer away from the real cep_public."""
    monkeypatch.setattr(middleware, "_insert_request_log", Mock())


@pytest.fixture
def fake_database(monkeypatch):
    connection = _build_database()
    factory = lambda: _Connection(connection)
    monkeypatch.setattr(main, "create_connection", factory)
    monkeypatch.setattr(excerpt, "create_connection", factory)
    # get_chapters_by_book is memoised for an hour across requests.
    main._cache.clear()
    main._cache_timestamps.clear()
    yield connection
    main._cache.clear()
    main._cache_timestamps.clear()
    connection.close()


def _books(translation_code: int) -> dict[int, dict]:
    response = client.get(
        f"/api/translations/{translation_code}/books", headers=HEADERS
    )
    assert response.status_code == 200, response.text
    return {book["book_number"]: book for book in response.json()}


# --------------------------------------------------------------------------
# GET /api/translations/{code}/books
# --------------------------------------------------------------------------


def test_bti_reports_exactly_its_own_missing_chapters(fake_database):
    books = _books(11)

    reported = {
        number: book["chapters_without_text"]
        for number, book in books.items()
        if book["chapters_without_text"]
    }
    assert reported == BTI_MISSING
    assert sum(len(chapters) for chapters in reported.values()) == 20


@pytest.mark.parametrize(
    "book_number, chapters_count, foreign_chapter",
    [
        (CHRONICLES_2, 36, 37),  # syn's 2 Chr 37
        (PSALMS, 150, 151),  # syn's Ps 151
        (DANIEL, 12, 13),  # syn's / ubh's Dan 13-14
        (ESTHER, 10, 11),  # ubh's Esth 11-12
    ],
)
def test_bti_never_inherits_a_deuterocanonical_chapter(
    fake_database, book_number, chapters_count, foreign_chapter
):
    book = _books(11)[book_number]
    assert book["chapters_count"] == chapters_count
    assert book["chapters_without_text"] == []
    assert foreign_chapter not in book["chapters_without_text"]


def test_a_translation_keeps_its_own_deuterocanonical_chapters(fake_database):
    books = _books(1)

    assert books[PSALMS]["chapters_count"] == 151
    assert books[CHRONICLES_2]["chapters_count"] == 37
    assert books[DANIEL]["chapters_count"] == 14
    assert all(not book["chapters_without_text"] for book in books.values())


def test_a_translation_is_not_told_to_miss_another_canon_s_chapters(fake_database):
    """syn was reported to lack Esther 11-12, which exist in ubh only."""
    books = _books(1)

    assert books[ESTHER]["chapters_count"] == 10
    assert books[ESTHER]["chapters_without_text"] == []


def test_a_hebrew_chapter_division_is_not_a_hole(fake_database):
    books = _books(20)

    assert books[MALACHI]["chapters_count"] == 3
    assert books[MALACHI]["chapters_without_text"] == []
    assert books[ESTHER]["chapters_count"] == 12
    assert all(not book["chapters_without_text"] for book in books.values())


def test_books_without_text_are_returned_and_counted_as_fully_missing(fake_database):
    books = _books(21)

    assert len(books) == 66
    genesis = books[GENESIS]
    assert genesis["has_text"] is False
    assert genesis["chapters_count"] == 50
    assert genesis["chapters_without_text"] == list(range(1, 51))

    without_text = {number for number, b in books.items() if not b["has_text"]}
    assert without_text == set(CANONICAL_CHAPTER_COUNTS) - NPU_BOOKS_WITH_TEXT

    psalms = books[PSALMS]
    assert psalms["has_text"] is True
    assert psalms["chapters_without_text"] == []

    total_missing = sum(len(b["chapters_without_text"]) for b in books.values())
    assert total_missing == CANONICAL_CHAPTERS_TOTAL - 150 - 28


def test_has_text_is_true_for_a_complete_translation(fake_database):
    assert all(book["has_text"] for book in _books(1).values())


def test_the_openapi_schema_documents_the_chapter_coverage_fields():
    schema = app.openapi()["components"]["schemas"]["TranslationBookModel"]

    assert schema["properties"]["has_text"]["type"] == "boolean"
    for field in ("chapters_count", "chapters_without_text", "has_text"):
        assert schema["properties"][field]["description"]


# --------------------------------------------------------------------------
# Excerpt navigation (app/excerpt.py)
# --------------------------------------------------------------------------


def _books_info(connection: sqlite3.Connection, translation: int, alias=None):
    cursor = _Connection(connection).cursor(dictionary=True)
    try:
        return excerpt.get_books_info(cursor, translation, alias)
    finally:
        cursor.close()


def test_navigation_counts_stay_inside_the_translation(fake_database):
    bti = _books_info(fake_database, 11, "psa")[0]
    syn = _books_info(fake_database, 1, "psa")[0]

    assert bti["chapters_count"] == 150  # not syn's 151
    assert syn["chapters_count"] == 151


def test_navigation_does_not_offer_a_foreign_next_chapter(fake_database):
    cursor = _Connection(fake_database).cursor(dictionary=True)
    try:
        bti = excerpt.get_books_info(cursor, 11, "psa")[0]
        syn = excerpt.get_books_info(cursor, 1, "psa")[0]

        assert excerpt.get_next_excerpt(cursor, 11, bti, 150) == "pro 1"
        assert excerpt.get_next_excerpt(cursor, 1, syn, 150) == "psa 151"
    finally:
        cursor.close()


def test_navigation_counts_a_book_without_text_canonically(fake_database):
    genesis = _books_info(fake_database, 21, "gen")[0]

    assert genesis["chapters_count"] == 50
