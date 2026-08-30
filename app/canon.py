"""
Chapter structure of the 66-book canon — the explicit source of "how many
chapters a book is expected to have" for the public API.

Why a table in code (ClickUp 86cbb2xxp)
---------------------------------------
`GET /api/translations/{code}/books` used to derive the expected chapter count
from ``SELECT max(chapter_number) FROM translation_verses WHERE book_number =
tb.book_number`` — a subquery that names no translation, so the maximum came
from *any* translation stored in `cep_public`. The consequences were reported
as data holes:

- `bti` was told it lacked 26 chapters while only 20 were really absent; the
  extra six were deuterocanonical chapters of other translations
  (`syn`: 2 Chr 37, Ps 151, Dan 13-14; `ubh`: Esth 11-12);
- `syn` itself was told Esther 11-12 were missing (they exist in `ubh` only),
  and `ubh` was told 2 Chr 37 and Ps 151 were missing (they exist in `syn`).

`cep_public` has no per-book chapter count: `bible_books` stores only
`verse_count`, and `translation_verses` stores what a translation happens to
contain — which is exactly the mixed-canon source that caused the bug. Adding
a column would mean changing the admin import contract, so the expected
structure lives here instead: a literal, reviewable table, versioned with the
code, in the spirit of `versification.py`'s explicit tables (never a silent
guess derived from whatever a neighbouring translation holds).

The numbers below are the standard Protestant 66-book canon (1189 chapters).
They were verified against the three structurally complete translations in
`cep_public` — `bsb` (16), `webus` (17), `webbe` (779): all three match this
table book for book, with no deviation.

What a translation is allowed to add
------------------------------------
A translation may legitimately contain *more* than the canon: `syn` carries
2 Chr 37, Ps 151 and Dan 13-14, `ubh` carries Esth 11-12 and Dan 13-14. Those
chapters stay visible — the expected count of a book is
``max(canonical, what this very translation contains)`` — but they are never
expected *from another translation*.

A translation may also divide the same text into fewer chapters. That is a
versification difference, not a hole, so it needs an explicit entry in
``TRANSLATION_CHAPTER_COUNTS`` below; without one the missing chapter would be
reported as missing text.
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

logger = logging.getLogger(__name__)

# (book_number, bible_books.code1, chapters in the 66-book canon).
# The code is carried as data so the table can be cross-checked against
# `bible_books` — the book numbering is the project's own (note the order of
# the Epistles: the Catholic ones, 45-51, precede the Pauline ones, 52-64).
CANONICAL_BOOKS: tuple[tuple[int, str, int], ...] = (
    (1, "gen", 50),
    (2, "exo", 40),
    (3, "lev", 27),
    (4, "num", 36),
    (5, "deu", 34),
    (6, "jos", 24),
    (7, "jdg", 21),
    (8, "rut", 4),
    (9, "1sa", 31),
    (10, "2sa", 24),
    (11, "1ki", 22),
    (12, "2ki", 25),
    (13, "1ch", 29),
    (14, "2ch", 36),
    (15, "ezr", 10),
    (16, "neh", 13),
    (17, "est", 10),
    (18, "job", 42),
    (19, "psa", 150),
    (20, "pro", 31),
    (21, "ecc", 12),
    (22, "sng", 8),
    (23, "isa", 66),
    (24, "jer", 52),
    (25, "lam", 5),
    (26, "ezk", 48),
    (27, "dan", 12),
    (28, "hos", 14),
    (29, "jol", 3),
    (30, "amo", 9),
    (31, "oba", 1),
    (32, "jon", 4),
    (33, "mic", 7),
    (34, "nam", 3),
    (35, "hab", 3),
    (36, "zep", 3),
    (37, "hag", 2),
    (38, "zec", 14),
    (39, "mal", 4),
    (40, "mat", 28),
    (41, "mrk", 16),
    (42, "luk", 24),
    (43, "jhn", 21),
    (44, "act", 28),
    (45, "jas", 5),
    (46, "1pe", 5),
    (47, "2pe", 3),
    (48, "1jn", 5),
    (49, "2jn", 1),
    (50, "3jn", 1),
    (51, "jud", 1),
    (52, "rom", 16),
    (53, "1co", 16),
    (54, "2co", 13),
    (55, "gal", 6),
    (56, "eph", 6),
    (57, "php", 4),
    (58, "col", 4),
    (59, "1th", 5),
    (60, "2th", 3),
    (61, "1ti", 6),
    (62, "2ti", 4),
    (63, "tit", 3),
    (64, "phm", 1),
    (65, "heb", 13),
    (66, "rev", 22),
)

CANONICAL_CHAPTER_COUNTS: dict[int, int] = {
    number: chapters for number, _code, chapters in CANONICAL_BOOKS
}
CANONICAL_BOOK_CODES: dict[int, str] = {
    number: code for number, code, _chapters in CANONICAL_BOOKS
}
CANONICAL_CHAPTERS_TOTAL = 1189

# Translations that divide a book into fewer chapters than the canon does.
# (translation alias, book_number) -> chapters this translation is expected to
# have. Only genuine versification differences belong here — a real data hole
# must stay visible.
#
# ubh (Ukrainian, Hebrew chapter division) closes Malachi at chapter 3: its
# Mal 3 has 24 verses, i.e. canonical 3:1-18 plus 4:1-6. Without this entry
# the endpoint would report Malachi 4 as missing text.
TRANSLATION_CHAPTER_COUNTS: dict[tuple[str, int], int] = {
    ("ubh", 39): 3,
}


def _validate_table() -> None:
    """Fail loudly at import time rather than shipping a mistyped canon."""
    numbers = [number for number, _code, _chapters in CANONICAL_BOOKS]
    if numbers != list(range(1, 67)):
        raise ValueError("CANONICAL_BOOKS must list book numbers 1..66 in order")
    total = sum(CANONICAL_CHAPTER_COUNTS.values())
    if total != CANONICAL_CHAPTERS_TOTAL:
        raise ValueError(
            f"the 66-book canon has {CANONICAL_CHAPTERS_TOTAL} chapters, "
            f"CANONICAL_BOOKS sums to {total}"
        )
    for (alias, book_number), chapters in TRANSLATION_CHAPTER_COUNTS.items():
        if book_number not in CANONICAL_CHAPTER_COUNTS:
            raise ValueError(
                f"TRANSLATION_CHAPTER_COUNTS names unknown book {book_number} "
                f"for translation {alias!r}"
            )
        if not isinstance(alias, str) or not alias or alias != alias.lower():
            raise ValueError(
                f"TRANSLATION_CHAPTER_COUNTS alias {alias!r} for book "
                f"{book_number} must be a non-empty lowercase string"
            )
        canonical_max = CANONICAL_CHAPTER_COUNTS[book_number]
        if not (1 <= chapters <= canonical_max):
            raise ValueError(
                f"TRANSLATION_CHAPTER_COUNTS[({alias!r}, {book_number})] = "
                f"{chapters} must be between 1 and the canonical "
                f"{canonical_max} (this table documents a translation that "
                "divides a book into FEWER chapters than the canon; a real "
                "data hole must stay visible instead of being hidden here)"
            )


_validate_table()


def expected_chapters(
    book_number: int, translation_alias: Optional[str] = None
) -> Optional[int]:
    """
    Chapters a book is expected to have in this translation, or None when the
    book is outside the canon table (an unknown book number — see
    ``chapter_coverage``, which then falls back to the translation's own text
    instead of inventing a structure).
    """
    override = TRANSLATION_CHAPTER_COUNTS.get((translation_alias, book_number))
    if override is not None:
        return override
    canonical = CANONICAL_CHAPTER_COUNTS.get(book_number)
    if canonical is None:
        logger.warning(
            "book_number %s is outside the 66-book canon table; falling "
            "back to %s's own text to determine its chapter structure",
            book_number,
            translation_alias or "the translation",
        )
    return canonical


def chapter_coverage(
    book_number: int,
    present_chapters: Iterable[int],
    translation_alias: Optional[str] = None,
) -> tuple[int, list[int]]:
    """
    Chapter coverage of one book in one translation.

    Returns ``(chapters_count, chapters_without_text)`` where

    - ``chapters_count`` is the expected structure of the book: the canonical
      count, widened to the translation's own last chapter when the
      translation legitimately carries more (deuterocanonical additions);
    - ``chapters_without_text`` lists every chapter of that range the
      translation has no verse for — including all of them when the
      translation ships no text for the book at all.

    Only ``present_chapters`` of *this* translation are ever taken into
    account, which is what keeps the canons of different translations apart.

    An unknown book number (outside the 66-book table) has no canonical
    structure to compare against, so the translation's own text defines it:
    the count is its last chapter and nothing before it is claimed missing
    except real gaps.
    """
    present = {int(chapter) for chapter in present_chapters}
    canonical = expected_chapters(book_number, translation_alias)
    highest_present = max(present) if present else 0
    chapters_count = max(canonical or 0, highest_present)
    if chapters_count <= 0:
        return 0, []
    chapters_without_text = sorted(set(range(1, chapters_count + 1)) - present)
    return chapters_count, chapters_without_text
