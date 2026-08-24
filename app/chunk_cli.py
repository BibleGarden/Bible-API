"""
CLI for structural chunking of Bible translations.

Computes a canonical boundary plan from a pivot translation and materializes
chunks for the requested translations into the translation_chunks table of
cep_public (see architect/adr/0001-structural-chunking.md).

Usage (inside the bible-api container):

    python app/chunk_cli.py --translations syn,bsb [--pivot syn]
        [--target-chars 1200] [--min-chars 400] [--max-chars 2400]
        [--overlap-units 1] [--dry-run]

The run is idempotent: existing chunks of the same translation and
CHUNKING_VERSION are deleted and re-inserted in one transaction.
"""

from __future__ import annotations

import argparse
import statistics
import sys

from dataclasses import replace

from chunking import (
    CHUNKING_VERSION,
    ChapterKey,
    Chunk,
    ChunkingConfig,
    Verse,
    build_plan,
    chunk_translation,
)
from database import create_connection
from versification import (
    CANONICAL_ALIAS,
    CANONICAL_SPLITS,
    PSALMS_BOOK,
    PsalmMap,
    build_psalm_map,
    canonical_counts_with_extras,
    canonicalize_psalm_chapters,
)

BATCH_SIZE = 1000

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS translation_chunks (
    code INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    canonical_id VARCHAR(40) NOT NULL,
    chunking_version SMALLINT UNSIGNED NOT NULL,
    translation INT NOT NULL,
    book_number SMALLINT NOT NULL,
    chapter_number SMALLINT NOT NULL,
    verse_number_start SMALLINT NOT NULL,
    verse_number_end SMALLINT NOT NULL,
    verse_count SMALLINT NOT NULL,
    title VARCHAR(1000) DEFAULT NULL,
    text MEDIUMTEXT NOT NULL,
    char_count INT UNSIGNED NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_chunks_translation_canonical (translation, canonical_id),
    INDEX idx_chunks_trans_book_chapter (translation, book_number, chapter_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def resolve_translation(cursor, alias_or_code: str) -> dict:
    """Resolve a translation by alias or numeric code."""
    query = "SELECT code, alias, language FROM translations WHERE alias = %s"
    params: tuple = (alias_or_code,)
    if alias_or_code.isdigit():
        query = "SELECT code, alias, language FROM translations WHERE code = %s"
        params = (int(alias_or_code),)
    cursor.execute(query, params)
    row = cursor.fetchone()
    if not row:
        raise SystemExit(f"Translation '{alias_or_code}' not found")
    return row


def load_translation_chapters(
    cursor, translation_code: int
) -> dict[ChapterKey, tuple[list[Verse], dict[int, str]]]:
    """Load verses and section titles of a translation grouped by chapter."""
    cursor.execute(
        """
        SELECT book_number, chapter_number, verse_number, text, start_paragraph
        FROM translation_verses
        WHERE translation = %s
        ORDER BY book_number, chapter_number, verse_number
        """,
        (translation_code,),
    )
    chapters: dict[ChapterKey, tuple[list[Verse], dict[int, str]]] = {}
    for row in cursor.fetchall():
        key = (row["book_number"], row["chapter_number"])
        verses, _titles = chapters.setdefault(key, ([], {}))
        verses.append(
            Verse(
                verse_number=row["verse_number"],
                text=row["text"],
                start_paragraph=bool(row["start_paragraph"]),
            )
        )

    # Section titles (subtitles like psalm inscriptions are not boundaries).
    # Ordered by code so that the most specific title before a verse wins.
    cursor.execute(
        """
        SELECT tv.book_number, tv.chapter_number, tv.verse_number, tt.text
        FROM translation_titles tt
        JOIN translation_verses tv ON tt.before_translation_verse = tv.code
        WHERE tv.translation = %s AND tt.subtitle = 0
        ORDER BY tt.code
        """,
        (translation_code,),
    )
    for row in cursor.fetchall():
        key = (row["book_number"], row["chapter_number"])
        if key in chapters:
            chapters[key][1][row["verse_number"]] = row["text"]
    return chapters


def load_psalm_max_verses(cursor, translation_code: int) -> dict[int, int]:
    """Translation's Psalm chapter -> its maximal verse number."""
    cursor.execute(
        """
        SELECT chapter_number, MAX(verse_number) AS max_verse
        FROM translation_verses
        WHERE translation = %s AND book_number = %s
        GROUP BY chapter_number
        """,
        (translation_code, PSALMS_BOOK),
    )
    return {row["chapter_number"]: row["max_verse"] for row in cursor.fetchall()}


def load_psalm_map(cursor, alias: str, translation_code: int,
                   canonical_counts: dict[int, int]) -> PsalmMap:
    """Versification map of one translation (see app/versification.py)."""
    max_verses = load_psalm_max_verses(cursor, translation_code)
    return PsalmMap(build_psalm_map(alias, max_verses, canonical_counts))


def canonicalize_book(
    chapters: dict[ChapterKey, tuple[list[Verse], dict[int, str]]],
    psalm_map: PsalmMap,
) -> tuple[
    dict[ChapterKey, tuple[list[Verse], dict[int, str]]],
    dict[tuple[int, int], tuple[int, int]],
]:
    """Replace the Psalm chapters of a translation with their canonical-space
    form (see versification.canonicalize_psalm_chapters); other books are
    passed through. Returns the converted chapters and the back-map
    (canonical chapter, canonical verse) -> (translation chapter, verse)."""
    verses_19 = {
        chapter: verses
        for (book, chapter), (verses, _titles) in chapters.items()
        if book == PSALMS_BOOK
    }
    titles_19 = {
        chapter: titles
        for (book, chapter), (_verses, titles) in chapters.items()
        if book == PSALMS_BOOK
    }
    c_verses, c_titles, back = canonicalize_psalm_chapters(
        verses_19, titles_19, psalm_map
    )
    result = {
        key: value for key, value in chapters.items() if key[0] != PSALMS_BOOK
    }
    for c_chapter, verses in c_verses.items():
        result[(PSALMS_BOOK, c_chapter)] = (verses, c_titles[c_chapter])
    return result, back


def restore_psalm_coordinates(
    chunks: list[Chunk],
    back: dict[tuple[int, int], tuple[int, int]],
) -> list[Chunk]:
    """Convert Psalm chunk display coordinates (chapter_number and
    verse_number_start/end) from canonical space back to the translation's
    own numbering. Canonical IDs stay canonical. The forced section
    boundaries at CANONICAL_SPLITS guarantee a chunk never spans two
    translation chapters; violated only on data drift, hence the hard stop."""
    result = []
    for chunk in chunks:
        if chunk.book_number != PSALMS_BOOK:
            result.append(chunk)
            continue
        start_chapter, start_verse = back[
            (chunk.chapter_number, chunk.verse_number_start)
        ]
        end_chapter, end_verse = back[
            (chunk.chapter_number, chunk.verse_number_end)
        ]
        if start_chapter != end_chapter:
            raise SystemExit(
                f"chunk {chunk.canonical_id} spans translation chapters "
                f"{start_chapter} and {end_chapter} — versification data drift"
            )
        result.append(
            replace(
                chunk,
                chapter_number=start_chapter,
                verse_number_start=start_verse,
                verse_number_end=end_verse,
            )
        )
    return result


def build_canonical_plan(cursor, pivot: dict, config: ChunkingConfig,
                         canonical_counts: dict[int, int]):
    """Canonical boundary plan from the pivot, with the Psalter converted to
    canonical coordinate space and hard section boundaries injected at the
    Septuagint split points of canonical 116 and 147."""
    chapters = load_translation_chapters(cursor, pivot["code"])
    psalm_map = load_psalm_map(
        cursor, pivot["alias"], pivot["code"], canonical_counts
    )
    canonical_chapters, _back = canonicalize_book(chapters, psalm_map)
    plan_input = {}
    for key, (verses, titles) in canonical_chapters.items():
        title_verses = set(titles)
        if key[0] == PSALMS_BOOK and key[1] in CANONICAL_SPLITS:
            title_verses.add(CANONICAL_SPLITS[key[1]])
        plan_input[key] = (verses, title_verses)
    return build_plan(plan_input, config)


def compute_translation_chunks(cursor, translation: dict, plan,
                               config: ChunkingConfig,
                               canonical_counts: dict[int, int]) -> list[Chunk]:
    """v3 chunk set of one translation: Psalms chunked in canonical space,
    display coordinates restored to the translation's own numbering."""
    chapters = load_translation_chapters(cursor, translation["code"])
    psalm_map = load_psalm_map(
        cursor, translation["alias"], translation["code"], canonical_counts
    )
    canonical_chapters, back = canonicalize_book(chapters, psalm_map)
    chunks = chunk_translation(plan, canonical_chapters, config)
    return restore_psalm_coordinates(chunks, back)


def load_canonical_counts(cursor) -> dict[int, int]:
    """Canonical Psalm verse counts from the canonical translation."""
    canonical = resolve_translation(cursor, CANONICAL_ALIAS)
    counts = load_psalm_max_verses(cursor, canonical["code"])
    if not counts:
        raise SystemExit(
            f"Canonical translation '{CANONICAL_ALIAS}' has no Psalms — "
            f"cannot build the versification mapping"
        )
    return canonical_counts_with_extras(counts)


def store_chunks(connection, cursor, translation_code: int, chunks: list[Chunk]):
    """Idempotently replace ALL chunks of a translation.

    translation_chunks holds only the current chunk set (one version at a
    time); embeddings of disappeared/stale chunks are cleaned up by
    versification_cli rechunk (text-preserving) or index_cli rebuild.
    """
    cursor.execute(CREATE_TABLE_SQL)
    cursor.execute(
        "DELETE FROM translation_chunks WHERE translation = %s",
        (translation_code,),
    )
    sql = """
        INSERT INTO translation_chunks
            (canonical_id, chunking_version, translation, book_number,
             chapter_number, verse_number_start, verse_number_end,
             verse_count, title, text, char_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            chunk.canonical_id, CHUNKING_VERSION, translation_code,
            chunk.book_number, chunk.chapter_number,
            chunk.verse_number_start, chunk.verse_number_end,
            chunk.verse_count, chunk.title, chunk.text, len(chunk.text),
        )
        for chunk in chunks
    ]
    for start in range(0, len(rows), BATCH_SIZE):
        cursor.executemany(sql, rows[start:start + BATCH_SIZE])
    connection.commit()


def print_stats(label: str, chunks: list[Chunk]):
    sizes = sorted(len(chunk.text) for chunk in chunks)
    if not sizes:
        print(f"{label}: no chunks")
        return
    with_title = sum(1 for chunk in chunks if chunk.title)
    verse_total = sum(chunk.verse_count for chunk in chunks)
    print(f"{label}:")
    print(f"  chunks:            {len(chunks)}")
    print(f"  verses in chunks:  {verse_total} (incl. overlap duplicates)")
    print(
        "  size chars:        "
        f"min={sizes[0]} p50={int(statistics.median(sizes))} "
        f"avg={int(statistics.mean(sizes))} "
        f"p90={sizes[int(len(sizes) * 0.9) - 1]} max={sizes[-1]}"
    )
    buckets = [(0, 400), (400, 800), (800, 1200), (1200, 1600), (1600, 2400), (2400, 10 ** 9)]
    parts = []
    for low, high in buckets:
        count = sum(1 for s in sizes if low <= s < high)
        top = f"{high}" if high < 10 ** 9 else "+"
        parts.append(f"[{low}-{top}): {count}")
    print(f"  size distribution: {'  '.join(parts)}")
    print(f"  with title:        {with_title} ({100.0 * with_title / len(chunks):.1f}%)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Structural chunking of Bible translations")
    parser.add_argument(
        "--translations", required=True,
        help="Comma-separated translation aliases or codes, e.g. syn,bsb",
    )
    parser.add_argument(
        "--pivot", default="syn",
        help="Translation whose structure defines canonical boundaries (default: syn)",
    )
    parser.add_argument("--target-chars", type=int, default=ChunkingConfig.target_chars)
    parser.add_argument("--min-chars", type=int, default=ChunkingConfig.min_chars)
    parser.add_argument("--max-chars", type=int, default=ChunkingConfig.max_chars)
    parser.add_argument("--overlap-units", type=int, default=ChunkingConfig.overlap_units)
    parser.add_argument("--overlap-max-chars", type=int, default=ChunkingConfig.overlap_max_chars)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute chunks and print statistics without writing to the database",
    )
    args = parser.parse_args(argv)

    config = ChunkingConfig(
        target_chars=args.target_chars,
        min_chars=args.min_chars,
        max_chars=args.max_chars,
        overlap_units=args.overlap_units,
        overlap_max_chars=args.overlap_max_chars,
    )

    connection = create_connection()
    if connection is None:
        print("Cannot connect to the database", file=sys.stderr)
        return 1
    cursor = connection.cursor(dictionary=True)
    try:
        pivot = resolve_translation(cursor, args.pivot)
        print(f"Building canonical plan from pivot '{pivot['alias']}' "
              f"(code {pivot['code']}), algorithm v{CHUNKING_VERSION}")
        canonical_counts = load_canonical_counts(cursor)
        plan = build_canonical_plan(cursor, pivot, config, canonical_counts)
        planned_count = sum(len(v) for v in plan.values())
        print(f"Plan: {len(plan)} chapters, {planned_count} canonical chunks")

        for raw in args.translations.split(","):
            raw = raw.strip()
            if not raw:
                continue
            translation = resolve_translation(cursor, raw)
            chunks = compute_translation_chunks(
                cursor, translation, plan, config, canonical_counts
            )
            print_stats(
                f"{translation['alias']} ({translation['language']}, "
                f"code {translation['code']})",
                chunks,
            )
            if args.dry_run:
                print("  dry-run: not stored")
            else:
                store_chunks(connection, cursor, translation["code"], chunks)
                print(f"  stored: {len(chunks)} rows in translation_chunks")
        return 0
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
