"""
CLI for the Psalm versification mapping and the canonical-space chunk
migration.

See app/versification.py and architect/adr/0003-psalm-versification-canon.md.

Usage (inside the bible-api container):

    # (re)build the mapping table for all translations (idempotent)
    python app/versification_cli.py build [--translations syn,ubh] [--dry-run]

    # verify the stored mapping against translation_verses
    python app/versification_cli.py verify

    # re-chunk every chunked translation with the canonical-space v3 plan,
    # carry embeddings of unchanged texts over, drop stale ones
    python app/versification_cli.py rechunk [--pivot syn] [--dry-run]

`build` is idempotent: rows of the same translation and VERSIFICATION_VERSION
are deleted and re-inserted in one transaction (same pattern as
app/chunk_cli.py). `rechunk` is idempotent too: it replaces the whole chunk
set of each chunked translation with the current CHUNKING_VERSION output,
renames the embedding rows whose chunk text (title + text, the embedded
content) is unchanged, and deletes embeddings whose chunks disappeared —
re-running on migrated data changes nothing. Embeddings for new/changed
chunks are NOT created here: run `python app/index_cli.py rebuild` after.
Known limitation: the carried/to-embed counters are derived from chunk
texts, not cross-checked against the actual chunk_embeddings rows — any
gap (e.g. an embedding missing before the migration) simply stays a gap
until the rebuild catches it up.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict

from chunk_cli import (
    build_canonical_plan,
    compute_translation_chunks,
    load_canonical_counts,
    load_psalm_max_verses,
    resolve_translation,
)
from chunking import CHUNKING_VERSION, ChunkingConfig
from database import create_connection
from vector_index import build_embedding_text
from versification import (
    PSALMS_BOOK,
    TRANSLATION_SCHEMES,
    VERSIFICATION_VERSION,
    VerseMapping,
    build_psalm_map,
)


BATCH_SIZE = 1000

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS psalm_verse_mappings (
    code INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    mapping_version SMALLINT UNSIGNED NOT NULL,
    translation INT NOT NULL,
    book_number SMALLINT NOT NULL,
    chapter_number SMALLINT NOT NULL,
    verse_number SMALLINT NOT NULL,
    canonical_chapter SMALLINT NOT NULL,
    canonical_verse_start SMALLINT NOT NULL,
    canonical_verse_end SMALLINT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_psalm_map_source
        (translation, mapping_version, book_number, chapter_number, verse_number),
    INDEX idx_psalm_map_canonical
        (translation, mapping_version, book_number, canonical_chapter,
         canonical_verse_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


def _require_known_schemes(rows: list[dict]) -> list[dict]:
    unknown = [r["alias"] for r in rows if r["alias"] not in TRANSLATION_SCHEMES]
    if unknown:
        raise SystemExit(
            f"Translations without a Psalm numbering scheme: {unknown}; "
            f"add them to versification.TRANSLATION_SCHEMES"
        )
    return rows


def mapped_translations(cursor, spec: str | None) -> list[dict]:
    """Translations to map: --translations or all with a known scheme."""
    if spec:
        return _require_known_schemes([
            resolve_translation(cursor, raw.strip())
            for raw in spec.split(",") if raw.strip()
        ])
    cursor.execute("SELECT code, alias, language FROM translations ORDER BY code")
    return _require_known_schemes(cursor.fetchall())


def load_existing_verses(cursor, translation_code: int) -> set[tuple[int, int]]:
    """(chapter, verse) pairs actually present in translation_verses."""
    cursor.execute(
        """
        SELECT chapter_number, verse_number FROM translation_verses
        WHERE translation = %s AND book_number = %s
        """,
        (translation_code, PSALMS_BOOK),
    )
    return {(row["chapter_number"], row["verse_number"]) for row in cursor.fetchall()}


def build_translation_mappings(
    cursor, translation: dict, canonical_counts: dict[int, int]
) -> tuple[list[VerseMapping], set[tuple[int, int]]]:
    """Mappings restricted to verses that exist in the database.

    The pure builder covers every verse number 1..max; a few translations
    skip verse numbers (e.g. bti 67:14 — the text is merged into a
    neighbouring verse), and those numbers get no row.
    """
    max_verses = load_psalm_max_verses(cursor, translation["code"])
    mappings = build_psalm_map(translation["alias"], max_verses, canonical_counts)
    existing = load_existing_verses(cursor, translation["code"])
    kept = [m for m in mappings if (m.chapter, m.verse) in existing]
    return kept, existing


def store_mappings(connection, cursor, translation_code: int,
                   mappings: list[VerseMapping]) -> None:
    """Idempotently replace one translation's rows for VERSIFICATION_VERSION."""
    cursor.execute(CREATE_TABLE_SQL)
    cursor.execute(
        "DELETE FROM psalm_verse_mappings "
        "WHERE translation = %s AND mapping_version = %s",
        (translation_code, VERSIFICATION_VERSION),
    )
    sql = """
        INSERT INTO psalm_verse_mappings
            (mapping_version, translation, book_number, chapter_number,
             verse_number, canonical_chapter, canonical_verse_start,
             canonical_verse_end)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            VERSIFICATION_VERSION, translation_code, PSALMS_BOOK,
            m.chapter, m.verse, m.canonical_chapter,
            m.canonical_verse_start, m.canonical_verse_end,
        )
        for m in mappings
    ]
    for start in range(0, len(rows), BATCH_SIZE):
        cursor.executemany(sql, rows[start:start + BATCH_SIZE])
    connection.commit()


def cmd_build(connection, cursor, args) -> int:
    canonical_counts = load_canonical_counts(cursor)
    for translation in mapped_translations(cursor, args.translations):
        mappings, existing = build_translation_mappings(
            cursor, translation, canonical_counts
        )
        skipped = len(existing) - len(mappings)
        superscriptions = sum(1 for m in mappings if m.canonical_verse_start == 0)
        merges = sum(
            1 for m in mappings
            if m.canonical_verse_end > m.canonical_verse_start
        )
        print(
            f"{translation['alias']} (code {translation['code']}): "
            f"{len(mappings)} verse mappings, "
            f"{superscriptions} superscription verses, {merges} merged"
            + (f", {skipped} numbering gaps" if skipped else "")
        )
        if args.dry_run:
            print("  dry-run: not stored")
        else:
            store_mappings(connection, cursor, translation["code"], mappings)
            print("  stored")
    return 0


def cmd_verify(connection, cursor, args) -> int:
    """Check the stored mapping against the database state."""
    cursor.execute("SHOW TABLES LIKE 'psalm_verse_mappings'")
    if not cursor.fetchall():
        print("psalm_verse_mappings does not exist yet — run build first")
        return 1
    canonical_counts = load_canonical_counts(cursor)
    failures = 0
    for translation in mapped_translations(cursor, None):
        code = translation["code"]
        expected, existing = build_translation_mappings(
            cursor, translation, canonical_counts
        )
        cursor.execute(
            """
            SELECT chapter_number, verse_number, canonical_chapter,
                   canonical_verse_start, canonical_verse_end
            FROM psalm_verse_mappings
            WHERE translation = %s AND mapping_version = %s AND book_number = %s
            """,
            (code, VERSIFICATION_VERSION, PSALMS_BOOK),
        )
        stored = {
            (r["chapter_number"], r["verse_number"]):
                (r["canonical_chapter"], r["canonical_verse_start"],
                 r["canonical_verse_end"])
            for r in cursor.fetchall()
        }
        rebuilt = {
            (m.chapter, m.verse):
                (m.canonical_chapter, m.canonical_verse_start,
                 m.canonical_verse_end)
            for m in expected
        }
        problems = []
        if set(stored) != set(existing):
            problems.append(
                f"coverage: {len(stored)} rows vs {len(existing)} verses "
                f"(missing {sorted(existing - set(stored))[:5]}, "
                f"extra {sorted(set(stored) - existing)[:5]})"
            )
        drift = {k for k in set(stored) & set(rebuilt) if stored[k] != rebuilt[k]}
        if drift:
            problems.append(f"drift vs rules for {sorted(drift)[:5]} ...")
        # canonical verses (>= 1) must be covered at most once
        covered: dict[tuple[int, int], int] = defaultdict(int)
        for (c_ch, c_start, c_end) in stored.values():
            for c_verse in range(max(c_start, 1), c_end + 1):
                covered[(c_ch, c_verse)] += 1
        doubles = [k for k, n in covered.items() if n > 1]
        if doubles:
            problems.append(f"canonical verses covered twice: {doubles[:5]}")
        holes = [
            (chapter, verse)
            for chapter, count in canonical_counts.items()
            for verse in range(1, count + 1)
            if (chapter, verse) not in covered
            and not (chapter == 151 and translation["alias"] != "syn")
        ]
        status = "OK " if not problems else "FAIL "
        extra = f", canonical holes (real numbering gaps): {holes}" if holes else ""
        print(f"{status}{translation['alias']:<6} rows={len(stored)}{extra}")
        for problem in problems:
            print(f"    {problem}")
        failures += bool(problems)
    return 1 if failures else 0


# ---------------------------------------------------------------------------
# Canonical-space re-chunking (v3) with embedding carry-over
# ---------------------------------------------------------------------------

def chunked_translations(cursor) -> list[dict]:
    cursor.execute(
        """
        SELECT DISTINCT t.code, t.alias
        FROM translation_chunks c JOIN translations t ON t.code = c.translation
        ORDER BY t.code
        """
    )
    return cursor.fetchall()


def plan_embedding_carry(
    old_rows: list[dict], new_chunks
) -> tuple[dict[str, str], list[str], list[str]]:
    """Pair old and new chunks by their embedding text (title + text).

    Returns (carry old_id -> new_id, new ids needing embedding, stale old
    ids whose embeddings must be deleted). Duplicate texts are paired
    deterministically by sorted canonical_id (multiset semantics).
    """
    old_by_text: dict[str, list[str]] = defaultdict(list)
    for row in old_rows:
        old_by_text[build_embedding_text(row["title"], row["text"])].append(
            row["canonical_id"]
        )
    for ids in old_by_text.values():
        ids.sort()
    carry: dict[str, str] = {}
    to_embed: list[str] = []
    for chunk in sorted(new_chunks, key=lambda c: c.canonical_id):
        bucket = old_by_text.get(build_embedding_text(chunk.title, chunk.text))
        if bucket:
            carry[bucket.pop(0)] = chunk.canonical_id
        else:
            to_embed.append(chunk.canonical_id)
    stale = sorted(
        old_id for ids in old_by_text.values() for old_id in ids
    )
    return carry, to_embed, stale


def cmd_rechunk(connection, cursor, args) -> int:
    config = ChunkingConfig(
        target_chars=args.target_chars,
        min_chars=args.min_chars,
        max_chars=args.max_chars,
        overlap_units=args.overlap_units,
        overlap_max_chars=args.overlap_max_chars,
    )
    canonical_counts = load_canonical_counts(cursor)
    pivot = resolve_translation(cursor, args.pivot)
    plan = build_canonical_plan(cursor, pivot, config, canonical_counts)

    translations = chunked_translations(cursor)
    if not translations:
        print("No chunked translations found — nothing to re-chunk")
        return 0

    version_prefix = f"c{CHUNKING_VERSION}:"
    prepared = []
    for translation in translations:
        new_chunks = compute_translation_chunks(
            cursor, translation, plan, config, canonical_counts
        )
        cursor.execute(
            "SELECT canonical_id, title, text FROM translation_chunks "
            "WHERE translation = %s",
            (translation["code"],),
        )
        old_rows = cursor.fetchall()
        carry, to_embed, stale = plan_embedding_carry(old_rows, new_chunks)
        renamed = sum(1 for old, new in carry.items() if old != new)
        print(
            f"{translation['alias']} (code {translation['code']}): "
            f"{len(old_rows)} chunks -> {len(new_chunks)}; embeddings: "
            f"{len(carry)} carried ({renamed} renamed), "
            f"{len(to_embed)} to embed, {len(stale)} stale"
        )
        prepared.append((translation, new_chunks, carry, to_embed, stale))

    if args.dry_run:
        print("dry-run: nothing written")
        return 0

    # One transaction over both tables: replace chunk sets, rename carried
    # embeddings to the new IDs and the current chunking version, delete
    # stale embeddings. No intermediate commits (and no DDL — DDL would
    # implicitly commit), so a failure rolls the whole migration back.
    insert_sql = """
        INSERT INTO translation_chunks
            (canonical_id, chunking_version, translation, book_number,
             chapter_number, verse_number_start, verse_number_end,
             verse_count, title, text, char_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for translation, new_chunks, carry, to_embed, stale in prepared:
        cursor.execute(
            "DELETE FROM translation_chunks WHERE translation = %s",
            (translation["code"],),
        )
        rows = [
            (
                chunk.canonical_id, CHUNKING_VERSION, translation["code"],
                chunk.book_number, chunk.chapter_number,
                chunk.verse_number_start, chunk.verse_number_end,
                chunk.verse_count, chunk.title, chunk.text, len(chunk.text),
            )
            for chunk in new_chunks
        ]
        for start in range(0, len(rows), BATCH_SIZE):
            cursor.executemany(insert_sql, rows[start:start + BATCH_SIZE])
        # Delete stale embeddings BEFORE renaming carried ones: a rename may
        # target an ID that a stale row still occupies (e.g. re-running
        # rechunk with a changed config without a version bump), which would
        # otherwise hit the unique key.
        if stale:
            for start in range(0, len(stale), BATCH_SIZE):
                batch = stale[start:start + BATCH_SIZE]
                placeholders = ", ".join(["%s"] * len(batch))
                cursor.execute(
                    f"DELETE FROM chunk_embeddings WHERE translation = %s "
                    f"AND canonical_id IN ({placeholders})",
                    (translation["code"], *batch),
                )
        pairs = sorted((old, new) for old, new in carry.items())
        for start in range(0, len(pairs), BATCH_SIZE):
            cursor.executemany(
                "UPDATE chunk_embeddings SET canonical_id = %s, "
                "embedding_version = CONCAT(%s, SUBSTRING(embedding_version, "
                "LOCATE(':', embedding_version) + 1)) "
                "WHERE translation = %s AND canonical_id = %s",
                [
                    (new, version_prefix, translation["code"], old)
                    for old, new in pairs[start:start + BATCH_SIZE]
                ],
            )
    # orphan cleanup: embeddings whose chunk no longer exists at all
    cursor.execute(
        """
        DELETE e FROM chunk_embeddings e
        LEFT JOIN translation_chunks c
          ON c.translation = e.translation AND c.canonical_id = e.canonical_id
        WHERE c.code IS NULL
        """
    )
    connection.commit()
    missing = sum(len(to_embed) for _, _, _, to_embed, _ in prepared)
    print(
        f"Re-chunked {len(prepared)} translations. "
        f"{missing} chunks lack embeddings — run: python app/index_cli.py rebuild"
    )
    return 0


# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Psalm versification mapping CLI"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("build", help="(re)build psalm_verse_mappings")
    p.add_argument("--translations",
                   help="Comma-separated aliases or codes (default: all)")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_build)

    p = sub.add_parser("verify", help="verify stored mapping vs the database")
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser(
        "rechunk",
        help="re-chunk with the canonical-space plan, carry embeddings over",
    )
    p.add_argument("--pivot", default="syn")
    p.add_argument("--target-chars", type=int, default=ChunkingConfig.target_chars)
    p.add_argument("--min-chars", type=int, default=ChunkingConfig.min_chars)
    p.add_argument("--max-chars", type=int, default=ChunkingConfig.max_chars)
    p.add_argument("--overlap-units", type=int, default=ChunkingConfig.overlap_units)
    p.add_argument("--overlap-max-chars", type=int,
                   default=ChunkingConfig.overlap_max_chars)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=cmd_rechunk)

    args = parser.parse_args(argv)

    connection = create_connection()
    if connection is None:
        print("Cannot connect to the database", file=sys.stderr)
        return 1
    cursor = connection.cursor(dictionary=True)
    try:
        return args.func(connection, cursor, args)
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
