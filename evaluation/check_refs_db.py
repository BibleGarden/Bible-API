#!/usr/bin/env python3
"""Verify that every canonical reference in evaluation/scenarios.json exists
in the cep_public database.

Canonical coordinates use english-masoretic psalm numbering (see
coordinate_system in scenarios.json), so book 19 (Psalms) is validated
against an English translation (bsb). Non-psalm references are additionally
validated against a native-language translation of the scenario language.

Run from the repo root on a machine that can reach the database, e.g.:

    docker cp evaluation bible-api:/tmp/evaluation
    docker exec bible-api python3 /tmp/evaluation/check_refs_db.py

or point it at the DB directly with the standard DB_* environment variables.
Exits non-zero if any reference is missing.
"""

import json
import os
import sys
from pathlib import Path

import mysql.connector

SCENARIOS_PATH = Path(__file__).resolve().parent / "scenarios.json"

# Translation aliases used for existence checks. Psalms are always checked
# against the English baseline (canonical numbering); other books are also
# checked against a full-Bible native translation. Note: npu is NOT used
# here because it covers only 28 books.
ENGLISH_BASELINE = "bsb"
NATIVE_BASELINE = {"ru": "bti", "en": "bsb", "uk": "ubh"}
PSALMS_BOOK = 19


def connect():
    return mysql.connector.connect(
        host=os.environ.get("DB_HOST", "cep-mysql"),
        user=os.environ.get("DB_USER", "root"),
        password=os.environ["DB_PASSWORD"],
        database=os.environ.get("DB_NAME", "cep_public"),
    )


def reference_exists(cursor, alias, book, chapter, verse_start, verse_end):
    cursor.execute(
        """
        SELECT COUNT(*) FROM translation_verses v
        JOIN translations t ON t.code = v.translation
        WHERE t.alias = %s AND v.book_number = %s AND v.chapter_number = %s
          AND (
            (v.verse_number BETWEEN %s AND %s)
            OR (v.verse_number <= %s AND v.verse_number_join >= %s)
          )
        """,
        (alias, book, chapter, verse_start, verse_end, verse_start, verse_start),
    )
    return cursor.fetchone()[0] > 0


def main():
    dataset = json.loads(SCENARIOS_PATH.read_text(encoding="utf-8"))
    connection = connect()
    cursor = connection.cursor()
    missing = []
    checked = 0
    for scenario in dataset["scenarios"]:
        native_alias = NATIVE_BASELINE[scenario["language"]]
        for ref in scenario["references"]:
            coords = (
                ref["book_number"],
                ref["chapter"],
                ref["verse_start"],
                ref["verse_end"],
            )
            # Canonical numbering is english-masoretic: psalms are checked
            # against the English baseline only (other translations need the
            # documented mapping layer).
            aliases = {ENGLISH_BASELINE}
            if ref["book_number"] != PSALMS_BOOK:
                aliases.add(native_alias)
            for alias in sorted(aliases):
                checked += 1
                if not reference_exists(cursor, alias, *coords):
                    missing.append((scenario["id"], alias, coords))
    cursor.close()
    connection.close()

    for scenario_id, alias, coords in missing:
        print(f"MISSING {scenario_id} [{alias}]: book={coords[0]} "
              f"chapter={coords[1]} verses={coords[2]}-{coords[3]}")
    print(f"Checked {checked} (scenario reference, translation) pairs; "
          f"missing: {len(missing)}")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
