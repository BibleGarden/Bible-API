# ADR 0001: Structural chunking of Bible texts for RAG

Status: accepted (2026-08-24); amended by ADR 0003 (2026-08-24):
CHUNKING_VERSION is now 3 — the Psalter is chunked in canonical
english-masoretic coordinate SPACE (verses converted through the
versification mapping before planning/application), so Psalm chunk
boundaries and plan-slot IDs are shared across traditions. Boundary/text
rules below are otherwise unchanged; `store_chunks` now replaces ALL
versions of a translation's chunks (one live chunk set at a time).
See architect/adr/0003-psalm-versification-canon.md.

## Context

The RAG passage-recommendation feature needs reproducible retrieval fragments
(chunks) built from the structure already present in `cep_public`:

- section titles — `translation_titles.text` attached to a verse via
  `before_translation_verse` (`subtitle=1` marks inscriptions, not sections);
- paragraphs — `translation_verses.start_paragraph`;
- verse coordinates — `(book_number, chapter_number, verse_number)`.

Requirements: a canonical chunk ID independent of the translation, natural
boundaries only (paragraph/verse), configurable target sizes, overlap in whole
natural units, parallel translations bound to the same canonical coordinates,
algorithm versioning, and full determinism. Embeddings are computed by a
separate follow-up task that must be able to index chunks idempotently.

## Decision

### Two-phase algorithm: canonical plan + application

`app/chunking.py` (pure, no I/O) implements two phases:

1. **Canonical boundary plan** is computed once from a **pivot translation**
   (CLI default: `syn`, configurable). For every chapter, verses are grouped
   into paragraphs (`start_paragraph`, plus every titled verse starts a
   paragraph), paragraphs into sections (a non-subtitle title starts a new
   section). Chapter and section boundaries are never crossed. Within a
   section, paragraphs are greedily merged up to `target_chars`; paragraphs
   longer than `max_chars` are split at verse boundaries; a trailing group
   below `min_chars` is merged back when the result stays within `max_chars`.
   Each planned chunk owns a verse range `own_start..own_end`; owned ranges of
   a chapter partition its verse axis.
2. **Application to a translation**: every verse of the translation is
   assigned to exactly one planned chunk of its chapter by verse number
   (the first chunk absorbs earlier verses, the last one absorbs trailing
   verses missing from the pivot), which guarantees full coverage even when
   versification differs from the pivot. The chunk title is the nearest
   preceding non-subtitle title of *this* translation within the chapter;
   the text keeps this translation's own paragraph breaks (`\n\n`).

### Canonical chunk ID

```
v{CHUNKING_VERSION}:{book:02d}.{chapter:03d}.{own_start:03d}-{own_end:03d}
e.g. v1:01.001.009-019
```

The ID contains only canonical coordinates (plan ownership range) plus the
algorithm version, so parallel translations chunked from the same plan share
IDs. Actual per-translation verse boundaries (which may differ due to overlap,
missing or trailing verses) are stored in `verse_number_start/verse_number_end`
columns — they, not the ID, are the source of truth for reconstruction.

### Sizes and overlap (defaults, CLI-configurable)

- `target_chars=1200`, `min_chars=400` (soft), `max_chars=2400` (hard for
  merging; a single verse longer than `max_chars` stays intact — e.g. the
  Esther additions in `syn` where a whole section lives in one verse).
- Overlap: `overlap_units=1` whole tail paragraph of the previous chunk of
  the same section, capped by `overlap_max_chars=400`; when the tail unit is
  larger, it degrades to the last verse, or to no overlap. Overlap never
  crosses a section or chapter boundary. Overlap verses are part of the chunk
  text and of `verse_number_start..end`, but ownership (and the ID) is
  overlap-free.

### Oversized-range refinement

When the verses owned by one planned chunk exceed `max_chars` in the applied
translation (versification offsets — e.g. en Psalm 119 vs ru Psalm 119 — or a
chapter absent from the pivot, e.g. Esther 11–12 in `ubh`), the range is
re-chunked with the same algorithm using the translation's own structure.
Refined IDs are still deterministic coordinate-based IDs, but may differ
between translations for such ranges. This is a deliberate trade-off: full
coverage and bounded sizes are preferred over ID sharing in the rare
mismatched ranges.

### Versioning

`CHUNKING_VERSION` (constant in `app/chunking.py`) is embedded in every ID and
stored in the `chunking_version` column. Any change to boundary or text rules
must bump it; the follow-up embedding task can then rebuild its index per
version without ambiguity.

### Storage

Chunks are stored in a new `cep_public` table (created by the CLI with
`CREATE TABLE IF NOT EXISTS`, following the DDL style of the existing stats
tables; no existing tables are modified):

```sql
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

A table (rather than a file artifact) was chosen because the embedding task
runs against the same database, needs incremental/idempotent indexing keyed by
`(translation, canonical_id)`, and the data must survive container rebuilds.
A run is idempotent: `DELETE` by `(translation, chunking_version)` + batch
`INSERT` in one transaction; re-running on unchanged data reproduces byte-identical
rows (verified by table checksum).

### Runner

`app/chunk_cli.py` (inside the `bible-api` container):

```
python app/chunk_cli.py --translations syn,bsb [--pivot syn] [--dry-run]
    [--target-chars N] [--min-chars N] [--max-chars N]
    [--overlap-units N] [--overlap-max-chars N]
```

It prints per-translation statistics (chunk count, size distribution, share of
chunks with a title).

## Consequences

- Every chunk reconstructs exact book, chapter, verse range, translation,
  title and text; every verse of a translation is covered by at least one
  chunk (verified against `syn` and `bsb`).
- Baseline run (pivot `syn`, defaults): `syn` — 3963 chunks, p50 810 chars,
  88.6% with title; `bsb` — 4046 chunks, p50 918 chars, 96.4% with title.
- The chunk text duplicates verse text (~1 MB per translation); acceptable.
- `translation_chunks` is not part of the admin-api import; after re-importing
  a translation the chunking CLI must be re-run for it.

## Open questions

- ~~Versification mapping between traditions (ru/en Psalms offset) is not
  solved; canonical coordinates follow the pivot's versification and the
  refinement fallback handles size blow-ups. A proper verse-mapping table
  could later make cross-language IDs semantically exact.~~ Resolved by
  ADR 0003: `psalm_verse_mappings` + canonical-space Psalm chunking (v3) —
  Psalm chunk boundaries and IDs are now shared across traditions.
- Title lookback is limited to the chunk's chapter; a section spanning a
  chapter boundary leaves the following chapter's first chunks untitled.
- Whether the embedding input should be `title + "\n\n" + text` is left to
  the embedding task (title is stored separately on purpose).
