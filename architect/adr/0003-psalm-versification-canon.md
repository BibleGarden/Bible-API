# ADR 0003: Psalm versification canon and the verse mapping table

Status: accepted (2026-08-24)
Ticket: ClickUp 86cb90j2f

## Context

`cep_public` stores three Psalm numbering traditions:

- `bsb`, `webus`, `webbe` — english-masoretic: Masoretic chapter numbers
  1-150, superscriptions unnumbered;
- `syn`, `bti`, `npu` — Septuagint chapter numbering (Masoretic 9+10 and
  114+115 merged into one psalm, 116 and 147 split into two), plus the
  superscription counted as verse 1 (or 1-2) in ~60 psalms, plus the extra
  Psalm 151 in `syn`;
- `ubh` — Masoretic chapter numbers but Hebrew-style verse numbering: the
  superscription is verse 1 (or 1-2) in ~60 psalms.

Everything downstream of the raw verses — RAG chunk IDs (ADR 0001), the
retrieval benchmark's reference coordinates, and the future selection
endpoint — needs one coordinate system. Before this ADR the canonical chunk
IDs implicitly carried the pivot's (`syn`, Septuagint) coordinates, and the
chapter-keyed plan application meant the *same* canonical ID pointed to
*different* content in different translations throughout the Psalter (e.g.
`v1:19.136.001-009` was Masoretic Ps 137 in `syn` but Ps 136 in `bsb`/`ubh`).
The benchmark carried its own private mapping layer (flagged as an open
question in ADR 0002).

## Decision

### Canon: english-masoretic (decision by Maria)

Canonical Psalm coordinates are Masoretic chapter numbers with unnumbered
superscriptions, exactly as `bsb` stores them (`versification.CANONICAL_ALIAS`).
Canonical verse 0 denotes the superscription — the canon does not number it,
but counted superscription verses of other translations need a target.
The Septuagint Psalm 151 is kept as canonical chapter 151 (7 verses).

### Mapping construction: rules + data-driven offsets + explicit exceptions

`app/versification.py` (pure, no I/O) builds a verse-level map
"translation (chapter, verse) -> canonical (chapter, verse range)":

1. **Chapter correspondence** follows the classical rules
   (`septuagint_segments`): Sept. 9 = Mas. 9+10, Sept. 113 = Mas. 114+115,
   Sept. 114/115 = Mas. 116:1-9 / 116:10-19, Sept. 146/147 = Mas. 147:1-11 /
   147:12-20, ±1 shift between the seams, identity at 1-8 and 148-151.
   Masoretic-numbered translations map chapters 1:1.
2. **Verse offsets are data-driven**: offset = MAX(verse_number) of the
   chapter minus the expected canonical verse count of its segment(s). The
   0-2 offset verses at the head are the counted superscription and map to
   canonical verse 0. This absorbs all per-translation inconsistencies
   (e.g. `ubh` counts the superscription in Ps 3 but merges it into verse 1
   in Ps 103; `syn` superscription slots are empty-text verses).
3. **Explicit exceptions** (`versification.EXCEPTIONS`) cover chapters the
   offset rule cannot express. Exactly one exists today: `syn` 114:8 merges
   canonical 116:8-9 into one verse (verified against the stored texts).
   Any unexplained chapter fails the build loudly.

Validation is built in: every verse number 1..max of every chapter receives
exactly one mapping, and the canonical verse axis of each chapter is
consumed exactly once. Against the live DB (all 7 translations, 1051 Psalm
chapters) the build is clean; seams and shifts were verified against actual
verse texts (Ps 3, 9/10, 51/52, 103, 113-116, 146/147, 151 across
translations). Two real numbering gaps exist (`bti` skips 67:14 and 105:6 —
text merged into a neighbour verse); they surface as canonical holes
(68:13, 105:6) in `verify`, not as mapping errors.

### Storage: `psalm_verse_mappings`

```sql
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

Same DDL/lifecycle style as `translation_chunks`: created by the CLI, rows
replaced idempotently per (translation, mapping_version) in one transaction.
One row per *existing* translation verse (17 490 rows for 7 translations);
`canonical_verse_start = 0` marks superscription verses; `start < end` marks
merges. The reverse direction (canonical -> translation) is the same table
queried by the canonical columns; both directions are available in Python
via `versification.PsalmMap` (`to_canonical` / `from_canonical`).

Runner: `python app/versification_cli.py build | verify | rechunk`.
`verify` recomputes the map from the live verses and checks the stored rows
(coverage, drift, double coverage of canonical verses).

### Canonical-space chunking (CHUNKING_VERSION 3)

The Psalter is chunked in canonical coordinate **space**: before planning
and application, `versification.canonicalize_psalm_chapters` converts every
translation's Psalm verses to canonical chapters and verse numbers
(Septuagint chapters covering two canonical psalms are split apart, the two
Septuagint halves of canonical 116/147 are joined, counted superscriptions
become canonical verse 0, the extra verse of a two-verse superscription
gets the placeholder -1 — IDs clamp negatives to 000). The chunker itself
(`chunking.py`) stays coordinate-agnostic.

Consequences of planning per canonical chapter:

- the pivot plan for canonical psalm N is built from the pivot's text of
  *that same* psalm in every translation — the v1 bug (plan of psalm N+1
  applied to Masoretic-numbered chapter N) is gone;
- canonical chapters are hard boundaries, and `CANONICAL_SPLITS`
  (116:10, 147:12 — the Septuagint split points) are injected into the plan
  as hard section boundaries, so no chunk ever spans two translation
  chapters of the Septuagint-numbered translations;
- Psalm plan-slot IDs are **shared across traditions** again, now
  semantically exact: after the local migration 182 of 187 canonical Psalm
  chunk IDs exist in all three indexed translations (`v3:19.023.001-006` =
  Ps 23:1-6 in syn ch22 / bsb ch23 / ubh ch23); the 5 exceptions are
  Ps 109 in `bsb` (English text exceeds max_chars -> per-translation
  refinement, the documented ADR 0001 trade-off) and Ps 151 (syn only);
- chunk rows keep the translation's own display coordinates
  (`chapter_number`, `verse_number_start/end` are mapped back through the
  versification map after chunking).

The ID semantics changed twice on the way here (v2 = content-describing
owned ranges as a pure rename step; v3 = shared canonical plan slots), and
each change bumped `CHUNKING_VERSION` — the version is part of every ID
precisely so that a semantic change produces a disjoint ID space.

### Migration with minimal re-embedding

`versification_cli.py rechunk` replaces the chunk set of every chunked
translation with the current-version output and migrates the embeddings in
the same transaction, pairing old and new chunks by their **embedding text**
(`title + "\n\n" + text`, multiset semantics, deterministic order):

- text unchanged -> the embedding row is renamed to the new canonical ID
  and the current `c{version}:` prefix (no API call);
- text changed/new -> left to `index_cli.py rebuild` (incremental);
- old chunk gone -> its embedding row is deleted (plus an orphan sweep).

Re-running on migrated data is a no-op. Executed 2026-08-24 on the local
DB (v2 -> v3): 11 987 -> 11 960 chunks; embeddings: 11 847 carried by
rename, 113 re-embedded (syn 4, bsb 51, ubh 58 — most psalms are
single-chunk, so the plan change only renamed their IDs without touching
texts), 140 stale rows deleted. `index_cli.py status`: complete
`c3:gemini-embedding-001@768` index. The v1 -> v2 step before it renamed
all 11 987 IDs with zero re-embedding. The benchmark corpus cache was
updated the same way (11 847 vectors reused, 113 embedded at 3072 dims).

**Operational order after ANY `CHUNKING_VERSION` bump** (local and prod):

1. `python app/versification_cli.py build` (once per mapping version);
2. `python app/versification_cli.py rechunk` — migrates chunks AND
   embeddings;
3. `python app/index_cli.py rebuild` — embeds only what rechunk left
   missing.

`rebuild` run out of order (embeddings present, no chunks of the current
version yet) REFUSES with a pointer to this sequence instead of silently
deleting the whole index; `--force` overrides for a deliberate
wipe-and-re-embed.

Known limitations (accepted): (a) on an `EMBEDDING_MODEL`/dimensions
change, `rebuild` deletes the stale-version rows (with a commit) before
the new ones are embedded — an abort mid-way leaves a partial index that
the next run completes (cheap on a billed key); (b) `rechunk` derives its
carried/to-embed counters from chunk texts without cross-checking the
actual `chunk_embeddings` rows — pre-existing gaps stay until `rebuild`;
(c) `index_cli.py status` shows `!!` rows while two embedding versions
coexist mid-migration — informational only.

## Consequences

- One shared implementation of the Psalm mapping: the chunker, the
  benchmark (`evaluation/retrieval_benchmark.py` now imports
  `app/versification.py`; its private mapping layer is gone) and the future
  selection endpoint all use the same rules — closing ADR 0002 open
  question 5.
- Canonical chunk IDs in the Psalter now truthfully name Masoretic
  coordinates; downstream consumers (reranking, deduplication across
  translations, user-facing references) can trust them.
- Retrieval quality is essentially unchanged by v3 (benchmark before/after
  identical: ALL hit@10 0.208, ru 0.400, en 0.143, uk 0.000): the plan
  granularity is still the pivot's (syn has sparser section titles than
  e.g. ubh, so most psalms remain 1-2 chunks), and the benchmark gap is
  dominated by query-formulation/model quality (ADR 0002). A diagnostic
  re-chunk of the ubh Psalter by its OWN (finer) structure lifted uk hit@10
  to 0.143 — the improvement came from finer granularity, not alignment.

## Deferred / open questions

1. **Psalm chunk granularity.** The canonical plan inherits syn's sparse
   sectioning; a finer plan (e.g. union of section boundaries across
   translations, or a smaller target size for the Psalter) measurably helps
   uk (0 -> 0.143 in the diagnostic) and should be considered together with
   the query-reformulation work of ADR 0002.
2. `npu`/`bti`/`webus`/`webbe` are mapped but still not chunked/indexed
   (ADR 0002 open question 6 unchanged).
3. The selection endpoint should resolve user-facing references through
   `psalm_verse_mappings` (canonical -> translation) once it exists.
4. Production: run `versification_cli.py build`, then `rechunk` (works from
   v1 directly — pairing is text-based), then `index_cli.py rebuild`.
