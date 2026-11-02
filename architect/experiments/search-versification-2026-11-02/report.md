# General search: references in the translations' own numbering — 2026-11-02

Task: ClickUp 123pfqn54zc (parent 123pfqn4yyg). Baseline: Bible-API `d649d6c`
(`feat/123pfqn46h2-semantic-search`, references of
[search-refs-2026-11-02](../search-refs-2026-11-02/report.md)).

## Problem

Expansion references use English numbering and were matched against the
canonical ID ranges of the request language's windows. Those ranges come
from the chunking plan of the `syn` pivot (ADR 0001), i.e. Synodal
numbering, whatever the translation. In the local `cep_public`, outside the
Psalms, the last verse of a chapter differs between `syn` and `bsb` in 38
chapters of 17 books, between `ubh` and `bsb` in 82 chapters of 30 books.
So a reference was lost or landed on neighbouring text in `ru` and `uk`,
and also in `en`: 37 `bsb` and 66 `ubh` windows store other verses than
their ID says (English Jonah 1:17, Romans 16:25-27 and 2 Corinthians 13:14
had no `en` window).

## Paths compared

- **B, vectors:** match the reference to an English window and search the
  `ru`/`uk` index with that window's stored vector (bge-m3 is multilingual).
  No versification data.
- **A, versification:** convert each window's stored verses from its
  translation's numbering to English and match references there. Data:
  Paratext versification files (`eng`, `org`, `rso`) from sillsdev/machine.py
  `machine/scripture` at `abe3718`, MIT, 50 KB, plus two correction files.
  Checked against `cep_public` chapter lengths: `syn` = `rso` in every
  chapter outside the Psalms (which keep ADR 0003); `bsb` = `eng` except
  3 John 1 and Revelation 12 (one verse each holds two); `ubh` = `org` except
  Numbers 25-26, Nehemiah 7 and Joel (English numbering), the Greek additions
  in Esther and Daniel, and Song of Songs 1, where the stored text lacks
  verses 15-17. `bsb.vrs` and `ubh.vrs` record these; the texts of every
  pair in the mini set were read in the database. No new table, import,
  export or production step: the files ship with the code and corpus
  loading validates them against the loaded index.

## Replay

`compare.py` replays without any Gemini call (query embeddings by local
bge-m3); `compare.jsonl` holds the output. Control: the references
recorded in `after-refs.jsonl` (36 positive runs, 72 anchors). Mini set: 13
references to differently numbered places (Jonah 1:17, Romans 16:25-27,
Numbers 16:46-48, Malachi 4:5-6, Daniel 4:34-37, Hosea 14:1-2,
2 Corinthians 13:14, Job 41:1-8, Isaiah 9:6-7, Jeremiah 9:23-24,
1 Kings 4:29-34, Exodus 8:1-4, Ecclesiastes 5:1-2), each with a short query
in `en`, `ru` and `uk`; the target is the passage in the indexed
translation's own numbering. Ranks are candidate positions before the ranker.

| Design | Control: anchors among candidates / in top 5 | Mini `en` top 1 / top 5 | Mini `ru` top 1 / top 5 | Mini `uk` top 1 / top 5 |
|---|---|---|---|---|
| Current (ID ranges) | 67/72 / 67/72 | 10/13 / 13/13 | 10/13 / 12/13 | 6/13 / 12/13 |
| B: English window vectors | 65/72 / 63/72 | 13/13 / 13/13 | 10/13 / 11/13 | 9/13 / 11/13 |
| A: versification (implemented) | 67/72 / 67/72 | 13/13 / 13/13 | 13/13 / 13/13 | 13/13 / 13/13 |

- Path B loses two `ru` control anchors: the English Exodus 37 vector finds
  the parallel Exodus 25 in `ru` first, and diversity keeps one window per
  chapter, so Exodus 37 drops out; the worry query's third anchor falls
  from 3 to 7. It also misses Daniel 4:34-37 in both languages (the nearest
  `syn`/`ubh` window is 4:25-30) and puts the Romans doxology at 6 in `ru`.
- Path A changes no control rank. The current design's mini-set hits come
  mostly from the query, which quotes the passage; it found Leviathan in no
  `ru` candidate and Malachi 4 in no `uk` candidate.
- A first prototype of path A with a hand-written table for these places,
  still matching ID ranges, gave `ru` 13/13 and `uk` 10/13 top 1 (12/13 in
  the top 5); matching stored verses fixed the rest.
- Converting all windows at corpus loading takes 0.06 s.

## Decision

Path A: better on every measure, with no change to retrieval, prompts, the
index or data publication. Candidates for `en`/`ru` are unchanged on the
control set, so no live run was made.
