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
  `machine/scripture` at `abe3718`, MIT, 50 KB, plus correction files
  `bsb.vrs`, `syn.vrs` and `ubh.vrs` (ADR 0028). No new table, import,
  export or production step: the files ship with the code and corpus
  loading validates chapter lengths against the loaded index.

## Checking the files against the texts

Chapter lengths (every non-Psalm chapter of `cep_public`): `syn` matches
`rso` everywhere; `bsb` matches `eng` except 3 John 1 and Revelation 12
(one verse each holds two); `ubh` matches `org` except Numbers 25-26,
Nehemiah 7 and Joel (English numbering), the Greek additions in Esther and
Daniel, and Song of Songs 1, where the stored text lacks verses 15-17.

Equal lengths do not exclude a shift inside a chapter, so `verify.py`
checks every verse: the local bge-m3 cosine between a `syn`/`ubh` verse and
its mapped `bsb` verses, against every `bsb` verse of the neighbouring
chapters; a verse is flagged when the mapped similarity is under 0.6 and
another `bsb` verse scores 0.1 higher. Coverage: all 28,829 non-Psalm `syn`
verses and 28,842 `ubh` verses, 28,621 and 28,624 of them with an English
counterpart (median cosine 0.82), embedded in 79 minutes on 8 CPU cores.
Output: `verify.jsonl`, and `verify-base.jsonl` with rso.vrs alone.

- With rso.vrs alone the check flags 50 `syn` verses, among them two runs:
  Isaiah 3:20-25 (6 verses) and Daniel 6:1-28 (26 verses), both mapped one
  verse off by rso.vrs (Daniel 5:31-6:28 of `syn` is numbered as in English
  Bibles). `syn.vrs` corrects them.
- Pairs whose order or division differs inside a chapter, all corrected in
  `syn.vrs`/`ubh.vrs`: `syn` Philippians 1:16-17 (Textus Receptus order) and
  Revelation 20:7-9; `ubh` Proverbs 6:22-23, Song of Songs 8:3-5 and
  Nahum 2:2-3. `syn` Proverbs 13:14 and 18:8 are Septuagint additions that
  rso.vrs left mapped onto the next verse; they are now marked as having no
  counterpart.
- Remaining after the corrections: 13 `syn` and 6 `ubh` single verses, no
  runs. Each was read: genealogies and lists, numerals, bracketed
  Septuagint text inside a verse (`syn` Genesis 11:12, 1 Kings 2:35,
  2 Chronicles 35:20, Esther 1:1), short variants (Luke 20:30) and poetry
  paraphrase; none is a numbering error. One is a data error: `ubh`
  1 Samuel 13:1 holds the text of 13:2 and its 13:2 is empty.
- The review also added mappings eng.vrs lacks (Numbers 26:1, Nehemiah
  7:68, Isaiah 64:1, Acts 19:41), so every `bsb` verse maps back onto
  itself or onto the pair the original numbering merges.

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
- Re-run after the corrections above: the same figures.

## Decision

Path A: better on every measure, with no change to retrieval, prompts, the
index or data publication. Candidates for `en`/`ru` are unchanged on the
control set, so no live run was made.
