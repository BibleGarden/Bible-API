# ADR 0028: Reference versification outside the Psalms

Status: accepted (2026-11-02).
Ticket: ClickUp 123pfqn54zc. Extends ADR 0003 (Psalms) for general search.

## Context

General search (`architect/scripture-search.md`) asks the model for
references in English numbering and maps them to windows of the request
language. Window IDs follow the `syn` pivot's chunking plan (ADR 0001), and
outside the Psalms each translation keeps its own numbering: English Jonah
1:17 is `syn`/`ubh` 2:1, Romans 16:25-27 is `syn` 14:24-26, Malachi 4 is
`ubh` 3:19-24, Numbers 16:36-50 is `ubh` 17:1-15. Matching references
against ID ranges lost such references or hit neighbouring text in `ru`,
`uk` and, at chapter tails, `en`.

## Decision

A second versification layer, used only to locate search references.
Psalms keep the canon of ADR 0003.

- Data: Paratext versification files in `app/vrs/`. `eng.vrs`, `org.vrs` and
  `rso.vrs` are copied unchanged from sillsdev/machine.py
  (`machine/scripture/*.vrs.txt` at commit `abe3718`, MIT, `app/vrs/LICENSE`),
  renamed from `.vrs.txt` to `.vrs`. Each lists a numbering's chapter lengths
  and maps its verses onto the original numbering (`org`); an unmapped verse
  keeps its number. Our correction files `bsb.vrs`, `syn.vrs` and `ubh.vrs`
  are layered over a base file (`versification.TRANSLATION_VERSIFICATIONS`)
  and record the checked differences of the stored texts, with the reason in
  comments. The English numbering of references is that of the indexed
  `bsb`.
- Corpus loading converts every indexed window's stored verses to English
  numbering (`scripture_select.reference_windows`); search matches
  references there. A verse without an English counterpart (a Septuagint
  addition) is dropped.
- Corpus loading refuses an indexed translation that has no versification,
  whose stored chapter lengths differ from it, that has a verse landing past
  the end of its English chapter without a `-` mark in its correction file
  (`-` is our extension of Paratext's, see `app/vrs/syn.vrs`), or that
  shares its language with another indexed translation. Search then answers 503; scripture
  selection does not use this layer and is unaffected.

### Alternatives

Measured offline without model calls
(`experiments/search-versification-2026-11-02/report.md`): 13 references to
differently numbered places in en/ru/uk plus the recorded control set.

| Design | Mini set: right passage first (en / ru / uk) | Control anchors in top 5 |
|---|---|---|
| ID ranges (before) | 10 / 10 / 6 of 13 | 67/72 |
| B: English window vector searched in the ru/uk index | 13 / 10 / 9 | 63/72 |
| A: this decision | 13 / 13 / 13 | 67/72 |

B needs no data but is less precise: the vector's nearest neighbour in
another language is often a parallel or adjacent passage, and diversity
then drops the right one (Exodus 37, Daniel 4:34-37).

## Consequences

- An edit of an indexed translation that changes a chapter's length (verses
  added, removed or renumbered), published and re-chunked, makes general
  search answer 503 until the matching `.vrs` file is corrected in
  Bible-API. Correct the file first, or in the same release.
- Chapter lengths do not reveal a shift inside a chapter (rso.vrs had two);
  the files were checked verse by verse against the stored texts with local
  embeddings, see the experiment report. A new indexed translation needs the
  same check.
- Window IDs, the index, retrieval, prompts and data publication are
  unchanged; conversion of all windows takes about 0.06 s per corpus load.
