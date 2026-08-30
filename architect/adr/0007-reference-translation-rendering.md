# ADR 0007: Serving any active translation from one indexed corpus

Status: accepted (2026-08-27).
Ticket: follow-up of ClickUp 86cb8vw1m (ADR 0006 open question 5)

> Note (2026-08-30, ClickUp 86cbbmwjk): `POST /api/scripture/v1/select` was
> renamed to `POST /api/ai/scripture`, and the catalogue endpoint described
> below (`GET /api/scripture/v1/translations`) was removed. This decision is
> the reason it could go: since it, the selection renders any active
> translation of an indexed language, so the client no longer needs to ask
> which ones are servable. The rule the catalogue published still holds and
> is documented in `architect/scripture-select.md`; only its HTTP surface is
> gone. Old path names are kept in the historical text.

> Note (2026-08-30, ClickUp 86cbbmy8d): `SCRIPTURE_PRIMARY_TRANSLATIONS`, the
> setting this ADR introduces, is now `AI_SCRIPTURE_PRIMARY_TRANSLATIONS`.
> Name only — the format (`ru=syn,en=bsb,uk=ubh`), the "must be indexed" rule
> and the empty-value default are unchanged.

> Note (2026-08-30, ClickUp 86cbb1reb): after that day's BTI backfill (BTI is
> now complete: 1189/1189 canonical chapters, 31111 verses), `bti`'s coverage
> in the table below grew from 3830 to **3899** / 3963 ru windows. The table
> row is left as originally measured; `tests/test_scripture_select.py::
> test_the_live_coverage_sets_are_the_documented_ones` asserts the current
> figure.

## Context

`POST /api/scripture/v1/select` accepted only translations that carry an
embedded chunk corpus: `resolve_translation` validated the request against
`CorpusResources.translations`, which was built from `index.metas`. Indexed
today: ru `syn` (1), en `bsb` (16), uk `ubh` (20). Active but not indexed:
ru `bti` (11), en `webus` (17) and `webbe` (779), uk `npu` (21) — asking for
any of them answered 422, so the app could offer the reader only one Bible
per language.

Indexing the other four is the expensive answer (four embedding runs, four
more corpora to keep in step with every CHUNKING_VERSION bump, ~2x the
index memory) and it buys nothing for retrieval quality: the pipeline
searches ONE translation per language by design (ADR 0004), the candidates
are canonical windows, and the second translation would only ever be used to
print the same window in different words.

Everything needed for that printing already exists:

- canonical chunk IDs (`v3:19.023.001-006`) are translation-independent by
  construction (ADR 0001 plan slots, ADR 0003 canonical Psalm space);
- `psalm_verse_mappings` is built and verified for all seven translations,
  and `passage_highlight.load_psalm_maps` already loads all of them;
- the cross-translation branch of `resolve_highlight`
  (`from_canonical_span` + `_inside_passage`) was written and unit-tested
  for exactly this case, and had never been reached on a live path.

## Decision

### Two catalogues: indexed and renderable

`CorpusResources` now distinguishes

- **indexed** (`.indexed`): the translations whose chunks and embeddings ARE
  the corpus. Retrieval, BM25, diversity and the rerank prompt are unchanged
  and still happen exclusively in this space.
- **renderable** (`.translations`, the catalogue the public contract speaks
  of): every ACTIVE translation of a language that has an index, provided it
  has a Psalm versification map and covers at least one canonical window.

A request names a renderable translation; the pipeline still runs in the
indexed one. 422 therefore means one thing only: "not in this language's
renderable catalogue" — the previous OpenAPI wording ("does not belong to
the language") was already wrong for an inactive or unknown code and is now
fixed. The message still repeats nothing but the number the caller sent.

### The primary translation is configuration, not index order (closes 0006 OQ5)

`AI_SCRIPTURE_PRIMARY_TRANSLATIONS`, format `ru=syn,en=bsb,uk=ubh` — comma
separated `language=alias` or `language=code`, whitespace ignored. An entry
whose translation is not INDEXED for that language, or whose language has no
index, is ignored with a warning and the language falls back to the default.

Default (and the current production value — the variable is unset): the
indexed translation with the lowest code. That is deterministic, and while
every language has exactly one indexed translation it is identical to the
previous `available[0][0]`, which depended on `ORDER BY e.code` over
`chunk_embeddings` and would have become arbitrary the moment a language got
a second index.

The primary must be indexed because it is what the corpus is made of. Note
the prompt passage the reranker reads is still `candidate.passages[0]`
(index insertion order), which coincides with the primary today; making the
prompt follow the configuration is a separate change, because it would alter
the input of the benchmarked rerank stage (ADR 0005).

### Coverage: filter candidates, never repair a passage

A translation may lack a verse, a chapter or twenty books. Measured over the
canonical windows of each language's corpus (`app/passage_render.py`,
`build_coverage`):

| translation | language | fully covered windows |
|---|---|---|
| `syn` (primary) | ru | 3960 / 3963 |
| `bti` | ru | 3830 / 3963 |
| `bsb` (primary) | en | 4000 / 4032 |
| `webus` | en | 3995 / 4032 |
| `webbe` | en | 3995 / 4032 |
| `ubh` (primary) | uk | 3934 / 3965 |
| `npu` | uk | 1163 / 3965 |

A window is covered when EVERY canonical verse of its own range exists in
that translation with non-empty text — canonical Psalm coordinates
converted through the stored versification map, and canonical verse 0 (the
superscription the canon does not number) not required, since translations
disagree on whether it exists at all. Empty verses count as absent, exactly
as `chunking.build_text` drops them. A window without a single positive
verse is never covered: an empty verse range would pass the "every verse
exists" test vacuously and then fail to resolve into any translation. The
corpus has no such window; the rule keeps it that way.

The primary rows are informational — the primary is served from its own
chunk and is never filtered. The three non-primary rows are the sets that
actually gate a selection, and they are built over a REDUCED window
universe: see the next section.

### Only windows the reference chunk really is

The reranker judges the reference translation's STORED chunk; a translation
without a corpus is served the window's own canonical range. The two texts
may legitimately differ by the overlap prefix the chunker copies from the
previous chunk (documented below). They may also differ by a TAIL: the last
plan slot of a chapter gets no upper bound in
`chunking.apply_chapter_plan`, so its chunk absorbs every trailing verse of
the chapter. `v3:18.041.012-026` is stored in `bsb` as Job 41:12-34; a
`webus` reader would have received 12-26 — up to 8 verses and 55 % of the
text shorter than what the AI actually read, and shorter at the END, which
no prefix rule covers.

`passage_render.reference_faithful_windows` therefore removes such windows
from the candidate universe of every non-indexed translation, comparing
real data and nothing else: the `(chapter, last verse)` stored in
`translation_chunks` for the reference translation against the window's own
canonical range converted into that same numbering. A window whose stored
chunk ends later than its own range is dropped, as is one with no stored
chunk at all (fail-closed). Measured over the corpus: **ru 0, en 17, uk 38**
windows dropped — and the rule is exact rather than conservative: the set it
drops is, window for window, the set whose stored chunk text does NOT end
with the own-range rendering (checked over all 11 960 reference chunks:
3963 ru + 4032 en + 3965 uk; a regression test does the same over every
covered window). Of them, 17 en and 4 uk windows were inside a coverage set,
which is why `webus` and `webbe` fall from 4012 to 3995 and `npu` from 1167
to 1163.

Two directions are deliberately left alone. The prefix: rendering the own
range means the reader gets the same passage the canonical ID names,
starting where the ID says it starts — the trade this ADR already makes.
And a reference chunk that stops EARLY (the reference lacks a verse the
served translation has: `bsb` has no Matthew 17:21, `webus` does — 5
windows) — there the reader sees one verse MORE than the reranker did, and
still exactly the range the canonical ID names.

The coverage sets are built with the corpus, cached with it (same TTL, same
`POST /api/cache/clear`) and cost ~0.45 s of the now ~1.4 s corpus load —
one `translation_verses` scan per non-primary renderable translation, plus
one `translation_chunks` range read per language for the reference filter.

They are applied **before the rerank**, next to the existing exclusion and
genre-blacklist filter (`ScriptureRetriever._filter`, new
`allowed_canonical_ids`), and to the safe pool resolution as well. Three
consequences on purpose:

- the rerank prompt and its numbered variant are byte-for-byte unchanged —
  the filter changes which candidates exist, never how they are rendered;
- the AI can never choose a passage the server would then fail to print;
- the primary translation passes `None` and is not filtered at all, so its
  request path is the pre-ADR-0007 path, instruction for instruction.

A non-primary translation with no coverage set at all is fail-CLOSED: the
filter refuses every window rather than reading a missing set as "no
restriction", which would let exactly the unverified translation through
unchecked. The catalogue never publishes such a translation, so this is a
guard against drift between the two, not a reachable mode.

`npu` is supported with a narrowed pool rather than excluded: an incomplete
Bible (New Testament + Psalms) is a normal product situation, and 1163
windows are plenty for a prayer selection.

### An emptied candidate list is a narrowed pool, not an error

The coverage set is applied at the same point as the existing exclusion and
genre-blacklist filter (`ScriptureRetriever._filter`) and can, together with
them, remove everything a query ranked — the code review measured 4 of 60
`npu` probe topics (Old Testament themes) ending with zero candidates, and
the pipeline then answered 503. `coverage_empty` names the resulting empty
pool, not coverage in isolation: a topic can just as well be emptied by a
client's own `exclude_canonical_ids` or the genre blacklist landing on the
few windows coverage had left standing. That is a property of an incomplete
Bible (or of an ordinary exclusion list interacting with one), not a
failure, so the selection degrades the way it does when the embedding
provider is down: the curated safe pool answers, with `source=safe_pool`
and the new category `fallback_reason=coverage_empty`. The pool is resolved
through the SAME coverage set, so what it serves is renderable by
construction.

The category is new and public (`FallbackReason.coverage_empty`, OpenAPI
`SelectResponse.fallback_reason`); it can never appear for a primary
translation, which has no filter.

**The primary path degrades the same way** (fix F1 extended). A coverage
set is not the only filter that can empty a ranking: the caller's
`exclude_canonical_ids` (up to 200 IDs) and the genre blacklist can between
them remove every window a narrow topic ranks, on a fully covered
translation with no coverage set at all. The condition is identical — a
narrowed pool, not a broken server — so the answer is identical: the safe
pool, `source=safe_pool`. The reason is reported under its own category,
`fallback_reason=ranking_empty`, and NOT under `coverage_empty`: on the
primary path there is no coverage filter, and naming one would blame a
mechanism that never ran. The two categories are how the difference between
"this Bible is incomplete" and "this client has exhausted the corpus for
its topic" actually reaches anyone: the client sees it in the response's
`fallback_reason`, and an operator sees it in the retrieval log line. It is
NOT visible in the request statistics — `api_requests` (`app/middleware.py`)
records only endpoint, method, status, timing, IP and user agent, never
`fallback_reason` — so collapsing the categories would have cost the log,
not a stats column. `ranking_empty` is likewise new and public, and needs
the same client announcement `coverage_empty` did; both are additive values
of an existing enum, and a client that does not know the value still
receives a valid passage.

**A caveat on what the category actually names.** The choice between the
two is made on whether a coverage filter is ACTIVE
(`self.allowed_canonical_ids is not None`), not on which filter is what
actually emptied the ranking. A non-primary translation with full coverage
of a topic, whose ranking is emptied purely by `exclude_canonical_ids` or
the genre blacklist — coverage removing nothing — still reports
`coverage_empty`, because it ran on the coverage-filtered path regardless.
That is deliberate, not a bug to fix here: `coverage_empty` means "the pool
emptied while a coverage restriction was in effect, under any combination
of it with the other filters," and `ranking_empty` means "the pool emptied
on the primary path, where no coverage filter runs at all." The category
names the PATH the request took, not the single mechanism responsible, and
the behavior described above is unchanged by this ADR.

Only when the pool itself resolves to nothing is there nothing verified
left to serve, and the request ends in the documented 503. Three things can
reach that: a coverage set hiding every place (unreachable through the
catalogue, which drops a translation covering no window), an empty pool
file, or a non-empty pool file whose entries name windows that are not in
the language's corpus at all — `_resolve_pool_ids` matches each ref against
that language's own canonical windows before coverage is even applied, and
a ref with no match resolves to `None` regardless (a data bug in
`safe_pool.json`, not a translation gap). The caller's exclusions cannot
cause it on their own, because `rotate_safe_pool` resets and repeats a
place once the exclusions cover the whole pool.

### Rendering: the canonical window, read from `translation_verses`

`app/passage_render.py` turns a chosen canonical window into the passage of
a translation that has no chunk of it:

- coordinates through `psalm_verse_mappings` (identity outside the Psalms);
  a window starting at canonical 0 grows back over the verses the target
  counts as the superscription, and starts at canonical verse 1 where it
  counts none;
- verses from `translation_verses`, assembled with `chunking.build_text`
  itself (the function is now public) — empty verses dropped, paragraphs at
  `start_paragraph` and at section titles;
- the section title by the chunker's own rule: the most specific
  non-subtitle title at or before the first verse of the window.

Deliberate difference from an indexed passage: the rendered range is the
window's OWN canonical range, without the overlap verses a chunk copies from
its predecessor. That overlap is a property of the pivot translation's
chunking plan, which does not exist for a translation that was never
chunked; rendering the own range is exact and reproducible instead of
approximated.

Verified against the corpus itself (all chunks of the three indexed
translations, coverage-filtered): `syn` 3463 windows render byte-identical
to the stored chunk text and 497 differ only by that overlap prefix, with
**zero** other differences; `bsb` 3437/547 with 16 windows differing
otherwise and `ubh` 3409/487 with 38 — all of them ranges where the chunker
refined or absorbed verses the pivot does not have, i.e. places where the
stored chunk is not the window's own range. Those windows (17 en and 38 uk
over the whole window set) are exactly the ones the reference filter above
removes, so none of them can be served from a non-indexed translation.
Nothing of that reaches the primary path, which never re-renders.

A window that cannot be rendered answers **503**, never another
translation's text: silently substituting a translation is exactly the kind
of ungrounded answer this endpoint exists to prevent. The same 503 covers a
database failure during the rendering and a request whose time budget is
already exhausted when the rendering would start — the rendering is the one
DB round trip after the pipeline, and there is no cheaper answer to fall
back to, because the passage of the requested translation IS the response.

### `GET /api/scripture/v1/translations`

The client needs to know what it may ask for. The new endpoint lists, per
language, the renderable translations and which one is `primary` (the
default when `translation` is omitted). It is served from the same cached
`CorpusResources` object `resolve_translation` validates against, so the
list and the validation cannot drift apart. Same `X-API-Key`, 503 on a cold
corpus.

Shape (codes + aliases only; names, descriptions and audio voices stay in
`GET /api/translations`, joined by `code`):

```json
{"languages": [
  {"language": "ru", "translations": [
    {"code": 1, "alias": "syn", "primary": true},
    {"code": 11, "alias": "bti", "primary": false}]}]}
```

### Highlight across translations

`build_highlight` already passed the served passage to `resolve_highlight`;
with a non-primary translation this reaches the previously unused branch:
marker span -> the prompt translation's verse numbers -> canonical ->
target's numbering -> re-checked against the served passage's own range.
Nothing is clamped: a span that lands outside answers without a highlight.

Live example now covered by a test: canonical Psalm 116:1-8 is chapter 114
in both `syn` and `bti`, but `syn` merges canonical 116:8 and 116:9 into its
verse 8 (versification EXCEPTIONS). Marker 7 comes back as bti 114:7;
marker 8 expands to canonical 116:8-9, which reaches past the window `bti`
renders — and is dropped.

## Consequences

- New module `app/passage_render.py`; `chunking._build_text` is now the
  public `chunking.build_text`.
- New env var `AI_SCRIPTURE_PRIMARY_TRANSLATIONS` (optional, default
  documented above).
- Corpus load grows from ~0.94 s to ~1.4 s and one selection of a
  non-primary translation opens a second short-lived DB connection for the
  rendering (the pipeline's own connection is already closed by then).
- The catalogue is corpus-derived: activating a translation in the database
  publishes it only after the corpus cache expires or is cleared.
- A translation with no Psalm map, or covering nothing, is dropped from the
  catalogue with a warning instead of failing the corpus load.
- New public `fallback_reason` values `coverage_empty` and `ranking_empty`
  (see above); both need announcing to the mobile client.

### Why a missing Psalm map drops the whole translation

A versification map is needed for Psalm windows only: every other book is
an identity conversion, and a translation without a map could in principle
still be served the ~92 % of the corpus that is not the Psalter. It is
nevertheless dropped whole, and on purpose. The map is what makes a Psalm
window mean the same words in a Septuagint- and a Masoretic-numbered
translation (ADR 0003), so its absence is not a gap in one book — it is a
signal that the translation has not been through the versification build at
all, and the safe default for a grounded endpoint is to publish nothing
rather than a silently reduced Bible whose reduction no client can see.
Every active translation has a verified map today, so the branch is a
guard, not a behaviour. Softening it (serve the non-Psalm part, mark the
translation partial in the catalogue) is possible future work, and it needs
a catalogue field before it is worth doing.

### Measured effect on quality (npu simulation, 2026-08-27)

`evaluation/retrieval_benchmark.py pipeline --rerank --coverage-translation
21` — the uk candidates restricted to what `npu` renders, ru/en untouched
(their cached rewrites, embeddings and rerank answers stay valid; the run
cost 6 fresh rerank calls, results in
`bench_data/results_adr0007_npu_coverage.json`):

| metric | baseline (ubh) | npu-restricted |
|---|---|---|
| uk hit@10 | 1.000 | 0.857 |
| uk recall@10 | 0.679 | 0.510 |
| uk MRR | 0.619 | 0.600 |
| ALL hit@10 / recall@10 / MRR | 1.000 / 0.781 / 0.664 | 0.958 / 0.740 / 0.658 |
| final top-1 relevant / rel+acc | 0.917 / 1.000 | 0.917 / 1.000 |
| final top-1 unacceptable | 0.000 | 0.000 |
| sensitive relevant / unacceptable | 1.000 / 0.000 | 0.857 / 0.000 |
| sensitive relevant-or-acceptable | 1.000 | 1.000 |

**Neither column is a clean before/after, in two different ways.**

The RETRIEVAL rows of the baseline come from the saved runs
`bench_data/results_v050_flashlite_p6/p9.json`, which were scored against
`scenarios.json` 0.5.0 — the version BEFORE the fourth grading package that
is in HEAD (0.6.0). There is no baseline run scored on the current
reference. The size of that alone is visible inside the npu run itself: its
ru and en entries in `details` (queries and top-10) are **byte-for-byte
identical** to the baseline's — the filter only ever touched uk — and their
metrics still differ, en recall 0.752 -> 0.776 and ru 0.872 -> 0.877. That
is the reference set gaining references, not this ADR. (The earlier line
"ru, en: every metric identical" claimed the opposite and was wrong.)

The FINAL TOP-1 rows of both columns are now recounted against the current
reference (`scenarios.json` 0.7.0) over the same set: version 0.7.0 grades
the two previously-ungraded npu top-1 choices — uk-005 (John 14:15-31, the
promise of the Comforter, "I will not leave you as orphans") and uk-007
(Psalm 128:1-6, the blessing of family and children) — both `relevant`. All
24 baseline and all 24 npu top-1 are now graded; the earlier
22-graded-of-24 denominator mismatch no longer applies.

Thresholds on the npu simulation, evaluated against `thresholds.json` 0.3.0's
new `final_top1_coverage_restricted` section (approved by Мария, 2026-08-28,
delegated to the orchestrator — same zero-unacceptable and
`sensitive_relevant_or_acceptable_share_min = 1.0` as the main section, but
`sensitive_relevant_share_min` relaxed to 0.8 because a narrowed pool
structurally has fewer Old-Testament candidates for sensitive scenarios):

- `sensitive_relevant_share_min` = 0.8 — the run gives **0.857 (6 of 7
  graded sensitive scenarios): PASS** (uk-002 remains the one acceptable
  outlier; baseline 7/7);
- `sensitive_relevant_or_acceptable_share_min` = 1.0 — **1.000 (7/7): PASS**;
- `unacceptable_share_max` / `sensitive_unacceptable_share_max` = 0.0 —
  **0.000 / 0.000: PASS**;
- `relevant_share_min` = 0.70 — **0.917 (22/24): PASS**;
- `relevant_or_acceptable_share_min` = 0.95 — **1.000 (24/24): PASS**;
- `ungraded_review_required` — **0 ungraded top-1: gate CLOSED**.

All thresholds pass. The open question is closed: `npu`'s coverage-restricted
run is acceptable under the dedicated threshold section, and `npu` is
included in the renderable-translation catalogue on the same footing as the
other coverage-restricted translations (`bti`, `webus`, `webbe`). This ADR
now closes it (previously: "a product-owner decision is expected"; decided
by Мария 2026-08-28, delegated to the orchestrator).

Nothing unacceptable was ever chosen, on any scenario or any fallback
policy. Only uk-002 is a genuine degradation, explained by npu having no Old
Testament outside the Psalms: it re-decides on a shorter list and answers
Psalm 121:7-8 (graded `acceptable` in that scenario) instead of Psalm 46
(`relevant`) — Psalm 46 is still a candidate, so this is rerank variance on
a changed list, not a coverage gap. uk-007 is not a degradation: npu simply
has no access to its reference passages in Proverbs and Deuteronomy outside
the Psalms, so the rerank reasonably lands on Psalm 128:1-6 instead — and
grading it in 0.7.0 confirmed it as a `relevant` answer in its own right,
not a compromise.

The run predates the reference-chunk filter and used the 1167-window npu
set; the corrected set is 1163. None of the four removed windows appears in
the top-10 of any uk scenario of that run, so its numbers stand as
measured.

The run also predates OQ1's safe-pool growth: it was made against the
6-place `data/safe_pool.json`, and the pool is now 9. That does not shift
the numbers above either — none of the three added places (Psalm 121:1-8,
Philippians 4:1-9, Matthew 6:25-34) coincides with the reference links of
the empty-topic scenarios (ru-009, en-006, uk-006), and `rotate_safe_pool`
returns pool indices in file order with the new entries appended at the
end, so the rank of every pre-existing place is unchanged (verified during
review verification, 2026-08-28).

## Open questions

1. CLOSED (Мария, 2026-08-28). The safe pool used to shrink from 6 places to
   5 for `npu` (Lamentations 3:22-23 is not there). `data/safe_pool.json` is
   now version 1.1.0 with three added places that every active translation
   carries — Psalm 121:1-8, Philippians 4:1-9, Matthew 6:25-34 — so the pool
   is 9 places and `npu` resolves **8 of 9**, every other translation 9 of 9.
   Romans 8 and 2 Corinthians 1 were considered and not taken.
2. The rerank prompt still reads the index-order passage
   (`candidate.passages[0]`) rather than the configured primary. They are
   the same translation for the current corpus, and a second indexed
   translation per language would separate them, so the two places that
   depend on it are now explicit rather than implicit:
   - the reference-chunk filter is built from the index-order translation,
     which is by definition the text the AI read;
   - `resolve_primary_translations` logs a warning when the configured
     primary is not that translation, and `_render_target_passage` REFUSES
     (documented 503, category-only log) to render a translation from its
     own range when the chosen candidate was judged in some other one —
     silently rendering an own range nothing verified is exactly the failure
     mode this question is about.
   Making the prompt follow the configuration is still a separate change: it
   would alter the input of the benchmarked rerank stage (ADR 0005).
3. A window whose own canonical range a translation covers only partially
   (48 of them for `bti`) is excluded entirely. Rendering the covered part
   would be possible but would return a range that does not match the
   canonical ID the client stores — rejected.
