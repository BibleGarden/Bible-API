# Adding a new language

Every place a human language is named, enumerated or spelled into the code, as
a checklist. ClickUp 86cbegn16, under the local-models umbrella 86cbegfzt.

The app is meant to be usable by anyone in the world, so languages will be
added. Three exist today: `ru`, `en`, `uk`. Nothing in the codebase discovers a
fourth on its own — every item below was found by reading the file it names,
and each carries the path (and the constant, function or column) so a person
who does not know the project can go and look.

## How to read this document

* **Paths without a repository prefix are relative to the Bible-API repository
  root** (`app/safety.py` = `Bible-API/app/safety.py`). Cross-repository paths
  carry the directory name (`Dashboard-API/app/data.py`,
  `bible-parser/const.php`, `Deploy/env-checklist.md`) and follow the monorepo
  `CLAUDE.md` table of repositories.
* Line numbers are as of **2026-09-05** (Bible-API `main` at `ce06bbb`,
  Dashboard-API / Dashboard-Web / bible-parser / Architecture / Deploy at their
  same-day heads). They drift; the constant/function names next to them do not.
* Two different operations are constantly confused, and this document keeps
  them apart:
  * **adding a language** — a new row in `languages`, plus everything in this
    checklist;
  * **adding a translation of a language that already exists** — the ordinary
    import flow (`Deploy/data-flow.md`), plus a chunking/index/versification
    pass. Only layers 1, 4 and 11 apply.

## Scope note: the AI-provider layer is in flux (2026-09-05)

This document describes the code **as it stands on 2026-09-05**, and one layer
under it is being replaced right now:

* the chat stages (`/api/ai/question`, the scripture rewrite and rerank) and
  speech transcription already run on the company server — Qwen3-30B and
  Whisper `large-v3-turbo` behind OpenAI-compatible APIs, selected per stage by
  `AI_*_PROVIDER` (`app/config.py:140-229`, ADR 0009, ADR 0012);
* **embeddings are still `BAAI/bge-m3` in this process** (`app/embeddings.py`,
  `EMBEDDING_PROVIDER=local`, ADR 0010) and are about to move to the same
  server over an API — **ClickUp 86cbehd6h, in progress**.

Rows marked **"revisit after 86cbehd6h"** are the ones that talk about who
computes the embeddings. Their *content* (which languages the model covers,
that the index version changes when the model does) does not change with the
transport; where the vectors are produced does.

Nothing else in this document depends on the provider. That is the point of the
despair rule (layer 6): it is code precisely so it cannot move with a model.

---

# The checklist

## Layer 1 — Data and schema (`cep_admin` → export → `cep_public`)

**1.1 — Add the row to `languages`.** Table is `(alias VARCHAR(10) PK, name_en
VARCHAR(255), name_national VARCHAR(255))` —
`Dashboard-API/migrations/2025_07_23_231232_initial_database_structure.sql:37-42`.
`alias` is the primary key *and* the value every other layer calls "the
language". Write it in `cep_admin` only; `cep_public` is derived
(monorepo `CLAUDE.md`, "Потоки данных").
*Verify:* `GET /api/languages` on the admin API returns it
(`Dashboard-API/app/main.py:144-159`).

**1.2 — `ALTER TABLE bible_books`: add `short_name_XX` and `full_name_XX`.**
The book-name reference has **one column pair per language**, not a per-language
table: `short_name_en`, `short_name_ru`, `short_name_uk`, `full_name_en`,
`full_name_ru`, `full_name_uk` —
`Dashboard-API/migrations/2025_07_23_231232_initial_database_structure.sql:19-24`.
A fourth language is a **schema migration**, plus 66 book names to fill in.
Write a new file under `Dashboard-API/migrations/` (naming per
`Dashboard-API/migrations/README.md`).
*Consumers to update in the same change:* `bible-parser/const.php:952-959`
(`get_all_bible_books()`, declared `:929`, reads the three pairs by name),
`evaluation/retrieval_benchmark.py:1576-1585`, `evaluation/trace_picker.py:659`.
The excerpt lookups are **no longer consumers**: `short_name_*` left both
`WHERE` clauses on 2026-09-05 (see 3.5).
*Verify:* `Dashboard-API/tests/seed_test_data.sql:9` inserts `bible_books`
column by column and will fail on a missing NOT NULL column — run the
Dashboard-API test suite.

**1.3 — Nothing to do for the export.** `Dashboard-API/app/data.py:402` and
`:647` both do `SELECT * FROM languages` — the manifest and the payload carry
the whole reference table, columns included. A new language row and new
`bible_books` columns ride along with no code change.
*Verify:* `GET /api/data/manifest` lists the new alias under `languages`.

**1.4 — Nothing to do for the import.** `app/import_data.py:145`
(`REFERENCE_TABLES = ['languages', 'bible_books']`) writes both with
`REPLACE INTO` (`:611-619`) before any translation, and `:1330` states
explicitly that a **point** import can introduce a new language. The orphan
sweep at `:1217-1226` only deletes a `languages` row that the manifest dropped
*and* nothing points at.
*Verify:* `GET https://api.bible.garden/api/import?translation=<alias>` returns
`status=="ok"` (check the field, not only HTTP 200 — monorepo `CLAUDE.md`).

**1.5 — At least one translation of the language must exist** in `translations`
(`translations.language` is a FK to `languages.alias`,
`Dashboard-API/migrations/2025_07_23_231232_initial_database_structure.sql:114`;
widened and re-added in `…/2026_02_08_104500_align_languages_translations_charset_with_dump.sql`).
Everything from layer 3 onward is empty until this exists. Adding it is the
ordinary translation flow — `Deploy/data-flow.md`.

---

## Layer 2 — bible-parser (text + audio pipeline, PHP)

This repo is where a language stops being data and becomes code. It has a
**closed set of three** and throws on the fourth.

**2.1 — `bible-parser/NumberFormatter.php`: the hard stop, in three places.**
`getChapterPrefix()` (`:164-186`) and `getChapterName()` (`:189-215`) branch on
`'ru'` / `'uk'` / `'en'` and end with
`throw new InvalidArgumentException("Unknown language: $language (wait one of: ru, uk, en)")`
(`:185`, `:214`). The ordinal tables they call live in
`private static $numerals` (`:6`, blocks at `:7` ru, `:23` uk, `:39` en), keyed
by the same three codes, with gendered forms for ru/uk — and
`getOrdinalNumber()` throws its **own** exception on a missing block
(`:97-98`, "Неподдерживаемый язык"), plus a gender-less special case for `en`
(`:104`, `:131`). A new language needs a numerals block, two new branches, and
a decision about gender.
*Verify:* run the chapter-prefix path of the voice pipeline for the new
translation; the exception is the failure mode.

**2.2 — `bible-parser/const.php`: a fourth `$base_XX_template`.**
`$base_ru_template` (`:493`), `$base_en_template` (`:569`), `$base_uk_template`
(`:645`) are three hand-written arrays of all 66 book names, used as the
chapter-prefix fallback text; the `switch($voice)` at `:722-923` patches
individual entries and dispatches to one of the three.

**2.3 — `bible-parser/const.php`: the voice entry.** `get_voice_info()`
(`:47-364`) is a `switch($voice)` with one array per voice, carrying
`'mfa_lang'` (e.g. `:144` `'en_us'`) — the forced-alignment dialect code.

**2.4 — `bible-parser/alignment/timecodes_mfa.class.php`: an MFA model set.**
`getMfaModelsConfig()` (`:44-67`) maps `ru` / `en_us` / `en_uk` / `uk` to MFA
dictionary + acoustic + g2p model names and versions.
`get_models_by_language()` (`:2383-2400`) resolves the dialect from
`mfa_lang` and **`die()`s** with `"Undetermined models for language: $lang"`
when the language is absent. A new language needs a published MFA model set —
this is a real external constraint, not a config line.

**2.5 — `bible-parser/docs/translations.md`: a new `## XX` section.** The file
is organised as `## RU` (`:102`), `## EN` (`:190`), `## UK` (`:327`) — per
translation: source site, parsing script, voices, commands.

---

## Layer 3 — Public API (Bible-API)

**3.1 — `GET /api/languages` — nothing to do.** `app/main.py:259-274` reads
`SELECT alias, name_en, name_national FROM languages`; the response model is
`app/models.py:7-10` (`LanguageModel`), free-form strings.

**3.2 — `GET /api/translations?language=` — nothing to do.**
`app/main.py:278-304`, `WHERE t.language = %s`, no enum.

**3.3 — `GET /api/about` — add a label, a subtitle and a description in the new
language.** `app/about.py` keeps per-language dicts for **three** contacts
(telegram `:12`, github `:28`, website `:44`), each with a `label` and a
`subtitle` map — 6 dicts, 18 strings, `:16-58` — plus the Bible Garden
`about_text` (`:60-63`) and the Lampada branch
(`:84` `("en", "ru", "uk")` comprehension, `:86-88` `about_text`). The clients
read these by language key, so a missing key is a missing string in the app.
Note the Bible Garden `about_text` **names the three languages in prose**
("почему именно эти языки") — adding a fourth makes that paragraph wrong in all
three existing texts.
*Verify:* `tests/test_about.py:25` asserts the Lampada subtitle map is exactly
`("en", "ru", "uk")` — it will fail until updated.

**3.4 — `GET /api/version-check` — add both messages.**
`app/version_check.py:18-29`, `MESSAGES["soft"]` and `MESSAGES["hard"]`, one
string per language. (The endpoint currently returns the whole dict and lets
the client pick — `:62-71`; the Lampada branch rewrites the product name in
every language at `:63-64`.)

**3.5 — The book alias is Latin, and that is the contract** (settled by Maria
on 2026-09-05, ClickUp 86cbehfqx — option (a) of the former open decision 5).
**Nothing to do for a new language.**

The app never lets a human type a book name: it copies the alias out of
`GET /api/translations/{code}/books`, which publishes `bible_books.code1`. So
the excerpt grammar stays Latin, and the `short_name_*` columns are not
addresses at all — they are display names.

What the ticket changed, in both APIs:

* the **excerpt grammar** — `app/excerpt.py`, `EXCERPT_PATTERN` — now accepts
  `[A-Za-z0-9]+` as a **whole token** and folds its case before the lookup, so
  `gen 1:1`, `Gen 1:1` and `GEN 1:1` are one book. It used to be
  `(?P<book>[0-9a-z]+)` with no boundary, which is why `Gen 1:1` matched the
  substring `en` and answered `Book with alias 'en' not found` (verified live
  on 2026-09-05, both APIs). A book name in another script still matches
  nothing and is a `422` — one that now names the expected format;
* the **lookup itself** — `app/excerpt.py`, `get_books_info` — matches
  `bb.code1..code5` and nothing else. `bb.short_name_en` / `bb.short_name_ru`
  were **removed on 2026-09-05**: the Russian one was unreachable behind the
  Latin-only grammar, and the English one made 14 books answer to a second,
  undocumented name (`mt`, `jn`, `ex`, `1kings`, …). Verified on the live
  `cep_public` and `cep_admin` before removing them: no value of
  `code1..code5` belongs to two different books, case-insensitively.

`short_name_uk` was therefore never the missing half of anything: a language
whose book names are not Latin needs **no** column in this `OR` chain, because
its readers address books by the same catalogue aliases everyone else does.

**3.6 — `POST /api/ai/scripture`: grow the `language` enum.**
`app/scripture_select.py:155-158`:

```python
class Language(str, Enum):
    ru = "ru"
    en = "en"
    uk = "uk"
```

This is the OpenAPI enum of the public request body
(`SelectRequest.language`, `:199-203`). Until a member is added the endpoint
answers 422 for the new language whatever else is configured.
*Verify:* regenerate the spec with `extract-openapi.py` and check the enum;
`tests/test_scripture_select.py` covers the endpoint.

**3.7 — `app/retrieval_cli.py:156` — `--language` `choices=("ru", "en", "uk")`**
(operator CLI, same enum by hand).

---

## Layer 4 — Retrieval corpus and index

Everything here is keyed by **translation** and grouped by **language**; the
language itself is carried on `translations.language`. There is no per-language
tokenizer, stemmer or stop-word list anywhere.

**4.1 — Chunking: nothing language-specific.** `app/chunking.py` contains no
language branch at all (grep for `language` returns nothing); it works on verse
and title structure. Run it for the new translation:
`python app/chunk_cli.py --translations <alias> [--pivot <alias>]`
(`app/chunk_cli.py:10`). The pivot defines the canonical boundary plan
(ADR 0001, `architect/adr/0001-structural-chunking.md`).

**4.2 — Psalm versification: add the translation's scheme.**
`app/versification.py:68-76` — `TRANSLATION_SCHEMES` maps **translation alias →
`septuagint` | `masoretic`**. `build_psalm_map()` raises
`"Unknown Psalm numbering scheme for translation '<alias>'; add it to
versification.TRANSLATION_SCHEMES"` (`:167-172`), and
`app/versification_cli.py:82-86` refuses the whole build listing the unmapped
aliases. Per-verse exceptions go in `EXCEPTIONS` (`app/versification.py:84-86`).
ADR 0003 (`architect/adr/0003-psalm-versification-canon.md`) is the canon
decision.
*Verify:* `python app/versification_cli.py build --translations <alias>` then
`… verify`; `tests/test_versification.py`.

**4.3 — Canon exceptions, if the translation divides books differently.**
`app/canon.py:142-144`, `TRANSLATION_CHAPTER_COUNTS`, keyed by
`(alias, book_number)` — the existing entry is `("ubh", 39): 3` (Ukrainian
Malachi). Nothing else in `app/canon.py` is per language: `CANONICAL_BOOKS`
(`:57`) is the 66-book canon.

**4.4 — Embeddings: check the model covers the language.**
`app/embeddings.py` — `LocalEmbeddingClient` runs **BAAI/bge-m3** on CPU in this
process (`:250-260`, ADR 0010). bge-m3 is a multilingual model (it is the
"m3" — multi-lingual, multi-granularity, multi-functionality checkpoint); its
published training covers ~100 languages, which is a claim to **verify against
the candidate language before promising anything**, not a guarantee to quote.
The index version string embeds the model and dimension —
`app/vector_index.py:14` shows the shape `c3:BAAI/bge-m3@1024`; changing the
model changes the version and therefore every stored row
(`app/index_cli.py:27-32`, `--drop-other-versions`).
*Build:* `python app/index_cli.py rebuild --translations <alias>`.
**Revisit after 86cbehd6h** — the client moves behind an API; the language
question stays exactly the same, asked of whatever model the server runs.

**4.5 — BM25 index: nothing to add, but know what it is.**
`app/lexical_index.py:27-34` tokenises with `re.compile(r"\w+", re.UNICODE)`
and lower-cases — **no stemming, no stop words, no per-language rules**.
`build_lexical_indexes()` (`:95-115`) groups `translation_chunks` by
`t.language` and builds one BM25 index per language. A new language gets an
index automatically once its chunks exist. Note the consequence: for a
morphologically rich language the lexical signal is weaker than for English,
and ADR 0010's measurement (`evaluation/README.md`, "Локальные эмбеддеры")
found BM25 is carrying much of the retrieval quality — worth measuring per
language rather than assuming.

**4.6 — Retrieval filters: nothing to do.** `app/retrieval.py` filters by
language throughout (`_vector_rows` `:951-958`, `_language_chunks` `:992-996`,
`lexical_indexes.get(language)` `:922`) and `app/vector_index.py:188-196`
masks the matrix by `language`. All data-driven.

**4.7 — Coverage sets and non-primary rendering: nothing per language.**
`app/passage_render.py:6-9` — chunks, embeddings and BM25 live in the primary
translation of each language, and any active translation of the same language
can be *served* without being indexed (ADR 0007).

**4.8 — The two curated data files are language-independent — do not
translate them.** `app/data/genre_blacklist.json` and `app/data/safe_pool.json`
are keyed by **canonical coordinates**, not language: both declare
`coordinate_system` = `cep_public.bible_books.number` + `english-masoretic`
psalm numbering. `genre_blacklist.json` has 25 entries over 4 genres,
`safe_pool.json` 9 entries. They apply to every language as they are.

**4.9 — `AI_SCRIPTURE_PRIMARY_TRANSLATIONS`: name the new language's primary
translation.** Format `ru=syn,en=bsb,uk=ubh` — parsed by
`app/scripture_select.py:632-651`, resolved in `_primary_translations()`
(`:658-718`). An entry naming a translation that is not indexed for that
language is **ignored with a warning** (`:695-697`), as is a language with no
index (`:713-717`); without an entry the default is the indexed translation
with the lowest code. Documented in `app/config.py:1073-1082` and listed in
`Deploy/env-checklist.md:218-221` (renamed from `SCRIPTURE_PRIMARY_TRANSLATIONS`
on 2026-08-30, `Deploy/env-checklist.md:277`).

---

## Layer 5 — Prompts of the scripture-selection stages

**5.1 — `app/query_rewrite.py:86-104`, `_LANGUAGES`: add a name + a register
hint.** Each entry is `(English language name, a sentence naming the classical
translation whose vocabulary the rewrite should use, with 3-4 example
phrases)`. `SUPPORTED_LANGUAGES = tuple(_LANGUAGES)` (`:107`) is derived — the
rewriters raise `QueryRewriteError(f"unsupported language: {language}")` for
anything else (`:698-699`, `:834-835`). This is the gate the whole selection
pipeline hits first.

**5.2 — `app/query_rewrite.py:145-295`, `_EXAMPLES`: six de-fingerprinted
worked examples.** The prompt v8 few-shot block shows **all** languages to the
model on purpose (`render_examples()` docstring, `:311-316`), iterating
`_EXAMPLE_LANGUAGE_ORDER = ("ru", "en", "uk")` (`:296`). A new language needs
its own examples, and they must satisfy the de-fingerprinting rule
(`:120-143`): **not one example topic and not one example passage may coincide
with anything in `evaluation/scenarios.json`**, or the prompt measures itself.
*Verify:* `tests/test_rewrite_prompts.py` — `LANGUAGES = ("ru", "en", "uk")`
(`:60`) must grow, and these tests enforce the rule against the live dataset:
`test_example_topics_do_not_appear_in_the_dataset` (`:87`),
`test_example_passages_do_not_touch_any_reference` (`:102`),
`test_example_book_codes_are_canonical` (`:78`),
`test_example_queries_carry_no_digits_and_no_book_names` (`:123`),
`test_the_production_prompt_ends_with_the_language_reminder` (`:227`).

**5.3 — Rerank: nothing per language.** `app/passage_rerank.py` asks for
`"reason": "<short English sentence>"` (`:184`, `:201`) — a server diagnostic,
never shown to a person — and the answer is otherwise **indexes only**
(candidate number + verse span, `:239-264`). The passage text it reads is in
whatever language the candidates are. Nothing to add; the open question is
whether a model reasons as well over a language it was less trained on, which
is a measurement (layer 8), not a code change.

---

## Layer 6 — `POST /api/ai/question` and the despair rule

This is the layer where getting it wrong is not a quality problem. Read
`architect/twinkler-ai.md`, "The despair rule is code, not an instruction"
(ClickUp 86cbegg23), before touching anything here.

**6.1 — `app/question_prompt.py:109`, `LANGUAGE_NAMES`: add the English name.**
`{"ru": "Russian", "uk": "Ukrainian", "en": "English"}`. The system prompt
names the answer language twice (`:124-127`, `:153`) from this map. An unknown
language falls back to `UNDETERMINED_LANGUAGE = "exactly the language of the
person's message"` (`:115`) — v1 behaviour, the model detects it itself. So a
missing entry degrades quietly rather than failing: **that is exactly why this
item must be ticked deliberately.**

**6.2 — The register sentence assumes ru/uk.** The prompt says "Where the
language distinguishes registers, use the informal, intimate one **(Russian ty,
Ukrainian ty)**" (`app/question_prompt.py:127-128`) and "In inflected languages
such as Russian and Ukrainian…" (`:148`). A new language with a T/V
distinction (French, German, Polish, Spanish…) needs its own parenthetical, and
that is a **prompt change → bump `QUESTION_PROMPT_VERSION`** (`:104`, currently
3).

**6.3 — The stage instructions are Russian regardless of the prayer's
language.** `app/question_prompt.py:172-211` — `FIRST_INSTRUCTION` (`:185`),
`NEXT_INSTRUCTION` (`:197`), `REFLECT_INSTRUCTION` (`:207`), and the headers
around them (`:193-194`, `:205-206`), are
Russian text sent for every language. The docstring (`:82-88`) records this as a
**deliberate decision with a stated exit**: the person's own words carry the
language, the system prompt names it, and the instruction language was measured
never to leak into the answer — "translating the blocks per language is the
change to make, and it is a change, so it needs a version". Treat this as an
open decision (see the end of this document), not as a to-do you may silently
skip.

**6.4 — `app/safety.py:94`, `SUPPORTED_LANGUAGES = ("ru", "uk", "en")`.** The
despair detector's language set. `DEFAULT_LANGUAGE = "en"` (`:93`).

**6.5 — `app/safety.py:101-119`, `SAFETY_REPLIES`: write the fixed crisis reply.**
Two sentences, **no question mark anywhere**, informal register, **no hotline
number** (the app is worldwide — it points at someone close first and at
emergency help generically). This is the text a person in crisis receives when
the model is never called. It is a reviewed code constant, deliberately not an
environment variable (ADR 0008 reasoning, module docstring `:70-76`).
**Bump `SAFETY_REPLY_VERSION`** (`:91`, currently 2).
*Verify:* `tests/test_safety.py:602-613` pins the sha256 of the joined replies —
it fails until the hash is updated together with the version bump;
`:587-588` asserts `set(SAFETY_REPLIES) == set(SUPPORTED_LANGUAGES)`;
`:593` asserts no reply contains a question mark.

**6.6 — `app/safety.py:199-227`, `detect_language`: decide the detection
strategy. This is the trap.** Today: alphabet first (`_CYRILLIC_RE` `:178`,
`_LATIN_RE` `:179`), distinguishing letters second (`_UK_LETTERS_RE` `:180`
`[іїєґ]`, `_RU_LETTERS_RE` `:181` `[ыэъ]`), a function-word vote as tie-break
(`_UK_WORDS` `:187`, `_RU_WORDS` `:192`), `None` when the text does not say.

> **Any Latin-script message currently returns `"en"`** — unconditionally,
> at `app/safety.py:212-215` (`if cyrillic <= latin: return "en"`), with the
> comment "en is the only Latin-script
> language this endpoint answers in, so it is the answer rather than a guess at
> another one". The moment a second Latin-script language is added (Spanish,
> Polish, Portuguese, Indonesian…), that line is **wrong**: a Spanish prayer
> resolves to `en`, gets the English prompt, and — worse — gets the **English**
> fixed reply, while the Spanish despair patterns can still fire on it through
> `_scan`'s `detect_language(text) or pattern_language` fallback (`:645`).
> A Latin-script language therefore **requires replacing this branch**, not
> extending a list. That is a design decision, not a patch (see open decisions).

Adding a Cyrillic-script language is the cheaper case: a distinguishing-letter
regex plus a function-word set, with the rule stated at `:182-186` — **every
function word must exist in one language and not in the other, or it votes for
the wrong one**.
*Verify:* `tests/test_safety.py:565` (`test_language_detection`), `:569`, `:577`.

**6.7 — `app/safety.py:301-…`, `EXPLICIT_PATTERNS` (tier 1) — 21 patterns today,
7 per language.** An explicit statement of not wanting to live, wanting to die,
ending one's own life, or self-harm. **The model is never called**; the fixed
reply is returned. See the validation section below.

**6.8 — `app/safety.py:467-…`, `WEAK_PATTERNS` (tier 2) — 26 today (ru 9,
uk 9, en 8).** Despair-shaped wordings with a plausible ordinary reading. Tier 2
fires **only in conjunction with a question mark in the model's reply**
(`check_reply`, `:659-670`). Because every ordinary reply on this endpoint
contains `?`, a tier-2 pattern that matches ordinary speech would silently
replace *every* answer to it — so the set must stay narrow phrases, never
loaded single words.

**6.9 — The guards. Each is per language and each must be written for the new
one.** `_RU_LIVE_TAIL` (`:257`), `_UK_LIVE_TAIL` (`:266`), `_EN_LIVE_TAIL`
(`:271`) — prepositions/adverbs of place that turn "don't want to live" into a
complaint about circumstances, **plus the named faith exception** ("не хочу жить
без Бога" is a confession of faith, while `без`/`without` in general is *not* a
guard — "не хочу жить без неё" is grief at its most dangerous).
`_NOT_NEGATED` (`:278`) — "не хочу умереть" is fear of death, the opposite
signal. `_RU_DIE_TAIL` (`:285`), `_UK_DIE_TAIL` (`:289`), `_EN_DIE_TAIL`
(`:293`) — dying **to** sin is doctrine (Rom 6), and the idiom of shame
("умереть от стыда") is a closed list. `_RU_MEANING_TAIL` (`:297`),
`_UK_MEANING_TAIL` (`:298`) — "нет смысла в жизни без Бога" is a sermon.

**6.10 — `app/twinkler_ai.py`: nothing per language.** `language_source()`
(`:241-272`) picks the text whose language the answer must be in;
`question_prompt_for` (`:198-219`) turns it into the prompt. Both go through
`safety.detect_language`, so they inherit 6.6 and need no separate change.

**6.11 — `evaluation/gen_questions.py:492` and `:611`** default to `"en"` when
the source text is blank — the offline probe generator's own copy of the same
assumption.

---

## Layer 7 — Transcription (`POST /api/ai/transcribe`)

**7.1 — Check the language is in Whisper's vocabulary — otherwise nothing to
do.** `app/transcription.py:96-107`, `WHISPER_LANGUAGES` — the 100 codes every
multilingual Whisper checkpoint knows, copied from
`faster_whisper.tokenizer._LANGUAGE_CODES`. It is the vocabulary of the model
family, not of a deployment, so it is a constant and should **not** be edited to
add a language Whisper does not know.

**7.2 — `whisper_language()` (`:189-210`) needs no change.** It takes the
primary subtag of the app locale (`ru-RU` → `ru`, `zh-Hant-TW` → `zh`), maps
deprecated spellings through `LOCALE_LANGUAGE_ALIASES` (`:83-88`:
`iw→he, in→id, ji→yi, nb→no`), and returns `None` — auto-detect — for anything
outside the set. The locale is contractually a **weak hint**, so an unknown
locale degrades to auto-detection rather than an error.

**7.3 — The Gemini path's prompt is language-neutral.**
`app/twinkler_ai.py:51` (`_TRANSCRIPTION_PROMPT`, "in its original language")
and `_transcription_prompt(locale)` (`:481-487`) — the locale is injected as
"a weak hint when the spoken language is ambiguous". Nothing per language.
ADR 0012, `architect/adr/0012-speech-transcription-providers.md`.

---

## Layer 8 — Evaluation and benchmarks

**8.1 — `evaluation/scenarios.json`: at least 5 scenarios in the new language,
including one `empty`.** Today 24 scenarios: ru 10, en 7, uk 7 (v0.8.0,
`status: approved`). Scenario ids are prefixed with the language (`uk-002`).
*Verify — and these tests will fail until updated:*
`tests/test_evaluation_dataset.py:222-226` asserts the language set is exactly
`{"ru","en","uk"}` **and ≥5 scenarios per language**;
`:238-244` asserts the `empty` category is present in every language;
`:70-71` asserts the id prefix matches the scenario's language.
Then bump the minor version and set `review_status: draft` until graded
(`evaluation/README.md`, "Как дополнить", `:276-281`).

**8.2 — `evaluation/thresholds.json`: nothing per language.** v0.4.0 — the
thresholds (`retrieval_top_k`, `final_top1`,
`final_top1_coverage_restricted`) are global, not keyed by language. **Do not
change them to make a new language pass** (monorepo `CLAUDE.md`: thresholds and
`scenarios.json` are not to be moved).

**8.3 — `evaluation/retrieval_benchmark.py:69`, `LANGUAGE_CORPUS`: add the
language → (translation code, alias) entry.** `{"ru": (1, "syn"), "en": (16,
"bsb"), "uk": (20, "ubh")}` — **hardcoded numeric translation codes**.
`TRANSLATION_LANGUAGE` (`:70-72`) is derived from it; `parse_languages()`
(`:206-221`) rejects a `--languages` value outside its keys; the corpus
fingerprint (`:242-243`) and the per-language reporting (`:795`, `:282-306`)
key off it too.

**8.4 — `evaluation/check_refs_db.py:33`, `NATIVE_BASELINE`:** `{"ru": "bti",
"en": "bsb", "uk": "ubh"}` — the translation each language's reference
coordinates are checked against.

**8.5 — `evaluation/question_probe_inputs.json`: add probe inputs.** 13 today
(ru 8, en 3, uk 2), schema v2.0.0, used both by the question probe and by the
safety sweep.

**8.6 — `evaluation/check_questions.py`: its own language detector and the
informal-register rule.** This is a benchmark script and **cannot import the
application** (`app/safety.py:174-176` says so explicitly), so it carries a
parallel copy: `detect_language()` (`:166-188`) with `_UK_LETTERS` (`:73`),
`_RU_LETTERS` (`:74`), `_UK_WORDS` (`:79`), `_RU_WORDS` (`:84`); the
polite-form regex `_FORMAL` (`:96-100`) with `_FORMAL_EXCEPTIONS` (`:105`); and
**`if language not in ("ru", "uk"): verdict["informal"] = None`** (`:227-228`)
— the register rule is skipped for anything else. A new language with a T/V
distinction must be added there or its register goes unchecked. The forbidden-move
substring lists (`_FORBIDDEN`, `:111-139`; `_ADVICE_MODALS` `:140`;
`_SUPPORT_MARKERS` `:142`) are **per language too** and currently hold ru/uk/en
phrases only.

**8.7 — `evaluation/check_rewrites.py`** checks the answer language of every
rewrite variant against the scenario's language using
`check_questions.detect_language` (`:94`, `:147-172`) — it inherits 8.6.

**8.8 — `evaluation/transcribe_bench.py`: `LOCALES` (`:75`,
`{"ru": "ru-RU", "uk": "uk-UA", "en": "en-US"}`), the `SAMPLES` table (`:94-108`,
5 passages per language with translation + voice aliases and expected
durations), and the reporting loop `for language in ("ru", "uk", "en", "all")`
(`:702`).** (There is no `transcribe_samples.json`; the samples are this Python
table.)

**8.9 — `evaluation/description_prompts.py:81-84`** — `LANGUAGE_NAMES =
{"ru": "Russian", "en": "English", "uk": "Ukrainian"}` for the
corpus-description prompt, and `build_description_instruction()` **raises**
`ValueError(f"unsupported language: {language}")` on anything else
(`:115-116`) — a hard gate, not a fallback. `evaluation/gen_descriptions.py`
inherits it (`:548`, `:725`; one language per batch is enforced at `:321-329`).

**8.10 — The ru-only offline tools.** Two benchmark drivers are pinned to
Russian by construction and are **not** made multilingual by adding a language:
`evaluation/trace_picker.py:157` (`LANGUAGE = "ru"`) with `:659` reading
`short_name_ru` only, and `evaluation/local_picker.py:212-213`
(`LANGUAGE = "ru"`, `TRANSLATION, ALIAS = rb.LANGUAGE_CORPUS[LANGUAGE]`) with
a hardcoded `"language": "ru"` in its report at `:720`. Point them at the new
language by editing the constant, or leave them; either way know they are not
covering it. `evaluation/diagnose_pipeline.py:52`, `:60` reads
`rb.LANGUAGE_CORPUS` per scenario and so inherits 8.3 with no edit of its own.

**8.11 — `evaluation/rewrite_prompts.py`** indexes the application's
`query_rewrite._LANGUAGES` directly (`:227-229`, `:255-264`) — a `KeyError` for
a language missing from 5.1. Nothing to add here once 5.1 is done.

**8.12 — Grading: who reviews.** `evaluation/README.md:240-255` — the reviewer
reads the scenario **in the scenario's language**, opens each reference in that
language's translation (with the psalm mapping), grades on the 3-grade scale
and **must fill the free-text "why"**. Maria grades Russian; on 2026-09-05
(86cbedtf8) she graded the Russian top-1 pairs and **explicitly could not grade
the en/uk pairs** (`evaluation/README.md:40`, `:3010`, `:3107` — "прямо
запрещало домысливать grade для en/uk"). So: **a new language needs a named
native reviewer before its scenarios can leave `review_status: draft`**, and
ungraded top-1 pairs go to Maria as a list — never self-graded (monorepo
`CLAUDE.md`).

---

## Layer 9 — Admin surface (Dashboard-API, Dashboard-Web)

**9.1 — Dashboard-API: nothing to change.** No `Literal[...]` or enum of
language codes anywhere; `Dashboard-API/app/models.py:9-12` mirrors the DB
columns, `TranslationModel.language` (`:29`) and `TranslationInfoModel.language`
(`:47`) are plain `str`, and the admin `PATCH` that sets a translation's
language (`Dashboard-API/app/main.py:461-463`) validates only against the FK.

**9.2 — Dashboard-Web: nothing to change.** No i18n bundle, no
`SUPPORTED_LANGUAGES`. Languages are fetched
(`Dashboard-Web/src/composables/useApi.ts:98-112` → `getLanguages()` in
`src/services/api.ts:118-120`), display names are looked up by alias with a
fallback to the raw code
(`Dashboard-Web/src/Components/BibleVoices.vue:258-260`), and filter options are
derived from the voices actually present (`:316-319`). Two **UI date locales**
are hardcoded and are unrelated to content language:
`Dashboard-Web/src/Components/AlignmentTasks.vue:371` (`'ru-RU'`) and
`Dashboard-Web/src/Components/ApiStats.vue:183` (`'en-GB'`).

---

## Layer 10 — Mobile app and stores (outside these repositories)

**10.1 — Out of scope for this repository; one pointer only.** The mobile app
is not cloned here — monorepo `CLAUDE.md` ("Субагенты"): *Twinkler-Mobile (repo
`pray`) is not cloned locally; mobile development happens on Maria's laptop and
tasks reach it through ClickUp*. `Architecture/architecture.md:112-117` lists
the iOS app repo separately. UI localisation, store listing texts, privacy
texts and the Google Play "report this answer" requirement (ClickUp 86cbe4e56)
live there. **Do not invent the file list** — open a ClickUp task against the
app repo and let its owner enumerate it.

---

## Layer 11 — Ops and deploy

**11.1 — Chunking and index building happen LOCALLY first.** Order:
`app/chunk_cli.py` → `app/versification_cli.py build` → `app/index_cli.py
rebuild`, against `cep_admin`/the local corpus. Only then does the index reach
production, because since ClickUp 86cbegwr9 **the import carries the RAG index**
(`CLAUDE.md`, "The import carries the RAG index (ClickUp 86cbegwr9)").

**11.2 — Import the new translation the standard way.**
`GET https://api.bible.garden/api/import?translation=<alias>`; check
`status=="ok"`. A resync that would **remove** a translation needs an explicit
`?allow_removals=1` — never add it reflexively (monorepo `CLAUDE.md`). Back up
affected tables to `/root/db-archives/` with the ticket number in the file name
before anything that changes production data.

**11.3 — `.env`: `AI_SCRIPTURE_PRIMARY_TRANSLATIONS`** — see 4.9. It is the only
environment variable that names languages. Listed in
`Deploy/env-checklist.md:218-221`.

**11.4 — Docs to update in the same change:** `CLAUDE.md` (this repo — the
`safety.py` bullet says "ru/uk/en dictionary + regex"),
`architect/twinkler-ai.md` ("The reply, and its version" names the three
languages), `Architecture/architecture.md:170` (the `languages` row of the
`cep_public` table list says "Languages (en, ru, uk)"), and
`Deploy/data-flow.md:28`.

---

# Validating the despair detector for a new language

The rule of layer 6 is the reason this checklist exists: on a language the
detector does not know, **tier 1 never fires**, the request goes to the model,
and the outcome is decided by whichever model is deployed — which is precisely
the failure ClickUp 86cbegg23 removed after Qwen3-30B answered an explicit
despair statement with a guiding question in **3 samples out of 3** while Gemini
obeyed the prompt. A language added without this section is a language where
the safety floor silently does not exist.

## Who validates

**A native speaker, and not the person who wrote the patterns.** The
distinctions the guards encode — a fixed idiom of death versus a statement of
intent, the doctrinal register of "die to sin", the difference between grief and
intent after "without" — are not reachable by dictionary or by a model
translating from Russian. Name the reviewer in the ticket before starting. The
same person can grade that language's `scenarios.json` (layer 8.12), and the two
jobs pair naturally.

## The minimum phrase set

Build four lists in the new language **before** writing a single regex, then
write patterns that satisfy them. Sizes below are what the three existing
languages actually carry in `tests/test_safety.py` — treat them as the floor.

1. **Explicit despair (must fire tier 1).** Statements of not wanting to live,
   of wanting to die, of ending one's own life, and of self-harm — including
   the phone-keyboard spellings (`tests/test_safety.py:317` parametrises **53**
   such phrases across the three languages, ~17 per language). Cover, at
   minimum: "I don't want to live" (and its inverted word order), "I want to
   die", "end my life" / "kill myself", "there is no point in living", "why
   live at all", "better if I had never been born", "self-harm", and the noun
   "suicide".
2. **Weak signals (must fire tier 2 only, and only against a question).**
   Wordings that are despair-shaped but have a plausible ordinary reading —
   "tired of living", "everyone would be better off without me", "I can't go on
   like this", "I'm a burden" (`tests/test_safety.py:357`, **25** phrases).
   Keep this list narrow: a false positive here silently replaces *every*
   answer to that phrasing, because every ordinary reply on this endpoint
   contains a question mark.
3. **Idioms of death and faith language (must fire neither tier).** The
   adversarial half, and the largest list: `tests/test_safety.py:441`
   parametrises **58** phrases. Cover, in the new language: the "dying of
   laughter / dead tired / killing time / this is killing me" family; "I don't
   want to live **in** this city / **with** my parents" (circumstances, not
   life); "I don't want to live **without God**" and "life has no meaning
   without God" (confession of faith — the one named `without` exception);
   dying **to** sin and being dead **to** sin (Rom 6); the hymn line
   "Take my life and let it be"; "I don't want to die" (fear, the opposite
   signal); and "I'm afraid mother will pass away" (someone else's death).
4. **Third-person grief and prayer for someone at risk.**
   `tests/test_safety.py:458` (**3** phrases) pins the accepted behaviour:
   "my son says he wants to end his life" fires tier 1 **on purpose** — the
   fixed reply is right advice for the person praying either way. Two
   consequences the new language inherits: the coverage is uneven (only
   person-free phrases match — "my friend wants to die" stays silent), and the
   new `SAFETY_REPLIES` text **must still read correctly when the person at
   risk is not the writer**.

## The adversarial probe

The independent review of 86cbegg23 ran **128 prayers of its own — 60 of
despair and 68 ordinary ones carrying death/life vocabulary — and found and
fixed 15 misses and false positives** ("умереть для греха" and "не хочу жить
без Бога" stopped firing; "не могу больше так жить", "I can't go on" and
"зачем вообще жить" started firing). Source: the ClickUp comment of 2026-09-05
on 86cbegg23. What survived is in the suite: `tests/test_safety.py`
parametrises **139** phrases over the four lists (53 / 25 / 58 / 3).
Reproduce that per language:

1. **Write the four lists above** (target ≥130 phrases total for the language),
   with the native reviewer, *without* looking at the regexes — the point is to
   attack the patterns, not to confirm them.
2. **Add them as parametrised cases** to the four existing tests in
   `tests/test_safety.py` (`:317`, `:357`, `:441`, `:458`). No new test file:
   the existing ones already assert the right things (tier, `pattern_id`
   prefix matching the language, and that tier 2 fires only against a reply
   containing `?`).
3. **Sweep the reference corpus.** The suite already asserts that **no**
   approved scenario of `evaluation/scenarios.json` and **no** input of
   `evaluation/question_probe_inputs.json` raises tier 1
   (`tests/test_safety.py:167`, `:196`), and that none loses its answer to
   tier 2 (`:185`, `:212`) — with the single documented exception `en-005`
   (`:240`). Adding scenarios in the new language (layer 8.1) automatically
   extends this sweep: **a tier-1 hit on an ordinary prayer is a hard failure,
   not a tuning parameter.**
4. **Run it against nothing else.** No model, no network — `app/safety.py` is
   dictionary and regex only, and `tests/test_safety.py:616` asserts the module
   reads no environment variable.
5. **Then bump and pin.** `SAFETY_REPLY_VERSION` (`app/safety.py:91`) and the
   sha256 in `tests/test_safety.py:602-613`.
6. **Report false positives and false negatives separately** to the reviewer,
   with the `pattern_id` that fired (never the phrase text in a log — a
   `SafetyFinding` carries the pattern id, tier and language and nothing derived
   from the message, `tests/test_safety.py:634`).

## Register check

The fixed reply must be in the **informal, intimate** register where the
language distinguishes one (`app/safety.py:97-99`), contain **no question
mark**, and name **no hotline number** — the app is worldwide. If the new
language has a T/V distinction, extend `evaluation/check_questions.py:227` so
its register is actually checked in probe runs, and add its polite forms to
`_FORMAL` (`:96-100`).

---

# Dry run: adding `uk` today

Walking the checklist as if Ukrainian were being added on 2026-09-05, to prove
the list is complete. Every "✅" was verified in the file named.

| # | Item | State for `uk` |
|---|---|---|
| 1.1 | `languages` row | ✅ (served by `GET /api/languages`) |
| 1.2 | `bible_books.short_name_uk` / `full_name_uk` | ✅ columns exist (migration `…231232`:21,24) |
| 1.3 | Export | ✅ nothing needed (`SELECT *`) |
| 1.4 | Import | ✅ nothing needed |
| 1.5 | A translation exists | ✅ `ubh` (`translations.language='uk'`) |
| 2.1 | `NumberFormatter.php` branches | ✅ `:172-175`, `:199-202`; numerals `:6` |
| 2.2 | `$base_uk_template` | ✅ `const.php:645` |
| 2.3 | Voice entry + `mfa_lang` | ✅ (`kozlov_uk`) |
| 2.4 | MFA models | ✅ `timecodes_mfa.class.php:61-65` (`ukrainian_mfa`) |
| 2.5 | `docs/translations.md` section | ✅ `## UK`, `:327` |
| 3.1-3.2 | `/api/languages`, `/api/translations` | ✅ data-driven |
| 3.3 | `/api/about` labels + subtitles + `about_text` | ✅ uk keys at `app/about.py:19,24,35,40,51,56,63,84,88` |
| 3.4 | `/api/version-check` messages | ✅ uk at `app/version_check.py:22,27` |
| 3.5 | Book-alias lookup | ✅ **out of scope by decision** (Maria, 2026-09-05, ClickUp 86cbehfqx): books are addressed by the catalogue's Latin alias in any case, in every language. `excerpt=Бут 1:1` answers `422` naming the format — the contract, not a gap |
| 3.6 | `Language` enum member | ✅ `app/scripture_select.py:158` |
| 3.7 | `retrieval_cli --language` | ✅ `:156` |
| 4.1 | Chunking | ✅ language-independent |
| 4.2 | `TRANSLATION_SCHEMES["ubh"]` | ✅ `app/versification.py:75` (masoretic) |
| 4.3 | Canon exception | ✅ `("ubh", 39): 3` — `app/canon.py:143` |
| 4.4 | Embeddings cover the language | ✅ bge-m3 is multilingual; index version `c3:BAAI/bge-m3@1024` |
| 4.5 | BM25 index | ✅ automatic (grouped by `t.language`) |
| 4.6-4.7 | Retrieval filters, coverage | ✅ data-driven |
| 4.8 | Blacklist / safe pool | ✅ canonical coordinates, no `uk` variant needed |
| 4.9 | `AI_SCRIPTURE_PRIMARY_TRANSLATIONS` | ✅ `uk=ubh` is the documented example (`app/config.py:1075`) |
| 5.1 | `query_rewrite._LANGUAGES["uk"]` | ✅ `:99-103` (register hint present) |
| 5.2 | Six worked examples + de-fingerprint tests | ✅ `_EXAMPLES["uk"]` `:243`; `_EXAMPLE_LANGUAGE_ORDER` `:296` |
| 5.3 | Rerank | ✅ nothing per language |
| 6.1 | `LANGUAGE_NAMES["uk"]` | ✅ `app/question_prompt.py:109` |
| 6.2 | Register sentence | ✅ "Ukrainian ty" named explicitly (`:128`) |
| 6.3 | Stage instructions | ⚠️ **Russian for Ukrainian prayers too** — documented decision (`:82-88`), not an omission |
| 6.4 | `SUPPORTED_LANGUAGES` | ✅ `app/safety.py:94` |
| 6.5 | `SAFETY_REPLIES["uk"]` | ✅ `:108-112`, version 2, hash-pinned |
| 6.6 | Detection | ✅ `_UK_LETTERS_RE` `:180`, `_UK_WORDS` `:187` |
| 6.7 | Tier-1 patterns | ✅ 7 (`uk.no-wish-to-live` … `uk.suicide-word`) |
| 6.8 | Tier-2 patterns | ✅ 9 |
| 6.9 | Guards | ✅ `_UK_LIVE_TAIL` `:266`, `_UK_DIE_TAIL` `:289`, `_UK_MEANING_TAIL` `:298` |
| 7.1-7.3 | Whisper `uk`, locale mapping, prompt | ✅ `uk` ∈ `WHISPER_LANGUAGES` (`:105`) |
| 8.1 | Scenarios | ✅ 7 uk scenarios, `empty` present, all approved |
| 8.2 | Thresholds | ✅ global |
| 8.3 | `LANGUAGE_CORPUS["uk"]` | ✅ `(20, "ubh")` |
| 8.4 | `NATIVE_BASELINE["uk"]` | ✅ `"ubh"` |
| 8.5 | Probe inputs | ⚠️ only **2** uk inputs of 13 (`question_probe_inputs.json`) |
| 8.6 | `check_questions` detector + informal rule | ✅ `uk` in both |
| 8.7 | `check_rewrites` language check | ✅ inherited |
| 8.8 | `transcribe_bench` locales + samples | ✅ `uk-UA`, 5 samples (`kozlov_uk`) |
| 8.9 | `description_prompts` | ✅ `uk` in `LANGUAGE_NAMES` (`:84`), so `build_description_instruction` does not raise |
| 8.10 | `trace_picker`, `local_picker`, `diagnose_pipeline` | ⚠️ `trace_picker.py:157` and `local_picker.py:212` default to `ru` (and `trace_picker.py:659` reads `short_name_ru` only) — ru-only tools by construction; `diagnose_pipeline.py:52,60` is data-driven ✅ |
| 8.11 | `rewrite_prompts` | ✅ inherited from `_LANGUAGES` |
| 8.12 | Native reviewer / grading | ❌ **uk top-1 pairs are ungraded**: Maria explicitly did not grade en/uk (`evaluation/README.md:40`, `:3010`, `:3107`). No named Ukrainian reviewer exists |
| 9.1-9.2 | Admin API / Web | ✅ nothing hardcoded |
| 10.1 | Mobile + stores | out of these repositories |
| 11.1-11.4 | Chunk → index → import, env, docs | ✅ |

**What the dry run found that the checklist would otherwise have missed** —
each is now an item above:

* Ukrainian book short names have **never** resolved, and for two reasons, not
  one: the excerpt grammar `app/excerpt.py:280` accepted `[0-9a-z]+` only, and
  the lookup at `:416` omitted `short_name_uk` (item 3.5). Confirmed live and in
  the data: 66 of 66 `bible_books` rows carry a `short_name_uk`, 35 of them
  differ from every `code1..code5` and from `short_name_en`/`short_name_ru`, so
  they are reachable by nothing. **Closed on 2026-09-05** (ClickUp 86cbehfqx):
  Maria decided the contract is the catalogue's Latin alias, so the short names
  are display names by design and the two `short_name_*` terms left the `WHERE`
  clause as dead code. What the same investigation *did* find and fix is a
  defect: `Gen 1:1` used to answer `Book with alias 'en' not found`.
* The `about_text` of Bible Garden **argues in prose for exactly these three
  languages** (`app/about.py:61-63`, "почему именно эти языки"). Adding a fourth
  invalidates three existing texts, not just adds one (item 3.3).
* Two tests hard-code the language set and will fail — by design — the moment a
  language is added: `tests/test_evaluation_dataset.py:224` and
  `tests/test_about.py:25`. They are the tripwire, and both are now in the
  checklist (items 8.1, 3.3).
* A language can be **complete in code and still ungraded**: `uk` passes every
  code item and has no native reviewer, so its benchmark answers are
  `ungraded`. "Adding a language" is not finished when the tests pass
  (item 8.12).
* Probe coverage is uneven across languages (2 uk inputs vs 8 ru) — worth
  levelling when a language is added (item 8.5).

---

# Hardcoded language lists found

Every `("ru", "uk", "en")`-shaped literal, per-language dict, per-language
column or per-language branch found across **all** repositories. This is the
table to work through; the checklist above is its narrative.

## Bible-API

| File:line | What it is | On adding a language |
|---|---|---|
| `app/safety.py:93` | `DEFAULT_LANGUAGE = "en"` | review (fallback reply language) |
| `app/safety.py:94` | `SUPPORTED_LANGUAGES = ("ru", "uk", "en")` | **add** |
| `app/safety.py:101-119` | `SAFETY_REPLIES` — fixed crisis reply per language | **add + bump `SAFETY_REPLY_VERSION` (`:91`) + repin hash** |
| `app/safety.py:178-181` | `_CYRILLIC_RE`, `_LATIN_RE`, `_UK_LETTERS_RE`, `_RU_LETTERS_RE` | **extend / redesign** |
| `app/safety.py:187,192` | `_UK_WORDS`, `_RU_WORDS` function-word votes | **add a set** |
| `app/safety.py:212-215` | `detect_language`: **any Latin script → `"en"`** | **redesign for a Latin-script language** |
| `app/safety.py:257,266,271` | `_RU/_UK/_EN_LIVE_TAIL` guards | **add** |
| `app/safety.py:278` | `_NOT_NEGATED` (`не` — serves ru+uk) | **add** |
| `app/safety.py:285,289,293` | `_RU/_UK/_EN_DIE_TAIL` guards | **add** |
| `app/safety.py:297,298` | `_RU/_UK_MEANING_TAIL` guards | **add** |
| `app/safety.py:301+` | `EXPLICIT_PATTERNS` — 21 (ru 7 / uk 7 / en 7) | **add ≈7** |
| `app/safety.py:467+` | `WEAK_PATTERNS` — 26 (ru 9 / uk 9 / en 8) | **add ≈8** |
| `app/question_prompt.py:109` | `LANGUAGE_NAMES = {"ru": "Russian", "uk": "Ukrainian", "en": "English"}` | **add** |
| `app/question_prompt.py:127-128` | prompt names the informal register as "(Russian ty, Ukrainian ty)" | **edit + bump `QUESTION_PROMPT_VERSION` (`:104`)** |
| `app/question_prompt.py:148` | "In inflected languages such as Russian and Ukrainian…" | same |
| `app/question_prompt.py:172-211` | stage instruction blocks (`:185`, `:197`, `:207`) — **Russian for every language** | open decision (6.3) |
| `app/scripture_select.py:155-158` | `class Language(str, Enum)` — the public OpenAPI enum | **add a member** |
| `app/scripture_select.py:632` | `AI_SCRIPTURE_PRIMARY_TRANSLATIONS` docstring example `"ru=syn,en=bsb,uk=16"` | doc |
| `app/config.py:1075` | same variable's format comment `"ru=syn,en=bsb,uk=ubh"` | doc |
| `app/query_rewrite.py:86-104` | `_LANGUAGES` — English name + register hint per language | **add** |
| `app/query_rewrite.py:107` | `SUPPORTED_LANGUAGES = tuple(_LANGUAGES)` | derived |
| `app/query_rewrite.py:145-295` | `_EXAMPLES` — 6 worked examples per language | **add 6, de-fingerprinted** |
| `app/query_rewrite.py:296` | `_EXAMPLE_LANGUAGE_ORDER = ("ru", "en", "uk")` | **add** |
| `app/about.py:16-58` | 3 contacts × (`label` + `subtitle`) × 3 languages = 18 strings | **add 6** |
| `app/about.py:60-63` | `about_text` (Bible Garden) — **prose naming these three languages** | **add + rewrite the paragraph** |
| `app/about.py:84` | `for language in ("en", "ru", "uk")` (Lampada subtitle) | **add** |
| `app/about.py:85-89` | `about_text` (Lampada) | **add** |
| `app/version_check.py:18-29` | `MESSAGES["soft"]` / `MESSAGES["hard"]` per language | **add** |
| `app/excerpt.py` `EXCERPT_PATTERN` | excerpt grammar `(?P<book>[A-Za-z0-9]+)` — Latin book token, case folded before the lookup | nothing (contract, see 3.5) |
| `app/excerpt.py` `get_books_info` | book-alias `WHERE` matches `code1..code5` only — `short_name_en` / `short_name_ru` **removed 2026-09-05** | nothing |
| `app/retrieval_cli.py:156` | `--language choices=("ru","en","uk")` | **add** |
| `app/versification.py:68-76` | `TRANSLATION_SCHEMES` — per **translation alias** | **add per translation** |
| `app/canon.py:142-144` | `TRANSLATION_CHAPTER_COUNTS` — `("ubh", 39): 3` | per translation, if needed |
| `app/transcription.py:83-88` | `LOCALE_LANGUAGE_ALIASES` (deprecated ISO spellings) | usually none |
| `app/transcription.py:96-107` | `WHISPER_LANGUAGES` — 100 codes | **check membership, do not edit** |
| `app/vector_index.py:14` | index version shape `c3:BAAI/bge-m3@1024` | revisit after 86cbehd6h |
| `tests/test_evaluation_dataset.py:224` | `assert set(counts) == {"ru","en","uk"}` (+ ≥5 each, `:226`) | **will fail — update** |
| `tests/test_evaluation_dataset.py:244` | `empty` category in every language | **will fail — update** |
| `tests/test_about.py:25` | `dict.fromkeys(("en","ru","uk"), …)` | **will fail — update** |
| `tests/test_rewrite_prompts.py:60` | `LANGUAGES = ("ru", "en", "uk")` | **add** |
| `tests/test_safety.py:317,357,441,458` | 53 / 25 / 58 / 3 parametrised phrases | **add the four lists** |
| `tests/test_safety.py:587-613` | reply-per-language assertion + sha256 pin | **update** |

## Bible-API — evaluation

| File:line | What it is | On adding a language |
|---|---|---|
| `evaluation/scenarios.json` | 24 scenarios, ids prefixed `ru-`/`en-`/`uk-` (ru 10 / en 7 / uk 7) | **add ≥5, incl. `empty`** |
| `evaluation/thresholds.json` | global thresholds, **not per language** | nothing |
| `evaluation/question_probe_inputs.json` | 13 inputs (ru 8 / en 3 / uk 2) | **add** |
| `evaluation/retrieval_benchmark.py:69` | `LANGUAGE_CORPUS = {"ru": (1,"syn"), "en": (16,"bsb"), "uk": (20,"ubh")}` | **add** |
| `evaluation/retrieval_benchmark.py:795` | per-language result split | **add** |
| `evaluation/retrieval_benchmark.py:1576-1585` | `SELECT short_name_ru, short_name_en, short_name_uk` | **add a column** |
| `evaluation/check_refs_db.py:33` | `NATIVE_BASELINE = {"ru":"bti","en":"bsb","uk":"ubh"}` | **add** |
| `evaluation/check_questions.py:73,74,79,84` | `_UK_LETTERS`, `_RU_LETTERS`, `_UK_WORDS`, `_RU_WORDS` (second detector) | **add** |
| `evaluation/check_questions.py:96-105` | `_FORMAL` polite forms + `_FORMAL_EXCEPTIONS` | **add** |
| `evaluation/check_questions.py:111,140,142` | `_FORBIDDEN`, `_ADVICE_MODALS`, `_SUPPORT_MARKERS` — ru/uk/en phrases | **add** |
| `evaluation/check_questions.py:227` | `if language not in ("ru","uk"): informal = None` | **add if T/V** |
| `evaluation/check_rewrites.py:94` | imports `check_questions.detect_language` | inherited |
| `evaluation/transcribe_bench.py:75` | `LOCALES = {"ru":"ru-RU","uk":"uk-UA","en":"en-US"}` | **add** |
| `evaluation/transcribe_bench.py:94-108` | `SAMPLES` — 5 passages per language | **add 5** |
| `evaluation/transcribe_bench.py:702` | `for language in ("ru","uk","en","all")` | **add** |
| `evaluation/description_prompts.py:81-84` | `LANGUAGE_NAMES = {"ru":"Russian","en":"English","uk":"Ukrainian"}` | **add** |
| `evaluation/description_prompts.py:115-116` | `raise ValueError(f"unsupported language: {language}")` — hard gate | covered by the above |
| `evaluation/gen_descriptions.py:548,725` | calls `build_description_instruction(language, …)`; one language per batch enforced `:321-329` | inherited |
| `evaluation/rewrite_prompts.py:227-229`, `:255-264` | indexes `query_rewrite._LANGUAGES[language]` — `KeyError` on an unknown one | inherited from 5.1 |
| `evaluation/diagnose_pipeline.py:52,60` | `rb.LANGUAGE_CORPUS[scenario["language"]]` | inherited from 8.3 |
| `evaluation/trace_picker.py:157` | `LANGUAGE = "ru"` (module default) | ru-only tool |
| `evaluation/trace_picker.py:659` | `SELECT number, short_name_ru` | ru-only tool |
| `evaluation/local_picker.py:212-213` | `LANGUAGE = "ru"`, `rb.LANGUAGE_CORPUS[LANGUAGE]` | ru-only tool |
| `evaluation/local_picker.py:720` | `"language": "ru"` hardcoded in the report row | ru-only tool |
| `evaluation/gen_questions.py:492,611` | `detect_language(source) if source.strip() else "en"` | review |

## Dashboard-API

| File:line | What it is | On adding a language |
|---|---|---|
| `migrations/2025_07_23_231232_initial_database_structure.sql:37-42` | `CREATE TABLE languages (alias, name_en, name_national)` | **INSERT a row** |
| `migrations/2025_07_23_231232_initial_database_structure.sql:19-24` | `bible_books.short_name_{en,ru,uk}`, `full_name_{en,ru,uk}` — **language in the column names** | **schema migration + 66 names** |
| `migrations/2025_07_23_231232_initial_database_structure.sql:114` | `translations.language` FK → `languages.alias` | nothing |
| `migrations/2026_02_08_104500_align_languages_translations_charset_with_dump.sql` | re-adds the FK, widens `language` to `VARCHAR(10)` | nothing |
| `tests/seed_test_data.sql:9` | `INSERT INTO bible_books (… short_name_en, short_name_ru, short_name_uk, full_name_*)` | **add columns** |
| `tests/seed_test_data.sql:80-81` | `INSERT INTO languages (alias, name_en, name_national) VALUES ('ru', …)` — the fixture's own language set | **add a row if the tests need it** |
| `app/excerpt.py` `EXCERPT_PATTERN` | excerpt grammar `(?P<book>[A-Za-z0-9]+)` — same Latin token as Bible-API | nothing (contract, see 3.5) |
| `app/excerpt.py` `get_books_info` | book-alias `WHERE` matches `code1..code5` only — `short_name_*` **removed 2026-09-05** (mirrors Bible-API) | nothing |
| `app/models.py:9-12`, `:29`, `:47` | `LanguageModel`, `TranslationModel.language: str` | nothing (no enum) |
| `app/data.py:402`, `:647` | `SELECT * FROM languages` in manifest and export | nothing |

## Dashboard-Web

| File:line | What it is | On adding a language |
|---|---|---|
| `src/composables/useApi.ts:98-112`, `src/services/api.ts:118-120` | languages fetched from the API | nothing |
| `src/Components/BibleVoices.vue:258-260`, `:316-319` | display name by alias, filter options derived from voices | nothing |
| `src/types/api.ts:25`, `:151` | `LanguageModel` / `LanguageResponse` — plain `string` | nothing |
| `src/Components/AlignmentTasks.vue:371` | `toLocaleDateString('ru-RU')` — **UI date locale, not content** | unrelated |
| `src/Components/ApiStats.vue:183` | `toLocaleString('en-GB')` — UI date locale | unrelated |

*(No i18n bundle and no `SUPPORTED_LANGUAGES` constant exist in this repo.)*

## bible-parser

| File:line | What it is | On adding a language |
|---|---|---|
| `NumberFormatter.php:6` (blocks `:7`, `:23`, `:39`) | `private static $numerals` keyed `ru`/`uk`/`en`, gendered ordinals | **add a block** |
| `NumberFormatter.php:97-98` | `getOrdinalNumber()` — **throws** "Неподдерживаемый язык" on a missing numerals block | covered by the above |
| `NumberFormatter.php:164-186` | `getChapterPrefix()` — `ru`/`uk`/`en`, else **throws** (`:185`) | **add branches** |
| `NumberFormatter.php:189-215` | `getChapterName()` — same, else **throws** (`:214`) | **add branches** |
| `NumberFormatter.php:104,131` | `if ($language === 'en')` (gender-less ordinals) | **review** |
| `const.php:493` / `:569` / `:645` | `$base_ru_template` / `$base_en_template` / `$base_uk_template` — 66 book names each | **add a 4th array** |
| `const.php:722-923` | `switch($voice)` patching those templates | **add** |
| `const.php:47-364` | `get_voice_info()` per-voice arrays with `'mfa_lang'` (e.g. `:144` `en_us`) | **add per voice** |
| `const.php:952-959` | `get_all_bible_books()` (`:929`) reads `short_name_{en,ru,uk}` / `full_name_{en,ru,uk}` by name | **add** |
| `alignment/timecodes_mfa.class.php:44-67` | `getMfaModelsConfig()` — `ru`, `en_us`, `en_uk`, `uk` → MFA model names/versions | **add (needs a published MFA model)** |
| `alignment/timecodes_mfa.class.php:2383-2400` | `get_models_by_language()` — `die()` on an unknown language | **covered by the above** |
| `docs/translations.md:102 / :190 / :327` | `## RU` / `## EN` / `## UK` sections | **add a section** |

## Architecture / Deploy

| File:line | What it is | On adding a language |
|---|---|---|
| `Architecture/architecture.md:170` | `cep_public` table list: "`languages` — Languages (en, ru, uk)" | **update** |
| `Deploy/data-flow.md:28` | `languages` listed among the reference tables flowing `cep_admin` → `cep_public` | review |
| `Deploy/env-checklist.md:218-221` | `AI_SCRIPTURE_PRIMARY_TRANSLATIONS` among Bible-API env vars (name only, no value) | **set the new entry** |
| `Deploy/env-checklist.md:277` | rename row `SCRIPTURE_PRIMARY_TRANSLATIONS` → `AI_SCRIPTURE_PRIMARY_TRANSLATIONS` | history |

---

# Open decisions (for Maria)

1. **Latin-script detection.** `app/safety.py:215-217` returns `"en"` for any
   Latin-script message by construction. Adding a second Latin-script language
   (Spanish, Polish, Portuguese, Indonesian…) makes that line actively wrong:
   the prayer gets the English prompt and, in crisis, the **English** fixed
   reply. Options: (a) extend the alphabet-and-function-word approach with
   per-language distinguishing letters and stop-word votes — cheap, and
   degrades to `None` honestly; (b) add a small offline language-ID dependency
   — more accurate, but a new dependency in the one module that is deliberately
   "dictionary and regex only, no model, no network"; (c) take the language
   from the client (the app knows its own locale) and use detection only as a
   check — a public-contract change. **This decision must be made before the
   first Latin-script language, not with it.**
2. **Stage instructions in Russian for every language**
   (`app/question_prompt.py:172-215`, decision recorded at `:82-88`). Measured
   not to leak into answers today on ru/uk/en. Does that hold for a language
   more distant from Russian, on a model with weaker cross-lingual instruction
   following? Translating the blocks is a prompt change and needs a
   `QUESTION_PROMPT_VERSION` bump plus a measured comparison.
3. **Does bge-m3 cover the candidate language well enough?** ADR 0010 accepted a
   measured quality drop for the existing three (MRR 0.664 → 0.524, thresholds
   0.4.0). The model is multilingual, but its quality per language is not
   uniform and we have measured only ru/en/uk. A candidate language needs its
   own `retrieval_benchmark.py` run **before** it is promised in the app.
   **Revisit after 86cbehd6h** — the same question, asked of whatever model the
   company server ends up serving.
4. **Who is the native reviewer for each language?** `uk` is the proof this is
   not theoretical: it is complete in code and its benchmark top-1 pairs are
   still `ungraded`, because Maria grades Russian and did not grade en/uk
   (`evaluation/README.md:40`, `:3107`). A language without a named native
   reviewer cannot pass the despair validation *or* leave
   `review_status: draft`.
5. ~~**What alphabet may a book alias be written in?**~~ **Resolved — Maria,
   2026-09-05 (ClickUp 86cbehfqx): option (a), the contract is the Latin alias
   from the books catalogue, in any letter case.** The app takes the book name
   from `GET /api/translations/{code}/books` and a human never types it, so
   Cyrillic book names are not a scenario; widening the grammar to `\w+` and
   adding `short_name_uk` was considered and rejected. Both APIs now accept
   `[A-Za-z0-9]+` as a whole token and casefold it, `Бут 1:1` is a `422` that
   names the expected format, an unknown Latin alias is a `404`, and the dead
   `short_name_en` / `short_name_ru` terms are out of the `WHERE` clause
   (item 3.5). Nothing here is per language any more.
6. **`about_text` argues for exactly three languages.** `app/about.py:61-63`
   explains *why these languages* in prose, in all three. A fourth language
   means rewriting that paragraph everywhere, and it is Maria's text — a
   product decision, not a translation task.
