# models.py
from pydantic import BaseModel, Field
from typing import Optional, Literal

# Languages & Translations

class LanguageModel(BaseModel):
    alias: str
    name_en: str
    name_national: str

class VoiceModel(BaseModel):
    code: int
    alias: str
    name: str
    description: Optional[str] = None
    is_music: bool
    active: bool

class TranslationModel(BaseModel):
    code: int
    alias: str
    name: str
    description: Optional[str] = None
    language: str
    active: bool
    voices: list[VoiceModel]

# TranslationInfo

class BookInfoModel(BaseModel):
    code: int
    number: int
    alias: str
    name: str
    chapters_count: int

class TranslationInfoModel(BaseModel):
    code: int
    alias: str
    name: str
    description: Optional[str] = None
    language: str
    books_info: list[BookInfoModel]

# ExcerptWithAlignment

class VerseWithAlignmentModel(BaseModel):
    code: int
    number: int
    join: int
    text: str
    html: str
    begin: float
    end: float
    start_paragraph: bool

class NoteModel(BaseModel):
    code: int
    number: int
    text: str
    verse_code: Optional[int] = None
    title_code: Optional[int] = None
    position_text: int
    position_html: int

class TitleModel(BaseModel):
    code: int
    text: str
    before_verse_code: Optional[int] = None
    metadata: Optional[str] = None
    reference: Optional[str] = None
    subtitle: bool = False
    position_text: Optional[int] = None
    position_html: Optional[int] = None

class PartsWithAlignmentModel(BaseModel):
    book: BookInfoModel
    chapter_number: int
    audio_link: str
    prev_excerpt: str = Field(
        description=(
            "The excerpt preceding this one in this translation, e.g. 'psa 150'. "
            "Books the translation ships no text for are skipped, so the "
            "reference always resolves. Empty when this translation publishes "
            "nothing before this chapter."
        )
    )
    next_excerpt: str = Field(
        description=(
            "The excerpt following this one in this translation, e.g. 'mat 1'. "
            "Books the translation ships no text for are skipped, so the "
            "reference always resolves. Empty when this translation publishes "
            "nothing after this chapter."
        )
    )
    verses: list[VerseWithAlignmentModel]
    notes: list[NoteModel]
    titles: list[TitleModel]

class ExcerptWithAlignmentModel(BaseModel):
    title: str
    is_single_chapter: bool
    parts: list[PartsWithAlignmentModel]

# Translation Books

class TranslationBookModel(BaseModel):
    code: int
    book_number: int
    name: str
    alias: str
    chapters_count: int = Field(
        description=(
            "Chapters the book is expected to have in this translation: the "
            "count of the 66-book canon, widened to the translation's own "
            "last chapter when it carries deuterocanonical additions "
            "(e.g. Ps 151 in syn). Never derived from other translations."
        )
    )
    chapters_without_audio: list[int] = Field(
        default_factory=list,
        description=(
            "Chapters with text but no audio file for the requested voice. "
            "Only populated when voice_code was passed to the request."
        ),
    )
    chapters_without_text: list[int] = Field(
        default_factory=list,
        description=(
            "Chapters of 1..chapters_count this translation has no text for. "
            "A book the translation ships no text for at all lists every "
            "chapter here and has has_text=false."
        ),
    )
    has_text: bool = Field(
        default=True,
        description=(
            "False when the translation declares the book but contains no "
            "verse of it (a publisher's editorial scope, e.g. npu ships the "
            "Psalms and the New Testament only). Such a book is still "
            "returned, with every chapter listed in chapters_without_text."
        ),
    )

# Audio Error Models

class AudioFileNotFoundError(BaseModel):
    detail: str
    alternative_url: Optional[str] = None

# About

class LocalizedText(BaseModel):
    en: str
    ru: str
    uk: str

class AboutContactModel(BaseModel):
    id: str
    icon: str
    url: str
    sort_order: int
    label: LocalizedText
    subtitle: LocalizedText

class AboutModel(BaseModel):
    contacts: list[AboutContactModel]
    about_text: LocalizedText

# Version Check

class VersionCheckModel(BaseModel):
    update_type: Literal["none", "soft", "hard"]
    latest_version: str
    store_url: str
    message: Optional[LocalizedText] = None

# Import

class ImportCountCheckModel(BaseModel):
    """One line of the post-import count check (ClickUp 86cbbq5zp).

    `expected` / `actual` are `null` on one line only — `chunks_digest`
    (ClickUp 86cbegwr9), where "this side has no chunks at all" has no
    number. Reporting that as 0 would be indistinguishable from a digest
    that really is 0 (a total XOR cancellation), which is why the comparison
    is made on the raw values and the raw values are what is reported. Every
    row-count line carries integers.
    """
    expected: Optional[int]
    actual: Optional[int]
    ok: bool


class ImportIndexReportModel(BaseModel):
    """The RAG index half of an import (ClickUp 86cbegwr9).

    Additive: an importer that ships no index (or a caller that ignores this
    block) sees exactly the report it saw before. The counts are rows
    *written*; whether they are the rows admin-api declared is answered by
    `translation_mismatches`, which now carries the index tables,
    `chunks_digest` and `chunk_embeddings_orphans` beside the text ones.
    """
    embedding_version: Optional[str] = Field(
        default=None,
        description=(
            "The index version this deployment reads and therefore imported: "
            "c{CHUNKING_VERSION}:{EMBEDDING_MODEL}@{EMBEDDING_DIMENSIONS}."
        ),
    )
    chunking_version: Optional[int] = None
    mapping_version: Optional[int] = Field(
        default=None,
        description=(
            "The Psalm-map version admin-api shipped; null when it holds more "
            "than one. A version other than this service's VERSIFICATION_VERSION "
            "is imported as stored and logged, not refused — the map is simply "
            "not read until the versions agree."
        ),
    )
    translations_indexed: list[str] = Field(
        default_factory=list,
        description=(
            "Translations whose index was replaced. A translation with no "
            "chunks at all is listed too: its Psalm map still travels, and "
            "having no chunks is normal data (bti, npu, webbe, webus)."
        ),
    )
    tables: dict[str, int] = Field(
        default_factory=dict,
        description="Index rows written, per table, across every translation.",
    )
    other_versions_removed: dict[str, int] = Field(
        default_factory=dict,
        description=(
            "chunk_embeddings rows of OTHER embedding versions deleted per "
            "translation. Always empty without ?drop_other_index_versions=1."
        ),
    )
    drop_other_index_versions: bool = False
    index_cache_cleared: bool = Field(
        default=False,
        description=(
            "The in-process corpus cache was dropped, so POST /api/ai/scripture "
            "serves the new index without a restart. False means the drop "
            "failed and a restart (or POST /api/cache/clear) is needed."
        ),
    )


class ImportReportModel(BaseModel):
    # "ok" — everything written and every count matches admin-api, in total
    # and per translation.
    # "mismatch" — the import finished, but cep_public does not hold what
    # admin-api declared. The data is there; something is off and `verification`
    # / `translation_mismatches` say where.
    # "removals_rejected" — the import finished and is correct, but the resync
    # would have dropped translations admin-api no longer publishes and was
    # not given `?allow_removals=1`. Nothing was removed; `removals_rejected`
    # names them and `detail` says what to do.
    # Callers that used to test `status == "ok"` keep working and now catch
    # more.
    status: str
    detail: Optional[str] = Field(
        default=None,
        description=(
            "Human-readable explanation of a status other than \"ok\"; "
            "None when the resync needs no explanation."
        ),
    )
    translation: Optional[str] = None
    tables: dict[str, int]
    # Everything below is additive (2026-08-30): the fields existing callers
    # read are unchanged, and `tables` still carries the rows written per table.
    translations_imported: list[str] = []
    translations_removed: list[str] = []
    removals_rejected: list[str] = Field(
        default_factory=list,
        description=(
            "Translations a full resync would have dropped but did not, "
            "because `allow_removals` was not set. Always empty for a point "
            "import and for a resync run with `?allow_removals=1`."
        ),
    )
    orphans_removed: dict[str, int] = {}
    verification: dict[str, ImportCountCheckModel] = {}
    translation_mismatches: dict[str, dict[str, ImportCountCheckModel]] = Field(
        default_factory=dict,
        description=(
            "Per-translation count disagreements of a full resync, "
            "{alias: {table: check}} — only the tables that disagree, so an "
            "empty object means every translation matched the manifest. "
            "Totals alone would pass on compensating errors. Since 2026-09-05 "
            "the index tables appear here too, plus `chunks_digest` (the "
            "source's order-independent digest of the chunk set against ours; "
            "the one line whose expected/actual may be null — no chunks at "
            "all) and `chunk_embeddings_orphans` (embeddings whose chunk is "
            "gone, counted for the imported embedding version only: the rows "
            "of an older version are kept as a rollback and are not this "
            "import's to verify)."
        ),
    )
    index: Optional[ImportIndexReportModel] = Field(
        default=None,
        description=(
            "What the import did to the RAG index (ClickUp 86cbegwr9). Written "
            "in the same transaction as each translation's text."
        ),
    )
    duration_seconds: Optional[float] = None
