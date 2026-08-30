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

class ImportReportModel(BaseModel):
    status: str
    translation: Optional[str] = None
    tables: dict[str, int]
