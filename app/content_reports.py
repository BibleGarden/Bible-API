from typing import Annotated, Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator
from starlette.responses import JSONResponse

from auth import RequireAPIKey
from database import create_connection


CONTENT_REPORT_PATH = "/api/ai/content-reports"
MAX_CONTENT_CHARS = 10_000
MAX_COMMENT_CHARS = 1_000
MAX_BODY_BYTES = 64 * 1024

ContentText = Annotated[str, StringConstraints(min_length=1, max_length=MAX_CONTENT_CHARS)]
UserComment = Annotated[str, StringConstraints(max_length=MAX_COMMENT_CHARS)]

router = APIRouter()


class ContentReportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content_type: Literal["question", "scripture"]
    content_text: ContentText
    user_comment: Optional[UserComment] = None
    language: Literal["ru", "en", "uk"]

    @field_validator("content_text")
    @classmethod
    def content_text_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("content_text must not be blank")
        return value

    @field_validator("user_comment")
    @classmethod
    def normalize_blank_comment(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value.strip():
            return value
        return None


class ContentReportResponse(BaseModel):
    status: Literal["ok"] = "ok"
    report_id: int = Field(ge=1)


class _BodyTooLarge(Exception):
    pass


class ContentReportBodyLimitMiddleware:
    """Reject an oversized report before FastAPI materializes its JSON body."""

    def __init__(self, app, max_body_bytes: int = MAX_BODY_BYTES):
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or scope.get("path", "").rstrip("/") != CONTENT_REPORT_PATH:
            await self.app(scope, receive, send)
            return

        headers = {name.lower(): value for name, value in scope.get("headers", [])}
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                if int(content_length) > self.max_body_bytes:
                    await self._reject(scope, receive, send)
                    return
            except ValueError:
                pass

        received = 0

        async def limited_receive():
            nonlocal received
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_body_bytes:
                    raise _BodyTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except _BodyTooLarge:
            await self._reject(scope, receive, send)

    @staticmethod
    async def _reject(scope, receive, send):
        response = JSONResponse(
            status_code=413,
            content={"detail": "request body is too large"},
        )
        await response(scope, receive, send)


@router.post(
    "/ai/content-reports",
    status_code=201,
    response_model=ContentReportResponse,
    operation_id="create_content_report",
    tags=["AI"],
    summary="Report AI-generated content",
    responses={
        403: {"description": "Invalid or missing API key"},
        413: {"description": "Request body is too large"},
        500: {"description": "Content report storage is unavailable"},
    },
)
def create_content_report(
    report: ContentReportRequest,
    api_key: bool = RequireAPIKey,
) -> ContentReportResponse:
    connection = create_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Content report storage is unavailable")

    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO ai_content_reports
                (content_type, content_text, user_comment, language)
            VALUES (%s, %s, %s, %s)
            """,
            (
                report.content_type,
                report.content_text,
                report.user_comment,
                report.language,
            ),
        )
        connection.commit()
        report_id = cursor.lastrowid
        if not isinstance(report_id, int) or report_id < 1:
            raise RuntimeError("database did not return a report id")
        return ContentReportResponse(report_id=report_id)
    except HTTPException:
        raise
    except Exception:
        connection.rollback()
        raise HTTPException(
            status_code=500,
            detail="Failed to store content report",
        ) from None
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()
