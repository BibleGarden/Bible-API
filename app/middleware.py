import re
import time
import threading
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from database import create_connection

EXCLUDED_PATHS = {"/docs", "/openapi.json", "/redoc", "/favicon.ico"}

# Normalize dynamic path segments for cleaner stats grouping
_NORMALIZE_RULES = [
    (re.compile(r"^/api/audio/.+"), "/api/audio/*"),
    (re.compile(r"^/api/translations/\d+/books"), "/api/translations/*/books"),
]


def _normalize_endpoint(path: str) -> str:
    for pattern, replacement in _NORMALIZE_RULES:
        if pattern.match(path):
            return replacement
    return path


class RequestStatsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        if path in EXCLUDED_PATHS:
            return await call_next(request)

        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        # Extract client IP from X-Forwarded-For or fall back to client host
        forwarded = request.headers.get("x-forwarded-for")
        client_ip = forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else "unknown")

        user_agent = (request.headers.get("user-agent") or "")[:512]

        endpoint = _normalize_endpoint(path)

        # Fire-and-forget insert in a daemon thread
        threading.Thread(
            target=_insert_request_log,
            args=(endpoint, request.method, response.status_code, elapsed_ms, client_ip, user_agent),
            daemon=True,
        ).start()

        return response


def _insert_request_log(endpoint: str, method: str, status_code: int, response_time_ms: int, client_ip: str, user_agent: str):
    try:
        connection = create_connection()
        if connection is None:
            return
        cursor = connection.cursor()
        try:
            cursor.execute(
                """INSERT INTO api_requests
                   (endpoint, method, status_code, response_time_ms, client_ip, user_agent)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (endpoint, method, status_code, response_time_ms, client_ip, user_agent),
            )
            connection.commit()
        finally:
            cursor.close()
            connection.close()
    except Exception:
        pass  # never break the app because of stats logging
