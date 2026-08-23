import re
import time
import threading
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from client_ip import pseudonymize_lampada_client, resolve_client_ip
from database import create_connection

EXCLUDED_PATHS = {
    "/docs",
    "/openapi.json",
    "/redoc",
    "/favicon.ico",
}
EXCLUDED_STATUS_CODES = {403, 404}
LAMPADA_PATH = "/api/lampada/v1/complete"

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
        comparison_path = path.rstrip("/") or "/"
        if comparison_path in EXCLUDED_PATHS or not path.startswith("/api/"):
            return await call_next(request)

        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        client_ip = resolve_client_ip(request)
        user_agent = (request.headers.get("user-agent") or "")[:512]
        if comparison_path == LAMPADA_PATH:
            try:
                client_ip = pseudonymize_lampada_client(client_ip)[:40]
            except RuntimeError:
                client_ip = "lampada-unconfigured"
            user_agent = ""

        if response.status_code in EXCLUDED_STATUS_CODES:
            return response

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
