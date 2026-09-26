import re
import time
import threading
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from client_ip import pseudonymize_client_ip, resolve_client_ip
from database import create_connection

logger = logging.getLogger(__name__)

EXCLUDED_PATHS = {
    "/docs",
    "/openapi.json",
    "/redoc",
    "/favicon.ico",
    "/api/health",
}
EXCLUDED_STATUS_CODES = {403, 404, 405}
# Endpoints handling prayer content. For these the user agent is dropped;
# every logged endpoint stores an HMAC client pseudonym. Request and response
# bodies are never read by this middleware, so neither the prayer context nor
# the selected passage can reach the stats table (ADR 0006: the passage alone
# is not private, but combined with a client identity it would reveal what
# the person prayed about).
PRIVATE_PATHS = frozenset({
    "/api/ai/question",
    "/api/ai/transcribe",
    "/api/ai/scripture",
    "/api/ai/content-reports",
})

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
        if (
            comparison_path in EXCLUDED_PATHS
            or not path.startswith("/api/")
            or (request.method == "OPTIONS" and path.startswith("/api/audio/"))
        ):
            return await call_next(request)

        start = time.monotonic()
        response = await call_next(request)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        if response.status_code in EXCLUDED_STATUS_CODES:
            return response

        application = getattr(request.state, "application", None)
        if application is None:
            # Routing redirects and validation errors can happen before auth.
            # Record only requests that actually passed the auth dependency.
            if response.status_code < 300 or response.status_code >= 500:
                logger.error(
                    "Request statistics omitted: application missing for status %s",
                    response.status_code,
                )
            return response
        if application not in {"bible-garden", "lampada", "ops"}:
            logger.error("Request statistics omitted: invalid application identity")
            return response

        client_ip = pseudonymize_client_ip(resolve_client_ip(request))[:40]
        user_agent = (request.headers.get("user-agent") or "")[:512]
        if comparison_path in PRIVATE_PATHS:
            user_agent = ""

        endpoint = _normalize_endpoint(path)

        # Fire-and-forget insert in a daemon thread
        threading.Thread(
            target=_insert_request_log,
            args=(endpoint, request.method, response.status_code, elapsed_ms,
                  client_ip, user_agent, application),
            daemon=True,
        ).start()

        return response


def _insert_request_log(endpoint: str, method: str, status_code: int, response_time_ms: int, client_ip: str, user_agent: str, application: str):
    try:
        connection = create_connection()
        if connection is None:
            raise RuntimeError("Request statistics database connection failed")
        cursor = connection.cursor()
        try:
            cursor.execute(
                """INSERT INTO api_requests
                   (endpoint, method, status_code, response_time_ms, client_ip, user_agent, application)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (endpoint, method, status_code, response_time_ms, client_ip, user_agent, application),
            )
            connection.commit()
        finally:
            cursor.close()
            connection.close()
    except Exception:
        logger.exception("Request statistics insert failed")
