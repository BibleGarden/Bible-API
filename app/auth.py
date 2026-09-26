"""
Authorization module for Public API

Only API Key authentication (no JWT, no admin login).
"""

from typing import Optional
import hmac

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

try:
    from config import BIBLE_GARDEN_API_KEY, LAMPADA_API_KEY, OPS_API_KEY
except Exception as e:
    raise RuntimeError(
        'Failed to import configuration. Ensure app/config.py is importable and required env vars are set.'
    ) from e

# Security schemes
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


CLIENT_KEYS = (
    ("bible-garden", BIBLE_GARDEN_API_KEY),
    ("lampada", LAMPADA_API_KEY),
    ("ops", OPS_API_KEY),
)


def resolve_application(api_key: Optional[str]) -> str:
    # Evaluate all comparisons, even after a match, without exposing the key.
    matches = [
        (name, hmac.compare_digest((api_key or "").encode("utf-8"),
                                   configured_key.encode("utf-8")))
        for name, configured_key in CLIENT_KEYS
    ]
    for name, matched in matches:
        if matched:
            return name
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Invalid or missing API Key",
    )


def verify_api_key(
    request: Request, api_key: Optional[str] = Security(api_key_header)
) -> bool:
    """
    Verify static API key from X-API-Key header

    Used for authenticated Bible-API endpoints
    """
    request.state.application = resolve_application(api_key)
    return True


def verify_api_key_query(request: Request, api_key: Optional[str] = None) -> bool:
    """
    Verify static API key from query parameter

    Used for audio endpoint (browser cannot send custom headers)
    """
    request.state.application = resolve_application(api_key)
    return True


# Dependencies for use in endpoints
RequireAPIKey = Depends(verify_api_key)
