"""
Authorization module for Public API

Only API Key authentication (no JWT, no admin login).
"""

from typing import Optional
import hmac
from hashlib import sha256

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


CLIENT_KEY_DIGESTS = (
    ("bible-garden", sha256(BIBLE_GARDEN_API_KEY.encode("utf-8")).digest()),
    ("lampada", sha256(LAMPADA_API_KEY.encode("utf-8")).digest()),
    ("ops", sha256(OPS_API_KEY.encode("utf-8")).digest()),
)


def resolve_application(api_key: Optional[str]) -> str:
    # Hash before comparison so every compare_digest operand is exactly 32 bytes.
    presented_digest = sha256((api_key or "").encode("utf-8")).digest()
    matches = [
        (name, hmac.compare_digest(presented_digest, configured_digest))
        for name, configured_digest in CLIENT_KEY_DIGESTS
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


def verify_ops_api_key(
    request: Request, authenticated: bool = Depends(verify_api_key)
) -> bool:
    if request.state.application != "ops":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API Key",
        )
    return True


# Dependencies for use in endpoints
RequireAPIKey = Depends(verify_api_key)
RequireOpsAPIKey = Depends(verify_ops_api_key)
