import hashlib
import hmac
from ipaddress import ip_address

from starlette.requests import Request

from config import TWINKLER_CLIENT_HMAC_KEY, TRUSTED_PROXY_IPS


def resolve_client_ip(request: Request) -> str:
    peer = request.client.host if request.client else "unknown"
    if peer not in TRUSTED_PROXY_IPS:
        return peer

    forwarded = request.headers.get("x-forwarded-for", "")
    candidate = forwarded.split(",", maxsplit=1)[0].strip()
    try:
        return str(ip_address(candidate))
    except ValueError:
        return peer


def pseudonymize_twinkler_client(client_ip: str) -> str:
    if not TWINKLER_CLIENT_HMAC_KEY:
        raise RuntimeError("TWINKLER_CLIENT_HMAC_KEY is not configured")
    return hmac.new(
        TWINKLER_CLIENT_HMAC_KEY.encode("utf-8"),
        client_ip.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
