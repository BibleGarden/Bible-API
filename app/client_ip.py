import hashlib
import hmac
import logging
import threading
import time
from ipaddress import ip_address

from starlette.requests import Request

from config import AI_CLIENT_HMAC_KEY
from trusted_proxies import TRUSTED_PROXIES

logger = logging.getLogger(__name__)

# How often the same untrusted forwarding peer is reported. The event is
# per-request by nature, so logging it per request would drown the log (and
# would be attacker-triggerable); logging it once per process would hide a
# misconfiguration introduced hours after startup. Once per peer per five
# minutes keeps it loud and bounded.
FORWARDING_LOG_INTERVAL_SECONDS = 300
# Bound on the memory of the "already reported" table: an untrusted peer is
# usually one address (a misconfigured proxy), never a crowd. When it is a
# crowd, the OLDEST entry is evicted — dropping the whole table would let a
# caller who cycles through addresses erase the record of the real
# misconfigured peer and get it re-reported on the next request.
_MAX_TRACKED_PEERS = 64
# Operational constant: however many distinct peers show up, at most this many
# UNTRUSTED-forwarding lines are written per interval. The trigger is a request
# header, so without a global ceiling the volume of this log is chosen by
# whoever sends the requests. Ten lines per five minutes is far more than the
# real failure needs (it has one peer) and far less than a flood.
_MAX_REPORTS_PER_INTERVAL = 10

_reported_peers: dict[str, float] = {}
_reported_lock = threading.Lock()
_report_window_started: float | None = None
_reports_in_window = 0


def _reset_report_state() -> None:
    """Forget every reporting decision. For tests; never called at runtime."""
    global _report_window_started, _reports_in_window
    with _reported_lock:
        _reported_peers.clear()
        _report_window_started = None
        _reports_in_window = 0


def _should_report(peer: str) -> bool:
    global _report_window_started, _reports_in_window
    now = time.monotonic()
    with _reported_lock:
        last = _reported_peers.get(peer)
        if last is not None and now - last < FORWARDING_LOG_INTERVAL_SECONDS:
            return False

        if (
            _report_window_started is None
            or now - _report_window_started >= FORWARDING_LOG_INTERVAL_SECONDS
        ):
            _report_window_started = now
            _reports_in_window = 0
        if _reports_in_window >= _MAX_REPORTS_PER_INTERVAL:
            # Deliberately not remembering the peer: it was silenced by the
            # ceiling, not reported, so it stays eligible for the next window.
            return False
        _reports_in_window += 1

        if peer not in _reported_peers and len(_reported_peers) >= _MAX_TRACKED_PEERS:
            oldest = min(_reported_peers, key=_reported_peers.__getitem__)
            del _reported_peers[oldest]
        _reported_peers[peer] = now
        return True


def _report_untrusted_forwarding(peer: str) -> None:
    """The 2026-08-30 failure, said out loud.

    An `X-Forwarded-For` arriving from a peer we do not trust means one of two
    things: a reverse proxy is in front of us and is not configured as trusted
    (the incident — statistics collapse onto the proxy address and the
    per-client rate limit becomes global), or a client is trying to spoof its
    address (correctly ignored). Both are worth a line; the first is worth an
    error.
    """
    if not _should_report(peer):
        return
    if TRUSTED_PROXIES.configured:
        logger.error(
            "X-Forwarded-For received from UNTRUSTED peer %s — the header is "
            "ignored and this peer is recorded as the client, so statistics "
            "and the per-client AI rate limit are wrong for every caller "
            "behind it. Trusted right now: %s. Fix TRUSTED_PROXY_HOSTS / "
            "TRUSTED_PROXY_IPS (ClickUp 86cbbq6vz)",
            peer,
            TRUSTED_PROXIES.describe(),
        )
    else:
        logger.warning(
            "X-Forwarded-For received from %s but no trusted proxy is "
            "configured — the header is ignored. Correct when this API is "
            "exposed directly; if a reverse proxy is in front of it, set "
            "TRUSTED_PROXY_HOSTS",
            peer,
        )


def client_from_forwarded(forwarded: str, peer: str) -> str:
    """The client address in an `X-Forwarded-For` we have decided to believe.

    **Read the header from the RIGHT.** `X-Forwarded-For` is a trace of hops,
    appended to by each proxy, and *only the part our own trusted proxies
    appended is trustworthy*. Everything to the left of it was supplied by
    whoever spoke to the outermost proxy — that is, by the client.

    Our production nginx uses `$proxy_add_x_forwarded_for` in every location,
    which **keeps the header the client sent** and appends `$remote_addr` to
    it. So a client sending `X-Forwarded-For: 1.2.3.4` makes us receive
    `1.2.3.4, <its real address>`. Taking the leftmost element (as this
    function did until 2026-08-30) meant the caller simply *declared* its own
    address: statistics recorded whatever it liked, and a fresh value per
    request evaded `AI_REQUESTS_PER_CLIENT_PER_MINUTE` entirely, since the
    per-client bucket is keyed by that address.

    The rule implemented here is the standard one, and it stays correct if a
    second trusted hop is ever added: walk right to left, skip addresses that
    are themselves trusted proxies, and the first address that is *not* one is
    the client. With today's single hop the answer is simply the rightmost
    element — the address nginx observed on the client connection.

    A malformed or empty element (nginx never produces one) means the header
    cannot be read as a hop chain, so it is ignored entirely and the direct
    peer is the client — the same conservative answer as an untrusted peer.
    """
    innermost = peer
    for token in reversed(forwarded.split(",")):
        try:
            candidate = str(ip_address(token.strip()))
        except ValueError:
            return peer
        if not TRUSTED_PROXIES.is_trusted(candidate):
            return candidate
        # A trusted proxy quoting another trusted proxy: keep walking left.
        innermost = candidate
    return innermost


def resolve_client_ip(request: Request) -> str:
    peer = request.client.host if request.client else "unknown"
    forwarded = request.headers.get("x-forwarded-for", "")
    if not TRUSTED_PROXIES.is_trusted(peer):
        if forwarded.strip():
            _report_untrusted_forwarding(peer)
        return peer

    return client_from_forwarded(forwarded, peer)


def pseudonymize_twinkler_client(client_ip: str) -> str:
    if not AI_CLIENT_HMAC_KEY:
        raise RuntimeError("AI_CLIENT_HMAC_KEY is not configured")
    return hmac.new(
        AI_CLIENT_HMAC_KEY.encode("utf-8"),
        client_ip.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
