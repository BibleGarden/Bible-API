"""
Shared retry policy of the Gemini stages of a scripture selection
(ClickUp 86cbbnaxn; the budget itself lives in app/deadline.py).

Three stages call the same provider with the same failure modes — query
rewrite, query embeddings and the grounded rerank — and each of them used to
carry its own copy of two decisions that must never be got wrong:

**1. What "one call may take T seconds" means to httpx.**

`httpx` applies a bare number to EVERY timeout phase separately: `connect`,
`write`, `read` and `pool` each get the full value. So `timeout=remaining`
authorises up to four times the remaining budget in the worst case, which is
how a 15-second request could answer after 21 seconds. `provider_timeout()`
returns an `httpx.Timeout` whose phases are carved OUT of the budget, so
their worst case sums to it: the handshake phases get a twelfth of it each
(capped at 1 second) and the read phase — where the model actually thinks,
because `:generateContent` is not streamed — keeps the remaining three
quarters. The share is sized against the measured stage latencies, not
picked for symmetry: see `_HANDSHAKE_SHARE` below.

(One honest limit: httpx's `read` timeout bounds the wait for the next chunk
of the body, not the whole body. A provider dribbling bytes forever inside
its read timeout cannot be cut off by httpx at all. Gemini answers these
stages with one small JSON document, and the stage boundaries in
`retrieval.select_final` re-check the deadline, so the residual risk is a
single hung call, not a summed retry ladder.)

**2. What a 429 body actually says.**

Gemini's `generativelanguage` API answers a quota rejection with
`error.details` carrying a `google.rpc.QuotaFailure` (which quota, in
`quotaId` / `quotaMetric`) and usually a `google.rpc.RetryInfo`
(`retryDelay`). The distinction that matters here is per-DAY versus
per-MINUTE: a daily free-tier quota cannot recover inside a 15-second
request, so retrying it is guaranteed waste — the request should degrade to
its verified fallback immediately instead of sleeping out the caller's
budget first. `retryDelay` alone does not carry that information (a daily
quota is observed answering "0s", "14s" and "55s" for the same violation),
so the quota id is what is read.

Real body (google-gemini/gemini-cli issue #6126, free-tier daily cap):

    {"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "...",
      "details": [
        {"@type": "type.googleapis.com/google.rpc.QuotaFailure",
         "violations": [{
           "quotaMetric":
             "generativelanguage.googleapis.com/generate_content_free_tier_requests",
           "quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier",
           "quotaDimensions": {"model": "gemini-2.5-pro", "location": "global"},
           "quotaValue": "100"}]},
        {"@type": "type.googleapis.com/google.rpc.Help", "links": [...]},
        {"@type": "type.googleapis.com/google.rpc.RetryInfo",
         "retryDelay": "14s"}]}}

Parsing is total: any body that is not exactly this shape — no JSON, a
string where a list was expected, a missing key, an unparsable delay —
means "details unknown" and the caller keeps its ordinary backoff. Nothing
in here raises.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

import httpx

from deadline import Deadline, request_timeout

# Quota scopes we distinguish. "per day" also covers per-hour quotas: the
# point of the category is that the window cannot reopen inside one
# request's budget, which is equally true of both.
QUOTA_UNKNOWN = "unknown"
QUOTA_PER_DAY = "per_day"
QUOTA_PER_MINUTE = "per_minute"

# Retryable provider statuses, shared by the three stages.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

# The smallest slice of the budget worth starting another provider call
# with. Below it a call cannot even complete its handshake, so sleeping out
# the backoff and calling anyway would only spend the budget the local
# fallback (safe pool / retrieval top-1) still has to run in.
MIN_ATTEMPT_SECONDS = 1.0

# Patience ceiling of a single backoff. Only reachable without a deadline
# (the offline indexing CLI): under a deadline the budget is stricter.
MAX_BACKOFF_SECONDS = 30.0

# One call's budget, split across httpx's four phases so that their worst
# case SUM stays inside it. connect/write/pool are handshake work —
# milliseconds against a reachable provider, and usually nothing at all
# because each stage's client keeps its connection — so they are capped low
# and the read phase keeps the rest.
#
# The share is not a free choice: `:generateContent` is NOT streamed, so the
# model's whole thinking time lands in the FIRST read and the read phase is
# effectively the stage's latency budget. The rewrite call measures 3.7-4.7 s
# (ADR 0006, live Gemini, production corpus) against a base timeout of 8 s,
# so a split leaving read half the call — 4.0 s — would have timed out the
# median production request and degraded it to the raw query, trading the
# overrun this ticket fixes for a permanent quality loss. A twelfth of the
# call each is more than a TLS handshake to a reachable Google endpoint ever
# needs, and leaves read three quarters of the budget: 6.0 s at base 8 s,
# 1.3x the slowest measured call.
_HANDSHAKE_CEILING_SECONDS = 1.0
_HANDSHAKE_SHARE = 1.0 / 12.0

_QUOTA_FAILURE_TYPE = "google.rpc.QuotaFailure"
_RETRY_INFO_TYPE = "google.rpc.RetryInfo"
_NON_ALPHANUMERIC = re.compile(r"[^a-z0-9]")


@dataclass(frozen=True)
class RateLimitInfo:
    """What a 429 body says about the quota that rejected the request."""

    scope: str = QUOTA_UNKNOWN
    # Seconds the provider asked us to wait, when it said so.
    retry_delay: float | None = None

    @property
    def hopeless(self) -> bool:
        """True when no retry inside this request can possibly succeed."""
        return self.scope == QUOTA_PER_DAY


UNKNOWN_RATE_LIMIT = RateLimitInfo()


def _quota_scope(violations) -> str:
    """Scope of the strictest quota named in a QuotaFailure detail."""
    if not isinstance(violations, list):
        return QUOTA_UNKNOWN
    scope = QUOTA_UNKNOWN
    for violation in violations:
        if not isinstance(violation, dict):
            continue
        # `GenerateRequestsPerDayPerProjectPerModel-FreeTier` and friends;
        # the metric is checked too so a differently shaped id still lands.
        # The two fields are normalised SEPARATELY and rejoined with a
        # space: concatenating them first would let an id ending in "Per"
        # and a metric starting with "day" spell "perday" across the seam
        # and mute a retry that was in fact affordable.
        text = " ".join(
            _NON_ALPHANUMERIC.sub("", str(violation.get(field, "")).lower())
            for field in ("quotaId", "quotaMetric")
        )
        if "perday" in text or "perhour" in text:
            # A violated daily quota decides the whole answer: it stays
            # violated even if a per-minute one is listed beside it.
            return QUOTA_PER_DAY
        if "perminute" in text or "persecond" in text:
            scope = QUOTA_PER_MINUTE
    return scope


def _parse_retry_delay(raw) -> float | None:
    """`"14s"` -> 14.0; anything unparsable -> None."""
    try:
        value = float(str(raw).strip().rstrip("s"))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value < 0.0:
        return None
    return value


def parse_rate_limit(response: httpx.Response) -> RateLimitInfo:
    """Read a Gemini 429 body. Never raises; unknown shapes are unknown."""
    try:
        details = response.json()["error"]["details"]
    except Exception:
        return UNKNOWN_RATE_LIMIT
    if not isinstance(details, list):
        return UNKNOWN_RATE_LIMIT
    scope = QUOTA_UNKNOWN
    delay: float | None = None
    for detail in details:
        if not isinstance(detail, dict):
            continue
        kind = str(detail.get("@type", ""))
        if kind.endswith(_QUOTA_FAILURE_TYPE):
            found = _quota_scope(detail.get("violations"))
            if found == QUOTA_PER_DAY or scope == QUOTA_UNKNOWN:
                scope = found
        elif kind.endswith(_RETRY_INFO_TYPE):
            delay = _parse_retry_delay(detail.get("retryDelay"))
    return RateLimitInfo(scope=scope, retry_delay=delay)


def rate_limit_of(response: httpx.Response) -> RateLimitInfo:
    """RateLimitInfo of a retryable response (only a 429 carries quotas)."""
    if response.status_code != 429:
        return UNKNOWN_RATE_LIMIT
    return parse_rate_limit(response)


def provider_timeout(
    deadline: Deadline | None, base: float
) -> httpx.Timeout | None:
    """Timeout of ONE provider call, or None when the budget is gone.

    The returned phases are carved out of `min(base, remaining budget)`, so
    a call cannot outlive it however the provider misbehaves — unlike a bare
    number, which httpx would hand to each phase in full.
    """
    total = request_timeout(deadline, base)
    if total <= 0.0:
        return None
    handshake = min(_HANDSHAKE_CEILING_SECONDS, total * _HANDSHAKE_SHARE)
    return httpx.Timeout(
        connect=handshake,
        write=handshake,
        pool=handshake,
        read=total - 3.0 * handshake,
    )


def retry_pause(
    deadline: Deadline | None,
    backoff: float,
    rate_limit: RateLimitInfo | None = None,
) -> float | None:
    """Seconds to wait before the next attempt, or None: do not retry.

    None means "degrade now", and is returned when

    - the provider says the quota is a daily one (retrying is hopeless), or
    - the pause plus a minimally useful call no longer fit in the budget.
      Sleeping first and failing afterwards is the trap this replaces: the
      caller would hand back an over-budget answer having learned nothing.
    """
    if rate_limit is not None:
        if rate_limit.hopeless:
            return None
        if rate_limit.retry_delay is not None:
            backoff = max(backoff, rate_limit.retry_delay)
    backoff = min(max(0.0, backoff), MAX_BACKOFF_SECONDS)
    if deadline is None:
        return backoff
    if deadline.remaining() < backoff + MIN_ATTEMPT_SECONDS:
        return None
    return backoff
