"""
Unit tests for the shared Gemini retry policy (app/gemini_retry.py,
ClickUp 86cbbnaxn).

Two contracts:

- a 429 body is read for the QUOTA that rejected the request (per day vs
  per minute) and for its RetryInfo delay, and any body that is not exactly
  the documented shape means "unknown" instead of raising;
- a retry is planned only when the pause AND the attempt after it still fit
  in the request's budget — an exhausted daily quota is never worth waiting
  for at all.
"""

import os

import httpx
import pytest

os.environ.setdefault("API_KEY", "test-api-key")

from deadline import Deadline
from gemini_retry import (
    MAX_BACKOFF_SECONDS,
    MIN_ATTEMPT_SECONDS,
    QUOTA_PER_DAY,
    QUOTA_PER_MINUTE,
    QUOTA_UNKNOWN,
    parse_rate_limit,
    provider_timeout,
    rate_limit_of,
    retry_pause,
)


class FakeClock:
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def quota_body(quota_id: str, retry_delay: str | None = "14s") -> dict:
    """The real free-tier shape (google-gemini/gemini-cli issue #6126)."""
    details = [
        {
            "@type": "type.googleapis.com/google.rpc.QuotaFailure",
            "violations": [{
                "quotaMetric": (
                    "generativelanguage.googleapis.com/"
                    "generate_content_free_tier_requests"
                ),
                "quotaId": quota_id,
                "quotaDimensions": {
                    "model": "gemini-2.5-pro", "location": "global",
                },
                "quotaValue": "100",
            }],
        },
        {
            "@type": "type.googleapis.com/google.rpc.Help",
            "links": [{
                "description": "Learn more about Gemini API quotas",
                "url": "https://ai.google.dev/gemini-api/docs/rate-limits",
            }],
        },
    ]
    if retry_delay is not None:
        details.append({
            "@type": "type.googleapis.com/google.rpc.RetryInfo",
            "retryDelay": retry_delay,
        })
    return {
        "error": {
            "code": 429,
            "message": "You exceeded your current quota.",
            "status": "RESOURCE_EXHAUSTED",
            "details": details,
        }
    }


def response_429(body) -> httpx.Response:
    return httpx.Response(429, json=body)


# ---------------------------------------------------------------------------
# Reading the 429 body
# ---------------------------------------------------------------------------

def test_daily_quota_is_recognised_with_its_retry_delay():
    info = parse_rate_limit(response_429(
        quota_body("GenerateRequestsPerDayPerProjectPerModel-FreeTier")
    ))
    assert info.scope == QUOTA_PER_DAY
    assert info.retry_delay == 14.0
    assert info.hopeless is True


def test_minute_quota_is_recognised_and_stays_retryable():
    info = parse_rate_limit(response_429(
        quota_body(
            "GenerateRequestsPerMinutePerProjectPerModel-FreeTier",
            retry_delay="7s",
        )
    ))
    assert info.scope == QUOTA_PER_MINUTE
    assert info.retry_delay == 7.0
    assert info.hopeless is False


def test_token_quota_metric_per_minute_is_recognised():
    info = parse_rate_limit(response_429(
        quota_body("GenerateContentInputTokensPerModelPerMinute-FreeTier")
    ))
    assert info.scope == QUOTA_PER_MINUTE


def test_the_id_and_the_metric_are_not_matched_across_their_seam():
    """`quotaId` and `quotaMetric` are read as two strings. Glued together
    they could spell "perday" across the join and mute an affordable retry
    for good — a per-day verdict is final, so a false one costs the stage."""
    body = quota_body("SomethingPer")
    body["error"]["details"][0]["violations"][0]["quotaMetric"] = "day_requests"
    assert parse_rate_limit(response_429(body)).scope == QUOTA_UNKNOWN


def test_a_daily_violation_beside_a_minute_one_decides():
    """One 429 can list several violations (gemini-cli issue #8437). The
    daily one is the binding constraint: it stays violated after any pause
    a request can afford."""
    body = quota_body("GenerateRequestsPerMinutePerProjectPerModel-FreeTier")
    body["error"]["details"][0]["violations"].append({
        "quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier",
    })
    assert parse_rate_limit(response_429(body)).scope == QUOTA_PER_DAY


def test_a_daily_quota_answering_zero_seconds_is_still_hopeless():
    """The retry delay does NOT carry the scope: the same daily violation is
    observed answering "0s", "14s" and "55s"."""
    info = parse_rate_limit(response_429(
        quota_body(
            "GenerateRequestsPerDayPerProjectPerModel-FreeTier",
            retry_delay="0s",
        )
    ))
    assert info.hopeless is True
    assert info.retry_delay == 0.0


@pytest.mark.parametrize("body", [
    {},
    {"error": {}},
    {"error": {"details": "not a list"}},
    {"error": {"details": [None, 42, "text"]}},
    {"error": {"details": [{"@type": "type.googleapis.com/google.rpc.Help"}]}},
    {"error": {"details": [{
        "@type": "type.googleapis.com/google.rpc.QuotaFailure",
        "violations": "not a list",
    }]}},
    {"error": {"details": [{
        "@type": "type.googleapis.com/google.rpc.QuotaFailure",
        "violations": [{"quotaId": "SomethingUnfamiliar"}],
    }]}},
    {"error": {"details": [{
        "@type": "type.googleapis.com/google.rpc.RetryInfo",
        "retryDelay": "soon",
    }]}},
    {"error": {"details": [{
        "@type": "type.googleapis.com/google.rpc.RetryInfo",
        "retryDelay": None,
    }]}},
    [1, 2, 3],
])
def test_unexpected_bodies_are_unknown_details_not_exceptions(body):
    info = parse_rate_limit(response_429(body))
    assert info.scope == QUOTA_UNKNOWN
    assert info.hopeless is False


def test_a_body_that_is_not_json_at_all_is_unknown():
    info = parse_rate_limit(httpx.Response(429, content=b"<html>429</html>"))
    assert info.scope == QUOTA_UNKNOWN
    assert info.retry_delay is None


def test_a_negative_retry_delay_is_ignored():
    body = quota_body(
        "GenerateRequestsPerMinutePerProjectPerModel-FreeTier",
        retry_delay="-5s",
    )
    assert parse_rate_limit(response_429(body)).retry_delay is None


def test_only_a_429_is_read_for_quota_details():
    """A 503 body is never a QuotaFailure; reading one would be guesswork."""
    assert rate_limit_of(httpx.Response(503)).scope == QUOTA_UNKNOWN
    assert rate_limit_of(response_429(
        quota_body("GenerateRequestsPerDayPerProjectPerModel-FreeTier")
    )).scope == QUOTA_PER_DAY


# ---------------------------------------------------------------------------
# Planning the pause
# ---------------------------------------------------------------------------

def test_without_a_deadline_the_backoff_is_kept():
    assert retry_pause(None, 2.0) == 2.0
    assert retry_pause(None, 9999.0) == MAX_BACKOFF_SECONDS


def test_a_daily_quota_is_never_worth_waiting_for():
    deadline = Deadline(15.0, clock=FakeClock())
    info = parse_rate_limit(response_429(
        quota_body("GenerateRequestsPerDayPerProjectPerModel-FreeTier")
    ))
    assert retry_pause(deadline, 2.0, info) is None
    assert retry_pause(None, 2.0, info) is None


def test_a_minute_quota_is_waited_out_when_the_budget_allows():
    deadline = Deadline(15.0, clock=FakeClock())
    info = parse_rate_limit(response_429(
        quota_body(
            "GenerateRequestsPerMinutePerProjectPerModel-FreeTier",
            retry_delay="7s",
        )
    ))
    # the provider's delay wins over our own shorter backoff
    assert retry_pause(deadline, 2.0, info) == 7.0


def test_a_minute_quota_longer_than_the_budget_degrades_now():
    """The bug this replaces: the pause used to be clipped to the remaining
    budget, so the request slept out every second it had left and then found
    it had none for the attempt it slept for."""
    clock = FakeClock()
    deadline = Deadline(15.0, clock=clock)
    clock.advance(10.0)
    info = parse_rate_limit(response_429(
        quota_body(
            "GenerateRequestsPerMinutePerProjectPerModel-FreeTier",
            retry_delay="55s",
        )
    ))
    assert retry_pause(deadline, 2.0, info) is None


def test_a_pause_leaving_no_room_for_the_attempt_degrades_now():
    clock = FakeClock()
    deadline = Deadline(15.0, clock=clock)
    clock.advance(15.0 - (2.0 + MIN_ATTEMPT_SECONDS) + 0.1)
    assert retry_pause(deadline, 2.0) is None

    clock.now = 1000.0
    clock.advance(15.0 - (2.0 + MIN_ATTEMPT_SECONDS) - 0.1)
    assert retry_pause(deadline, 2.0) == 2.0


# ---------------------------------------------------------------------------
# The per-call timeout
# ---------------------------------------------------------------------------

def test_every_httpx_phase_is_carved_out_of_the_same_budget():
    """httpx applies a bare number to EVERY phase, so `timeout=remaining`
    authorises ~4x the budget. The phases must sum to it instead."""
    timeout = provider_timeout(Deadline(15.0, clock=FakeClock()), 8.0)
    phases = [timeout.connect, timeout.write, timeout.pool, timeout.read]
    assert all(phase > 0 for phase in phases)
    assert sum(phases) == pytest.approx(8.0)
    # the model thinks during the read phase: it keeps the lion's share
    assert timeout.read >= 6.0


@pytest.mark.parametrize("total", [
    0.05, 0.5, 1.0, 1.4, 1.5, 3.0, 7.9, 8.0, 12.0, 15.0, 60.0, 600.0,
])
def test_the_phases_always_sum_to_the_call_and_read_keeps_three_quarters(total):
    """The invariant, over the whole range: nothing is granted twice, no
    phase is zero (httpx reads 0 as "no timeout" nowhere, but a zero phase
    would fail every call), and read never drops below 3/4."""
    timeout = provider_timeout(Deadline(total, clock=FakeClock()), 1e9)
    phases = [timeout.connect, timeout.write, timeout.pool, timeout.read]
    assert all(phase > 0 for phase in phases)
    assert sum(phases) == pytest.approx(total)
    assert timeout.read >= 0.75 * total * (1 - 1e-9)


def test_the_read_phase_outlasts_the_slowest_measured_stage():
    """Regression guard on the SPLIT, not just on the sum.

    `:generateContent` is not streamed, so the model's whole thinking time
    lands in the first read — the read phase IS the stage's latency budget.
    The rewrite call measures 3.7-4.7 s (ADR 0006, live Gemini) against the
    8 s serve-time base, so a split leaving read half of it (4.0 s) would
    time out the median production request and degrade a healthy pipeline
    to the raw query. Whatever the split, read must clear the slowest
    measured call with room to spare.
    """
    slowest_measured_call = 4.7  # ADR 0006, query rewrite
    timeout = provider_timeout(Deadline(15.0, clock=FakeClock()), 8.0)
    assert timeout.read >= slowest_measured_call * 1.2


def test_the_call_timeout_never_exceeds_what_is_left():
    clock = FakeClock()
    deadline = Deadline(15.0, clock=clock)
    clock.advance(12.0)
    timeout = provider_timeout(deadline, 8.0)
    assert sum([
        timeout.connect, timeout.write, timeout.pool, timeout.read
    ]) == pytest.approx(3.0)


def test_no_timeout_at_all_once_the_budget_is_gone():
    assert provider_timeout(Deadline(0.0, clock=FakeClock()), 8.0) is None


def test_without_a_deadline_the_base_is_the_whole_call():
    timeout = provider_timeout(None, 60.0)
    assert sum([
        timeout.connect, timeout.write, timeout.pool, timeout.read
    ]) == pytest.approx(60.0)
