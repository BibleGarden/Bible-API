"""
In-memory rolling-window rate limiter shared by the AI endpoints
(Twinkler companion and scripture selection).

Properties (unchanged from the original Twinkler implementation, see
architect/twinkler-ai.md):

- two 60-second windows per limiter: a global one and a per-client one;
- the in-memory client identity is an HMAC pseudonym, never the address;
- missing HMAC configuration fails closed (`RateLimitError` without a
  retry hint -> the caller answers 503), so a misconfigured server cannot
  silently lose its per-client limit;
- expired timestamps and inactive client buckets are swept periodically;
- state is process-local: counters reset on restart and are not shared
  across workers, so production runs a single API worker.

Limits are passed in per reservation instead of being captured at import,
so each endpoint keeps its own configuration (and tests can override it).
"""

from __future__ import annotations

import math
import threading
import time
from collections import deque

from client_ip import pseudonymize_twinkler_client

WINDOW_SECONDS = 60.0


class RateLimitError(RuntimeError):
    """Reservation refused.

    `retry_after` is the number of seconds until a slot frees up; it is
    None when the limiter itself could not run (misconfiguration), which
    callers must translate into a 503 rather than a 429.
    """

    def __init__(self, message: str, retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after


def discard_expired(request_times: deque[float], cutoff: float) -> None:
    while request_times and request_times[0] <= cutoff:
        request_times.popleft()


def retry_after_seconds(
    request_times: deque[float], now: float, window: float = WINDOW_SECONDS
) -> int:
    if not request_times:  # a zero limit refuses with an empty window
        return max(1, math.ceil(window))
    return max(1, math.ceil(window - (now - request_times[0])))


class RateLimiter:
    """One pair of rolling windows (global + per client pseudonym)."""

    def __init__(
        self,
        window_seconds: float = WINDOW_SECONDS,
        pseudonymize=pseudonymize_twinkler_client,
        name: str = "AI",
    ):
        self.window = window_seconds
        self.name = name
        self._pseudonymize = pseudonymize
        self._lock = threading.Lock()
        self.request_times: deque[float] = deque()
        self.client_request_times: dict[str, deque[float]] = {}
        self._last_client_cleanup = 0.0

    def reset(self) -> None:
        with self._lock:
            self.request_times.clear()
            self.client_request_times.clear()
            self._last_client_cleanup = 0.0

    def reserve(self, client_key: str, global_limit: int, client_limit: int) -> None:
        """Book one request slot or raise RateLimitError."""
        try:
            client_hash = self._pseudonymize(client_key)
        except RuntimeError as error:
            raise RateLimitError(str(error)) from error

        now = time.monotonic()
        cutoff = now - self.window

        with self._lock:
            discard_expired(self.request_times, cutoff)

            if now - self._last_client_cleanup >= self.window:
                for stored_hash, stored_times in list(self.client_request_times.items()):
                    discard_expired(stored_times, cutoff)
                    if not stored_times:
                        del self.client_request_times[stored_hash]
                self._last_client_cleanup = now

            client_times = self.client_request_times.setdefault(client_hash, deque())
            discard_expired(client_times, cutoff)

            if len(self.request_times) >= global_limit:
                raise RateLimitError(
                    f"global {self.name} request limit exceeded",
                    retry_after_seconds(self.request_times, now, self.window),
                )
            if len(client_times) >= client_limit:
                raise RateLimitError(
                    f"client {self.name} request limit exceeded",
                    retry_after_seconds(client_times, now, self.window),
                )

            self.request_times.append(now)
            client_times.append(now)
