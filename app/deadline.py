"""
Cooperative time budget for a whole scripture selection
(architect/adr/0006-scripture-select-api.md).

One `Deadline` object is created per public request and threaded through
every stage that can block: query rewrite, query embeddings and the
grounded rerank. Two mechanisms use it:

- stage boundaries check `expired()` and degrade instead of starting a
  stage that cannot finish (safe pool / retrieval top-1);
- every provider HTTP call takes `budget(base)` as its timeout, through
  `gemini_retry.provider_timeout` — which splits it across httpx's four
  timeout phases, because httpx applies a bare number to each phase
  separately and would authorise four times the budget.

Retry pauses are planned by `gemini_retry.retry_pause`, not here: a backoff
that merely fits the budget is not enough — the attempt AFTER it has to fit
too, or the request sleeps out the caller's time only to give up anyway.
That was the bug this module's earlier `sleep_budget()` helper contained,
and it is why the helper is gone (ClickUp 86cbbnaxn).

Together they make the endpoint's worst case the budget itself plus the
local DB work, instead of the sum of every stage's retry ladder.
"""

from __future__ import annotations

import time


class Deadline:
    """A monotonic time budget shared by the stages of one request."""

    def __init__(self, seconds: float, clock=time.monotonic):
        self._clock = clock
        self.total = max(0.0, float(seconds))
        self.started_at = clock()

    @property
    def expires_at(self) -> float:
        return self.started_at + self.total

    def elapsed(self) -> float:
        return self._clock() - self.started_at

    def remaining(self) -> float:
        return self.expires_at - self._clock()

    def expired(self) -> bool:
        return self.remaining() <= 0.0

    def budget(self, base: float) -> float:
        """Timeout for one provider call: never more than what is left.

        Returns 0.0 when the budget is gone; callers must treat that as
        "do not start this call".
        """
        return max(0.0, min(float(base), self.remaining()))


def request_timeout(deadline: Deadline | None, base: float) -> float:
    """Total time one provider call may take under an optional deadline.

    Callers must not hand this to httpx directly — see
    `gemini_retry.provider_timeout`, which spreads it over the phases.
    """
    return base if deadline is None else deadline.budget(base)
