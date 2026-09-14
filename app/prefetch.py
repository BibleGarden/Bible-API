"""Admission control for speculative AI requests, before any generation."""

import logging
from dataclasses import dataclass, field

from fastapi import HTTPException

import config
from rate_limit import RateLimitError, RateLimiter

logger = logging.getLogger(__name__)


@dataclass
class PrefetchPolicy:
    enabled: bool
    global_limit: int
    client_limit: int
    limiter: RateLimiter = field(
        default_factory=lambda: RateLimiter(name="AI prefetch")
    )

    def enforce(self, client_key: str) -> None:
        if not self.enabled:
            raise HTTPException(status_code=429, detail="prefetch_disabled")
        try:
            self.limiter.reserve(client_key, self.global_limit, self.client_limit)
        except RateLimitError as error:
            if error.retry_after is not None:
                raise HTTPException(
                    status_code=429,
                    detail="prefetch_limit_exceeded",
                    headers={"Retry-After": str(error.retry_after)},
                ) from error
            logger.warning("AI prefetch rate limiter unavailable: %s", error)
            raise HTTPException(
                status_code=503, detail="AI prefetch temporarily unavailable"
            ) from error


question_policy = PrefetchPolicy(
    config.AI_QUESTION_PREFETCH_ENABLED,
    config.AI_QUESTION_PREFETCH_REQUESTS_PER_MINUTE,
    config.AI_QUESTION_PREFETCH_REQUESTS_PER_CLIENT_PER_MINUTE,
)
scripture_policy = PrefetchPolicy(
    config.AI_SCRIPTURE_PREFETCH_ENABLED,
    config.AI_SCRIPTURE_PREFETCH_REQUESTS_PER_MINUTE,
    config.AI_SCRIPTURE_PREFETCH_REQUESTS_PER_CLIENT_PER_MINUTE,
)
