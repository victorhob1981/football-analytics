from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from threading import Lock


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: int = 0


class FixedWindowRateLimiter:
    def __init__(self, *, max_requests: int, window_seconds: int) -> None:
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._lock = Lock()
        self._buckets: dict[str, deque[float]] = {}

    def allow(self, key: str) -> RateLimitDecision:
        now = time.monotonic()
        cutoff = now - self._window_seconds

        with self._lock:
            bucket = self._buckets.setdefault(key, deque())
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= self._max_requests:
                retry_after_seconds = max(1, int(math.ceil(self._window_seconds - (now - bucket[0]))))
                return RateLimitDecision(allowed=False, retry_after_seconds=retry_after_seconds)

            bucket.append(now)
            return RateLimitDecision(allowed=True)
