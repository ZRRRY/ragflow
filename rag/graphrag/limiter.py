# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Adaptive concurrency limiter for KG tasks.

Replaces a static ``asyncio.Semaphore`` with one whose limit adjusts based on
runtime signals (LLM rate limits, doc-store latency, CAS conflicts).
"""

from __future__ import annotations

import asyncio
import collections
import logging
import time
from typing import Counter, Deque, Optional

logger = logging.getLogger(__name__)

# Global reference set by ``task_executor.py`` so downstream modules can
# record events without importing the executor.
current_limiter: Optional["AdaptiveConcurrencyLimiter"] = None


class _SlidingWindow:
    """Thread-safe (asyncio-safe) sliding window counter."""

    def __init__(self, bucket_count: int = 6, bucket_seconds: int = 30):
        self.bucket_count = bucket_count
        self.bucket_seconds = bucket_seconds
        # Each entry: [bucket_key, Counter]
        self._buckets: Deque[list] = collections.deque()
        self._lock = asyncio.Lock()

    def _bucket_key(self) -> int:
        return int(time.time()) // self.bucket_seconds

    async def add(self, event_type: str) -> None:
        key = self._bucket_key()
        async with self._lock:
            cutoff = key - self.bucket_count + 1
            while self._buckets and self._buckets[0][0] < cutoff:
                self._buckets.popleft()

            if not self._buckets or self._buckets[-1][0] != key:
                self._buckets.append([key, collections.Counter()])

            self._buckets[-1][1][event_type] += 1

    async def summary(self) -> dict[str, int]:
        key = self._bucket_key()
        async with self._lock:
            cutoff = key - self.bucket_count + 1
            while self._buckets and self._buckets[0][0] < cutoff:
                self._buckets.popleft()

            total: Counter = collections.Counter()
            for _, ctr in self._buckets:
                total.update(ctr)
            return dict(total)


class AdaptiveConcurrencyLimiter:
    """Asyncio-compatible semaphore with dynamic limit adjustment.

    Usage::

        limiter = AdaptiveConcurrencyLimiter(initial_limit=4, min_limit=1, max_limit=10)
        limiter.start_monitoring()
        async with limiter:
            ...  # protected work
    """

    def __init__(
        self,
        initial_limit: int,
        min_limit: int = 1,
        max_limit: int = 20,
        adjust_interval: int = 30,
        degrade_threshold: int = 2,
        increase_threshold: int = 6,
    ):
        self.min_limit = max(1, min_limit)
        self.max_limit = max(self.min_limit, max_limit)
        self.adjust_interval = adjust_interval
        self.degrade_threshold = degrade_threshold
        self.increase_threshold = increase_threshold

        self._limit = max(self.min_limit, min(initial_limit, self.max_limit))
        self._count = 0
        self._waiters: Deque[asyncio.Future] = collections.deque()
        self._lock = asyncio.Lock()
        self._events = _SlidingWindow()
        self._adjust_task: Optional[asyncio.Task] = None
        self._shutdown = False

        logger.info(
            "AdaptiveConcurrencyLimiter initial=%d min=%d max=%d interval=%ds degrade_thr=%d increase_thr=%d",
            self._limit,
            self.min_limit,
            self.max_limit,
            self.adjust_interval,
            self.degrade_threshold,
            self.increase_threshold,
        )

    # ------------------------------------------------------------------
    # Semaphore-like API
    # ------------------------------------------------------------------

    async def acquire(self) -> None:
        async with self._lock:
            if self._count < self._limit:
                self._count += 1
                return
            fut = asyncio.get_running_loop().create_future()
            self._waiters.append(fut)
        await fut

    def release(self) -> None:
        """Release a permit (safe to call from the event-loop thread)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._release_async())

    async def _release_async(self) -> None:
        async with self._lock:
            if self._waiters:
                fut = self._waiters.popleft()
                if not fut.done():
                    fut.set_result(None)
            else:
                self._count = max(0, self._count - 1)

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()
        return False

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def current_tasks(self) -> int:
        return self._count

    # ------------------------------------------------------------------
    # Event recording
    # ------------------------------------------------------------------

    async def record_event(self, event_type: str) -> None:
        await self._events.add(event_type)

    def record_event_sync(self, event_type: str) -> None:
        """Fire-and-forget event recording for sync contexts."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.record_event(event_type))
        except RuntimeError:
            pass

    # ------------------------------------------------------------------
    # Adjustment loop
    # ------------------------------------------------------------------

    def start_monitoring(self) -> None:
        if self._adjust_task is None or self._adjust_task.done():
            self._shutdown = False
            self._adjust_task = asyncio.create_task(self._adjust_loop())
            logger.info("AdaptiveConcurrencyLimiter monitoring started")

    def stop_monitoring(self) -> None:
        self._shutdown = True
        if self._adjust_task and not self._adjust_task.done():
            self._adjust_task.cancel()
            logger.info("AdaptiveConcurrencyLimiter monitoring stopped")

    async def _adjust_loop(self) -> None:
        while not self._shutdown:
            try:
                await asyncio.sleep(self.adjust_interval)
                await self._do_adjust()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("AdaptiveConcurrencyLimiter adjustment loop error")

    async def _do_adjust(self) -> None:
        summary = await self._events.summary()
        bad = (
            summary.get("llm_rate_limit", 0)
            + summary.get("cas_conflict", 0)
            + summary.get("es_slow", 0)
        )
        good = summary.get("success", 0)
        old_limit = self._limit

        if bad >= self.degrade_threshold:
            self._limit = max(self.min_limit, self._limit - 1)
            logger.warning(
                "AdaptiveLimiter degraded: bad=%d good=%d limit %d -> %d",
                bad,
                good,
                old_limit,
                self._limit,
            )
        elif good >= self.increase_threshold and bad == 0 and self._limit < self.max_limit:
            self._limit = min(self.max_limit, self._limit + 1)
            logger.info(
                "AdaptiveLimiter increased: bad=%d good=%d limit %d -> %d",
                bad,
                good,
                old_limit,
                self._limit,
            )

        # If limit increased, wake up waiters
        if self._limit > old_limit:
            async with self._lock:
                to_wake = min(self._limit - old_limit, len(self._waiters))
                for _ in range(to_wake):
                    fut = self._waiters.popleft()
                    if not fut.done():
                        fut.set_result(None)
