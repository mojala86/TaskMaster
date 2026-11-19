"""Outbox relay: moves committed outbox rows to Redis Streams.

Loop:
  1. In one transaction, lock a batch of due, unpublished rows (FOR UPDATE SKIP LOCKED, so any
     number of relays can run side by side without handling the same rows).
  2. XADD them to Redis in one pipeline.
  3. Stamp published_at on the rows that made it; record the error on the rest. Commit.
  4. If the batch was not full, sleep until NOTIFY outbox_new or the poll interval elapses.

Every `outbox_cleanup_interval` it also deletes published rows older than the retention period.

Delivery is at-least-once: a crash between step 2 and the commit re-publishes the batch.
Consumers must be idempotent (see ADR 0003).
"""

import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import psycopg
import structlog
from redis.exceptions import RedisError
from sqlalchemy import CursorResult, delete, func, select

from taskmaster import metrics
from taskmaster.async_utils import sleep_unless
from taskmaster.config import Settings
from taskmaster.db import Database, Job, OutboxEvent
from taskmaster.db.models import TERMINAL_STATUSES, JobStatus
from taskmaster.queue import Streams

log = structlog.get_logger("taskmaster.relay")

NOTIFY_CHANNEL = "outbox_new"
CLEANUP_BATCH = 5000


@dataclass(frozen=True)
class BatchResult:
    fetched: int
    published: int

    @property
    def failed(self) -> int:
        return self.fetched - self.published


class Relay:
    def __init__(self, db: Database, streams: Streams, settings: Settings) -> None:
        self.db = db
        self.streams = streams
        self.settings = settings
        self._listener: psycopg.AsyncConnection[tuple[object, ...]] | None = None

    async def publish_batch(self) -> BatchResult:
        async with self.db.session() as session, session.begin():
            events = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(
                        OutboxEvent.published_at.is_(None),
                        OutboxEvent.available_at <= func.now(),
                    )
                    .order_by(OutboxEvent.id)
                    .limit(self.settings.relay_batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            if not events:
                return BatchResult(0, 0)

            try:
                async with self.streams.redis.pipeline(transaction=False) as pipe:
                    # An event can fan out to several streams (job.dead -> events + dead-letter).
                    counts = [self.streams.stage_publish(pipe, event) for event in events]
                    flat = await pipe.execute(raise_on_error=False)
                results: list[Any] = []
                for n in counts:
                    chunk, flat = flat[:n], flat[n:]
                    results.append(next((r for r in chunk if isinstance(r, Exception)), None))
            except RedisError as exc:
                results = [exc] * len(events)

            now = datetime.now(UTC)
            published = 0
            for event, error in zip(events, results, strict=True):
                if error is not None:
                    # A partially fanned-out event is republished in full: at-least-once.
                    event.attempts += 1
                    event.last_error = f"{type(error).__name__}: {error}"
                    metrics.OUTBOX_PUBLISH_ERRORS.inc()
                else:
                    event.published_at = now
                    published += 1
                    metrics.OUTBOX_PUBLISHED.labels(event.event_type).inc()

        result = BatchResult(len(events), published)
        if result.failed:
            log.warning(
                "Some outbox events failed to publish", published=published, failed=result.failed
            )
        else:
            log.debug("Published outbox events", published=published)
        return result

    async def cleanup_published(self) -> int:
        """Delete published rows older than the retention period, in bounded batches."""
        cutoff = func.now() - timedelta(hours=self.settings.outbox_retention_hours)
        deleted = 0
        while True:
            async with self.db.session() as session, session.begin():
                batch = (
                    select(OutboxEvent.id)
                    .where(OutboxEvent.published_at < cutoff)
                    .order_by(OutboxEvent.id)
                    .limit(CLEANUP_BATCH)
                    # Several relays may clean up at once: each takes rows nobody else has
                    # locked, so they never wait on (or deadlock with) each other.
                    .with_for_update(skip_locked=True)
                    .scalar_subquery()
                )
                result = cast(
                    CursorResult[Any],
                    await session.execute(delete(OutboxEvent).where(OutboxEvent.id.in_(batch))),
                )
            deleted += result.rowcount
            if result.rowcount < CLEANUP_BATCH:
                break
        if deleted:
            log.info("Deleted old published outbox rows", deleted=deleted)
        return deleted

    async def sample_queue_stats(self) -> None:
        """Update queue-depth gauges: unfinished jobs by status, stream lengths, outbox lag."""
        async with self.db.session() as session:
            counts = dict(
                (
                    await session.execute(
                        select(Job.status, func.count())
                        .where(Job.status.not_in(TERMINAL_STATUSES))
                        .group_by(Job.status)
                    )
                ).all()
            )
            due, oldest = (
                await session.execute(
                    select(func.count(), func.min(OutboxEvent.available_at)).where(
                        OutboxEvent.published_at.is_(None), OutboxEvent.available_at <= func.now()
                    )
                )
            ).one()
            now = await session.scalar(select(func.now()))
        for status in JobStatus:
            if status not in TERMINAL_STATUSES:
                metrics.JOBS_BY_STATUS.labels(status.value).set(counts.get(status, 0))
        metrics.OUTBOX_DUE.set(due)
        metrics.OUTBOX_LAG.set((now - oldest).total_seconds() if oldest and now else 0)

        redis = self.streams.redis
        s = self.settings
        for stream in (s.jobs_stream, s.events_stream, s.dead_letter_stream):
            metrics.STREAM_LENGTH.labels(stream).set(await redis.xlen(stream))
        pending = await redis.xpending(s.jobs_stream, s.consumer_group)
        metrics.STREAM_PENDING.set(pending["pending"] if pending else 0)

    async def _sample_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.sample_queue_stats()
            except Exception:
                log.warning("Sampling queue stats failed", exc_info=True)
            await sleep_unless(stop, self.settings.queue_stats_interval)

    async def run(self, stop: asyncio.Event) -> None:
        await self.streams.ensure_group()
        log.info("Relay started")
        next_cleanup = time.monotonic()
        sampler = asyncio.create_task(self._sample_forever(stop))
        try:
            while not stop.is_set():
                if time.monotonic() >= next_cleanup:
                    next_cleanup = time.monotonic() + self.settings.outbox_cleanup_interval
                    try:
                        await self.cleanup_published()
                    except Exception:
                        log.exception("Outbox cleanup failed")
                try:
                    result = await self.publish_batch()
                except Exception:
                    log.exception("Relay batch failed")
                    await sleep_unless(stop, self.settings.relay_error_backoff)
                    continue
                if result.failed:
                    await sleep_unless(stop, self.settings.relay_error_backoff)
                elif result.fetched < self.settings.relay_batch_size:
                    await self._wait_for_notify(stop)
                # A full batch loops immediately: there is probably more backlog.
        finally:
            sampler.cancel()
            if self._listener is not None:
                await self._listener.close()
        log.info("Relay stopped")

    async def _wait_for_notify(self, stop: asyncio.Event) -> None:
        timeout = self.settings.relay_poll_interval
        try:
            if self._listener is None or self._listener.closed:
                self._listener = await psycopg.AsyncConnection.connect(
                    self.settings.psycopg_dsn, autocommit=True
                )
                await self._listener.execute(f"LISTEN {NOTIFY_CHANNEL}")
            async for _ in self._listener.notifies(timeout=timeout, stop_after=1):
                pass
        except psycopg.Error:
            log.warning("LISTEN connection failed; falling back to polling", exc_info=True)
            if self._listener is not None:
                await self._listener.close()
                self._listener = None
            await sleep_unless(stop, timeout)
