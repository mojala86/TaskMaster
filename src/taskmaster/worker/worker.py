"""Worker process: consumes job ids from the Redis stream and runs them.

- Claiming is a conditional UPDATE (queued/retrying -> running, backoff over) that also sets a
  lease (`JobLifecycle.claim`). Duplicate or early deliveries match no row and are dropped. The
  stream message is acked right after the claim: from then on the lease, not the message, is what
  recovers the job if this worker dies.
- Each claimed job is run by a `JobSupervisor` (heartbeats, cancellation, recording the outcome).
- Messages read but never claimed (worker died in between) are taken over with XAUTOCLAIM.
- On SIGTERM, running jobs get `worker_shutdown_timeout` to finish; the rest are released back
  to the queue without consuming an attempt.
- Lease sweeping and other background upkeep live in `maintenance.py`.
"""

import asyncio
import math
import os
import secrets
import socket
import time
import uuid

import structlog
from redis.exceptions import RedisError

from taskmaster import metrics
from taskmaster.async_utils import sleep_unless
from taskmaster.config import Settings
from taskmaster.db import Database, JobStatus
from taskmaster.jobs.lifecycle import Claim, JobLifecycle
from taskmaster.queue import Message, Streams
from taskmaster.storage import Storage
from taskmaster.worker.maintenance import Maintenance
from taskmaster.worker.supervisor import JobSupervisor

log = structlog.get_logger("taskmaster.worker")


class Worker:
    def __init__(
        self,
        db: Database,
        streams: Streams,
        settings: Settings,
        worker_id: str | None = None,
        storage: Storage | None = None,
    ) -> None:
        self.db = db
        self.streams = streams
        self.settings = settings
        self.storage = storage
        # Random suffix: a restarted container keeps its hostname and PID 1, but must not look
        # like its previous (dead) self.
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(2)}"
        self.lifecycle = JobLifecycle(settings)
        self._abort = asyncio.Event()
        self._supervisor = JobSupervisor(db, settings, self.lifecycle, self._abort, storage)
        self._last_reclaim: dict[str, float] = {}

    # --- Process lifecycle ------------------------------------------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        await self.streams.ensure_group()
        log.info(
            "Worker started",
            worker_id=self.worker_id,
            concurrency=self.settings.worker_concurrency,
        )
        consumers = [
            asyncio.create_task(self._consume(f"{self.worker_id}-{i}", stop))
            for i in range(self.settings.worker_concurrency)
        ]
        maintenance = asyncio.create_task(
            Maintenance(self.db, self.streams, self.settings, self.worker_id, self.storage).run(
                stop
            )
        )
        try:
            await stop.wait()
            # Consumers stop taking new jobs; give running ones time to finish.
            _, running = await asyncio.wait(
                consumers, timeout=self.settings.worker_shutdown_timeout
            )
            if running:
                log.warning(
                    "Shutdown timeout reached; releasing running jobs back to the queue",
                    running=len(running),
                    waited_seconds=self.settings.worker_shutdown_timeout,
                )
                self._abort.set()
                await asyncio.wait(running)
        finally:
            for t in [maintenance, *consumers]:
                t.cancel()
            await asyncio.gather(maintenance, *consumers, return_exceptions=True)
        log.info("Worker stopped", worker_id=self.worker_id)

    async def _consume(self, consumer: str, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.poll_once(consumer)
            except Exception:
                # E.g. Postgres or Redis unavailable. An unclaimed message stays pending and is
                # reclaimed later with XAUTOCLAIM.
                log.exception("Consumer failed; retrying", consumer=consumer)
                await sleep_unless(stop, 1.0)

    # --- Messages ---------------------------------------------------------------------------

    async def poll_once(self, consumer: str) -> int:
        """Take one message (a reclaimed one first, if due) and handle it. Returns the count."""
        messages = await self._reclaim_due(consumer)
        if not messages:
            messages = await self.streams.read(consumer, block_ms=self.settings.worker_block_ms)
        for message in messages:
            await self.handle_message(message)
        return len(messages)

    async def _reclaim_due(self, consumer: str) -> list[Message]:
        now = time.monotonic()
        if now - self._last_reclaim.get(consumer, -math.inf) < self.settings.reclaim_interval:
            return []
        self._last_reclaim[consumer] = now
        messages = await self.streams.reclaim(
            consumer, min_idle_ms=self.settings.reclaim_idle_ms, count=1
        )
        metrics.MESSAGES_RECLAIMED.inc(len(messages))
        for m in messages:
            log.warning("Reclaimed unacked message", message_id=m.id, job_id=m.job_id)
        return messages

    async def handle_message(self, message: Message) -> JobStatus | None:
        claim = await self.claim(uuid.UUID(message.job_id))
        try:
            await self.streams.ack(message.id)
        except RedisError:
            # Harmless: the message gets reclaimed later and skipped as a duplicate.
            log.warning("Could not ack message", message_id=message.id, exc_info=True)
        if claim is None:
            log.info("Job not claimable (duplicate or already handled)", job_id=message.job_id)
            return None
        return await self.run_claimed(claim)

    # --- Jobs -------------------------------------------------------------------------------

    async def handle(self, job_id: uuid.UUID) -> JobStatus | None:
        """Claim and run a job directly (no stream message). Returns the new status, or None if
        the job was not claimable or this worker lost it."""
        claim = await self.claim(job_id)
        return None if claim is None else await self.run_claimed(claim)

    async def claim(self, job_id: uuid.UUID) -> Claim | None:
        """Take ownership of a job; None if it isn't claimable (see `JobLifecycle.claim`)."""
        async with self.db.session() as session, session.begin():
            return await self.lifecycle.claim(session, job_id, self.worker_id)

    async def run_claimed(self, claim: Claim) -> JobStatus | None:
        """Run a claimed job to an outcome and record it (if this worker still owns the job)."""
        return await self._supervisor.run(claim)
