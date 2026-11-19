"""Background maintenance that every worker process runs alongside its consumers.

- Expired-lease sweep: running jobs whose worker stopped heartbeating (crash, kill -9,
  partition) go back to `retrying`, or to `dead` when out of attempts (ADR 0004).
- Stale re-dispatch: queued jobs whose stream message was lost get a fresh dispatch event.
- Storage janitor: scheduled about once per interval across the whole cluster.

Every step is safe to run concurrently in any number of workers (SKIP LOCKED, conditional
updates, a Redis slot plus an advisory lock for the janitor).
"""

import asyncio

import structlog

from taskmaster import metrics
from taskmaster.async_utils import sleep_unless
from taskmaster.config import Settings
from taskmaster.db import Database
from taskmaster.janitor import StorageJanitor
from taskmaster.jobs.lifecycle import JobLifecycle
from taskmaster.queue import Streams
from taskmaster.storage import Storage

log = structlog.get_logger("taskmaster.worker.maintenance")

SWEEP_BATCH = 100
JANITOR_SLOT_KEY = "taskmaster:janitor:slot"


class Maintenance:
    def __init__(
        self,
        db: Database,
        streams: Streams,
        settings: Settings,
        worker_id: str,
        storage: Storage | None = None,
    ) -> None:
        self.db = db
        self.streams = streams
        self.settings = settings
        self.worker_id = worker_id
        self.storage = storage
        self.lifecycle = JobLifecycle(settings)

    async def run(self, stop: asyncio.Event) -> None:
        loops = [self._sweep_forever(stop)]
        if self.storage is not None:
            loops.append(self._clean_storage_forever(stop, self.storage))
        await asyncio.gather(*loops)

    async def _sweep_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.sweep_expired_leases()
                await self.redispatch_stale()
            except Exception:
                log.exception("Maintenance failed")
            await sleep_unless(stop, self.settings.maintenance_interval)

    async def _clean_storage_forever(self, stop: asyncio.Event, storage: Storage) -> None:
        """Run the storage janitor about once per `storage_cleanup_interval` across all workers.

        SET NX EX is the cluster-wide schedule (the first worker to claim the slot runs it); the
        janitor's advisory lock additionally prevents overlapping runs.
        """
        janitor = StorageJanitor(self.db, storage, self.settings)
        interval = self.settings.storage_cleanup_interval
        while not stop.is_set():
            try:
                if await self.streams.redis.set(
                    JANITOR_SLOT_KEY, self.worker_id, nx=True, ex=max(1, int(interval))
                ):
                    await janitor.run_once()
            except Exception:
                log.exception("Storage cleanup failed")
            await sleep_unless(stop, min(60.0, interval))

    async def sweep_expired_leases(self) -> int:
        """Recover running jobs whose worker stopped heartbeating (see JobLifecycle)."""
        async with self.db.session() as session, session.begin():
            recovered = await self.lifecycle.recover_expired_leases(session, SWEEP_BATCH)
        metrics.LEASES_EXPIRED.inc(len(recovered))
        return len(recovered)

    async def redispatch_stale(self) -> int:
        """Give queued jobs whose stream message was lost a fresh dispatch event."""
        async with self.db.session() as session, session.begin():
            count = await self.lifecycle.redispatch_stale(session, SWEEP_BATCH)
        if count:
            metrics.JOBS_REDISPATCHED.inc(count)
            log.warning("Re-dispatched stale jobs", count=count)
        return count
