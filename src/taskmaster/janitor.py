"""Storage janitor: deletes objects nothing needs any more.

A bucket lifecycle rule can't do this safely. It only sees prefixes and ages, so expiring
`results/` would also delete the outputs of succeeded jobs that the database still lists. The
janitor asks Postgres what is still referenced:

- results/<job>/<attempt id>/...  Deleted when no job_outputs row references the key and the
                                  attempt has been over for `orphan_grace_hours` (failed, lost,
                                  cancelled or released attempts). Files of running attempts stay.
- uploads/<uuid>/<name>           Deleted after `upload_retention_hours`, unless a job that hasn't
                                  finished (queued, running, retrying) still references it.
- succeeded outputs               Only if `output_retention_hours` is set: the job_outputs rows are
                                  deleted first, then the files, so no row ever points at a
                                  missing file.

Any number of workers may call run_once(); a Postgres advisory lock lets one run at a time.
"""

import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import delete, func, select, text

from taskmaster.config import Settings
from taskmaster.db import AttemptStatus, Database, Job, JobAttempt, JobOutput
from taskmaster.db.models import TERMINAL_STATUSES
from taskmaster.storage import ListedObject, Storage

log = structlog.get_logger("taskmaster.janitor")

LOCK_ID = 0x74_61_73_6B  # arbitrary, constant across processes ("task")
BATCH = 1000
RESULT_KEY = re.compile(r"^results/(?P<job>[0-9a-f-]{36})/(?P<attempt>\d+)/")


@dataclass
class CleanupReport:
    orphaned_results: int = 0
    expired_uploads: int = 0
    expired_outputs: int = 0
    unrecognized_keys: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


class StorageJanitor:
    def __init__(self, db: Database, storage: Storage, settings: Settings) -> None:
        self.db = db
        self.storage = storage
        self.settings = settings

    async def run_once(self) -> CleanupReport | None:
        """Run a full cleanup pass. Returns None if another process is already running one."""
        async with self.db.engine.connect() as raw:
            # Autocommit: hold the session-level lock without an open transaction.
            conn = await raw.execution_options(isolation_level="AUTOCOMMIT")
            if not await conn.scalar(text("SELECT pg_try_advisory_lock(:id)"), {"id": LOCK_ID}):
                log.debug("Storage cleanup already running elsewhere")
                return None
            try:
                report = await self._cleanup()
            finally:
                await conn.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": LOCK_ID})
        if any(report.as_dict().values()):
            log.info("Storage cleanup finished", **report.as_dict())
        return report

    async def _cleanup(self) -> CleanupReport:
        report = CleanupReport()
        if self.settings.output_retention_hours is not None:
            report.expired_outputs = await self._expire_outputs(
                self.settings.output_retention_hours
            )
        report.orphaned_results, report.unrecognized_keys = await self._delete_orphaned_results()
        report.expired_uploads = await self._delete_stale_uploads()
        return report

    # --- Succeeded outputs past retention ---------------------------------------------------

    async def _expire_outputs(self, retention_hours: float) -> int:
        async with self.db.session() as session, session.begin():
            keys = (
                await session.scalars(
                    delete(JobOutput)
                    .where(JobOutput.created_at < func.now() - timedelta(hours=retention_hours))
                    .returning(JobOutput.key)
                )
            ).all()
        # Rows first: if deleting the files fails now, they are unreferenced and the orphan pass
        # removes them next time.
        return await self.storage.delete_many(list(keys))

    # --- Orphaned result files --------------------------------------------------------------

    async def _delete_orphaned_results(self) -> tuple[int, int]:
        cutoff = datetime.now(UTC) - timedelta(hours=self.settings.orphan_grace_hours)
        deleted = unrecognized = 0
        async for batch in _batches(self.storage.list_objects("results/")):
            by_attempt: dict[str, int] = {}
            for obj in batch:
                match = RESULT_KEY.match(obj.key)
                if match:
                    by_attempt[obj.key] = int(match["attempt"])
                else:
                    unrecognized += 1  # not ours to judge; leave it
            if not by_attempt:
                continue

            async with self.db.session() as session:
                referenced = set(
                    await session.scalars(
                        select(JobOutput.key).where(JobOutput.key.in_(list(by_attempt)))
                    )
                )
                attempts: dict[int, Any] = {
                    row.id: row
                    for row in await session.execute(
                        select(JobAttempt.id, JobAttempt.status, JobAttempt.finished_at).where(
                            JobAttempt.id.in_(set(by_attempt.values()))
                        )
                    )
                }

            doomed = []
            for obj in batch:
                if obj.key not in by_attempt or obj.key in referenced:
                    continue
                attempt = attempts.get(by_attempt[obj.key])
                if attempt is None:
                    # Attempt row gone (job deleted): judge by the object's own age.
                    if obj.last_modified < cutoff:
                        doomed.append(obj.key)
                elif (
                    attempt.status != AttemptStatus.RUNNING
                    and attempt.finished_at is not None
                    and attempt.finished_at < cutoff
                ):
                    doomed.append(obj.key)
            deleted += await self.storage.delete_many(doomed)
        return deleted, unrecognized

    # --- Stale uploads ----------------------------------------------------------------------

    async def _delete_stale_uploads(self) -> int:
        cutoff = datetime.now(UTC) - timedelta(hours=self.settings.upload_retention_hours)
        in_use = await self._upload_keys_of_unfinished_jobs()
        deleted = 0
        async for batch in _batches(self.storage.list_objects("uploads/")):
            deleted += await self.storage.delete_many(
                [o.key for o in batch if o.last_modified < cutoff and o.key not in in_use]
            )
        return deleted

    async def _upload_keys_of_unfinished_jobs(self) -> set[str]:
        async with self.db.session() as session:
            payloads = await session.scalars(
                select(Job.payload).where(Job.status.not_in(TERMINAL_STATUSES))
            )
            return {key for payload in payloads for key in _upload_keys(payload)}


def _upload_keys(value: Any) -> Iterable[str]:
    """All strings in a JSON payload that look like upload keys (any nesting depth)."""
    if isinstance(value, str):
        if value.startswith("uploads/"):
            yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from _upload_keys(v)
    elif isinstance(value, list):
        for v in value:
            yield from _upload_keys(v)


async def _batches(objects: Any, size: int = BATCH) -> Any:
    batch: list[ListedObject] = []
    async for obj in objects:
        batch.append(obj)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
