"""Job lifecycle: every change to a job's status happens here, and only here.

    queued ──claim──▶ running ──▶ succeeded
      ▲ ▲  │             │ ├──▶ retrying ──claim, after backoff──▶ running
      │ │  │             │ ├──▶ dead ───────┐
      │ │  └─cancel──▶ cancelled ◀──────────┤ (running: stops at the next heartbeat)
      │ └──release (worker shutdown)────────┘
      └────────redrive (POST /jobs/{id}/retry) from dead or cancelled

Rules:

- Each function works inside the caller's transaction and stages the matching outbox event, so
  the status change and its event commit together or not at all (ADR 0002). Nothing here commits.
- A move not listed in ALLOWED raises JobConflict.
- A worker's changes only apply while it still owns the job (status running, its worker_id), so a
  worker that lost its lease can't overwrite what happened since (ADR 0004).
- A failure is retried with backoff while attempts remain, otherwise the job is dead. Workers and
  the expired-lease sweeper share that decision (`_retry_delay`).
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast

import structlog
from sqlalchemy import CursorResult, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from taskmaster.config import Settings
from taskmaster.db.models import (
    CLAIMABLE_STATUSES,
    TERMINAL_STATUSES,
    AttemptStatus,
    Job,
    JobAttempt,
    JobOutput,
    JobStatus,
    OutboxEvent,
)
from taskmaster.db.outbox import add_event
from taskmaster.errors import PermanentError
from taskmaster.jobs.errors import JobConflict
from taskmaster.jobs.events import (
    JobCancelled,
    JobCreated,
    JobDead,
    JobEvent,
    JobRedispatched,
    JobReleased,
    JobRequeued,
    JobRetryScheduled,
    JobSucceeded,
)
from taskmaster.jobs.retry import retry_at, retry_delay
from taskmaster.tasks.context import OutputFile

log = structlog.get_logger("taskmaster.jobs")

S = JobStatus
ALLOWED: dict[JobStatus, frozenset[JobStatus]] = {
    S.QUEUED: frozenset({S.RUNNING, S.CANCELLED}),
    S.RETRYING: frozenset({S.RUNNING, S.CANCELLED}),
    S.RUNNING: frozenset({S.SUCCEEDED, S.RETRYING, S.DEAD, S.CANCELLED, S.QUEUED}),
    S.DEAD: frozenset({S.QUEUED}),
    S.CANCELLED: frozenset({S.QUEUED}),
    S.SUCCEEDED: frozenset(),
}


def check_transition(current: JobStatus, target: JobStatus) -> None:
    if target not in ALLOWED[current]:
        raise JobConflict(f"A {current} job can't become {target}")


@dataclass(frozen=True)
class Claim:
    """A job a worker has claimed (status running, lease held) but not finished."""

    job_id: uuid.UUID
    worker_id: str
    type: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    attempt_id: int  # job_attempts.id: unique per run, used to namespace output files
    created_at: datetime


class JobLifecycle:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    # --- API ----------------------------------------------------------------------------------

    def create(self, session: AsyncSession, job: Job) -> None:
        job.status = JobStatus.QUEUED
        session.add(job)
        add_event(session, JobCreated(job_id=job.id, job_type=job.type))

    def cancel(self, session: AsyncSession, job: Job) -> bool:
        """Cancel a job locked FOR UPDATE. Returns True if it is cancelled now, False if it is
        running and will stop at its next heartbeat."""
        if job.status in TERMINAL_STATUSES:
            raise JobConflict(f"Job is already {job.status}")
        if job.status == JobStatus.RUNNING:
            job.cancel_requested = True
            return False
        # Not running anywhere: a dispatch message still in flight will fail to claim it.
        self._move(job, JobStatus.CANCELLED, error="Cancelled by request")
        add_event(session, JobCancelled(job_id=job.id, job_type=job.type))
        return True

    def redrive(self, session: AsyncSession, job: Job, additional_attempts: int) -> None:
        """Send a dead or cancelled job (locked FOR UPDATE) back to the queue."""
        if job.status not in (JobStatus.DEAD, JobStatus.CANCELLED):
            raise JobConflict(f"Only dead or cancelled jobs can be retried (is {job.status})")
        self._move(job, JobStatus.QUEUED)
        job.max_attempts = job.attempts + additional_attempts
        job.cancel_requested = False
        job.finished_at = None
        job.next_attempt_at = None
        add_event(
            session,
            JobRequeued(job_id=job.id, job_type=job.type, max_attempts=job.max_attempts),
        )

    # --- Worker ---------------------------------------------------------------------------

    async def claim(self, session: AsyncSession, job_id: uuid.UUID, worker_id: str) -> Claim | None:
        """Take a queued/retrying job: status running, a lease, an attempt row.

        Returns None if it isn't claimable: running elsewhere, finished, cancelled, or still in its
        retry backoff (a duplicate or stale message arrived early).
        """
        row = (
            await session.execute(
                update(Job)
                .where(
                    Job.id == job_id,
                    Job.status.in_(CLAIMABLE_STATUSES),
                    or_(Job.next_attempt_at.is_(None), Job.next_attempt_at <= func.now()),
                )
                .values(
                    status=JobStatus.RUNNING,
                    attempts=Job.attempts + 1,
                    worker_id=worker_id,
                    started_at=func.now(),
                    lease_until=self._lease_expiry(),
                    next_attempt_at=None,
                    progress=None,
                    progress_message=None,
                )
                .returning(Job.type, Job.payload, Job.attempts, Job.max_attempts, Job.created_at)
            )
        ).one_or_none()
        if row is None:
            return None
        attempt = JobAttempt(
            job_id=job_id, attempt=row.attempts, worker_id=worker_id, status=AttemptStatus.RUNNING
        )
        session.add(attempt)
        await session.flush()
        return Claim(
            job_id=job_id,
            worker_id=worker_id,
            type=row.type,
            payload=row.payload,
            attempt=row.attempts,
            max_attempts=row.max_attempts,
            attempt_id=attempt.id,
            created_at=row.created_at,
        )

    async def renew_lease(
        self,
        session: AsyncSession,
        claim: Claim,
        *,
        progress: float | None,
        progress_message: str | None,
    ) -> bool | None:
        """Heartbeat: extend the lease and save progress.

        Returns None if the job is no longer this worker's (lease expired and swept), otherwise
        whether cancellation has been requested.
        """
        row = (
            await session.execute(
                update(Job)
                .where(self._owned(claim))
                .values(
                    lease_until=self._lease_expiry(),
                    progress=progress,
                    progress_message=progress_message,
                )
                .returning(Job.cancel_requested)
            )
        ).one_or_none()
        return None if row is None else bool(row.cancel_requested)

    async def succeed(
        self,
        session: AsyncSession,
        claim: Claim,
        result: Any,
        *,
        outputs: Sequence[OutputFile] = (),
        progress_message: str | None = None,
    ) -> JobStatus | None:
        recorded = await self._finish_run(
            session,
            claim,
            JobStatus.SUCCEEDED,
            {
                "result": result,
                "error": None,
                "finished_at": func.now(),
                "progress": 1.0,
                "progress_message": progress_message,
            },
            attempt=AttemptStatus.SUCCEEDED,
            event=JobSucceeded(job_id=claim.job_id, job_type=claim.type, attempt=claim.attempt),
            outputs=outputs,
        )
        if recorded:
            log.info("Job succeeded")
        return JobStatus.SUCCEEDED if recorded else None

    async def fail(
        self, session: AsyncSession, claim: Claim, exc: BaseException
    ) -> JobStatus | None:
        """Record a failed run: retry after backoff while attempts remain, else dead."""
        error = f"{type(exc).__name__}: {exc}"
        permanent = isinstance(exc, PermanentError)
        delay = None if permanent else self._retry_delay(claim.attempt, claim.max_attempts)
        if delay is not None:
            recorded = await self._finish_run(
                session,
                claim,
                JobStatus.RETRYING,
                {"error": error, "next_attempt_at": retry_at(delay)},
                attempt=AttemptStatus.FAILED,
                error=error,
                event=JobRetryScheduled(
                    job_id=claim.job_id,
                    job_type=claim.type,
                    attempt=claim.attempt,
                    error=error,
                    retry_in_seconds=round(delay, 3),
                ),
                delay=delay,
            )
            if recorded:
                log.warning("Job failed; will retry", error=error, retry_in=round(delay, 2))
            return JobStatus.RETRYING if recorded else None

        recorded = await self._finish_run(
            session,
            claim,
            JobStatus.DEAD,
            {"error": error, "finished_at": func.now()},
            attempt=AttemptStatus.FAILED,
            error=error,
            event=JobDead(
                job_id=claim.job_id,
                job_type=claim.type,
                attempt=claim.attempt,
                error=error,
                permanent=permanent,
            ),
        )
        if recorded:
            log.error("Job dead", error=error, permanent=permanent)
        return JobStatus.DEAD if recorded else None

    async def cancel_running(self, session: AsyncSession, claim: Claim) -> JobStatus | None:
        """The handler was stopped because cancellation was requested."""
        recorded = await self._finish_run(
            session,
            claim,
            JobStatus.CANCELLED,
            {"error": "Cancelled by request", "finished_at": func.now()},
            attempt=AttemptStatus.CANCELLED,
            error="Cancelled by request",
            event=JobCancelled(job_id=claim.job_id, job_type=claim.type),
        )
        if recorded:
            log.info("Job cancelled while running")
        return JobStatus.CANCELLED if recorded else None

    async def release(self, session: AsyncSession, claim: Claim) -> JobStatus | None:
        """Hand the job back without using an attempt (graceful shutdown)."""
        recorded = await self._finish_run(
            session,
            claim,
            JobStatus.QUEUED,
            {"attempts": Job.attempts - 1, "worker_id": None},
            attempt=AttemptStatus.RELEASED,
            error="Released: worker shutting down",
            event=JobReleased(job_id=claim.job_id, job_type=claim.type, attempt=claim.attempt),
        )
        if recorded:
            log.warning("Job released back to the queue")
        return JobStatus.QUEUED if recorded else None

    # --- Maintenance ----------------------------------------------------------------------

    async def recover_expired_leases(self, session: AsyncSession, limit: int) -> list[Job]:
        """Recover running jobs whose worker stopped heartbeating.

        The lost run counts as an attempt, so a job that keeps crashing workers ends up dead
        instead of looping forever. SKIP LOCKED lets every worker run this concurrently.
        """
        expired = (
            await session.scalars(
                select(Job)
                .where(Job.status == JobStatus.RUNNING, Job.lease_until < func.now())
                .order_by(Job.lease_until)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).all()
        for job in expired:
            error = f"Lease expired: worker {job.worker_id} stopped heartbeating"
            await session.execute(
                update(JobAttempt)
                .where(
                    JobAttempt.job_id == job.id,
                    JobAttempt.attempt == job.attempts,
                    JobAttempt.status == AttemptStatus.RUNNING,
                )
                .values(status=AttemptStatus.LOST, error=error, finished_at=func.now())
            )
            event: JobEvent
            delay = 0.0
            if job.cancel_requested:
                self._move(job, JobStatus.CANCELLED, error="Cancelled by request")
                event = JobCancelled(job_id=job.id, job_type=job.type)
            elif (retry := self._retry_delay(job.attempts, job.max_attempts)) is not None:
                delay = retry
                self._move(job, JobStatus.RETRYING, error=error)
                job.next_attempt_at = retry_at(delay)
                event = JobRetryScheduled(
                    job_id=job.id,
                    job_type=job.type,
                    attempt=job.attempts,
                    error=error,
                    retry_in_seconds=round(delay, 3),
                )
            else:
                self._move(job, JobStatus.DEAD, error=error)
                event = JobDead(
                    job_id=job.id,
                    job_type=job.type,
                    attempt=job.attempts,
                    error=error,
                    permanent=False,
                )
            job.lease_until = None
            add_event(session, event, delay_seconds=delay)
            log.warning(
                "Lease expired; recovered job",
                job_id=str(job.id),
                lost_worker=job.worker_id,
                attempt=job.attempts,
                new_status=job.status,
            )
        return list(expired)

    async def redispatch_stale(self, session: AsyncSession, limit: int) -> int:
        """Safety net for lost stream messages (e.g. Redis restarted without persisting them).

        A queued/retrying job with no unpublished outbox event that hasn't changed for
        `stale_dispatch_seconds` gets a fresh dispatch event. A duplicate is harmless (ADR 0003).
        """
        has_pending_event = (
            select(OutboxEvent.id)
            .where(OutboxEvent.aggregate_id == Job.id, OutboxEvent.published_at.is_(None))
            .exists()
        )
        rows = (
            await session.execute(
                select(Job.id, Job.type)
                .where(
                    Job.status.in_(CLAIMABLE_STATUSES),
                    Job.updated_at
                    < func.now() - timedelta(seconds=self.settings.stale_dispatch_seconds),
                    ~has_pending_event,
                )
                .order_by(Job.updated_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).all()
        if not rows:
            return 0
        await session.execute(
            update(Job).where(Job.id.in_([r.id for r in rows])).values(updated_at=func.now())
        )
        for row in rows:
            add_event(session, JobRedispatched(job_id=row.id, job_type=row.type))
        return len(rows)

    # --- Helpers --------------------------------------------------------------------------

    def _retry_delay(self, attempt: int, max_attempts: int) -> float | None:
        """Backoff before the next attempt, or None when no attempts are left."""
        if attempt >= max_attempts:
            return None
        return retry_delay(self.settings, attempt)

    def _move(self, job: Job, target: JobStatus, *, error: str | None = None) -> None:
        """Change a loaded (and locked) job's status."""
        check_transition(job.status, target)
        job.status = target
        if error is not None:
            job.error = error
        if target in TERMINAL_STATUSES:
            job.finished_at = func.now()

    async def _finish_run(
        self,
        session: AsyncSession,
        claim: Claim,
        target: JobStatus,
        values: dict[str, Any],
        *,
        attempt: AttemptStatus,
        event: JobEvent,
        error: str | None = None,
        delay: float = 0.0,
        outputs: Sequence[OutputFile] = (),
    ) -> bool:
        """End a run: job, attempt row, outputs and event, only if the worker still owns it."""
        check_transition(JobStatus.RUNNING, target)
        updated = cast(
            CursorResult[Any],
            await session.execute(
                update(Job)
                .where(self._owned(claim))
                .values(status=target, lease_until=None, **values)
            ),
        )
        if updated.rowcount == 0:
            log.warning("Job no longer owned by this worker; not recording the outcome")
            return False
        await session.execute(
            update(JobAttempt)
            .where(JobAttempt.id == claim.attempt_id)
            .values(status=attempt, error=error, finished_at=func.now())
        )
        # Outputs become part of the job only together with its success.
        session.add_all(
            JobOutput(
                job_id=claim.job_id,
                attempt_id=claim.attempt_id,
                name=o.name,
                key=o.key,
                size=o.size,
                content_type=o.content_type,
            )
            for o in outputs
        )
        add_event(session, event, delay_seconds=delay)
        return True

    def _owned(self, claim: Claim) -> ColumnElement[bool]:
        return (
            (Job.id == claim.job_id)
            & (Job.status == JobStatus.RUNNING)
            & (Job.worker_id == claim.worker_id)
        )

    def _lease_expiry(self) -> ColumnElement[Any]:
        return func.now() + timedelta(seconds=self.settings.lease_seconds)
