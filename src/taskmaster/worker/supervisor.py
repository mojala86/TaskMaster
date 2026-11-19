"""Runs one claimed job: executes its handler, heartbeats while it runs, records the outcome.

While the handler runs, a heartbeat every `heartbeat_interval` renews the lease and saves
progress. The same call reports a cancellation request, or that the job was taken away (the lease
expired and was swept); either way the handler is cancelled. On worker shutdown (`abort`), a still
running handler is stopped and the job released.

What each outcome does to the job is decided in `taskmaster.jobs.lifecycle`.
"""

import asyncio
import tempfile
import time
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import structlog
from pydantic import BaseModel, ValidationError

from taskmaster import metrics
from taskmaster.config import Settings
from taskmaster.db import Database, JobStatus
from taskmaster.jobs.lifecycle import Claim, JobLifecycle
from taskmaster.storage import Storage
from taskmaster.tasks import JobContext, PermanentError, get_task

log = structlog.get_logger("taskmaster.worker")

# Metric outcome per status a run ends in.
_OUTCOMES = {
    JobStatus.SUCCEEDED: "succeeded",
    JobStatus.RETRYING: "failed",
    JobStatus.DEAD: "dead",
    JobStatus.CANCELLED: "cancelled",
    JobStatus.QUEUED: "released",
}


class StopReason(StrEnum):
    CANCELLED = "cancelled"  # cancellation requested through the API
    LOST = "lost"  # lease expired and the job was reassigned; record nothing
    SHUTDOWN = "shutdown"  # graceful shutdown timed out


class JobSupervisor:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        lifecycle: JobLifecycle,
        abort: asyncio.Event,
        storage: Storage | None = None,
    ) -> None:
        self.db = db
        self.settings = settings
        self.lifecycle = lifecycle
        self.abort = abort
        self.storage = storage

    async def run(self, claim: Claim) -> JobStatus | None:
        """Run a claimed job to an outcome and record it. Returns the job's new status, or None if
        this worker lost the job meanwhile."""
        with tempfile.TemporaryDirectory(
            prefix=f"taskmaster-{claim.job_id}-",
            dir=self.settings.work_dir,
            ignore_cleanup_errors=True,
        ) as workdir:
            ctx = JobContext(
                job_id=claim.job_id,
                attempt=claim.attempt,
                max_attempts=claim.max_attempts,
                log=structlog.get_logger(f"taskmaster.tasks.{claim.type}"),
                storage=self.storage,
                workdir=Path(workdir),
                output_prefix=f"results/{claim.job_id}/{claim.attempt_id}/",
            )
            return await self._run_observed(claim, ctx)

    async def _run_observed(self, claim: Claim, ctx: JobContext) -> JobStatus | None:
        """Metrics and log context around one run."""
        if claim.attempt == 1:
            metrics.JOB_QUEUE_WAIT.labels(claim.type).observe(
                max(0.0, (datetime.now(UTC) - claim.created_at).total_seconds())
            )
        in_progress = metrics.JOBS_IN_PROGRESS.labels(claim.type)
        in_progress.inc()
        start = time.perf_counter()
        status: JobStatus | None = None
        # Every log line during this run (ours, the handler's, libraries') carries these fields.
        with structlog.contextvars.bound_contextvars(
            job_id=str(claim.job_id), job_type=claim.type, attempt=claim.attempt
        ):
            try:
                status = await self._run_and_record(claim, ctx)
            finally:
                in_progress.dec()
                outcome = _OUTCOMES.get(status, "lost") if status else "lost"
                metrics.JOB_RUNS.labels(claim.type, outcome).inc()
                metrics.JOB_DURATION.labels(claim.type, outcome).observe(
                    time.perf_counter() - start
                )
        return status

    async def _run_and_record(self, claim: Claim, ctx: JobContext) -> JobStatus | None:
        log.info("Job started", max_attempts=claim.max_attempts)
        task = asyncio.create_task(self._execute(claim, ctx))
        reason = await self._supervise(claim, task, ctx)
        if reason is StopReason.LOST:
            log.warning("Job lost: lease expired and job reassigned; discarding this run")
            return None

        async with self.db.session() as session, session.begin():
            if reason is None:
                if task.cancelled():
                    return await self.lifecycle.fail(
                        session, claim, RuntimeError("Handler was cancelled")
                    )
                if (exc := task.exception()) is not None:
                    return await self.lifecycle.fail(session, claim, exc)
                return await self.lifecycle.succeed(
                    session,
                    claim,
                    task.result(),
                    outputs=ctx.outputs,
                    progress_message=ctx.progress_message,
                )
            if reason is StopReason.CANCELLED:
                return await self.lifecycle.cancel_running(session, claim)
            return await self.lifecycle.release(session, claim)  # StopReason.SHUTDOWN

    async def _execute(self, claim: Claim, ctx: JobContext) -> Any:
        definition = get_task(claim.type)
        if definition is None:
            raise PermanentError(f"No handler registered for task type {claim.type!r}")
        try:
            parsed = definition.input_model.model_validate(claim.payload)
        except ValidationError as exc:
            raise PermanentError(f"Invalid payload: {exc}") from exc
        try:
            async with asyncio.timeout(definition.timeout) as deadline:
                result = await definition.fn(parsed, ctx)
        except TimeoutError as exc:
            # Only relabel our own deadline; a TimeoutError the handler raised itself (an HTTP
            # call timing out, say) keeps its own message.
            if deadline.expired():
                raise TimeoutError(f"Task timed out after {definition.timeout:g}s") from exc
            raise
        return result.model_dump(mode="json") if isinstance(result, BaseModel) else result

    async def _supervise(
        self, claim: Claim, task: asyncio.Task[Any], ctx: JobContext
    ) -> StopReason | None:
        """Heartbeat while `task` runs. Returns None when it finished on its own, otherwise why it
        was stopped."""
        abort = asyncio.create_task(self.abort.wait())
        try:
            while True:
                done, _ = await asyncio.wait(
                    {task, abort},
                    timeout=self.settings.heartbeat_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if task in done:
                    return None
                if abort in done:
                    reason: StopReason | None = StopReason.SHUTDOWN
                else:
                    try:
                        reason = await self._heartbeat(claim, ctx)
                    except Exception:
                        # Keep running; if this persists the lease expires and the job is swept.
                        log.warning("Heartbeat failed", exc_info=True)
                        continue
                    if reason is None:
                        continue

                task.cancel()
                await asyncio.wait({task})
                if reason is not StopReason.LOST and not task.cancelled():
                    return None  # finished just before the cancel landed: record the real outcome
                return reason
        finally:
            abort.cancel()

    async def _heartbeat(self, claim: Claim, ctx: JobContext) -> StopReason | None:
        """Renew the lease, publish progress, and learn whether to stop."""
        async with self.db.session() as session, session.begin():
            cancel_requested = await self.lifecycle.renew_lease(
                session, claim, progress=ctx.progress, progress_message=ctx.progress_message
            )
        if cancel_requested is None:
            return StopReason.LOST
        return StopReason.CANCELLED if cancel_requested else None
