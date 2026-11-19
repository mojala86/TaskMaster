"""Idempotency-Key handling for POST /jobs.

A client that retries a request (say, after a timeout) with the same key gets the job the first
request created instead of a second one. Keys are per caller. Reusing a key with a different
request is an error rather than a silent replay of the wrong job.

Example: two requests with `Idempotency-Key: order-42` arrive at the same time. Both find no
job, both insert; the unique index (api_key_id, idempotency_key) lets one win, and the loser
reads and returns the winner's job.
"""

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from taskmaster.api.access import owned_by
from taskmaster.auth import Principal
from taskmaster.db import Job
from taskmaster.jobs.errors import IdempotencyConflict
from taskmaster.jobs.lifecycle import JobLifecycle


@dataclass(frozen=True)
class Submission:
    job: Job
    replayed: bool  # True if the job already existed for this key


def fingerprint(job_type: str, payload: dict[str, Any], max_attempts: int) -> str:
    """Hash of what makes two requests "the same request"."""
    canonical = json.dumps(
        {"type": job_type, "payload": payload, "max_attempts": max_attempts},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


async def submit_once(
    session: AsyncSession, lifecycle: JobLifecycle, job: Job, principal: Principal
) -> Submission:
    """Create `job`, unless a job with its idempotency key already exists for this caller."""
    key = job.idempotency_key
    if key is not None:
        async with session.begin():
            existing = await _find(session, key, principal)
        if existing is not None:
            return _replay(existing, job)
    try:
        async with session.begin():
            lifecycle.create(session, job)
    except IntegrityError:
        if key is None:
            raise
        async with session.begin():
            existing = await _find(session, key, principal)  # a concurrent request won the race
        if existing is None:
            raise
        return _replay(existing, job)
    return Submission(job, replayed=False)


async def _find(session: AsyncSession, key: str, principal: Principal) -> Job | None:
    job: Job | None = await session.scalar(
        select(Job).where(Job.idempotency_key == key, owned_by(principal))
    )
    return job


def _replay(existing: Job, attempted: Job) -> Submission:
    if existing.request_hash != attempted.request_hash:
        raise IdempotencyConflict("Idempotency-Key was already used with a different request")
    return Submission(existing, replayed=True)
