"""Who may see which job: callers only see their own jobs; anonymous callers see anonymous jobs."""

import uuid

from sqlalchemy import ColumnElement, select
from sqlalchemy.ext.asyncio import AsyncSession

from taskmaster.auth import Principal
from taskmaster.db import Job
from taskmaster.jobs.errors import JobNotFound


def owned_by(principal: Principal) -> ColumnElement[bool]:
    if principal.key_id is None:
        return Job.api_key_id.is_(None)
    return Job.api_key_id == principal.key_id


async def load_job(
    session: AsyncSession, job_id: uuid.UUID, principal: Principal, *, for_update: bool = False
) -> Job:
    stmt = select(Job).where(Job.id == job_id, owned_by(principal))
    if for_update:
        stmt = stmt.with_for_update()
    job: Job | None = await session.scalar(stmt)
    if job is None:  # also for someone else's job: don't reveal that it exists
        raise JobNotFound("Job not found")
    return job
