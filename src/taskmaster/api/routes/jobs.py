"""Jobs: submit, inspect, list, cancel and redrive. Status changes go through JobLifecycle."""

import uuid
from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Query, Request, Response
from pydantic import ValidationError
from sqlalchemy import func, select

from taskmaster import metrics
from taskmaster.api import idempotency
from taskmaster.api.access import load_job, owned_by
from taskmaster.api.deps import (
    IdempotencyKey,
    LifecycleDep,
    PrincipalDep,
    SessionDep,
    SettingsDep,
    get_storage,
)
from taskmaster.api.routes.files import check_input_files
from taskmaster.api.schemas import AttemptRead, JobCreate, JobRead, JobRedrive
from taskmaster.db import Job, JobAttempt, JobStatus
from taskmaster.tasks import all_tasks, file_refs, get_task

router = APIRouter(tags=["jobs"])


@router.post(
    "/jobs",
    status_code=201,
    responses={
        200: {"description": "Replay: a job with this Idempotency-Key already exists"},
        413: {"description": "An input file is larger than the upload limit"},
        422: {"description": "Invalid payload, or Idempotency-Key reused with a different body"},
    },
)
async def create_job(
    body: JobCreate,
    session: SessionDep,
    request: Request,
    response: Response,
    principal: PrincipalDep,
    lifecycle: LifecycleDep,
    settings: SettingsDep,
    idempotency_key: IdempotencyKey = None,
) -> JobRead:
    definition = get_task(body.type)
    if definition is None:
        known = [t.name for t in all_tasks()]
        raise HTTPException(422, f"Unknown task type {body.type!r}. Known types: {known}")
    try:
        parsed = definition.input_model.model_validate(body.payload)
    except ValidationError as exc:
        raise HTTPException(
            422, exc.errors(include_url=False, include_context=False, include_input=False)
        ) from exc
    if keys := file_refs(parsed):
        await check_input_files(get_storage(request), keys, settings.max_upload_bytes)

    payload = parsed.model_dump(mode="json")
    max_attempts = body.max_attempts or definition.max_attempts
    job = Job(
        id=uuid.uuid4(),
        api_key_id=principal.key_id,
        type=definition.name,
        payload=payload,
        max_attempts=max_attempts,
        idempotency_key=idempotency_key,
        request_hash=(
            idempotency.fingerprint(definition.name, payload, max_attempts)
            if idempotency_key
            else None
        ),
    )
    submission = await idempotency.submit_once(session, lifecycle, job, principal)
    if submission.replayed:
        response.status_code = 200
        response.headers["Idempotent-Replayed"] = "true"
    else:
        metrics.JOBS_SUBMITTED.labels(job.type).inc()
    return JobRead.model_validate(submission.job)


@router.get("/jobs/stats")
async def job_stats(session: SessionDep, principal: PrincipalDep) -> dict[str, int]:
    """Number of the caller's jobs per status."""
    rows = await session.execute(
        select(Job.status, func.count()).where(owned_by(principal)).group_by(Job.status)
    )
    return {str(status): count for status, count in rows.all()}


@router.get("/jobs/{job_id}")
async def get_job(job_id: uuid.UUID, session: SessionDep, principal: PrincipalDep) -> JobRead:
    return JobRead.model_validate(await load_job(session, job_id, principal))


@router.post(
    "/jobs/{job_id}/cancel",
    responses={
        202: {"description": "Job is running; it will be stopped at its next heartbeat"},
        409: {"description": "Job already finished"},
    },
)
async def cancel_job(
    job_id: uuid.UUID,
    session: SessionDep,
    response: Response,
    principal: PrincipalDep,
    lifecycle: LifecycleDep,
) -> JobRead:
    async with session.begin():
        job = await load_job(session, job_id, principal, for_update=True)
        if not lifecycle.cancel(session, job):
            response.status_code = 202
    await session.refresh(job)
    return JobRead.model_validate(job)


@router.post(
    "/jobs/{job_id}/retry",
    responses={409: {"description": "Only dead or cancelled jobs can be redriven"}},
)
async def redrive_job(
    job_id: uuid.UUID,
    session: SessionDep,
    principal: PrincipalDep,
    lifecycle: LifecycleDep,
    body: Annotated[JobRedrive | None, Body()] = None,
) -> JobRead:
    """Redrive a job from the dead-letter queue (or a cancelled job) with extra attempts."""
    async with session.begin():
        job = await load_job(session, job_id, principal, for_update=True)
        lifecycle.redrive(session, job, (body or JobRedrive()).additional_attempts)
    await session.refresh(job)
    return JobRead.model_validate(job)


@router.get("/jobs/{job_id}/attempts")
async def get_job_attempts(
    job_id: uuid.UUID, session: SessionDep, principal: PrincipalDep
) -> list[AttemptRead]:
    await load_job(session, job_id, principal)
    attempts: Sequence[JobAttempt] = (
        await session.scalars(
            select(JobAttempt).where(JobAttempt.job_id == job_id).order_by(JobAttempt.id)
        )
    ).all()
    return [AttemptRead.model_validate(a) for a in attempts]


@router.get("/jobs")
async def list_jobs(
    session: SessionDep,
    principal: PrincipalDep,
    status: JobStatus | None = None,
    type: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[JobRead]:
    """List jobs. `?status=dead` is the dead-letter queue."""
    stmt = (
        select(Job)
        .where(owned_by(principal))
        .order_by(Job.created_at.desc(), Job.id)
        .limit(limit)
        .offset(offset)
    )
    if status is not None:
        stmt = stmt.where(Job.status == status)
    if type is not None:
        stmt = stmt.where(Job.type == type)
    return [JobRead.model_validate(j) for j in (await session.scalars(stmt)).all()]
