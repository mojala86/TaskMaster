"""Uploads (presigned PUT) and job outputs (presigned GET). File bytes never pass the API."""

import re
import uuid
from collections.abc import Iterable
from pathlib import PurePath

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from taskmaster.api.access import load_job
from taskmaster.api.deps import PrincipalDep, SessionDep, SettingsDep, StorageDep
from taskmaster.api.schemas import OutputRead, UploadCreate, UploadRead
from taskmaster.db import JobOutput
from taskmaster.jobs.errors import InputFileMissing, InputFileTooLarge
from taskmaster.storage import Storage

router = APIRouter(tags=["files"])


@router.post("/uploads", status_code=201, responses={413: {"description": "File too large"}})
async def create_upload(
    body: UploadCreate, storage: StorageDep, settings: SettingsDep
) -> UploadRead:
    """Get a presigned URL to PUT a file to; the file never passes through the API.

    The URL only accepts a body of exactly `size` bytes (up to the configured maximum).
    Use the returned `key` in a job payload (fields of type FileRef).
    """
    if body.size > settings.max_upload_bytes:
        raise HTTPException(
            413, f"File too large: {body.size} bytes (limit {settings.max_upload_bytes})"
        )
    key = f"uploads/{uuid.uuid4()}/{_safe_filename(body.filename)}"
    url = await storage.presign_put(key, body.content_type, body.size, settings.upload_url_ttl)
    return UploadRead(
        key=key,
        upload_url=url,
        headers={"Content-Type": body.content_type, "Content-Length": str(body.size)},
        expires_in=settings.upload_url_ttl,
    )


@router.get("/jobs/{job_id}/outputs")
async def list_job_outputs(
    job_id: uuid.UUID,
    session: SessionDep,
    storage: StorageDep,
    settings: SettingsDep,
    principal: PrincipalDep,
) -> list[OutputRead]:
    """Files produced by the job's successful attempt, with presigned download URLs."""
    await load_job(session, job_id, principal)
    outputs = (
        await session.scalars(
            select(JobOutput).where(JobOutput.job_id == job_id).order_by(JobOutput.name)
        )
    ).all()
    ttl = settings.download_url_ttl
    return [
        OutputRead(
            name=o.name,
            size=o.size,
            content_type=o.content_type,
            created_at=o.created_at,
            url=await storage.presign_get(o.key, ttl, filename=o.name),
            expires_in=ttl,
        )
        for o in outputs
    ]


async def check_input_files(storage: Storage, keys: Iterable[str], max_bytes: int) -> None:
    """Fail fast on missing or oversized inputs, rather than letting a worker find out later."""
    for key in keys:
        info = await storage.head(key)
        if info is None:
            raise InputFileMissing(f"Input file {key!r} not found. Upload it first.")
        # Second line of defence, in case the store doesn't enforce the signed length.
        if info.size > max_bytes:
            raise InputFileTooLarge(f"Input file {key!r} is larger than {max_bytes} bytes")


def _safe_filename(filename: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]", "_", PurePath(filename.replace("\\", "/")).name)
    return name.strip(".")[:255] or "file"
