import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from taskmaster.db.models import AttemptStatus, JobStatus


class JobCreate(BaseModel):
    type: str = Field(min_length=1, max_length=100, examples=["echo"])
    payload: dict[str, Any] = Field(default_factory=dict, examples=[{"data": {"hello": "world"}}])
    max_attempts: int | None = Field(
        None, ge=1, le=50, description="Overrides the task type's default (max_retries + 1)"
    )


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    status: JobStatus
    result: Any | None
    error: str | None
    attempts: int
    max_attempts: int
    progress: float | None = Field(description="0..1, reported by the handler")
    progress_message: str | None
    worker_id: str | None
    lease_until: datetime | None
    cancel_requested: bool
    idempotency_key: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class JobRedrive(BaseModel):
    additional_attempts: int = Field(1, ge=1, le=50)


class AttemptRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt: int
    worker_id: str
    status: AttemptStatus
    error: str | None
    started_at: datetime
    finished_at: datetime | None


class TaskInfo(BaseModel):
    name: str
    max_retries: int
    timeout_seconds: float
    input_schema: dict[str, Any]


class UploadCreate(BaseModel):
    filename: str = Field(min_length=1, max_length=255, examples=["sales.csv"])
    content_type: str = Field("application/octet-stream", max_length=255, examples=["text/csv"])
    size: int = Field(
        gt=0,
        examples=[1048576],
        description="Exact size of the file in bytes. The upload must send exactly this many.",
    )


class UploadRead(BaseModel):
    key: str = Field(description='Reference this in a job payload, e.g. {"file": key}')
    upload_url: str
    method: Literal["PUT"] = "PUT"
    headers: dict[str, str] = Field(description="Headers the upload request must send")
    expires_in: int


class OutputRead(BaseModel):
    name: str
    size: int
    content_type: str
    created_at: datetime
    url: str = Field(description="Presigned download URL")
    expires_in: int
