import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    Enum,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from taskmaster.db.base import (
    TIMESTAMPTZ,
    Base,
    BigIntPrimaryKey,
    CreatedAt,
    UuidPrimaryKey,
)


class JobStatus(StrEnum):
    """Job lifecycle.

    queued -> running -> succeeded
                 |-> retrying -> (re-dispatched after backoff) -> running ...
                 |-> dead        (attempts exhausted or permanent error; see the dead-letter stream)
                 |-> cancelled   (cancel requested; a running job is stopped at its next heartbeat)
                 '-> queued      (released on graceful worker shutdown; attempt not consumed)

    A running job whose lease expires (worker crashed) is moved to retrying/dead by the sweeper.
    """

    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    DEAD = "dead"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = frozenset({JobStatus.SUCCEEDED, JobStatus.DEAD, JobStatus.CANCELLED})
CLAIMABLE_STATUSES = (JobStatus.QUEUED, JobStatus.RETRYING)


class AttemptStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"  # handler raised or timed out
    CANCELLED = "cancelled"  # stopped on request
    RELEASED = "released"  # handed back on graceful shutdown; does not count against max_attempts
    LOST = "lost"  # lease expired: the worker died or stopped heartbeating


def _str_enum(cls: type[StrEnum], name: str) -> Enum:
    # VARCHAR + CHECK instead of a native PG enum: adding a value later is a trivial migration.
    return Enum(
        cls,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=20,
        values_callable=lambda e: [m.value for m in e],
    )


class ApiKey(UuidPrimaryKey, CreatedAt, Base):
    """A client credential. Only a SHA-256 of the key is stored; the key is shown once."""

    __tablename__ = "api_keys"

    name: Mapped[str] = mapped_column(String(100))
    # Public part of the key ("tm_<prefix>_..."), for listing and revoking.
    prefix: Mapped[str] = mapped_column(String(16), unique=True)
    key_hash: Mapped[str] = mapped_column(String(64), unique=True)
    # Requests per minute; NULL means the configured default.
    rate_limit_per_minute: Mapped[int | None]
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)


class Job(UuidPrimaryKey, CreatedAt, Base):
    __tablename__ = "jobs"
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012
    __table_args__ = (
        # Lease sweeper: "running jobs whose lease has expired".
        Index("ix_jobs_running_lease", "lease_until", postgresql_where=text("status = 'running'")),
        # Idempotency keys are per client: (owner, key) is unique; anonymous callers share NULL.
        Index(
            "ux_jobs_owner_idempotency_key",
            "api_key_id",
            "idempotency_key",
            unique=True,
            postgresql_nulls_not_distinct=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
    )

    # Owner. NULL for anonymous submissions (when API keys are not required).
    api_key_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("api_keys.id", ondelete="SET NULL"), index=True
    )
    type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    status: Mapped[JobStatus] = mapped_column(
        _str_enum(JobStatus, "job_status"), default=JobStatus.QUEUED, index=True
    )
    result: Mapped[Any | None] = mapped_column(JSONB)
    error: Mapped[str | None] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    max_attempts: Mapped[int]
    worker_id: Mapped[str | None] = mapped_column(String(255))
    lease_until: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    # A retrying job can't be claimed before this (its backoff), even if a stale or duplicate
    # dispatch message for it shows up early.
    next_attempt_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    cancel_requested: Mapped[bool] = mapped_column(default=False, server_default=text("false"))
    # Reported by the handler, written with each heartbeat. 0..1.
    progress: Mapped[float | None]
    progress_message: Mapped[str | None] = mapped_column(Text)
    # Client-supplied Idempotency-Key and a hash of the request it was first used with.
    idempotency_key: Mapped[str | None] = mapped_column(String(255))
    request_hash: Mapped[str | None] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)


class JobAttempt(BigIntPrimaryKey, Base):
    """One row per execution attempt, so the full error history survives retries.

    `attempt` is the job's attempt counter at claim time. A released attempt gives its number back,
    so the next run reuses it; rows are ordered by id.
    """

    __tablename__ = "job_attempts"

    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    attempt: Mapped[int]
    worker_id: Mapped[str] = mapped_column(String(255))
    status: Mapped[AttemptStatus] = mapped_column(_str_enum(AttemptStatus, "attempt_status"))
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)


class JobOutput(BigIntPrimaryKey, CreatedAt, Base):
    """A file produced by a job, stored in object storage.

    Rows are written in the same transaction that marks the job succeeded, so a job's outputs
    are exactly those of its successful attempt. Objects live under results/<job>/<attempt id>/,
    so a stale worker that lost its lease can never overwrite them.
    """

    __tablename__ = "job_outputs"
    __table_args__ = (UniqueConstraint("job_id", "name", name="uq_job_outputs_job_name"),)

    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"))
    attempt_id: Mapped[int] = mapped_column(ForeignKey("job_attempts.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String(255))
    key: Mapped[str] = mapped_column(Text)
    size: Mapped[int] = mapped_column(BigInteger)
    content_type: Mapped[str] = mapped_column(String(255))


class OutboxEvent(BigIntPrimaryKey, CreatedAt, Base):
    """Transactional outbox: written in the same transaction as the state change it describes.

    The relay publishes unpublished rows to Redis and stamps published_at.
    available_at lets a row be scheduled for the future (retry backoff).
    """

    __tablename__ = "outbox"
    __table_args__ = (
        # Partial index keeps relay polling fast however large the published history grows.
        Index("outbox_unpublished", "id", postgresql_where=text("published_at IS NULL")),
        # Cleanup of old published rows, and the stale-dispatch check by job.
        Index("ix_outbox_published_at", "published_at"),
        Index("ix_outbox_aggregate_id", "aggregate_id"),
    )

    aggregate_id: Mapped[uuid.UUID]
    event_type: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    available_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)
