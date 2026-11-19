"""Job events: the payloads written to the outbox and published to Redis.

Each event is a typed model, so every producer writes the same fields, and consumers (the
events stream today, webhooks in phase 4) can rely on them. On the wire an event is its
`event_type` plus `model_dump(mode="json")`; `parse_event` turns that back into the model.

Payload example (job.dead):

    {"job_id": "9c1e…", "job_type": "csv.aggregate", "attempt": 3,
     "error": "KeyError: 'amount'", "permanent": false}
"""

import uuid
from enum import StrEnum
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict


class EventType(StrEnum):
    # Dispatch events: put the job (back) on the work stream.
    JOB_CREATED = "job.created"
    JOB_RETRY_SCHEDULED = "job.retry_scheduled"  # after a failure (with backoff) or a lost lease
    JOB_RELEASED = "job.released"  # handed back by a worker shutting down
    JOB_REQUEUED = "job.requeued"  # redriven from the dead-letter queue via the API
    JOB_REDISPATCHED = "job.redispatched"  # safety net: queued too long, message presumed lost
    # Lifecycle events: go to the events stream (webhooks, phase 4).
    JOB_SUCCEEDED = "job.succeeded"
    JOB_DEAD = "job.dead"  # also copied to the dead-letter stream
    JOB_CANCELLED = "job.cancelled"


DISPATCH_EVENTS = frozenset(
    {
        EventType.JOB_CREATED,
        EventType.JOB_RETRY_SCHEDULED,
        EventType.JOB_RELEASED,
        EventType.JOB_REQUEUED,
        EventType.JOB_REDISPATCHED,
    }
)


class JobEvent(BaseModel):
    """Fields every job event carries. Subclasses set `event_type` and add their own."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_type: ClassVar[EventType]
    job_id: uuid.UUID
    job_type: str


class JobCreated(JobEvent):
    event_type = EventType.JOB_CREATED


class JobRetryScheduled(JobEvent):
    event_type = EventType.JOB_RETRY_SCHEDULED
    attempt: int  # the attempt that failed or was lost
    error: str
    retry_in_seconds: float


class JobReleased(JobEvent):
    event_type = EventType.JOB_RELEASED
    attempt: int


class JobRequeued(JobEvent):
    event_type = EventType.JOB_REQUEUED
    max_attempts: int


class JobRedispatched(JobEvent):
    event_type = EventType.JOB_REDISPATCHED


class JobSucceeded(JobEvent):
    event_type = EventType.JOB_SUCCEEDED
    attempt: int


class JobDead(JobEvent):
    event_type = EventType.JOB_DEAD
    attempt: int
    error: str
    permanent: bool  # failed with PermanentError, rather than running out of attempts


class JobCancelled(JobEvent):
    event_type = EventType.JOB_CANCELLED


EVENT_MODELS: dict[EventType, type[JobEvent]] = {
    model.event_type: model for model in JobEvent.__subclasses__()
}


def parse_event(event_type: str, payload: dict[str, Any]) -> JobEvent:
    """Rebuild a typed event from an outbox row or stream entry."""
    return EVENT_MODELS[EventType(event_type)].model_validate(payload)
