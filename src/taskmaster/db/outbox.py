"""Transactional outbox: stage a job event in the same transaction as the change it describes."""

from datetime import timedelta

from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from taskmaster.db.models import OutboxEvent
from taskmaster.jobs.events import JobEvent


def add_event(session: AsyncSession, event: JobEvent, *, delay_seconds: float = 0) -> OutboxEvent:
    """Stage an outbox row in the caller's transaction. Never publishes directly.

    With `delay_seconds`, the relay publishes it only after that long (retry backoff).
    """
    row = OutboxEvent(
        aggregate_id=event.job_id,
        event_type=event.event_type.value,
        payload=event.model_dump(mode="json"),
    )
    if delay_seconds > 0:
        # DB clock, same as the relay's `available_at <= now()` check.
        row.available_at = func.now() + timedelta(seconds=delay_seconds)
    session.add(row)
    return row
