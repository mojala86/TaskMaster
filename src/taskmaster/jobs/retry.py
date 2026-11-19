import random
from datetime import timedelta
from typing import Any

from sqlalchemy import func
from sqlalchemy.sql.elements import ColumnElement

from taskmaster.config import Settings


def backoff_delay(attempt: int, *, base: float, cap: float, jitter: float = 0.0) -> float:
    """Exponential backoff for the retry after `attempt` (1-based): base * 2**(attempt-1).

    Capped at `cap`, then spread by +/- `jitter` (fraction) so failed jobs don't retry in lockstep.
    """
    delay = min(cap, base * 2 ** (attempt - 1))
    if jitter:
        delay *= 1 + random.uniform(-jitter, jitter)
    return max(0.0, delay)


def retry_delay(settings: Settings, attempt: int) -> float:
    """Backoff before retrying after `attempt`, using the configured policy."""
    return backoff_delay(
        attempt,
        base=settings.retry_base_delay,
        cap=settings.retry_max_delay,
        jitter=settings.retry_jitter,
    )


def retry_at(delay: float) -> ColumnElement[Any]:
    """When a retry scheduled now becomes due, on the DB clock.

    Same expression as the outbox row's `available_at`, so the job becomes claimable exactly when
    its dispatch event is published.
    """
    return func.now() + timedelta(seconds=delay)
