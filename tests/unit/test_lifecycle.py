import uuid

import pytest

from taskmaster.db import JobStatus
from taskmaster.jobs.errors import JobConflict
from taskmaster.jobs.events import EVENT_MODELS, EventType, JobDead, parse_event
from taskmaster.jobs.lifecycle import ALLOWED, check_transition

S = JobStatus


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (S.QUEUED, S.RUNNING),
        (S.RETRYING, S.RUNNING),
        (S.RUNNING, S.SUCCEEDED),
        (S.RUNNING, S.QUEUED),  # released on shutdown
        (S.DEAD, S.QUEUED),  # redrive
        (S.CANCELLED, S.QUEUED),
    ],
)
def test_allowed_transitions(current: JobStatus, target: JobStatus) -> None:
    check_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (S.SUCCEEDED, S.QUEUED),  # a finished job is final
        (S.QUEUED, S.SUCCEEDED),  # must run first
        (S.DEAD, S.RUNNING),  # must be redriven, not claimed
        (S.CANCELLED, S.RUNNING),
    ],
)
def test_forbidden_transitions(current: JobStatus, target: JobStatus) -> None:
    with pytest.raises(JobConflict):
        check_transition(current, target)


def test_every_status_has_rules() -> None:
    assert set(ALLOWED) == set(JobStatus)


def test_every_event_type_has_a_model() -> None:
    assert set(EVENT_MODELS) == set(EventType)


def test_event_round_trips_through_its_wire_form() -> None:
    event = JobDead(
        job_id=uuid.uuid4(), job_type="csv.aggregate", attempt=3, error="boom", permanent=False
    )
    wire = event.model_dump(mode="json")
    assert wire["job_id"] == str(event.job_id)
    assert parse_event("job.dead", wire) == event


def test_events_reject_unknown_fields() -> None:
    with pytest.raises(ValueError, match="extra"):
        parse_event("job.created", {"job_id": str(uuid.uuid4()), "job_type": "echo", "x": 1})
