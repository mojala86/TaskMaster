"""Phase 2: failure modes. Each test sets up one way things go wrong and checks recovery."""

import asyncio
import uuid
from typing import Any

import httpx
import pytest
from pydantic import BaseModel

from taskmaster.config import Settings
from taskmaster.db import Database, JobStatus
from taskmaster.queue import Streams
from taskmaster.relay import Relay
from taskmaster.tasks import JobContext, task
from taskmaster.worker import Maintenance, Worker
from tests.integration.helpers import (
    attempt_statuses,
    create_job,
    execute,
    get_job,
    outbox_events,
    wait_for_status,
)

pytestmark = pytest.mark.integration


def make_worker(
    db: Database, streams: Streams, settings: Settings, worker_id: str, **overrides: Any
) -> Worker:
    return Worker(db, streams, settings.model_copy(update=overrides), worker_id=worker_id)


async def expire_lease(db: Database, job_id: str) -> None:
    await execute(
        db,
        "UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(job_id),
    )


# --- Idempotency keys -----------------------------------------------------------------------


async def test_idempotency_key_replays_the_original_job(
    client: httpx.AsyncClient, db: Database
) -> None:
    body = {"type": "echo", "payload": {"data": {"n": 1}}}
    headers = {"Idempotency-Key": "order-42"}

    first = await client.post("/jobs", json=body, headers=headers)
    second = await client.post("/jobs", json=body, headers=headers)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json()["id"] == first.json()["id"]
    assert await outbox_events(db, first.json()["id"]) == [("job.created", False)]


async def test_idempotency_key_reused_with_different_body_is_rejected(
    client: httpx.AsyncClient,
) -> None:
    headers = {"Idempotency-Key": "order-43"}
    await create_job(client, headers=headers, type="echo", payload={"data": {"n": 1}})

    response = await client.post(
        "/jobs", json={"type": "echo", "payload": {"data": {"n": 2}}}, headers=headers
    )

    assert response.status_code == 422
    assert "different request" in response.json()["detail"]


async def test_concurrent_requests_with_same_key_create_one_job(
    client: httpx.AsyncClient,
) -> None:
    headers = {"Idempotency-Key": "order-44"}

    responses = await asyncio.gather(
        *(client.post("/jobs", json={"type": "echo"}, headers=headers) for _ in range(10))
    )

    assert sorted(r.status_code for r in responses) == [200] * 9 + [201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert len((await client.get("/jobs")).json()) == 1


# --- Cancellation ---------------------------------------------------------------------------


async def test_cancel_queued_job(
    client: httpx.AsyncClient, db: Database, relay: Relay, worker: Worker
) -> None:
    job = await create_job(client, type="echo")

    response = await client.post(f"/jobs/{job['id']}/cancel")
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"

    # The dispatch message still arrives, but the job can no longer be claimed.
    await relay.publish_batch()
    assert await worker.poll_once("c1") == 1
    assert (await get_job(client, job["id"]))["status"] == "cancelled"
    assert await attempt_statuses(client, job["id"]) == []
    assert [e for e, _ in await outbox_events(db, job["id"])] == ["job.created", "job.cancelled"]


async def test_cancel_running_job_stops_the_handler(
    client: httpx.AsyncClient, db: Database, relay: Relay, worker: Worker
) -> None:
    job = await create_job(client, type="sleep", payload={"seconds": 30})
    await relay.publish_batch()
    running = asyncio.create_task(worker.poll_once("c1"))
    await wait_for_status(client, job["id"], "running")

    response = await client.post(f"/jobs/{job['id']}/cancel")
    assert response.status_code == 202
    assert response.json()["cancel_requested"] is True

    async with asyncio.timeout(5):  # far less than the 30s the handler would sleep
        await running
    assert (await get_job(client, job["id"]))["status"] == "cancelled"
    assert await attempt_statuses(client, job["id"]) == ["cancelled"]
    assert [e for e, _ in await outbox_events(db, job["id"])][-1] == "job.cancelled"


async def test_cancel_finished_job_conflicts(
    client: httpx.AsyncClient, relay: Relay, worker: Worker
) -> None:
    job = await create_job(client, type="echo")
    await relay.publish_batch()
    await worker.poll_once("c1")

    response = await client.post(f"/jobs/{job['id']}/cancel")
    assert response.status_code == 409


# --- Leases: crashed and partitioned workers ------------------------------------------------


async def test_job_of_crashed_worker_is_recovered(
    client: httpx.AsyncClient,
    db: Database,
    relay: Relay,
    worker: Worker,
    maintenance: Maintenance,
    streams: Streams,
    settings: Settings,
) -> None:
    job = await create_job(client, type="echo", payload={"data": {"v": "real"}}, max_attempts=3)
    crashed = make_worker(db, streams, settings, "crashed")
    claim = await crashed.claim(uuid.UUID(job["id"]))  # claims, then "dies" without running
    assert claim is not None

    await expire_lease(db, job["id"])
    assert await maintenance.sweep_expired_leases() == 1

    assert (await get_job(client, job["id"]))["status"] == "retrying"
    assert await attempt_statuses(client, job["id"]) == ["lost"]

    await relay.publish_batch()  # job.created (stale) + job.retry_scheduled
    while await worker.poll_once("c1"):
        pass
    done = await get_job(client, job["id"])
    assert done["status"] == "succeeded"
    assert await attempt_statuses(client, job["id"]) == ["lost", "succeeded"]

    # The "crashed" worker coming back late and finishing its run records nothing.
    assert await crashed.run_claimed(claim) is None
    after = await get_job(client, job["id"])
    assert (after["worker_id"], after["finished_at"]) == (done["worker_id"], done["finished_at"])
    assert await attempt_statuses(client, job["id"]) == ["lost", "succeeded"]


async def test_lost_lease_with_no_attempts_left_goes_to_dead_letter(
    client: httpx.AsyncClient,
    db: Database,
    relay: Relay,
    maintenance: Maintenance,
    streams: Streams,
    settings: Settings,
) -> None:
    job = await create_job(client, type="echo", max_attempts=1)
    await make_worker(db, streams, settings, "crashed").claim(uuid.UUID(job["id"]))
    await expire_lease(db, job["id"])

    await maintenance.sweep_expired_leases()
    await relay.publish_batch()

    dead = await get_job(client, job["id"])
    assert dead["status"] == "dead"
    assert "Lease expired" in dead["error"]
    entries = await streams.redis.xrange(settings.dead_letter_stream)
    assert [f["job_id"] for _, f in entries] == [job["id"]]


async def test_worker_that_loses_its_lease_stops_and_records_nothing(
    client: httpx.AsyncClient,
    db: Database,
    maintenance: Maintenance,
    streams: Streams,
    settings: Settings,
) -> None:
    # Heartbeat slower than the lease: simulates a worker that is alive but cut off.
    slow = make_worker(
        db, streams, settings, "partitioned", lease_seconds=0.1, heartbeat_interval=0.5
    )
    job = await create_job(client, type="sleep", payload={"seconds": 30}, max_attempts=3)
    running = asyncio.create_task(slow.handle(uuid.UUID(job["id"])))
    await wait_for_status(client, job["id"], "running")
    await asyncio.sleep(0.2)  # lease expired, next heartbeat not yet sent

    assert await maintenance.sweep_expired_leases() == 1
    async with asyncio.timeout(5):
        assert await running is None  # its next heartbeat noticed and cancelled the handler

    assert (await get_job(client, job["id"]))["status"] == "retrying"
    assert await attempt_statuses(client, job["id"]) == ["lost"]


async def test_heartbeat_keeps_long_job_alive(
    client: httpx.AsyncClient,
    db: Database,
    maintenance: Maintenance,
    streams: Streams,
    settings: Settings,
) -> None:
    # Lease far shorter than the job: only heartbeats keep the sweeper away.
    w = make_worker(db, streams, settings, "steady", lease_seconds=0.3, heartbeat_interval=0.05)
    job = await create_job(client, type="sleep", payload={"seconds": 1})
    running = asyncio.create_task(w.handle(uuid.UUID(job["id"])))

    for _ in range(8):
        await asyncio.sleep(0.1)
        assert await maintenance.sweep_expired_leases() == 0
    assert await running == JobStatus.SUCCEEDED


# --- Stream-level recovery ------------------------------------------------------------------


async def test_unacked_message_of_dead_consumer_is_reclaimed(
    client: httpx.AsyncClient,
    db: Database,
    relay: Relay,
    streams: Streams,
    settings: Settings,
) -> None:
    job = await create_job(client, type="echo")
    await relay.publish_batch()
    # A consumer reads the message and dies before claiming the job.
    assert len(await streams.read("dead-consumer", block_ms=100)) == 1

    rescuer = make_worker(db, streams, settings, "rescuer", reclaim_idle_ms=0)
    assert await rescuer.poll_once("rescuer-0") == 1

    assert (await get_job(client, job["id"]))["status"] == "succeeded"
    pending = await streams.redis.xpending(settings.jobs_stream, settings.consumer_group)
    assert pending["pending"] == 0


async def test_job_whose_message_was_lost_is_redispatched(
    client: httpx.AsyncClient,
    db: Database,
    relay: Relay,
    worker: Worker,
    maintenance: Maintenance,
    streams: Streams,
    settings: Settings,
) -> None:
    job = await create_job(client, type="echo")
    await relay.publish_batch()
    # Redis loses the stream (e.g. restarted without persistence).
    await streams.redis.delete(settings.jobs_stream)
    await streams.ensure_group()
    await execute(
        db,
        "UPDATE jobs SET updated_at = now() - interval '1 hour' WHERE id = :id",
        id=uuid.UUID(job["id"]),
    )

    assert await maintenance.redispatch_stale() == 1
    assert await maintenance.redispatch_stale() == 0  # a dispatch event is already pending

    await relay.publish_batch()
    await worker.poll_once("c1")
    assert (await get_job(client, job["id"]))["status"] == "succeeded"
    assert [e for e, _ in await outbox_events(db, job["id"])][:2] == [
        "job.created",
        "job.redispatched",
    ]


# --- Graceful shutdown ----------------------------------------------------------------------


async def test_shutdown_waits_for_short_running_job(
    client: httpx.AsyncClient, db: Database, relay: Relay, streams: Streams, settings: Settings
) -> None:
    w = make_worker(db, streams, settings, "w", worker_concurrency=1, worker_shutdown_timeout=5)
    job = await create_job(client, type="sleep", payload={"seconds": 0.3})
    await relay.publish_batch()
    stop = asyncio.Event()
    run = asyncio.create_task(w.run(stop))
    await wait_for_status(client, job["id"], "running")

    stop.set()
    async with asyncio.timeout(5):
        await run

    assert (await get_job(client, job["id"]))["status"] == "succeeded"


async def test_shutdown_releases_long_running_job_without_using_an_attempt(
    client: httpx.AsyncClient, db: Database, relay: Relay, streams: Streams, settings: Settings
) -> None:
    w = make_worker(db, streams, settings, "w", worker_concurrency=1, worker_shutdown_timeout=0.2)
    job = await create_job(client, type="sleep", payload={"seconds": 30})
    await relay.publish_batch()
    stop = asyncio.Event()
    run = asyncio.create_task(w.run(stop))
    await wait_for_status(client, job["id"], "running")

    stop.set()
    async with asyncio.timeout(5):
        await run

    released = await get_job(client, job["id"])
    assert released["status"] == "queued"
    assert released["attempts"] == 0
    assert await attempt_statuses(client, job["id"]) == ["released"]
    assert (await outbox_events(db, job["id"]))[-1] == ("job.released", False)


# --- Dead-letter queue ----------------------------------------------------------------------


async def test_dead_job_lands_in_dead_letter_stream_and_can_be_redriven(
    client: httpx.AsyncClient,
    db: Database,
    relay: Relay,
    worker: Worker,
    streams: Streams,
    settings: Settings,
) -> None:
    job = await create_job(client, type="flaky", payload={"fail_rate": 1}, max_attempts=1)
    await relay.publish_batch()
    await worker.poll_once("c1")
    await relay.publish_batch()

    assert [j["id"] for j in (await client.get("/jobs?status=dead")).json()] == [job["id"]]
    assert await streams.redis.xlen(settings.dead_letter_stream) == 1

    response = await client.post(f"/jobs/{job['id']}/retry", json={"additional_attempts": 2})
    assert response.status_code == 200
    redriven = response.json()
    assert (redriven["status"], redriven["max_attempts"]) == ("queued", 3)
    assert (await outbox_events(db, job["id"]))[-1] == ("job.requeued", False)

    # Back through the normal pipeline: two more attempts, then dead again.
    for _ in range(2):
        await relay.publish_batch()
        await worker.poll_once("c1")
    assert (await get_job(client, job["id"]))["status"] == "dead"
    assert await attempt_statuses(client, job["id"]) == ["failed"] * 3


async def test_only_dead_or_cancelled_jobs_can_be_redriven(client: httpx.AsyncClient) -> None:
    job = await create_job(client, type="echo")
    response = await client.post(f"/jobs/{job['id']}/retry")
    assert response.status_code == 409


# --- Outbox housekeeping --------------------------------------------------------------------


async def test_outbox_cleanup_deletes_only_old_published_rows(
    client: httpx.AsyncClient, db: Database, relay: Relay
) -> None:
    old = await create_job(client, type="echo")
    recent = await create_job(client, type="echo")
    await relay.publish_batch()
    unpublished = await create_job(client, type="echo")
    await execute(
        db,
        "UPDATE outbox SET published_at = now() - interval '30 days' WHERE aggregate_id = :id",
        id=uuid.UUID(old["id"]),
    )

    assert await relay.cleanup_published() == 1
    assert await outbox_events(db, old["id"]) == []
    assert await outbox_events(db, recent["id"]) == [("job.created", True)]
    assert await outbox_events(db, unpublished["id"]) == [("job.created", False)]


async def test_early_duplicate_message_cannot_skip_the_retry_backoff(
    client: httpx.AsyncClient, db: Database, relay: Relay, streams: Streams, settings: Settings
) -> None:
    job = await create_job(client, type="flaky", payload={"fail_rate": 1})
    slow_retry = make_worker(db, streams, settings, "w", retry_base_delay=60, retry_jitter=0)
    await relay.publish_batch()
    await slow_retry.poll_once("c1")
    assert (await get_job(client, job["id"]))["status"] == "retrying"

    # A duplicate or stale message for the job arrives while its 60s backoff is still running.
    assert await slow_retry.handle(uuid.UUID(job["id"])) is None
    assert (await get_job(client, job["id"]))["status"] == "retrying"
    assert await attempt_statuses(client, job["id"]) == ["failed"]

    # Once the backoff is over, the job can be claimed again.
    await execute(
        db, "UPDATE jobs SET next_attempt_at = now() WHERE id = :id", id=uuid.UUID(job["id"])
    )
    assert await slow_retry.handle(uuid.UUID(job["id"])) == JobStatus.RETRYING
    assert await attempt_statuses(client, job["id"]) == ["failed", "failed"]


class _Empty(BaseModel):
    pass


@task("test.raises_timeout", max_retries=0, timeout=10)
async def _raises_timeout(payload: _Empty, ctx: JobContext) -> None:
    raise TimeoutError("upstream API did not answer")


@task("test.too_slow", max_retries=0, timeout=0.05)
async def _too_slow(payload: _Empty, ctx: JobContext) -> None:
    await asyncio.sleep(10)


async def test_handler_timeout_error_is_not_reported_as_task_timeout(
    client: httpx.AsyncClient, worker: Worker
) -> None:
    own = await create_job(client, type="test.raises_timeout")
    slow = await create_job(client, type="test.too_slow")

    assert await worker.handle(uuid.UUID(own["id"])) == JobStatus.DEAD
    assert await worker.handle(uuid.UUID(slow["id"])) == JobStatus.DEAD

    assert (await get_job(client, own["id"]))[
        "error"
    ] == "TimeoutError: upstream API did not answer"
    assert (await get_job(client, slow["id"]))["error"] == (
        "TimeoutError: Task timed out after 0.05s"
    )
