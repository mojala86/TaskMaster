import asyncio
import uuid

import httpx
import pytest
from sqlalchemy import func, select

from taskmaster.config import Settings
from taskmaster.db import Database, JobStatus, OutboxEvent
from taskmaster.queue import Streams
from taskmaster.relay import Relay
from taskmaster.worker import Worker
from tests.integration.helpers import create_job, outbox_events

pytestmark = pytest.mark.integration


async def test_create_job_writes_job_and_outbox_event_only(
    client: httpx.AsyncClient, db: Database, streams: Streams, settings: Settings
) -> None:
    job = await create_job(client, type="echo", payload={"data": {"a": 1}})

    assert job["status"] == "queued"
    assert job["max_attempts"] == 3
    assert await outbox_events(db, job["id"]) == [("job.created", False)]
    # The API never touches Redis; only the relay does.
    assert await streams.redis.xlen(settings.jobs_stream) == 0


async def test_rejects_unknown_type_and_invalid_payload(client: httpx.AsyncClient) -> None:
    unknown = await client.post("/jobs", json={"type": "nope"})
    invalid = await client.post("/jobs", json={"type": "sleep", "payload": {"seconds": -1}})

    assert unknown.status_code == 422
    assert invalid.status_code == 422
    assert invalid.json()["detail"][0]["loc"] == ["seconds"]


async def test_job_runs_end_to_end(
    client: httpx.AsyncClient,
    db: Database,
    relay: Relay,
    worker: Worker,
    streams: Streams,
    settings: Settings,
) -> None:
    job = await create_job(client, type="echo", payload={"data": {"hello": "world"}})

    assert (await relay.publish_batch()).published == 1
    assert await worker.poll_once("c1") == 1

    done = (await client.get(f"/jobs/{job['id']}")).json()
    assert done["status"] == "succeeded"
    assert done["result"] == {"data": {"hello": "world"}}
    attempts = (await client.get(f"/jobs/{job['id']}/attempts")).json()
    assert [(a["attempt"], a["status"]) for a in attempts] == [(1, "succeeded")]

    # Completion event goes through the outbox too, onto the events stream.
    assert (await relay.publish_batch()).published == 1
    assert await outbox_events(db, job["id"]) == [("job.created", True), ("job.succeeded", True)]
    assert await streams.redis.xlen(settings.events_stream) == 1
    # Handled message is acked and removed.
    assert await streams.redis.xlen(settings.jobs_stream) == 0


async def test_duplicate_delivery_runs_job_once(
    client: httpx.AsyncClient, relay: Relay, worker: Worker, streams: Streams, settings: Settings
) -> None:
    job = await create_job(client, type="echo")
    await relay.publish_batch()
    # Simulate at-least-once redelivery (e.g. relay crashed before stamping published_at).
    await streams.redis.xadd(settings.jobs_stream, {"job_id": job["id"]})

    assert await worker.handle(uuid.UUID(job["id"])) == JobStatus.SUCCEEDED
    assert await worker.handle(uuid.UUID(job["id"])) is None

    attempts = (await client.get(f"/jobs/{job['id']}/attempts")).json()
    assert len(attempts) == 1


async def test_failing_job_retries_then_dies(
    client: httpx.AsyncClient, db: Database, relay: Relay, worker: Worker
) -> None:
    job = await create_job(client, type="flaky", payload={"fail_rate": 1}, max_attempts=2)

    await relay.publish_batch()
    await worker.poll_once("c1")
    assert (await client.get(f"/jobs/{job['id']}")).json()["status"] == "retrying"

    await relay.publish_batch()  # publishes job.retry_scheduled (backoff 0 in tests)
    await worker.poll_once("c1")

    dead = (await client.get(f"/jobs/{job['id']}")).json()
    assert dead["status"] == "dead"
    assert dead["attempts"] == 2
    assert "Simulated failure" in dead["error"]
    attempts = (await client.get(f"/jobs/{job['id']}/attempts")).json()
    assert [a["status"] for a in attempts] == ["failed", "failed"]
    assert [e for e, _ in await outbox_events(db, job["id"])] == [
        "job.created",
        "job.retry_scheduled",
        "job.dead",
    ]


async def test_retry_waits_for_backoff(
    client: httpx.AsyncClient, db: Database, streams: Streams, settings: Settings
) -> None:
    slow = settings.model_copy(update={"retry_base_delay": 3600})
    worker = Worker(db, streams, slow, worker_id="slow-worker")
    relay = Relay(db, streams, slow)
    job = await create_job(client, type="flaky", payload={"fail_rate": 1}, max_attempts=3)

    await relay.publish_batch()
    await worker.poll_once("c1")

    # The retry event exists but is not due for an hour, so nothing is published.
    assert (await relay.publish_batch()).fetched == 0
    assert await outbox_events(db, job["id"]) == [
        ("job.created", True),
        ("job.retry_scheduled", False),
    ]


async def test_relay_keeps_events_when_redis_is_down(
    client: httpx.AsyncClient, db: Database, settings: Settings
) -> None:
    job = await create_job(client, type="echo")
    broken = Streams.from_settings(
        settings.model_copy(update={"redis_url": "redis://127.0.0.1:1/0"})
    )
    try:
        result = await Relay(db, broken, settings).publish_batch()
    finally:
        await broken.close()

    assert (result.fetched, result.published) == (1, 0)
    async with db.session() as session:
        event = (
            await session.scalars(
                select(OutboxEvent).where(OutboxEvent.aggregate_id == uuid.UUID(job["id"]))
            )
        ).one()
    assert event.published_at is None
    assert event.attempts == 1
    assert event.last_error


async def test_concurrent_relays_publish_each_event_once(
    client: httpx.AsyncClient, db: Database, streams: Streams, settings: Settings
) -> None:
    for _ in range(60):
        await create_job(client, type="echo")
    small_batches = settings.model_copy(update={"relay_batch_size": 7})
    relays = [Relay(db, streams, small_batches) for _ in range(3)]

    async def drain(relay: Relay) -> int:
        total = 0
        while (result := await relay.publish_batch()).fetched:
            total += result.published
        return total

    published = await asyncio.gather(*(drain(r) for r in relays))

    assert sum(published) == 60
    assert await streams.redis.xlen(settings.jobs_stream) == 60
    async with db.session() as session:
        unpublished = await session.scalar(
            select(func.count()).where(OutboxEvent.published_at.is_(None))
        )
    assert unpublished == 0
