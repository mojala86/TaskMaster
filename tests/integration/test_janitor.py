"""Storage janitor: deletes what nothing references, keeps what something still needs.

Retentions of -1 hour mean "already expired", so tests don't depend on the host, Postgres and
the S3 container agreeing on the time to the millisecond.
"""

import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from sqlalchemy import text

from taskmaster.config import Settings
from taskmaster.db import Database
from taskmaster.janitor import LOCK_ID, StorageJanitor
from taskmaster.queue import Streams
from taskmaster.relay import Relay
from taskmaster.storage import Storage
from taskmaster.worker import Maintenance, Worker
from tests.integration.helpers import create_job, execute, get_job, upload

pytestmark = pytest.mark.integration

EXPIRED = -1.0


@pytest.fixture(autouse=True)
async def empty_bucket(storage: Storage) -> AsyncIterator[None]:
    keys = [o.key async for o in storage.list_objects("")]
    await storage.delete_many(keys)
    yield


def janitor(db: Database, storage: Storage, settings: Settings, **overrides: Any) -> StorageJanitor:
    return StorageJanitor(db, storage, settings.model_copy(update=overrides))


async def exists(storage: Storage, key: str) -> bool:
    return await storage.head(key) is not None


async def test_files_of_lost_attempt_are_deleted_and_real_outputs_kept(
    client: httpx.AsyncClient,
    db: Database,
    relay: Relay,
    worker: Worker,
    maintenance: Maintenance,
    streams: Streams,
    storage: Storage,
    settings: Settings,
) -> None:
    key = await upload(client, b"region,amount\nn,1\nn,2\n", "in.csv", "text/csv")
    job = await create_job(
        client,
        type="csv.aggregate",
        payload={"file": key, "aggregations": [{"op": "sum", "column": "amount"}]},
    )
    # A worker claims the job, writes a partial output, then dies.
    zombie = Worker(db, streams, settings, worker_id="zombie", storage=storage)
    claim = await zombie.claim(uuid.UUID(job["id"]))
    assert claim is not None
    partial = f"results/{job['id']}/{claim.attempt_id}/aggregate.csv"
    await storage.put_bytes(partial, b"partial", "text/csv")

    # While the attempt is still running its files are untouchable.
    report = await janitor(db, storage, settings, orphan_grace_hours=EXPIRED).run_once()
    assert report is not None and report.orphaned_results == 0

    await execute(
        db,
        "UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = :id",
        id=uuid.UUID(job["id"]),
    )
    await maintenance.sweep_expired_leases()
    await relay.publish_batch()
    while await worker.poll_once("c1"):
        pass
    assert (await get_job(client, job["id"]))["status"] == "succeeded"
    [output] = (await client.get(f"/jobs/{job['id']}/outputs")).json()

    # Within the grace period the lost attempt's file stays (a lost worker may still be writing).
    report = await janitor(db, storage, settings).run_once()
    assert report is not None and report.orphaned_results == 0
    assert await exists(storage, partial)

    report = await janitor(db, storage, settings, orphan_grace_hours=EXPIRED).run_once()
    assert report is not None and report.orphaned_results == 1
    assert not await exists(storage, partial)
    async with httpx.AsyncClient() as http:
        assert (await http.get(output["url"])).status_code == 200  # the real output survived


async def test_stale_uploads_are_deleted_unless_an_unfinished_job_needs_them(
    client: httpx.AsyncClient, db: Database, storage: Storage, settings: Settings
) -> None:
    abandoned = await upload(client, b"abc", "abandoned.bin", "application/octet-stream")
    needed = await upload(client, b"def", "needed.bin", "application/octet-stream")
    await create_job(client, type="file.checksum", payload={"file": needed})  # queued, not run

    report = await janitor(db, storage, settings).run_once()  # default: 7 days
    assert report is not None and report.expired_uploads == 0

    report = await janitor(db, storage, settings, upload_retention_hours=EXPIRED).run_once()
    assert report is not None and report.expired_uploads == 1
    assert not await exists(storage, abandoned)
    assert await exists(storage, needed)


async def test_output_retention_removes_rows_and_files(
    client: httpx.AsyncClient,
    db: Database,
    relay: Relay,
    worker: Worker,
    storage: Storage,
    settings: Settings,
) -> None:
    key = await upload(client, b"region,amount\nn,1\n", "in.csv", "text/csv")
    job = await create_job(
        client, type="csv.aggregate", payload={"file": key, "aggregations": [{"op": "count"}]}
    )
    await relay.publish_batch()
    await worker.poll_once("c1")
    assert len((await client.get(f"/jobs/{job['id']}/outputs")).json()) == 1

    # Off by default: outputs are kept.
    report = await janitor(db, storage, settings, orphan_grace_hours=EXPIRED).run_once()
    assert report is not None and report.expired_outputs == 0

    report = await janitor(
        db, storage, settings, output_retention_hours=EXPIRED, orphan_grace_hours=EXPIRED
    ).run_once()
    assert report is not None and report.expired_outputs == 1
    assert (await client.get(f"/jobs/{job['id']}/outputs")).json() == []
    assert [o.key async for o in storage.list_objects("results/")] == []


async def test_unrecognized_keys_are_left_alone(
    db: Database, storage: Storage, settings: Settings
) -> None:
    await storage.put_bytes("results/not-a-job/file.txt", b"x", "text/plain")

    report = await janitor(db, storage, settings, orphan_grace_hours=EXPIRED).run_once()

    assert report is not None and report.unrecognized_keys == 1
    assert await exists(storage, "results/not-a-job/file.txt")


async def test_only_one_cleanup_runs_at_a_time(
    db: Database, storage: Storage, settings: Settings
) -> None:
    async with db.engine.connect() as raw:
        conn = await raw.execution_options(isolation_level="AUTOCOMMIT")
        assert await conn.scalar(text("SELECT pg_try_advisory_lock(:id)"), {"id": LOCK_ID})
        try:
            assert await janitor(db, storage, settings).run_once() is None
        finally:
            await conn.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": LOCK_ID})

    assert await janitor(db, storage, settings).run_once() is not None
