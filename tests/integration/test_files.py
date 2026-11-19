"""Phase 3: files through object storage, end to end (presigned upload -> job -> download)."""

import csv
import hashlib
import io
import os
import uuid
from typing import Any

import httpx
import pytest
from PIL import Image

from taskmaster.api.app import create_app
from taskmaster.config import Settings
from taskmaster.db import Database
from taskmaster.relay import Relay
from taskmaster.resources import Resources
from taskmaster.storage import Storage
from taskmaster.worker import Worker
from tests.integration.helpers import attempt_statuses, create_job, get_job, upload

pytestmark = pytest.mark.integration


async def run_job(
    client: httpx.AsyncClient, relay: Relay, worker: Worker, **body: Any
) -> dict[str, Any]:
    job = await create_job(client, **body)
    await relay.publish_batch()
    assert await worker.poll_once("c1") == 1
    return await get_job(client, job["id"])


async def download_outputs(client: httpx.AsyncClient, job_id: str) -> dict[str, bytes]:
    response = await client.get(f"/jobs/{job_id}/outputs")
    assert response.status_code == 200, response.text
    files = {}
    async with httpx.AsyncClient() as http:
        for output in response.json():
            got = await http.get(output["url"])
            assert got.status_code == 200
            assert got.headers["content-disposition"] == f'attachment; filename="{output["name"]}"'
            assert len(got.content) == output["size"]
            files[output["name"]] = got.content
    return files


async def test_csv_aggregate_end_to_end(
    client: httpx.AsyncClient, relay: Relay, worker: Worker
) -> None:
    rows = ["region,amount"] + [f"{r},{i}" for i in range(1, 1001) for r in ("north", "south")]
    key = await upload(client, "\n".join(rows).encode(), "sales.csv", "text/csv")

    job = await run_job(
        client,
        relay,
        worker,
        type="csv.aggregate",
        payload={
            "file": key,
            "group_by": ["region"],
            "aggregations": [{"op": "count"}, {"op": "sum", "column": "amount"}],
        },
    )

    assert job["status"] == "succeeded", job["error"]
    assert job["result"] == {
        "rows": 2000,
        "groups": 2,
        "skipped_rows": 0,
        "invalid_values": 0,
        "output": "aggregate.csv",
    }
    assert job["progress"] == 1.0
    outputs = await download_outputs(client, job["id"])
    assert list(csv.reader(io.StringIO(outputs["aggregate.csv"].decode()))) == [
        ["region", "count", "sum_amount"],
        ["north", "1000", "500500"],
        ["south", "1000", "500500"],
    ]


async def test_checksum_streams_the_input(
    client: httpx.AsyncClient, relay: Relay, worker: Worker
) -> None:
    data = os.urandom(3 * 1024 * 1024 + 17)  # several chunks
    key = await upload(client, data, "blob.bin", "application/octet-stream")

    job = await run_job(client, relay, worker, type="file.checksum", payload={"file": key})

    assert job["status"] == "succeeded", job["error"]
    assert job["result"] == {
        "algorithm": "sha256",
        "digest": hashlib.sha256(data).hexdigest(),
        "bytes": len(data),
    }


async def test_image_resize_end_to_end(
    client: httpx.AsyncClient, relay: Relay, worker: Worker
) -> None:
    buffer = io.BytesIO()
    Image.new("RGB", (640, 480), "blue").save(buffer, format="PNG")
    key = await upload(client, buffer.getvalue(), "photo.png", "image/png")

    job = await run_job(
        client,
        relay,
        worker,
        type="image.resize",
        payload={"file": key, "width": 160, "format": "webp"},
    )

    assert job["status"] == "succeeded", job["error"]
    assert (job["result"]["width"], job["result"]["height"]) == (160, 120)
    outputs = await download_outputs(client, job["id"])
    with Image.open(io.BytesIO(outputs["resized.webp"])) as img:
        assert (img.format, img.size) == ("WEBP", (160, 120))


async def test_malformed_input_fails_permanently_without_outputs(
    client: httpx.AsyncClient, db: Database, relay: Relay, worker: Worker
) -> None:
    key = await upload(client, b"a,b\n1,2\n", "wrong.csv", "text/csv")

    job = await run_job(
        client,
        relay,
        worker,
        type="csv.aggregate",
        payload={"file": key, "aggregations": [{"op": "sum", "column": "amount"}]},
    )

    # PermanentError: dead on the first attempt although retries were available.
    assert job["status"] == "dead"
    assert job["max_attempts"] == 3
    assert "Columns not found" in job["error"]
    assert await attempt_statuses(client, job["id"]) == ["failed"]
    assert (await client.get(f"/jobs/{job['id']}/outputs")).json() == []


async def test_missing_input_file_is_rejected_at_submission(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={"type": "file.checksum", "payload": {"file": f"uploads/{uuid.uuid4()}/nope.bin"}},
    )
    assert response.status_code == 422
    assert "not found" in response.json()["detail"]


async def test_file_reference_must_be_an_upload(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/jobs", json={"type": "file.checksum", "payload": {"file": "results/other-job/secret"}}
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["file"]


async def test_upload_filename_is_sanitized(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/uploads", json={"filename": "..\\..\\etc/pass wd?.csv", "size": 1}
    )
    assert response.status_code == 201
    assert response.json()["key"].endswith("/pass_wd_.csv")


async def test_file_endpoints_need_storage(settings: Settings, db: Database) -> None:
    app = create_app(resources=Resources(settings, db))  # no object storage configured
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        assert (await c.post("/uploads", json={"filename": "x.csv", "size": 1})).status_code == 503
        # Jobs that don't use files still work.
        assert (await c.post("/jobs", json={"type": "echo"})).status_code == 201


async def test_upload_url_only_accepts_the_declared_size(client: httpx.AsyncClient) -> None:
    ticket = (await client.post("/uploads", json={"filename": "a.bin", "size": 3})).json()
    headers = {k: v for k, v in ticket["headers"].items() if k != "Content-Length"}
    async with httpx.AsyncClient() as http:
        bigger = await http.put(ticket["upload_url"], content=b"x" * 1000, headers=headers)
        assert bigger.status_code == 403, bigger.text  # signature covers Content-Length
        exact = await http.put(ticket["upload_url"], content=b"abc", headers=headers)
        assert exact.status_code == 200, exact.text


async def test_uploads_above_the_limit_are_refused(
    settings: Settings, db: Database, storage: Storage
) -> None:
    limited = settings.model_copy(update={"max_upload_bytes": 10})
    app = create_app(resources=Resources(limited, db, storage=storage))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        response = await c.post("/uploads", json={"filename": "big.bin", "size": 11})
        assert response.status_code == 413
        # An oversized object that got into the bucket anyway can't be used as job input.
        key = f"uploads/{uuid.uuid4()}/big.bin"
        await storage.put_bytes(key, b"x" * 11, "application/octet-stream")
        response = await c.post("/jobs", json={"type": "file.checksum", "payload": {"file": key}})
        assert response.status_code == 413
