"""When Postgres or object storage can't be reached, the API answers 503 (retry later), not 500."""

from collections.abc import AsyncIterator

import httpx
import pytest

from taskmaster.api.app import create_app
from taskmaster.config import Settings
from taskmaster.db import Database
from taskmaster.resources import Resources
from taskmaster.storage import Storage
from tests.integration.helpers import upload

pytestmark = pytest.mark.integration

NOWHERE = "127.0.0.1:1"  # nothing listens on port 1: connections are refused immediately


@pytest.fixture
async def unreachable_db() -> AsyncIterator[Database]:
    db = Database(f"postgresql+psycopg://x:y@{NOWHERE}/x", connect_timeout=1)
    yield db
    await db.dispose()


async def test_database_down_gives_503(settings: Settings, unreachable_db: Database) -> None:
    app = create_app(resources=Resources(settings, unreachable_db))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        health = await c.get("/health")
        assert (health.status_code, health.json()) == (503, {"status": "unavailable"})

        response = await c.post("/jobs", json={"type": "echo"})
        assert response.status_code == 503
        assert response.headers["Retry-After"]


async def test_storage_down_gives_503(
    settings: Settings, db: Database, client: httpx.AsyncClient
) -> None:
    key = await upload(client, b"abc", "a.bin", "application/octet-stream")
    broken = await Storage(
        settings.model_copy(update={"s3_endpoint_url": f"http://{NOWHERE}"})
    ).start()
    try:
        app = create_app(resources=Resources(settings, db, storage=broken))
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            response = await c.post(
                "/jobs", json={"type": "file.checksum", "payload": {"file": key}}
            )
            assert response.status_code == 503
    finally:
        await broken.close()
