import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from sqlalchemy import select, text

from taskmaster.db import Database, OutboxEvent


async def create_job(
    client: httpx.AsyncClient, headers: dict[str, str] | None = None, **body: Any
) -> dict[str, Any]:
    response = await client.post("/jobs", json=body, headers=headers)
    assert response.status_code == 201, response.text
    job: dict[str, Any] = response.json()
    return job


async def get_job(client: httpx.AsyncClient, job_id: str) -> dict[str, Any]:
    response = await client.get(f"/jobs/{job_id}")
    assert response.status_code == 200, response.text
    job: dict[str, Any] = response.json()
    return job


async def attempt_statuses(client: httpx.AsyncClient, job_id: str) -> list[str]:
    attempts = (await client.get(f"/jobs/{job_id}/attempts")).json()
    return [a["status"] for a in attempts]


async def outbox_events(db: Database, job_id: str) -> list[tuple[str, bool]]:
    """(event_type, published?) for a job, oldest first."""
    async with db.session() as session:
        rows = await session.execute(
            select(OutboxEvent.event_type, OutboxEvent.published_at.is_not(None))
            .where(OutboxEvent.aggregate_id == uuid.UUID(job_id))
            .order_by(OutboxEvent.id)
        )
        return [(t, bool(p)) for t, p in rows.all()]


async def execute(db: Database, sql: str, **params: Any) -> None:
    async with db.session() as session, session.begin():
        await session.execute(text(sql), params)


async def wait_until(predicate: Callable[[], Awaitable[bool]], interval: float = 0.02) -> None:
    """Poll until `predicate` is true. Bound it with `async with asyncio.timeout(...)`."""
    # Polling is the point here: the state lives in Postgres, there is no event to wait on.
    while not await predicate():  # noqa: ASYNC110
        await asyncio.sleep(interval)


async def wait_for_status(client: httpx.AsyncClient, job_id: str, status: str) -> None:
    async def reached() -> bool:
        return (await get_job(client, job_id))["status"] == status

    async with asyncio.timeout(5):
        await wait_until(reached)


async def upload(client: httpx.AsyncClient, data: bytes, filename: str, content_type: str) -> str:
    response = await client.post(
        "/uploads", json={"filename": filename, "content_type": content_type, "size": len(data)}
    )
    assert response.status_code == 201, response.text
    ticket = response.json()
    # Straight to object storage, like a real client: the API never sees the bytes.
    async with httpx.AsyncClient() as http:
        put = await http.put(ticket["upload_url"], content=data, headers=ticket["headers"])
    assert put.status_code == 200, put.text
    key: str = ticket["key"]
    return key
