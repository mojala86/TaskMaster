"""Phase 4: API keys, per-client rate limits, and per-owner job visibility."""

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from prometheus_client import REGISTRY
from redis.asyncio import Redis

from taskmaster.api.app import create_app
from taskmaster.auth import create_api_key, revoke_api_key
from taskmaster.config import Settings
from taskmaster.db import Database
from taskmaster.queue import Streams
from taskmaster.ratelimit import RateLimiter
from taskmaster.resources import Resources
from taskmaster.storage import Storage

pytestmark = pytest.mark.integration

ClientFactory = Callable[..., Any]


@pytest.fixture
def api(settings: Settings, db: Database, storage: Storage, streams: Streams) -> ClientFactory:
    """Build a client for an app with real rate limiting and the given setting overrides."""

    @asynccontextmanager
    async def make(
        limiter: RateLimiter | None = None, **overrides: Any
    ) -> AsyncIterator[httpx.AsyncClient]:
        app = create_app(
            resources=Resources(
                settings.model_copy(update=overrides),
                db,
                storage=storage,
                rate_limiter=limiter or RateLimiter(streams.redis),
            )
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client

    return make


async def new_key(db: Database, name: str = "client", limit: int | None = None) -> dict[str, str]:
    _, key = await create_api_key(db, name, limit)
    return {"X-API-Key": key}


# --- Rate limiting --------------------------------------------------------------------------


async def test_key_is_limited_to_its_rate(api: ClientFactory, db: Database) -> None:
    headers = await new_key(db, limit=5)
    async with api() as c:
        responses = [await c.get("/jobs", headers=headers) for _ in range(6)]

    assert [r.status_code for r in responses] == [200] * 5 + [429]
    assert [r.headers["RateLimit-Remaining"] for r in responses[:5]] == ["4", "3", "2", "1", "0"]
    assert responses[0].headers["RateLimit-Limit"] == "5"
    rejected = responses[5]
    assert int(rejected.headers["Retry-After"]) >= 1
    assert "5 requests per minute" in rejected.json()["detail"]


async def test_limits_are_per_key(api: ClientFactory, db: Database) -> None:
    busy, other = await new_key(db, "busy", 2), await new_key(db, "other", 2)
    async with api() as c:
        for _ in range(2):
            await c.get("/jobs", headers=busy)
        assert (await c.get("/jobs", headers=busy)).status_code == 429
        assert (await c.get("/jobs", headers=other)).status_code == 200


async def test_anonymous_clients_are_limited_per_ip(api: ClientFactory) -> None:
    async with api(anonymous_rate_limit_per_minute=3) as c:
        codes = [(await c.get("/jobs")).status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]


async def test_bucket_refills_over_time(streams: Streams) -> None:
    limiter = RateLimiter(streams.redis)
    for _ in range(120):  # 120/min = 2 tokens per second
        await limiter.hit("refill-test", 120)
    assert not (await limiter.hit("refill-test", 120)).allowed

    await asyncio.sleep(0.6)

    assert (await limiter.hit("refill-test", 120)).allowed


async def test_limiter_fails_open_when_redis_is_down(api: ClientFactory, db: Database) -> None:
    broken = Redis.from_url("redis://127.0.0.1:1/0")
    before = REGISTRY.get_sample_value("taskmaster_rate_limiter_errors_total") or 0
    try:
        async with api(limiter=RateLimiter(broken)) as c:
            response = await c.post("/jobs", json={"type": "echo"}, headers=await new_key(db))
    finally:
        await broken.aclose()
    assert response.status_code == 201
    assert REGISTRY.get_sample_value("taskmaster_rate_limiter_errors_total") == before + 1


# --- Authentication -------------------------------------------------------------------------


async def test_invalid_and_revoked_keys_are_rejected(api: ClientFactory, db: Database) -> None:
    row, key = await create_api_key(db, "soon-revoked")
    async with api(api_key_cache_seconds=0) as c:
        assert (await c.get("/jobs", headers={"X-API-Key": key})).status_code == 200
        await revoke_api_key(db, row.prefix)
        assert (await c.get("/jobs", headers={"X-API-Key": key})).status_code == 401
        assert (await c.get("/jobs", headers={"X-API-Key": "tm_nope_nope"})).status_code == 401


async def test_bearer_token_works_too(api: ClientFactory, db: Database) -> None:
    _, key = await create_api_key(db, "bearer")
    async with api(require_api_key=True) as c:
        response = await c.get("/jobs", headers={"Authorization": f"Bearer {key}"})
    assert response.status_code == 200


async def test_required_keys_reject_anonymous_but_not_health(api: ClientFactory) -> None:
    async with api(require_api_key=True) as c:
        assert (await c.get("/jobs")).status_code == 401
        assert (await c.get("/health")).status_code == 200
        assert (await c.get("/metrics/")).status_code == 200


# --- Ownership ------------------------------------------------------------------------------


async def test_clients_only_see_their_own_jobs(api: ClientFactory, db: Database) -> None:
    alice, bob = await new_key(db, "alice"), await new_key(db, "bob")
    async with api() as c:
        job = (await c.post("/jobs", json={"type": "echo"}, headers=alice)).json()

        assert (await c.get(f"/jobs/{job['id']}", headers=alice)).status_code == 200
        for path in ("", "/attempts"):
            assert (await c.get(f"/jobs/{job['id']}{path}", headers=bob)).status_code == 404
        assert (await c.post(f"/jobs/{job['id']}/cancel", headers=bob)).status_code == 404
        assert (await c.get(f"/jobs/{job['id']}")).status_code == 404  # anonymous
        assert (await c.get("/jobs", headers=bob)).json() == []
        assert (await c.get("/jobs/stats", headers=alice)).json() == {"queued": 1}
        assert (await c.get("/jobs/stats", headers=bob)).json() == {}


async def test_idempotency_keys_are_per_client(api: ClientFactory, db: Database) -> None:
    alice, bob = await new_key(db, "alice"), await new_key(db, "bob")
    async with api() as c:
        a = await c.post("/jobs", json={"type": "echo"}, headers=alice | {"Idempotency-Key": "k"})
        b = await c.post("/jobs", json={"type": "echo"}, headers=bob | {"Idempotency-Key": "k"})
        anon = [
            await c.post("/jobs", json={"type": "echo"}, headers={"Idempotency-Key": "k"})
            for _ in range(2)
        ]

    assert (a.status_code, b.status_code) == (201, 201)
    assert a.json()["id"] != b.json()["id"]
    # Anonymous callers share one namespace, and still deduplicate.
    assert [r.status_code for r in anon] == [201, 200]
