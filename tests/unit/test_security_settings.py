"""Production safety checks, request size limit and CORS. No database needed."""

from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
from pydantic import ValidationError

from taskmaster.api.app import create_app
from taskmaster.config import Settings
from taskmaster.db import Database
from taskmaster.resources import Resources

SAFE = {
    "env": "production",
    "require_api_key": True,
    "database_url": "postgresql+psycopg://tm:s3cret-pw@db:5432/tm?sslmode=verify-full",
    "redis_url": "rediss://cache:6380/0",
    "s3_endpoint_url": "https://s3.internal",
    "s3_access_key": "AKIAEXAMPLE",
    "s3_secret_key": "a-real-secret",
}


@pytest.fixture(autouse=True)
def no_env_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    # Settings read TASKMASTER_* from the environment; keep the developer's shell out of it.
    import os

    for name in [n for n in os.environ if n.startswith("TASKMASTER_")]:
        monkeypatch.delenv(name)
    yield


def test_development_defaults_are_allowed() -> None:
    assert Settings().env == "development"


def test_production_with_safe_settings_starts() -> None:
    assert Settings(**SAFE).production_problems() == []


@pytest.mark.parametrize(
    ("override", "problem"),
    [
        ({"require_api_key": False}, "require_api_key"),
        ({"database_url": "postgresql+psycopg://tm:taskmaster@db/tm?sslmode=require"}, "password"),
        ({"s3_secret_key": "taskmaster-secret"}, "s3_secret_key"),
        ({"s3_verify": False}, "s3_verify"),
        ({"database_url": "postgresql+psycopg://tm:pw@db/tm"}, "sslmode"),
        ({"redis_url": "redis://cache:6379/0"}, "rediss://"),
        ({"s3_endpoint_url": "http://s3.internal"}, "https://"),
    ],
)
def test_production_refuses_unsafe_settings(override: dict[str, object], problem: str) -> None:
    with pytest.raises(ValidationError, match="Refusing to start") as info:
        Settings(**(SAFE | override))
    assert problem in str(info.value)


def test_insecure_transport_can_be_allowed_explicitly() -> None:
    settings = Settings(
        **SAFE
        | {
            "redis_url": "redis://cache:6379/0",
            "database_url": "postgresql+psycopg://tm:pw@db/tm",
            "allow_insecure_transport": True,
        }
    )
    assert settings.production_problems() == []


def test_redis_timeout_must_exceed_the_blocking_read() -> None:
    with pytest.raises(ValidationError, match="redis_socket_timeout"):
        Settings(redis_socket_timeout=1.0, worker_block_ms=2000)


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    settings = Settings(max_request_bytes=100, cors_origins=["https://app.example.com"])
    # Never connects: these requests are answered before anything touches the database.
    db = Database("postgresql+psycopg://x:y@127.0.0.1:1/x")
    app = create_app(resources=Resources(settings, db))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def test_oversized_body_is_refused(client: httpx.AsyncClient) -> None:
    body = b'{"type": "echo", "payload": {"data": "' + b"x" * 200 + b'"}}'
    response = await client.post(
        "/jobs", content=body, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 413


async def test_oversized_chunked_body_is_refused(client: httpx.AsyncClient) -> None:
    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(10):
            yield b"x" * 50

    response = await client.post(
        "/jobs", content=chunks(), headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 413


async def test_cors_allows_only_configured_origins(client: httpx.AsyncClient) -> None:
    def preflight(origin: str) -> dict[str, str]:
        return {"Origin": origin, "Access-Control-Request-Method": "POST"}

    ok = await client.options("/jobs", headers=preflight("https://app.example.com"))
    assert ok.headers["access-control-allow-origin"] == "https://app.example.com"
    evil = await client.options("/jobs", headers=preflight("https://evil.example"))
    assert "access-control-allow-origin" not in evil.headers
