"""Integration fixtures: real Postgres, Redis and S3 (RustFS) via testcontainers (needs Docker)."""

import time
import urllib.request
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from taskmaster.api.app import create_app
from taskmaster.config import Settings
from taskmaster.db import Database
from taskmaster.queue import Streams
from taskmaster.relay import Relay
from taskmaster.resources import Resources
from taskmaster.storage import Storage
from taskmaster.worker import Maintenance, Worker

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


S3_IMAGE = "rustfs/rustfs:1.0.4"
S3_ACCESS_KEY = "test-access"
S3_SECRET_KEY = "test-secret-key"


@pytest.fixture(scope="session")
def containers() -> Iterator[tuple[str, str, str]]:
    try:
        from testcontainers.community.postgres import PostgresContainer
        from testcontainers.community.redis import RedisContainer
        from testcontainers.core.container import DockerContainer

        pg = PostgresContainer("postgres:17-alpine", driver="psycopg").start()
    except Exception as exc:  # Docker not running / not installed
        pytest.skip(f"Docker unavailable: {exc}")
    redis = RedisContainer("redis:7-alpine").start()
    s3 = (
        DockerContainer(S3_IMAGE)
        .with_env("RUSTFS_ACCESS_KEY", S3_ACCESS_KEY)
        .with_env("RUSTFS_SECRET_KEY", S3_SECRET_KEY)
        .with_exposed_ports(9000)
        .start()
    )
    try:
        redis_url = f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"
        s3_url = f"http://{s3.get_container_host_ip()}:{s3.get_exposed_port(9000)}"
        _wait_for_http(f"{s3_url}/health")
        yield pg.get_connection_url(), redis_url, s3_url
    finally:
        s3.stop()
        redis.stop()
        pg.stop()


def _wait_for_http(url: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


@pytest.fixture(scope="session")
def settings(containers: tuple[str, str, str]) -> Settings:
    database_url, redis_url, s3_url = containers
    return Settings(
        database_url=database_url,
        redis_url=redis_url,
        s3_endpoint_url=s3_url,
        s3_access_key=S3_ACCESS_KEY,
        s3_secret_key=S3_SECRET_KEY,
        worker_block_ms=100,
        heartbeat_interval=0.05,
        retry_base_delay=0,  # retries become due immediately
        retry_jitter=0,
    )


@pytest.fixture(scope="session")
def migrated(settings: Settings) -> None:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", settings.database_url)
    cfg.attributes["configure_logging"] = False
    command.upgrade(cfg, "head")


@pytest.fixture(scope="session")
async def db(settings: Settings, migrated: None) -> AsyncIterator[Database]:
    database = Database(settings.database_url)
    yield database
    await database.dispose()


@pytest.fixture(scope="session")
async def storage(settings: Settings) -> AsyncIterator[Storage]:
    s = await Storage(settings).start()
    await s.ensure_bucket()
    yield s
    await s.close()


@pytest.fixture
async def streams(settings: Settings) -> AsyncIterator[Streams]:
    s = Streams.from_settings(settings)
    await s.redis.flushdb()
    await s.ensure_group()
    yield s
    await s.close()


@pytest.fixture(autouse=True)
async def clean_db(db: Database) -> None:
    async with db.session() as session, session.begin():
        await session.execute(
            text("TRUNCATE jobs, job_attempts, outbox, api_keys RESTART IDENTITY CASCADE")
        )


@pytest.fixture
async def client(
    settings: Settings, db: Database, storage: Storage
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(resources=Resources(settings, db, storage=storage))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
def relay(db: Database, streams: Streams, settings: Settings) -> Relay:
    return Relay(db, streams, settings)


@pytest.fixture
def worker(db: Database, streams: Streams, settings: Settings, storage: Storage) -> Worker:
    return Worker(db, streams, settings, worker_id="test-worker", storage=storage)


@pytest.fixture
def maintenance(
    db: Database, streams: Streams, settings: Settings, storage: Storage
) -> Maintenance:
    return Maintenance(db, streams, settings, worker_id="test-maintenance", storage=storage)
