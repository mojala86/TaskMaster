"""Shared connections (Postgres, Redis, object storage), built in one place.

Every entry point — the API, worker, relay and CLI commands — opens what it needs with
`open_resources` and gets them closed again on exit. Timeouts, pool sizes and TLS settings are
therefore applied the same way everywhere. Tests build a `Resources` directly from their own
fixtures.

    async with open_resources(settings, streams=True, storage=True) as r:
        await Worker(r.db, r.streams, r.settings, storage=r.storage).run(stop)
"""

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

import structlog

from taskmaster.config import Settings
from taskmaster.db import Database
from taskmaster.queue import Streams
from taskmaster.queue.streams import connect_redis
from taskmaster.ratelimit import RateLimiter
from taskmaster.storage import Storage

log = structlog.get_logger("taskmaster")


@dataclass(frozen=True)
class Resources:
    settings: Settings
    db: Database
    streams: Streams | None = None
    storage: Storage | None = None  # None when object storage is not configured
    rate_limiter: RateLimiter | None = None

    def require_streams(self) -> Streams:
        if self.streams is None:
            raise RuntimeError("Redis streams were not opened for this process")
        return self.streams

    def require_storage(self) -> Storage:
        if self.storage is None:
            raise RuntimeError("Object storage is not configured (TASKMASTER_S3_ENDPOINT_URL)")
        return self.storage


@asynccontextmanager
async def open_resources(
    settings: Settings,
    *,
    streams: bool = False,
    storage: bool = False,
    rate_limiter: bool = False,
    pool_size: int = 10,
) -> AsyncGenerator[Resources]:
    """Open the requested connections; close them all on exit, in reverse order."""
    async with AsyncExitStack() as stack:
        db = Database(
            settings.database_url, pool_size=pool_size, connect_timeout=settings.db_connect_timeout
        )
        stack.push_async_callback(db.dispose)

        opened_streams = None
        if streams:
            opened_streams = Streams.from_settings(settings)
            stack.push_async_callback(opened_streams.close)

        opened_storage = None
        if storage and settings.s3_endpoint_url:
            opened_storage = await Storage(settings).start()
            stack.push_async_callback(opened_storage.close)
            try:
                await opened_storage.ensure_bucket()
            except Exception:
                # Not fatal: file endpoints fail (503) until it is reachable; the rest works.
                log.warning("Object storage not reachable at startup", exc_info=True)

        limiter = None
        if rate_limiter:
            # Short timeout: a slow Redis must not slow every request (the limiter fails open).
            redis = connect_redis(settings, socket_timeout=settings.rate_limit_timeout)
            stack.push_async_callback(redis.aclose)
            limiter = RateLimiter(redis)

        yield Resources(settings, db, opened_streams, opened_storage, limiter)
