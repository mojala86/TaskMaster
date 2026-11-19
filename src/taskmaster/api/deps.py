"""FastAPI dependencies: database session, object storage, settings, and the caller.

Every non-public route resolves the caller (`get_principal`) and charges one request against
their rate limit (`enforce_rate_limit`); FastAPI caches both per request.
"""

import math
from collections.abc import AsyncIterator
from typing import Annotated

import structlog
from fastapi import Depends, Header, HTTPException, Request, Response
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession

from taskmaster import metrics
from taskmaster.auth import ApiKeyResolver, Principal
from taskmaster.config import Settings
from taskmaster.jobs.lifecycle import JobLifecycle
from taskmaster.resources import Resources
from taskmaster.storage import Storage

log = structlog.get_logger("taskmaster.api")


def get_resources(request: Request) -> Resources:
    resources: Resources = request.app.state.resources
    return resources


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with get_resources(request).db.session() as session:
        yield session


def get_storage(request: Request) -> Storage:
    storage = get_resources(request).storage
    if storage is None:
        raise HTTPException(503, "Object storage is not configured")
    return storage


def get_app_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def get_lifecycle(request: Request) -> JobLifecycle:
    lifecycle: JobLifecycle = request.app.state.lifecycle
    return lifecycle


async def get_principal(request: Request) -> Principal:
    """The caller: from `X-API-Key` (or `Authorization: Bearer`), else anonymous by client IP."""
    settings: Settings = request.app.state.settings
    key = request.headers.get("x-api-key")
    auth = request.headers.get("authorization", "")
    if not key and auth.lower().startswith("bearer "):
        key = auth[7:].strip()
    if key:
        resolver: ApiKeyResolver = request.app.state.api_keys
        principal = await resolver.resolve(key)
        if principal is None:
            raise HTTPException(401, "Invalid or revoked API key")
        return principal
    if settings.require_api_key:
        raise HTTPException(
            401, "API key required (X-API-Key header)", headers={"WWW-Authenticate": "Bearer"}
        )
    return Principal.anonymous_client(request.client.host if request.client else "?", settings)


async def enforce_rate_limit(
    request: Request, response: Response, principal: Annotated[Principal, Depends(get_principal)]
) -> Principal:
    limiter = get_resources(request).rate_limiter
    limit = principal.rate_limit_per_minute
    if limiter is None or limit <= 0:
        return principal
    try:
        result = await limiter.hit(principal.subject, limit)
    except RedisError:
        # Fail open: rate limiting is protection, not a reason to stop serving (the API keeps
        # working during a Redis outage, like job submission through the outbox).
        metrics.RATE_LIMITER_ERRORS.inc()
        log.warning("Rate limiter unavailable; allowing request", exc_info=True)
        return principal
    headers = {
        "RateLimit-Limit": str(result.limit),
        "RateLimit-Remaining": str(result.remaining),
        "RateLimit-Reset": str(math.ceil(result.reset_after)),
    }
    if not result.allowed:
        metrics.RATE_LIMITED.labels("anonymous" if principal.anonymous else "api_key").inc()
        raise HTTPException(
            429,
            f"Rate limit of {result.limit} requests per minute exceeded",
            headers=headers | {"Retry-After": str(max(1, math.ceil(result.retry_after)))},
        )
    response.headers.update(headers)
    return principal


SessionDep = Annotated[AsyncSession, Depends(get_session)]
StorageDep = Annotated[Storage, Depends(get_storage)]
SettingsDep = Annotated[Settings, Depends(get_app_settings)]
LifecycleDep = Annotated[JobLifecycle, Depends(get_lifecycle)]
# Resolves the caller and charges one request against their rate limit (once per request).
PrincipalDep = Annotated[Principal, Depends(enforce_rate_limit)]
IdempotencyKey = Annotated[
    str | None,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Retrying a request with the same key returns the original job.",
    ),
]
