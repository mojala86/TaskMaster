"""API application factory."""

import time
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from taskmaster import __version__, metrics
from taskmaster.api.errors import install_error_handlers
from taskmaster.api.limits import BodySizeLimit
from taskmaster.api.routes import include_routes
from taskmaster.auth import ApiKeyResolver
from taskmaster.config import Settings, get_settings
from taskmaster.jobs.lifecycle import JobLifecycle
from taskmaster.log import configure_logging
from taskmaster.resources import Resources, open_resources

log = structlog.get_logger("taskmaster.api")


def create_app(settings: Settings | None = None, resources: Resources | None = None) -> FastAPI:
    """App factory. Without `resources` the lifespan opens them (production); tests pass their
    own (the lifespan doesn't run under httpx's ASGITransport).

    Object storage is optional: without it, file endpoints answer 503. Redis is only used for
    rate limiting, which fails open, so the API keeps accepting jobs while Redis is down.
    """
    settings = settings or (resources.settings if resources else get_settings())
    if resources is None:
        # Production path (also under `--reload`, where the factory runs in a fresh process).
        configure_logging(settings.log_level, settings.log_format)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        if resources is not None:
            yield
            return
        async with open_resources(settings, storage=True, rate_limiter=True) as opened:
            _attach(app, opened)
            yield

    app = FastAPI(title="TaskMaster", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.lifecycle = JobLifecycle(settings)
    if resources is not None:
        _attach(app, resources)
    install_error_handlers(app)
    # Middleware added later wraps what was added earlier. The size limit sits inside the request
    # observer, so its 413 is logged and counted, and nothing in between buffers the body.
    app.add_middleware(BodySizeLimit, max_bytes=settings.max_request_bytes)
    app.middleware("http")(_observe_requests)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_methods=["GET", "POST"],
            allow_headers=["Authorization", "X-API-Key", "Content-Type", "Idempotency-Key"],
            expose_headers=[
                "X-Request-ID",
                "Retry-After",
                "Idempotent-Replayed",
                "RateLimit-Limit",
                "RateLimit-Remaining",
                "RateLimit-Reset",
            ],
        )
    include_routes(app)
    app.mount("/metrics", make_asgi_app())
    return app


def _attach(app: FastAPI, resources: Resources) -> None:
    """The one place request handlers' shared objects are set (see api/deps.py)."""
    app.state.resources = resources
    app.state.api_keys = ApiKeyResolver(resources.db, resources.settings)


async def _observe_requests(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Request id in logs and response headers; latency and status metrics per route."""
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
    start = time.perf_counter()
    status = 500
    with structlog.contextvars.bound_contextvars(request_id=request_id):
        try:
            response = await call_next(request)
            status = response.status_code
        finally:
            # Route template ("/jobs/{job_id}"), not the raw path, to keep label cardinality low.
            route = getattr(request.scope.get("route"), "path", None)
            if route is None:
                route = "/metrics" if request.url.path.startswith("/metrics") else "unmatched"
            elapsed = time.perf_counter() - start
            metrics.HTTP_REQUESTS.labels(request.method, route, str(status)).inc()
            metrics.HTTP_LATENCY.labels(request.method, route).observe(elapsed)
    response.headers["X-Request-ID"] = request_id
    return response
