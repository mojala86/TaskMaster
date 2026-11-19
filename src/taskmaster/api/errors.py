"""Exception -> HTTP response mapping, in one place.

Code below the routes raises domain errors (JobNotFound, JobConflict, …) and doesn't know about
HTTP. A dependency we can't reach (Postgres, object storage) is a temporary condition, not a bug:
503 + Retry-After tells load balancers and clients to try again, where a 500 says "this request
is broken".
"""

import structlog
from botocore.exceptions import ConnectionError as StorageConnectionError
from botocore.exceptions import HTTPClientError as StorageHTTPError
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import exc as sa_exc

from taskmaster.jobs.errors import (
    IdempotencyConflict,
    InputFileMissing,
    InputFileTooLarge,
    JobConflict,
    JobError,
    JobNotFound,
)

log = structlog.get_logger("taskmaster.api")

STATUS_CODES: dict[type[JobError], int] = {
    JobNotFound: 404,
    JobConflict: 409,
    InputFileTooLarge: 413,
    IdempotencyConflict: 422,
    InputFileMissing: 422,
}

UNAVAILABLE: tuple[type[Exception], ...] = (
    sa_exc.OperationalError,  # Postgres down, connection refused/reset, connect timeout
    sa_exc.InterfaceError,
    sa_exc.TimeoutError,  # connection pool exhausted
    StorageConnectionError,  # object storage unreachable
    StorageHTTPError,
)


def install_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(JobError, _job_error)
    for exc_type in UNAVAILABLE:
        app.add_exception_handler(exc_type, _service_unavailable)


async def _job_error(request: Request, exc: Exception) -> Response:
    status = next((code for t, code in STATUS_CODES.items() if isinstance(exc, t)), 400)
    return JSONResponse({"detail": str(exc)}, status_code=status)


async def _service_unavailable(request: Request, exc: Exception) -> Response:
    log.warning(
        "Dependency unavailable", error=f"{type(exc).__name__}: {exc}", path=request.url.path
    )
    return JSONResponse(
        {"detail": "Service temporarily unavailable, please retry"},
        status_code=503,
        headers={"Retry-After": "5"},
    )
