"""HTTP routes, one module per resource."""

from fastapi import Depends, FastAPI

from taskmaster.api.deps import enforce_rate_limit
from taskmaster.api.routes import files, health, jobs, tasks


def include_routes(app: FastAPI) -> None:
    # Unauthenticated, not rate limited.
    app.include_router(health.router)
    # Everything else: authenticated (if required) and rate limited per client.
    for module in (tasks, jobs, files):
        app.include_router(module.router, dependencies=[Depends(enforce_rate_limit)])
