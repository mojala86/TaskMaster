import structlog
from fastapi import APIRouter, Response
from sqlalchemy import text

from taskmaster.api.deps import SessionDep

log = structlog.get_logger("taskmaster.api")
router = APIRouter(tags=["health"])


@router.get("/health", responses={503: {"description": "Postgres is unreachable"}})
async def health(session: SessionDep, response: Response) -> dict[str, str]:
    # Deliberately no Redis check: thanks to the outbox the API keeps accepting jobs while
    # Redis is down, and they are dispatched once it is back.
    try:
        await session.execute(text("SELECT 1"))
    except Exception:
        log.warning("Health check failed", exc_info=True)
        response.status_code = 503
        return {"status": "unavailable"}
    return {"status": "ok"}
