"""Structured logging.

All logs, ours and third-party (uvicorn, alembic...), go through structlog's ProcessorFormatter,
so every line has the same shape: JSON in containers (`log_format=json`), readable key=value on a
terminal (`console`). Context bound with `structlog.contextvars` (job_id, attempt, request_id...)
is attached to every line logged while it is bound, including by plain `logging` loggers.

Our modules log events with fields, not prose with interpolated values:

    log.warning("Job failed; will retry", error=error, retry_in=2.0)
"""

import logging
from typing import Any

import structlog


def configure_logging(level: str, fmt: str = "console") -> None:
    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    final: list[Any] = [structlog.stdlib.ProcessorFormatter.remove_processors_meta]
    if fmt == "json":
        final.append(structlog.processors.format_exc_info)  # ConsoleRenderer does its own
    final.append(renderer)
    formatter = structlog.stdlib.ProcessorFormatter(foreign_pre_chain=shared, processors=final)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())
    # Uvicorn installs its own handlers unless log_config=None; make sure they propagate here.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).handlers = []
        logging.getLogger(name).propagate = True
    access = logging.getLogger("uvicorn.access")
    access.filters = [f for f in access.filters if not isinstance(f, DropProbeRequests)]
    access.addFilter(DropProbeRequests())

    structlog.configure(
        processors=[
            *shared,
            # Allows `log.info("attempt %d", n)` as well as `log.info("event", n=n)`.
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


class DropProbeRequests(logging.Filter):
    """Drop access-log lines for health checks and metric scrapes (one every few seconds)."""

    PATHS = ("/health", "/metrics")

    def filter(self, record: logging.LogRecord) -> bool:
        # uvicorn.access args: (client_addr, method, full_path, http_version, status_code)
        args = record.args
        if isinstance(args, tuple) and len(args) >= 3 and isinstance(args[2], str):
            return not args[2].startswith(self.PATHS)
        return True
