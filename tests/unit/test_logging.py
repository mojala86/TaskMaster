import json
import logging
from collections.abc import Iterator

import pytest
import structlog

from taskmaster.log import configure_logging


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    yield
    configure_logging("INFO", "console")
    structlog.contextvars.clear_contextvars()


def test_json_logs_carry_bound_context(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", "json")

    with structlog.contextvars.bound_contextvars(job_id="abc", attempt=2):
        logging.getLogger("taskmaster.worker").info("Job %s succeeded", "abc")

    line = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert line["event"] == "Job abc succeeded"
    assert line["level"] == "info"
    assert line["logger"] == "taskmaster.worker"
    assert (line["job_id"], line["attempt"]) == ("abc", 2)
    assert "timestamp" in line


def test_json_logs_include_exceptions(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", "json")

    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("taskmaster").exception("failed")

    line = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert "ValueError: boom" in line["exception"]


def test_structlog_events_carry_fields(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", "json")

    structlog.get_logger("taskmaster.worker").warning("Job failed; will retry", retry_in=2.0)
    structlog.get_logger("taskmaster.tasks.x").info("attempt %d of %d", 2, 3)  # %-style works too

    first, second = (json.loads(line) for line in capsys.readouterr().err.strip().splitlines())
    assert (first["event"], first["retry_in"], first["level"]) == (
        "Job failed; will retry",
        2.0,
        "warning",
    )
    assert second["event"] == "attempt 2 of 3"


def test_access_log_skips_health_and_metrics(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", "json")
    access = logging.getLogger("uvicorn.access")

    for path in ("/health", "/metrics/", "/jobs"):
        access.info('%s - "%s %s HTTP/%s" %d', "1.2.3.4", "GET", path, "1.1", 200)

    lines = capsys.readouterr().err.strip().splitlines()
    assert len(lines) == 1
    assert "/jobs" in json.loads(lines[0])["event"]
