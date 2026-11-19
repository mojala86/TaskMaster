"""Phase 5: metrics recorded by the API, workers and relays."""

import httpx
import pytest
from prometheus_client import REGISTRY

from taskmaster.config import Settings
from taskmaster.relay import Relay
from taskmaster.worker import Worker
from tests.integration.helpers import create_job

pytestmark = pytest.mark.integration


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_api_records_requests_by_route_template(client: httpx.AsyncClient) -> None:
    job = await create_job(client, type="echo")
    before = sample(
        "taskmaster_http_requests_total", method="GET", route="/jobs/{job_id}", status="200"
    )

    await client.get(f"/jobs/{job['id']}")

    after = sample(
        "taskmaster_http_requests_total", method="GET", route="/jobs/{job_id}", status="200"
    )
    assert after == before + 1
    response = await client.get("/metrics/")
    assert 'taskmaster_jobs_submitted_total{type="echo"}' in response.text


async def test_worker_records_outcomes_durations_and_queue_wait(
    client: httpx.AsyncClient, relay: Relay, worker: Worker
) -> None:
    tracked = {
        "failed_runs": ("taskmaster_job_runs_total", {"type": "flaky", "outcome": "failed"}),
        "dead_runs": ("taskmaster_job_runs_total", {"type": "flaky", "outcome": "dead"}),
        "queue_waits": ("taskmaster_job_queue_wait_seconds_count", {"type": "flaky"}),
        "durations": (
            "taskmaster_job_duration_seconds_count",
            {"type": "flaky", "outcome": "dead"},
        ),
    }

    def read() -> dict[str, float]:
        return {k: sample(name, **labels) for k, (name, labels) in tracked.items()}

    before = read()

    await create_job(client, type="flaky", payload={"fail_rate": 1}, max_attempts=2)
    for _ in range(2):
        await relay.publish_batch()
        await worker.poll_once("c1")

    delta = {k: v - before[k] for k, v in read().items()}
    # Two attempts: one failed (retried), one dead. Queue wait is only measured for attempt 1.
    assert delta == {"failed_runs": 1, "dead_runs": 1, "queue_waits": 1, "durations": 1}
    assert sample("taskmaster_jobs_in_progress", type="flaky") == 0


async def test_relay_samples_queue_depth(
    client: httpx.AsyncClient, relay: Relay, settings: Settings
) -> None:
    for _ in range(3):
        await create_job(client, type="echo")

    await relay.sample_queue_stats()
    assert sample("taskmaster_jobs", status="queued") == 3
    assert sample("taskmaster_outbox_due") == 3

    await relay.publish_batch()
    await relay.sample_queue_stats()
    assert sample("taskmaster_outbox_due") == 0
    assert sample("taskmaster_stream_length", stream=settings.jobs_stream) == 3
    assert sample("taskmaster_outbox_published_total", event_type="job.created") >= 3
