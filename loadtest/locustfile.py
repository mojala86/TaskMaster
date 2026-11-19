"""API load test.

    docker compose exec api taskmaster apikey create --name load --rate-limit 1000000
    TASKMASTER_API_KEY=<key> uv run locust -f loadtest/locustfile.py --host http://localhost:8000

Headless: add `--headless -u 50 -r 10 -t 60s`. A realistic client mix: mostly submitting jobs and
polling their status, sometimes listing.
"""

import os
import random
from typing import Any

from locust import FastHttpUser, between, task


class TaskMasterClient(FastHttpUser):
    wait_time = between(0.05, 0.25)

    def on_start(self) -> None:
        key = os.environ.get("TASKMASTER_API_KEY")
        self.headers: dict[str, str] = {"X-API-Key": key} if key else {}
        self.job_ids: list[str] = []

    @task(5)
    def submit(self) -> None:
        body: dict[str, Any] = {"type": "echo", "payload": {"data": {"n": random.randint(0, 999)}}}
        with self.client.post(
            "/jobs", json=body, headers=self.headers, name="POST /jobs", catch_response=True
        ) as response:
            if response.status_code == 201:
                self.job_ids = [*self.job_ids[-49:], response.json()["id"]]
                response.success()
            else:
                response.failure(f"{response.status_code}: {response.text[:200]}")

    @task(4)
    def poll_status(self) -> None:
        if self.job_ids:
            self.client.get(
                f"/jobs/{random.choice(self.job_ids)}", headers=self.headers, name="GET /jobs/{id}"
            )

    @task(1)
    def list_jobs(self) -> None:
        self.client.get("/jobs?limit=20", headers=self.headers, name="GET /jobs")
