"""Helpers shared by the benchmark and chaos scripts (they drive a running docker compose stack)."""

import asyncio
import subprocess
import time
from typing import Any

import aiohttp


def compose(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", "compose", *args], capture_output=True, text=True, check=False
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout


def docker(*args: str) -> str:
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=False).stdout


def create_api_key(name: str) -> str:
    """A fresh key per run: /jobs/stats then counts only this run's jobs."""
    out = compose(
        "exec",
        "-T",
        "api",
        "taskmaster",
        "apikey",
        "create",
        "--name",
        name,
        "--rate-limit",
        "10000000",
    )
    return out.strip().splitlines()[-1]


def worker_containers(running_only: bool = True) -> list[str]:
    args = ["ps", "-q", "--filter", "label=com.docker.compose.service=worker"]
    if not running_only:
        args.insert(1, "-a")
    return docker(*args).split()


def scale_workers(n: int) -> None:
    compose("up", "-d", "--no-recreate", "--scale", f"worker={n}", "worker")
    deadline = time.monotonic() + 60
    while len(worker_containers()) != n:
        if time.monotonic() > deadline:
            raise RuntimeError(f"workers did not scale to {n}")
        time.sleep(0.5)
    time.sleep(3)  # let them connect and join the consumer group


def redis_cli(*args: str) -> str:
    return compose("exec", "-T", "redis", "redis-cli", *args).strip()


def psql(sql: str) -> str:
    return compose("exec", "-T", "postgres", "psql", "-U", "taskmaster", "-tAc", sql).strip()


def http_session(api: str, key: str, concurrency: int = 32) -> aiohttp.ClientSession:
    """aiohttp rather than httpx: on Windows httpx's async client was the bottleneck at ~50 req/s,
    which made the benchmark measure the client instead of the platform."""
    return aiohttp.ClientSession(
        api,
        headers={"X-API-Key": key},
        connector=aiohttp.TCPConnector(limit=concurrency),
        timeout=aiohttp.ClientTimeout(total=30),
    )


async def submit_jobs(
    session: aiohttp.ClientSession, bodies: list[dict[str, Any]], concurrency: int = 32
) -> list[str]:
    """Submit jobs with bounded concurrency; Idempotency-Key makes client retries safe."""
    sem = asyncio.Semaphore(concurrency)

    async def one(i: int, body: dict[str, Any]) -> str:
        async with sem:
            for attempt in range(5):
                try:
                    async with session.post(
                        "/jobs", json=body, headers={"Idempotency-Key": f"job-{i}"}
                    ) as r:
                        if r.status in (200, 201):
                            job_id: str = (await r.json())["id"]
                            return job_id
                except aiohttp.ClientError:
                    pass
                await asyncio.sleep(0.2 * (attempt + 1))
            raise RuntimeError(f"could not submit job {i}")

    return await asyncio.gather(*(one(i, b) for i, b in enumerate(bodies)))


async def get_json(session: aiohttp.ClientSession, path: str) -> Any:
    async with session.get(path) as r:
        r.raise_for_status()
        return await r.json()


async def stats(session: aiohttp.ClientSession) -> dict[str, int]:
    result: dict[str, int] = await get_json(session, "/jobs/stats")
    return result
