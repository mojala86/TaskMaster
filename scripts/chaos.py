"""Chaos test: kill workers at random under load and prove that no job is lost or run twice.

    LEASE_SECONDS=10 docker compose up -d --scale worker=4   # faster crash detection
    uv run python scripts/chaos.py --jobs 300 --chaos-seconds 60

While jobs run, it repeatedly picks a random worker container and either SIGKILLs it (crash: no
cleanup at all) or restarts it gracefully (SIGTERM: the deploy case), then brings it back. At least
one worker is always left running. Afterwards it checks:

  - every submitted job exists exactly once and ended `succeeded`
  - every job has exactly one succeeded attempt (no job's result was recorded twice)
  - nothing is stuck: no pending stream messages, no due unpublished outbox events

Exit code 1 if any check fails.
"""

import argparse
import asyncio
import random
import subprocess
import time
from collections import Counter

from _compose import (
    create_api_key,
    docker,
    get_json,
    http_session,
    psql,
    redis_cli,
    stats,
    submit_jobs,
    worker_containers,
)


def chaos_step(rng: random.Random, events: Counter[str]) -> None:
    running = worker_containers()
    stopped = [c for c in worker_containers(running_only=False) if c not in running]
    for container in stopped:  # bring back what we killed last round
        docker("start", container)
    if len(running) < 2:
        return
    victim = rng.choice(running)
    if rng.random() < 0.7:
        docker("kill", "--signal", "KILL", victim)
        events["SIGKILL"] += 1
    else:
        # Graceful restart in the background: SIGTERM, wait for in-flight jobs, start again.
        subprocess.Popen(
            ["docker", "restart", "-t", "30", victim],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        events["graceful restart"] += 1


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--jobs", type=int, default=300)
    parser.add_argument("--chaos-seconds", type=float, default=60)
    parser.add_argument("--interval", type=float, nargs=2, default=[2.0, 5.0])
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    rng = random.Random(args.seed)

    key = create_api_key(f"chaos-{int(time.time())}")
    bodies = [
        # max_attempts 10: a lost run (worker killed) counts as an attempt (ADR 0004).
        {
            "type": "sleep",
            "payload": {"seconds": round(rng.uniform(0.5, 3.0), 2)},
            "max_attempts": 10,
        }
        for _ in range(args.jobs)
    ]
    events: Counter[str] = Counter()
    async with http_session(args.api, key) as client:
        start = time.monotonic()
        job_ids = await submit_jobs(client, bodies)
        print(f"submitted {len(job_ids)} jobs ({len(set(job_ids))} distinct)", flush=True)
        chaos_until = time.monotonic() + args.chaos_seconds
        while time.monotonic() < chaos_until:
            counts = await stats(client)
            if counts.get("succeeded", 0) + counts.get("dead", 0) >= args.jobs:
                break
            chaos_step(rng, events)
            print(f"  t={time.monotonic() - start:5.1f}s {dict(events)} {counts}", flush=True)
            await asyncio.sleep(rng.uniform(*args.interval))

        print("chaos over; restoring all workers and waiting for the queue to drain", flush=True)
        for container in worker_containers(running_only=False):
            docker("start", container)
        deadline = time.monotonic() + args.timeout
        # Polling the API is the point here; there is no event to wait on.
        while (counts := await stats(client)).get("succeeded", 0) + counts.get(  # noqa: ASYNC110
            "dead", 0
        ) < args.jobs and time.monotonic() < deadline:
            await asyncio.sleep(1)
        elapsed = time.monotonic() - start

        histories: Counter[str] = Counter()
        attempt_statuses: Counter[str] = Counter()
        bad: list[str] = []
        for job_id in job_ids:
            attempts = await get_json(client, f"/jobs/{job_id}/attempts")
            statuses = [a["status"] for a in attempts]
            attempt_statuses.update(statuses)
            histories[" -> ".join(statuses)] += 1
            if statuses.count("succeeded") != 1:
                bad.append(f"{job_id}: {statuses}")

    pending = redis_cli("XPENDING", "taskmaster:jobs", "workers").splitlines()[0]
    due = psql("SELECT count(*) FROM outbox WHERE published_at IS NULL AND available_at <= now()")

    checks = {
        f"all {args.jobs} jobs exist exactly once": len(set(job_ids)) == args.jobs
        and sum(counts.values()) == args.jobs,
        "all jobs succeeded": counts.get("succeeded", 0) == args.jobs,
        "exactly one succeeded attempt per job": not bad,
        "no pending stream messages": pending == "0",
        "no due unpublished outbox events": due == "0",
    }

    print(f"\nChaos: {dict(events)} over {elapsed:.0f}s")
    print(f"Final job statuses: {dict(counts)}")
    print(f"Attempts by status: {dict(attempt_statuses)}")
    print("Most common attempt histories:")
    for history, n in histories.most_common(6):
        print(f"  {n:4d}  {history}")
    for problem in bad[:10]:
        print(f"  PROBLEM {problem}")
    print()
    for name, ok in checks.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
