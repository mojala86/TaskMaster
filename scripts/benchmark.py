"""End-to-end throughput as the number of workers grows.

Runs against the docker compose stack: for each worker count it scales the `worker` service,
submits N jobs and measures the time from the first submission until the last job has finished.

    uv run python scripts/benchmark.py --workers 1 2 4 8 --jobs 400 --seconds 0.2
    uv run python scripts/benchmark.py --workers 1 2 4 8 --jobs 2000 --seconds 0   # echo jobs

--seconds > 0 submits `sleep` jobs (work that scales with workers); 0 submits `echo` jobs, which
measures the platform's own per-job overhead (claim, outbox, relay, result).

--prefill queues all jobs with no workers running, then starts the workers, and computes the rate
from the jobs' own started_at/finished_at timestamps. That measures how fast the workers drain a
backlog, independent of how fast one API process can accept submissions.
"""

import argparse
import asyncio
import json
import time
from pathlib import Path

from _compose import (
    compose,
    create_api_key,
    http_session,
    psql,
    scale_workers,
    stats,
    submit_jobs,
)

TERMINAL = ("succeeded", "dead", "cancelled")


async def run_once(
    api: str, workers: int, jobs: int, seconds: float, prefill: bool
) -> dict[str, float]:
    scale_workers(0 if prefill else workers)
    name = f"bench-{workers}w-{int(time.time())}"
    key = create_api_key(name)
    body = (
        {"type": "sleep", "payload": {"seconds": seconds}}
        if seconds > 0
        else {"type": "echo", "payload": {"data": {}}}
    )

    async with http_session(api, key) as session:
        start = time.perf_counter()
        await submit_jobs(session, [body] * jobs)
        submitted = time.perf_counter() - start
        if prefill:
            scale_workers(workers)
        while True:
            counts = await stats(session)
            if sum(counts.get(s, 0) for s in TERMINAL) >= jobs:
                break
            await asyncio.sleep(0.2)
    elapsed = time.perf_counter() - start
    if prefill:  # first start to last finish, by the database's clock
        elapsed = float(
            psql(
                "SELECT extract(epoch FROM max(j.finished_at) - min(j.started_at)) FROM jobs j "
                f"JOIN api_keys k ON k.id = j.api_key_id WHERE k.name = '{name}'"
            )
        )

    result = {
        "workers": workers,
        "jobs": jobs,
        "seconds": round(elapsed, 2),
        "jobs_per_second": round(jobs / elapsed, 1),
        "submit_per_second": round(jobs / submitted, 1),
        "succeeded": counts.get("succeeded", 0),
    }
    print(json.dumps(result), flush=True)
    return result


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--jobs", type=int, default=400)
    parser.add_argument("--seconds", type=float, default=0.2, help="sleep per job; 0 = echo")
    parser.add_argument("--prefill", action="store_true", help="measure backlog drain rate")
    parser.add_argument("--out", type=Path, help="write results as JSON")
    args = parser.parse_args()

    concurrency = compose("exec", "-T", "worker", "printenv", "TASKMASTER_WORKER_CONCURRENCY")
    print(f"worker concurrency: {concurrency.strip()} jobs per worker", flush=True)
    results = [
        await run_once(args.api, n, args.jobs, args.seconds, args.prefill) for n in args.workers
    ]
    scale_workers(2)

    print("\n| Workers | Jobs/s | Speed-up |\n|---|---|---|")
    base = results[0]["jobs_per_second"]
    for r in results:
        print(f"| {r['workers']} | {r['jobs_per_second']} | {r['jobs_per_second'] / base:.1f}x |")
    if args.out:
        args.out.write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
