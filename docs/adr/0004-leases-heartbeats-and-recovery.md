# ADR 0004: Leases, heartbeats and two-layer recovery

## Context

Workers die: OOM kills, `kill -9`, node failures, network partitions. A job taken by a dead worker
must not stay `running` forever, and a worker that is merely slow or cut off must not be able to
overwrite the result of whoever took over. There are two places where work can get stuck:

1. **The message**: a worker read it from the stream (it is in that consumer's pending list) but
   died before claiming the job in Postgres. The job is still `queued`, but no new message will
   ever be sent for it.
2. **The job**: a worker claimed it (`running`) and died while running the handler.

## Decision

### Layer 1: the message lives only until the claim

A worker acks (and deletes) the stream message right after the claim transaction commits. Before
that point, recovery is Redis's job: each consumer periodically runs `XAUTOCLAIM` for messages idle
longer than `reclaim_idle_ms` (30s) and handles them like new ones. Because claiming is idempotent
(ADR 0003), reclaiming a message whose job was in fact handled is harmless.

### Layer 2: after the claim, the lease is the source of truth

The claim sets `lease_until = now() + lease_seconds` (30s). While the handler runs, a heartbeat
renews it every `heartbeat_interval` (5s) with:

```sql
UPDATE jobs SET lease_until = now() + :lease
WHERE id = :id AND status = 'running' AND worker_id = :me
RETURNING cancel_requested;
```

- No row: the job was taken away (the lease expired and was swept). The worker cancels the handler
  and records nothing. All of its final updates carry the same ownership condition, so even a
  worker that never notices cannot overwrite anything.
- `cancel_requested`: the worker cancels the handler and records `cancelled`. The heartbeat
  doubles as the cancellation channel, so no extra pub/sub is needed.

Every worker runs a maintenance loop (every 5s) that sweeps
`status = 'running' AND lease_until < now()` (partial index, `FOR UPDATE SKIP LOCKED`). Each
expired job gets its attempt marked `lost` and moves to `retrying` (with backoff), or to `dead` if
it is out of attempts, through the outbox like any other transition.

### Attempt accounting

| Outcome                     | Attempt status | Counts against `max_attempts`? |
|-----------------------------|----------------|--------------------------------|
| Handler raised / timed out  | `failed`       | yes                            |
| Lease expired (worker died) | `lost`         | yes: a job that kills workers must end up dead, not loop |
| Graceful shutdown timeout   | `released`     | no: a deploy should not burn a customer's retries |
| Cancel requested            | `cancelled`    | (terminal)                     |

On SIGTERM a worker stops reading, gives running jobs `worker_shutdown_timeout` (20s, below the
container's 30s grace period) to finish, then cancels the rest and releases them back to `queued`
with a `job.released` dispatch event.

### Safety net for lost messages

If Redis loses stream data (restart without persistence, failover), a `queued` job might have no
message anywhere. The maintenance loop re-dispatches `queued`/`retrying` jobs that have had no
change for `stale_dispatch_seconds` (10 min) and have no unpublished outbox event. A duplicate
message is harmless, so being a bit eager here is fine.

## Consequences

- A crashed worker's job resumes within about `lease_seconds + maintenance_interval` (~35s by
  default). A shorter lease means faster recovery but more heartbeat writes and less tolerance for
  pauses (GC, slow DB); 30s/5s gives six missed heartbeats of margin.
- Heartbeats cost one indexed UPDATE per running job every 5s. That is negligible at hundreds of
  concurrent jobs. At very high concurrency, heartbeats can be batched per worker.
- Cancellation and lease loss stop a handler by cancelling its asyncio task. That works for
  cooperative async code. A handler stuck in blocking CPU work won't stop until it yields; CPU-heavy
  tasks will move to a process pool later, where they can be terminated.
- External side effects can still run twice when a job is retried after a crash. Handlers use the
  job id as the idempotency key towards external systems (ADR 0003).
