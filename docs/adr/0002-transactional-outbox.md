# ADR 0002: Transactional outbox between Postgres and Redis Streams

## Context

Postgres holds job state; Redis Streams carries work to the workers. Creating a job therefore
touches two systems:

```python
await db.insert(job)  # 1. commit to Postgres
await redis.xadd(job.id)  # 2. publish to the stream; this can fail
```

If the process dies between 1 and 2, the job stays `queued` forever and no worker ever sees it.
Reversing the order creates "ghost" messages for jobs that were never committed. This is the
dual-write problem, and it applies equally when a worker finishes a job and something downstream
(e.g. a webhook) must be told.

## Decision

Every state change writes an `outbox` row **in the same Postgres transaction**. Nothing writes to
Redis directly except a separate relay process, which:

1. selects due, unpublished rows with `FOR UPDATE SKIP LOCKED` (so several relays can run in
   parallel without handling the same rows),
2. `XADD`s them to Redis in one pipeline,
3. stamps `published_at` (or records `attempts`/`last_error` on failure) and commits.

A statement-level trigger issues `NOTIFY outbox_new` so the relay wakes up as soon as a
transaction commits; a short poll interval remains as a fallback if a notification is missed.

Retries reuse the same mechanism: a failed attempt writes a `job.retry_scheduled` row whose
`available_at` is in the future, so backoff needs no separate scheduler.

Event routing:

| Event                                 | Destination stream  |
|---------------------------------------|---------------------|
| `job.created`, `job.retry_scheduled`  | `taskmaster:jobs`   |
| `job.succeeded`, `job.dead`           | `taskmaster:events` |

## Consequences

- A job and its dispatch event commit atomically: no lost jobs and no ghost messages.
- The API does not depend on Redis at all; it keeps accepting jobs during a Redis outage and they
  are dispatched when Redis returns (covered by a test and a manual compose check).
- Delivery is at-least-once; consumers must be idempotent.
- One more process to run (the relay), plus a little dispatch latency (milliseconds with
  `NOTIFY`).
- Published rows accumulate; they need periodic cleanup (planned: delete after 7 days).

## Alternatives considered

- **Postgres-only queue (`SELECT … FOR UPDATE SKIP LOCKED` on `jobs`)**: no dual write at all and
  one fewer dependency. Rejected because we want a separate broker with consumer groups and to show
  the pattern that keeps a database and a broker consistent.
- **Change data capture (Debezium reading the WAL)**: no polling and no outbox writes in
  application code, but it needs Kafka Connect and is far heavier than the problem.
