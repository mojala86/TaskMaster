# ADR 0003: At-least-once delivery with idempotent workers, not exactly-once

## Context

With a transactional outbox (ADR 0002), the relay can crash after `XADD` but before committing
`published_at`. On restart it publishes the same events again. Redis Streams consumer groups add
another redelivery path: a message that is read but never acknowledged can be claimed again.
Exactly-once delivery across Postgres and Redis would need a distributed transaction, which
neither system offers here.

## Decision

Accept at-least-once delivery and make job execution idempotent at the claim step. A worker claims
a job with a conditional state transition:

```sql
UPDATE jobs
SET status = 'running', attempts = attempts + 1, worker_id = :w,
    lease_until = now() + interval '30 seconds'
WHERE id = :job_id AND status IN ('queued', 'retrying')
RETURNING ...;
```

If no row is returned, the job is already running elsewhere or finished. The worker acks the
message and does nothing. Completion updates are also conditional on
`status = 'running' AND worker_id = :w`, so a worker that has lost ownership cannot overwrite the
result.

## Consequences

- Duplicate messages are harmless and cheap (one UPDATE that matches nothing). This is covered by
  `test_duplicate_delivery_runs_job_once`.
- The guarantee is "each job's state transitions happen once". A handler's **external side
  effects** (calling a third-party API, sending an email) can still repeat if a worker dies
  mid-job and the job is retried. Handlers with side effects should use the job id as an
  idempotency key with the external system.
- Crash recovery builds on this: lease expiry plus heartbeats let another worker take over a job
  from a dead worker, and the same conditional updates keep that safe (ADR 0004).
- The same reasoning applies at the API edge: a client retrying `POST /jobs` after a timeout sends
  an `Idempotency-Key`, and the key's unique index turns the retry into a replay of the original
  job instead of a second one.
