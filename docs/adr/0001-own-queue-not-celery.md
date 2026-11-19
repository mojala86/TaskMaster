# ADR 0001: Build the job queue ourselves instead of using Celery

## Context

TaskMaster needs to run background jobs reliably across many workers. Mature libraries exist
(Celery, RQ, Dramatiq, arq). They would give a working system quickly, but they hide the problems
we most want to solve and explain:

- What happens when a worker dies in the middle of a job (leases, heartbeats, reclaiming)?
- How do we make sure a job's effects are not applied twice (idempotency)?
- How are failures retried (exponential backoff with jitter) and where do permanent failures go
  (dead-letter)?
- How does a job's state stay consistent between the database and the broker?

## Decision

Implement the queue on top of Postgres (source of truth) and Redis Streams (transport), with our
own worker loop, retry policy and task registry.

## Consequences

- More code to own and test, especially around failure modes. We accept this because those
  mechanisms are the point of the project.
- Every reliability guarantee is explicit and visible in the code, and covered by tests.
- Job state is queryable with plain SQL (`jobs`, `job_attempts`).
