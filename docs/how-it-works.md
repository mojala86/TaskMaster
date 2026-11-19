# How TaskMaster works

A plain-language tour of what TaskMaster does, how it does it, and why it is built this way.
For the precise details, see the design records in [`docs/adr/`](adr/).

## What it does

TaskMaster runs work in the background, so an app doesn't have to make its user wait.

**Example.** A user uploads a 100 MB sales CSV and wants totals per region. That takes a few
seconds, and if the web server did it while the user waited, the request might time out. With
TaskMaster:

1. The app says "please do this job" and gets back a job ID immediately.
2. A worker somewhere does the work.
3. The app asks "is job 42 done?" and downloads the result when it is.

Think of a restaurant. The waiter (the **API**) takes your order and gives you a ticket number.
The order goes on the kitchen rail (the **queue**). Cooks (the **workers**) pick up orders and
cook them. You check your ticket number to see whether your food is ready.

The hard part is not cooking. It's making sure **no order is ever lost or cooked twice**, even
when a cook collapses halfway through a dish. Most of TaskMaster is about that.

## A job's journey, step by step

Say you want to sum sales per region from `sales.csv`. (Responses below are shortened.)

**1. Upload the file.**

```
POST /uploads {"filename": "sales.csv", "size": 104857600}
→ {"key": "uploads/7f3a…/sales.csv", "upload_url": "http://localhost:9000/…signed…"}
```

You then send the file straight to file storage using that `upload_url`. The API never touches
the 100 MB. The link only accepts a file of exactly the size you declared, so nobody can use it
to fill the bucket with 50 GB.

**2. Submit the job.**

```
POST /jobs {"type": "csv.aggregate",
            "payload": {"file": "uploads/7f3a…/sales.csv", "group_by": ["region"],
                        "aggregations": [{"op": "sum", "column": "amount"}]}}
→ 201 {"id": "9c1e…", "status": "queued"}
```

The API checks the request right away. A misspelled job type or a missing file gets an error
now, not ten minutes later.

**3. A worker picks it up.** The status changes to `running`, and `progress` climbs while it
works (0.25, 0.5, …).

**4. Get the result.**

```
GET /jobs/9c1e…           → {"status": "succeeded", "result": {"rows": 3000000, "groups": 5}}
GET /jobs/9c1e…/outputs   → [{"name": "aggregate.csv", "url": "…download link…"}]
```

## The pieces, and why each one exists

| Piece | In the restaurant | Why it's there |
|---|---|---|
| **API** (FastAPI) | The waiter | Takes orders and answers "is it ready?" Fast, because it never does the actual work. |
| **PostgreSQL** | The order book | The single source of truth: every job, its status, and every attempt. |
| **Redis Streams** | The kitchen rail | A fast way to hand out work to many workers at once. |
| **Relay** | The runner who carries tickets from the order book to the rail | Solves the "two places" problem described below. |
| **Workers** | The cooks | Run the jobs. Add more workers to get more throughput. |
| **Object storage** (S3 / RustFS) | The pantry and the pickup shelf | Holds the large input and output files. |

## The hard problems and how each is solved

### 1. "Saved in one place but not the other" → transactional outbox

Creating a job means writing to Postgres (the order book) *and* Redis (the rail). Suppose the
server crashes between the two writes: the job is saved in Postgres, but no worker ever hears
about it. It stays `queued` forever.

**The fix:** the API writes **only to Postgres**. In one transaction it saves the job *and* a
note saying "this job needs to go on the rail" (the **outbox** table). One transaction means
either both rows are saved or neither is. A separate **relay** process reads those notes and
puts them on the Redis rail.

**Nice side effect:** if Redis is down, the API keeps accepting jobs. The notes pile up in
Postgres, and the relay delivers them when Redis comes back. This was tested by stopping Redis
and submitting a job: it got a `201` and ran once Redis returned.

Details: [ADR 0002](adr/0002-transactional-outbox.md).

### 2. "The same order arrives twice" → claim with a condition

The relay can send the same note twice. For example, it sends it to Redis, then crashes before
writing "sent" back to Postgres, so after restarting it sends it again. That's unavoidable, and
it's called *at-least-once delivery*.

**The fix:** a worker doesn't just start a job. It **claims** it:

```sql
UPDATE jobs SET status = 'running' WHERE id = 42 AND status = 'queued'
```

Only one worker can win this. The second copy of the message finds the job already `running`,
changes nothing, and is simply thrown away. Duplicates become harmless.

Details: [ADR 0003](adr/0003-at-least-once-delivery.md).

### 3. "A cook collapses mid-dish" → leases and heartbeats

When a worker claims a job, it gets a **lease**: "this job is mine for the next 30 seconds."
While it works, it renews the lease every 5 seconds (the **heartbeat**), like saying
"still on it!"

If the worker is killed (a crash, out of memory, someone pulls the power cord), the heartbeats
stop. After 30 seconds the lease expires. Another worker's **sweeper** notices, marks that
attempt `lost`, and puts the job back in the queue.

**Example from the chaos test:** workers were killed at random while 400 jobs ran. Every one of
the 400 jobs still succeeded exactly once. One job's attempt history looked like this:

```
lost -> succeeded
```

The first worker died; another worker finished the job.

What if the "dead" worker was only *slow* and comes back later? Its result is rejected, because
every write it makes checks "do I still own this job?", and it no longer does.

Details: [ADR 0004](adr/0004-leases-heartbeats-and-recovery.md).

### 4. "Restaurant closing for a deploy" → graceful shutdown

When a worker is told to stop, it takes no new jobs and gives running jobs 20 seconds to finish.
Anything still running after that is **released** back to the queue, and a release doesn't
count as a failed attempt.

### 5. "A dish keeps failing" → retries, backoff, dead-letter queue

If a job fails, it's retried after about 2s, then 4s, then 8s, and so on (**exponential
backoff**, with a little randomness so retries don't all land at once). The growing wait means a
struggling service isn't hammered with retries.

After the last allowed attempt, the job becomes `dead` and lands in the **dead-letter queue**:
`GET /jobs?status=dead`. Once you've fixed the cause, `POST /jobs/{id}/retry` sends it back
through.

Some errors are pointless to retry. For example, if a CSV is missing the column you asked for,
the job is marked `dead` immediately, because retrying would fail the same way every time.

### 6. "The customer orders twice by accident" → idempotency keys

Suppose your app sends `POST /jobs`, the network drops before the answer comes back, and the app
retries. Did you just create two jobs?

Not if the request includes `Idempotency-Key: order-42`. The second request with the same key
gets back **the original job** instead of a new one.

### 7. Big files → presigned URLs and streaming

- **Uploads and downloads go straight to file storage**, using temporary signed links. The API
  only hands out the links, so 1 GB files don't slow it down.
- **The CSV is read row by row** as it downloads. Memory use depends on how many *groups* there
  are (for example 5 regions), not on the file size. A 103 MB, 3-million-row file took about
  4 seconds.

Details: [ADR 0005](adr/0005-files-and-cpu-work.md).

### 8. Heavy computation → its own process, which can be killed

CSV crunching and image resizing run in a **separate process** for each job. Two reasons:

- It doesn't freeze the worker, so the heartbeats keep going.
- If you **cancel** the job, that process is actually killed. A shared pool of processes can't
  stop a task once it has started.

### 9. One client hogging everything → API keys and rate limits

Each client gets an API key, and with it:

- **Its own jobs only.** Someone else's job just returns "not found".
- **Its own speed limit**, 600 requests per minute by default. If you go over, you get a `429`
  with `Retry-After: 12`, which means "try again in 12 seconds".

The limit is stored in Redis, so every API server shares the same count. If Redis is down,
requests are allowed rather than refused.

Details: [ADR 0006](adr/0006-api-keys-and-rate-limiting.md).

### 10. Leftover files → janitor

Failed attempts leave partial files behind, and uploads get abandoned. Once an hour, a
**janitor** deletes:

- files from failed attempts, after a grace hour
- uploads older than 7 days that no unfinished job needs

It checks the database first, so it never deletes a finished job's results.

## Seeing what's going on

- **Grafana dashboard** (`docker compose --profile monitoring up`): jobs per second, error rate,
  how long jobs wait and run, queue depth, crashed workers recovered, API speed, and rate-limit
  hits.
- **Logs** are one JSON object per line. Every line logged during a job automatically includes
  that job's ID:

  ```json
  {"event": "Job failed; will retry", "job_id": "9c1e…", "attempt": 1, "retry_in": 2.16}
  ```

## Proof that it works

- **81 automated tests** against a real Postgres, Redis and file store. They cover each failure
  above: crashes, duplicate messages, Redis outages, cancellations and rate limits.
- **Chaos test:** 400 jobs while workers were killed at random. All 400 succeeded, each exactly
  once.
- **Scaling:** 1 worker handled 18 jobs/s and 8 workers handled 143 jobs/s, which is 7.9×
  faster with 8× the workers.

## Why not just use Celery?

Celery is a popular ready-made library that would have given a working system in a day. But all
the interesting parts (claiming, leases, the outbox, retries) would be hidden inside it. This is
a portfolio project, so the point is to show those problems solved and explained. See
[ADR 0001](adr/0001-own-queue-not-celery.md).
