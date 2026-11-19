# ADR 0005: Files through object storage, CPU work in a process per job

## Context

Jobs that work on files: aggregating large CSVs and resizing images. That raises
three questions:

1. How do potentially large files get in and out without passing through the API?
2. How does a job process a file larger than the worker's memory?
3. Where does CPU-heavy work run? It would block the worker's event loop, which must keep sending
   heartbeats (ADR 0004).

## Decision

### Object storage with presigned URLs

Files live in an S3-compatible store. The API only signs URLs:

```
POST /uploads {filename, size} -> {key, upload_url}   client PUTs exactly `size` bytes to storage
POST /jobs {file: key}     ->  API HEADs the key: 422 if the upload is missing
GET  /jobs/{id}/outputs    ->  presigned GET URLs for the result files
```

- Input keys must match `uploads/<uuid>/<name>` (the `FileRef` type). A job can't be pointed at
  another job's results or any other object.
- Outputs are written under `results/<job id>/<attempt id>/`. The attempt id is unique per run, so
  a worker that lost its lease can never overwrite the winning attempt's files.
- Output rows (`job_outputs`) are inserted in the same transaction that marks the job succeeded.
  Files of failed or lost attempts stay unreferenced.
- Two endpoints are configured, because a presigned URL is signed for one host. Services use the
  internal one (`http://s3:9000`); URLs given to clients use the public one
  (`http://localhost:9000`).


### Streaming, not loading

- `file.checksum` streams the object in 1 MiB chunks inside the worker (I/O bound, async).
- `csv.aggregate` reads the HTTP response through `csv.reader` row by row. Memory grows with the
  number of distinct groups, not with the file size. Quoted fields with embedded newlines work
  because the parser sees one continuous stream instead of pre-split lines.
- Images are the exception: Pillow decodes the whole image anyway, so the input is read into memory
  with a 100 MB cap, and Pillow's decompression-bomb check stays on.

### CPU work: a dedicated subprocess per job

`ctx.run_cpu(fn, *args)` spawns a fresh process, passes it a presigned URL for the input (the
child never needs credentials) and a destination path, and relays `progress(fraction, message)`
messages over a pipe. Cancelling the awaiting task (job cancelled, lease lost, shutdown timeout)
terminates the process, and kills it if SIGTERM is ignored. The heartbeat keeps running in the
parent's event loop the whole time.

Progress from any handler is kept in memory and written with the next heartbeat (every 5s), so
reporting it costs no extra database writes.

## Consequences

- The API stays small and fast whatever the file size. Upload bandwidth goes to the storage
  service, which can scale or move to a CDN on its own.
- A job's outputs are atomic with its success; clients never see partial results.
- Objects from failed attempts and old uploads would accumulate. A storage janitor removes them
  (see below).
- Each CPU job pays a process start (spawn: roughly 0.1-0.3s) and imports only the light
  `taskmaster.processing` package. That is negligible for work that deserves a process.

## Cleanup: a database-aware janitor, not a lifecycle rule

A bucket lifecycle rule only sees prefixes and ages. Expiring `results/` would also delete the
outputs of succeeded jobs that `job_outputs` still lists, leaving broken download links, and it
can't tell a failed attempt's files from the successful one's. Instead, a janitor
(`taskmaster/janitor.py`) asks Postgres what is still referenced:

| Objects | Deleted when |
|---|---|
| `results/<job>/<attempt>/…` not in `job_outputs` | the attempt ended (failed, lost, cancelled, released) more than `orphan_grace_hours` (1h) ago; a running attempt's files are never touched |
| `uploads/…` | older than `upload_retention_hours` (7 days), unless a queued/running/retrying job references it |
| succeeded outputs | only if `output_retention_hours` is set (off by default): rows are deleted first, then files, so no row points at a missing file |

Workers run it about once per `storage_cleanup_interval` (1h) across the whole cluster. A Redis
`SET NX EX` key picks which worker runs each slot, and a Postgres advisory lock prevents
overlapping runs. `taskmaster cleanup` runs it once by hand.

Known edge: redriving a dead job whose upload has passed retention fails permanently with "input
file does not exist". The retention period is the knob for that.

## Alternatives considered

- **Upload through the API (multipart form):** simplest for clients, but every byte crosses the API
  process and its memory/timeouts. Rejected; presigned URLs are the standard answer.
- **`ProcessPoolExecutor`:** warm processes, no per-job start cost, but a submitted task can't be
  stopped once it runs. Cancellation and lease loss would only take effect when the task finished,
  breaking the guarantees of ADR 0004. Rejected for now; a pool of killable workers could replace
  it if start-up cost ever matters.
