"""Prometheus metrics.

The API serves them at /metrics; workers and relays start a metrics server on `metrics_port`
(9100). Label sets are kept bounded: task types and outcomes, never job ids, API keys or IPs.
"""

from prometheus_client import Counter, Gauge, Histogram

# --- API ------------------------------------------------------------------------------------

HTTP_REQUESTS = Counter(
    "taskmaster_http_requests_total", "HTTP requests", ["method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "taskmaster_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
JOBS_SUBMITTED = Counter("taskmaster_jobs_submitted_total", "Jobs created", ["type"])
RATE_LIMITED = Counter(
    "taskmaster_rate_limited_total", "Requests rejected with 429", ["client"]
)  # client: "api_key" or "anonymous"
RATE_LIMITER_ERRORS = Counter(
    "taskmaster_rate_limiter_errors_total",
    "Rate limiter unavailable (Redis error); the request was allowed",
)

# --- Worker ---------------------------------------------------------------------------------

JOB_RUNS = Counter(
    "taskmaster_job_runs_total",
    "Finished job runs by outcome: succeeded, failed (will retry), dead, cancelled, released, lost",
    ["type", "outcome"],
)
JOB_DURATION = Histogram(
    "taskmaster_job_duration_seconds",
    "Run time of one attempt",
    ["type", "outcome"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600),
)
JOB_QUEUE_WAIT = Histogram(
    "taskmaster_job_queue_wait_seconds",
    "Time from creation to the first attempt starting",
    ["type"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300),
)
JOBS_IN_PROGRESS = Gauge("taskmaster_jobs_in_progress", "Jobs running in this worker", ["type"])
LEASES_EXPIRED = Counter(
    "taskmaster_leases_expired_total", "Running jobs recovered from a dead worker"
)
MESSAGES_RECLAIMED = Counter(
    "taskmaster_messages_reclaimed_total", "Unacked stream messages taken over (XAUTOCLAIM)"
)
JOBS_REDISPATCHED = Counter(
    "taskmaster_jobs_redispatched_total", "Stale queued jobs re-dispatched (lost messages)"
)

# --- Relay ----------------------------------------------------------------------------------

OUTBOX_PUBLISHED = Counter(
    "taskmaster_outbox_published_total", "Outbox events published to Redis", ["event_type"]
)
OUTBOX_PUBLISH_ERRORS = Counter(
    "taskmaster_outbox_publish_errors_total", "Outbox events that failed to publish"
)

# --- Queue state (sampled by every relay; aggregate with max() across instances) ------------

JOBS_BY_STATUS = Gauge("taskmaster_jobs", "Jobs that haven't finished, by status", ["status"])
STREAM_LENGTH = Gauge("taskmaster_stream_length", "Entries in a Redis stream", ["stream"])
STREAM_PENDING = Gauge(
    "taskmaster_stream_pending", "Messages delivered to workers but not yet acked"
)
OUTBOX_DUE = Gauge("taskmaster_outbox_due", "Unpublished outbox events that are due")
OUTBOX_LAG = Gauge(
    "taskmaster_outbox_lag_seconds", "How long the oldest due outbox event has been waiting"
)
