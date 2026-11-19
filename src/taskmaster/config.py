from functools import lru_cache
from pathlib import Path
from typing import Literal, Self
from urllib.parse import parse_qs, urlsplit

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Docker/Kubernetes secrets: a file named like the variable (e.g.
# /run/secrets/taskmaster_s3_secret_key) is used when the environment variable is not set.
SECRETS_DIR = Path("/run/secrets")

# Development defaults that must never reach a shared environment.
DEV_SECRETS = {"taskmaster", "taskmaster-secret"}


class Settings(BaseSettings):
    """Runtime configuration, read from TASKMASTER_* environment variables (or secret files)."""

    model_config = SettingsConfigDict(
        env_prefix="TASKMASTER_",
        extra="ignore",
        secrets_dir=SECRETS_DIR if SECRETS_DIR.is_dir() else None,
    )

    # "production" turns insecure settings into startup errors (see `production_problems`).
    env: Literal["development", "production"] = "development"
    # In production, plaintext connections to Postgres, Redis and S3 are refused unless this is
    # set (e.g. everything runs on a private network or behind a service mesh doing mTLS).
    allow_insecure_transport: bool = False

    database_url: str = "postgresql+psycopg://taskmaster:taskmaster@localhost:5432/taskmaster"
    redis_url: str = "redis://localhost:6379/0"
    # Without timeouts a dropped connection (network partition, Redis host gone) can hang a
    # worker or relay forever. The read timeout must exceed `worker_block_ms` (XREADGROUP BLOCK).
    redis_socket_timeout: float = 10.0
    redis_connect_timeout: float = 5.0
    # PING idle connections before reuse this often (seconds), so dead ones are replaced.
    redis_health_check_interval: int = 30
    # Postgres: give up connecting after this long instead of hanging (seconds).
    db_connect_timeout: int = 5
    log_level: str = "INFO"
    log_format: str = "console"  # "json" for one JSON object per line (containers)
    # Workers and relays serve Prometheus metrics on this port (None disables). The API serves
    # them at /metrics on its own port.
    metrics_port: int | None = 9100
    # How often relays sample queue depth gauges (jobs by status, stream lengths, outbox lag).
    queue_stats_interval: float = 5.0

    # Redis Streams
    jobs_stream: str = "taskmaster:jobs"
    events_stream: str = "taskmaster:events"
    dead_letter_stream: str = "taskmaster:dead-letter"
    stream_maxlen: int = 100_000  # approximate cap for the events and dead-letter streams
    consumer_group: str = "workers"

    # Worker
    worker_concurrency: int = 4
    # How long one XREADGROUP blocks; also bounds how quickly an idle worker notices shutdown.
    worker_block_ms: int = 2000
    # A running job's lease; renewed every heartbeat. Expired lease => worker presumed dead.
    lease_seconds: float = 30.0
    # Heartbeat period: renews the lease and picks up cancellation requests.
    heartbeat_interval: float = 5.0
    retry_base_delay: float = 2.0
    retry_max_delay: float = 300.0
    retry_jitter: float = 0.1
    # On SIGTERM, wait this long for running jobs, then stop them and hand them back to the queue.
    # Keep below the container's stop grace period.
    worker_shutdown_timeout: float = 20.0
    # Maintenance loop (each worker): expired-lease sweep and stale-dispatch safety net.
    maintenance_interval: float = 5.0
    # Re-dispatch queued/retrying jobs with no pending outbox event after this long (lost message).
    stale_dispatch_seconds: float = 600.0
    # XAUTOCLAIM: take over stream messages a dead consumer read but never acked.
    reclaim_idle_ms: int = 30_000
    reclaim_interval: float = 10.0

    # Outbox relay
    relay_batch_size: int = 100
    # Fallback poll when no NOTIFY arrives; also how promptly delayed (retry) events go out.
    relay_poll_interval: float = 1.0
    relay_error_backoff: float = 2.0
    outbox_retention_hours: float = 168.0  # delete published outbox rows after 7 days
    outbox_cleanup_interval: float = 300.0

    # API keys and rate limiting
    # Reject requests without a valid API key. When false, anonymous requests are allowed and
    # rate limited per client IP.
    require_api_key: bool = False
    rate_limit_per_minute: int = 600  # default per API key (keys can override)
    anonymous_rate_limit_per_minute: int = 120  # per client IP; 0 disables
    # Browser origins allowed to call the API (CORS), e.g. ["https://app.example.com"]. Empty
    # (default) sends no CORS headers, so browsers block cross-origin calls.
    cors_origins: list[str] = []
    # Largest request body the API accepts (bytes). Files go to object storage, not the API, so
    # job payloads stay small.
    max_request_bytes: int = 1024 * 1024
    # Give up on the rate limiter after this long and allow the request (fail open).
    rate_limit_timeout: float = 0.5
    api_key_cache_seconds: float = 30.0  # how long a revoked key may keep working per API process

    # Object storage (any S3-compatible service; RustFS in docker compose)
    s3_endpoint_url: str | None = "http://localhost:9000"
    # Endpoint that clients can reach, used for presigned URLs returned by the API. Defaults to
    # s3_endpoint_url; differs when services reach the store by an internal hostname.
    s3_public_endpoint_url: str | None = None
    s3_access_key: str = "taskmaster"
    s3_secret_key: str = "taskmaster-secret"
    # TLS verification for S3: True (system CAs), False (don't verify; never in production), or
    # a path to a CA bundle for a private CA.
    s3_verify: bool | str = True
    s3_region: str = "us-east-1"
    s3_bucket: str = "taskmaster"
    upload_url_ttl: int = 3600
    # Largest file a client may upload (bytes). The size is signed into the upload URL, and input
    # files are checked again when a job is submitted. 5 GiB is S3's single-PUT maximum.
    max_upload_bytes: int = 5 * 1024**3
    download_url_ttl: int = 3600
    # Where workers put per-job scratch files (system temp dir if unset).
    work_dir: str | None = None

    # Storage janitor (runs in one worker at a time, see taskmaster/janitor.py)
    storage_cleanup_interval: float = 3600.0
    # Delete uploads after this long, unless a job that hasn't finished still references them.
    upload_retention_hours: float = 168.0
    # Delete result files no succeeded job references (failed/lost/cancelled attempts) once the
    # attempt has been over for this long. The grace covers a lost worker still finishing writes.
    orphan_grace_hours: float = 1.0
    # Delete succeeded jobs' outputs (files and rows) after this long. None keeps them forever.
    output_retention_hours: float | None = None

    @model_validator(mode="after")
    def _check_redis_timeouts(self) -> Self:
        if self.redis_socket_timeout * 1000 <= self.worker_block_ms:
            raise ValueError(
                "redis_socket_timeout must be longer than worker_block_ms, or every idle "
                "XREADGROUP would time out"
            )
        if self.env == "production" and (problems := self.production_problems()):
            raise ValueError(
                "Refusing to start with TASKMASTER_ENV=production:"
                + "".join(f"\n- {p}" for p in problems)
            )
        return self

    def production_problems(self) -> list[str]:
        """Settings that are fine on a laptop but unsafe in a shared environment."""
        problems = []
        if not self.require_api_key:
            problems.append("require_api_key is false: anyone can submit and read jobs")
        db = urlsplit(self.database_url)
        if db.password in DEV_SECRETS:
            problems.append("database_url uses the development password")
        if self.s3_endpoint_url and (
            self.s3_access_key in DEV_SECRETS or self.s3_secret_key in DEV_SECRETS
        ):
            problems.append("s3_access_key / s3_secret_key are the development defaults")
        if self.s3_verify is False:
            problems.append("s3_verify is false: TLS certificates are not checked")
        if not self.allow_insecure_transport:
            sslmode = parse_qs(db.query).get("sslmode", [""])[0]
            if sslmode not in {"require", "verify-ca", "verify-full"}:
                problems.append(
                    "database_url has no sslmode=require/verify-ca/verify-full (plaintext)"
                )
            if not self.redis_url.startswith("rediss://"):
                problems.append("redis_url is not rediss:// (plaintext)")
            if self.s3_endpoint_url and not self.s3_endpoint_url.startswith("https://"):
                problems.append("s3_endpoint_url is not https:// (plaintext)")
        return problems

    @property
    def psycopg_dsn(self) -> str:
        """Plain libpq DSN (no SQLAlchemy driver suffix) for raw psycopg connections."""
        return self.database_url.replace("postgresql+psycopg://", "postgresql://", 1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
