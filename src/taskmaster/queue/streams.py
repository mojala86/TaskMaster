"""Redis Streams transport.

- jobs stream:        one entry per "run this job" (dispatch events), consumed by the worker
                      consumer group. An entry is XACKed and XDELed as soon as the job is claimed
                      in Postgres: from then on the job's lease, not the message, guards recovery.
- events stream:      job lifecycle events (succeeded / dead / cancelled) for downstream consumers
                      such as the webhook dispatcher (phase 4).
- dead-letter stream: a copy of every job.dead event, for alerting and inspection.

Both event streams are capped with an approximate MAXLEN. Redis only carries ids and event
metadata; Postgres stays the source of truth.
"""

import json
from dataclasses import dataclass
from typing import Any, cast

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline
from redis.exceptions import ResponseError

from taskmaster.config import Settings
from taskmaster.db.models import OutboxEvent
from taskmaster.jobs.events import DISPATCH_EVENTS, EventType


def connect_redis(settings: Settings, *, socket_timeout: float | None = None) -> Redis:
    """A Redis client with timeouts and TCP keepalive, so a dead peer surfaces as an error."""
    return Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=socket_timeout or settings.redis_socket_timeout,
        socket_connect_timeout=settings.redis_connect_timeout,
        socket_keepalive=True,
        health_check_interval=settings.redis_health_check_interval,
        retry_on_timeout=False,  # callers already retry with backoff; don't double up
    )


@dataclass(frozen=True)
class Message:
    id: str
    job_id: str


class Streams:
    def __init__(self, client: Redis, settings: Settings) -> None:
        self.redis = client
        self.settings = settings

    @classmethod
    def from_settings(cls, settings: Settings) -> "Streams":
        return cls(connect_redis(settings), settings)

    async def close(self) -> None:
        await self.redis.aclose()

    async def ensure_group(self) -> None:
        """Create the worker consumer group (and the stream) if missing. Idempotent."""
        try:
            await self.redis.xgroup_create(
                self.settings.jobs_stream, self.settings.consumer_group, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def stage_publish(self, pipe: Pipeline, event: OutboxEvent) -> int:
        """Queue the XADD(s) for one outbox event on a pipeline. Returns how many were queued."""
        event_type = EventType(event.event_type)
        if event_type in DISPATCH_EVENTS:
            pipe.xadd(
                self.settings.jobs_stream,
                {"job_id": str(event.aggregate_id), "outbox_id": str(event.id)},
            )
            return 1

        fields: dict[Any, Any] = {
            "type": event.event_type,
            "job_id": str(event.aggregate_id),
            "outbox_id": str(event.id),
            "payload": json.dumps(event.payload),
        }
        streams = [self.settings.events_stream]
        if event_type is EventType.JOB_DEAD:
            streams.append(self.settings.dead_letter_stream)
        for stream in streams:
            pipe.xadd(stream, fields, maxlen=self.settings.stream_maxlen, approximate=True)
        return len(streams)

    async def read(self, consumer: str, *, count: int = 1, block_ms: int) -> list[Message]:
        response = cast(
            list[tuple[str, list[tuple[str, dict[str, str]]]]] | None,
            await self.redis.xreadgroup(
                self.settings.consumer_group,
                consumer,
                {self.settings.jobs_stream: ">"},
                count=count,
                block=block_ms,
            ),
        )
        return [
            Message(id=msg_id, job_id=fields["job_id"])
            for _stream, entries in response or []
            for msg_id, fields in entries
        ]

    async def reclaim(self, consumer: str, *, min_idle_ms: int, count: int = 1) -> list[Message]:
        """XAUTOCLAIM messages another consumer read but never acked (it died before claiming)."""
        response = await self.redis.xautoclaim(
            self.settings.jobs_stream,
            self.settings.consumer_group,
            consumer,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )
        entries = cast(list[tuple[str, dict[str, str] | None]], response[1])
        # Entries deleted from the stream come back without fields; nothing to run.
        return [Message(id=msg_id, job_id=f["job_id"]) for msg_id, f in entries if f]

    async def ack(self, message_id: str) -> None:
        # Single consumer group, so an acked entry is never needed again: delete it to keep the
        # stream small.
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xack(self.settings.jobs_stream, self.settings.consumer_group, message_id)
            pipe.xdel(self.settings.jobs_stream, message_id)
            await pipe.execute()
