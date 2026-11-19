"""Per-client rate limiting: a token bucket in Redis.

Each client (API key, or IP for anonymous callers) has a bucket of `limit` tokens that refills at
`limit` per minute; a request takes one token. That allows short bursts up to the per-minute limit
while holding the long-run rate to it.

The bucket update is one Lua script, so it is atomic and shared by every API replica. It uses
Redis's clock, so API hosts don't need synchronized clocks.
"""

from dataclasses import dataclass

from redis.asyncio import Redis

_TOKEN_BUCKET = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or capacity
local ts = tonumber(state[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate)

local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = (cost - tokens) / rate
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], math.ceil(capacity / rate) + 1)
-- Floats as strings: Lua numbers are truncated to integers in Redis replies.
return {allowed, tostring(tokens), tostring(retry_after), tostring((capacity - tokens) / rate)}
"""


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after: float  # seconds until a request would be allowed (0 if allowed)
    reset_after: float  # seconds until the bucket is full again


class RateLimiter:
    def __init__(self, redis: Redis, prefix: str = "taskmaster:ratelimit:") -> None:
        self.prefix = prefix
        self._script = redis.register_script(_TOKEN_BUCKET)

    async def hit(self, subject: str, limit_per_minute: int, cost: int = 1) -> RateLimitResult:
        allowed, tokens, retry_after, reset_after = await self._script(
            keys=[self.prefix + subject], args=[limit_per_minute, limit_per_minute / 60.0, cost]
        )
        return RateLimitResult(
            allowed=bool(int(allowed)),
            limit=limit_per_minute,
            remaining=int(float(tokens)),
            retry_after=float(retry_after),
            reset_after=float(reset_after),
        )
