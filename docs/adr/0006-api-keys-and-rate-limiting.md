# ADR 0006: API keys and per-key rate limiting

## Context

Several clients share one TaskMaster. Without limits, one client submitting in a tight loop can
fill the queue and starve everyone else. Clients also must not be able to see or cancel each
other's jobs.

## Decision

### API keys

- Keys look like `tm_<prefix>_<secret>` and are sent as `X-API-Key` or `Authorization: Bearer`.
  The database stores only a SHA-256 of the key. Keys are long random strings, so a fast hash is
  enough (there is nothing to brute-force, unlike a password); bcrypt or argon2 would add
  milliseconds to every request for no gain. The prefix is stored in clear text so keys can be
  listed and revoked.
- Keys are managed from the CLI (`taskmaster apikey create|list|revoke`), not over HTTP, so there
  is no admin endpoint to protect.
- Each API process caches the key lookup for 30s. A revoked key can therefore keep working for up
  to 30s per process, in exchange for no database query per request.
- `require_api_key=false` (compose default) keeps the one-curl quick start working: anonymous
  callers are allowed and rate limited per client IP.
- **Ownership:** jobs carry `api_key_id`, and every read or write is filtered by it. Someone else's
  job returns 404, not 403, so its existence isn't revealed. Idempotency keys are unique per owner
  (`UNIQUE (api_key_id, idempotency_key) NULLS NOT DISTINCT`), so two clients can't collide on
  `Idempotency-Key: 1`.

### Rate limiting: token bucket in Redis

Each client has a bucket of `limit` tokens that refills at `limit` per minute; a request takes one
token. One Lua script reads, refills, takes and writes the bucket:

- **Atomic and shared**: every API replica sees the same bucket, with no race between read and
  write.
- **Redis's clock** (`TIME`): API hosts don't need synchronized clocks.
- **Bursts allowed** up to one minute's worth, with the long-run rate held to the limit. A fixed
  window would allow 2× the limit across a window boundary; a sliding log would store a timestamp
  per request.

Responses carry `RateLimit-Limit`, `RateLimit-Remaining` and `RateLimit-Reset`, and a 429 adds
`Retry-After`, computed from the refill rate.

**Fail open.** If Redis is unavailable, requests are allowed and counted in
`taskmaster_rate_limiter_errors_total`. The outbox already keeps job submission independent of
Redis (ADR 0002); losing rate limiting for the length of an outage is a smaller risk than refusing
all traffic.

## Consequences

- Fair sharing between clients at the API edge, at the cost of one Redis round-trip per request
  (about 0.3 ms locally).
- Per-key limits are configurable (`--rate-limit` at creation, or the default), so a batch
  importer and an interactive client can have different budgets.
- Anonymous limiting by IP is approximate behind proxies or NAT. Production should require keys.
- Uploaded files aren't owned yet: a job can reference any upload key it knows. Keys contain a
  random UUID, so they can't be guessed, but ownership checks belong here once uploads are tracked
  per key.

## Alternatives considered

- **In-process limiter:** no Redis round-trip, but every API replica gets its own budget, so N
  replicas allow N× the limit.
- **Limit at a gateway (nginx, Envoy):** good at the edge, but it doesn't know API keys from the
  database, and it adds infrastructure that a local stack doesn't otherwise need.
