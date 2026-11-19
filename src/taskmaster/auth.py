"""API keys.

Format: `tm_<prefix>_<secret>`. The prefix is stored in clear text so keys can be listed and
revoked; only a SHA-256 of the whole key is stored. API keys are long random strings, so a fast
hash is enough: there is nothing to brute-force (unlike passwords).
"""

import hashlib
import secrets
import time
import uuid
from dataclasses import dataclass
from typing import Self

from sqlalchemy import func, select, update

from taskmaster.config import Settings
from taskmaster.db import ApiKey, Database


@dataclass(frozen=True)
class Principal:
    """Who is calling: an API key, or an anonymous client identified by IP."""

    key_id: uuid.UUID | None
    name: str
    rate_limit_per_minute: int
    subject: str  # rate-limit bucket: "key:<id>" or "ip:<address>"

    @property
    def anonymous(self) -> bool:
        return self.key_id is None

    @classmethod
    def anonymous_client(cls, address: str, settings: Settings) -> Self:
        return cls(None, "anonymous", settings.anonymous_rate_limit_per_minute, f"ip:{address}")


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def generate_key() -> tuple[str, str, str]:
    """Returns (key, prefix, hash). The key is shown to the user once and never stored."""
    prefix = secrets.token_hex(4)
    key = f"tm_{prefix}_{secrets.token_urlsafe(24)}"
    return key, prefix, hash_key(key)


class ApiKeyResolver:
    """Resolves a presented key to a Principal, with a short in-process cache.

    The cache saves a database query per request. The cost: a revoked key keeps working for up to
    `api_key_cache_seconds` on each API process.
    """

    def __init__(self, db: Database, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self._cache: dict[str, tuple[float, Principal | None]] = {}

    async def resolve(self, key: str) -> Principal | None:
        digest = hash_key(key)
        now = time.monotonic()
        cached = self._cache.get(digest)
        if cached is not None and cached[0] > now:
            return cached[1]

        async with self.db.session() as session:
            row = await session.scalar(
                select(ApiKey).where(ApiKey.key_hash == digest, ApiKey.revoked_at.is_(None))
            )
        principal = (
            None
            if row is None
            else Principal(
                key_id=row.id,
                name=row.name,
                rate_limit_per_minute=row.rate_limit_per_minute
                or self.settings.rate_limit_per_minute,
                subject=f"key:{row.id}",
            )
        )
        if len(self._cache) > 10_000:
            self._cache.clear()
        self._cache[digest] = (now + self.settings.api_key_cache_seconds, principal)
        return principal


# --- Management (used by the `taskmaster apikey` CLI) -------------------------------------


async def create_api_key(
    db: Database, name: str, rate_limit_per_minute: int | None = None
) -> tuple[ApiKey, str]:
    key, prefix, digest = generate_key()
    row = ApiKey(
        name=name, prefix=prefix, key_hash=digest, rate_limit_per_minute=rate_limit_per_minute
    )
    async with db.session() as session, session.begin():
        session.add(row)
    return row, key


async def list_api_keys(db: Database) -> list[ApiKey]:
    async with db.session() as session:
        return list(await session.scalars(select(ApiKey).order_by(ApiKey.created_at)))


async def revoke_api_key(db: Database, prefix: str) -> bool:
    async with db.session() as session, session.begin():
        revoked = await session.scalar(
            update(ApiKey)
            .where(ApiKey.prefix == prefix, ApiKey.revoked_at.is_(None))
            .values(revoked_at=func.now())
            .returning(ApiKey.id)
        )
    return revoked is not None
