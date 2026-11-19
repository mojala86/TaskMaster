"""Declarative base and the columns tables share.

Every table has an `id` primary key, but of one of two kinds, so each model picks a mixin:

- `UuidPrimaryKey`: random UUIDs, for rows whose id is shown to clients (`/jobs/{id}`) and must
  not be guessable or reveal how many rows exist.
- `BigIntPrimaryKey`: an increasing BIGINT identity, for internal rows. Cheaper to index, and
  ordered (the outbox relay publishes events in id order).

    class Job(UuidPrimaryKey, CreatedAt, Base): ...
"""

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Identity, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

TIMESTAMPTZ = DateTime(timezone=True)


class Base(DeclarativeBase):
    pass


class UuidPrimaryKey:
    # sort_order: keep `id` as the first column in tables created from these models.
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4, sort_order=-10)


class BigIntPrimaryKey:
    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True, sort_order=-10)


class CreatedAt:
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, server_default=func.now())
