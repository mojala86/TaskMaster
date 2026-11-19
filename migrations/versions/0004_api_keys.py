"""API keys: owners for jobs, per-key rate limits, per-owner idempotency keys.

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | Sequence[str] | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "api_keys",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("prefix", sa.String(length=16), nullable=False),
        sa.Column("key_hash", sa.String(length=64), nullable=False),
        sa.Column("rate_limit_per_minute", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("prefix"),
        sa.UniqueConstraint("key_hash"),
    )
    op.add_column("jobs", sa.Column("api_key_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "jobs_api_key_id_fkey", "jobs", "api_keys", ["api_key_id"], ["id"], ondelete="SET NULL"
    )
    op.create_index("ix_jobs_api_key_id", "jobs", ["api_key_id"])
    op.drop_index("ux_jobs_idempotency_key", table_name="jobs")
    op.create_index(
        "ux_jobs_owner_idempotency_key",
        "jobs",
        ["api_key_id", "idempotency_key"],
        unique=True,
        postgresql_nulls_not_distinct=True,
        postgresql_where=sa.text("idempotency_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ux_jobs_owner_idempotency_key", table_name="jobs")
    op.create_index("ux_jobs_idempotency_key", "jobs", ["idempotency_key"], unique=True)
    op.drop_index("ix_jobs_api_key_id", table_name="jobs")
    op.drop_constraint("jobs_api_key_id_fkey", "jobs", type_="foreignkey")
    op.drop_column("jobs", "api_key_id")
    op.drop_table("api_keys")
