"""Reliability: idempotency keys, cancellation, lease sweep index, more attempt statuses.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OLD_ATTEMPT_STATUSES = ("running", "succeeded", "failed")
NEW_ATTEMPT_STATUSES = (*OLD_ATTEMPT_STATUSES, "cancelled", "released", "lost")


def _in(values: tuple[str, ...]) -> str:
    return "status IN (" + ", ".join(f"'{v}'" for v in values) + ")"


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "cancel_requested", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
    )
    op.add_column("jobs", sa.Column("idempotency_key", sa.String(length=255), nullable=True))
    op.add_column("jobs", sa.Column("request_hash", sa.String(length=64), nullable=True))
    op.create_index("ux_jobs_idempotency_key", "jobs", ["idempotency_key"], unique=True)
    op.create_index(
        "ix_jobs_running_lease",
        "jobs",
        ["lease_until"],
        postgresql_where=sa.text("status = 'running'"),
    )

    # A released attempt gives its number back, so (job_id, attempt) is no longer unique.
    op.drop_constraint("uq_job_attempts_job_attempt", "job_attempts", type_="unique")
    op.create_index("ix_job_attempts_job_id", "job_attempts", ["job_id"])
    op.drop_constraint("attempt_status", "job_attempts", type_="check")
    op.create_check_constraint("attempt_status", "job_attempts", _in(NEW_ATTEMPT_STATUSES))

    op.create_index("ix_outbox_published_at", "outbox", ["published_at"])
    op.create_index("ix_outbox_aggregate_id", "outbox", ["aggregate_id"])


def downgrade() -> None:
    op.drop_index("ix_outbox_aggregate_id", table_name="outbox")
    op.drop_index("ix_outbox_published_at", table_name="outbox")

    op.execute("UPDATE job_attempts SET status = 'failed' WHERE status IN ('cancelled', 'lost')")
    op.execute("DELETE FROM job_attempts WHERE status = 'released'")
    op.drop_constraint("attempt_status", "job_attempts", type_="check")
    op.create_check_constraint("attempt_status", "job_attempts", _in(OLD_ATTEMPT_STATUSES))
    op.drop_index("ix_job_attempts_job_id", table_name="job_attempts")
    op.create_unique_constraint(
        "uq_job_attempts_job_attempt", "job_attempts", ["job_id", "attempt"]
    )

    op.drop_index("ix_jobs_running_lease", table_name="jobs")
    op.drop_index("ux_jobs_idempotency_key", table_name="jobs")
    op.drop_column("jobs", "request_hash")
    op.drop_column("jobs", "idempotency_key")
    op.drop_column("jobs", "cancel_requested")
