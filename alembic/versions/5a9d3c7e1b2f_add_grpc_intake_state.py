"""add grpc intake state and device enrollment

Revision ID: 5a9d3c7e1b2f
Revises: e9f0a1b2c3d4
Create Date: 2026-06-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "5a9d3c7e1b2f"
down_revision: str | Sequence[str] | None = "e9f0a1b2c3d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""

    op.create_table(
        "grpc_intake",
        sa.Column("intake_id", sa.String(length=128), nullable=False),
        sa.Column("operator", sa.String(length=128), nullable=False),
        sa.Column("device_id", sa.String(length=256), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("manifest_digest", sa.String(length=64), nullable=True),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("source_plan_digest", sa.String(length=64), nullable=False),
        sa.Column("artifactclass", sa.String(length=128), nullable=False),
        sa.Column("source_kind", sa.String(length=32), nullable=False),
        sa.Column("source_ref", sa.String(length=1024), nullable=True),
        sa.Column("label", sa.String(length=512), nullable=True),
        sa.Column("landing_root", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "state IN ('streaming', 'committing', 'committed', 'aborted')",
            name="ck_grpc_intake_state",
        ),
        sa.PrimaryKeyConstraint("intake_id"),
    )
    op.create_index("ix_grpc_intake_owner", "grpc_intake", ["operator", "device_id"])

    op.create_table(
        "grpc_device_enrollment",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("device_id", sa.String(length=256), nullable=False),
        sa.Column("cert_fingerprint", sa.String(length=95), nullable=False),
        sa.Column("operator", sa.String(length=128), nullable=False),
        sa.Column("revoked", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "device_id",
            "cert_fingerprint",
            name="uq_grpc_device_fingerprint",
        ),
    )
    op.create_index("ix_grpc_device_enrollment_device_id", "grpc_device_enrollment", ["device_id"])
    op.create_index(
        "ix_grpc_device_fingerprint",
        "grpc_device_enrollment",
        ["cert_fingerprint"],
    )

    op.create_table(
        "grpc_enroll_token",
        sa.Column("token", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("token"),
    )


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_table("grpc_enroll_token")
    op.drop_index("ix_grpc_device_fingerprint", table_name="grpc_device_enrollment")
    op.drop_index("ix_grpc_device_enrollment_device_id", table_name="grpc_device_enrollment")
    op.drop_table("grpc_device_enrollment")
    op.drop_index("ix_grpc_intake_owner", table_name="grpc_intake")
    op.drop_table("grpc_intake")
