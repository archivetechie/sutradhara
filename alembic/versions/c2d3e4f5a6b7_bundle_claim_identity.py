"""Bundle claim identity: ``bundle.claimed_by`` for the flush CAS and reaper.

design-bundle-groups §4 (claim discipline): the guarded ``open -> flushing``
transition writes the flusher's process identity (``hostname:pid``, per
``jobs/attempts.py::default_worker_id``). The sweeper's reaper checks that
identity for liveness before returning a stuck ``flushing`` bundle to ``open``,
and ``close_bundle`` is a guarded compare-and-set on
``status = 'flushing' AND claimed_by = :token`` so a reaped-then-returning
flusher fails loudly instead of sealing a member set that is not on media.

Revision ID: c2d3e4f5a6b7
Revises: b9c8d7e6f5a4
Create Date: 2026-08-05
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c2d3e4f5a6b7"
down_revision: str | Sequence[str] | None = "b9c8d7e6f5a4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("bundle") as batch:
        batch.add_column(sa.Column("claimed_by", sa.String(255), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("bundle") as batch:
        batch.drop_column("claimed_by")
