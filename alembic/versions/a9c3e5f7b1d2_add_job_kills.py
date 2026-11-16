"""Count worker deaths separately from completed job attempts.

Revision ID: a9c3e5f7b1d2
Revises: f8b2d4e6a0c1
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a9c3e5f7b1d2"
down_revision: str | Sequence[str] | None = "f8b2d4e6a0c1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("job") as batch:
        batch.add_column(
            sa.Column("kills", sa.Integer(), nullable=False, server_default=sa.text("0"))
        )


def downgrade() -> None:
    with op.batch_alter_table("job") as batch:
        batch.drop_column("kills")
