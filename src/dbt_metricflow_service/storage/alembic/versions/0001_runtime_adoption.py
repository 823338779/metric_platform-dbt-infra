"""Adopt supported runtime databases without rewriting published identities."""

from alembic import op

from dbt_metricflow_service.storage.legacy_schema import adopt

revision = "0001_runtime_adoption"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    adopt(op.get_bind())


def downgrade() -> None:
    raise RuntimeError("Runtime adoption does not support downgrade; restore a verified backup")
