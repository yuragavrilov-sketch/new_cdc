"""baseline State DB schema

Revision ID: 0001_state_db_baseline
Revises:
Create Date: 2026-06-22
"""

from __future__ import annotations


revision = "0001_state_db_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Keep the existing idempotent bootstrap as the single source of truth for
    # the baseline. New schema changes should be added as normal Alembic
    # revisions after this file.
    from db.state_db import _init_schema_legacy

    _init_schema_legacy()


def downgrade() -> None:
    # Destructive baseline downgrade would drop the operational State DB.
    # Leave it as an explicit no-op; create forward migrations instead.
    pass
