"""migration pause flag

Revision ID: 0006_migration_pause_flag
Revises: 0005_target_index_jobs
Create Date: 2026-07-08
"""

from __future__ import annotations

from alembic import op


revision = "0006_migration_pause_flag"
down_revision = "0005_target_index_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE migrations
            ADD COLUMN IF NOT EXISTS paused BOOLEAN NOT NULL DEFAULT FALSE
    """)
    op.execute("""
        ALTER TABLE migrations
            ADD COLUMN IF NOT EXISTS paused_at TIMESTAMP
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE migrations DROP COLUMN IF EXISTS paused_at")
    op.execute("ALTER TABLE migrations DROP COLUMN IF EXISTS paused")
