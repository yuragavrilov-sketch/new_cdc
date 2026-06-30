"""worker heartbeat registry

Revision ID: 0003_worker_heartbeats
Revises: 0002_target_trigger_jobs
Create Date: 2026-06-26
"""

from __future__ import annotations

from alembic import op


revision = "0003_worker_heartbeats"
down_revision = "0002_target_trigger_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS worker_heartbeats (
            worker_id       VARCHAR(200) PRIMARY KEY,
            role            VARCHAR(64) NOT NULL DEFAULT 'universal',
            capabilities    JSONB NOT NULL DEFAULT '[]'::jsonb,
            started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_last
            ON worker_heartbeats(last_heartbeat DESC)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_worker_heartbeats_last")
    op.execute("DROP TABLE IF EXISTS worker_heartbeats")
