"""target trigger enable jobs

Revision ID: 0002_target_trigger_jobs
Revises: 0001_state_db_baseline
Create Date: 2026-06-24
"""

from __future__ import annotations

from alembic import op


revision = "0002_target_trigger_jobs"
down_revision = "0001_state_db_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS target_trigger_jobs (
            job_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            migration_id   UUID NOT NULL REFERENCES migrations(migration_id) ON DELETE CASCADE,
            state          VARCHAR(16) NOT NULL DEFAULT 'PENDING',
            enabled_count  INTEGER NOT NULL DEFAULT 0,
            result_json    JSONB,
            error_text     TEXT,
            requested_by   VARCHAR(128),
            created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at     TIMESTAMPTZ,
            completed_at   TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_target_trigger_jobs_migration
            ON target_trigger_jobs(migration_id, created_at DESC)
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_target_trigger_jobs_open
            ON target_trigger_jobs(migration_id)
            WHERE state IN ('PENDING', 'RUNNING')
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_target_trigger_jobs_open")
    op.execute("DROP INDEX IF EXISTS idx_target_trigger_jobs_migration")
    op.execute("DROP TABLE IF EXISTS target_trigger_jobs")
