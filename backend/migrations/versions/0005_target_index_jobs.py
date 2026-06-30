"""target index enable jobs

Revision ID: 0005_target_index_jobs
Revises: 0004_ddl_apply_job_types
Create Date: 2026-06-29
"""

from __future__ import annotations

from alembic import op


revision = "0005_target_index_jobs"
down_revision = "0004_ddl_apply_job_types"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS target_index_jobs (
            job_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            migration_id   UUID NOT NULL REFERENCES migrations(migration_id) ON DELETE CASCADE,
            state          VARCHAR(16) NOT NULL DEFAULT 'PENDING',
            enabled_count  INTEGER NOT NULL DEFAULT 0,
            result_json    JSONB,
            error_text     TEXT,
            requested_by   VARCHAR(128),
            worker_id      VARCHAR(200),
            created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at     TIMESTAMPTZ,
            completed_at   TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_target_index_jobs_pending
            ON target_index_jobs(state, created_at)
            WHERE state = 'PENDING'
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_target_index_jobs_migration
            ON target_index_jobs(migration_id, created_at DESC)
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_target_index_jobs_open
            ON target_index_jobs(migration_id)
            WHERE state IN ('PENDING', 'RUNNING')
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_target_index_jobs_open")
    op.execute("DROP INDEX IF EXISTS idx_target_index_jobs_migration")
    op.execute("DROP INDEX IF EXISTS idx_target_index_jobs_pending")
    op.execute("DROP TABLE IF EXISTS target_index_jobs")
