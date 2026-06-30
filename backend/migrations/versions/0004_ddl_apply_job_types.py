"""DDL apply job types

Revision ID: 0004_ddl_apply_job_types
Revises: 0003_worker_heartbeats
Create Date: 2026-06-26
"""

from __future__ import annotations

from alembic import op


revision = "0004_ddl_apply_job_types"
down_revision = "0003_worker_heartbeats"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE ddl_apply_jobs
            ADD COLUMN IF NOT EXISTS job_type VARCHAR(64) NOT NULL DEFAULT 'SYNC_DDL'
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_ddl_apply_jobs_type
            ON ddl_apply_jobs(schema_migration_id, job_type, state, created_at DESC)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_ddl_apply_jobs_type")
    op.execute("ALTER TABLE ddl_apply_jobs DROP COLUMN IF EXISTS job_type")
