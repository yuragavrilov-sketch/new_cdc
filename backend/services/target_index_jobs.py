"""Target index/constraint enable jobs for table migrations."""

from __future__ import annotations

import json

from db.state_db import row_to_dict


def _clean_job(row: dict) -> dict:
    for key in ("job_id", "migration_id"):
        if row.get(key) is not None:
            row[key] = str(row[key])
    if isinstance(row.get("result_json"), str):
        try:
            row["result_json"] = json.loads(row["result_json"])
        except Exception:
            pass
    return row


def list_jobs(conn, migration_id: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_id, migration_id, state, enabled_count, result_json,
                   error_text, requested_by, worker_id,
                   created_at, started_at, completed_at
            FROM   target_index_jobs
            WHERE  migration_id = %s
            ORDER BY created_at DESC
        """, (migration_id,))
        return [_clean_job(row_to_dict(cur, row)) for row in cur.fetchall()]


def ensure_pending_job(
    conn,
    migration_id: str,
    requested_by: str | None = None,
    *,
    force_new: bool = False,
) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
            (migration_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Migration {migration_id} not found")
        phase = row[0]
        if phase != "INDEXES_ENABLING":
            raise ValueError(
                f"Cannot create index job from phase {phase}; expected INDEXES_ENABLING"
            )

        cur.execute("""
            SELECT job_id, migration_id, state, enabled_count, result_json,
                   error_text, requested_by, worker_id,
                   created_at, started_at, completed_at
            FROM   target_index_jobs
            WHERE  migration_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (migration_id,))
        latest = cur.fetchone()
        if latest:
            job = _clean_job(row_to_dict(cur, latest))
            if job.get("state") in ("PENDING", "RUNNING", "DONE") or not force_new:
                job["created"] = False
                conn.commit()
                return job

        cur.execute("""
            INSERT INTO target_index_jobs (migration_id, requested_by)
            VALUES (%s, %s)
            RETURNING job_id, migration_id, state, enabled_count, result_json,
                      error_text, requested_by, worker_id,
                      created_at, started_at, completed_at
        """, (migration_id, requested_by))
        job = _clean_job(row_to_dict(cur, cur.fetchone()))
        job["created"] = True
        conn.commit()
        return job
