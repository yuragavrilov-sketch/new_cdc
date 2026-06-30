"""Manual target-trigger enable jobs for table migrations."""

from __future__ import annotations

import json
import threading
from datetime import datetime

import db.oracle_browser as oracle_browser
import services.oracle_scn as oracle_scn
from db.state_db import row_to_dict
from services.strategy import Strategy


_ALLOWED_PHASES = {
    "COMPLETED",
    "CDC_CAUGHT_UP",
    "STEADY_STATE",
}


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
                   error_text, requested_by, created_at, started_at, completed_at
            FROM   target_trigger_jobs
            WHERE  migration_id = %s
            ORDER BY created_at DESC
        """, (migration_id,))
        return [_clean_job(row_to_dict(cur, row)) for row in cur.fetchall()]


def ensure_pending_job(conn, migration_id: str, requested_by: str | None = None) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT phase, strategy FROM migrations WHERE migration_id = %s FOR UPDATE",
            (migration_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Migration {migration_id} not found")

        phase, raw_strategy = row
        try:
            Strategy.parse(raw_strategy)
        except ValueError as exc:
            raise ValueError(f"Invalid migration strategy: {exc}") from exc

        if phase not in _ALLOWED_PHASES:
            raise ValueError(
                f"Cannot create trigger job from phase {phase}; "
                f"expected one of {', '.join(sorted(_ALLOWED_PHASES))}"
            )

        cur.execute("""
            SELECT job_id, migration_id, state, enabled_count, result_json,
                   error_text, requested_by, created_at, started_at, completed_at
            FROM   target_trigger_jobs
            WHERE  migration_id = %s
              AND  state IN ('PENDING', 'RUNNING')
            ORDER BY created_at DESC
            LIMIT 1
        """, (migration_id,))
        existing = cur.fetchone()
        if existing:
            job = _clean_job(row_to_dict(cur, existing))
            job["created"] = False
            conn.commit()
            return job

        cur.execute("""
            INSERT INTO target_trigger_jobs (migration_id, requested_by)
            VALUES (%s, %s)
            RETURNING job_id, migration_id, state, enabled_count, result_json,
                      error_text, requested_by, created_at, started_at, completed_at
        """, (migration_id, requested_by))
        job = _clean_job(row_to_dict(cur, cur.fetchone()))
        job["created"] = True
        conn.commit()
        return job


def run_job_async(
    *,
    get_conn_fn,
    load_configs_fn,
    broadcast_fn,
    migration_id: str,
    job_id: str,
) -> dict:
    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT job_id, state
                FROM   target_trigger_jobs
                WHERE  job_id = %s AND migration_id = %s
                FOR UPDATE
            """, (job_id, migration_id))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Trigger job {job_id} not found")
            if row[1] == "RUNNING":
                raise ValueError("Trigger job is already running")
            if row[1] != "PENDING":
                raise ValueError(f"Trigger job is {row[1]}, expected PENDING")

            cur.execute("""
                UPDATE target_trigger_jobs
                SET    state = 'RUNNING',
                       started_at = NOW(),
                       error_text = NULL
                WHERE  job_id = %s
                RETURNING job_id, migration_id, state, enabled_count, result_json,
                          error_text, requested_by, created_at, started_at, completed_at
            """, (job_id,))
            job = _clean_job(row_to_dict(cur, cur.fetchone()))
        conn.commit()
    finally:
        conn.close()

    broadcast_fn({
        "type": "target_trigger_job",
        "migration_id": migration_id,
        "job_id": job_id,
        "state": "RUNNING",
        "enabled_count": job.get("enabled_count") or 0,
        "ts": datetime.utcnow().isoformat() + "Z",
    })

    def _run() -> None:
        _execute_job(
            get_conn_fn=get_conn_fn,
            load_configs_fn=load_configs_fn,
            broadcast_fn=broadcast_fn,
            migration_id=migration_id,
            job_id=job_id,
        )

    threading.Thread(
        target=_run,
        daemon=True,
        name=f"trigger-job-{str(job_id)[:8]}",
    ).start()
    return job


def _execute_job(*, get_conn_fn, load_configs_fn, broadcast_fn, migration_id: str, job_id: str) -> None:
    try:
        conn = get_conn_fn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"Migration {migration_id} not found")
                migration = row_to_dict(cur, row)
        finally:
            conn.close()

        configs = load_configs_fn()
        dst_cfg = configs.get(migration["target_connection_id"], {})
        ora_conn = oracle_scn.open_oracle_conn(dst_cfg)
        try:
            result = oracle_browser.enable_triggers(
                ora_conn,
                migration["target_schema"],
                migration["target_table"],
            )
        finally:
            ora_conn.close()

        if result["errors"]:
            names = [e["name"] for e in result["errors"]]
            raise RuntimeError(
                f"Could not enable triggers: {', '.join(names)}. {result['errors']}"
            )

        enabled_count = len(result["enabled"])
        conn = get_conn_fn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE target_trigger_jobs
                    SET    state = 'DONE',
                           enabled_count = %s,
                           result_json = %s::jsonb,
                           completed_at = NOW()
                    WHERE  job_id = %s
                """, (enabled_count, json.dumps(result), job_id))
            conn.commit()
        finally:
            conn.close()

        broadcast_fn({
            "type": "target_trigger_job",
            "migration_id": migration_id,
            "job_id": job_id,
            "state": "DONE",
            "enabled_count": enabled_count,
            "ts": datetime.utcnow().isoformat() + "Z",
        })
    except Exception as exc:
        conn = get_conn_fn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE target_trigger_jobs
                    SET    state = 'FAILED',
                           error_text = %s,
                           completed_at = NOW()
                    WHERE  job_id = %s
                """, (str(exc)[:4000], job_id))
            conn.commit()
        finally:
            conn.close()
        broadcast_fn({
            "type": "target_trigger_job",
            "migration_id": migration_id,
            "job_id": job_id,
            "state": "FAILED",
            "error_text": str(exc)[:4000],
            "ts": datetime.utcnow().isoformat() + "Z",
        })
