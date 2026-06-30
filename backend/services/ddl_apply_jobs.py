"""DDL apply jobs — API-side service.

Queues jobs into ddl_apply_jobs; workers (workers/ddl_apply_worker.py) pick them
up via SELECT … FOR UPDATE SKIP LOCKED. Worker writes progress to
schema_migration_events; this module also writes a "queued" event on submit
so the user sees feedback before the worker picks anything up.
"""

# Actions supported by the DDL job executor.
_VALID_ACTIONS = {"create_missing", "sync_diff", "recreate"}

_TYPE_JOB_MAP = {
    "TABLE":             "SYNC_TABLE_DDL",
    "INDEX":             "SYNC_INDEX",
    "VIEW":              "SYNC_VIEW",
    "MATERIALIZED VIEW": "SYNC_MVIEW",
    "PROCEDURE":         "SYNC_CODE",
    "FUNCTION":          "SYNC_CODE",
    "PACKAGE":           "SYNC_CODE",
    "PACKAGE BODY":      "SYNC_CODE",
    "TYPE":              "SYNC_CODE",
    "TYPE BODY":         "SYNC_CODE",
    "TRIGGER":           "SYNC_TRIGGER",
    "SEQUENCE":          "SYNC_SEQUENCE",
    "SYNONYM":           "SYNC_SYNONYM",
    "GRANT":             "SYNC_GRANT",
    "DATABASE LINK":     "SYNC_DBLINK",
    "JOB":               "SYNC_JOB",
}

# Oracle types whose DDL can be re-applied via CREATE OR REPLACE.
# For other types sync_diff would need DROP+CREATE — refused by default.
REPLACEABLE_TYPES = {
    "VIEW", "PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE BODY",
    "TRIGGER", "TYPE", "TYPE BODY", "SYNONYM",
}


def _resolve_object_type(fe_or_oracle: str) -> str:
    """Accept either Oracle canonical (TABLE, MATERIALIZED VIEW, ...) or
    frontend alias (MVIEW, DBLINK). Return Oracle canonical."""
    fe_or_oracle = (fe_or_oracle or "").upper().strip()
    fe_map = {
        "MVIEW":  "MATERIALIZED VIEW",
        "DBLINK": "DATABASE LINK",
    }
    return fe_map.get(fe_or_oracle, fe_or_oracle)


def job_type_for_object_type(fe_or_oracle: str) -> str:
    otype = _resolve_object_type(fe_or_oracle)
    return _TYPE_JOB_MAP.get(otype, "SYNC_DDL")


def submit_jobs(
    conn,
    sm_id: str,
    action: str,
    objects: list[dict],
    job_type: str | None = None,
    state: str = "PENDING",
) -> dict:
    """Insert one ddl_apply_jobs row per object + a queued event.

    objects: [{"type": "<oracle or fe>", "name": "..."}]
    Returns {"queued": n, "skipped": [...]}.
    """
    if action not in _VALID_ACTIONS:
        raise ValueError(f"unknown action: {action}")
    state = (state or "PENDING").upper()
    if state not in {"DRAFT", "PENDING"}:
        raise ValueError(f"unsupported initial DDL job state: {state}")
    queued: list[str] = []
    skipped: list[dict] = []

    with conn.cursor() as cur:
        # Verify the schema_migration exists (avoids cryptic FK error later)
        cur.execute(
            "SELECT 1 FROM schema_migrations WHERE schema_migration_id = %s",
            (sm_id,),
        )
        if not cur.fetchone():
            raise ValueError("schema_migration not found")

        for obj in objects:
            otype = _resolve_object_type(obj.get("type", ""))
            oname = (obj.get("name") or "").strip()
            if not otype or not oname:
                skipped.append({**obj, "reason": "missing type/name"})
                continue
            if action == "sync_diff" and otype not in REPLACEABLE_TYPES:
                skipped.append({**obj, "reason": f"sync_diff not supported for {otype}"})
                continue

            resolved_job_type = (job_type or obj.get("job_type") or job_type_for_object_type(otype)).upper()
            cur.execute("""
                SELECT job_id, state
                FROM   ddl_apply_jobs
                WHERE  schema_migration_id = %s
                  AND  object_type = %s
                  AND  object_name = %s
                  AND  state IN ('DRAFT', 'PENDING', 'CLAIMED', 'RUNNING')
                LIMIT  1
            """, (sm_id, otype, oname))
            existing = cur.fetchone()
            if existing:
                skipped.append({
                    **obj,
                    "reason": f"already has active DDL job {existing[0]} ({existing[1]})",
                })
                continue

            cur.execute("""
                INSERT INTO ddl_apply_jobs
                    (schema_migration_id, action, job_type, object_type, object_name, state)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING job_id
            """, (sm_id, action, resolved_job_type, otype, oname, state))
            job_id = cur.fetchone()[0]
            queued.append(str(job_id))

            event_type = "ddl_pack.added" if state == "DRAFT" else "ddl_apply.queued"
            message = (
                f"added {resolved_job_type} to DDL pack for {action.replace('_', ' ')}"
                if state == "DRAFT"
                else f"queued {resolved_job_type} for {action.replace('_', ' ')}"
            )
            cur.execute("""
                INSERT INTO schema_migration_events
                    (schema_migration_id, event_type, object_type, object_name,
                     level, message, job_id)
                VALUES (%s, %s, %s, %s, 'info',
                        %s, %s)
            """, (sm_id, event_type, otype, oname, message, job_id))

        conn.commit()
    return {"queued": len(queued), "job_ids": queued, "skipped": skipped, "state": state}


def start_draft_pack(conn, sm_id: str, job_ids: list[str] | None = None) -> dict:
    """Move DRAFT DDL jobs to PENDING so workers can claim them."""
    params: list = [sm_id]
    id_filter = ""
    if job_ids:
        id_filter = "AND job_id = ANY(%s::uuid[])"
        params.append(job_ids)
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE ddl_apply_jobs
            SET    state = 'PENDING'
            WHERE  schema_migration_id = %s
              AND  state = 'DRAFT'
              {id_filter}
            RETURNING job_id, job_type, action, object_type, object_name
        """, tuple(params))
        rows = cur.fetchall()
        for job_id, job_type, action, object_type, object_name in rows:
            cur.execute("""
                INSERT INTO schema_migration_events
                    (schema_migration_id, event_type, object_type, object_name,
                     level, message, job_id)
                VALUES (%s, 'ddl_pack.started', %s, %s, 'info', %s, %s)
            """, (
                sm_id,
                object_type,
                object_name,
                f"started {job_type} from DDL pack for {action.replace('_', ' ')}",
                job_id,
            ))
    conn.commit()
    return {
        "started": len(rows),
        "job_ids": [str(row[0]) for row in rows],
    }


def cancel_pending(conn, sm_id: str) -> int:
    """Cancel only PENDING jobs (running ones are left alone — they'll finish).
    Returns count cancelled."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ddl_apply_jobs
            SET    state = 'CANCELLED', completed_at = NOW()
            WHERE  schema_migration_id = %s AND state = 'PENDING'
        """, (sm_id,))
        n = cur.rowcount
        if n:
            cur.execute("""
                INSERT INTO schema_migration_events
                    (schema_migration_id, event_type, level, message)
                VALUES (%s, 'ddl_apply.cancelled', 'info', %s)
            """, (sm_id, f"cancelled {n} pending job(s)"))
        conn.commit()
    return n


def list_jobs(conn, sm_id: str, limit: int = 100) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_id, action, job_type, object_type, object_name, state,
                   error_text, created_at, started_at, completed_at
            FROM   ddl_apply_jobs
            WHERE  schema_migration_id = %s
            ORDER BY created_at DESC
            LIMIT  %s
        """, (sm_id, limit))
        out = []
        for r in cur.fetchall():
            out.append({
                "job_id":       str(r[0]),
                "action":       r[1],
                "job_type":     r[2],
                "object_type":  r[3],
                "object_name":  r[4],
                "state":        r[5],
                "error_text":   r[6],
                "created_at":   r[7].isoformat() if r[7] else None,
                "started_at":   r[8].isoformat() if r[8] else None,
                "completed_at": r[9].isoformat() if r[9] else None,
            })
        return out
