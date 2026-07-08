"""
Shared utilities for workers: PostgreSQL state DB access, Oracle connections,
config loading.  Workers operate directly on the state DB — no HTTP to Flask.
"""

import json
import os
import socket
from datetime import datetime
from typing import Optional

import re
import time

import psycopg2
import psycopg2.extras

STATE_DB_DSN = os.environ.get(
    "STATE_DB_DSN",
    "postgresql://postgres:postgres@localhost:5432/migration_state",
)
WORKER_ID = os.environ.get("WORKER_ID", f"{socket.gethostname()}:{os.getpid()}")


def _masked_dsn(dsn: str) -> str:
    return re.sub(r"(://[^:]+:)[^@]+(@)", r"\1***\2", dsn)


print(f"[state_db] DSN = {_masked_dsn(STATE_DB_DSN)}")

_MAX_RETRIES   = 3
# How long without a heartbeat before a CDC worker is considered stale.
# Must match the value used in backend/routes/workers.py.
CDC_HEARTBEAT_STALE_MINUTES = int(os.environ.get("CDC_HEARTBEAT_STALE_MINUTES", "2"))
_STALE_MINUTES = 10


def cdc_topic_name(topic_prefix: str, source_schema: str, source_table: str) -> str:
    """Return the exact Kafka topic name used by Debezium and the CDC worker."""
    return f"{topic_prefix}.{source_schema.upper()}.{source_table.upper()}".replace("#", "_")


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

def get_pg_conn():
    """Open a new PostgreSQL connection to the state DB."""
    try:
        conn = psycopg2.connect(STATE_DB_DSN)
        return conn
    except Exception as exc:
        print(f"[state_db] connection FAILED ({_masked_dsn(STATE_DB_DSN)}): {exc}")
        raise


def get_pg_conn_with_retry(retries: int = 0) -> "psycopg2.connection":
    """Connect to state DB, retrying indefinitely (retries=0) or N times.

    Logs each failed attempt so container logs show progress.
    """
    attempt = 1
    while True:
        try:
            conn = psycopg2.connect(STATE_DB_DSN)
            if attempt > 1:
                print(f"[state_db] connected after {attempt} attempt(s)")
            return conn
        except Exception as exc:
            print(f"[state_db] attempt #{attempt} FAILED ({_masked_dsn(STATE_DB_DSN)}): {exc}")
            if retries and attempt >= retries:
                raise
            attempt += 1
            time.sleep(5)


# ---------------------------------------------------------------------------
# Service configs (oracle_source / oracle_target / kafka / kafka_connect)
# ---------------------------------------------------------------------------

def load_configs(conn) -> dict:
    """Read all service configs from service_configs table."""
    with conn.cursor() as cur:
        cur.execute("SELECT service_name, config FROM service_configs")
        return {row[0]: row[1] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Oracle connection
# ---------------------------------------------------------------------------

_ORACLE_CONTEXT_LIMITS = {
    "module": 48,
    "action": 32,
    "client_identifier": 64,
}


def _truncate_context_value(key: str, value: object) -> str:
    limit = _ORACLE_CONTEXT_LIMITS[key]
    return str(value or "")[:limit]


def apply_oracle_session_context(conn, context: Optional[dict]) -> None:
    """Set Oracle end-to-end tracing fields visible in v$session."""
    if not context:
        return
    for key in ("module", "action", "client_identifier"):
        value = context.get(key)
        if not value:
            continue
        try:
            setattr(conn, key, _truncate_context_value(key, value))
        except Exception:
            pass


def open_oracle(connection_id: str, configs: dict, session_context: Optional[dict] = None):
    """
    Open an Oracle connection using the service config keyed by connection_id.
    connection_id = 'oracle_source' | 'oracle_target'
    """
    try:
        import oracledb
    except ImportError:
        raise ImportError("oracledb not installed: pip install oracledb")

    cfg          = configs.get(connection_id) or {}
    host         = (cfg.get("host") or "").strip()
    port         = int(cfg.get("port") or 1521)
    service_name = (cfg.get("service_name") or "").strip()
    user         = (cfg.get("user") or "").strip()
    password     = cfg.get("password") or ""

    if not host or not service_name or not user:
        raise ValueError(
            f"Oracle '{connection_id}' not configured — "
            "check Settings in the UI."
        )
    conn = oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:{port}/{service_name}",
    )

    # By default oracledb binds Python datetime as Oracle DATE which has no
    # fractional seconds.  Override to TIMESTAMP so .microsecond is preserved.
    def _input_type_handler(cursor, value, arraysize):
        if isinstance(value, datetime):
            return cursor.var(oracledb.DB_TYPE_TIMESTAMP, arraysize=arraysize)

    conn.inputtypehandler = _input_type_handler
    apply_oracle_session_context(conn, session_context)
    return conn


# ---------------------------------------------------------------------------
# Chunk job queue  (mirrors backend/services/job_queue.py but for workers)
# ---------------------------------------------------------------------------

def claim_chunk(conn) -> Optional[dict]:
    """
    Atomically claim one PENDING chunk (BULK or BASELINE).

    Respects per-type parallelism limits:
      BULK     chunks → max_parallel_workers
      BASELINE chunks → baseline_parallel_degree
    Skips any migration that already has >= limit chunks in CLAIMED/RUNNING state.

    Fair scheduling: among eligible migrations prefers the one with the
    fewest active chunks, then earliest state_changed_at, then chunk_seq.

    Returns a dict with chunk + migration context, or None if nothing available.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH candidate AS (
                SELECT c.chunk_id
                FROM   migration_chunks c
                JOIN   migrations m ON m.migration_id = c.migration_id
                WHERE  c.status = 'PENDING'
                  AND  COALESCE(m.paused, FALSE) = FALSE
                  AND  (
                      (COALESCE(c.chunk_type, 'BULK') = 'BULK'     AND m.phase = 'BULK_LOADING')
                   OR (c.chunk_type = 'BASELINE' AND m.phase = 'BASELINE_LOADING')
                  )
                  AND  (
                      SELECT COUNT(*)
                      FROM   migration_chunks c2
                      WHERE  c2.migration_id = c.migration_id
                        AND  c2.chunk_type   = c.chunk_type
                        AND  c2.status IN ('CLAIMED', 'RUNNING')
                  ) < GREATEST(
                      CASE WHEN COALESCE(c.chunk_type, 'BULK') = 'BASELINE'
                           THEN COALESCE(m.baseline_parallel_degree, 1)
                           ELSE COALESCE(m.max_parallel_workers, 1)
                      END, 1)
                ORDER BY (
                      SELECT COUNT(*)
                      FROM   migration_chunks c3
                      WHERE  c3.migration_id = c.migration_id
                        AND  c3.chunk_type   = c.chunk_type
                        AND  c3.status IN ('CLAIMED', 'RUNNING')
                  ) ASC,
                  m.state_changed_at ASC,
                  c.chunk_seq ASC
                FOR UPDATE OF c SKIP LOCKED
                LIMIT 1
            )
            UPDATE migration_chunks
            SET    status     = 'CLAIMED',
                   worker_id  = %s,
                   claimed_at = NOW(),
                   started_at = NOW()
            WHERE  chunk_id = (SELECT chunk_id FROM candidate)
            RETURNING chunk_id, migration_id, chunk_seq, rowid_start, rowid_end,
                      COALESCE(chunk_type, 'BULK'), rows_loaded, retry_count
        """, (WORKER_ID,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        (
            chunk_id,
            migration_id,
            chunk_seq,
            rowid_start,
            rowid_end,
            chunk_type,
            rows_loaded,
            retry_count,
        ) = row

        cur.execute("""
            SELECT source_connection_id, target_connection_id,
                   source_schema, source_table,
                   target_schema, target_table, stage_table_name,
                   start_scn, strategy
            FROM   migrations
            WHERE  migration_id = %s
        """, (migration_id,))
        mrow = cur.fetchone()
        if not mrow:
            conn.rollback()
            return None

        (src_conn_id, dst_conn_id,
         src_schema, src_table,
         tgt_schema, tgt_table, stage_table,
         start_scn, strategy) = mrow

    conn.commit()
    return {
        "chunk_id":             str(chunk_id),
        "migration_id":         str(migration_id),
        "chunk_seq":            chunk_seq,
        "chunk_type":           chunk_type,
        "rowid_start":          rowid_start,
        "rowid_end":            rowid_end,
        "rows_loaded":          rows_loaded,
        "retry_count":          retry_count,
        "source_connection_id": src_conn_id,
        "target_connection_id": dst_conn_id,
        "source_schema":        src_schema,
        "source_table":         src_table,
        "target_schema":        tgt_schema,
        "target_table":         tgt_table,
        "stage_table":          stage_table,
        "start_scn":            str(start_scn) if start_scn is not None else None,
        "strategy":             (strategy or "CDC_STAGE").upper(),
    }


def update_chunk_progress(conn, chunk_id: str, rows_loaded: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks
            SET    rows_loaded = %s, status = 'RUNNING'
            WHERE  chunk_id   = %s
        """, (rows_loaded, chunk_id))
    conn.commit()


def complete_chunk(conn, chunk_id: str, rows_loaded: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks
            SET    status       = 'DONE',
                   rows_loaded  = %s,
                   completed_at = NOW()
            WHERE  chunk_id     = %s
            RETURNING migration_id, COALESCE(chunk_type, 'BULK')
        """, (rows_loaded, chunk_id))
        row = cur.fetchone()
        if row:
            migration_id, chunk_type = row
            if chunk_type == 'BASELINE':
                cur.execute("""
                    UPDATE migrations
                    SET    baseline_chunks_done = baseline_chunks_done + 1,
                           rows_loaded          = rows_loaded + %s,
                           updated_at           = NOW()
                    WHERE  migration_id = %s
                """, (rows_loaded, migration_id))
            else:
                cur.execute("""
                    UPDATE migrations
                    SET    chunks_done  = chunks_done + 1,
                           rows_loaded  = rows_loaded + %s,
                           updated_at   = NOW()
                    WHERE  migration_id = %s
                """, (rows_loaded, migration_id))
    conn.commit()


def chunk_is_active(conn, chunk_id: str) -> bool:
    """Return False when a claimed/running chunk should stop before more Oracle work."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.status, m.phase
            FROM   migration_chunks c
            JOIN   migrations m ON m.migration_id = c.migration_id
            WHERE  c.chunk_id = %s
        """, (chunk_id,))
        row = cur.fetchone()
    if not row:
        return False
    status, phase = row
    return (
        status in ("CLAIMED", "RUNNING")
        and phase not in ("CANCELLING", "CANCELLED", "FAILED", "COMPLETED")
    )


def cancel_chunk(conn, chunk_id: str, reason: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks
            SET    status       = 'CANCELLED',
                   error_text   = %s,
                   completed_at = NOW()
            WHERE  chunk_id     = %s
              AND  status IN ('PENDING', 'CLAIMED', 'RUNNING')
        """, (reason[:2000], chunk_id))
    conn.commit()


def fail_chunk_permanent(conn, chunk_id: str, error_text: str) -> None:
    """Mark a chunk FAILED immediately, skipping retry logic.

    Use for errors where retrying with the same parameters is pointless
    (e.g. ORA-01555: snapshot too old — the SCN won't get newer on retry).
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks
            SET    status       = 'FAILED',
                   error_text   = %s,
                   completed_at = NOW()
            WHERE  chunk_id     = %s
            RETURNING migration_id
        """, (error_text[:2000], chunk_id))
        row = cur.fetchone()
        if row:
            cur.execute("""
                UPDATE migrations
                SET    chunks_failed = chunks_failed + 1,
                       updated_at    = NOW()
                WHERE  migration_id  = %s
            """, (row[0],))
    conn.commit()


def fail_chunk(conn, chunk_id: str, error_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT retry_count, migration_id
            FROM   migration_chunks
            WHERE  chunk_id = %s
            FOR UPDATE
        """, (chunk_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return
        retry_count, migration_id = row

        if retry_count < _MAX_RETRIES:
            cur.execute("""
                UPDATE migration_chunks
                SET    status      = 'PENDING',
                       worker_id   = NULL,
                       claimed_at  = NULL,
                       started_at  = NULL,
                       error_text  = %s,
                       retry_count = retry_count + 1
                WHERE  chunk_id    = %s
            """, (error_text[:2000], chunk_id))
        else:
            cur.execute("""
                UPDATE migration_chunks
                SET    status       = 'FAILED',
                       error_text   = %s,
                       completed_at = NOW()
                WHERE  chunk_id     = %s
            """, (error_text[:2000], chunk_id))
            cur.execute("""
                UPDATE migrations
                SET    chunks_failed = chunks_failed + 1,
                       updated_at    = NOW()
                WHERE  migration_id  = %s
            """, (migration_id,))
    conn.commit()


# ---------------------------------------------------------------------------
# CDC state
# ---------------------------------------------------------------------------

def claim_cdc_migration(conn, exclude_migration_ids: Optional[list[str]] = None) -> Optional[dict]:
    """
    Claim a CDC migration that has no active worker (heartbeat stale / absent).
    Immediately writes our heartbeat to prevent double-claiming.
    exclude_migration_ids prevents this manager from refreshing heartbeat for
    a CDC thread it already owns but that stopped checking in.
    Returns migration dict or None.
    """
    exclude_ids = [str(mid) for mid in (exclude_migration_ids or []) if mid]
    exclude_clause = ""
    params: list = [CDC_HEARTBEAT_STALE_MINUTES]
    if exclude_ids:
        exclude_clause = "AND NOT (m.migration_id = ANY(%s::uuid[]))"
        params.append(exclude_ids)

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT m.migration_id,
                   m.target_connection_id, m.target_schema, m.target_table,
                   m.source_schema, m.source_table,
                   m.topic_prefix, m.consumer_group,
                   m.effective_key_columns_json
            FROM   migrations m
            LEFT JOIN migration_cdc_state cs ON cs.migration_id = m.migration_id
            WHERE  m.phase IN ('CDC_APPLY_STARTING', 'CDC_APPLYING', 'CDC_CATCHING_UP', 'STEADY_STATE')
              AND  COALESCE(m.paused, FALSE) = FALSE
              AND  (
                     cs.worker_heartbeat IS NULL
                  OR cs.worker_heartbeat < NOW() - make_interval(mins => %s)
              )
              {exclude_clause}
            ORDER BY m.state_changed_at
            LIMIT 1
            FOR UPDATE OF m SKIP LOCKED
        """, tuple(params))
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return None

        keys = [
            "migration_id",
            "target_connection_id", "target_schema", "target_table",
            "source_schema", "source_table",
            "topic_prefix", "consumer_group",
            "effective_key_columns_json",
        ]
        migration = dict(zip(keys, row))
        topic = cdc_topic_name(
            migration["topic_prefix"],
            migration["source_schema"],
            migration["source_table"],
        )

        # Reserve: write our heartbeat immediately
        cur.execute("""
            INSERT INTO migration_cdc_state
                (migration_id, consumer_group, topic,
                 total_lag, worker_id, worker_heartbeat, updated_at)
            SELECT migration_id, consumer_group, %s,
                   0, %s, NOW(), NOW()
            FROM   migrations
            WHERE  migration_id = %s
            ON CONFLICT (migration_id) DO UPDATE
                SET worker_id        = EXCLUDED.worker_id,
                    topic            = EXCLUDED.topic,
                    worker_heartbeat = NOW(),
                    updated_at       = NOW()
        """, (topic, WORKER_ID, migration["migration_id"]))

    conn.commit()
    return migration


def cdc_migration_should_run(conn, migration_id: str) -> bool:
    """Return False when a CDC thread should stop at a safe boundary."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT phase, paused
            FROM   migrations
            WHERE  migration_id = %s
        """, (migration_id,))
        row = cur.fetchone()
    if not row:
        return False
    phase, paused = row
    return (
        not bool(paused)
        and phase in (
            "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP",
            "CDC_CAUGHT_UP", "STEADY_STATE",
        )
    )


def release_cdc_migration(conn, migration_id: str) -> None:
    """Release this worker's CDC heartbeat so the migration can be reclaimed."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_cdc_state
            SET    worker_id        = NULL,
                   worker_heartbeat = NULL,
                   updated_at       = NOW()
            WHERE  migration_id = %s
              AND  worker_id = %s
        """, (migration_id, WORKER_ID))
    conn.commit()


def cdc_checkin(conn, migration_id: str, total_lag: int,
                rows_applied: int, last_event_ts: Optional[str] = None,
                lag_by_partition: Optional[dict] = None) -> None:
    """Update CDC heartbeat + lag in state DB.

    *lag_by_partition* — словарь {"<topic>-<partition>": lag}; пишется в JSONB
    колонку migration_cdc_state.lag_by_partition, чтобы UI мог показать
    разбивку по топикам/партициям.
    """
    import json
    parts_json = json.dumps(lag_by_partition or {})
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO migration_cdc_state
                (migration_id, consumer_group, topic,
                 total_lag, lag_by_partition,
                 rows_applied, worker_id, worker_heartbeat, updated_at)
            SELECT migration_id,
                   consumer_group,
                   REPLACE(COALESCE(topic_prefix, '') || '.' ||
                           UPPER(source_schema) || '.' || UPPER(source_table), '#', '_'),
                   %s, %s::jsonb, %s, %s, NOW(), NOW()
            FROM   migrations
            WHERE  migration_id = %s
            ON CONFLICT (migration_id) DO UPDATE
                SET total_lag        = EXCLUDED.total_lag,
                    topic            = EXCLUDED.topic,
                    lag_by_partition = EXCLUDED.lag_by_partition,
                    rows_applied     = EXCLUDED.rows_applied,
                    worker_id        = EXCLUDED.worker_id,
                    worker_heartbeat = NOW(),
                    updated_at       = NOW()
        """, (total_lag, parts_json, rows_applied, WORKER_ID, migration_id))

        # Mirror lag onto migrations table so UI can read it
        cur.execute("""
            UPDATE migrations
            SET    kafka_lag            = %s,
                   kafka_lag_checked_at = NOW(),
                   updated_at          = NOW()
            WHERE  migration_id = %s
        """, (total_lag, migration_id))
    conn.commit()


def cdc_heartbeat(conn, migration_id: str) -> None:
    """Refresh CDC worker heartbeat without declaring lag as measured."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_cdc_state
            SET    worker_id        = %s,
                   worker_heartbeat = NOW(),
                   updated_at       = NOW()
            WHERE  migration_id = %s
        """, (WORKER_ID, migration_id))
    conn.commit()


def worker_heartbeat(conn, role: str = "universal", capabilities: Optional[list[str]] = None) -> None:
    """Publish process-level worker liveness for coordinator/UI diagnostics."""
    caps_json = json.dumps(capabilities or [])
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO worker_heartbeats
                (worker_id, role, capabilities, started_at, last_heartbeat)
            VALUES (%s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (worker_id) DO UPDATE
                SET role           = EXCLUDED.role,
                    capabilities   = EXCLUDED.capabilities,
                    last_heartbeat = NOW()
        """, (WORKER_ID, role, caps_json))
    conn.commit()


# ---------------------------------------------------------------------------
# Data comparison chunks
# ---------------------------------------------------------------------------

_MAX_COMPARE_RETRIES = 3


def claim_compare_chunk(conn) -> Optional[dict]:
    """Atomically claim one PENDING data-compare chunk.

    Returns chunk dict with task context, or None.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH candidate AS (
                SELECT c.chunk_id
                FROM   data_compare_chunks c
                JOIN   data_compare_tasks  t ON t.task_id = c.task_id
                LEFT JOIN migrations m ON m.data_compare_task_id = t.task_id
                WHERE  c.status = 'PENDING'
                  AND  t.status = 'RUNNING'
                  AND  COALESCE(m.paused, FALSE) = FALSE
                ORDER BY c.created_at, c.chunk_seq
                FOR UPDATE OF c SKIP LOCKED
                LIMIT 1
            )
            UPDATE data_compare_chunks
            SET    status     = 'CLAIMED',
                   worker_id  = %s,
                   claimed_at = NOW()
            WHERE  chunk_id = (SELECT chunk_id FROM candidate)
            RETURNING chunk_id, task_id, side, chunk_seq, rowid_start, rowid_end
        """, (WORKER_ID,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None

        chunk_id, task_id, side, chunk_seq, rowid_start, rowid_end = row

        cur.execute("""
            SELECT source_schema, source_table, target_schema, target_table
            FROM   data_compare_tasks
            WHERE  task_id = %s
        """, (task_id,))
        trow = cur.fetchone()
        if not trow:
            conn.rollback()
            return None

        src_schema, src_table, tgt_schema, tgt_table = trow

    conn.commit()

    schema = src_schema if side == "source" else tgt_schema
    table = src_table if side == "source" else tgt_table
    conn_id = "oracle_source" if side == "source" else "oracle_target"

    return {
        "chunk_id":      str(chunk_id),
        "task_id":       str(task_id),
        "side":          side,
        "chunk_seq":     chunk_seq,
        "rowid_start":   rowid_start,
        "rowid_end":     rowid_end,
        "schema":        schema,
        "table":         table,
        "connection_id": conn_id,
    }


def complete_compare_chunk(conn, chunk_id: str, row_count: int, hash_sum) -> str:
    """Mark a compare chunk DONE and increment task.chunks_done.
    Returns the task_id.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE data_compare_chunks
            SET    status       = 'DONE',
                   row_count    = %s,
                   hash_sum     = %s,
                   completed_at = NOW()
            WHERE  chunk_id     = %s
            RETURNING task_id
        """, (row_count, str(hash_sum) if hash_sum is not None else None, chunk_id))
        row = cur.fetchone()
        task_id = str(row[0]) if row else None

        if task_id:
            cur.execute("""
                UPDATE data_compare_tasks
                SET    chunks_done = chunks_done + 1
                WHERE  task_id = %s
            """, (task_id,))
    conn.commit()
    return task_id


def fail_compare_chunk(conn, chunk_id: str, error_text: str) -> Optional[str]:
    """Handle compare chunk failure with retry logic. Returns task_id."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT retry_count, task_id
            FROM   data_compare_chunks
            WHERE  chunk_id = %s
            FOR UPDATE
        """, (chunk_id,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        retry_count, task_id = row

        if retry_count < _MAX_COMPARE_RETRIES:
            cur.execute("""
                UPDATE data_compare_chunks
                SET    status      = 'PENDING',
                       worker_id   = NULL,
                       claimed_at  = NULL,
                       error_text  = %s,
                       retry_count = retry_count + 1
                WHERE  chunk_id    = %s
            """, (error_text[:2000], chunk_id))
        else:
            cur.execute("""
                UPDATE data_compare_chunks
                SET    status       = 'FAILED',
                       error_text   = %s,
                       completed_at = NOW()
                WHERE  chunk_id     = %s
            """, (error_text[:2000], chunk_id))
    conn.commit()
    return str(task_id)


# ---------------------------------------------------------------------------
# DDL apply jobs
# ---------------------------------------------------------------------------

def claim_ddl_apply_job(conn) -> Optional[dict]:
    """Atomically claim one PENDING ddl_apply_jobs row for this worker.

    Returns dict with job + schema-migration context (src/tgt schemas), or None.
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH candidate AS (
                SELECT j.job_id
                FROM   ddl_apply_jobs j
                WHERE  j.state = 'PENDING'
                ORDER BY j.created_at
                FOR UPDATE OF j SKIP LOCKED
                LIMIT 1
            )
            UPDATE ddl_apply_jobs
            SET    state      = 'CLAIMED',
                   worker_id  = %s,
                   claimed_at = NOW(),
                   started_at = NOW()
            WHERE  job_id = (SELECT job_id FROM candidate)
            RETURNING job_id, schema_migration_id, action, job_type,
                      object_type, object_name
        """, (WORKER_ID,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        job_id, sm_id, action, job_type, object_type, object_name = row

        cur.execute("""
            SELECT src_schema, tgt_schema
            FROM   schema_migrations
            WHERE  schema_migration_id = %s
        """, (sm_id,))
        srow = cur.fetchone()
        if not srow:
            conn.rollback()
            return None
        src_schema, tgt_schema = srow
    conn.commit()
    return {
        "job_id":              str(job_id),
        "schema_migration_id": str(sm_id),
        "action":              action,
        "job_type":            job_type,
        "object_type":         object_type,
        "object_name":         object_name,
        "src_schema":          src_schema,
        "tgt_schema":          tgt_schema,
    }


def complete_ddl_apply_job(conn, job_id: str, applied_ddl: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ddl_apply_jobs
            SET    state        = 'DONE',
                   applied_ddl  = %s,
                   completed_at = NOW()
            WHERE  job_id       = %s
        """, (applied_ddl, job_id))
    conn.commit()


def fail_ddl_apply_job(conn, job_id: str, error_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ddl_apply_jobs
            SET    state        = 'FAILED',
                   error_text   = %s,
                   completed_at = NOW()
            WHERE  job_id       = %s
        """, (error_text[:4000], job_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Target index jobs
# ---------------------------------------------------------------------------

def claim_target_index_job(conn) -> Optional[dict]:
    """Claim one target index/constraint enable job for this worker."""
    with conn.cursor() as cur:
        cur.execute("""
            WITH candidate AS (
                SELECT j.job_id
                FROM   target_index_jobs j
                JOIN   migrations m ON m.migration_id = j.migration_id
                WHERE  j.state = 'PENDING'
                  AND  COALESCE(m.paused, FALSE) = FALSE
                ORDER BY j.created_at
                FOR UPDATE OF j SKIP LOCKED
                LIMIT 1
            )
            UPDATE target_index_jobs
            SET    state      = 'RUNNING',
                   worker_id  = %s,
                   started_at = NOW(),
                   error_text = NULL
            WHERE  job_id = (SELECT job_id FROM candidate)
            RETURNING job_id, migration_id
        """, (WORKER_ID,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        job_id, migration_id = row

        cur.execute("""
            SELECT target_connection_id, target_schema, target_table
            FROM   migrations
            WHERE  migration_id = %s
        """, (migration_id,))
        mrow = cur.fetchone()
        if not mrow:
            conn.rollback()
            return None
        target_connection_id, target_schema, target_table = mrow
    conn.commit()
    return {
        "job_id": str(job_id),
        "migration_id": str(migration_id),
        "target_connection_id": target_connection_id,
        "target_schema": target_schema,
        "target_table": target_table,
    }


def complete_target_index_job(conn, job_id: str, result: dict) -> None:
    enabled = result.get("enabled") or {}
    enabled_count = (
        len(enabled.get("indexes") or [])
        + len(enabled.get("constraints") or [])
        + len(enabled.get("fk_novalidate") or [])
        + len(enabled.get("referencing_fk_novalidate") or [])
    )
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE target_index_jobs
            SET    state         = 'DONE',
                   enabled_count = %s,
                   result_json   = %s::jsonb,
                   completed_at  = NOW()
            WHERE  job_id        = %s
        """, (enabled_count, json.dumps(result), job_id))
    conn.commit()


def fail_target_index_job(conn, job_id: str, error_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE target_index_jobs
            SET    state        = 'FAILED',
                   error_text   = %s,
                   completed_at = NOW()
            WHERE  job_id       = %s
        """, (error_text[:4000], job_id))
    conn.commit()


def log_sm_event(
    conn, sm_id: str, event_type: str,
    *,
    object_type: str | None = None,
    object_name: str | None = None,
    level: str = "info",
    message: str = "",
    job_id: str | None = None,
) -> None:
    """Persist a schema-migration-scope event. Read by the dashboard via
    /api/schema-migrations/:id/events."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO schema_migration_events
                (schema_migration_id, event_type, object_type, object_name,
                 level, message, job_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (sm_id, event_type, object_type, object_name,
              level, message[:4000], job_id))
    conn.commit()


def fail_cdc_migration(conn, migration_id: str,
                       error_code: str, error_text: str) -> None:
    """Перевод CDC-миграции в FAILED + запись диагностики.

    Срабатывает только из CDC_* фаз — не трогает уже завершённые миграции.
    Освобождает heartbeat, чтобы менеджер не пытался reclaim.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migrations
            SET    phase            = 'FAILED',
                   error_code       = %s,
                   error_text       = %s,
                   state_changed_at = NOW(),
                   updated_at       = NOW()
            WHERE  migration_id = %s
              AND  phase IN ('CDC_APPLY_STARTING','CDC_APPLYING',
                             'CDC_CATCHING_UP','CDC_CAUGHT_UP','STEADY_STATE')
        """, (error_code[:64], error_text[:4000], migration_id))
        if cur.rowcount > 0:
            cur.execute("""
                WITH changed_items AS (
                    UPDATE migration_plan_items
                    SET    status = 'FAILED'
                    WHERE  migration_id = %s
                      AND  status <> 'FAILED'
                    RETURNING plan_id
                )
                UPDATE migration_plans p
                SET    status = 'FAILED'
                WHERE  p.plan_id IN (SELECT DISTINCT plan_id FROM changed_items)
            """, (migration_id,))
        cur.execute("""
            UPDATE migration_cdc_state
            SET    worker_id        = NULL,
                   worker_heartbeat = NULL,
                   updated_at       = NOW()
            WHERE  migration_id = %s
        """, (migration_id,))
    conn.commit()


def trigger_lag_zero(conn, migration_id: str) -> None:
    """
    Transition migration CDC_CATCHING_UP → CDC_CAUGHT_UP when lag reaches 0.
    Only acts if migration is currently in CDC_CATCHING_UP.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migrations
            SET    phase            = 'CDC_CAUGHT_UP',
                   state_changed_at = NOW(),
                   updated_at       = NOW()
            WHERE  migration_id = %s
              AND  phase = 'CDC_CATCHING_UP'
        """, (migration_id,))
    conn.commit()
