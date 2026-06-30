"""
Migration phase orchestrator — background thread that drives state transitions.

Each tick (every TICK_INTERVAL seconds) the orchestrator:
  1. Resets stale claimed/running chunks back to PENDING.
  2. Fetches all migrations in active phases.
  3. For each migration calls the appropriate phase handler.

Long-running operations (STAGE_VALIDATING, BASELINE_PUBLISHING) are executed
in per-migration daemon threads to avoid blocking the orchestrator loop.
"""

import json
import threading
import time
from datetime import datetime, timezone

# DB helpers
from db.state_db import (
    get_active_migrations,
    row_to_dict,
    transition_phase,
    update_migration_fields,
)

# Services
import services.debezium        as debezium
import services.oracle_scn      as oracle_scn
import services.oracle_stage    as oracle_stage
import services.oracle_chunker  as oracle_chunker
import services.oracle_baseline as oracle_baseline
import services.kafka_lag       as kafka_lag_svc
import services.validator       as validator
import services.job_queue       as job_queue
import services.kafka_topics    as kafka_topics
import services.connector_groups as connector_groups_svc
import services.target_trigger_jobs as target_trigger_jobs
import services.target_index_jobs as target_index_jobs
import db.oracle_browser        as oracle_browser
from services.strategy import Strategy

TICK_INTERVAL = 5  # seconds

# Phases that occupy a data-load slot. Bulk and CDC have independent slots, so
# a CDC table and a historical table can move in parallel. Post-load index
# rebuilds are worker jobs and do not keep the load slot busy.
#
# The slot covers only the phases that READ FROM SOURCE and therefore contend
# on the source Oracle: chunking (DBMS_PARALLEL_EXECUTE on source), bulk load
# (SELECT ... AS OF SCN), and stage validation (COUNT source AS OF SCN).
#
# Baseline (stage -> target) and stage-drop touch only the TARGET Oracle, so
# they are intentionally NOT in the slot: as soon as a migration reaches
# BASELINE_PUBLISHING the slot frees and the NEXT migration starts its bulk load
# from source. Source-read (next table) and target-write (current baseline) then
# overlap, since they hit different databases.
_HEAVY_PHASES = frozenset({
    "TOPIC_CREATING",
    "CHUNKING",
    "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
})

# Track migrations running in a dedicated thread (long-running phases)
_in_progress: set[str] = set()
_in_progress_lock = threading.Lock()

_state: dict = {}


# ---------------------------------------------------------------------------
# Init + start
# ---------------------------------------------------------------------------

def init(get_conn_fn, load_configs_fn, broadcast_fn) -> None:
    _state["get_conn"]      = get_conn_fn
    _state["load_configs"]  = load_configs_fn
    _state["broadcast"]     = broadcast_fn


_orchestrator_started = False


def is_running() -> bool:
    return _orchestrator_started


def start_orchestrator() -> None:
    global _orchestrator_started
    if _orchestrator_started:
        return
    _orchestrator_started = True

    def _run():
        time.sleep(3)
        while True:
            try:
                _tick()
            except Exception as exc:
                print(f"[orchestrator] tick error: {exc}")
            time.sleep(TICK_INTERVAL)

    threading.Thread(target=_run, daemon=True, name="orchestrator").start()
    print("[orchestrator] started")


# ---------------------------------------------------------------------------
# Main tick
# ---------------------------------------------------------------------------

def _tick() -> None:
    get_conn = _state["get_conn"]
    conn = get_conn()
    try:
        # Reset stale chunks first
        job_queue.reset_stale_chunks(conn)

        migrations = get_active_migrations(conn)
    finally:
        conn.close()

    for m in migrations:
        mid   = m["migration_id"]
        phase = m["phase"]
        try:
            _dispatch(mid, phase, m)
        except Exception as exc:
            print(f"[orchestrator] migration {mid} phase {phase} error: {exc}")
            _fail(mid, str(exc))

    # Drive connector group lifecycle
    _tick_groups()

    # Check connector group health
    _check_group_connectors()


def _dispatch(migration_id: str, phase: str, m: dict) -> None:
    handler = _PHASE_HANDLERS.get(phase)
    if handler:
        handler(migration_id, m)


# Phase handlers (group-based migrations only)
_PHASE_HANDLERS = {
    "NEW":                  lambda mid, m: _handle_new(mid, m),
    "TOPIC_CREATING":       lambda mid, m: _handle_topic_creating(mid, m),
    "CHUNKING":             lambda mid, m: _handle_chunking(mid, m),
    "BULK_LOADING":         lambda mid, m: _handle_bulk_loading(mid, m),
    "BULK_LOADED":          lambda mid, m: _handle_bulk_loaded(mid, m),
    "STAGE_VALIDATING":     lambda mid, m: _handle_stage_validating(mid, m),
    "STAGE_VALIDATED":      lambda mid, m: _handle_stage_validated(mid, m),
    "BASELINE_PUBLISHING":  lambda mid, m: _handle_baseline_publishing(mid, m),
    "BASELINE_LOADING":     lambda mid, m: _handle_baseline_loading(mid, m),
    "BASELINE_PUBLISHED":   lambda mid, m: _handle_baseline_published(mid, m),
    "STAGE_DROPPING":       lambda mid, m: _handle_stage_dropping(mid, m),
    "INDEXES_ENABLING":     lambda mid, m: _handle_indexes_enabling(mid, m),
    "DATA_VERIFYING":       lambda mid, m: _handle_data_verifying(mid, m),
    "DATA_MISMATCH":        lambda mid, m: _handle_data_mismatch(mid, m),
    "CDC_APPLYING":         lambda mid, m: _handle_cdc_applying(mid, m),
    "CDC_CATCHING_UP":      lambda mid, m: _handle_cdc_catching_up(mid, m),
    "CDC_CAUGHT_UP":        lambda mid, m: _handle_cdc_caught_up(mid, m),
    "STEADY_STATE":         lambda mid, m: _handle_steady_state(mid, m),
    "CANCELLING":           lambda mid, m: _handle_cancelling(mid, m),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _transition(migration_id: str, to_phase: str,
                message: str | None = None,
                error_code: str | None = None,
                error_text: str | None = None,
                extra_fields: dict | None = None) -> None:
    conn = _state["get_conn"]()
    try:
        from_phase = transition_phase(
            conn, migration_id, to_phase,
            message=message,
            error_code=error_code,
            error_text=error_text,
            extra_fields=extra_fields,
        )
        conn.commit()
    finally:
        conn.close()

    _state["broadcast"]({
        "type":         "migration_phase",
        "migration_id": migration_id,
        "from_phase":   from_phase,
        "phase":        to_phase,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })
    print(f"[orchestrator] {migration_id}: {from_phase} → {to_phase}"
          + (f" ({message})" if message else ""))


    _sync_plan_after_transition(migration_id, to_phase)


def _sync_plan_after_transition(migration_id: str, to_phase: str) -> None:
    """Keep migration_plan_items in step with child migrations."""
    if to_phase not in ("INDEXES_ENABLING", "COMPLETED", "STEADY_STATE", "DATA_MISMATCH", "FAILED", "CANCELLED"):
        return

    if to_phase in ("INDEXES_ENABLING", "COMPLETED", "STEADY_STATE"):
        item_status = "DONE"
    elif to_phase == "CANCELLED":
        item_status = "CANCELLED"
    else:
        item_status = "FAILED"

    started_ids: list[str] = []
    started_cdc_group_ids: set[str] = set()
    next_batch: int | None = None

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE migration_plan_items
                SET    status = %s
                WHERE  migration_id = %s
                  AND  status <> %s
                RETURNING plan_id, batch_order
            """, (item_status, migration_id, item_status))
            row = cur.fetchone()
            if not row:
                conn.commit()
                return

            plan_id, batch_order = row

            if item_status != "DONE":
                cur.execute("""
                    UPDATE migration_plans
                    SET    status = %s
                    WHERE  plan_id = %s
                """, (item_status, plan_id))
                conn.commit()
                return

            cur.execute("""
                SELECT COUNT(*)
                FROM   migration_plan_items
                WHERE  plan_id = %s
                  AND  batch_order = %s
                  AND  status <> 'DONE'
            """, (plan_id, batch_order))
            if (cur.fetchone()[0] or 0) > 0:
                conn.commit()
                return

            cur.execute("""
                SELECT batch_order
                FROM   migration_plan_items
                WHERE  plan_id = %s
                  AND  status = 'PENDING'
                GROUP  BY batch_order
                ORDER  BY batch_order
                LIMIT  1
            """, (plan_id,))
            pending = cur.fetchone()
            if not pending:
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (
                            WHERE status NOT IN ('DONE', 'FAILED', 'CANCELLED')
                        ) AS active_count,
                        COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_count,
                        COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled_count
                    FROM migration_plan_items
                    WHERE plan_id = %s
                """, (plan_id,))
                active_count, failed_count, cancelled_count = cur.fetchone()
                plan_status = _plan_status_without_pending(
                    active_count or 0,
                    failed_count or 0,
                    cancelled_count or 0,
                )
                cur.execute("""
                    UPDATE migration_plans
                    SET    status = %s
                    WHERE  plan_id = %s
                """, (plan_status, plan_id))
                conn.commit()
                return

            next_batch = pending[0]
            cur.execute("""
                SELECT i.item_id, i.migration_id, m.phase, m.group_id, m.strategy
                FROM   migration_plan_items i
                LEFT JOIN migrations m ON m.migration_id = i.migration_id
                WHERE  i.plan_id = %s
                  AND  i.batch_order = %s
                  AND  i.status = 'PENDING'
                ORDER  BY sort_order, item_id
            """, (plan_id, next_batch))
            items = cur.fetchall()

            now = datetime.now(timezone.utc).isoformat()
            for item_id, next_mid, phase, group_id, strategy in items:
                next_mid = str(next_mid)
                cur.execute("""
                    UPDATE migrations
                    SET    phase = 'NEW',
                           state_changed_at = %s,
                           updated_at = %s
                    WHERE  migration_id = %s
                      AND  phase = 'DRAFT'
                """, (now, now, next_mid))
                if cur.rowcount <= 0:
                    item_status = _plan_item_status_for_phase(phase)
                    if item_status:
                        cur.execute("""
                            UPDATE migration_plan_items
                            SET    status = %s
                            WHERE  item_id = %s
                              AND  status = 'PENDING'
                        """, (item_status, item_id))
                    continue

                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, 'DRAFT', 'NEW', %s, 'SYSTEM')
                """, (next_mid, f"Auto-started from pack queue (step {next_batch})"))

                cur.execute("""
                    UPDATE migration_plan_items
                    SET    status = 'RUNNING'
                    WHERE  item_id = %s
                      AND  status = 'PENDING'
                """, (item_id,))
                started_ids.append(next_mid)
                if str(strategy or "").startswith("CDC_") and group_id:
                    started_cdc_group_ids.add(str(group_id))

            cur.execute("""
                UPDATE migration_plans
                SET    status = 'RUNNING',
                       started_at = COALESCE(started_at, %s)
                WHERE  plan_id = %s
            """, (now, plan_id))

        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"[orchestrator] {migration_id}: plan sync failed: {exc}")
        return
    finally:
        conn.close()

    for mid in started_ids:
        _state["broadcast"]({
            "type": "migration_phase",
            "migration_id": mid,
            "phase": "NEW",
            "ts": datetime.utcnow().isoformat() + "Z",
        })
    if started_ids:
        print(
            f"[orchestrator] plan launch order {next_batch} auto-started: "
            f"{', '.join(started_ids)}"
        )
    for group_id in sorted(started_cdc_group_ids):
        _kick_new_migrations_for_group(group_id)


def _fail(migration_id: str, error_text: str, error_code: str = "ORCHESTRATOR_ERROR") -> None:
    try:
        _transition(
            migration_id, "FAILED",
            message=error_text[:500],
            error_code=error_code,
            error_text=error_text[:2000],
        )
    except Exception as exc:
        print(f"[orchestrator] could not set FAILED for {migration_id}: {exc}")


def _update(migration_id: str, fields: dict) -> None:
    conn = _state["get_conn"]()
    try:
        update_migration_fields(conn, migration_id, fields)
        conn.commit()
    finally:
        conn.close()


def _plan_item_status_for_phase(phase: str | None) -> str | None:
    phase = str(phase or "").upper()
    if phase in ("INDEXES_ENABLING", "COMPLETED", "STEADY_STATE"):
        return "DONE"
    if phase == "CANCELLED":
        return "CANCELLED"
    if phase == "FAILED":
        return "FAILED"
    if phase and phase != "DRAFT":
        return "RUNNING"
    return None


def _plan_status_without_pending(active_count: int, failed_count: int, cancelled_count: int) -> str:
    if active_count > 0:
        return "RUNNING"
    if failed_count > 0:
        return "FAILED"
    if cancelled_count > 0:
        return "CANCELLED"
    return "DONE"


def _configs() -> dict:
    return _state["load_configs"]()


def _oracle_cfg(connection_id: str) -> dict:
    return _configs().get(connection_id, {})


def _open_source_metadata_conn(connection_id: str):
    if connection_id == "oracle_source":
        return oracle_browser.get_oracle_conn("source", _configs(), prefer_owner=True)
    return oracle_scn.open_oracle_conn(_oracle_cfg(connection_id))


def _derive_source_key_from_info(info: dict) -> dict | None:
    pk_columns = info.get("pk_columns") or []
    uk_constraints = info.get("uk_constraints") or []
    if pk_columns:
        return {
            "source_pk_exists": True,
            "source_uk_exists": bool(uk_constraints),
            "effective_key_type": "PRIMARY_KEY",
            "effective_key_source": "PK",
            "effective_key_columns_json": json.dumps(pk_columns),
        }
    if uk_constraints:
        return {
            "source_pk_exists": False,
            "source_uk_exists": True,
            "effective_key_type": "UNIQUE_KEY",
            "effective_key_source": "UK",
            "effective_key_columns_json": json.dumps(uk_constraints[0].get("columns") or []),
        }
    return None


def _sync_group_table_key(m: dict, key_fields: dict) -> None:
    group_id = m.get("group_id")
    if not group_id:
        return
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE group_tables
                SET    source_pk_exists = %s,
                       source_uk_exists = %s,
                       effective_key_type = %s,
                       effective_key_columns_json = %s
                WHERE  group_id = %s
                  AND  UPPER(source_schema) = UPPER(%s)
                  AND  UPPER(source_table) = UPPER(%s)
            """, (
                bool(key_fields.get("source_pk_exists")),
                bool(key_fields.get("source_uk_exists")),
                key_fields.get("effective_key_type") or "NONE",
                key_fields.get("effective_key_columns_json") or "[]",
                group_id,
                m.get("source_schema") or "",
                m.get("source_table") or "",
            ))
        conn.commit()
    finally:
        conn.close()
    try:
        connector_groups_svc.refresh_connector_tables(str(group_id))
    except Exception as exc:
        print(f"[orchestrator] refresh_connector_tables warning: {exc}")


def _sync_cdc_runtime_context(mid: str, m: dict, group: dict) -> dict:
    """Keep CDC migration topic/consumer fields aligned with its connector group."""
    source_schema = (m.get("source_schema") or "").strip().upper()
    source_table = (m.get("source_table") or "").strip().upper()
    if not source_schema or not source_table:
        return m

    prefix = group.get("consumer_group_prefix") or group.get("topic_prefix") or ""
    fields = {
        "connector_name": connector_groups_svc._active_connector_name(group),
        "topic_prefix": connector_groups_svc._active_topic_prefix(group),
        "consumer_group": f"{prefix}_{source_schema}_{source_table}",
    }
    changes = {
        key: value
        for key, value in fields.items()
        if value and (m.get(key) or "") != value
    }
    if not changes:
        return m

    _update(mid, changes)
    return {**m, **changes}


def _try_infer_cdc_key(mid: str, m: dict) -> dict | None:
    source_schema = (m.get("source_schema") or "").strip().upper()
    source_table = (m.get("source_table") or "").strip().upper()
    source_connection_id = m.get("source_connection_id") or "oracle_source"
    if not source_schema or not source_table:
        return None
    conn = None
    try:
        conn = _open_source_metadata_conn(source_connection_id)
        info = oracle_browser.get_table_info(conn, source_schema, source_table)
        key_fields = _derive_source_key_from_info(info)
        if not key_fields:
            return None
        _update(mid, key_fields)
        _sync_group_table_key(m, key_fields)
        return key_fields
    except Exception as exc:
        print(f"[orchestrator] could not infer CDC key for {source_schema}.{source_table}: {exc}")
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _current_phase(migration_id: str) -> str | None:
    """Read the current phase from the DB.  Used by threaded handlers to
    detect if the user cancelled while they were running."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT phase FROM migrations WHERE migration_id = %s",
                (migration_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _safe_transition(migration_id: str, expected_phase: str, to_phase: str,
                     **kwargs) -> bool:
    """Transition only if the migration is still in *expected_phase*.
    Returns True if transitioned, False if phase changed (e.g. cancelled)."""
    cur = _current_phase(migration_id)
    if cur != expected_phase:
        print(f"[orchestrator] {migration_id}: skip transition {expected_phase}→{to_phase}, "
              f"current phase is {cur} (cancelled?)")
        return False
    _transition(migration_id, to_phase, **kwargs)
    return True


def _in_prog(migration_id: str) -> bool:
    with _in_progress_lock:
        return migration_id in _in_progress


def _mark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.add(migration_id)


def _unmark_in_prog(migration_id: str) -> None:
    with _in_progress_lock:
        _in_progress.discard(migration_id)


def _update_queue_positions() -> None:
    """Recalculate queue_position for runnable migrations waiting in NEW.

    CDC migrations whose connector is not RUNNING are not in the load queue
    yet. They wait for the connector lifecycle and must not block other NEW
    migrations. CDC and non-CDC migrations use independent load slots.
    """
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH active_slots AS (
                    SELECT
                        EXISTS (
                            SELECT 1 FROM migrations
                            WHERE phase = ANY(%s)
                              AND LEFT(COALESCE(strategy, ''), 4) <> 'CDC_'
                        ) AS bulk_busy,
                        EXISTS (
                            SELECT 1 FROM migrations
                            WHERE phase = ANY(%s)
                              AND LEFT(COALESCE(strategy, ''), 4) = 'CDC_'
                        ) AS cdc_busy
                ),
                candidates AS (
                    SELECT m.migration_id,
                           LEFT(COALESCE(m.strategy, ''), 4) = 'CDC_' AS is_cdc,
                           ROW_NUMBER() OVER (
                               PARTITION BY (LEFT(COALESCE(m.strategy, ''), 4) = 'CDC_')
                               ORDER BY m.state_changed_at ASC
                           ) AS pos
                    FROM   migrations m
                    LEFT JOIN connector_groups cg ON cg.group_id = m.group_id
                    WHERE  m.phase = 'NEW'
                      AND  (
                            LEFT(COALESCE(m.strategy, ''), 4) <> 'CDC_'
                            OR cg.status = 'RUNNING'
                      )
                ),
                desired AS (
                    SELECT c.migration_id,
                           CASE
                             WHEN c.is_cdc AND a.cdc_busy THEN c.pos
                             WHEN NOT c.is_cdc AND a.bulk_busy THEN c.pos
                             WHEN c.pos = 1 THEN NULL
                             ELSE c.pos - 1
                           END AS queue_position
                    FROM candidates c
                    CROSS JOIN active_slots a
                )
                UPDATE migrations m
                SET    queue_position = desired.queue_position
                FROM   desired
                WHERE  m.migration_id = desired.migration_id
                  AND  m.queue_position IS DISTINCT FROM desired.queue_position
            """, (list(_HEAVY_PHASES), list(_HEAVY_PHASES)))

            # Clear stale queue_position on non-NEW or non-runnable NEW migrations.
            cur.execute("""
                UPDATE migrations m
                SET    queue_position = NULL
                WHERE  (
                         m.phase != 'NEW'
                         OR NOT EXISTS (
                             SELECT 1
                             FROM   connector_groups cg
                             WHERE  cg.group_id = m.group_id
                               AND  cg.status = 'RUNNING'
                         )
                            AND LEFT(COALESCE(m.strategy, ''), 4) = 'CDC_'
                       )
                  AND  queue_position IS NOT NULL
            """)
        conn.commit()
    finally:
        conn.close()


def _kick_new_migrations_for_group(group_id: str) -> None:
    """Try to process the first NEW CDC migration for a just-running group."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.*
                FROM   migrations m
                WHERE  m.group_id = %s
                  AND  m.phase = 'NEW'
                  AND  LEFT(COALESCE(m.strategy, ''), 4) = 'CDC_'
                ORDER BY m.state_changed_at ASC
                LIMIT  1
            """, (group_id,))
            rows = [row_to_dict(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()

    for m in rows:
        _handle_new(m["migration_id"], m)


def _prepare_target_for_direct_load(mid: str, m: dict, dst_cfg: dict, message_parts: list[str]) -> None:
    """Prepare target table for a DIRECT bulk load.

    STAGE strategies do the same target preparation later in
    BASELINE_PUBLISHING, immediately before publishing stage into target.
    """
    tgt_schema = m["target_schema"]
    tgt_table = m["target_table"]
    tgt_quoted = f'"{tgt_schema.upper()}"."{tgt_table.upper()}"'

    conn = oracle_scn.open_oracle_conn(dst_cfg)
    try:
        with conn.cursor() as cur:
            if m.get("truncate_target", True):
                truncate_result = oracle_browser.truncate_table_for_load(
                    conn, tgt_schema, tgt_table,
                )
                disabled_fk = truncate_result.get("referencing_fk") or []
                disabled_keys = truncate_result.get("key_constraints") or []
                if disabled_fk:
                    message_parts.append(f"referencing FKs disabled={len(disabled_fk)}")
                    print(f"[orchestrator] {mid}: disabled referencing FKs: {disabled_fk}")
                if disabled_keys:
                    message_parts.append(f"key constraints disabled={len(disabled_keys)}")
                    print(f"[orchestrator] {mid}: disabled key constraints CASCADE: {disabled_keys}")
                message_parts.append("target truncated")
                print(f"[orchestrator] {mid}: truncated {tgt_quoted}")
        conn.commit()

        rebuilt = oracle_browser.rebuild_unusable_constraint_indexes(
            conn, tgt_schema, tgt_table,
        )
        if rebuilt:
            message_parts.append(f"constraint indexes rebuilt={len(rebuilt)}")
            print(f"[orchestrator] {mid}: rebuilt UNUSABLE constraint indexes: {rebuilt}")

        marked = oracle_browser.mark_indexes_unusable(
            conn, tgt_schema, tgt_table, skip_pk=True,
        )
        if marked:
            message_parts.append(f"indexes unusable={len(marked)}")
            print(f"[orchestrator] {mid}: marked UNUSABLE: {marked}")

        disabled_trg = oracle_browser.disable_triggers(conn, tgt_schema, tgt_table)
        if disabled_trg:
            message_parts.append(f"triggers disabled={len(disabled_trg)}")
            print(f"[orchestrator] {mid}: disabled triggers: {disabled_trg}")

        oracle_browser.set_table_logging(conn, tgt_schema, tgt_table, nologging=True)
        message_parts.append("target NOLOGGING")
        print(f"[orchestrator] {mid}: set NOLOGGING on {tgt_quoted}")
    finally:
        conn.close()


def _ensure_trigger_job(mid: str, requested_by: str = "orchestrator") -> None:
    conn = _state["get_conn"]()
    try:
        job = target_trigger_jobs.ensure_pending_job(conn, mid, requested_by=requested_by)
        _broadcast_trigger_job_created(job)
    except ValueError as exc:
        print(f"[orchestrator] {mid}: trigger job not created: {exc}")
    except Exception as exc:
        print(f"[orchestrator] {mid}: trigger job create warning: {exc}")
    finally:
        conn.close()


def _broadcast_trigger_job_created(job: dict) -> None:
    if not job.get("created"):
        return
    try:
        _broadcast({
            "type": "target_trigger_job",
            "migration_id": job["migration_id"],
            "job_id": job["job_id"],
            "state": job["state"],
            "enabled_count": job.get("enabled_count") or 0,
            "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        })
    except Exception as exc:
        print(f"[orchestrator] trigger job broadcast warning: {exc}")


# ---------------------------------------------------------------------------
# Phase handlers
# ---------------------------------------------------------------------------

def _create_chunks_and_transition(mid: str, m: dict) -> None:
    """Create ROWID chunks and transition to CHUNKING."""
    if _in_prog(mid):
        return

    # Check if chunks already created
    conn = _state["get_conn"]()
    try:
        stats = job_queue.get_chunk_stats(conn, mid)
    finally:
        conn.close()

    if stats["total"] > 0:
        _update(mid, {"total_chunks": stats["total"]})
        _transition(mid, "CHUNKING",
                    message=f"Чанки уже созданы ({stats['total']}), переход к нарезке")
        return

    _mark_in_prog(mid)

    def _run():
        try:
            src_cfg = _oracle_cfg(m["source_connection_id"])
            chunks = oracle_chunker.create_chunks(
                src_cfg,
                m["source_schema"],
                m["source_table"],
                int(m.get("chunk_size") or 100_000),
                mid,
            )
            if not chunks:
                # Table might genuinely be empty — check before failing
                src_conn = oracle_scn.open_oracle_conn(src_cfg)
                try:
                    with src_conn.cursor() as cur:
                        cur.execute(
                            f'SELECT 1 FROM "{m["source_schema"].upper()}"'
                            f'."{m["source_table"].upper()}" WHERE ROWNUM = 1'
                        )
                        has_rows = cur.fetchone() is not None
                finally:
                    src_conn.close()

                if has_rows:
                    _fail(mid,
                          "DBMS_PARALLEL_EXECUTE вернул 0 чанков, но таблица не пуста — "
                          "проверьте привилегии EXECUTE ON DBMS_PARALLEL_EXECUTE",
                          "NO_CHUNKS")
                    return

                # Source table is genuinely empty — skip bulk loading,
                # go straight to enabling indexes and then CDC listener
                _update(mid, {"total_chunks": 0})
                _transition(mid, "INDEXES_ENABLING",
                            message="Таблица-источник пуста (0 строк), "
                                    "пропуск bulk-загрузки — включение индексов и запуск CDC")
                return

            pg_conn = _state["get_conn"]()
            try:
                job_queue.save_chunks(pg_conn, mid, chunks)
            finally:
                pg_conn.close()

            _update(mid, {"total_chunks": len(chunks)})
            _transition(mid, "CHUNKING",
                        message=f"Создано {len(chunks)} чанков")
        except Exception as exc:
            _fail(mid, str(exc), "CHUNKING_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_chunking(mid: str, m: dict) -> None:
    """Chunks are written — transition to BULK_LOADING."""
    _transition(mid, "BULK_LOADING",
                message="Чанки записаны, запуск bulk-загрузки")


def _handle_bulk_loading(mid: str, m: dict) -> None:
    """Monitor chunk completion."""
    conn = _state["get_conn"]()
    try:
        stats = job_queue.get_chunk_stats(conn, mid)
    finally:
        conn.close()

    total   = stats["total"]
    done    = stats["done"]
    failed  = stats["failed"]
    active  = stats["claimed"] + stats["running"] + stats["pending"]

    _state["broadcast"]({
        "type":         "chunk_progress",
        "migration_id": mid,
        "chunks_done":  done,
        "total_chunks": total,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })

    if total == 0:
        return  # Chunks not yet written

    if done == total:
        _transition(mid, "BULK_LOADED",
                    message=f"Все {total} чанков загружены")
        return

    if failed > 0 and active == 0 and (done + failed) == total:
        _fail(mid,
              f"Bulk load завершился с ошибками: {failed} чанков не удалось загрузить",
              "BULK_LOAD_FAILED")


def _handle_bulk_loaded(mid: str, m: dict) -> None:
    try:
        strategy = Strategy.parse(m.get("strategy"))
    except ValueError:
        _fail(mid, f"Неизвестная стратегия: {m.get('strategy')!r}", "UNKNOWN_STRATEGY")
        return
    if not strategy.uses_stage:
        _transition(mid, "INDEXES_ENABLING",
                    message="DIRECT: данные загружены напрямую, включение индексов")
    else:
        _transition(mid, "STAGE_VALIDATING")


def _handle_stage_validating(mid: str, m: dict) -> None:
    """Run validation in a separate thread."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            src_cfg = _oracle_cfg(m["source_connection_id"])
            dst_cfg = _oracle_cfg(m["target_connection_id"])
            result  = validator.validate_stage(m, src_cfg, dst_cfg)
            _update(mid, {"validation_result": json.dumps(result.to_dict())})

            if result.ok:
                _safe_transition(mid, "STAGE_VALIDATING", "STAGE_VALIDATED",
                                 message=result.message)
            else:
                if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                    _fail(mid, result.message, "VALIDATION_FAILED")
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "VALIDATION_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_stage_validated(mid: str, m: dict) -> None:
    _transition(mid, "BASELINE_PUBLISHING")


def _handle_baseline_publishing(mid: str, m: dict) -> None:
    """
    TRUNCATE target, chunk the stage table on the target Oracle, store
    chunks (chunk_type='BASELINE') in migration_chunks, then move to
    BASELINE_LOADING so workers pick them up.
    """
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            dst_cfg    = _oracle_cfg(m["target_connection_id"])
            tgt_schema = m["target_schema"]
            tgt_table  = m["target_table"]
            stg_table  = m["stage_table_name"]
            chunk_size = int(m.get("baseline_batch_size") or 500_000)

            # Wait for in-flight BASELINE chunks to drain (restart scenario).
            # Phase is already BASELINE_PUBLISHING so workers won't claim new
            # chunks; we just need active ones to finish before TRUNCATE.
            pg_conn = _state["get_conn"]()
            try:
                while True:
                    with pg_conn.cursor() as cur:
                        cur.execute("""
                            SELECT COUNT(*) FROM migration_chunks
                            WHERE  migration_id = %s
                              AND  chunk_type = 'BASELINE'
                              AND  status IN ('CLAIMED', 'RUNNING')
                        """, (mid,))
                        active = cur.fetchone()[0]
                    if active == 0:
                        break
                    print(f"[baseline_publishing] waiting for {active} in-flight chunks to drain…")
                    time.sleep(3)
            finally:
                pg_conn.close()

            # Delete old BASELINE chunks (DONE/FAILED/CANCELLED from previous run)
            pg_conn = _state["get_conn"]()
            try:
                with pg_conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM migration_chunks
                        WHERE  migration_id = %s AND chunk_type = 'BASELINE'
                    """, (mid,))
                pg_conn.commit()
            finally:
                pg_conn.close()

            # TRUNCATE target table before loading so retries start from a clean slate
            tgt_quoted = f'"{tgt_schema.upper()}"."{tgt_table.upper()}"'
            conn = oracle_scn.open_oracle_conn(dst_cfg)
            try:
                truncate_result = oracle_browser.truncate_table_for_load(
                    conn, tgt_schema, tgt_table,
                )
                disabled_fk = truncate_result.get("referencing_fk") or []
                disabled_keys = truncate_result.get("key_constraints") or []
                if disabled_fk:
                    print(f"[baseline_publishing] disabled referencing FKs: {disabled_fk}")
                if disabled_keys:
                    print(f"[baseline_publishing] disabled key constraints CASCADE: {disabled_keys}")
                print(f"[baseline_publishing] truncated {tgt_quoted}")

                # Recover PK/UK-backing indexes left UNUSABLE by a previous failed
                # attempt.  ORA-26026 is raised on INSERT if such an index is UNUSABLE.
                rebuilt = oracle_browser.rebuild_unusable_constraint_indexes(
                    conn, tgt_schema, tgt_table,
                )
                if rebuilt:
                    print(f"[baseline_publishing] rebuilt UNUSABLE constraint indexes: {rebuilt}")

                # Mark secondary (non-unique) indexes UNUSABLE so Oracle skips
                # index maintenance during INSERT — rebuilt by INDEXES_ENABLING.
                marked = oracle_browser.mark_indexes_unusable(
                    conn, tgt_schema, tgt_table, skip_pk=True,
                )
                if marked:
                    print(f"[baseline_publishing] marked UNUSABLE: {marked}")

                # Disable triggers so they don't fire on every INSERT row during
                # baseline load.  Re-enabled manually via "Включить триггеры"
                # button (enable_triggers API) after CDC apply catches up.
                disabled_trg = oracle_browser.disable_triggers(
                    conn, tgt_schema, tgt_table,
                )
                if disabled_trg:
                    print(f"[baseline_publishing] disabled triggers: {disabled_trg}")

                # Switch to NOLOGGING so direct-path INSERTs (APPEND hint) skip
                # redo generation.  Restored to LOGGING in INDEXES_ENABLING.
                oracle_browser.set_table_logging(conn, tgt_schema, tgt_table, nologging=True)
                print(f"[baseline_publishing] set NOLOGGING on {tgt_quoted}")
            finally:
                conn.close()

            # Create BASELINE chunks from stage table on target
            task_id = f"BAS_{m['migration_id']}"
            chunks = oracle_chunker.create_chunks(
                dst_cfg, tgt_schema, stg_table, chunk_size, task_id
            )

            if not chunks:
                # Stage is empty — skip straight to BASELINE_PUBLISHED
                _update(mid, {"baseline_chunks_total": 0, "baseline_chunks_done": 0})
                _transition(mid, "BASELINE_PUBLISHED",
                            message="Stage таблица пуста — целевая таблица обнулена")
                return

            # 3. Store as BASELINE chunks in migration_chunks
            pg_conn = _state["get_conn"]()
            try:
                with pg_conn.cursor() as cur:
                    for ch in chunks:
                        cur.execute("""
                            INSERT INTO migration_chunks
                                (migration_id, chunk_seq, rowid_start, rowid_end, chunk_type)
                            VALUES (%s, %s, %s, %s, 'BASELINE')
                            ON CONFLICT (migration_id, chunk_type, chunk_seq) DO NOTHING
                        """, (mid, ch.chunk_seq, ch.rowid_start, ch.rowid_end))
                pg_conn.commit()
            finally:
                pg_conn.close()

            _update(mid, {
                "baseline_chunks_total": len(chunks),
                "baseline_chunks_done":  0,
            })
            _safe_transition(mid, "BASELINE_PUBLISHING", "BASELINE_LOADING",
                            message=f"Создано {len(chunks)} baseline-чанков, запуск воркеров")
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "BASELINE_PUBLISH_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_baseline_loading(mid: str, m: dict) -> None:
    """Monitor BASELINE chunk completion — analogous to _handle_bulk_loading."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status, COUNT(*)
                FROM   migration_chunks
                WHERE  migration_id = %s AND chunk_type = 'BASELINE'
                GROUP BY status
            """, (mid,))
            counts = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()

    total   = sum(counts.values())
    done    = counts.get("DONE",    0)
    failed  = counts.get("FAILED",  0)
    active  = counts.get("CLAIMED", 0) + counts.get("RUNNING", 0) + counts.get("PENDING", 0)

    _state["broadcast"]({
        "type":                  "baseline_progress",
        "migration_id":          mid,
        "baseline_chunks_done":  done,
        "baseline_chunks_total": total,
        "ts":                    datetime.utcnow().isoformat() + "Z",
    })

    if total == 0:
        return

    _update(mid, {"baseline_chunks_done": done})

    if done == total:
        _transition(mid, "BASELINE_PUBLISHED",
                    message=f"Все {total} baseline-чанков загружены")
        return

    if failed > 0 and active == 0 and (done + failed) == total:
        _fail(mid,
              f"Baseline loading завершился с ошибками: {failed}/{total} чанков не удалось",
              "BASELINE_LOAD_FAILED")


def _handle_baseline_published(mid: str, m: dict) -> None:
    _transition(mid, "STAGE_DROPPING",
                message="Удаление stage-таблицы")


def _handle_stage_dropping(mid: str, m: dict) -> None:
    """Drop the stage table — runs in a thread."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            dst_cfg = _oracle_cfg(m["target_connection_id"])
            oracle_stage.drop_stage_table(
                dst_cfg, m["target_schema"], m["stage_table_name"],
            )
            _safe_transition(mid, "STAGE_DROPPING", "INDEXES_ENABLING",
                            message="Stage-таблица удалена, включение индексов и прочих объектов")
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "STAGE_DROP_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_cancelling(mid: str, m: dict) -> None:
    """Wait for any in-flight thread to finish, then transition to CANCELLED."""
    if _in_prog(mid):
        return  # thread still running — wait for next tick
    _transition(mid, "CANCELLED", message="Миграция отменена")


# ---------------------------------------------------------------------------
# Public API — manual triggers
# ---------------------------------------------------------------------------

def trigger_indexes_enabling(migration_id: str) -> None:
    """Called by the API endpoint when the user clicks 'Enable Indexes'.

    Accepts migrations in INDEXES_ENABLING phase.  Also accepts FAILED
    migrations whose error_code is INDEXES_ENABLE_ERROR so the user can
    recover a stuck migration without manual DB intervention — the migration
    is transitioned back to INDEXES_ENABLING before the handler runs.
    """
    get_conn = _state["get_conn"]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM migrations WHERE migration_id = %s",
                (migration_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Migration {migration_id} not found")
            m = row_to_dict(cur, row)
    finally:
        conn.close()

    phase = m["phase"]
    if phase == "FAILED" and m.get("error_code") == "INDEXES_ENABLE_ERROR":
        # Recovery path: transition back to INDEXES_ENABLING, then re-fetch
        _transition(migration_id, "INDEXES_ENABLING",
                    message="Повторный запуск пересчёта индексов")
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                m = row_to_dict(cur, cur.fetchone())
        finally:
            conn.close()
    elif phase != "INDEXES_ENABLING":
        raise ValueError(
            f"Migration is in phase {phase}, expected INDEXES_ENABLING"
        )
    conn = get_conn()
    try:
        job = target_index_jobs.ensure_pending_job(
            conn, migration_id, requested_by="user", force_new=True,
        )
    finally:
        conn.close()
    if job.get("created"):
        _broadcast({
            "type": "target_index_job",
            "migration_id": migration_id,
            "job_id": job["job_id"],
            "state": job.get("state"),
            "enabled_count": job.get("enabled_count") or 0,
            "ts": datetime.utcnow().isoformat() + "Z",
        })
    _update_queue_positions()


def list_trigger_jobs(migration_id: str) -> list[dict]:
    conn = _state["get_conn"]()
    try:
        return target_trigger_jobs.list_jobs(conn, migration_id)
    finally:
        conn.close()


def create_trigger_job(migration_id: str, requested_by: str | None = None) -> dict:
    conn = _state["get_conn"]()
    try:
        job = target_trigger_jobs.ensure_pending_job(
            conn,
            migration_id,
            requested_by=requested_by or "user",
        )
    finally:
        conn.close()
    _broadcast_trigger_job_created(job)
    return job


def run_trigger_job(migration_id: str, job_id: str) -> dict:
    return target_trigger_jobs.run_job_async(
        get_conn_fn=_state["get_conn"],
        load_configs_fn=_state["load_configs"],
        broadcast_fn=_state["broadcast"],
        migration_id=migration_id,
        job_id=job_id,
    )


def trigger_enable_triggers(migration_id: str) -> dict:
    """Compatibility shortcut: create a pending job and start it."""
    job = create_trigger_job(migration_id, requested_by="user")
    if job["state"] == "PENDING":
        return run_trigger_job(migration_id, job["job_id"])
    return job


def trigger_baseline_restart(migration_id: str) -> None:
    """Restart the baseline phase from scratch.

    Allowed from FAILED (baseline-related errors) or BASELINE_LOADING
    (e.g. when chunks are stuck or ORA-26026 occurred).

    1. Cancel PENDING chunks (prevent new claims).
    2. Transition to BASELINE_PUBLISHING (workers won't claim from this phase).
    3. _handle_baseline_publishing will wait for in-flight chunks to drain,
       delete old chunks, TRUNCATE, rebuild unique indexes, re-chunk, and load.
    """
    _ALLOWED_ERRORS = {"BASELINE_PUBLISH_ERROR", "BASELINE_LOAD_FAILED"}
    _ALLOWED_PHASES = {"BASELINE_LOADING"}

    get_conn = _state["get_conn"]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM migrations WHERE migration_id = %s",
                (migration_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Migration {migration_id} not found")
            m = row_to_dict(cur, row)
    finally:
        conn.close()

    phase = m["phase"]
    ok = (
        phase in _ALLOWED_PHASES
        or (phase == "FAILED" and m.get("error_code") in _ALLOWED_ERRORS)
    )
    if not ok:
        raise ValueError(
            f"Нельзя перезапустить baseline из фазы {phase}"
            + (f" (error_code={m.get('error_code')})" if phase == "FAILED" else "")
        )

    # Cancel PENDING chunks immediately so workers don't pick them up
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE migration_chunks
                SET    status = 'CANCELLED'
                WHERE  migration_id = %s
                  AND  chunk_type = 'BASELINE'
                  AND  status = 'PENDING'
            """, (migration_id,))
        conn.commit()
    finally:
        conn.close()

    # Transition away from BASELINE_LOADING — workers won't claim new chunks.
    # _handle_baseline_publishing will drain in-flight chunks before TRUNCATE.
    _transition(
        migration_id, "BASELINE_PUBLISHING",
        message="Перезапуск baseline: ожидание завершения активных воркеров…",
        extra_fields={"error_code": None, "error_text": None},
    )
    print(f"[orchestrator] {migration_id}: baseline restart triggered")


def _handle_cdc_catching_up(mid: str, m: dict) -> None:
    """Sync lag from migration_cdc_state; lag=0 → CDC_CAUGHT_UP."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT total_lag, lag_by_partition, updated_at
                FROM   migration_cdc_state
                WHERE  migration_id = %s
            """, (m["migration_id"],))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return

    lag, lag_by_partition, updated_at = int(row[0] or 0), row[1], row[2]
    if lag_by_partition is None:
        return

    _update(mid, {"kafka_lag": lag, "kafka_lag_checked_at": updated_at})
    _state["broadcast"]({
        "type":         "kafka_lag",
        "migration_id": mid,
        "total_lag":    lag,
        "updated_at":   updated_at.isoformat() + "Z" if updated_at else None,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })

    if lag == 0:
        _transition(mid, "CDC_CAUGHT_UP",
                    message="Kafka consumer group lag = 0")


def _handle_cdc_caught_up(mid: str, m: dict) -> None:
    transitioned = _safe_transition(
        mid,
        "CDC_CAUGHT_UP",
        "STEADY_STATE",
        message="Миграция догнала источник",
    )
    if transitioned:
        _ensure_trigger_job(mid)


def _handle_steady_state(mid: str, m: dict) -> None:
    """Propagate lag updates to the UI while in steady state."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT total_lag, worker_heartbeat, updated_at
                FROM   migration_cdc_state
                WHERE  migration_id = %s
            """, (m["migration_id"],))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return

    lag, heartbeat, updated_at = row
    lag = int(lag or 0)
    _update(mid, {"kafka_lag": lag, "kafka_lag_checked_at": updated_at})
    _state["broadcast"]({
        "type":         "kafka_lag",
        "migration_id": mid,
        "total_lag":    lag,
        "updated_at":   updated_at.isoformat() + "Z" if updated_at else None,
        "ts":           datetime.utcnow().isoformat() + "Z",
    })


# ---------------------------------------------------------------------------
# Phase handlers
# ---------------------------------------------------------------------------

_CDC_GROUP_AUTOSTART_STATUSES = {"PENDING", "STOPPED"}


def _ensure_cdc_group_runtime_progress(mid: str, group_id: str, group: dict) -> None:
    """Best-effort nudge for CDC runtime while table migration waits in NEW."""
    status = group.get("status")
    try:
        if status in _CDC_GROUP_AUTOSTART_STATUSES:
            result = connector_groups_svc.request_start(group_id)
            status = result.get("status") or status
            print(
                f"[orchestrator] {mid}: requested CDC pack {group_id} start "
                f"while waiting in NEW; status={status}"
            )
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id,
                "status": status,
            })

        kicked = kick_connector_group_lifecycle(group_id)
        print(
            f"[orchestrator] {mid}: waiting for CDC pack {group_id} "
            f"to become RUNNING; status={status}, lifecycle_kicked={kicked}"
        )
    except Exception as exc:
        print(
            f"[orchestrator] {mid}: CDC pack {group_id} runtime kick warning: {exc}"
        )


def _sync_cdc_group_runtime_status(mid: str, group_id: str, group: dict) -> dict:
    """Refresh CDC pack status from Kafka Connect before keeping a table in NEW."""
    if group.get("status") == "RUNNING":
        return group
    try:
        runtime_status = connector_groups_svc.get_connector_status(group_id)
    except Exception as exc:
        print(
            f"[orchestrator] {mid}: CDC pack {group_id} runtime status check failed: {exc}"
        )
        return group

    if runtime_status != "RUNNING":
        print(
            f"[orchestrator] {mid}: CDC pack {group_id} DB status={group.get('status')}, "
            f"Kafka Connect status={runtime_status}; migration remains NEW"
        )
        return group

    refreshed = connector_groups_svc.get_group(group_id) or group
    if refreshed.get("status") != "RUNNING":
        try:
            connector_groups_svc.transition_group(
                group_id,
                "RUNNING",
                "CDC runtime already RUNNING in Kafka Connect",
            )
            refreshed = connector_groups_svc.get_group(group_id) or refreshed
        except Exception as exc:
            print(
                f"[orchestrator] {mid}: CDC pack {group_id} RUNNING status sync failed: {exc}"
            )
            refreshed = {**refreshed, "status": "RUNNING"}

    print(
        f"[orchestrator] {mid}: CDC pack {group_id} synced to RUNNING from Kafka Connect"
    )
    _broadcast({
        "type": "connector_group_status",
        "group_id": group_id,
        "status": "RUNNING",
    })
    return {**refreshed, "status": "RUNNING"}


def _handle_new(mid: str, m: dict) -> None:
    """Validate keys, queue gate, create stage, → TOPIC_CREATING (CDC)
    или → CHUNKING (BULK).  Коннектор управляется на уровне группы."""
    try:
        strategy = Strategy.parse(m.get("strategy"))
    except ValueError:
        _fail(mid, f"Неизвестная стратегия: {m.get('strategy')!r}", "UNKNOWN_STRATEGY")
        return

    pk = m.get("source_pk_exists", False)
    uk = m.get("source_uk_exists", False)
    key_cols = json.loads(m.get("effective_key_columns_json") or "[]")

    if strategy.has_cdc and not pk and not uk and not key_cols:
        inferred = _try_infer_cdc_key(mid, m)
        if inferred:
            pk = inferred.get("source_pk_exists", False)
            uk = inferred.get("source_uk_exists", False)
            key_cols = json.loads(inferred.get("effective_key_columns_json") or "[]")

    if strategy.has_cdc and not pk and not uk and not key_cols:
        _fail(mid,
              "Таблица не имеет PK/UK и ключевые колонки не заданы.",
              "NO_KEY_COLUMNS")
        return

    # CDC migrations are queue candidates only after their CDC runtime is RUNNING.
    if strategy.has_cdc:
        group = connector_groups_svc.get_group(m["group_id"])
        if not group:
            _fail(mid, "CDC-пачка не найдена", "CDC_PACK_NOT_FOUND")
            return
        if group["status"] != "RUNNING":
            group = _sync_cdc_group_runtime_status(mid, m["group_id"], group)
        if group["status"] != "RUNNING":
            _update(mid, {"queue_position": None})
            _ensure_cdc_group_runtime_progress(mid, m["group_id"], group)
            _update_queue_positions()
            return
        m = _sync_cdc_runtime_context(mid, m, group)

    # Queue gate. Bulk and CDC use independent slots.
    is_cdc = strategy.has_cdc
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(_HEAVY_PHASES))
            cur.execute(
                f"""SELECT 1 FROM migrations
                    WHERE  phase IN ({placeholders})
                      AND  (LEFT(COALESCE(strategy, ''), 4) = 'CDC_') = %s
                      AND  migration_id != %s
                    LIMIT 1""",
                (*_HEAVY_PHASES, is_cdc, mid),
            )
            slot_busy = cur.fetchone() is not None
    finally:
        conn.close()

    if slot_busy:
        lane = "CDC" if is_cdc else "bulk"
        print(f"[orchestrator] {mid}: {lane} load slot is busy; migration remains NEW")
        _update_queue_positions()
        return

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.migration_id
                FROM   migrations m
                LEFT JOIN connector_groups cg ON cg.group_id = m.group_id
                WHERE  m.phase = 'NEW'
                  AND  (LEFT(COALESCE(m.strategy, ''), 4) = 'CDC_') = %s
                  AND  (
                        LEFT(COALESCE(m.strategy, ''), 4) <> 'CDC_'
                        OR cg.status = 'RUNNING'
                  )
                ORDER BY m.state_changed_at ASC
                LIMIT 1
            """, (is_cdc,))
            first = cur.fetchone()
    finally:
        conn.close()

    if first and first[0] != mid:
        lane = "CDC" if is_cdc else "bulk"
        print(
            f"[orchestrator] {mid}: waiting for earlier {lane} NEW migration "
            f"{first[0]}; migration remains NEW"
        )
        _update_queue_positions()
        return

    # Verify group connector is RUNNING (for CDC strategies)
    if strategy.has_cdc:
        group = connector_groups_svc.get_group(m["group_id"])
        if not group:
            _fail(mid, "CDC-пачка не найдена", "CDC_PACK_NOT_FOUND")
            return
        if group["status"] != "RUNNING":
            # Группа ещё не запущена — ждём, миграция остаётся в NEW.
            # После /start группы оркестратор подхватит её на следующем тике.
            group = _sync_cdc_group_runtime_status(mid, m["group_id"], group)
        if group["status"] != "RUNNING":
            _ensure_cdc_group_runtime_progress(mid, m["group_id"], group)
            _update_queue_positions()
            return

    _update(mid, {"queue_position": None})

    # Create stage table (if STAGE strategy)
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            src_cfg = _oracle_cfg(m["source_connection_id"])
            dst_cfg = _oracle_cfg(m["target_connection_id"])
            msg_parts: list[str] = []

            if strategy.has_cdc:
                try:
                    has_supp = oracle_scn.check_supplemental_logging(
                        src_cfg, m["source_schema"], m["source_table"]
                    )
                    if not has_supp:
                        print(
                            f"[orchestrator] WARNING: {m['source_schema']}.{m['source_table']} "
                            "does not have ALL COLUMNS supplemental logging."
                        )
                except Exception as exc:
                    print(f"[orchestrator] supplemental logging check failed: {exc}")

            if strategy.uses_stage:
                ts = m.get("stage_tablespace") or ""
                oracle_stage.create_stage_table(
                    src_cfg, dst_cfg,
                    m["source_schema"], m["source_table"],
                    m["target_schema"], m["stage_table_name"],
                    tablespace=ts,
                )
                msg_parts.append("stage table created")
            else:
                msg_parts.append("direct load")

            if not strategy.uses_stage:
                _prepare_target_for_direct_load(mid, m, dst_cfg, msg_parts)

            if not strategy.has_cdc:
                _safe_transition(mid, "NEW", "CHUNKING",
                                 message=", ".join(msg_parts) + ", no CDC -> chunking")
                _unmark_in_prog(mid)
                _create_chunks_and_transition(mid, m)
                return
            else:
                _safe_transition(mid, "NEW", "TOPIC_CREATING",
                                 message=", ".join(msg_parts) + ", create Kafka topic")
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "PREPARING_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_topic_creating(mid: str, m: dict) -> None:
    """Pre-create the Kafka topic for this table, then create chunks."""
    if _in_prog(mid):
        return
    _mark_in_prog(mid)

    def _run():
        try:
            topic_prefix = m.get("topic_prefix", "")
            src_schema = m["source_schema"].upper()
            src_table = m["source_table"].upper()
            topic_name = f"{topic_prefix}.{src_schema}.{src_table}".replace("#", "_")

            # Get Kafka bootstrap servers
            configs = _configs()
            kafka_cfg = configs.get("kafka", {})
            bootstrap = [
                s.strip()
                for s in (kafka_cfg.get("bootstrap_servers") or "kafka:9092").split(",")
            ]

            kafka_topics.create_topic(
                bootstrap_servers=bootstrap,
                topic_name=topic_name,
            )

            # Transition to chunking
            _safe_transition(mid, "TOPIC_CREATING", "CHUNKING",
                             message=f"Топик {topic_name} создан, нарезка чанков")

            # Create chunks
            _unmark_in_prog(mid)
            _create_chunks_and_transition(mid, m)
            return
        except Exception as exc:
            if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                _fail(mid, str(exc), "TOPIC_CREATE_ERROR")
        finally:
            _unmark_in_prog(mid)

    threading.Thread(target=_run, daemon=True).start()


def _handle_indexes_enabling(mid: str, m: dict) -> None:
    """Queue index/constraint enabling work and follow worker completion.

    The load slot and the table-pack queue are released as soon as this phase is
    reached. The actual rebuild happens in the universal worker.
    """
    conn = _state["get_conn"]()
    try:
        job = target_index_jobs.ensure_pending_job(
            conn, mid, requested_by="orchestrator", force_new=False,
        )
    finally:
        conn.close()

    state = str(job.get("state") or "").upper()
    if job.get("created"):
        _broadcast({
            "type": "target_index_job",
            "migration_id": mid,
            "job_id": job["job_id"],
            "state": state,
            "enabled_count": job.get("enabled_count") or 0,
            "ts": datetime.utcnow().isoformat() + "Z",
        })
        _update_queue_positions()
        return

    if state in ("PENDING", "RUNNING"):
        return

    if state == "FAILED":
        _update(mid, {
            "error_code": "INDEXES_ENABLE_ERROR",
            "error_text": (job.get("error_text") or "Index enable job failed")[:2000],
        })
        return

    if state == "DONE":
        result = job.get("result_json") or {}
        enabled = result.get("enabled") or {}
        n_idx = len(enabled.get("indexes") or [])
        n_con = len(enabled.get("constraints") or [])
        n_fk_nv = len(enabled.get("fk_novalidate") or [])
        n_ref_fk_nv = len(enabled.get("referencing_fk_novalidate") or [])
        try:
            strategy = Strategy.parse(m.get("strategy"))
        except ValueError:
            _fail(mid, f"Unknown strategy: {m.get('strategy')!r}", "UNKNOWN_STRATEGY")
            return
        if strategy.has_cdc:
            _safe_transition(
                mid, "INDEXES_ENABLING", "CDC_APPLYING",
                message=(
                    f"Indexes enabled by worker: indexes={n_idx}, constraints={n_con}, "
                    f"FK NOVALIDATE={n_fk_nv}, referencing FK NOVALIDATE={n_ref_fk_nv}. "
                    "Waiting for CDC apply worker"
                ),
                extra_fields={"error_code": None, "error_text": None},
            )
        else:
            _safe_transition(
                mid, "INDEXES_ENABLING", "DATA_VERIFYING",
                message=(
                    f"Indexes enabled by worker: indexes={n_idx}, constraints={n_con}, "
                    f"FK NOVALIDATE={n_fk_nv}, referencing FK NOVALIDATE={n_ref_fk_nv}. "
                    "Triggers stay disabled until the manual trigger job is run. No CDC -> data verification"
                ),
                extra_fields={"error_code": None, "error_text": None},
            )

def _handle_data_verifying(mid: str, m: dict) -> None:
    """Create data_compare task on first tick, then monitor its completion."""
    task_id = m.get("data_compare_task_id")

    if not task_id:
        # First tick — create the data_compare task in a daemon thread
        if _in_prog(mid):
            return
        _mark_in_prog(mid)

        def _run():
            try:
                conn = _state["get_conn"]()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO data_compare_tasks
                                (source_schema, source_table, target_schema, target_table,
                                 compare_mode, chunk_size, status)
                            VALUES (%s, %s, %s, %s, 'full', %s, 'PENDING')
                            RETURNING task_id
                        """, (m["source_schema"], m["source_table"],
                              m["target_schema"], m["target_table"],
                              m.get("chunk_size") or 100_000))
                        new_task_id = str(cur.fetchone()[0])

                        cur.execute(
                            "UPDATE migrations SET data_compare_task_id = %s, updated_at = NOW() "
                            "WHERE migration_id = %s",
                            (new_task_id, mid))
                    conn.commit()
                finally:
                    conn.close()

                # Launch chunking in background (reuse data_compare logic)
                from routes.data_compare import _create_chunks_and_start
                configs = _state["load_configs"]()
                threading.Thread(
                    target=_create_chunks_and_start,
                    args=(new_task_id, configs,
                          m["source_schema"], m["source_table"],
                          m["target_schema"], m["target_table"],
                          m.get("chunk_size") or 100_000),
                    daemon=True,
                    name=f"dv-chunk-{mid[:8]}",
                ).start()

                print(f"[orchestrator] {mid}: data_compare task created: {new_task_id}")

            except Exception as exc:
                if _current_phase(mid) not in ("CANCELLING", "CANCELLED"):
                    _fail(mid, f"Ошибка создания сверки: {exc}", "DATA_VERIFY_ERROR")
            finally:
                _unmark_in_prog(mid)

        threading.Thread(target=_run, daemon=True, name=f"dv-init-{mid[:8]}").start()
        return

    # Subsequent ticks — check data_compare task status
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, counts_match, hash_match, "
                "       source_count, target_count, chunks_done, chunks_total, error_text "
                "FROM data_compare_tasks WHERE task_id = %s",
                (task_id,))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        _fail(mid, f"data_compare task {task_id} not found", "DATA_VERIFY_ERROR")
        return

    status, counts_match, hash_match, src_count, tgt_count, done, total, err_text = row

    if status == "FAILED":
        _fail(mid, f"Сверка данных завершилась ошибкой: {err_text or 'unknown'}", "DATA_VERIFY_ERROR")
        return

    if status not in ("DONE", "COMPLETED"):
        # Self-heal: пустая таблица → 0 чанков → воркеры не дёргают
        # try_aggregate, задача висит в RUNNING вечно. Пытаемся
        # дофинализировать её отсюда. try_aggregate безопасна для
        # повторного вызова (берёт FOR UPDATE и проверяет status).
        if status == "RUNNING" and (total or 0) == 0:
            try:
                from routes.data_compare import try_aggregate
                try_aggregate(task_id)
            except Exception as exc:
                print(f"[orchestrator] {mid}: empty-table self-heal failed: {exc}")
        return  # Still running

    # Verification complete — check results
    if counts_match and hash_match:
        completed = _safe_transition(
            mid, "DATA_VERIFYING", "COMPLETED",
            message=(
                f"Сверка данных пройдена. Source: {src_count}, Target: {tgt_count}. "
                "COUNT и HASH совпадают."
            ),
            extra_fields={"error_code": None, "error_text": None},
        )
        if completed:
            _ensure_trigger_job(mid)
    else:
        details = []
        if not counts_match:
            details.append(f"COUNT mismatch: source={src_count}, target={tgt_count}")
        if not hash_match:
            details.append("HASH mismatch")
        _safe_transition(
            mid, "DATA_VERIFYING", "DATA_MISMATCH",
            message=f"Сверка выявила расхождения: {'; '.join(details)}",
            extra_fields={
                "error_code": "DATA_MISMATCH",
                "error_text": f"source_count={src_count}, target_count={tgt_count}, "
                              f"counts_match={counts_match}, hash_match={hash_match}",
            },
        )


def _handle_data_mismatch(mid: str, m: dict) -> None:
    """Idle phase — wait for user action (retry_verify, force_complete, cancel)."""
    pass


def _handle_cdc_applying(mid: str, m: dict) -> None:
    """Wait for CDC worker heartbeat (group-based variant of CDC_APPLY_STARTING)."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT worker_heartbeat
                FROM   migration_cdc_state
                WHERE  migration_id = %s
            """, (m["migration_id"],))
            row = cur.fetchone()
    finally:
        conn.close()

    if row and row[0]:
        _transition(mid, "CDC_CATCHING_UP",
                    message="CDC apply-worker подключился")


# ---------------------------------------------------------------------------
# Group lifecycle tick (TOPICS_CREATING → CONNECTOR_STARTING → RUNNING)
# ---------------------------------------------------------------------------

_group_in_progress: set[str] = set()
_group_in_progress_lock = threading.Lock()


def _tick_groups() -> None:
    """Drive connector-group lifecycle phases."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT group_id, status, connector_name
                FROM   connector_groups
                WHERE  status IN ('TOPICS_CREATING', 'CONNECTOR_STARTING', 'STOPPING')
            """)
            groups = [{"group_id": r[0], "status": r[1], "connector_name": r[2]}
                      for r in cur.fetchall()]
    except Exception:
        return
    finally:
        conn.close()

    for g in groups:
        gid = g["group_id"]
        status = g["status"]
        try:
            if status == "TOPICS_CREATING":
                _handle_group_topics_creating(gid)
            elif status == "CONNECTOR_STARTING":
                _handle_group_connector_starting(gid)
            elif status == "STOPPING":
                _handle_group_stopping(gid)
        except Exception as exc:
            print(f"[orchestrator] group {gid} status {status} error: {exc}")
            connector_groups_svc.transition_group(gid, "FAILED", str(exc))
            _broadcast({
                "type": "connector_group_status",
                "group_id": gid,
                "status": "FAILED",
            })


def kick_connector_group_lifecycle(group_id: str) -> bool:
    """Drive one connector group now instead of waiting for the next tick."""
    group = connector_groups_svc.get_group(group_id)
    if not group:
        return False
    status = group.get("status")
    if status == "TOPICS_CREATING":
        _handle_group_topics_creating(group_id)
        return True
    if status == "CONNECTOR_STARTING":
        _handle_group_connector_starting(group_id)
        return True
    if status == "STOPPING":
        _handle_group_stopping(group_id)
        return True
    return False


def _handle_group_topics_creating(group_id: str) -> None:
    """Create Kafka topics, then move to CONNECTOR_STARTING."""
    with _group_in_progress_lock:
        if group_id in _group_in_progress:
            return
        _group_in_progress.add(group_id)

    def _run():
        start_connector = False
        try:
            results = connector_groups_svc.do_create_topics(group_id)
            errors = [r for r in results if r.get("status") == "error"]
            if errors:
                msg = "; ".join(f"{r['topic_name']}: {r.get('error','?')}" for r in errors)
                connector_groups_svc.transition_group(
                    group_id, "FAILED", f"Ошибка создания топиков: {msg}")
                _broadcast({
                    "type": "connector_group_status",
                    "group_id": group_id, "status": "FAILED",
                })
                return

            ok_topics = [r["topic_name"] for r in results if r.get("status") == "ok"]
            connector_groups_svc.transition_group(
                group_id, "CONNECTOR_STARTING",
                f"Создано {len(ok_topics)} топиков")
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "CONNECTOR_STARTING",
            })
            start_connector = True
        except Exception as exc:
            connector_groups_svc.transition_group(
                group_id, "FAILED", str(exc))
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "FAILED",
            })
        finally:
            with _group_in_progress_lock:
                _group_in_progress.discard(group_id)
            if start_connector:
                _handle_group_connector_starting(group_id)

    threading.Thread(target=_run, daemon=True).start()


def _handle_group_connector_starting(group_id: str) -> None:
    """Create and start Debezium connector, then move to RUNNING."""
    with _group_in_progress_lock:
        if group_id in _group_in_progress:
            return
        _group_in_progress.add(group_id)

    def _run():
        try:
            result = connector_groups_svc.do_start_connector(group_id)
            connector_groups_svc.refresh_connector_tables(group_id)
            connector_groups_svc.transition_group(
                group_id, "RUNNING",
                f"CDC runtime запущен: {result.get('name', '?')}")
            _update_queue_positions()
            _kick_new_migrations_for_group(group_id)
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "RUNNING",
            })
        except Exception as exc:
            connector_groups_svc.transition_group(
                group_id, "FAILED", str(exc))
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "FAILED",
            })
        finally:
            with _group_in_progress_lock:
                _group_in_progress.discard(group_id)

    threading.Thread(target=_run, daemon=True).start()


def _handle_group_stopping(group_id: str) -> None:
    """Stop and delete Debezium connector, then move to STOPPED."""
    with _group_in_progress_lock:
        if group_id in _group_in_progress:
            return
        _group_in_progress.add(group_id)

    def _run():
        try:
            connector_groups_svc.do_stop_connector(group_id)
            connector_groups_svc.transition_group(
                group_id, "STOPPED", "CDC runtime остановлен")
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "STOPPED",
            })
        except Exception as exc:
            connector_groups_svc.transition_group(
                group_id, "FAILED", str(exc))
            _broadcast({
                "type": "connector_group_status",
                "group_id": group_id, "status": "FAILED",
            })
        finally:
            with _group_in_progress_lock:
                _group_in_progress.discard(group_id)

    threading.Thread(target=_run, daemon=True).start()


def _broadcast(event: dict) -> None:
    """Send SSE event."""
    _state["broadcast"](event)


# ---------------------------------------------------------------------------
# Group connector health check (runs every tick)
# ---------------------------------------------------------------------------

def _check_group_connectors() -> None:
    """Poll all RUNNING connector groups.  If a CDC runtime failed,
    fail all active CDC migrations in that group."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT group_id, connector_name, COALESCE(run_id, '') as run_id
                FROM   connector_groups
                WHERE  status = 'RUNNING'
            """)
            groups = cur.fetchall()
    except Exception:
        return  # table may not exist yet on first run
    finally:
        conn.close()

    for group_id, connector_name, run_id in groups:
        active_name = f"{connector_name}_{run_id}" if run_id else connector_name
        try:
            status = debezium.get_connector_status(active_name)
        except Exception as exc:
            print(f"[orchestrator] group {group_id} connector check error: {exc}")
            continue

        if status in ("FAILED", "NOT_FOUND"):
            reason = "CDC runtime FAILED" if status == "FAILED" else "CDC runtime NOT_FOUND"
            print(f"[orchestrator] group connector {active_name} {status} - failing group migrations")
            connector_groups_svc.update_group_status(group_id, "FAILED", reason)

            # Fail all active CDC migrations in this group
            conn = _state["get_conn"]()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT migration_id FROM migrations
                        WHERE  group_id = %s
                          AND  phase NOT IN ('DRAFT', 'COMPLETED', 'CANCELLED', 'FAILED')
                          AND  strategy LIKE 'CDC_%'
                    """, (group_id,))
                    for row in cur.fetchall():
                        _fail(row[0],
                              f"CDC runtime пачки {connector_name} перешёл в FAILED",
                              "CDC_RUNTIME_FAILED")
            finally:
                conn.close()
