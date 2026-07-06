"""Schema-migration dashboard API.

GET    /api/schema-migrations              → list (each with computed KPIs)
GET    /api/schema-migrations/:id          → detail (header + KPIs)
GET    /api/schema-migrations/:id/objects  → object table rows (SchemaObject[])
GET    /api/schema-migrations/:id/events   → recent events
GET    /api/schema-migrations/:id/metrics  → live metrics (placeholder)
POST   /api/schema-migrations              → create (Wizard submit)
POST   /api/schema-migrations/:id/pause    → pause whole schema migration
POST   /api/schema-migrations/:id/resume   → resume
POST   /api/schema-migrations/:id/rollback → cancel (mark child migrations)
"""

import json
import uuid
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

import services.schema_migrations as svc
import services.ddl_apply_jobs as ddl_jobs
from services.strategy import Strategy

bp = Blueprint("schema_migrations", __name__)

_state: dict = {}


def _utc_iso_z(value):
    if not value:
        return None
    if getattr(value, "tzinfo", None) is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    if hasattr(value, "isoformat"):
        return value.isoformat() + "Z"
    return value


def init(*, get_conn_fn, db_available_ref, broadcast_fn, load_configs_fn=None):
    _state["get_conn"]     = get_conn_fn
    _state["db_available"] = db_available_ref
    _state["broadcast"]    = broadcast_fn
    _state["load_configs"] = load_configs_fn


def _db_ok() -> bool:
    return _state["db_available"]["value"]


def _source_oracle_conn():
    load_configs = _state.get("load_configs")
    if load_configs is None:
        raise RuntimeError("load_configs not wired")
    from db.oracle_browser import get_oracle_conn
    return get_oracle_conn("source", load_configs(), prefer_owner=True)


def _derive_cdc_key_info(info: dict) -> tuple[str, str, list[str], bool, bool]:
    pk_columns = info.get("pk_columns") or []
    uk_constraints = info.get("uk_constraints") or []
    if pk_columns:
        return "PRIMARY_KEY", "PK", pk_columns, True, bool(uk_constraints)
    if uk_constraints:
        return "UNIQUE_KEY", "UK", uk_constraints[0].get("columns") or [], False, True
    return "NONE", "NONE", [], False, False


def _start_created_cdc_plan_batches(plan_id: int, created: list[dict]) -> list[dict]:
    """Move each newly added CDC queue position from PENDING/DRAFT to RUNNING/NEW."""
    if not created:
        return []
    from routes.planner import _start_next_plan_batch

    starts: list[dict] = []
    for batch_order in sorted({item["batch_order"] for item in created}):
        starts.append(_start_next_plan_batch(
            plan_id,
            actor="SYSTEM",
            batch_order=batch_order,
            allow_cdc_queue_when_blocked=True,
        ))
    return starts


def _should_start_created_cdc_plan_batches(
    connector_group_status: str | None,
    connector_start_error: str | None,
) -> bool:
    if not connector_start_error:
        return True
    return str(connector_group_status or "").upper() != "RUNNING"


def _record_cdc_connector_start_error(
    connector_group_id: str | None,
    connector_group_status: str | None,
    error_text: str,
) -> str | None:
    if not connector_group_id:
        return None
    from services import connector_groups as groups

    current_status = str(connector_group_status or "").upper()
    next_status = "RUNNING" if current_status == "RUNNING" else "FAILED"
    groups.transition_group(
        connector_group_id,
        next_status,
        f"CDC runtime autostart failed: {error_text}",
        error_text=error_text,
    )
    return next_status


def _clear_cdc_connector_start_error(connector_group_id: str | None) -> None:
    if not connector_group_id:
        return
    from services import connector_groups as groups

    groups.clear_group_error(connector_group_id)


def _kick_cdc_group_best_effort(group_id: str | None) -> bool:
    if not group_id:
        return False
    try:
        from services import orchestrator
        orchestrator._update_queue_positions()
        orchestrator._kick_new_migrations_for_group(group_id)
        return True
    except Exception as exc:
        print(f"[schema_migrations] CDC queue kick warning: {exc}")
        return False


def _kick_cdc_group_lifecycle_best_effort(group_id: str | None) -> bool:
    if not group_id:
        return False
    try:
        from services import orchestrator
        return bool(orchestrator.kick_connector_group_lifecycle(group_id))
    except Exception as exc:
        print(f"[schema_migrations] CDC runtime lifecycle kick warning: {exc}")
        return False


def _autostart_created_cdc_items(
    connector_group_id: str | None,
    connector_group_status: str | None,
    plan_id: int,
    created: list[dict],
) -> dict:
    connector_start = None
    connector_start_error = None
    plan_start = None
    plan_starts: list[dict] = []
    plan_start_error = None
    cdc_queue_kicked = False

    try:
        connector_start = _sync_and_request_cdc_connector_start(
            connector_group_id,
            connector_group_status,
        )
        _clear_cdc_connector_start_error(connector_group_id)
        _state["broadcast"]({
            "type": "connector_group_status",
            "group_id": connector_group_id,
            "status": connector_start.get("status"),
        })
        _kick_cdc_group_lifecycle_best_effort(connector_group_id)
    except Exception as exc:
        connector_start_error = str(exc)
        recorded_status = _record_cdc_connector_start_error(
            connector_group_id,
            connector_group_status,
            connector_start_error,
        )
        if recorded_status:
            _state["broadcast"]({
                "type": "connector_group_status",
                "group_id": connector_group_id,
                "status": recorded_status,
            })
        print(f"[schema_migrations.add_plan_items] CDC runtime autostart warning: {exc}")

    if _should_start_created_cdc_plan_batches(connector_group_status, connector_start_error):
        try:
            plan_starts = _start_created_cdc_plan_batches(plan_id, created)
            plan_start = plan_starts[0] if plan_starts else None
            cdc_queue_kicked = _kick_cdc_group_best_effort(connector_group_id)
        except Exception as start_exc:
            plan_start_error = str(start_exc)
            print(f"[schema_migrations.add_plan_items] CDC plan autostart warning: {start_exc}")

    return {
        "connector_start": connector_start,
        "connector_start_error": connector_start_error,
        "plan_start": plan_start,
        "plan_starts": plan_starts,
        "plan_start_error": plan_start_error,
        "cdc_queue_kicked": cdc_queue_kicked,
    }


def _ensure_cdc_group_topics(group_id: str) -> list[dict]:
    from services.connector_groups import create_group_topics

    results = create_group_topics(group_id)
    errors = [r for r in results if r.get("status") == "error"]
    if errors:
        msg = "; ".join(
            f"{r.get('topic_name', '?')}: {r.get('error', '?')}"
            for r in errors
        )
        raise ValueError(f"CDC topic creation failed: {msg}")
    return results


def _is_missing_running_connector_error(exc: Exception) -> bool:
    text = str(exc)
    return "marked RUNNING" in text and "missing in Kafka Connect" in text


def _sync_and_request_cdc_connector_start(
    connector_group_id: str,
    connector_group_status: str | None,
) -> dict:
    """Sync Debezium config and request start after adding CDC tables.

    Kafka Connect may no longer have a connector while coordinator state still
    says RUNNING. In that case refresh marks the group STOPPED and raises; the
    add-table flow should recover by requesting a fresh start.
    """
    from services.connector_groups import get_connector_status, refresh_connector_tables, request_start

    if connector_group_status == "RUNNING":
        if get_connector_status(connector_group_id) == "STOPPED":
            return request_start(connector_group_id)
    if connector_group_status in ("RUNNING", "TOPICS_CREATING", "CONNECTOR_STARTING"):
        _ensure_cdc_group_topics(connector_group_id)
    try:
        refresh_connector_tables(connector_group_id)
    except ValueError as exc:
        if not _is_missing_running_connector_error(exc):
            raise
        print(
            "[schema_migrations.add_plan_items] CDC runtime missing in Kafka Connect; "
            "requesting fresh start"
        )
    return request_start(connector_group_id)


def _active_cdc_migration_for_group_table(cur, group_id: str, source_schema: str, source_table: str):
    cur.execute("""
        SELECT migration_id, phase
        FROM   migrations
        WHERE  group_id = %s
          AND  UPPER(source_schema) = UPPER(%s)
          AND  UPPER(source_table) = UPPER(%s)
          AND  LEFT(COALESCE(strategy, ''), 4) = 'CDC_'
          AND  COALESCE(phase, '') NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
        LIMIT  1
    """, (group_id, source_schema, source_table))
    return cur.fetchone()


def _prune_cdc_group_tables_tx(cur, group_id: str, keep_tables: set[str]) -> list[dict]:
    cur.execute("""
        SELECT id, source_schema, source_table, target_schema, target_table
        FROM   group_tables
        WHERE  group_id = %s
        ORDER BY source_schema, source_table
    """, (group_id,))
    rows = [
        {
            "id": str(row[0]),
            "source_schema": row[1],
            "source_table": row[2],
            "target_schema": row[3],
            "target_table": row[4],
        }
        for row in cur.fetchall()
    ]
    to_remove = [
        row for row in rows
        if f"{str(row['source_schema']).upper()}.{str(row['source_table']).upper()}" not in keep_tables
    ]
    for row in to_remove:
        active = _active_cdc_migration_for_group_table(
            cur,
            group_id,
            row["source_schema"],
            row["source_table"],
        )
        if active:
            active_mid, active_phase = active
            raise ValueError(
                f"Cannot remove {row['source_schema']}.{row['source_table']} from CDC pack: "
                f"active migration {active_mid} is in phase {active_phase}"
            )
    for row in to_remove:
        cur.execute("""
            DELETE FROM group_tables
            WHERE group_id = %s
              AND UPPER(source_schema) = UPPER(%s)
              AND UPPER(source_table) = UPPER(%s)
        """, (group_id, row["source_schema"], row["source_table"]))
    return to_remove


def _resolve_cdc_connector_group_id(
    sm_group_id,
    plan_group_id,
    payload_group_id,
) -> str | None:
    values = [
        str(value)
        for value in (sm_group_id, plan_group_id, payload_group_id)
        if value
    ]
    if len(set(values)) > 1:
        raise ValueError(
            "CDC pack mismatch between schema migration, plan and request. "
            "A schema migration can use only one CDC pack."
        )
    return values[0] if values else None


def _load_cdc_connector_summary(group_id: str | None) -> dict | None:
    if not group_id:
        return None
    try:
        from services import connector_groups as groups

        group = groups.get_group(group_id)
        if not group:
            return None
        tables = groups.get_group_tables(group_id)
        group["active_connector_name"] = groups._active_connector_name(group)
        group["active_topic_prefix"] = groups._active_topic_prefix(group)
        group["tables"] = tables
        group["table_include_list"] = groups._build_table_include_list(group_id)
        group["message_key_columns"] = groups._build_key_columns(group_id)
        return group
    except Exception as exc:
        print(f"[schema_migrations] CDC runtime summary warning: {exc}")
        return None


def _build_add_plan_items_response_payload(
    *,
    plan_id: int,
    created: list[dict],
    strategy: Strategy,
    connector_group_id: str | None,
    item_states: list[dict] | None = None,
    connector_start: dict | None = None,
    connector_start_error: str | None = None,
    plan_start: dict | None = None,
    plan_starts: list[dict] | None = None,
    plan_start_error: str | None = None,
    cdc_queue_kicked: bool = False,
    cdc_pruned_tables: list[dict] | None = None,
) -> dict:
    cdc_next_action = _build_cdc_next_action(
        strategy=strategy,
        item_states=item_states or [],
        connector_start=connector_start,
        connector_start_error=connector_start_error,
        plan_start_error=plan_start_error,
        cdc_queue_kicked=cdc_queue_kicked,
    )
    return {
        "plan_id": plan_id,
        "items": created,
        "item_states": item_states or [],
        "connector_group_id": connector_group_id if strategy.has_cdc else None,
        "cdc_group": _load_cdc_connector_summary(connector_group_id) if strategy.has_cdc else None,
        "cdc_pruned_tables": cdc_pruned_tables or [],
        "connector_start": connector_start,
        "connector_start_error": connector_start_error,
        "plan_start": plan_start,
        "plan_starts": plan_starts or [],
        "plan_start_error": plan_start_error,
        "cdc_queue_kicked": cdc_queue_kicked,
        "cdc_next_action": cdc_next_action,
    }


def _build_cdc_next_action(
    *,
    strategy: Strategy,
    item_states: list[dict],
    connector_start: dict | None,
    connector_start_error: str | None,
    plan_start_error: str | None,
    cdc_queue_kicked: bool,
) -> dict | None:
    if not strategy.has_cdc:
        return None

    if plan_start_error:
        return {
            "level": "error",
            "code": "PLAN_START_FAILED",
            "message": f"CDC-строки созданы, но очередь не запущена: {plan_start_error}",
        }

    phases = [str(item.get("phase") or "").upper() for item in item_states]
    statuses = [str(item.get("status") or "").upper() for item in item_states]
    has_worker_wait = any(
        phase in ("CDC_APPLY_STARTING", "CDC_APPLYING")
        and not item.get("cdc_worker_heartbeat")
        for item, phase in zip(item_states, phases)
    )
    has_apply = any(
        phase == "CDC_CATCHING_UP"
        or (
            phase in ("CDC_APPLY_STARTING", "CDC_APPLYING")
            and bool(item.get("cdc_worker_heartbeat"))
        )
        for item, phase in zip(item_states, phases)
    )
    has_new = any(status == "RUNNING" and phase == "NEW" for status, phase in zip(statuses, phases))
    has_queued = any(
        status == "RUNNING"
        and phase == "NEW"
        and item.get("queue_position") is not None
        for item, status, phase in zip(item_states, statuses, phases)
    )
    has_draft = any(status == "PENDING" or phase == "DRAFT" for status, phase in zip(statuses, phases))

    if connector_start_error:
        if has_new or has_queued:
            return {
                "level": "warn",
                "code": "CONNECTOR_START_FAILED_WAITING",
                "message": (
                    "CDC-строки добавлены в очередь ожидания. "
                    f"Исправьте CDC-пачку и запустите её: {connector_start_error}"
                ),
            }
        return {
            "level": "error",
            "code": "CONNECTOR_SYNC_FAILED",
            "message": (
                "CDC-строки сохранены, но не запущены. "
                f"Синхронизируйте Debezium: {connector_start_error}"
            ),
        }

    connector_status = str((connector_start or {}).get("status") or "").upper()
    if has_apply:
        return {
            "level": "ok",
            "code": "CDC_APPLY_STARTED",
            "message": "CDC apply уже запущен.",
        }
    if has_worker_wait:
        return {
            "level": "warn",
            "code": "WAITING_WORKER",
            "message": "CDC-строки готовы к apply и ждут universal worker.",
        }
    if has_queued:
        return {
            "level": "ok",
            "code": "QUEUED",
            "message": "CDC-строки поставлены в очередь.",
        }
    if has_new and connector_status and connector_status != "RUNNING":
        return {
            "level": "warn",
            "code": "WAITING_CONNECTOR",
            "message": f"CDC-строки ждут CDC-пачку ({connector_status}).",
        }
    if has_new:
        return {
            "level": "ok",
            "code": "STARTING",
            "message": "CDC-строки запущены и ожидают назначения worker.",
        }
    if has_draft:
        return {
            "level": "warn",
            "code": "DRAFT",
            "message": "CDC-строки созданы, но еще не переведены в запуск.",
        }
    if cdc_queue_kicked:
        return {
            "level": "ok",
            "code": "QUEUE_KICKED",
            "message": "CDC-очередь проверена.",
        }
    return {
        "level": "info",
        "code": "ADDED",
        "message": "CDC-таблицы добавлены в пачку.",
    }


def _load_created_plan_item_states(conn, created: list[dict]) -> list[dict]:
    migration_ids = [
        str(item.get("migration_id"))
        for item in created
        if item.get("migration_id")
    ]
    if not migration_ids:
        return []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT i.item_id,
                   i.table_name AS table,
                   i.migration_id,
                   i.batch_order,
                   i.status,
                   m.phase,
                   m.queue_position,
                   m.error_text,
                   cs.worker_heartbeat AS cdc_worker_heartbeat
            FROM   migration_plan_items i
            LEFT JOIN migrations m ON m.migration_id = i.migration_id
            LEFT JOIN migration_cdc_state cs ON cs.migration_id = m.migration_id
            WHERE  i.migration_id = ANY(%s::uuid[])
        """, (migration_ids,))
        cols = [desc[0] for desc in cur.description]
        rows = {
            str(row_dict["migration_id"]): row_dict
            for row_dict in (dict(zip(cols, row)) for row in cur.fetchall())
        }
    for row in rows.values():
        row["cdc_worker_heartbeat"] = _utc_iso_z(row.get("cdc_worker_heartbeat"))
    states = []
    for item in created:
        migration_id = str(item.get("migration_id"))
        states.append(rows.get(migration_id, {
            "item_id": item.get("item_id"),
            "table": item.get("table"),
            "migration_id": migration_id,
            "batch_order": item.get("batch_order"),
            "status": None,
            "phase": None,
            "queue_position": None,
            "error_text": None,
            "cdc_worker_heartbeat": None,
        }))
    return states


def _requested_plan_table_name(table) -> str:
    if isinstance(table, dict):
        return (table.get("source_table") or table.get("table") or "").strip().upper()
    return str(table).strip().upper()


def _validate_manual_cdc_key_columns(info: dict, key_columns: list[str]) -> list[str]:
    available = {
        str(col.get("name") or "").strip().upper()
        for col in info.get("columns") or []
        if isinstance(col, dict)
    }
    missing = [col for col in key_columns if col not in available]
    return missing


def _normalize_manual_cdc_key_columns(raw_key_columns) -> list[str]:
    if isinstance(raw_key_columns, str):
        values = raw_key_columns.split(",")
    elif isinstance(raw_key_columns, list):
        values = raw_key_columns
    else:
        values = []
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        col = str(value).strip().upper()
        if not col or col in seen:
            continue
        seen.add(col)
        out.append(col)
    return out


def _effective_cdc_key_info(
    info: dict,
    manual_key_columns: list[str],
    source_schema: str,
    table_name: str,
) -> tuple[str, str, list[str], bool, bool]:
    derived = _derive_cdc_key_info(info)
    _key_type, _key_source, _key_columns, source_pk_exists, source_uk_exists = derived
    if manual_key_columns:
        if source_pk_exists or source_uk_exists:
            raise ValueError(
                f"CDC table {source_schema}.{table_name} already has PK/UK. "
                "Manual CDC key columns are allowed only when PK/UK is missing."
            )
        missing_key_columns = _validate_manual_cdc_key_columns(info, manual_key_columns)
        if missing_key_columns:
            raise ValueError(
                f"CDC key columns not found in {source_schema}.{table_name}: "
                f"{', '.join(missing_key_columns)}"
            )
        return "USER_DEFINED", "USER", manual_key_columns, False, False
    return derived


def _require_cdc_supplemental_logging(info: dict, source_schema: str, table_name: str) -> None:
    supp_log = str(info.get("supplemental_log_data_all") or "").strip().upper()
    if supp_log == "YES":
        return
    if supp_log == "NO":
        raise ValueError(
            f"CDC table {source_schema}.{table_name} does not have "
            "ALL COLUMNS supplemental logging."
        )
    raise ValueError(
        f"CDC table {source_schema}.{table_name} supplemental logging could not be checked. "
        "Grant access to V$DATABASE/ALL_LOG_GROUPS metadata or enable ALL COLUMNS supplemental logging and retry."
    )


@bp.get("/api/schema-migrations")
def list_schema_migrations():
    if not _db_ok():
        return jsonify([])
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.list_schema_migrations(conn))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>")
def get_schema_migration(sm_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        sm = svc.get_schema_migration(conn, sm_id)
        if not sm:
            return jsonify({"error": "Not found"}), 404
        return jsonify(sm)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/cdc-group")
def get_schema_migration_cdc_group(sm_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sm.group_id, p.connector_group_id
                FROM   schema_migrations sm
                LEFT JOIN migration_plans p ON p.plan_id = sm.plan_id
                WHERE  sm.schema_migration_id = %s
            """, (sm_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Not found"}), 404
            group_id = _resolve_cdc_connector_group_id(row[0], row[1], None)
        return jsonify(_load_cdc_connector_summary(group_id))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/objects")
def get_objects(sm_id: str):
    if not _db_ok():
        return jsonify([])
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.get_objects(conn, sm_id))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/objects/<path:obj_id>/detail")
def get_object_detail(sm_id: str, obj_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        detail = svc.get_object_detail(conn, sm_id, obj_id)
        if detail is None:
            return jsonify({"error": "Not found"}), 404
        return jsonify(detail)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/events")
def get_events(sm_id: str):
    if not _db_ok():
        return jsonify([])
    limit = int(request.args.get("limit", 100))
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.get_events(conn, sm_id, limit=limit))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/metrics")
def get_metrics(sm_id: str):
    if not _db_ok():
        return jsonify({})
    conn = _state["get_conn"]()
    try:
        return jsonify(svc.get_metrics(conn, sm_id))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations")
def create_schema_migration():
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    if not payload.get("src_schema") or not payload.get("tgt_schema"):
        return jsonify({"error": "src_schema and tgt_schema required"}), 400
    conn = _state["get_conn"]()
    try:
        new_id = svc.create_schema_migration(conn, payload)
        _state["broadcast"]({"type": "schema_migration.created", "id": new_id})
        return jsonify({"schema_migration_id": new_id}), 201
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/pause")
def pause(sm_id: str):
    return _set_paused(sm_id, True)


@bp.post("/api/schema-migrations/<sm_id>/resume")
def resume(sm_id: str):
    return _set_paused(sm_id, False)


def _set_paused(sm_id: str, paused: bool):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        ok = svc.set_paused(conn, sm_id, paused)
        if not ok:
            return jsonify({"error": "Not found"}), 404
        _state["broadcast"]({
            "type": "schema_migration.paused" if paused else "schema_migration.resumed",
            "id": sm_id,
        })
        return jsonify({"ok": True, "paused": paused})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/ddl-apply")
def ddl_apply(sm_id: str):
    """Queue DDL apply jobs.

    Body: {"action": "create_missing"|"sync_diff"|"recreate",
           "objects": [{"type": "TABLE"|"MVIEW"|..., "name": "FOO"}]}
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    action  = payload.get("action") or ""
    objects = payload.get("objects") or []
    job_type = payload.get("job_type") or None
    if not action:
        return jsonify({"error": "action required"}), 400
    if not isinstance(objects, list) or not objects:
        return jsonify({"error": "objects required"}), 400
    conn = _state["get_conn"]()
    try:
        result = ddl_jobs.submit_jobs(conn, sm_id, action, objects, job_type=job_type)
        _state["broadcast"]({
            "type":   "ddl_apply.queued",
            "sm_id":  sm_id,
            "action": action,
            "job_type": job_type,
            "queued": result["queued"],
        })
        return jsonify(result), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/ddl-pack/items")
def ddl_pack_add_items(sm_id: str):
    """Add DDL apply jobs to a manual pack.

    DRAFT rows are visible in /ddl-jobs, but workers do not claim them until
    /ddl-pack/start moves them to PENDING.
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    action = payload.get("action") or ""
    objects = payload.get("objects") or []
    job_type = payload.get("job_type") or None
    if not action:
        return jsonify({"error": "action required"}), 400
    if not isinstance(objects, list) or not objects:
        return jsonify({"error": "objects required"}), 400
    conn = _state["get_conn"]()
    try:
        result = ddl_jobs.submit_jobs(
            conn,
            sm_id,
            action,
            objects,
            job_type=job_type,
            state="DRAFT",
        )
        _state["broadcast"]({
            "type": "ddl_pack.changed",
            "sm_id": sm_id,
            "action": action,
            "job_type": job_type,
            "queued": result["queued"],
        })
        return jsonify(result), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/ddl-pack/start")
def ddl_pack_start(sm_id: str):
    """Start the manual DDL pack by moving DRAFT jobs to PENDING."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    job_ids = payload.get("job_ids") or None
    if job_ids is not None and not isinstance(job_ids, list):
        return jsonify({"error": "job_ids must be a list"}), 400
    conn = _state["get_conn"]()
    try:
        result = ddl_jobs.start_draft_pack(conn, sm_id, job_ids=job_ids)
        _state["broadcast"]({
            "type": "ddl_pack.started",
            "sm_id": sm_id,
            "started": result["started"],
        })
        return jsonify(result), 202
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.get("/api/schema-migrations/<sm_id>/ddl-jobs")
def list_ddl_jobs(sm_id: str):
    if not _db_ok():
        return jsonify([])
    limit = int(request.args.get("limit", 100))
    conn = _state["get_conn"]()
    try:
        return jsonify(ddl_jobs.list_jobs(conn, sm_id, limit=limit))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/plan/items")
def add_plan_items(sm_id: str):
    """Create or extend this schema migration's table-migration plan."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    payload = request.get_json(silent=True) or {}
    tables = payload.get("tables") or []
    if not isinstance(tables, list) or not tables:
        return jsonify({"error": "tables required"}), 400
    if not any(_requested_plan_table_name(table) for table in tables):
        return jsonify({"error": "at least one table name is required"}), 400

    raw_strategy = payload.get("strategy") or "BULK_DIRECT"
    try:
        strategy = Strategy.parse(raw_strategy)
    except ValueError as exc:
        return jsonify({"error": f"Invalid strategy: {exc}"}), 400
    truncate_target = bool(payload.get("truncate_target", True))
    if strategy.uses_stage and not truncate_target:
        return jsonify({"error": "STAGE strategy requires truncate_target=true"}), 400
    connector_group_id = payload.get("connector_group_id") or None
    prune_cdc_pack = payload.get("prune_cdc_pack") is True and strategy.has_cdc

    sequential = True if strategy.has_cdc else bool(payload.get("sequential", True))
    chunk_size = int(payload.get("chunk_size") or 1_000_000)
    max_workers = int(payload.get("max_parallel_workers") or 1)
    baseline_pd = int(payload.get("baseline_parallel_degree") or 4)
    stage_tablespace = (payload.get("stage_tablespace") or "PAYSTAGE").strip().upper()

    conn = _state["get_conn"]()
    src_oconn = None
    connector_group_status = None
    cdc_pruned_tables: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sm.schema_migration_id, sm.name, sm.src_schema, sm.tgt_schema,
                       sm.plan_id, sm.group_id, p.connector_group_id
                FROM   schema_migrations sm
                LEFT JOIN migration_plans p ON p.plan_id = sm.plan_id
                WHERE  sm.schema_migration_id = %s
                FOR UPDATE OF sm
            """, (sm_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "schema_migration not found"}), 404
            _, sm_name, src_schema, tgt_schema, plan_id, sm_group_id, plan_group_id = row
            src_schema = (src_schema or "").strip().upper()
            tgt_schema = (tgt_schema or "").strip().upper()
            if not src_schema or not tgt_schema:
                return jsonify({"error": "schema migration src_schema/tgt_schema required"}), 400
            if strategy.has_cdc:
                connector_group_id = _resolve_cdc_connector_group_id(
                    sm_group_id,
                    plan_group_id,
                    connector_group_id,
                )
                if connector_group_id is None:
                    connector_group_id = str(uuid.uuid4())
                    suffix = connector_group_id.split("-")[0]
                    base = f"{src_schema.lower()}_{tgt_schema.lower()}_{suffix}"
                    cur.execute("""
                        INSERT INTO connector_groups
                            (group_id, group_name, source_connection_id,
                             connector_name, topic_prefix, consumer_group_prefix)
                        VALUES (%s, %s, 'oracle_source', %s, %s, %s)
                    """, (
                        connector_group_id,
                        f"{src_schema}->{tgt_schema} CDC",
                        f"sm_{base}_connector",
                        f"sm.{src_schema.lower()}.{tgt_schema.lower()}.{suffix}",
                        f"sm.{src_schema.lower()}.{tgt_schema.lower()}.{suffix}",
                    ))
                if sm_group_id is None:
                    cur.execute("""
                        UPDATE schema_migrations
                        SET    group_id = %s,
                               updated_at = NOW()
                        WHERE  schema_migration_id = %s
                    """, (connector_group_id, sm_id))

                cur.execute("""
                    SELECT status, topic_prefix, run_id
                    FROM   connector_groups
                    WHERE  group_id = %s
                """, (connector_group_id,))
                group_row = cur.fetchone()
                if not group_row:
                    return jsonify({"error": "CDC pack not found"}), 400
                connector_group_status, group_topic_prefix, _group_run_id = group_row
                active_topic_prefix = group_topic_prefix
            else:
                active_topic_prefix = None

            if plan_id is None:
                defaults = {
                    "strategy": strategy.value,
                    "truncate_target": truncate_target,
                    "chunk_size": chunk_size,
                    "max_parallel_workers": max_workers,
                }
                cur.execute("""
                    INSERT INTO migration_plans
                        (name, src_schema, tgt_schema, connector_group_id, defaults_json, status)
                    VALUES (%s, %s, %s, %s, %s, 'READY')
                    RETURNING plan_id
                """, (
                    sm_name or f"{src_schema}->{tgt_schema}",
                    src_schema, tgt_schema,
                    connector_group_id if strategy.has_cdc else None,
                    json.dumps(defaults),
                ))
                plan_id = cur.fetchone()[0]
                cur.execute("""
                    UPDATE schema_migrations
                    SET    plan_id = %s,
                           updated_at = NOW()
                    WHERE  schema_migration_id = %s
                """, (plan_id, sm_id))
            elif strategy.has_cdc:
                cur.execute("""
                    UPDATE migration_plans
                    SET    connector_group_id = COALESCE(connector_group_id, %s)
                    WHERE  plan_id = %s
                """, (connector_group_id, plan_id))

            if prune_cdc_pack:
                keep_tables = {
                    f"{src_schema}.{_requested_plan_table_name(table)}"
                    for table in tables
                    if _requested_plan_table_name(table)
                }
                cdc_pruned_tables = _prune_cdc_group_tables_tx(
                    cur,
                    connector_group_id,
                    keep_tables,
                )

            cur.execute("""
                SELECT COALESCE(MAX(batch_order), 0)
                FROM   migration_plan_items
                WHERE  plan_id = %s
            """, (plan_id,))
            batch_base = cur.fetchone()[0] or 0

            created = []
            seen_tables: set[str] = set()
            for idx, table in enumerate(tables):
                manual_key_columns = []
                if isinstance(table, dict):
                    table_name = _requested_plan_table_name(table)
                    target_table = (table.get("target_table") or table_name).strip().upper()
                    raw_manual_key_columns = table.get("effective_key_columns") or table.get("key_columns") or []
                    manual_key_columns = _normalize_manual_cdc_key_columns(raw_manual_key_columns)
                else:
                    table_name = _requested_plan_table_name(table)
                    target_table = table_name
                if not table_name:
                    continue

                if table_name in seen_tables:
                    raise ValueError(f"Table {src_schema}.{table_name} is selected more than once")
                seen_tables.add(table_name)

                cur.execute("""
                    SELECT i.status, m.phase
                    FROM   migration_plan_items i
                    LEFT JOIN migrations m ON m.migration_id = i.migration_id
                    WHERE  i.plan_id = %s
                      AND  UPPER(i.table_name) = %s
                      AND  i.status NOT IN ('DONE', 'FAILED', 'CANCELLED')
                      AND  COALESCE(m.phase, '') NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
                    LIMIT  1
                """, (plan_id, table_name))
                duplicate = cur.fetchone()
                if duplicate:
                    dup_status, dup_phase = duplicate
                    raise ValueError(
                        f"Table {src_schema}.{table_name} is already in this plan "
                        f"({dup_status}, {dup_phase or 'no migration phase'})"
                    )

                effective_key_type = "NONE"
                effective_key_source = "NONE"
                effective_key_columns: list[str] = []
                source_pk_exists = False
                source_uk_exists = False
                if strategy.has_cdc:
                    active = _active_cdc_migration_for_group_table(
                        cur, connector_group_id, src_schema, table_name,
                    )
                    if active:
                        active_mid, active_phase = active
                        raise ValueError(
                            f"CDC table {src_schema}.{table_name} already has active migration "
                            f"in this CDC pack ({active_mid}, {active_phase or 'no phase'})."
                        )
                    if src_oconn is None:
                        src_oconn = _source_oracle_conn()
                    from db.oracle_browser import get_table_info
                    info = get_table_info(src_oconn, src_schema, table_name)
                    _require_cdc_supplemental_logging(info, src_schema, table_name)
                    (effective_key_type,
                     effective_key_source,
                     effective_key_columns,
                     source_pk_exists,
                     source_uk_exists) = _effective_cdc_key_info(
                        info, manual_key_columns, src_schema, table_name,
                    )
                    if not source_pk_exists and not source_uk_exists and not effective_key_columns:
                        raise ValueError(
                            f"CDC table {src_schema}.{table_name} has no PK/UK and no key columns. "
                            "Add it to the regular pack or provide CDC key columns."
                        )

                batch_order = batch_base + idx + 1 if sequential else batch_base + 1
                mid = str(uuid.uuid4())
                stage_table = f"STG_{src_schema}_{table_name}"[:128]
                if strategy.has_cdc:
                    topic_name = f"{active_topic_prefix}.{src_schema}.{table_name}".replace("#", "_")
                    cur.execute("""
                        INSERT INTO group_tables
                            (id, group_id, source_schema, source_table,
                             target_schema, target_table,
                             effective_key_type, effective_key_columns_json,
                             source_pk_exists, source_uk_exists, topic_name)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (group_id, source_schema, source_table) DO UPDATE
                        SET    target_schema = EXCLUDED.target_schema,
                               target_table = EXCLUDED.target_table,
                               effective_key_type = EXCLUDED.effective_key_type,
                               effective_key_columns_json = EXCLUDED.effective_key_columns_json,
                               source_pk_exists = EXCLUDED.source_pk_exists,
                               source_uk_exists = EXCLUDED.source_uk_exists,
                               topic_name = EXCLUDED.topic_name
                    """, (
                        str(uuid.uuid4()), connector_group_id,
                        src_schema, table_name,
                        tgt_schema, target_table,
                        effective_key_type,
                        json.dumps(effective_key_columns),
                        source_pk_exists,
                        source_uk_exists,
                        topic_name,
                    ))

                cur.execute("""
                    INSERT INTO migrations (
                        migration_id, migration_name, phase, state_changed_at,
                        source_connection_id, target_connection_id,
                        source_schema, source_table,
                        target_schema, target_table,
                        stage_table_name, stage_tablespace,
                        chunk_size, max_parallel_workers,
                        baseline_parallel_degree,
                        source_pk_exists, source_uk_exists,
                        effective_key_type, effective_key_source, effective_key_columns_json,
                        strategy,
                        truncate_target,
                        group_id,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, 'DRAFT', %s,
                        'oracle_source', 'oracle_target',
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s,
                        %s, %s,
                        %s, %s, %s,
                        %s,
                        %s,
                        %s,
                        %s, %s
                    )
                """, (
                    mid, f"{src_schema}.{table_name}", now,
                    src_schema, table_name,
                    tgt_schema, target_table,
                    stage_table, stage_tablespace,
                    chunk_size, max(1, max_workers),
                    max(1, baseline_pd),
                    source_pk_exists, source_uk_exists,
                    effective_key_type, effective_key_source, json.dumps(effective_key_columns),
                    strategy.value,
                    truncate_target,
                    connector_group_id if strategy.has_cdc else None,
                    now, now,
                ))

                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, NULL, 'DRAFT', %s, 'USER')
                """, (mid, f"Added to pack queue {plan_id}"))

                overrides = {
                    "strategy": strategy.value,
                    "truncate_target": truncate_target,
                    "chunk_size": chunk_size,
                    "max_parallel_workers": max_workers,
                }
                cur.execute("""
                    INSERT INTO migration_plan_items
                        (plan_id, table_name, mode, batch_order, sort_order,
                         overrides_json, migration_id, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
                    RETURNING item_id
                """, (
                    plan_id, table_name, "BULK" if not strategy.has_cdc else "CDC",
                    batch_order, idx, json.dumps(overrides), mid,
                ))
                item_id = cur.fetchone()[0]
                created.append({
                    "item_id": item_id,
                    "table": table_name,
                    "migration_id": mid,
                    "batch_order": batch_order,
                })

            cur.execute("""
                UPDATE migration_plans
                SET    status = CASE
                           WHEN status IN ('DONE', 'FAILED', 'CANCELLED') THEN 'READY'
                           ELSE status
                       END
                WHERE  plan_id = %s
            """, (plan_id,))

        conn.commit()
        connector_start = None
        connector_start_error = None
        plan_start = None
        plan_starts: list[dict] = []
        plan_start_error = None
        cdc_queue_kicked = False
        if strategy.has_cdc:
            autostart = _autostart_created_cdc_items(
                connector_group_id,
                connector_group_status,
                plan_id,
                created,
            )
            connector_start = autostart["connector_start"]
            connector_start_error = autostart["connector_start_error"]
            plan_start = autostart["plan_start"]
            plan_starts = autostart["plan_starts"]
            plan_start_error = autostart["plan_start_error"]
            cdc_queue_kicked = bool(autostart.get("cdc_queue_kicked", False))
        item_states = []
        try:
            item_states = _load_created_plan_item_states(conn, created)
        except Exception as exc:
            print(f"[schema_migrations.add_plan_items] item state summary warning: {exc}")
        _state["broadcast"]({
            "type": "schema_migration.plan_items_added",
            "id": sm_id,
            "plan_id": plan_id,
            "count": len(created),
        })
        return jsonify(_build_add_plan_items_response_payload(
            plan_id=plan_id,
            created=created,
            strategy=strategy,
            connector_group_id=connector_group_id,
            item_states=item_states,
            connector_start=connector_start,
            connector_start_error=connector_start_error,
            plan_start=plan_start,
            plan_starts=plan_starts,
            plan_start_error=plan_start_error,
            cdc_queue_kicked=cdc_queue_kicked,
            cdc_pruned_tables=cdc_pruned_tables,
        )), 201
    except ValueError as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        if src_oconn is not None:
            try:
                src_oconn.close()
            except Exception:
                pass
        conn.close()


@bp.post("/api/schema-migrations/<sm_id>/rollback")
def rollback(sm_id: str):
    """Mark all child migrations as CANCELLING. Best-effort: orchestrator
    drives them to CANCELLED."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE migrations m
                SET phase = 'CANCELLING', state_changed_at = NOW(), updated_at = NOW()
                FROM migration_plan_items mpi, schema_migrations sm
                WHERE m.migration_id = mpi.migration_id
                  AND mpi.plan_id = sm.plan_id
                  AND sm.schema_migration_id = %s
                  AND m.phase NOT IN ('COMPLETED', 'CANCELLED', 'FAILED')
            """, (sm_id,))
            affected = cur.rowcount
            conn.commit()
        _state["broadcast"]({"type": "schema_migration.rollback", "id": sm_id, "affected": affected})
        return jsonify({"ok": True, "affected": affected})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()
