"""Connector Groups — CRUD + lifecycle + tables API."""

from flask import Blueprint, jsonify, request

bp = Blueprint("connector_groups", __name__)

_state: dict = {}


def init(*, get_conn_fn, row_to_dict_fn, load_configs_fn, broadcast_fn):
    _state["get_conn"] = get_conn_fn
    _state["row_to_dict"] = row_to_dict_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn

    # Initialise the service layer
    from services.connector_groups import init as svc_init
    svc_init(
        get_conn_fn=get_conn_fn,
        row_to_dict_fn=row_to_dict_fn,
        load_configs_fn=load_configs_fn,
    )


def _r2d(cur, row):
    return _state["row_to_dict"](cur, row)


def _legacy_cdc_migration_error() -> dict:
    return {
        "error": (
            "Legacy direct CDC migration flow is disabled. "
            "Add CDC tables through the schema migration screen so the table is "
            "registered in the single CDC pack, queued and autostarted."
        )
    }


def _legacy_cdc_membership_error() -> dict:
    return {
        "error": (
            "Direct CDC pack table edits are disabled. "
            "Add CDC tables through the schema migration screen so each table gets "
            "a migration row, enters the queue and is autostarted."
        )
    }


def _legacy_cdc_group_create_error() -> dict:
    return {
        "error": (
            "Direct CDC pack creation is disabled. "
            "Add the first CDC table through the schema migration screen so the "
            "single CDC pack is created, queued and autostarted."
        )
    }


# ── CRUD ──────────────────────────────────────────────────────────────────────

def _pending_cdc_plan_batches_for_group(group_id: str) -> list[tuple[int, int]]:
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT i.plan_id, i.batch_order
                FROM   migration_plan_items i
                JOIN   migrations m ON m.migration_id = i.migration_id
                WHERE  m.group_id = %s
                  AND  i.status = 'PENDING'
                  AND  m.phase = 'DRAFT'
                  AND  LEFT(COALESCE(m.strategy, ''), 4) = 'CDC_'
                ORDER BY i.plan_id, i.batch_order
            """, (group_id,))
            return [(int(row[0]), int(row[1])) for row in cur.fetchall()]
    finally:
        conn.close()


def _start_pending_cdc_plan_batches_for_group(group_id: str) -> list[dict]:
    from routes.planner import _start_next_plan_batch
    from services import orchestrator

    starts = []
    for plan_id, batch_order in _pending_cdc_plan_batches_for_group(group_id):
        starts.append(_start_next_plan_batch(
            plan_id,
            actor="SYSTEM",
            batch_order=batch_order,
            allow_cdc_queue_when_blocked=True,
        ))
    if starts:
        orchestrator._update_queue_positions()
        orchestrator._kick_new_migrations_for_group(group_id)
    return starts


def _has_existing_new_cdc_rows_for_group(group_id: str) -> bool:
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1
                FROM   migrations
                WHERE  group_id = %s
                  AND  phase = 'NEW'
                  AND  LEFT(COALESCE(strategy, ''), 4) = 'CDC_'
                LIMIT  1
            """, (group_id,))
            return cur.fetchone() is not None
    finally:
        conn.close()


def _kick_existing_new_cdc_for_running_group(group_id: str) -> bool:
    """Resume CDC rows already in NEW when the connector is RUNNING."""
    from services import orchestrator
    from services.connector_groups import get_group as svc_get

    group = svc_get(group_id)
    if not group or str(group.get("status") or "").upper() != "RUNNING":
        return False
    if not _has_existing_new_cdc_rows_for_group(group_id):
        return False
    orchestrator._update_queue_positions()
    orchestrator._kick_new_migrations_for_group(group_id)
    return True


def _kick_group_lifecycle_best_effort(group_id: str) -> bool:
    try:
        from services import orchestrator

        return bool(orchestrator.kick_connector_group_lifecycle(group_id))
    except Exception as exc:
        print(f"[connector_groups] connector lifecycle kick warning: {exc}")
        return False


def _build_cdc_group_next_action(
    *,
    status: str | None,
    plan_starts: list[dict],
    plan_start_error: str | None,
    cdc_queue_kicked: bool,
) -> dict:
    normalized = str(status or "").upper()
    started_count = sum(len(item.get("started") or []) for item in plan_starts)

    if plan_start_error:
        return {
            "level": "error",
            "code": "PLAN_START_FAILED",
            "message": f"CDC-пачка обработана, но очередь не продолжена: {plan_start_error}",
        }
    if started_count:
        if normalized == "RUNNING":
            return {
                "level": "ok",
                "code": "QUEUED",
                "message": f"CDC-строки поставлены в очередь: {started_count}.",
            }
        return {
            "level": "warn",
            "code": "WAITING_CONNECTOR",
            "message": f"CDC-строки ждут CDC-пачку ({normalized or 'UNKNOWN'}): {started_count}.",
        }
    if cdc_queue_kicked:
        return {
            "level": "ok",
            "code": "QUEUE_KICKED",
            "message": "CDC-очередь проверена и продолжена.",
        }
    if normalized in ("TOPICS_CREATING", "CONNECTOR_STARTING"):
        return {
            "level": "warn",
            "code": "WAITING_CONNECTOR",
            "message": f"CDC-пачка запускается ({normalized}); строки продолжат работу после RUNNING.",
        }
    if normalized == "FAILED":
        return {
            "level": "error",
            "code": "CONNECTOR_FAILED",
            "message": "CDC-пачка в FAILED; исправьте ошибку и запустите её снова.",
        }
    if normalized == "RUNNING":
        return {
            "level": "info",
            "code": "NO_PENDING_CDC_ROWS",
            "message": "CDC-пачка RUNNING; новых CDC-строк для запуска не найдено.",
        }
    return {
        "level": "info",
        "code": "NO_PENDING_CDC_ROWS",
        "message": "Новых CDC-строк для запуска не найдено.",
    }


@bp.get("/api/connector-groups")
def list_groups():
    from services.connector_groups import list_groups as svc_list
    groups = svc_list()
    return jsonify(groups)


@bp.get("/api/connector-groups/<group_id>")
def get_group(group_id: str):
    from services.connector_groups import (
        get_group as svc_get,
        get_group_tables,
        get_group_migrations,
        _build_key_columns,
        _build_table_include_list,
    )
    group = svc_get(group_id)
    if not group:
        return jsonify({"error": "Группа не найдена"}), 404
    group["tables"] = get_group_tables(group_id)
    group["migrations"] = get_group_migrations(group_id)
    group["table_include_list"] = _build_table_include_list(group_id)
    group["message_key_columns"] = _build_key_columns(group_id)
    return jsonify(group)


@bp.post("/api/connector-groups")
def create_group():
    body = request.get_json(force=True)
    required = ("group_name", "connector_name", "topic_prefix")
    missing = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"Отсутствуют поля: {', '.join(missing)}"}), 400
    return jsonify(_legacy_cdc_group_create_error()), 400


@bp.post("/api/connector-groups/wizard")
def create_group_wizard():
    """Legacy endpoint: CDC pack membership is managed by schema migrations."""
    body = request.get_json(force=True)

    # ── validate group fields ──────────────────────────────────────────────
    required = ("group_name", "connector_name", "topic_prefix")
    missing = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"Отсутствуют поля: {', '.join(missing)}"}), 400

    tables = body.get("tables")
    if not tables or not isinstance(tables, list) or len(tables) == 0:
        return jsonify({"error": "Нужно выбрать хотя бы одну таблицу"}), 400
    return jsonify(_legacy_cdc_membership_error()), 400


@bp.delete("/api/connector-groups/<group_id>")
def delete_group(group_id: str):
    from services.connector_groups import delete_group as svc_delete
    force = request.args.get("force", "").lower() in ("1", "true", "yes")
    try:
        svc_delete(group_id, force=force)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return "", 204


# ── Group tables ──────────────────────────────────────────────────────────────

@bp.get("/api/connector-groups/<group_id>/tables")
def list_group_tables(group_id: str):
    from services.connector_groups import get_group_tables
    return jsonify(get_group_tables(group_id))


@bp.post("/api/connector-groups/<group_id>/tables")
def add_group_tables(group_id: str):
    body = request.get_json(force=True)
    tables = body.get("tables", [])
    if not tables:
        return jsonify({"error": "Нужно указать хотя бы одну таблицу"}), 400
    return jsonify(_legacy_cdc_membership_error()), 400


@bp.post("/api/connector-groups/<group_id>/tables/prune")
def prune_group_tables(group_id: str):
    body = request.get_json(force=True)
    keep_tables = body.get("keep_tables", [])
    if not isinstance(keep_tables, list) or not keep_tables:
        return jsonify({"error": "keep_tables must contain at least one table"}), 400

    from services.connector_groups import prune_tables, refresh_connector_tables
    try:
        removed = prune_tables(group_id, keep_tables)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        refresh_connector_tables(group_id)
    except Exception as exc:
        return jsonify({
            "removed": removed,
            "removed_count": len(removed),
            "sync_error": f"CDC runtime config sync failed: {exc}",
        }), 200
    return jsonify({
        "removed": removed,
        "removed_count": len(removed),
        "synced": True,
    })


@bp.delete("/api/connector-groups/<group_id>/tables/<source_schema>/<source_table>")
def remove_group_table(group_id: str, source_schema: str, source_table: str):
    from services.connector_groups import remove_table, refresh_connector_tables
    try:
        remove_table(group_id, source_schema, source_table)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        refresh_connector_tables(group_id)
    except Exception as exc:
        return jsonify({
            "removed": True,
            "sync_error": f"CDC runtime config sync failed: {exc}",
        }), 200
    return "", 204


# ── Debezium config preview ──────────────────────────────────────────────────

@bp.get("/api/connector-groups/<group_id>/debezium-config")
def debezium_config(group_id: str):
    """Return the Debezium connector config that would be sent to Kafka Connect."""
    from services.connector_groups import build_connector_config
    try:
        cfg = build_connector_config(group_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify(cfg)


@bp.get("/api/connector-groups/<group_id>/debezium-sync-status")
def debezium_sync_status(group_id: str):
    """Compare desired state DB connector config with actual Kafka Connect config."""
    from services.connector_groups import get_debezium_sync_status
    try:
        return jsonify(get_debezium_sync_status(group_id))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


# ── CDC readiness check ──────────────────────────────────────────────────────

@bp.post("/api/connector-groups/check-readiness")
def check_readiness():
    """Check ARCHIVELOG / supplemental logging readiness for a list of tables.

    Body: {"source_connection_id": "oracle_source",
           "tables": [{"source_schema": "X", "source_table": "Y"}, ...]}
    """
    body = request.get_json(force=True) or {}
    src_conn_id = body.get("source_connection_id", "oracle_source")
    tables = body.get("tables", [])
    if not tables:
        return jsonify({"error": "tables is required"}), 400

    from services.connector_groups import check_cdc_readiness
    try:
        result = check_cdc_readiness(src_conn_id, tables)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify(result)


# ── Kafka topics ──────────────────────────────────────────────────────────────

@bp.post("/api/connector-groups/<group_id>/create-topics")
def create_topics(group_id: str):
    """Pre-create Kafka topics for all tables in the group."""
    from services.connector_groups import create_group_topics
    try:
        results = create_group_topics(group_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify(results)


@bp.get("/api/connector-groups/<group_id>/topic-counts")
def topic_counts(group_id: str):
    """Return message counts for each topic in the group."""
    from services.connector_groups import get_topic_message_counts
    try:
        counts = get_topic_message_counts(group_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    return jsonify(counts)


# ── Lifecycle ─────────────────────────────────────────────────────────────────

@bp.post("/api/connector-groups/<group_id>/start")
def start_group(group_id: str):
    from services.connector_groups import request_start
    try:
        result = request_start(group_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    _state["broadcast"]({
        "type": "connector_group_status",
        "group_id": group_id,
        "status": result["status"],
    })
    _kick_group_lifecycle_best_effort(group_id)
    plan_starts = []
    plan_start_error = None
    cdc_queue_kicked = False
    try:
        plan_starts = _start_pending_cdc_plan_batches_for_group(group_id)
        cdc_queue_kicked = bool(plan_starts)
        if not plan_starts and str(result.get("status") or "").upper() == "RUNNING":
            cdc_queue_kicked = _kick_existing_new_cdc_for_running_group(group_id)
    except Exception as exc:
        plan_start_error = str(exc)
    result["plan_starts"] = plan_starts
    result["plan_start_error"] = plan_start_error
    result["cdc_queue_kicked"] = cdc_queue_kicked
    result["cdc_next_action"] = _build_cdc_group_next_action(
        status=result.get("status"),
        plan_starts=plan_starts,
        plan_start_error=plan_start_error,
        cdc_queue_kicked=cdc_queue_kicked,
    )
    return jsonify(result)


@bp.post("/api/connector-groups/<group_id>/stop")
def stop_group(group_id: str):
    from services.connector_groups import request_stop
    try:
        request_stop(group_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    _state["broadcast"]({
        "type": "connector_group_status",
        "group_id": group_id,
        "status": "STOPPING",
    })
    return jsonify({"status": "STOPPING"})


@bp.get("/api/connector-groups/<group_id>/status")
def group_status(group_id: str):
    from services.connector_groups import get_connector_status, get_group as svc_get
    status = get_connector_status(group_id)
    group = svc_get(group_id)
    return jsonify({"status": status, "group": group})


@bp.get("/api/connector-groups/<group_id>/history")
def group_history(group_id: str):
    from services.connector_groups import get_group_history
    return jsonify(get_group_history(group_id))


@bp.post("/api/connector-groups/<group_id>/refresh-tables")
def refresh_tables(group_id: str):
    """Re-sync table.include.list and message.key.columns from group_tables."""
    from services.connector_groups import get_group, refresh_connector_tables
    try:
        refresh_connector_tables(group_id)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    group = get_group(group_id) or {}
    plan_starts = []
    plan_start_error = None
    cdc_queue_kicked = False
    try:
        plan_starts = _start_pending_cdc_plan_batches_for_group(group_id)
        cdc_queue_kicked = bool(plan_starts)
        if not plan_starts:
            cdc_queue_kicked = _kick_existing_new_cdc_for_running_group(group_id)
    except Exception as exc:
        plan_start_error = str(exc)
    return jsonify({
        "ok": True,
        "status": group.get("status"),
        "plan_starts": plan_starts,
        "plan_start_error": plan_start_error,
        "cdc_queue_kicked": cdc_queue_kicked,
        "cdc_next_action": _build_cdc_group_next_action(
            status=group.get("status"),
            plan_starts=plan_starts,
            plan_start_error=plan_start_error,
            cdc_queue_kicked=cdc_queue_kicked,
        ),
    })


# ── Create migration from group table ─────────────────────────────────────────

@bp.post("/api/connector-groups/<group_id>/create-migration")
def create_migration_from_table(group_id: str):
    """Legacy endpoint kept only to reject old CDC migration creation."""
    return jsonify(_legacy_cdc_migration_error()), 400
