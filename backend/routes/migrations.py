"""Migrations CRUD, phase-transition, action, and monitoring routes."""

import json
import uuid
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

import services.debezium    as debezium
import services.job_queue   as job_queue
import services.kafka_lag   as kafka_lag_svc
import services.oracle_stage as oracle_stage
from services.strategy import Strategy


def _utc_iso_z(v):
    """Return UTC-normalized ISO8601 string with 'Z' suffix (no double TZ)."""
    if not v:
        return None
    if v.tzinfo is not None:
        v = v.astimezone(timezone.utc).replace(tzinfo=None)
    return v.isoformat() + "Z"


bp = Blueprint("migrations", __name__)

_VALID_PHASES = {
    "DRAFT", "NEW", "PREPARING", "SCN_FIXED",
    "CONNECTOR_STARTING", "CDC_BUFFERING",
    "TOPIC_CREATING",
    "CHUNKING", "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING", "INDEXES_ENABLING",
    "DATA_VERIFYING", "DATA_MISMATCH",
    "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
    "STEADY_STATE", "PAUSED",
    "CANCELLING", "CANCELLED",
    "COMPLETED", "FAILED",
}

_LIST_COLS = """
    migration_id, migration_name, phase, state_changed_at,
    source_connection_id, target_connection_id,
    source_schema, source_table, target_schema, target_table,
    created_at, updated_at,
    error_code, error_text, failed_phase, retry_count,
    description, created_by,
    total_rows, total_chunks, chunks_done, chunks_failed, rows_loaded,
    strategy, truncate_target, group_id, paused, paused_at
"""

_state: dict = {}


def init(get_conn_fn, row_to_dict_fn, db_available_ref, broadcast_fn,
         load_configs_fn=None, enable_indexes_fn=None):
    _state["get_conn"]        = get_conn_fn
    _state["row_to_dict"]     = row_to_dict_fn
    _state["db_available"]    = db_available_ref
    _state["broadcast"]       = broadcast_fn
    _state["load_configs"]    = load_configs_fn
    _state["enable_indexes"]  = enable_indexes_fn


def _db_ok() -> bool:
    return _state["db_available"]["value"]


def _cdc_group_table_keys(group_id: str) -> set[tuple[str, str]]:
    from services.connector_groups import get_group_tables
    return {
        (
            str(t.get("source_schema") or "").strip().upper(),
            str(t.get("source_table") or "").strip().upper(),
        )
        for t in get_group_tables(group_id)
    }


def _cdc_pack_membership_error(source_schema: str, source_table: str) -> str:
    return (
        f"{source_schema}.{source_table} is not registered in the CDC pack. "
        'Add the table through "This migration" before creating a CDC migration.'
    )


def _legacy_cdc_creation_error() -> str:
    return (
        "Legacy direct CDC migration creation is disabled. "
        "Add CDC tables through the schema migration screen so the table is "
        "registered in the single CDC pack, queued and autostarted."
    )


def _link_to_schema_migration(cur, migration_id: str,
                              source_schema: str, target_schema: str,
                              source_table: str, strategy: Strategy) -> None:
    """Линкует только что созданную миграцию к подходящему schema_migration.

    Использует ту же cursor/transaction, что и INSERT в migrations. Если
    подходящего schema_migration нет — тихо выходит (миграция остаётся orphan,
    но всё равно подхватится дашбордом через UPPER-match по схемам).

    Если у найденного schema_migration plan_id ещё NULL — создаёт `migration_plans`
    запись (status=READY, defaults пустые) и обновляет sm.plan_id.
    Затем вставляет строку в migration_plan_items.
    """
    src = (source_schema or "").strip()
    tgt = (target_schema or "").strip()
    if not src or not tgt:
        return

    cur.execute("""
        SELECT schema_migration_id, name, plan_id
        FROM   schema_migrations
        WHERE  UPPER(src_schema) = UPPER(%s)
          AND  UPPER(tgt_schema) = UPPER(%s)
        ORDER  BY created_at DESC
        LIMIT  1
    """, (src, tgt))
    row = cur.fetchone()
    if not row:
        return
    sm_id, sm_name, plan_id = row

    if plan_id is None:
        cur.execute("""
            INSERT INTO migration_plans (name, src_schema, tgt_schema, status)
            VALUES (%s, %s, %s, 'READY')
            RETURNING plan_id
        """, (sm_name or f"{src}→{tgt}", src, tgt))
        plan_id = cur.fetchone()[0]
        cur.execute("""
            UPDATE schema_migrations
            SET    plan_id = %s, updated_at = NOW()
            WHERE  schema_migration_id = %s
        """, (plan_id, sm_id))

    cur.execute("""
        INSERT INTO migration_plan_items
            (plan_id, table_name, mode, batch_order, sort_order,
             overrides_json, migration_id, status)
        VALUES (%s, %s, %s, 1, 0, '{}', %s, 'RUNNING')
    """, (plan_id, source_table or "", strategy.value, migration_id))


@bp.get("/api/migrations")
def list_migrations():
    if not _db_ok():
        return jsonify([])
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {_LIST_COLS} FROM migrations ORDER BY state_changed_at DESC")
                return jsonify([_state["row_to_dict"](cur, r) for r in cur.fetchall()])
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/migrations/<migration_id>")
def get_migration(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM migrations WHERE migration_id = %s", (migration_id,))
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                result = _state["row_to_dict"](cur, row)
                cur.execute("""
                    SELECT id, migration_id, from_phase, to_phase,
                           transition_status, transition_reason, message,
                           actor_type, actor_id, correlation_id, created_at
                    FROM migration_state_history
                    WHERE migration_id = %s
                    ORDER BY created_at DESC
                """, (migration_id,))
                result["history"] = [_state["row_to_dict"](cur, r) for r in cur.fetchall()]
            return jsonify(result)
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations")
def create_migration():
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    body = request.get_json(force=True) or {}
    if not body.get("migration_name", "").strip():
        return jsonify({"error": "migration_name is required"}), 400

    initial_phase = body.get("initial_phase", "DRAFT").strip().upper()
    if initial_phase not in _VALID_PHASES:
        return jsonify({"error": f"Invalid initial_phase: {initial_phase}"}), 400

    try:
        strategy = Strategy.parse(body.get("strategy"))
    except ValueError as exc:
        return jsonify({"error": f"Invalid strategy: {exc}"}), 400
    if strategy.has_cdc:
        return jsonify({"error": _legacy_cdc_creation_error()}), 400

    # ── truncate_target: default TRUE; STAGE forces TRUE ──
    truncate_target = bool(body.get("truncate_target", True))
    if strategy.uses_stage and truncate_target is False:
        return jsonify({
            "error": "STAGE-стратегия требует TRUNCATE target (поведение неизменяемо). "
                     "Используйте DIRECT, если нужно сохранить существующие данные."
        }), 400

    mid = str(uuid.uuid4())
    now = datetime.utcnow()

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                # group_id обязателен только для CDC-стратегий — BULK его не использует.
                group_id = body.get("group_id") or None
                connector_name = ""
                topic_prefix = ""
                consumer_group = ""

                if strategy.has_cdc:
                    if not group_id:
                        return jsonify({"error": "group_id is required for CDC strategies (legacy per-table CDC runtime is no longer supported)"}), 400
                    from services.connector_groups import get_group as _get_group, _active_topic_prefix, refresh_connector_tables
                    group = _get_group(group_id)
                    if not group:
                        return jsonify({"error": f"Группа {group_id} не найдена"}), 404
                    if group.get("status") != "RUNNING":
                        return jsonify({"error": (
                            f"CDC-пачка не запущена (status={group.get('status')}). "
                            "Запустите CDC-пачку перед созданием CDC-миграции."
                        )}), 409
                    src_schema = body.get("source_schema", "").upper()
                    src_table = body.get("source_table", "").upper()
                    if (src_schema, src_table) not in _cdc_group_table_keys(group_id):
                        return jsonify({"error": _cdc_pack_membership_error(src_schema, src_table)}), 400
                    try:
                        refresh_connector_tables(group_id)
                    except Exception as exc:
                        return jsonify({"error": f"CDC runtime config sync failed: {exc}"}), 409
                    connector_name = group["connector_name"]
                    topic_prefix = _active_topic_prefix(group)
                    prefix = group.get("consumer_group_prefix") or group["topic_prefix"]
                    consumer_group = f"{prefix}_{src_schema}_{src_table}"

                cur.execute("""
                    INSERT INTO migrations (
                        migration_id, migration_name, phase, state_changed_at,
                        source_connection_id, target_connection_id,
                        source_schema, source_table, target_schema, target_table,
                        stage_table_name, stage_tablespace,
                        connector_name, topic_prefix, consumer_group,
                        chunk_size, max_parallel_workers, baseline_parallel_degree,
                        baseline_batch_size,
                        validate_hash_sample,
                        source_pk_exists, source_uk_exists,
                        effective_key_type, effective_key_source, effective_key_columns_json,
                        strategy, truncate_target,
                        group_id,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s,
                        %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s,
                        %s, %s
                    )
                """, (
                    mid, body["migration_name"], initial_phase, now,
                    body.get("source_connection_id", ""),
                    body.get("target_connection_id", ""),
                    body.get("source_schema", ""), body.get("source_table", ""),
                    body.get("target_schema", ""), body.get("target_table", ""),
                    body.get("stage_table_name", ""),
                    body.get("stage_tablespace", "").strip().upper(),
                    connector_name,
                    topic_prefix,
                    consumer_group,
                    body.get("chunk_size", 1_000_000),
                    max(1, int(body.get("max_parallel_workers",          1) or 1)),
                    max(1, int(body.get("baseline_parallel_degree",      4) or 4)),
                    max(1000, int(body.get("baseline_batch_size", 500_000) or 500_000)),
                    body.get("validate_hash_sample", False),
                    body.get("source_pk_exists", False), body.get("source_uk_exists", False),
                    body.get("effective_key_type", ""), body.get("effective_key_source", ""),
                    body.get("effective_key_columns_json", "[]"),
                    strategy.value, truncate_target,
                    group_id,
                    now, now,
                ))
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, NULL, %s, %s, 'USER')
                """, (mid, initial_phase, "Migration created"))

                # Best-effort: привязать миграцию к подходящему schema_migration
                # (по совпадению пары схем). Это даёт корректный link через
                # plan_id → migration_plan_items, благодаря которому миграция
                # попадает в дашборд штатным путём.
                try:
                    _link_to_schema_migration(
                        cur, mid,
                        body.get("source_schema", ""),
                        body.get("target_schema", ""),
                        body.get("source_table", ""),
                        strategy,
                    )
                except Exception as link_exc:
                    print(f"[migrations] schema-link failed (non-fatal): {link_exc}")

            conn.commit()
        finally:
            conn.close()

        _state["broadcast"]({
            "type":         "migration_phase",
            "migration_id": mid,
            "phase":        initial_phase,
            "ts":           now.isoformat() + "Z",
        })
        return jsonify({"ok": True, "migration_id": mid}), 201

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/bulk")
def create_migrations_bulk():
    """Создать пачку миграций таблиц с общими параметрами.

    Body: {
        tables: [
            { source_schema, source_table, target_schema, target_table?,
              source_connection_id?, target_connection_id? }, ...
        ],
        strategy:             "CDC_STAGE" | "CDC_DIRECT" | "BULK_STAGE" | "BULK_DIRECT",
        group_id?:            UUID (обязательно для CDC),
        truncate_target?:     bool (default true; STAGE форсит true),
        validate_hash_sample?: bool,
        stage_tablespace?:    str (default "PAYSTAGE"),
        chunk_size?:          int,
        max_parallel_workers?: int,
        baseline_parallel_degree?: int,
    }

    Per-table: PK/UK подбирается автоматически (PK → UK → NONE), стейдж-имя
    `STG_<schema>_<table>`. Целевая таблица по умолчанию совпадает с
    исходной. Per-table вставки изолированы SAVEPOINT'ом — частичные сбои
    не валят всю пачку.

    Ответ: { created: [{table, migration_id}], failed: [{table, error}],
             total: N } HTTP 201 если есть хоть один success, 207 если все упали.
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body = request.get_json(force=True) or {}
    tables = body.get("tables") or []
    if not isinstance(tables, list) or not tables:
        return jsonify({"error": "tables list is required"}), 400
    if len(tables) > 500:
        return jsonify({"error": "Too many tables (max 500 per bulk)"}), 400

    try:
        strategy = Strategy.parse(body.get("strategy"))
    except ValueError as exc:
        return jsonify({"error": f"Invalid strategy: {exc}"}), 400
    if strategy.has_cdc:
        return jsonify({"error": _legacy_cdc_creation_error()}), 400

    truncate_target = bool(body.get("truncate_target", True))
    if strategy.uses_stage and truncate_target is False:
        return jsonify({
            "error": "STAGE-стратегия требует TRUNCATE target. "
                     "Используйте DIRECT, если нужно сохранить существующие данные."
        }), 400

    validate_hash_sample      = bool(body.get("validate_hash_sample", False))
    stage_tablespace          = (body.get("stage_tablespace") or "PAYSTAGE").strip().upper()
    chunk_size                = int(body.get("chunk_size", 1_000_000) or 1_000_000)
    max_parallel_workers      = max(1, int(body.get("max_parallel_workers", 1) or 1))
    baseline_parallel_degree  = max(1, int(body.get("baseline_parallel_degree", 4) or 4))
    baseline_batch_size       = max(1000, int(body.get("baseline_batch_size", 500_000) or 500_000))
    src_conn_id_default       = body.get("source_connection_id") or "oracle_source"
    tgt_conn_id_default       = body.get("target_connection_id") or "oracle_target"

    # Validate group once for CDC strategies
    group_id = body.get("group_id") or None
    group = None
    group_table_keys: set[tuple[str, str]] = set()
    if strategy.has_cdc:
        if not group_id:
            return jsonify({"error": "group_id is required for CDC strategies"}), 400
        from services.connector_groups import get_group as _get_group, refresh_connector_tables
        group = _get_group(group_id)
        if not group:
            return jsonify({"error": f"Группа {group_id} не найдена"}), 404
        if group.get("status") != "RUNNING":
            return jsonify({"error": (
                f"CDC-пачка не запущена (status={group.get('status')}). "
                "Запустите CDC-пачку перед созданием CDC-миграций."
            )}), 409

    if strategy.has_cdc:
        group_table_keys = _cdc_group_table_keys(group_id)
        try:
            refresh_connector_tables(group_id)
        except Exception as exc:
            return jsonify({"error": f"CDC runtime config sync failed: {exc}"}), 409

    # Lazy Oracle source connection — only opened if we actually need to
    # fetch PK/UK info (i.e. there is at least one table to process).
    load_configs = _state.get("load_configs")
    src_oconn = None

    def _ora_src():
        nonlocal src_oconn
        if src_oconn is None and load_configs is not None:
            from db.oracle_browser import get_oracle_conn
            src_oconn = get_oracle_conn("source", load_configs())
        return src_oconn

    created: list[dict] = []
    failed:  list[dict] = []
    now = datetime.utcnow()

    try:
        conn = _state["get_conn"]()
        try:
            for t in tables:
                src_schema = (t.get("source_schema") or "").strip().upper()
                src_table  = (t.get("source_table")  or "").strip().upper()
                tgt_schema = (t.get("target_schema") or "").strip().upper()
                tgt_table  = (t.get("target_table")  or src_table).strip().upper()
                key_label  = f"{src_schema}.{src_table}"

                if not src_schema or not src_table or not tgt_schema:
                    failed.append({"table": key_label or "—",
                                   "error": "source_schema/source_table/target_schema обязательны"})
                    continue

                if strategy.has_cdc and (src_schema, src_table) not in group_table_keys:
                    failed.append({"table": key_label,
                                   "error": _cdc_pack_membership_error(src_schema, src_table)})
                    continue

                # Lookup PK/UK on source
                pk_columns:     list = []
                uk_constraints: list = []
                try:
                    oconn = _ora_src()
                    if oconn is None:
                        raise RuntimeError("load_configs not wired")
                    from db.oracle_browser import get_table_info
                    info = get_table_info(oconn, src_schema, src_table)
                    pk_columns     = info.get("pk_columns")     or []
                    uk_constraints = info.get("uk_constraints") or []
                except Exception as info_exc:
                    failed.append({"table": key_label,
                                   "error": f"Не удалось получить PK/UK: {info_exc}"})
                    continue

                # Auto-derive effective key: PK → UK → NONE
                if pk_columns:
                    eff_type   = "PRIMARY_KEY"
                    eff_source = "PK"
                    eff_cols   = pk_columns
                elif uk_constraints:
                    eff_type   = "UNIQUE_KEY"
                    eff_source = "UK"
                    eff_cols   = uk_constraints[0].get("columns") or []
                else:
                    eff_type   = "NONE"
                    eff_source = "NONE"
                    eff_cols   = []

                # Per-table wiring
                stage_table_name = f"STG_{src_schema}_{src_table}" if strategy.uses_stage else ""
                connector_name   = ""
                topic_prefix     = ""
                consumer_group   = ""
                if strategy.has_cdc and group:
                    from services.connector_groups import _active_topic_prefix
                    connector_name = group["connector_name"]
                    topic_prefix   = _active_topic_prefix(group)
                    prefix         = group.get("consumer_group_prefix") or group["topic_prefix"]
                    consumer_group = f"{prefix}_{src_schema}_{src_table}"

                mid = str(uuid.uuid4())
                migration_name = f"{src_schema}.{src_table}"

                # SAVEPOINT — изолируем per-table сбой, чтобы остальные
                # таблицы успешно вставились.
                with conn.cursor() as cur:
                    cur.execute("SAVEPOINT bulk_item")
                    try:
                        cur.execute("""
                            INSERT INTO migrations (
                                migration_id, migration_name, phase, state_changed_at,
                                source_connection_id, target_connection_id,
                                source_schema, source_table, target_schema, target_table,
                                stage_table_name, stage_tablespace,
                                connector_name, topic_prefix, consumer_group,
                                chunk_size, max_parallel_workers, baseline_parallel_degree,
                                baseline_batch_size,
                                validate_hash_sample,
                                source_pk_exists, source_uk_exists,
                                effective_key_type, effective_key_source, effective_key_columns_json,
                                strategy, truncate_target,
                                group_id,
                                created_at, updated_at
                            ) VALUES (
                                %s, %s, 'DRAFT', %s,
                                %s, %s,
                                %s, %s, %s, %s,
                                %s, %s,
                                %s, %s, %s,
                                %s, %s, %s,
                                %s,
                                %s,
                                %s, %s,
                                %s, %s, %s,
                                %s, %s,
                                %s,
                                %s, %s
                            )
                        """, (
                            mid, migration_name, now,
                            src_conn_id_default, tgt_conn_id_default,
                            src_schema, src_table, tgt_schema, tgt_table,
                            stage_table_name, stage_tablespace,
                            connector_name, topic_prefix, consumer_group,
                            chunk_size, max_parallel_workers, baseline_parallel_degree,
                            baseline_batch_size,
                            validate_hash_sample,
                            bool(pk_columns), bool(uk_constraints),
                            eff_type, eff_source, json.dumps(eff_cols),
                            strategy.value, truncate_target,
                            group_id,
                            now, now,
                        ))
                        cur.execute("""
                            INSERT INTO migration_state_history
                                (migration_id, from_phase, to_phase, message, actor_type)
                            VALUES (%s, NULL, 'DRAFT', 'Bulk-created', 'USER')
                        """, (mid,))
                        try:
                            _link_to_schema_migration(
                                cur, mid, src_schema, tgt_schema, src_table, strategy)
                        except Exception as link_exc:
                            # Линковка best-effort — её сбой не валит item.
                            print(f"[migrations/bulk] schema-link failed for {key_label}: {link_exc}")
                        cur.execute("RELEASE SAVEPOINT bulk_item")
                        created.append({"table": key_label, "migration_id": mid})
                    except Exception as ins_exc:
                        try:
                            cur.execute("ROLLBACK TO SAVEPOINT bulk_item")
                        except Exception:
                            pass
                        failed.append({"table": key_label, "error": str(ins_exc)})

            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        if src_oconn is not None:
            try: src_oconn.close()
            except Exception: pass
        return jsonify({"error": str(exc)}), 500

    if src_oconn is not None:
        try: src_oconn.close()
        except Exception: pass

    # Per-migration broadcast
    for c in created:
        _state["broadcast"]({
            "type":         "migration_phase",
            "migration_id": c["migration_id"],
            "phase":        "DRAFT",
            "ts":           now.isoformat() + "Z",
        })

    status = 201 if created else 207  # 207 — multi-status: all failed
    return jsonify({
        "created": created,
        "failed":  failed,
        "total":   len(tables),
    }), status


@bp.patch("/api/migrations/<migration_id>/phase")
def transition_phase(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    body = request.get_json(force=True) or {}
    to_phase = body.get("to_phase", "").strip().upper()
    if to_phase not in _VALID_PHASES:
        return jsonify({"error": f"Invalid phase: {to_phase}"}), 400

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                from_phase = row[0]
                now = datetime.utcnow()

                update_fields: dict = {
                    "phase":            to_phase,
                    "state_changed_at": now,
                    "updated_at":       now,
                }
                if to_phase == "FAILED":
                    if body.get("error_code"):  update_fields["error_code"] = body["error_code"]
                    if body.get("error_text"):  update_fields["error_text"] = body["error_text"]
                    update_fields["failed_phase"] = from_phase
                if body.get("retry_count") is not None:
                    update_fields["retry_count"] = body["retry_count"]

                set_clause = ", ".join(f"{k} = %s" for k in update_fields)
                cur.execute(
                    f"UPDATE migrations SET {set_clause} WHERE migration_id = %s",
                    [*update_fields.values(), migration_id],
                )
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase,
                         transition_status, transition_reason, message,
                         actor_type, actor_id, correlation_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    migration_id, from_phase, to_phase,
                    body.get("transition_status", "SUCCESS"),
                    body.get("transition_reason"),
                    body.get("message"),
                    body.get("actor_type", "SYSTEM"),
                    body.get("actor_id"),
                    body.get("correlation_id"),
                ))
            conn.commit()
        finally:
            conn.close()

        _state["broadcast"]({
            "type":         "migration_phase",
            "migration_id": migration_id,
            "from_phase":   from_phase,
            "phase":        to_phase,
            "ts":           now.isoformat() + "Z",
        })
        return jsonify({"ok": True, "from_phase": from_phase, "to_phase": to_phase})

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


_DELETABLE_PHASES = {"DRAFT", "CANCELLING", "CANCELLED", "FAILED"}


def _migration_owns_dedicated_connector(group_id) -> bool:
    """Rows linked to connector_groups share that group's runtime."""
    return not group_id


@bp.delete("/api/migrations/<migration_id>")
def delete_migration(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                phase = row[0]
                if phase not in _DELETABLE_PHASES:
                    return jsonify({
                        "error": f"Нельзя удалить миграцию в фазе {phase}. "
                                 f"Допустимо: {', '.join(sorted(_DELETABLE_PHASES))}"
                    }), 409
                cur.execute(
                    "SELECT connector_name, target_connection_id, "
                    "       target_schema, stage_table_name, group_id "
                    "FROM   migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                crow = cur.fetchone()
                connector_name      = crow[0] if crow else None
                target_conn_id      = crow[1] if crow else None
                target_schema       = crow[2] if crow else None
                stage_table_name    = crow[3] if crow else None
                group_id            = crow[4] if crow and len(crow) > 4 else None
                cur.execute(
                    "DELETE FROM migration_state_history WHERE migration_id = %s",
                    (migration_id,),
                )
                # migration_plan_items.migration_id ссылается без ON DELETE
                # CASCADE — иначе DELETE FROM migrations упрётся в FK.
                cur.execute(
                    "DELETE FROM migration_plan_items WHERE migration_id = %s",
                    (migration_id,),
                )
                cur.execute(
                    "DELETE FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
            conn.commit()
        finally:
            conn.close()
        # Delete legacy per-migration Debezium connector best-effort
        # (after DB commit so row is gone). CDC packs own shared runtime.
        if connector_name and _migration_owns_dedicated_connector(group_id):
            try:
                debezium.delete_connector(connector_name)
            except Exception as exc:
                print(f"[delete_migration] connector delete failed (ignored): {exc}")
        # Drop stage table on target Oracle best-effort
        load_configs = _state.get("load_configs")
        if load_configs and target_conn_id and target_schema and stage_table_name:
            try:
                dst_cfg = load_configs().get(target_conn_id, {})
                oracle_stage.drop_stage_table(dst_cfg, target_schema, stage_table_name)
            except Exception as exc:
                print(f"[delete_migration] stage table drop failed (ignored): {exc}")
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Action endpoint (user-triggered transitions)
# ---------------------------------------------------------------------------

_ACTION_TRANSITIONS = {
    "run":            ("DRAFT",           "NEW"),
    "cancel":         (None,              "CANCELLING"),
    "lag_zero":       ("CDC_CATCHING_UP", "CDC_CAUGHT_UP"),   # called by the universal worker
    "retry_verify":   ("DATA_MISMATCH",   "DATA_VERIFYING"),
    "force_complete": ("DATA_MISMATCH",   "COMPLETED"),
}

_ACTIVE_PHASES = {
    "NEW", "PREPARING", "SCN_FIXED",
    "CONNECTOR_STARTING", "CDC_BUFFERING",
    "TOPIC_CREATING",
    "CHUNKING", "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING", "INDEXES_ENABLING",
    "DATA_VERIFYING", "DATA_MISMATCH",
    "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
    "STEADY_STATE",
}

_CDC_WORKER_PHASES = {
    "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP",
    "CDC_CAUGHT_UP", "STEADY_STATE",
}


@bp.post("/api/migrations/<migration_id>/action")
def migration_action(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body   = request.get_json(force=True) or {}
    action = body.get("action", "").strip().lower()

    if action not in _ACTION_TRANSITIONS and action not in {"pause", "resume"}:
        return jsonify({"error": f"Unknown action: {action}. "
                                 f"Valid: {list(_ACTION_TRANSITIONS) + ['pause', 'resume']}"}), 400

    required_from, to_phase = _ACTION_TRANSITIONS.get(action, (None, None))

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                current_phase = row[0]

            if action in {"pause", "resume"}:
                if action == "pause" and current_phase not in _ACTIVE_PHASES:
                    return jsonify({
                        "error": f"Action 'pause' requires active phase, "
                                 f"current phase is '{current_phase}'"
                    }), 409

                now = datetime.utcnow()
                paused = action == "pause"
                final_phase = current_phase
                with conn.cursor() as cur:
                    if action == "resume" and current_phase == "PAUSED":
                        # Legacy compatibility for rows paused by the previous
                        # phase-based implementation. The original phase was not
                        # persisted, so keep the old fallback behavior.
                        final_phase = "BULK_LOADING"
                        cur.execute(
                            "UPDATE migrations SET phase = %s, paused = FALSE, paused_at = NULL, "
                            "state_changed_at = %s, updated_at = %s WHERE migration_id = %s",
                            (final_phase, now, now, migration_id),
                        )
                    else:
                        cur.execute(
                            "UPDATE migrations SET paused = %s, paused_at = %s, updated_at = %s "
                            "WHERE migration_id = %s",
                            (paused, now if paused else None, now, migration_id),
                        )
                    if paused and current_phase in _CDC_WORKER_PHASES:
                        cur.execute("""
                            UPDATE migration_cdc_state
                            SET    worker_id        = NULL,
                                   worker_heartbeat = NULL,
                                   updated_at       = NOW()
                            WHERE  migration_id = %s
                        """, (migration_id,))
                    cur.execute("""
                        INSERT INTO migration_state_history
                            (migration_id, from_phase, to_phase, message, actor_type, actor_id)
                        VALUES (%s, %s, %s, %s, 'USER', %s)
                    """, (migration_id, current_phase, final_phase,
                          body.get("message", f"Action: {action}"),
                          body.get("actor_id")))
                conn.commit()
                event = {
                    "type":         "migration_phase",
                    "migration_id": migration_id,
                    "from_phase":   current_phase,
                    "phase":        final_phase,
                    "paused":       paused,
                    "ts":           now.isoformat() + "Z",
                }
                _state["broadcast"](event)
                return jsonify({
                    "ok": True,
                    "from_phase": current_phase,
                    "to_phase": final_phase,
                    "paused": paused,
                })

            if required_from and current_phase != required_from:
                return jsonify({
                    "error": f"Action '{action}' requires phase '{required_from}', "
                             f"current phase is '{current_phase}'"
                }), 409

            now = datetime.utcnow()
            extra: dict = {}
            if action == "cancel":
                # stop Debezium connector async (best-effort)
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT connector_name, group_id "
                            "FROM migrations WHERE migration_id = %s",
                            (migration_id,),
                        )
                        crow = cur.fetchone()
                    if crow and crow[0] and _migration_owns_dedicated_connector(crow[1]):
                        debezium.delete_connector(crow[0])
                except Exception as exc:
                    print(f"[action/cancel] connector delete failed: {exc}")

            if action == "retry_verify":
                # Clear old data_compare task reference so orchestrator creates a new one
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE migrations SET data_compare_task_id = NULL "
                        "WHERE migration_id = %s",
                        (migration_id,))

            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE migrations SET phase=%s, state_changed_at=%s, updated_at=%s "
                    "WHERE migration_id=%s",
                    (to_phase, now, now, migration_id),
                )
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type, actor_id)
                    VALUES (%s, %s, %s, %s, 'USER', %s)
                """, (migration_id, current_phase, to_phase,
                      body.get("message", f"Action: {action}"),
                      body.get("actor_id")))
            conn.commit()
        finally:
            conn.close()

        _state["broadcast"]({
            "type":         "migration_phase",
            "migration_id": migration_id,
            "from_phase":   current_phase,
            "phase":        to_phase,
            "ts":           now.isoformat() + "Z",
        })
        return jsonify({"ok": True, "from_phase": current_phase, "to_phase": to_phase})

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Monitoring endpoints
# ---------------------------------------------------------------------------

@bp.get("/api/migrations/<migration_id>/chunks")
def get_migration_chunks(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    chunk_type = request.args.get("chunk_type", "BULK").strip().upper()
    if chunk_type not in ("BULK", "BASELINE"):
        chunk_type = "BULK"
    page      = max(1, int(request.args.get("page", 1)))
    page_size = max(1, min(500, int(request.args.get("page_size", 100))))
    status_filter = request.args.get("status", "").strip().upper()
    if status_filter and status_filter not in ("PENDING", "CLAIMED", "RUNNING", "DONE", "FAILED"):
        status_filter = ""
    try:
        conn = _state["get_conn"]()
        try:
            result = job_queue.list_chunks(
                conn, migration_id, chunk_type,
                page=page, page_size=page_size,
                status_filter=status_filter,
            )
            stats  = job_queue.get_chunk_stats(conn, migration_id, chunk_type)
        finally:
            conn.close()
        return jsonify({
            "stats":      stats,
            "chunks":     result["chunks"],
            "total":      result["total"],
            "page":       result["page"],
            "page_size":  result["page_size"],
            "chunk_type": chunk_type,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/migrations/<migration_id>/connector")
def get_connector_status(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT connector_name FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            return jsonify({"error": "Not found"}), 404

        connector_name = row[0]
        if not connector_name:
            return jsonify({"connector_name": None, "status": "NOT_CONFIGURED"})

        status = debezium.get_connector_status(connector_name)
        return jsonify({"connector_name": connector_name, "status": status})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/migrations/<migration_id>/lag")
def get_migration_lag(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT total_lag, lag_by_partition, worker_id,
                           worker_heartbeat, updated_at, rows_applied
                    FROM   migration_cdc_state
                    WHERE  migration_id = %s
                """, (migration_id,))
                row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            return jsonify({"total_lag": None, "message": "CDC state not yet initialised"})

        total_lag, lag_by_partition, worker_id, heartbeat, updated_at, rows_applied = row
        return jsonify({
            "total_lag":        int(total_lag or 0),
            "lag_by_partition": lag_by_partition,
            "worker_id":        worker_id,
            "worker_heartbeat": _utc_iso_z(heartbeat),
            "updated_at":       _utc_iso_z(updated_at),
            "rows_applied":     int(rows_applied or 0),
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.patch("/api/migrations/<migration_id>/workers")
def update_workers(migration_id: str):
    """Update max_parallel_workers / baseline_parallel_degree / baseline_batch_size on the fly."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    body = request.get_json(force=True) or {}

    fields: dict = {}
    if "max_parallel_workers" in body:
        fields["max_parallel_workers"] = max(1, int(body["max_parallel_workers"]))
    if "baseline_parallel_degree" in body:
        fields["baseline_parallel_degree"] = max(1, int(body["baseline_parallel_degree"]))
    if "baseline_batch_size" in body:
        fields["baseline_batch_size"] = max(1000, int(body["baseline_batch_size"]))
    if not fields:
        return jsonify({"error": "Nothing to update"}), 400

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s FOR UPDATE",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404

                fields["updated_at"] = datetime.utcnow()
                set_clause = ", ".join(f"{k} = %s" for k in fields)
                cur.execute(
                    f"UPDATE migrations SET {set_clause} WHERE migration_id = %s",
                    [*fields.values(), migration_id],
                )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, **{k: v for k, v in fields.items() if k != "updated_at"}})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/retry-chunks")
def retry_failed_chunks(migration_id: str):
    """Reset FAILED chunks back to PENDING so workers will retry them.

    Optional query param: chunk_type=BULK|BASELINE (default: reset all failed chunks).
    Allowed phases: BULK_LOADING, BASELINE_LOADING, FAILED.
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    chunk_type = request.args.get("chunk_type", "").strip().upper()
    if chunk_type not in ("BULK", "BASELINE"):
        chunk_type = ""  # reset all types

    _ALLOWED = {"BULK_LOADING", "BASELINE_LOADING", "FAILED"}

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                if row[0] not in _ALLOWED:
                    return jsonify({
                        "error": f"Повтор чанков недоступен в фазе {row[0]}. "
                                 f"Допустимо: {', '.join(sorted(_ALLOWED))}"
                    }), 409

                if chunk_type:
                    cur.execute("""
                        UPDATE migration_chunks
                        SET    status        = 'PENDING',
                               worker_id    = NULL,
                               claimed_at   = NULL,
                               started_at   = NULL,
                               completed_at = NULL,
                               error_text   = NULL,
                               retry_count  = 0
                        WHERE  migration_id = %s
                          AND  status       = 'FAILED'
                          AND  COALESCE(chunk_type, 'BULK') = %s
                    """, (migration_id, chunk_type))
                else:
                    cur.execute("""
                        UPDATE migration_chunks
                        SET    status        = 'PENDING',
                               worker_id    = NULL,
                               claimed_at   = NULL,
                               started_at   = NULL,
                               completed_at = NULL,
                               error_text   = NULL,
                               retry_count  = 0
                        WHERE  migration_id = %s
                          AND  status       = 'FAILED'
                    """, (migration_id,))
                reset_count = cur.rowcount

                # Reset the migration's failed-chunk counter
                cur.execute("""
                    UPDATE migrations
                    SET    chunks_failed = 0,
                           updated_at   = NOW()
                    WHERE  migration_id = %s
                """, (migration_id,))
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, "reset": reset_count})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/full-restart")
def full_restart_migration(migration_id: str):
    """Reset one table migration to a clean state.

    The migration row and pack membership are preserved. Runtime artifacts
    owned by this migration are removed/reset so the coordinator can process it
    again from DRAFT or NEW. CDC connector groups are intentionally not stopped:
    one group can serve multiple tables.
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503

    body = request.get_json(silent=True) or {}
    start = bool(body.get("start", False))
    actor_id = body.get("actor_id")
    message = body.get("message") or (
        "Full restart: reset state and started from pack queue"
        if start else
        "Full restart: reset state to draft"
    )
    to_phase = "NEW" if start else "DRAFT"
    item_status = "RUNNING" if start else "PENDING"
    now = datetime.utcnow()

    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT phase, data_compare_task_id FROM migrations WHERE migration_id = %s FOR UPDATE",
                    (migration_id,),
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                from_phase, data_compare_task_id = row

                cur.execute(
                    "DELETE FROM migration_chunks WHERE migration_id = %s",
                    (migration_id,),
                )
                deleted_chunks = cur.rowcount

                cur.execute(
                    "DELETE FROM migration_cdc_state WHERE migration_id = %s",
                    (migration_id,),
                )
                deleted_cdc_state = cur.rowcount

                cur.execute(
                    "DELETE FROM target_index_jobs WHERE migration_id = %s",
                    (migration_id,),
                )
                deleted_index_jobs = cur.rowcount

                cur.execute(
                    "DELETE FROM target_trigger_jobs WHERE migration_id = %s",
                    (migration_id,),
                )
                deleted_trigger_jobs = cur.rowcount

                deleted_compare_tasks = 0
                if data_compare_task_id:
                    cur.execute(
                        "DELETE FROM data_compare_tasks WHERE task_id = %s",
                        (data_compare_task_id,),
                    )
                    deleted_compare_tasks = cur.rowcount

                cur.execute("""
                    UPDATE migrations
                    SET    phase = %s,
                           state_changed_at = %s,
                           updated_at = %s,
                           queue_position = NULL,
                           error_code = NULL,
                           error_text = NULL,
                           failed_phase = NULL,
                           retry_count = 0,
                           total_rows = NULL,
                           total_chunks = NULL,
                           chunks_done = 0,
                           chunks_failed = 0,
                           rows_loaded = 0,
                           baseline_chunks_total = NULL,
                           baseline_chunks_done = 0,
                           validation_result = NULL,
                           data_compare_task_id = NULL,
                           kafka_lag = NULL,
                           kafka_lag_checked_at = NULL
                    WHERE  migration_id = %s
                """, (to_phase, now, now, migration_id))

                cur.execute("""
                    UPDATE migration_plan_items
                    SET    status = %s
                    WHERE  migration_id = %s
                """, (item_status, migration_id))
                updated_plan_items = cur.rowcount

                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type, actor_id)
                    VALUES (%s, %s, %s, %s, 'USER', %s)
                """, (migration_id, from_phase, to_phase, message, actor_id))
            conn.commit()
        finally:
            conn.close()

        _state["broadcast"]({
            "type": "migration_phase",
            "migration_id": migration_id,
            "from_phase": from_phase,
            "phase": to_phase,
            "ts": now.isoformat() + "Z",
        })
        return jsonify({
            "ok": True,
            "from_phase": from_phase,
            "to_phase": to_phase,
            "started": start,
            "deleted_chunks": deleted_chunks,
            "deleted_cdc_state": deleted_cdc_state,
            "deleted_index_jobs": deleted_index_jobs,
            "deleted_trigger_jobs": deleted_trigger_jobs,
            "deleted_compare_tasks": deleted_compare_tasks,
            "updated_plan_items": updated_plan_items,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/enable-indexes")
def enable_indexes(migration_id: str):
    """Manually trigger INDEXES_ENABLING work.

    Rebuilds indexes and re-enables constraints. Triggers are handled by a
    separate target-trigger job after data verification.
    """
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    fn = _state.get("enable_indexes")
    if fn is None:
        return jsonify({"error": "enable_indexes not wired"}), 500
    try:
        fn(migration_id)
        return jsonify({"ok": True})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/enable-triggers")
def enable_triggers(migration_id: str):
    """Compatibility shortcut: create and start a target-trigger job."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    fn = _state.get("enable_triggers")
    if fn is None:
        return jsonify({"error": "enable_triggers not wired"}), 500
    try:
        return jsonify({"ok": True, "job": fn(migration_id)})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/migrations/<migration_id>/trigger-jobs")
def list_trigger_jobs(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    fn = _state.get("list_trigger_jobs")
    if fn is None:
        return jsonify({"error": "list_trigger_jobs not wired"}), 500
    try:
        return jsonify(fn(migration_id))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/trigger-jobs")
def create_trigger_job(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    fn = _state.get("create_trigger_job")
    if fn is None:
        return jsonify({"error": "create_trigger_job not wired"}), 500
    requested_by = (request.get_json(silent=True) or {}).get("requested_by")
    try:
        return jsonify({"ok": True, "job": fn(migration_id, requested_by)})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/trigger-jobs/<job_id>/run")
def run_trigger_job(migration_id: str, job_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    fn = _state.get("run_trigger_job")
    if fn is None:
        return jsonify({"error": "run_trigger_job not wired"}), 500
    try:
        return jsonify({"ok": True, "job": fn(migration_id, job_id)})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.post("/api/migrations/<migration_id>/restart-baseline")
def restart_baseline(migration_id: str):
    """Restart the baseline phase: delete old BASELINE chunks, TRUNCATE target,
    rebuild unique indexes, re-chunk and re-load."""
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    fn = _state.get("restart_baseline")
    if fn is None:
        return jsonify({"error": "restart_baseline not wired"}), 500
    try:
        fn(migration_id)
        return jsonify({"ok": True})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/migrations/<migration_id>/validation")
def get_validation_result(migration_id: str):
    if not _db_ok():
        return jsonify({"error": "DB unavailable"}), 503
    try:
        conn = _state["get_conn"]()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT validation_result FROM migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            return jsonify({"error": "Not found"}), 404

        raw = row[0]
        if raw is None:
            return jsonify({"result": None, "message": "Validation not yet run"})

        result = raw if isinstance(raw, dict) else json.loads(raw)
        return jsonify({"result": result})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
