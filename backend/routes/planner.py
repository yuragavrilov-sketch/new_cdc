"""Migration Planner API — schema comparison, plan CRUD, launch-order execution."""

import uuid
import json
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request
from db.oracle_browser import get_oracle_conn, get_full_ddl_info, list_tables
from services.strategy import Strategy

bp = Blueprint("planner", __name__)

_state: dict = {}


def init(*, get_conn_fn, row_to_dict_fn, load_configs_fn, broadcast_fn):
    _state["get_conn"] = get_conn_fn
    _state["row_to_dict"] = row_to_dict_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn


def _is_cdc_plan_item(mode, strategy):
    return (
        str(mode or "").upper().startswith("CDC")
        or str(strategy or "").upper().startswith("CDC_")
    )


def _legacy_payload_has_cdc(batches, defaults) -> bool:
    defaults = defaults or {}
    for batch in batches or []:
        for tbl_cfg in batch.get("tables", []) or []:
            mode = tbl_cfg.get("mode", defaults.get("mode"))
            overrides = tbl_cfg.get("overrides", {}) or {}
            strategy = overrides.get("strategy") or defaults.get("strategy")
            if mode is None and strategy is None:
                return True
            if _is_cdc_plan_item(mode, strategy):
                return True
    return False


def _legacy_cdc_error_response():
    return jsonify({
        "error": (
            "Legacy CDC queue flow is disabled. "
            "Add CDC tables through the schema migration screen so the table is "
            "registered in the single CDC pack and Debezium config is synced."
        )
    }), 400


def _can_start_plan_batch(running_items, pending_items) -> bool:
    """Allow overlapping starts across independent CDC and non-CDC load slots."""
    if not running_items:
        return True
    running_has_non_cdc = any(
        not _is_cdc_plan_item(mode, strategy)
        for mode, strategy in running_items
    )
    pending_has_non_cdc = any(
        not _is_cdc_plan_item(mode, strategy)
        for mode, strategy in pending_items
    )
    return not (running_has_non_cdc and pending_has_non_cdc)


def _can_force_queue_cdc_batch(batch_order, pending_items) -> bool:
    """Allow explicit CDC batches to enter NEW; orchestrator still enforces load slots."""
    return batch_order is not None and bool(pending_items) and all(
        _is_cdc_plan_item(mode, strategy)
        for mode, strategy in pending_items
    )


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


def _refresh_queue_positions_best_effort() -> None:
    try:
        from services import orchestrator
        orchestrator._update_queue_positions()
    except Exception as exc:
        print(f"[planner] queue position refresh warning: {exc}")


def _group_table_include_list(tables: list[dict]) -> str:
    entries: list[str] = []
    for table in tables:
        source_schema = str(table.get("source_schema") or "").strip().upper()
        source_table = str(table.get("source_table") or "").strip().upper()
        if not source_schema or not source_table:
            continue
        entry = f"{source_schema}.{source_table}"
        if entry not in entries:
            entries.append(entry)
    return ",".join(entries)


def _group_message_key_columns(tables: list[dict]) -> str:
    parts: list[str] = []
    for table in tables:
        if table.get("source_pk_exists"):
            continue
        raw = table.get("effective_key_columns_json") or "[]"
        try:
            key_columns = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            key_columns = []
        if not key_columns:
            continue
        source_schema = str(table.get("source_schema") or "").strip().upper()
        source_table = str(table.get("source_table") or "").strip().upper()
        cols = ",".join(str(col).strip().upper() for col in key_columns if str(col).strip())
        if source_schema and source_table and cols:
            parts.append(f"{source_schema}.{source_table}:{cols}")
    return ";".join(parts)


def _load_cdc_group_snapshot(cur, group_id) -> dict | None:
    if not group_id:
        return None
    from services.connector_groups import _active_connector_name, _active_topic_prefix, _topic_name

    cur.execute("""
        SELECT group_id, group_name, status, connector_name,
               topic_prefix, consumer_group_prefix, run_id, error_text
        FROM   connector_groups
        WHERE  group_id = %s
    """, (group_id,))
    row = cur.fetchone()
    if not row:
        return None
    group = _state["row_to_dict"](cur, row)
    active_topic_prefix = _active_topic_prefix(group)
    cur.execute("""
        SELECT id, source_schema, source_table, target_schema, target_table,
               effective_key_type, effective_key_columns_json,
               source_pk_exists, source_uk_exists, topic_name, created_at
        FROM   group_tables
        WHERE  group_id = %s
        ORDER BY source_schema, source_table
    """, (group_id,))
    tables = [_state["row_to_dict"](cur, r) for r in cur.fetchall()]
    for table in tables:
        table["topic_name"] = _topic_name(
            active_topic_prefix,
            table["source_schema"],
            table["source_table"],
        )
    group["active_connector_name"] = _active_connector_name(group)
    group["active_topic_prefix"] = active_topic_prefix
    group["tables"] = tables
    group["table_include_list"] = _group_table_include_list(tables)
    group["message_key_columns"] = _group_message_key_columns(tables)
    return group


# ── helpers (reused from target_prep) ────────────────────────────────────────

def _start_next_plan_batch(
    plan_id: int,
    *,
    actor: str = "USER",
    batch_order: int | None = None,
    allow_cdc_queue_when_blocked: bool = False,
) -> dict:
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM migration_plans WHERE plan_id = %s", (plan_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("Plan not found")
            plan = _state["row_to_dict"](cur, row)

            if plan["status"] not in ("READY", "RUNNING"):
                raise ValueError(f"Cannot start plan in status {plan['status']}")

            if batch_order is None:
                cur.execute("""
                    SELECT DISTINCT batch_order
                    FROM migration_plan_items
                    WHERE plan_id = %s AND status = 'PENDING'
                    ORDER BY batch_order
                    LIMIT 1
                """, (plan_id,))
                batch_row = cur.fetchone()
                if not batch_row:
                    raise ValueError("No pending launch orders")
                next_batch = batch_row[0]
            else:
                next_batch = int(batch_order)

            cur.execute("""
                SELECT i.item_id, i.migration_id, i.mode, m.strategy, m.phase
                FROM migration_plan_items i
                LEFT JOIN migrations m ON m.migration_id = i.migration_id
                WHERE i.plan_id = %s AND i.batch_order = %s AND i.status = 'PENDING'
                ORDER BY i.sort_order, i.item_id
            """, (plan_id, next_batch))
            items = cur.fetchall()
            if not items:
                raise ValueError(f"No pending items for launch order {next_batch}")

            cur.execute("""
                SELECT i.mode, m.strategy
                FROM migration_plan_items i
                LEFT JOIN migrations m ON m.migration_id = i.migration_id
                WHERE i.plan_id = %s AND i.status = 'RUNNING'
            """, (plan_id,))
            running_items = cur.fetchall()
            pending_items = [(mode, strategy) for _, _, mode, strategy, _ in items]
            if (
                not _can_start_plan_batch(running_items, pending_items)
                and not (allow_cdc_queue_when_blocked and _can_force_queue_cdc_batch(batch_order, pending_items))
            ):
                raise ValueError("A plan launch order is already running")

            now = datetime.now(timezone.utc).isoformat()
            started_ids = []
            for item_id, migration_id, _, _, phase in items:
                cur.execute("""
                    UPDATE migrations SET phase = 'NEW', state_changed_at = %s, updated_at = %s
                    WHERE migration_id = %s AND phase = 'DRAFT'
                """, (now, now, str(migration_id)))
                if cur.rowcount <= 0:
                    item_status = _plan_item_status_for_phase(phase)
                    if item_status:
                        cur.execute("""
                            UPDATE migration_plan_items SET status = %s
                            WHERE item_id = %s AND status = 'PENDING'
                        """, (item_status, item_id))
                    continue

                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, 'DRAFT', 'NEW', %s, %s)
                """, (str(migration_id), f"Started from pack queue (step {next_batch})", actor))

                cur.execute("""
                    UPDATE migration_plan_items SET status = 'RUNNING'
                    WHERE item_id = %s
                """, (item_id,))
                started_ids.append(str(migration_id))

            cur.execute("""
                UPDATE migration_plans SET status = 'RUNNING', started_at = COALESCE(started_at, %s)
                WHERE plan_id = %s
            """, (now, plan_id))

        conn.commit()
        _refresh_queue_positions_best_effort()

        broadcast = _state["broadcast"]
        for mid in started_ids:
            broadcast({
                "type": "migration_phase",
                "migration_id": mid,
                "phase": "NEW",
            })

        return {
            "batch": next_batch,
            "started": started_ids,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _diff_summary(src: dict, tgt: dict) -> dict:
    tgt_col = {c["name"] for c in tgt["columns"]}
    src_col_map = {c["name"]: c for c in src["columns"]}
    tgt_col_map = {c["name"]: c for c in tgt["columns"]}

    cols_missing = sum(1 for c in src["columns"] if c["name"] not in tgt_col)
    cols_extra = sum(1 for c in tgt["columns"] if c["name"] not in src_col_map)
    cols_type = sum(
        1 for c in src["columns"]
        if c["name"] in tgt_col_map
        and c["data_type"] != tgt_col_map[c["name"]]["data_type"]
    )

    def _idx_key(i: dict) -> tuple:
        return (i["unique"], ",".join(i["columns"]))

    tgt_idx_names = {i["name"] for i in tgt["indexes"]}
    tgt_idx_keys = {_idx_key(i) for i in tgt["indexes"]}
    idx_missing = sum(
        1 for i in src["indexes"]
        if i["name"] not in tgt_idx_names and _idx_key(i) not in tgt_idx_keys
    )
    idx_disabled = sum(1 for i in tgt["indexes"] if i["status"] != "VALID")

    tgt_con_keys = {(c["type_code"], ",".join(c["columns"])) for c in tgt["constraints"]}
    con_missing = sum(
        1 for c in src["constraints"]
        if (c["type_code"], ",".join(c["columns"])) not in tgt_con_keys
    )
    con_disabled = sum(
        1 for c in tgt["constraints"]
        if c["status"] == "DISABLED" and c["type_code"] != "P"
    )

    tgt_trg = {t["name"] for t in tgt["triggers"]}
    trg_missing = sum(1 for t in src["triggers"] if t["name"] not in tgt_trg)

    total = (cols_missing + cols_extra + cols_type + idx_missing
             + idx_disabled + con_missing + con_disabled + trg_missing)
    return {
        "ok": total == 0,
        "total": total,
        "cols_missing": cols_missing,
        "cols_extra": cols_extra,
        "cols_type": cols_type,
        "idx_missing": idx_missing,
        "idx_disabled": idx_disabled,
        "con_missing": con_missing,
        "con_disabled": con_disabled,
        "trg_missing": trg_missing,
    }


# ── Step 1: Schema comparison ────────────────────────────────────────────────

@bp.get("/api/planner/compare-schema")
def compare_schema():
    src_schema = request.args.get("src_schema", "").strip().upper()
    tgt_schema = request.args.get("tgt_schema", "").strip().upper()
    if not src_schema or not tgt_schema:
        return jsonify({"error": "src_schema and tgt_schema required"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    try:
        src_tables = list_tables(src_conn, src_schema)
        tgt_tables_set = set(list_tables(tgt_conn, tgt_schema))

        results = []
        for tbl in src_tables:
            if tbl not in tgt_tables_set:
                results.append({
                    "table": tbl,
                    "exists_in_target": False,
                    "diff": None,
                    "error": None,
                })
                continue
            try:
                si = get_full_ddl_info(src_conn, src_schema, tbl)
                ti = get_full_ddl_info(tgt_conn, tgt_schema, tbl)
                results.append({
                    "table": tbl,
                    "exists_in_target": True,
                    "diff": _diff_summary(si, ti),
                    "error": None,
                })
            except Exception as exc:
                results.append({
                    "table": tbl,
                    "exists_in_target": True,
                    "diff": None,
                    "error": str(exc)[:120],
                })

        return jsonify(results)
    finally:
        src_conn.close()
        tgt_conn.close()


# ── Step 3: FK dependency detection ──────────────────────────────────────────

@bp.get("/api/planner/fk-dependencies")
def fk_dependencies():
    schema = request.args.get("schema", "").strip().upper()
    tables_csv = request.args.get("tables", "").strip().upper()
    if not schema or not tables_csv:
        return jsonify({"error": "schema and tables required"}), 400

    table_set = set(t.strip() for t in tables_csv.split(",") if t.strip())

    configs = _state["load_configs"]()
    try:
        conn = get_oracle_conn("source", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    try:
        deps: dict[str, set] = {t: set() for t in table_set}
        with conn.cursor() as cur:
            placeholders = ",".join(f":t{i}" for i in range(len(table_set)))
            bind = {f"t{i}": t for i, t in enumerate(table_set)}
            bind["s"] = schema
            cur.execute(f"""
                SELECT ac.table_name,
                       rc.table_name AS ref_table
                FROM   all_constraints ac
                JOIN   all_constraints rc
                       ON ac.r_constraint_name = rc.constraint_name
                       AND ac.r_owner = rc.owner
                WHERE  ac.owner = :s
                  AND  ac.constraint_type = 'R'
                  AND  ac.table_name IN ({placeholders})
                  AND  rc.table_name IN ({placeholders})
            """, bind)
            for row in cur.fetchall():
                child, parent = row[0], row[1]
                if child != parent:
                    deps[child].add(parent)

        result = [
            {"table": t, "depends_on": sorted(d)}
            for t, d in deps.items() if d
        ]
        return jsonify(result)
    finally:
        conn.close()


# ── Plan CRUD ─────────────────────────────────────────────────────────────────

@bp.get("/api/planner/plans")
def list_plans():
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.*,
                       COUNT(i.item_id) AS item_count,
                       COUNT(i.item_id) FILTER (WHERE i.status = 'DONE') AS items_done
                FROM migration_plans p
                LEFT JOIN migration_plan_items i ON i.plan_id = p.plan_id
                GROUP BY p.plan_id
                ORDER BY p.created_at DESC
            """)
            rows = [_state["row_to_dict"](cur, r) for r in cur.fetchall()]
        return jsonify(rows)
    finally:
        conn.close()


@bp.get("/api/planner/plans/<int:plan_id>")
def get_plan(plan_id):
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM migration_plans WHERE plan_id = %s", (plan_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Plan not found"}), 404
            plan = _state["row_to_dict"](cur, row)

            cur.execute("""
                SELECT i.*,
                       m.phase,
                       m.strategy,
                       m.rows_loaded,
                       m.total_rows,
                       m.error_text,
                       m.queue_position,
                       m.state_changed_at,
                       cs.total_lag AS cdc_total_lag,
                       cs.rows_applied AS cdc_rows_applied,
                       cs.worker_heartbeat AS cdc_worker_heartbeat
                FROM migration_plan_items i
                LEFT JOIN migrations m ON m.migration_id = i.migration_id
                LEFT JOIN migration_cdc_state cs ON cs.migration_id = m.migration_id
                WHERE i.plan_id = %s
                ORDER BY i.batch_order, i.sort_order
            """, (plan_id,))
            plan["items"] = [_state["row_to_dict"](cur, r) for r in cur.fetchall()]
            plan["cdc_group"] = _load_cdc_group_snapshot(cur, plan.get("connector_group_id"))
        return jsonify(plan)
    finally:
        conn.close()


@bp.delete("/api/planner/plans/<int:plan_id>")
def delete_plan(plan_id):
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM migration_plans WHERE plan_id = %s", (plan_id,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


@bp.post("/api/planner/plans/<int:plan_id>/items")
def add_plan_items(plan_id):
    """Append migrations to an existing plan as pending batches."""
    data = request.json or {}
    batches = data.get("batches", [])
    defaults = data.get("defaults", {})
    connector_group_id = data.get("connector_group_id")

    if not batches:
        return jsonify({"error": "launch orders required"}), 400
    if _legacy_payload_has_cdc(batches, defaults):
        return _legacy_cdc_error_response()

    conn = _state["get_conn"]()
    now = datetime.now(timezone.utc).isoformat()

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM migration_plans WHERE plan_id = %s", (plan_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Plan not found"}), 404
            plan = _state["row_to_dict"](cur, row)

            src_schema = (data.get("src_schema") or plan.get("src_schema") or "").strip().upper()
            tgt_schema = (data.get("tgt_schema") or plan.get("tgt_schema") or "").strip().upper()
            group_id = connector_group_id or plan.get("connector_group_id")
            if not src_schema or not tgt_schema:
                return jsonify({"error": "src_schema and tgt_schema required"}), 400

            cur.execute("""
                SELECT COALESCE(MAX(batch_order), 0)
                FROM   migration_plan_items
                WHERE  plan_id = %s
            """, (plan_id,))
            batch_offset = cur.fetchone()[0] or 0

            items_created = []
            for batch in batches:
                batch_order = batch_offset + int(batch.get("order", 1))
                for idx, tbl_cfg in enumerate(batch.get("tables", [])):
                    table_name = tbl_cfg.get("table", "").strip().upper()
                    if not table_name:
                        continue
                    mode = tbl_cfg.get("mode", defaults.get("mode", "CDC"))
                    overrides = tbl_cfg.get("overrides", {})

                    chunk_size = overrides.get("chunk_size", defaults.get("chunk_size", 1_000_000))
                    max_workers = overrides.get("max_parallel_workers", defaults.get("max_parallel_workers", 1))
                    raw_strategy = overrides.get("strategy") or defaults.get("strategy") or "CDC_STAGE"
                    try:
                        strategy = Strategy.parse(raw_strategy)
                    except ValueError as exc:
                        return jsonify({"error": f"Invalid strategy for {table_name}: {exc}"}), 400
                    raw_truncate = overrides.get("truncate_target")
                    if raw_truncate is None:
                        raw_truncate = defaults.get("truncate_target", True)
                    truncate_target = bool(raw_truncate)
                    if strategy.uses_stage and truncate_target is False:
                        return jsonify({
                            "error": f"Invalid truncate_target for {table_name}: "
                                     "STAGE-стратегия требует TRUNCATE target."
                        }), 400
                    baseline_pd = overrides.get("baseline_parallel_degree", defaults.get("baseline_parallel_degree", 4))

                    mid = str(uuid.uuid4())
                    cur.execute("""
                        INSERT INTO migrations (
                            migration_id, migration_name, phase, state_changed_at,
                            source_connection_id, target_connection_id,
                            source_schema, source_table,
                            target_schema, target_table,
                            chunk_size, max_parallel_workers,
                            baseline_parallel_degree,
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
                            %s,
                            %s,
                            %s,
                            %s,
                            %s, %s
                        )
                    """, (
                        mid, f"{src_schema}.{table_name}", now,
                        src_schema, table_name,
                        tgt_schema, table_name,
                        chunk_size, max(1, int(max_workers)),
                        max(1, int(baseline_pd)),
                        strategy.value,
                        truncate_target,
                        group_id if strategy.has_cdc else None,
                        now, now,
                    ))

                    cur.execute("""
                        INSERT INTO migration_state_history
                            (migration_id, from_phase, to_phase, message, actor_type)
                        VALUES (%s, NULL, 'DRAFT', %s, 'USER')
                    """, (mid, f"Added to pack queue {plan_id}"))

                    cur.execute("""
                        INSERT INTO migration_plan_items
                            (plan_id, table_name, mode, batch_order, sort_order,
                             overrides_json, migration_id, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
                    """, (plan_id, table_name, mode, batch_order, idx,
                          json.dumps(overrides), mid))

                    items_created.append({
                        "table": table_name,
                        "migration_id": mid,
                        "batch_order": batch_order,
                        "mode": mode,
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
        return jsonify({"plan_id": plan_id, "items": items_created})
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


# ── Execute plan (create migrations) ─────────────────────────────────────────

@bp.post("/api/planner/execute")
def execute_plan():
    """Create a plan + all its migrations in DRAFT phase."""
    data = request.json or {}
    src_schema = data.get("src_schema", "").strip().upper()
    tgt_schema = data.get("tgt_schema", "").strip().upper()
    plan_name = data.get("name", "").strip() or f"Plan {src_schema}->{tgt_schema}"
    batches = data.get("batches", [])
    defaults = data.get("defaults", {})
    connector_group_id = data.get("connector_group_id")
    create_group = data.get("create_connector_group")

    if not src_schema or not tgt_schema or not batches:
        return jsonify({"error": "src_schema, tgt_schema, launch orders required"}), 400
    if _legacy_payload_has_cdc(batches, defaults):
        return _legacy_cdc_error_response()

    conn = _state["get_conn"]()
    now = datetime.now(timezone.utc).isoformat()

    try:
        with conn.cursor() as cur:
            # Optionally create connector group
            group_id = None
            if connector_group_id:
                group_id = connector_group_id
            elif create_group:
                group_id = str(uuid.uuid4())
                gname = create_group.get("group_name", f"plan_{src_schema.lower()}")
                tprefix = create_group.get("topic_prefix", f"{src_schema.lower()}")
                cname = create_group.get("connector_name", f"{gname}_connector")
                cprefix = create_group.get("consumer_group_prefix", f"{gname}_cg")
                cur.execute("""
                    INSERT INTO connector_groups
                        (group_id, group_name, source_connection_id,
                         connector_name, topic_prefix, consumer_group_prefix)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (group_id, gname, "oracle_source",
                      cname, tprefix, cprefix))

            # Create plan
            cur.execute("""
                INSERT INTO migration_plans
                    (name, src_schema, tgt_schema, connector_group_id, defaults_json, status)
                VALUES (%s, %s, %s, %s, %s, 'DRAFT')
                RETURNING plan_id
            """, (plan_name, src_schema, tgt_schema, group_id,
                  json.dumps(defaults)))
            plan_id = cur.fetchone()[0]

            # Create migrations + plan items
            items_created = []
            for batch in batches:
                batch_order = batch.get("order", 1)
                for idx, tbl_cfg in enumerate(batch.get("tables", [])):
                    table_name = tbl_cfg.get("table", "").strip().upper()
                    mode = tbl_cfg.get("mode", defaults.get("mode", "CDC"))
                    overrides = tbl_cfg.get("overrides", {})

                    # Merge defaults with overrides
                    chunk_size = overrides.get("chunk_size", defaults.get("chunk_size", 1_000_000))
                    max_workers = overrides.get("max_parallel_workers", defaults.get("max_parallel_workers", 1))
                    raw_strategy = overrides.get("strategy") or defaults.get("strategy") or "CDC_STAGE"
                    try:
                        strategy = Strategy.parse(raw_strategy)
                    except ValueError as exc:
                        return jsonify({"error": f"Invalid strategy for {table_name}: {exc}"}), 400
                    raw_truncate = overrides.get("truncate_target")
                    if raw_truncate is None:
                        raw_truncate = defaults.get("truncate_target", True)
                    truncate_target = bool(raw_truncate)
                    if strategy.uses_stage and truncate_target is False:
                        return jsonify({
                            "error": f"Invalid truncate_target for {table_name}: "
                                     "STAGE-стратегия требует TRUNCATE target (поведение неизменяемо)."
                        }), 400
                    baseline_pd = overrides.get("baseline_parallel_degree", defaults.get("baseline_parallel_degree", 4))

                    mid = str(uuid.uuid4())

                    cur.execute("""
                        INSERT INTO migrations (
                            migration_id, migration_name, phase, state_changed_at,
                            source_connection_id, target_connection_id,
                            source_schema, source_table,
                            target_schema, target_table,
                            chunk_size, max_parallel_workers,
                            baseline_parallel_degree,
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
                            %s,
                            %s,
                            %s,
                            %s,
                            %s, %s
                        )
                    """, (
                        mid, f"{src_schema}.{table_name}", now,
                        src_schema, table_name,
                        tgt_schema, table_name,
                        chunk_size, max(1, int(max_workers)),
                        max(1, int(baseline_pd)),
                        strategy.value,
                        truncate_target,
                        group_id if strategy.has_cdc else None,
                        now, now,
                    ))

                    # Record state history
                    cur.execute("""
                        INSERT INTO migration_state_history
                            (migration_id, from_phase, to_phase, message, actor_type)
                        VALUES (%s, NULL, 'DRAFT', %s, 'USER')
                    """, (mid, f"Created by pack queue {plan_id}"))

                    # Create plan item
                    cur.execute("""
                        INSERT INTO migration_plan_items
                            (plan_id, table_name, mode, batch_order, sort_order,
                             overrides_json, migration_id, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
                    """, (plan_id, table_name, mode, batch_order, idx,
                          json.dumps(overrides), mid))

                    items_created.append({
                        "table": table_name,
                        "migration_id": mid,
                        "batch_order": batch_order,
                        "mode": mode,
                    })

            # Mark plan as READY
            cur.execute("""
                UPDATE migration_plans SET status = 'READY'
                WHERE plan_id = %s
            """, (plan_id,))

        conn.commit()

        return jsonify({
            "plan_id": plan_id,
            "items": items_created,
            "connector_group_id": str(group_id) if group_id else None,
        })
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        conn.close()


# ── Start plan (DRAFT -> NEW by batches) ──────────────────────────────────────

@bp.post("/api/planner/plans/<int:plan_id>/start")
def start_plan(plan_id):
    """Start first batch: transition DRAFT -> NEW for batch_order = 1."""
    try:
        return jsonify(_start_next_plan_batch(plan_id))
    except ValueError as exc:
        msg = str(exc)
        status = 404 if msg == "Plan not found" else 400
        return jsonify({"error": msg}), status
