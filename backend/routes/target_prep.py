"""Target preparation API — DDL comparison and target object management."""

import os
from datetime import datetime

from flask import Blueprint, jsonify, request, render_template
from db.oracle_browser import get_oracle_conn, get_full_ddl_info, execute_target_action, list_tables
from services.oracle_stage    import sync_target_columns, create_target_table_like_source
from services.oracle_ddl_sync import sync_target_objects

bp = Blueprint(
    "target_prep", __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "..", "templates"),
)

_state: dict = {}


def init(load_configs_fn):
    _state["load_configs"] = load_configs_fn


@bp.get("/api/target-prep/ddl")
def compare_ddl():
    src_schema = request.args.get("src_schema", "").strip().upper()
    src_table  = request.args.get("src_table",  "").strip().upper()
    tgt_schema = request.args.get("tgt_schema", "").strip().upper()
    tgt_table  = request.args.get("tgt_table",  "").strip().upper()

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "src_schema, src_table, tgt_schema, tgt_table required"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
        try:
            src_info = get_full_ddl_info(src_conn, src_schema, src_table)
            tgt_info = get_full_ddl_info(tgt_conn, tgt_schema, tgt_table)
            return jsonify({"source": src_info, "target": tgt_info})
        finally:
            src_conn.close()
            tgt_conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.post("/api/target-prep/action")
def target_action():
    data        = request.json or {}
    action      = data.get("action", "").strip()
    tgt_schema  = data.get("tgt_schema", "").strip().upper()
    tgt_table   = data.get("tgt_table",  "").strip().upper()
    object_name = data.get("object_name", "").strip()

    if not all([action, tgt_schema, tgt_table, object_name]):
        return jsonify({"error": "action, tgt_schema, tgt_table, object_name required"}), 400

    valid = {
        "disable_index", "enable_index",
        "disable_trigger", "enable_trigger",
        "disable_constraint", "enable_constraint",
    }
    if action not in valid:
        return jsonify({"error": f"Invalid action: {action}"}), 400

    configs = _state["load_configs"]()
    try:
        conn = get_oracle_conn("target", configs)
        try:
            execute_target_action(conn, action, tgt_schema, tgt_table, object_name)
            return jsonify({"ok": True})
        finally:
            conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


def _diff_summary(src: dict, tgt: dict) -> dict:
    """Compute diff counts between source and target DDL info dicts."""
    tgt_col     = {c["name"] for c in tgt["columns"]}
    src_col_map = {c["name"]: c for c in src["columns"]}
    tgt_col_map = {c["name"]: c for c in tgt["columns"]}

    cols_missing = sum(1 for c in src["columns"] if c["name"] not in tgt_col)
    cols_extra   = sum(1 for c in tgt["columns"] if c["name"] not in src_col_map)
    cols_type    = sum(
        1 for c in src["columns"]
        if c["name"] in tgt_col_map
        and c["data_type"] != tgt_col_map[c["name"]]["data_type"]
    )

    def _idx_key(i: dict) -> tuple:
        return (i["unique"], ",".join(i["columns"]))

    tgt_idx_names = {i["name"] for i in tgt["indexes"]}
    tgt_idx_keys  = {_idx_key(i) for i in tgt["indexes"]}
    idx_missing   = sum(
        1 for i in src["indexes"]
        if i["name"] not in tgt_idx_names and _idx_key(i) not in tgt_idx_keys
    )
    idx_disabled  = sum(1 for i in tgt["indexes"] if i["status"] != "VALID")

    tgt_con_keys = {(c["type_code"], ",".join(c["columns"])) for c in tgt["constraints"]}
    con_missing  = sum(
        1 for c in src["constraints"]
        if (c["type_code"], ",".join(c["columns"])) not in tgt_con_keys
    )
    con_disabled = sum(
        1 for c in tgt["constraints"]
        if c["status"] == "DISABLED" and c["type_code"] != "P"
    )

    tgt_trg     = {t["name"] for t in tgt["triggers"]}
    trg_missing = sum(1 for t in src["triggers"] if t["name"] not in tgt_trg)

    total = cols_missing + cols_extra + cols_type + idx_missing + idx_disabled + con_missing + con_disabled + trg_missing
    return {
        "ok":           total == 0,
        "total":        total,
        "cols_missing": cols_missing,
        "cols_extra":   cols_extra,
        "cols_type":    cols_type,
        "idx_missing":  idx_missing,
        "idx_disabled": idx_disabled,
        "con_missing":  con_missing,
        "con_disabled": con_disabled,
        "trg_missing":  trg_missing,
    }


@bp.post("/api/target-prep/compare-summary")
def compare_summary():
    """Return diff summary (counts only) for a source/target table pair."""
    data       = request.json or {}
    src_schema = data.get("src_schema", "").strip().upper()
    src_table  = data.get("src_table",  "").strip().upper()
    tgt_schema = data.get("tgt_schema", "").strip().upper()
    tgt_table  = data.get("tgt_table",  "").strip().upper()

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "src_schema, src_table, tgt_schema, tgt_table required"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
        try:
            src_info = get_full_ddl_info(src_conn, src_schema, src_table)
            tgt_info = get_full_ddl_info(tgt_conn, tgt_schema, tgt_table)
            return jsonify(_diff_summary(src_info, tgt_info))
        finally:
            src_conn.close()
            tgt_conn.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.post("/api/target-prep/sync-columns")
def sync_columns():
    """Add columns present in source but missing in target (ALTER TABLE ADD)."""
    data       = request.json or {}
    src_schema = data.get("src_schema", "").strip().upper()
    src_table  = data.get("src_table",  "").strip().upper()
    tgt_schema = data.get("tgt_schema", "").strip().upper()
    tgt_table  = data.get("tgt_table",  "").strip().upper()

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "src_schema, src_table, tgt_schema, tgt_table required"}), 400

    configs = _state["load_configs"]()
    src_cfg = configs.get("oracle_source", {})
    dst_cfg = configs.get("oracle_target", {})
    try:
        result = sync_target_columns(src_cfg, dst_cfg, src_schema, src_table, tgt_schema, tgt_table)
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.post("/api/target-prep/ensure-table")
def ensure_table():
    """Create target table (if missing) matching source structure, then sync columns & objects."""
    data       = request.json or {}
    src_schema = data.get("src_schema", "").strip().upper()
    src_table  = data.get("src_table",  "").strip().upper()
    tgt_schema = data.get("tgt_schema", "").strip().upper()
    tgt_table  = data.get("tgt_table",  "").strip().upper()

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "src_schema, src_table, tgt_schema, tgt_table required"}), 400

    configs = _state["load_configs"]()
    src_cfg = configs.get("oracle_source", {})
    dst_cfg = configs.get("oracle_target", {})

    created = False
    try:
        # Check if target table exists
        tgt_conn = get_oracle_conn("target", configs)
        try:
            existing = set(list_tables(tgt_conn, tgt_schema))
        finally:
            tgt_conn.close()

        if tgt_table not in existing:
            create_target_table_like_source(
                src_cfg, dst_cfg,
                src_schema, src_table,
                tgt_schema, tgt_table,
            )
            created = True

        # Sync columns (add missing)
        col_result = sync_target_columns(
            src_cfg, dst_cfg,
            src_schema, src_table,
            tgt_schema, tgt_table,
        )

        # Sync objects (constraints, indexes, triggers)
        obj_result = sync_target_objects(
            src_cfg, dst_cfg,
            src_schema, src_table,
            tgt_schema, tgt_table,
        )

        return jsonify({
            "created": created,
            "columns": col_result,
            "objects": obj_result,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@bp.post("/api/target-prep/sync-objects")
def sync_objects():
    """Create missing indexes, constraints and/or triggers on target from source."""
    data       = request.json or {}
    src_schema = data.get("src_schema", "").strip().upper()
    src_table  = data.get("src_table",  "").strip().upper()
    tgt_schema = data.get("tgt_schema", "").strip().upper()
    tgt_table  = data.get("tgt_table",  "").strip().upper()
    # optional list: ["constraints", "indexes", "triggers"] — defaults to all
    req_types  = data.get("types")

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "src_schema, src_table, tgt_schema, tgt_table required"}), 400

    types = set(req_types) if req_types else None

    configs = _state["load_configs"]()
    src_cfg = configs.get("oracle_source", {})
    dst_cfg = configs.get("oracle_target", {})
    try:
        result = sync_target_objects(src_cfg, dst_cfg, src_schema, src_table, tgt_schema, tgt_table, types)
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


# ---------------------------------------------------------------------------
# HTML comparison report (print → PDF)
# ---------------------------------------------------------------------------

def _fmt_col_type(c: dict) -> str:
    """Format Oracle column type with precision/length."""
    t = c["data_type"]
    p, s, l = c.get("data_precision"), c.get("data_scale"), c.get("data_length")
    if p is not None:
        return f"{t}({p},{s})" if s else f"{t}({p})"
    if t in ("VARCHAR2", "CHAR", "NVARCHAR2", "RAW") and l:
        return f"{t}({l})"
    return t


@bp.get("/api/target-prep/report")
def compare_report():
    """Return a printable HTML comparison report for a table pair."""
    src_schema = request.args.get("src_schema", "").strip().upper()
    src_table  = request.args.get("src_table",  "").strip().upper()
    tgt_schema = request.args.get("tgt_schema", "").strip().upper()
    tgt_table  = request.args.get("tgt_table",  "").strip().upper()

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return "src_schema, src_table, tgt_schema, tgt_table required", 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
        try:
            src_info = get_full_ddl_info(src_conn, src_schema, src_table)
            tgt_info = get_full_ddl_info(tgt_conn, tgt_schema, tgt_table)
        finally:
            src_conn.close()
            tgt_conn.close()
    except Exception as exc:
        return f"<h1>Ошибка</h1><pre>{exc}</pre>", 503

    diff = _diff_summary(src_info, tgt_info)

    # ── Column rows ──────────────────────────────────────────────────
    src_col_map = {c["name"]: c for c in src_info["columns"]}
    tgt_col_map = {c["name"]: c for c in tgt_info["columns"]}
    all_col_names = list(dict.fromkeys(
        [c["name"] for c in src_info["columns"]]
        + [c["name"] for c in tgt_info["columns"]]
    ))
    col_rows = []
    for name in all_col_names:
        sc = src_col_map.get(name)
        tc = tgt_col_map.get(name)
        if sc and not tc:
            css, status = "miss", "Нет в target"
        elif tc and not sc:
            css, status = "extra", "Лишняя в target"
        elif sc and tc and sc["data_type"] != tc["data_type"]:
            css, status = "diff", "Тип различается"
        else:
            css, status = "ok-row", "OK"
        col_rows.append({
            "name": name, "css": css, "status": status,
            "src_type":    _fmt_col_type(sc) if sc else "",
            "tgt_type":    _fmt_col_type(tc) if tc else "",
            "src_null":    "Y" if sc and sc.get("nullable") else ("N" if sc else ""),
            "tgt_null":    "Y" if tc and tc.get("nullable") else ("N" if tc else ""),
            "src_default": (sc.get("data_default") or "") if sc else "",
            "tgt_default": (tc.get("data_default") or "") if tc else "",
        })

    # ── Index rows (match by name first, then by structure for SYS_ indexes) ──
    def _idx_struct_key(i: dict) -> tuple:
        return (i["unique"], ",".join(i["columns"]))

    src_idx_map = {i["name"]: i for i in src_info["indexes"]}
    tgt_idx_map = {i["name"]: i for i in tgt_info["indexes"]}
    tgt_idx_by_struct = {}
    for i in tgt_info["indexes"]:
        tgt_idx_by_struct.setdefault(_idx_struct_key(i), i)

    matched_tgt_names: set[str] = set()
    idx_rows = []

    for name, si in src_idx_map.items():
        ti = tgt_idx_map.get(name)
        if not ti:
            ti = tgt_idx_by_struct.get(_idx_struct_key(si))
        if ti:
            matched_tgt_names.add(ti["name"])
        ref = si
        if not ti:
            css = "miss"
        elif ti["status"] != "VALID":
            css = "diff"
        else:
            css = "ok-row"
        display_name = name if not ti or ti["name"] == name else f"{name} / {ti['name']}"
        idx_rows.append({
            "name": display_name, "css": css,
            "index_type": ref["index_type"],
            "unique": "Да" if ref["unique"] else "",
            "columns": ", ".join(ref["columns"]),
            "in_source": "Да",
            "tgt_status": ti["status"] if ti else "Нет",
        })

    for name, ti in tgt_idx_map.items():
        if name in matched_tgt_names or name in src_idx_map:
            continue
        idx_rows.append({
            "name": name, "css": "extra",
            "index_type": ti["index_type"],
            "unique": "Да" if ti["unique"] else "",
            "columns": ", ".join(ti["columns"]),
            "in_source": "Нет",
            "tgt_status": ti["status"],
        })

    # ── Constraint rows ──────────────────────────────────────────────
    src_con_map = {c["name"]: c for c in src_info["constraints"]}
    tgt_con_map = {c["name"]: c for c in tgt_info["constraints"]}
    all_con_names = list(dict.fromkeys(
        list(src_con_map) + list(tgt_con_map)
    ))
    con_rows = []
    for name in all_con_names:
        sc = src_con_map.get(name)
        tc = tgt_con_map.get(name)
        ref = sc or tc
        if sc and not tc:
            css = "miss"
        elif tc and not sc:
            css = "extra"
        elif tc and tc["status"] == "DISABLED":
            css = "diff"
        else:
            css = "ok-row"
        con_rows.append({
            "name": name, "css": css,
            "type": ref["type"],
            "columns": ", ".join(ref["columns"]),
            "in_source": "Да" if sc else "Нет",
            "tgt_status": tc["status"] if tc else "Нет",
        })

    # ── Trigger rows ─────────────────────────────────────────────────
    src_trg_map = {t["name"]: t for t in src_info["triggers"]}
    tgt_trg_map = {t["name"]: t for t in tgt_info["triggers"]}
    all_trg_names = list(dict.fromkeys(
        list(src_trg_map) + list(tgt_trg_map)
    ))
    trg_rows = []
    for name in all_trg_names:
        st = src_trg_map.get(name)
        tt = tgt_trg_map.get(name)
        ref = st or tt
        if st and not tt:
            css = "miss"
        elif tt and not st:
            css = "extra"
        elif tt and tt["status"] == "DISABLED":
            css = "diff"
        else:
            css = "ok-row"
        trg_rows.append({
            "name": name, "css": css,
            "trigger_type": ref.get("trigger_type", ""),
            "event": ref.get("event", ""),
            "in_source": "Да" if st else "Нет",
            "tgt_status": tt["status"] if tt else "Нет",
        })

    return render_template(
        "compare_report.html",
        title=f"Сравнение DDL: {src_schema}.{src_table}",
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        src_schema=src_schema, src_table=src_table,
        tgt_schema=tgt_schema, tgt_table=tgt_table,
        diff=diff,
        src_info=src_info, tgt_info=tgt_info,
        src_cols=src_info["columns"], tgt_cols=tgt_info["columns"],
        col_rows=col_rows,
        idx_rows=idx_rows,
        con_rows=con_rows,
        trg_rows=trg_rows,
    )


# ---------------------------------------------------------------------------
# Full-schema comparison report (all tables)
# ---------------------------------------------------------------------------

def _problem_cols(src_info: dict, tgt_info: dict) -> list[dict]:
    """Return only columns with issues (for detail section)."""
    src_map = {c["name"]: c for c in src_info["columns"]}
    tgt_map = {c["name"]: c for c in tgt_info["columns"]}
    rows = []
    for c in src_info["columns"]:
        if c["name"] not in tgt_map:
            rows.append({"name": c["name"], "css": "miss", "src_type": _fmt_col_type(c), "tgt_type": "", "status": "Нет в target"})
        elif c["data_type"] != tgt_map[c["name"]]["data_type"]:
            rows.append({"name": c["name"], "css": "diff", "src_type": _fmt_col_type(c), "tgt_type": _fmt_col_type(tgt_map[c["name"]]), "status": "Тип различается"})
    for c in tgt_info["columns"]:
        if c["name"] not in src_map:
            rows.append({"name": c["name"], "css": "extra", "src_type": "", "tgt_type": _fmt_col_type(c), "status": "Лишняя в target"})
    return rows


def _problem_indexes(src_info: dict, tgt_info: dict) -> list[dict]:
    tgt_name_set = {i["name"] for i in tgt_info["indexes"]}
    tgt_struct    = {(i["unique"], ",".join(i["columns"])) for i in tgt_info["indexes"]}
    rows = []
    for i in src_info["indexes"]:
        key = (i["unique"], ",".join(i["columns"]))
        if i["name"] not in tgt_name_set and key not in tgt_struct:
            rows.append({"name": i["name"], "css": "miss", "columns": ", ".join(i["columns"]), "status": "Нет в target"})
    for i in tgt_info["indexes"]:
        if i["status"] != "VALID":
            rows.append({"name": i["name"], "css": "diff", "columns": ", ".join(i["columns"]), "status": i["status"]})
    return rows


def _problem_constraints(src_info: dict, tgt_info: dict) -> list[dict]:
    tgt_keys = {(c["type_code"], ",".join(c["columns"])) for c in tgt_info["constraints"]}
    tgt_map = {c["name"]: c for c in tgt_info["constraints"]}
    rows = []
    for c in src_info["constraints"]:
        if (c["type_code"], ",".join(c["columns"])) not in tgt_keys:
            rows.append({"name": c["name"], "css": "miss", "type": c["type"], "columns": ", ".join(c["columns"]), "status": "Нет в target"})
    for c in tgt_info["constraints"]:
        if c["status"] == "DISABLED" and c["type_code"] != "P":
            rows.append({"name": c["name"], "css": "diff", "type": c["type"], "columns": ", ".join(c["columns"]), "status": "DISABLED"})
    return rows


def _problem_triggers(src_info: dict, tgt_info: dict) -> list[dict]:
    tgt_names = {t["name"] for t in tgt_info["triggers"]}
    rows = []
    for t in src_info["triggers"]:
        if t["name"] not in tgt_names:
            rows.append({"name": t["name"], "css": "miss", "status": "Нет в target"})
    return rows


@bp.get("/api/target-prep/report-all")
def compare_report_all():
    """Full-schema comparison report: all source tables vs target."""
    src_schema = request.args.get("src_schema", "").strip().upper()
    tgt_schema = request.args.get("tgt_schema", "").strip().upper()
    if not src_schema or not tgt_schema:
        return "src_schema and tgt_schema required", 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
    except Exception as exc:
        return f"<h1>Ошибка подключения</h1><pre>{exc}</pre>", 503

    try:
        src_tables = list_tables(src_conn, src_schema)
        tgt_tables_set = set(list_tables(tgt_conn, tgt_schema))

        _EMPTY_DIFF = {"ok": True, "total": 0, "cols_missing": 0, "cols_extra": 0,
                       "cols_type": 0, "idx_missing": 0, "idx_disabled": 0,
                       "con_missing": 0, "con_disabled": 0, "trg_missing": 0}

        summary_rows = []
        detail_pairs = []

        for tbl in src_tables:
            if tbl not in tgt_tables_set:
                summary_rows.append({
                    "table": tbl, "css": "row-miss", "badge": "miss",
                    "status_text": "Нет в target", "d": _EMPTY_DIFF,
                })
                continue

            try:
                si = get_full_ddl_info(src_conn, src_schema, tbl)
                ti = get_full_ddl_info(tgt_conn, tgt_schema, tbl)
                d = _diff_summary(si, ti)
            except Exception as exc:
                summary_rows.append({
                    "table": tbl, "css": "row-err", "badge": "err",
                    "status_text": f"Ошибка: {str(exc)[:60]}",
                    "d": _EMPTY_DIFF,
                })
                continue

            if d["ok"]:
                summary_rows.append({
                    "table": tbl, "css": "row-ok", "badge": "ok",
                    "status_text": "OK", "d": d,
                })
            else:
                summary_rows.append({
                    "table": tbl, "css": "row-diff", "badge": "warn",
                    "status_text": f"Расхождения: {d['total']}", "d": d,
                })
                detail_pairs.append({
                    "table": tbl, "diff": d,
                    "col_rows": _problem_cols(si, ti),
                    "idx_rows": _problem_indexes(si, ti),
                    "con_rows": _problem_constraints(si, ti),
                    "trg_rows": _problem_triggers(si, ti),
                })
    finally:
        src_conn.close()
        tgt_conn.close()

    ok_count = sum(1 for r in summary_rows if r["badge"] == "ok")
    diff_count = sum(1 for r in summary_rows if r["badge"] == "warn")
    error_count = sum(1 for r in summary_rows if r["badge"] == "err")
    no_target_count = sum(1 for r in summary_rows if r["badge"] == "miss")

    return render_template(
        "compare_report_all.html",
        title=f"Сравнение DDL: {src_schema} → {tgt_schema}",
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        src_schema=src_schema, tgt_schema=tgt_schema,
        total_tables=len(src_tables),
        compared_count=ok_count + diff_count + error_count,
        ok_count=ok_count, diff_count=diff_count,
        error_count=error_count, no_target_count=no_target_count,
        summary_rows=summary_rows,
        detail_pairs=detail_pairs,
    )
