"""
Sync non-table DDL objects from source to target Oracle.
Tables are handled by existing oracle_stage.py and oracle_ddl_sync.py.
"""
from db.oracle_browser import (
    get_source_code, get_view_info,
    get_mview_info, get_sequence_info, get_synonym_info,
    get_trigger_info, get_index_info,
)


def _exec_on_target(tgt_conn, sql: str):
    """Execute DDL on target and commit."""
    with tgt_conn.cursor() as cur:
        cur.execute(sql)
    tgt_conn.commit()


def _drop_ignore_missing(tgt_conn, ddl: str) -> None:
    try:
        _exec_on_target(tgt_conn, ddl)
    except Exception:
        pass


def sync_view(src_conn, tgt_conn, source_schema: str, target_schema: str, name: str) -> dict:
    """CREATE OR REPLACE VIEW on target from source definition."""
    info = get_view_info(src_conn, source_schema, name)
    if not info.get("sql_text"):
        return {"error": f"View {name} has no SQL text on source"}
    ddl = f'CREATE OR REPLACE VIEW "{target_schema}"."{name}" AS\n{info["sql_text"]}'
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name, "ddl": ddl}


def sync_mview(src_conn, tgt_conn, source_schema: str, target_schema: str, name: str) -> dict:
    """CREATE MATERIALIZED VIEW on target. Drops existing first."""
    info = get_mview_info(src_conn, source_schema, name)
    if not info.get("sql_text"):
        return {"error": f"MView {name} has no SQL text on source"}
    _drop_ignore_missing(tgt_conn, f'DROP MATERIALIZED VIEW "{target_schema}"."{name}"')
    refresh = info.get("refresh_type", "FORCE/DEMAND")
    method = refresh.split("/")[0] if "/" in refresh else "FORCE"
    ddl = f'CREATE MATERIALIZED VIEW "{target_schema}"."{name}" REFRESH {method} AS\n{info["sql_text"]}'
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name, "ddl": ddl}


def sync_code_object(src_conn, tgt_conn, source_schema: str, _target_schema: str, name: str, obj_type: str) -> dict:
    """CREATE OR REPLACE function/procedure/type on target."""
    if obj_type == "PACKAGE":
        spec = get_source_code(src_conn, source_schema, name, "PACKAGE")
        body = get_source_code(src_conn, source_schema, name, "PACKAGE BODY")
        if spec:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {spec}')
        if body:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {body}')
        return {"action": "compiled", "object": name, "spec": bool(spec), "body": bool(body)}
    elif obj_type == "TYPE":
        src = get_source_code(src_conn, source_schema, name, "TYPE")
        body = get_source_code(src_conn, source_schema, name, "TYPE BODY")
        if src:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {src}')
        if body:
            _exec_on_target(tgt_conn, f'CREATE OR REPLACE {body}')
        return {"action": "compiled", "object": name, "source": bool(src), "body": bool(body)}
    else:
        code = get_source_code(src_conn, source_schema, name, obj_type)
        if not code:
            return {"error": f"{obj_type} {name} has no source code on source"}
        _exec_on_target(tgt_conn, f'CREATE OR REPLACE {code}')
        return {"action": "compiled", "object": name}


def sync_sequence(src_conn, tgt_conn, source_schema: str, target_schema: str, name: str, action: str = "create") -> dict:
    """Create or alter sequence on target."""
    info = get_sequence_info(src_conn, source_schema, name)
    if not info:
        return {"error": f"Sequence {name} not found on source"}

    if action in ("create", "create_missing", "recreate"):
        if action == "recreate":
            _drop_ignore_missing(tgt_conn, f'DROP SEQUENCE "{target_schema}"."{name}"')
        ddl = (
            f'CREATE SEQUENCE "{target_schema}"."{name}"'
            f' MINVALUE {info["min_value"]}'
            f' MAXVALUE {info["max_value"]}'
            f' INCREMENT BY {info["increment_by"]}'
            f' CACHE {info["cache_size"]}'
            f' START WITH {info["last_number"]}'
        )
        _exec_on_target(tgt_conn, ddl)
        return {"action": "created", "object": name}
    else:
        ddl = (
            f'ALTER SEQUENCE "{target_schema}"."{name}"'
            f' INCREMENT BY {info["increment_by"]}'
            f' MINVALUE {info["min_value"]}'
            f' MAXVALUE {info["max_value"]}'
            f' CACHE {info["cache_size"]}'
        )
        _exec_on_target(tgt_conn, ddl)
        return {"action": "altered", "object": name}


def sync_synonym(src_conn, tgt_conn, source_schema: str, target_schema: str, name: str) -> dict:
    """CREATE OR REPLACE SYNONYM on target."""
    info = get_synonym_info(src_conn, source_schema, name)
    if not info:
        return {"error": f"Synonym {name} not found on source"}
    target_ref = f'"{info["table_owner"]}"."{info["table_name"]}"'
    if info.get("db_link"):
        target_ref += f'@{info["db_link"]}'
    ddl = f'CREATE OR REPLACE SYNONYM "{target_schema}"."{name}" FOR {target_ref}'
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name, "ddl": ddl}


def sync_trigger(src_conn, tgt_conn, source_schema: str, target_schema: str, name: str) -> dict:
    """CREATE OR REPLACE one trigger on target from source all_triggers metadata."""
    info = get_trigger_info(src_conn, source_schema, name)
    if not info:
        return {"error": f"Trigger {name} not found on source"}
    table_name = info.get("table_name")
    body = info.get("trigger_body") or ""
    if not table_name or not body:
        return {"error": f"Trigger {name} has incomplete metadata on source"}
    ddl = (
        f'CREATE OR REPLACE TRIGGER "{target_schema}"."{name}"\n'
        f'{info.get("trigger_type") or ""} {info.get("triggering_event") or ""}\n'
        f'ON "{target_schema}"."{table_name}"\n'
    )
    if info.get("when_clause"):
        ddl += f'WHEN ({info["when_clause"]})\n'
    ddl += body
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name, "ddl": ddl}


def sync_index(src_conn, tgt_conn, source_schema: str, target_schema: str, name: str, action: str) -> dict:
    """Create or recreate a simple normal index on target."""
    info = get_index_info(src_conn, source_schema, name)
    if not info:
        return {"error": f"Index {name} not found on source"}
    index_type = str(info.get("index_type") or "").upper()
    if not index_type.startswith("NORMAL"):
        return {"error": f"Index {name} type {index_type or 'UNKNOWN'} is not supported by DDL job"}
    columns = info.get("columns") or []
    if not columns:
        return {"error": f"Index {name} has no indexed columns on source"}
    if action == "recreate":
        _drop_ignore_missing(tgt_conn, f'DROP INDEX "{target_schema}"."{name}"')
    unique_kw = "UNIQUE " if info.get("uniqueness") == "UNIQUE" else ""
    reverse_kw = " REVERSE" if index_type == "NORMAL/REV" else ""
    col_sql = ", ".join(
        f'"{col["name"]}" DESC' if col.get("descending") else f'"{col["name"]}"'
        for col in columns
    )
    ddl = (
        f'CREATE {unique_kw}INDEX "{target_schema}"."{name}" '
        f'ON "{target_schema}"."{info["table_name"]}" ({col_sql}){reverse_kw}'
    )
    _exec_on_target(tgt_conn, ddl)
    return {"action": "created", "object": name, "ddl": ddl}


# ── Dispatcher ───────────────────────────────────────────────────────────────

def sync_to_target(src_conn, tgt_conn, schema: str, name: str,
                   object_type: str, action: str = "create",
                   target_schema: str | None = None) -> dict:
    """Route sync request to the correct handler."""
    source_schema = schema
    target_schema = target_schema or schema
    if object_type == "VIEW":
        return sync_view(src_conn, tgt_conn, source_schema, target_schema, name)
    elif object_type == "MATERIALIZED VIEW":
        return sync_mview(src_conn, tgt_conn, source_schema, target_schema, name)
    elif object_type in ("FUNCTION", "PROCEDURE", "PACKAGE", "TYPE"):
        return sync_code_object(src_conn, tgt_conn, source_schema, target_schema, name, object_type)
    elif object_type == "SEQUENCE":
        return sync_sequence(src_conn, tgt_conn, source_schema, target_schema, name, action)
    elif object_type == "SYNONYM":
        return sync_synonym(src_conn, tgt_conn, source_schema, target_schema, name)
    elif object_type == "TRIGGER":
        return sync_trigger(src_conn, tgt_conn, source_schema, target_schema, name)
    elif object_type == "INDEX":
        return sync_index(src_conn, tgt_conn, source_schema, target_schema, name, action)
    else:
        return {"error": f"Unsupported object type: {object_type}"}
