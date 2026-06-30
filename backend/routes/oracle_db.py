"""Oracle DB browser routes — used by the migration creation wizard."""

import logging
import uuid

from flask import Blueprint, jsonify, request
from db.oracle_browser import (
    get_oracle_conn, list_schemas, list_tables, get_table_info, get_oracle_version,
    get_compilation_errors,
)

bp = Blueprint("oracle_db", __name__)
log = logging.getLogger(__name__)

_state: dict = {}


def init(load_configs_fn):
    _state["load_configs"] = load_configs_fn


def _oracle_connect_context(db: str, configs: dict, *, prefer_owner: bool) -> dict:
    cfg = configs.get(f"oracle_{db}", {}) or {}
    host = (cfg.get("host") or "").strip()
    service_name = (cfg.get("service_name") or "").strip()
    regular_user = (cfg.get("user") or "").strip()
    owner_user = (cfg.get("owner_user") or "").strip()
    credential_mode = "owner_user" if prefer_owner and owner_user else "user"
    selected_user = owner_user if credential_mode == "owner_user" else regular_user
    selected_password_key = "owner_password" if credential_mode == "owner_user" else "password"

    missing = []
    if not host:
        missing.append("host")
    if not service_name:
        missing.append("service_name")
    if not selected_user:
        missing.append(credential_mode)
    if not cfg.get(selected_password_key):
        missing.append(selected_password_key)

    return {
        "db": db,
        "config_key": f"oracle_{db}",
        "prefer_owner": prefer_owner,
        "credential_mode": credential_mode,
        "host": host,
        "port": cfg.get("port", 1521),
        "service_name": service_name,
        "user": selected_user,
        "regular_user_configured": bool(regular_user),
        "owner_user_configured": bool(owner_user),
        "selected_password_configured": bool(cfg.get(selected_password_key)),
        "missing": missing,
    }


def _oracle_503(db: str, configs: dict, exc: Exception, *, prefer_owner: bool):
    error_id = uuid.uuid4().hex[:12]
    log.exception(
        "Oracle DB browser request failed: error_id=%s endpoint=%s method=%s "
        "args=%s connect_context=%s error=%s",
        error_id,
        request.path,
        request.method,
        dict(request.args),
        _oracle_connect_context(db, configs, prefer_owner=prefer_owner),
        exc,
    )
    return jsonify({"error": str(exc), "error_id": error_id}), 503


@bp.get("/api/db/<db>/schemas")
def list_oracle_schemas(db: str):
    if db not in ("source", "target"):
        return jsonify({"error": "Invalid db"}), 400
    configs = _state["load_configs"]()
    try:
        conn = get_oracle_conn(db, configs, prefer_owner=True)
        try:
            return jsonify(list_schemas(conn))
        finally:
            conn.close()
    except Exception as exc:
        return _oracle_503(db, configs, exc, prefer_owner=True)


@bp.get("/api/db/<db>/oracle-errors")
def oracle_errors(db: str):
    """Return all_errors rows for an INVALID PL/SQL object.

    Query string: schema, type, name (case-insensitive).
    Used by the Drawer to surface why a VIEW/PACKAGE/etc is INVALID.
    """
    if db not in ("source", "target"):
        return jsonify({"error": "Invalid db"}), 400
    schema = request.args.get("schema", "").strip().upper()
    otype  = request.args.get("type",   "").strip().upper()
    name   = request.args.get("name",   "").strip().upper()
    if not (schema and otype and name):
        return jsonify({"error": "schema/type/name required"}), 400
    configs = _state["load_configs"]()
    try:
        conn = get_oracle_conn(db, configs, prefer_owner=True)
        try:
            return jsonify(get_compilation_errors(conn, schema, otype, name))
        finally:
            conn.close()
    except Exception as exc:
        return _oracle_503(db, configs, exc, prefer_owner=True)


@bp.get("/api/db/<db>/tables")
def list_oracle_tables(db: str):
    if db not in ("source", "target"):
        return jsonify({"error": "Invalid db"}), 400
    schema = request.args.get("schema", "").strip().upper()
    if not schema:
        return jsonify({"error": "schema required"}), 400
    configs = _state["load_configs"]()
    try:
        conn = get_oracle_conn(db, configs, prefer_owner=True)
        try:
            return jsonify(list_tables(conn, schema))
        finally:
            conn.close()
    except Exception as exc:
        return _oracle_503(db, configs, exc, prefer_owner=True)


@bp.get("/api/db/source/table-info")
def source_table_info():
    schema = request.args.get("schema", "").strip().upper()
    table  = request.args.get("table",  "").strip().upper()
    if not schema or not table:
        return jsonify({"error": "schema and table required"}), 400
    configs = _state["load_configs"]()
    try:
        conn = get_oracle_conn("source", configs)
        try:
            return jsonify(get_table_info(conn, schema, table))
        finally:
            conn.close()
    except Exception as exc:
        return _oracle_503("source", configs, exc, prefer_owner=False)


@bp.get("/api/db/info")
def db_info():
    """Wizard helper: returns {source, target} = { host, service_name, version, ok }
    so the New-Migration form can show prefilled host/version from existing settings.
    Failures are returned as { ok: false, error } per side — endpoint never 5xx's."""
    configs = _state["load_configs"]()
    out: dict = {}
    for side in ("source", "target"):
        cfg = configs.get(f"oracle_{side}", {})
        info = {
            "host":         cfg.get("host", ""),
            "port":         cfg.get("port", 1521),
            "service_name": cfg.get("service_name", ""),
            "schema":       cfg.get("schema", ""),
            "configured":   bool(cfg.get("host") and cfg.get("service_name") and cfg.get("user")),
            "version":      "",
            "version_banner": "",
            "ok":           False,
            "error":        None,
        }
        if info["configured"]:
            try:
                conn = get_oracle_conn(side, configs)
                try:
                    v = get_oracle_version(conn)
                    info["version"]        = v.get("short", "")
                    info["version_banner"] = v.get("banner", "")
                    info["ok"]             = True
                finally:
                    conn.close()
            except Exception as exc:
                info["error"] = str(exc)[:200]
        out[side] = info
    return jsonify(out)
