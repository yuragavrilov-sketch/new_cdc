"""Service config & status routes."""

import hmac
import os
import threading
from datetime import datetime

from flask import Blueprint, jsonify, request

bp = Blueprint("config", __name__)

# These are injected by app.py after import
_state: dict = {}   # holds references set by init()


def init(db_available_ref, mem_configs, service_status, status_lock, broadcast_fn,
         load_configs_fn, save_config_fn, checkers: dict):
    _state["db_available"]  = db_available_ref
    _state["mem_configs"]   = mem_configs
    _state["service_status"] = service_status
    _state["status_lock"]   = status_lock
    _state["broadcast"]     = broadcast_fn
    _state["load_configs"]  = load_configs_fn
    _state["save_config"]   = save_config_fn
    _state["checkers"]      = checkers


_ALLOWED = {"oracle_source", "oracle_target", "kafka", "kafka_connect"}
_SECRET_KEYS = {"password", "owner_password"}
_SECRET_MASK = "********"


def _config_token() -> str:
    return os.environ.get("CONFIG_API_TOKEN", "").strip()


def _check_config_access():
    token = _config_token()
    if not token:
        return None
    supplied = request.headers.get("X-Config-Token", "")
    if hmac.compare_digest(supplied, token):
        return None
    return jsonify({"error": "CONFIG_API_TOKEN required"}), 401


def _masked_config(config: dict) -> dict:
    masked = {}
    for service, cfg in (config or {}).items():
        if not isinstance(cfg, dict):
            masked[service] = cfg
            continue
        copy = dict(cfg)
        for key in _SECRET_KEYS:
            if copy.get(key):
                copy[key] = _SECRET_MASK
        masked[service] = copy
    return masked


def _resolve_secret_placeholders(service: str, body: dict) -> dict:
    current = _state["load_configs"]().get(service, {}) or {}
    resolved = dict(body)
    for key in _SECRET_KEYS:
        if resolved.get(key) == _SECRET_MASK or key not in resolved:
            if current.get(key):
                resolved[key] = current[key]
    return resolved


@bp.get("/api/config")
def get_configs():
    denied = _check_config_access()
    if denied:
        return denied
    return jsonify(_masked_config(_state["load_configs"]()))


@bp.post("/api/config/<service>")
def save_config_route(service: str):
    denied = _check_config_access()
    if denied:
        return denied
    if service not in _ALLOWED:
        return jsonify({"error": f"Unknown service: {service}"}), 400
    body = request.get_json(force=True)
    if not isinstance(body, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400
    body = _resolve_secret_placeholders(service, body)
    _state["save_config"](service, body)
    threading.Thread(target=_check_and_broadcast, args=(service,), daemon=True).start()
    return jsonify({"ok": True})


@bp.get("/api/status")
def get_status():
    with _state["status_lock"]:
        return jsonify(dict(_state["service_status"]))


@bp.post("/api/config/<service>/test")
def test_config_route(service: str):
    """Test a connection using the provided config without saving it."""
    denied = _check_config_access()
    if denied:
        return denied
    if service not in _ALLOWED:
        return jsonify({"error": f"Unknown service: {service}"}), 400
    body = request.get_json(force=True)
    if not isinstance(body, dict):
        return jsonify({"error": "Body must be a JSON object"}), 400
    body = _resolve_secret_placeholders(service, body)
    checker = _state["checkers"].get(service)
    if not checker:
        return jsonify({"status": "unknown", "message": "No checker available for this service"}), 200
    try:
        status, message = checker(body)
    except Exception as exc:
        status, message = "down", str(exc)[:200]
    return jsonify({"status": status, "message": message})


def _check_and_broadcast(service: str) -> None:
    checker = _state["checkers"][service]
    cfg = _state["load_configs"]().get(service, {})
    try:
        status, message = checker(cfg)
    except Exception as exc:
        status, message = "down", str(exc)[:150]
    with _state["status_lock"]:
        _state["service_status"][service] = {"status": status, "message": message}
    _state["broadcast"]({
        "type":    "service_status",
        "service": service,
        "status":  status,
        "message": message,
        "ts":      datetime.utcnow().isoformat() + "Z",
    })
