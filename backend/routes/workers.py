"""Worker liveness/status API."""

from __future__ import annotations

from datetime import datetime, timezone

from flask import Blueprint, jsonify


bp = Blueprint("workers", __name__)

_state: dict = {}


def init(*, get_conn_fn, db_available_ref):
    _state["get_conn"] = get_conn_fn
    _state["db_available"] = db_available_ref


def _utc_iso_z(value):
    if not value:
        return None
    if getattr(value, "tzinfo", None) is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    if hasattr(value, "isoformat"):
        return value.isoformat() + "Z"
    return value


def _normalize_capabilities(raw) -> list[str]:
    if isinstance(raw, list):
        return [str(item) for item in raw]
    return []


@bp.get("/api/workers/status")
def worker_status():
    if not _state["db_available"]["value"]:
        return jsonify({
            "workers": [],
            "active_count": 0,
            "cdc_ready": False,
            "stale_after_seconds": 30,
        })

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT worker_id,
                       role,
                       capabilities,
                       started_at,
                       last_heartbeat,
                       last_heartbeat >= NOW() - INTERVAL '30 seconds' AS active
                FROM   worker_heartbeats
                ORDER  BY last_heartbeat DESC
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    workers = []
    for worker_id, role, capabilities, started_at, last_heartbeat, active in rows:
        caps = _normalize_capabilities(capabilities)
        workers.append({
            "worker_id": worker_id,
            "role": role,
            "capabilities": caps,
            "started_at": _utc_iso_z(started_at),
            "last_heartbeat": _utc_iso_z(last_heartbeat),
            "active": bool(active),
        })

    active_workers = [worker for worker in workers if worker["active"]]
    cdc_ready = any("cdc" in worker["capabilities"] for worker in active_workers)
    return jsonify({
        "workers": workers,
        "active_count": len(active_workers),
        "cdc_ready": cdc_ready,
        "stale_after_seconds": 30,
    })
