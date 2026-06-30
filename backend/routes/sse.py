"""SSE event bus — subscribe/publish, service-status stream."""

import json
import queue
import threading
import time
from datetime import datetime

from flask import Blueprint, Response, jsonify, request

bp = Blueprint("sse", __name__)

_subscribers: list[queue.Queue] = []
_lock = threading.Lock()

# Injected by app.py
_state: dict = {}


def init(service_status_ref, status_lock):
    _state["service_status"] = service_status_ref
    _state["status_lock"]    = status_lock


# ── Public API ────────────────────────────────────────────────────────────────

def broadcast(event: dict) -> None:
    payload = f"data: {json.dumps(event)}\n\n"
    with _lock:
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _subscribers.remove(q)


def subscriber_count() -> int:
    with _lock:
        return len(_subscribers)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _subscribe() -> queue.Queue:
    q: queue.Queue = queue.Queue(maxsize=100)
    with _lock:
        _subscribers.append(q)
    return q


def _unsubscribe(q: queue.Queue) -> None:
    with _lock:
        try:
            _subscribers.remove(q)
        except ValueError:
            pass


def _stream(q: queue.Queue):
    try:
        yield "event: connected\ndata: {}\n\n"
        with _state["status_lock"]:
            for svc, info in _state["service_status"].items():
                evt = {
                    "type":    "service_status",
                    "service": svc,
                    "status":  info["status"],
                    "message": info["message"],
                    "ts":      datetime.utcnow().isoformat() + "Z",
                }
                yield f"data: {json.dumps(evt)}\n\n"
        while True:
            try:
                yield q.get(timeout=20)
            except queue.Empty:
                yield ": keepalive\n\n"
    finally:
        _unsubscribe(q)


# ── Routes ────────────────────────────────────────────────────────────────────

@bp.get("/api/events")
def sse_events():
    q = _subscribe()
    return Response(
        _stream(q),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@bp.post("/api/events")
def push_event():
    body  = request.get_json(force=True)
    event = {
        "id":        body.get("id", str(time.time_ns())),
        "table":     body.get("table", "unknown"),
        "schema":    body.get("schema", "public"),
        "operation": body.get("operation", "UNKNOWN").upper(),
        "data":      body.get("data", {}),
        "old_data":  body.get("old_data"),
        "ts":        datetime.utcnow().isoformat() + "Z",
    }
    broadcast(event)
    return jsonify({"ok": True, "event": event}), 202
