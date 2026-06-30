from __future__ import annotations

from datetime import datetime, timezone

from flask import Flask

from routes import workers


def test_worker_status_returns_empty_when_db_unavailable(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(workers.bp)
    monkeypatch.setitem(workers._state, "db_available", {"value": False})

    response = app.test_client().get("/api/workers/status")

    assert response.status_code == 200
    assert response.get_json() == {
        "workers": [],
        "active_count": 0,
        "cdc_ready": False,
        "stale_after_seconds": 30,
    }


def test_worker_status_reports_active_cdc_worker(monkeypatch):
    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _sql):
            pass

        def fetchall(self):
            return [
                (
                    "worker-1",
                    "universal",
                    ["bulk", "cdc"],
                    datetime(2026, 6, 25, 12, 0, tzinfo=timezone.utc),
                    datetime(2026, 6, 25, 12, 1, tzinfo=timezone.utc),
                    True,
                ),
                (
                    "worker-2",
                    "universal",
                    ["bulk"],
                    datetime(2026, 6, 25, 11, 0, tzinfo=timezone.utc),
                    datetime(2026, 6, 25, 11, 1, tzinfo=timezone.utc),
                    False,
                ),
            ]

    class ConnStub:
        def cursor(self):
            return CursorStub()

        def close(self):
            pass

    app = Flask(__name__)
    app.register_blueprint(workers.bp)
    monkeypatch.setitem(workers._state, "db_available", {"value": True})
    monkeypatch.setitem(workers._state, "get_conn", lambda: ConnStub())

    response = app.test_client().get("/api/workers/status")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["active_count"] == 1
    assert payload["cdc_ready"] is True
    assert payload["workers"][0]["worker_id"] == "worker-1"
    assert payload["workers"][0]["last_heartbeat"] == "2026-06-25T12:01:00Z"
    assert payload["workers"][1]["active"] is False
