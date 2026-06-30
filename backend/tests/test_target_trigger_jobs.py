from __future__ import annotations

import pytest

from services import target_trigger_jobs


class CursorStub:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.row


class ConnStub:
    def __init__(self, row):
        self.cursor_stub = CursorStub(row)
        self.committed = False

    def cursor(self):
        return self.cursor_stub

    def commit(self):
        self.committed = True


def test_trigger_job_rejects_cdc_catching_up_before_lag_zero():
    conn = ConnStub(("CDC_CATCHING_UP", "CDC_DIRECT"))

    with pytest.raises(ValueError) as exc:
        target_trigger_jobs.ensure_pending_job(conn, "mid-cdc")

    assert "Cannot create trigger job from phase CDC_CATCHING_UP" in str(exc.value)
    assert not conn.committed


class RunCursorStub:
    def __init__(self, rows):
        self.rows = list(rows)
        self.executed = []
        self.description = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        row = self.rows.pop(0)
        if len(row) == 2:
            self.description = [("job_id",), ("state",)]
        else:
            self.description = [
                ("job_id",),
                ("migration_id",),
                ("state",),
                ("enabled_count",),
                ("result_json",),
                ("error_text",),
                ("requested_by",),
                ("created_at",),
                ("started_at",),
                ("completed_at",),
            ]
        return row


class RunConnStub:
    def __init__(self, rows):
        self.cursor_stub = RunCursorStub(rows)
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_stub

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def test_run_trigger_job_broadcasts_running_state(monkeypatch):
    broadcasts = []

    class NoopThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            pass

    conn = RunConnStub([
        ("job-1", "PENDING"),
        ("job-1", "mid-cdc", "RUNNING", 0, None, None, "user", None, None, None),
    ])
    monkeypatch.setattr(target_trigger_jobs.threading, "Thread", NoopThread)

    job = target_trigger_jobs.run_job_async(
        get_conn_fn=lambda: conn,
        load_configs_fn=lambda: {},
        broadcast_fn=broadcasts.append,
        migration_id="mid-cdc",
        job_id="job-1",
    )

    assert job["state"] == "RUNNING"
    assert conn.committed
    assert conn.closed
    assert broadcasts[0]["type"] == "target_trigger_job"
    assert broadcasts[0]["migration_id"] == "mid-cdc"
    assert broadcasts[0]["job_id"] == "job-1"
    assert broadcasts[0]["state"] == "RUNNING"
