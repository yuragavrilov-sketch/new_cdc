from flask import Flask

from routes import migrations


class DeleteMigrationCursorStub:
    def __init__(self, calls, *, connector_row=("cdc-main", None, None, None)):
        self.calls = calls
        self.connector_row = connector_row
        self._next_fetchone = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        compact_sql = " ".join(sql.split())
        self.calls.append((compact_sql, params))
        if "SELECT phase FROM migrations" in compact_sql:
            self._next_fetchone = ("DRAFT",)
        elif "SELECT connector_name" in compact_sql:
            self._next_fetchone = self.connector_row
        else:
            self._next_fetchone = None

    def fetchone(self):
        return self._next_fetchone


class DeleteMigrationConnStub:
    def __init__(self, calls, *, connector_row=("cdc-main", None, None, None)):
        self.calls = calls
        self.connector_row = connector_row

    def cursor(self):
        return DeleteMigrationCursorStub(self.calls, connector_row=self.connector_row)

    def commit(self):
        self.calls.append(("COMMIT", None))

    def close(self):
        self.calls.append(("CLOSE", None))


class FullRestartCursorStub:
    def __init__(self, calls, *, row=("CDC_APPLYING", "task-1"), rowcounts=None):
        self.calls = calls
        self.row = row
        self.rowcounts = list(rowcounts or [3, 1, 2, 4, 1, 1])
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        compact_sql = " ".join(sql.split())
        self.calls.append((compact_sql, params))
        if compact_sql.startswith(("DELETE", "UPDATE migration_plan_items")):
            self.rowcount = self.rowcounts.pop(0) if self.rowcounts else 0

    def fetchone(self):
        return self.row


class FullRestartConnStub:
    def __init__(self, calls, *, row=("CDC_APPLYING", "task-1"), rowcounts=None):
        self.calls = calls
        self.row = row
        self.rowcounts = rowcounts

    def cursor(self):
        return FullRestartCursorStub(self.calls, row=self.row, rowcounts=self.rowcounts)

    def commit(self):
        self.calls.append(("COMMIT", None))

    def close(self):
        self.calls.append(("CLOSE", None))


class MigrationActionCursorStub:
    def __init__(self, calls, *, phase="CDC_CATCHING_UP"):
        self.calls = calls
        self.phase = phase
        self._next_fetchone = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        compact_sql = " ".join(sql.split())
        self.calls.append((compact_sql, params))
        if "SELECT phase FROM migrations" in compact_sql:
            self._next_fetchone = (self.phase,)
        else:
            self._next_fetchone = None

    def fetchone(self):
        return self._next_fetchone


class MigrationActionConnStub:
    def __init__(self, calls, *, phase="CDC_CATCHING_UP"):
        self.calls = calls
        self.phase = phase

    def cursor(self):
        return MigrationActionCursorStub(self.calls, phase=self.phase)

    def commit(self):
        self.calls.append(("COMMIT", None))

    def close(self):
        self.calls.append(("CLOSE", None))


def test_legacy_direct_cdc_creation_error_points_to_schema_screen():
    message = migrations._legacy_cdc_creation_error()

    assert "Legacy direct CDC migration creation is disabled" in message
    assert "schema migration screen" in message
    assert "queued and autostarted" in message


def test_create_migration_rejects_direct_cdc_before_db(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(migrations.bp)

    monkeypatch.setitem(migrations._state, "db_available", {"value": True})

    def fail_get_conn():
        raise AssertionError("CDC direct reject must not open state DB connection")

    monkeypatch.setitem(migrations._state, "get_conn", fail_get_conn)

    res = app.test_client().post("/api/migrations", json={
        "migration_name": "TCBPAY.ALLORDERS",
        "strategy": "CDC_DIRECT",
        "source_schema": "TCBPAY",
        "source_table": "ALLORDERS",
        "target_schema": "TCBPAY",
        "target_table": "ALLORDERS",
        "group_id": "gid-1",
    })

    assert res.status_code == 400
    body = res.get_json()
    assert "Legacy direct CDC migration creation is disabled" in body["error"]
    assert "schema migration screen" in body["error"]


def test_bulk_create_migrations_rejects_direct_cdc_before_db(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(migrations.bp)

    monkeypatch.setitem(migrations._state, "db_available", {"value": True})

    def fail_get_conn():
        raise AssertionError("CDC bulk reject must not open state DB connection")

    monkeypatch.setitem(migrations._state, "get_conn", fail_get_conn)

    res = app.test_client().post("/api/migrations/bulk", json={
        "strategy": "CDC_STAGE",
        "group_id": "gid-1",
        "tables": [{
            "source_schema": "TCBPAY",
            "source_table": "ALLORDERS",
            "target_schema": "TCBPAY",
            "target_table": "ALLORDERS",
        }],
    })

    assert res.status_code == 400
    body = res.get_json()
    assert "Legacy direct CDC migration creation is disabled" in body["error"]
    assert "schema migration screen" in body["error"]


def test_delete_group_managed_cdc_migration_keeps_shared_connector(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(migrations.bp)
    calls = []
    deleted_connectors = []

    monkeypatch.setitem(migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(
        migrations._state,
        "get_conn",
        lambda: DeleteMigrationConnStub(
            calls,
            connector_row=("cdc-main", None, None, None, "gid-1", "CDC_STAGE"),
        ),
    )
    monkeypatch.setattr(
        migrations.debezium,
        "delete_connector",
        lambda connector_name: deleted_connectors.append(connector_name),
    )

    res = app.test_client().delete("/api/migrations/mid-1")

    assert res.status_code == 200
    assert res.get_json() == {"ok": True}
    assert deleted_connectors == []
    assert any("DELETE FROM migrations" in sql for sql, _params in calls)


def test_cancel_group_managed_cdc_migration_keeps_shared_connector(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(migrations.bp)
    calls = []
    broadcasts = []
    deleted_connectors = []

    monkeypatch.setitem(migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(
        migrations._state,
        "get_conn",
        lambda: DeleteMigrationConnStub(
            calls,
            connector_row=("cdc-main", "gid-1", "CDC_STAGE"),
        ),
    )
    monkeypatch.setitem(migrations._state, "broadcast", lambda event: broadcasts.append(event))
    monkeypatch.setattr(
        migrations.debezium,
        "delete_connector",
        lambda connector_name: deleted_connectors.append(connector_name),
    )

    res = app.test_client().post("/api/migrations/mid-1/action", json={"action": "cancel"})

    assert res.status_code == 200
    assert res.get_json()["to_phase"] == "CANCELLING"
    assert deleted_connectors == []
    assert broadcasts[-1]["phase"] == "CANCELLING"


def test_pause_keeps_current_phase_and_sets_paused(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(migrations.bp)
    calls = []
    broadcasts = []

    monkeypatch.setitem(migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(
        migrations._state,
        "get_conn",
        lambda: MigrationActionConnStub(calls, phase="CDC_CATCHING_UP"),
    )
    monkeypatch.setitem(migrations._state, "broadcast", lambda event: broadcasts.append(event))

    res = app.test_client().post("/api/migrations/mid-1/action", json={"action": "pause"})

    assert res.status_code == 200
    body = res.get_json()
    assert body["to_phase"] == "CDC_CATCHING_UP"
    assert body["paused"] is True
    assert broadcasts[-1]["phase"] == "CDC_CATCHING_UP"
    assert broadcasts[-1]["paused"] is True
    assert any("UPDATE migrations SET paused = %s" in sql for sql, _params in calls)
    assert any("UPDATE migration_cdc_state SET worker_id = NULL" in sql for sql, _params in calls)
    assert not any("UPDATE migrations SET phase=%s" in sql for sql, _params in calls)


def test_resume_keeps_current_phase_and_clears_paused(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(migrations.bp)
    calls = []
    broadcasts = []

    monkeypatch.setitem(migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(
        migrations._state,
        "get_conn",
        lambda: MigrationActionConnStub(calls, phase="BULK_LOADING"),
    )
    monkeypatch.setitem(migrations._state, "broadcast", lambda event: broadcasts.append(event))

    res = app.test_client().post("/api/migrations/mid-1/action", json={"action": "resume"})

    assert res.status_code == 200
    body = res.get_json()
    assert body["to_phase"] == "BULK_LOADING"
    assert body["paused"] is False
    assert broadcasts[-1]["phase"] == "BULK_LOADING"
    assert broadcasts[-1]["paused"] is False
    assert any("UPDATE migrations SET paused = %s" in sql for sql, _params in calls)
    assert not any("UPDATE migrations SET phase=%s" in sql for sql, _params in calls)


def test_full_restart_resets_migration_to_draft(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(migrations.bp)
    calls = []
    broadcasts = []

    monkeypatch.setitem(migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(migrations._state, "get_conn", lambda: FullRestartConnStub(calls))
    monkeypatch.setitem(migrations._state, "broadcast", lambda event: broadcasts.append(event))

    res = app.test_client().post("/api/migrations/mid-1/full-restart", json={})

    assert res.status_code == 200
    body = res.get_json()
    assert body["ok"] is True
    assert body["from_phase"] == "CDC_APPLYING"
    assert body["to_phase"] == "DRAFT"
    assert body["started"] is False
    assert body["deleted_chunks"] == 3
    assert body["deleted_cdc_state"] == 1
    assert body["deleted_index_jobs"] == 2
    assert body["deleted_trigger_jobs"] == 4
    assert body["deleted_compare_tasks"] == 1
    assert body["updated_plan_items"] == 1
    assert broadcasts[-1]["phase"] == "DRAFT"
    assert any("DELETE FROM migration_chunks" in sql for sql, _params in calls)
    assert any("DELETE FROM migration_cdc_state" in sql for sql, _params in calls)
    assert any("DELETE FROM target_index_jobs" in sql for sql, _params in calls)
    assert any("DELETE FROM target_trigger_jobs" in sql for sql, _params in calls)
    assert any("UPDATE migrations SET phase" in sql for sql, _params in calls)


def test_full_restart_can_start_migration_immediately(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(migrations.bp)
    calls = []
    broadcasts = []

    monkeypatch.setitem(migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(
        migrations._state,
        "get_conn",
        lambda: FullRestartConnStub(calls, row=("FAILED", None), rowcounts=[0, 0, 0, 0, 1]),
    )
    monkeypatch.setitem(migrations._state, "broadcast", lambda event: broadcasts.append(event))

    res = app.test_client().post("/api/migrations/mid-1/full-restart", json={"start": True})

    assert res.status_code == 200
    body = res.get_json()
    assert body["ok"] is True
    assert body["from_phase"] == "FAILED"
    assert body["to_phase"] == "NEW"
    assert body["started"] is True
    assert body["deleted_compare_tasks"] == 0
    assert body["updated_plan_items"] == 1
    assert broadcasts[-1]["phase"] == "NEW"
    assert any(
        sql.startswith("UPDATE migration_plan_items") and params == ("RUNNING", "mid-1")
        for sql, params in calls
    )
