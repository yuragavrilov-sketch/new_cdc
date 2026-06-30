from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from flask import Flask

from routes import schema_migrations
from routes import migrations
from db import oracle_browser
from services import orchestrator
from services import connector_groups


def test_schema_migration_cdc_key_derivation_prefers_pk():
    assert schema_migrations._derive_cdc_key_info({
        "pk_columns": ["ID"],
        "uk_constraints": [{"name": "UK_T", "columns": ["CODE"]}],
    }) == ("PRIMARY_KEY", "PK", ["ID"], True, True)


def test_schema_migration_cdc_key_derivation_uses_unique_key():
    assert schema_migrations._derive_cdc_key_info({
        "pk_columns": [],
        "uk_constraints": [{"name": "UK_T", "columns": ["CODE", "DATE_ID"]}],
    }) == ("UNIQUE_KEY", "UK", ["CODE", "DATE_ID"], False, True)


def test_schema_migration_cdc_autostart_recovers_missing_running_connector(monkeypatch):
    calls = []

    monkeypatch.setattr(
        schema_migrations,
        "_ensure_cdc_group_topics",
        lambda group_id: calls.append(("topics", group_id)) or [],
    )
    monkeypatch.setattr(
        connector_groups,
        "get_connector_status",
        lambda group_id: calls.append(("status", group_id)) or "STOPPED",
    )
    monkeypatch.setattr(
        connector_groups,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)) or (
            (_ for _ in ()).throw(
                ValueError("CDC runtime cdc-main is marked RUNNING but is missing in Kafka Connect")
            )
        ),
    )
    monkeypatch.setattr(
        connector_groups,
        "request_start",
        lambda group_id: calls.append(("start", group_id)) or {
            "group_id": group_id,
            "status": "TOPICS_CREATING",
        },
    )

    result = schema_migrations._sync_and_request_cdc_connector_start("gid-1", "RUNNING")

    assert result == {"group_id": "gid-1", "status": "TOPICS_CREATING"}
    assert calls == [
        ("status", "gid-1"),
        ("start", "gid-1"),
    ]


def test_schema_migration_cdc_autostart_refreshes_running_connector_after_topics(monkeypatch):
    calls = []

    monkeypatch.setattr(
        schema_migrations,
        "_ensure_cdc_group_topics",
        lambda group_id: calls.append(("topics", group_id)) or [],
    )
    monkeypatch.setattr(
        connector_groups,
        "get_connector_status",
        lambda group_id: calls.append(("status", group_id)) or "RUNNING",
    )
    monkeypatch.setattr(
        connector_groups,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)),
    )
    monkeypatch.setattr(
        connector_groups,
        "request_start",
        lambda group_id: calls.append(("start", group_id)) or {
            "group_id": group_id,
            "status": "RUNNING",
            "already_started": True,
        },
    )

    result = schema_migrations._sync_and_request_cdc_connector_start("gid-1", "RUNNING")

    assert result == {"group_id": "gid-1", "status": "RUNNING", "already_started": True}
    assert calls == [
        ("status", "gid-1"),
        ("topics", "gid-1"),
        ("refresh", "gid-1"),
        ("start", "gid-1"),
    ]


def test_schema_migration_cdc_autostart_creates_topics_while_connector_is_starting(monkeypatch):
    calls = []

    monkeypatch.setattr(
        schema_migrations,
        "_ensure_cdc_group_topics",
        lambda group_id: calls.append(("topics", group_id)) or [],
    )
    monkeypatch.setattr(
        connector_groups,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)),
    )
    monkeypatch.setattr(
        connector_groups,
        "request_start",
        lambda group_id: calls.append(("start", group_id)) or {
            "group_id": group_id,
            "status": "CONNECTOR_STARTING",
            "already_started": True,
        },
    )

    result = schema_migrations._sync_and_request_cdc_connector_start("gid-1", "CONNECTOR_STARTING")

    assert result == {
        "group_id": "gid-1",
        "status": "CONNECTOR_STARTING",
        "already_started": True,
    }
    assert calls == [
        ("topics", "gid-1"),
        ("refresh", "gid-1"),
        ("start", "gid-1"),
    ]


def test_schema_migration_cdc_autostart_keeps_refresh_errors_blocking(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)) or (
            (_ for _ in ()).throw(ValueError("bad table.include.list"))
        ),
    )
    monkeypatch.setattr(
        connector_groups,
        "request_start",
        lambda group_id: calls.append(("start", group_id)),
    )

    try:
        schema_migrations._sync_and_request_cdc_connector_start("gid-1", "STOPPED")
    except ValueError as exc:
        assert "bad table.include.list" in str(exc)
    else:
        raise AssertionError("expected refresh error")

    assert calls == [("refresh", "gid-1")]


def test_orchestrator_source_key_derivation_returns_none_without_key():
    assert orchestrator._derive_source_key_from_info({
        "pk_columns": [],
        "uk_constraints": [],
    }) is None


def test_orchestrator_source_key_derivation_serializes_pk():
    result = orchestrator._derive_source_key_from_info({
        "pk_columns": ["ID"],
        "uk_constraints": [],
    })

    assert result is not None
    assert result["source_pk_exists"] is True
    assert result["effective_key_type"] == "PRIMARY_KEY"
    assert json.loads(result["effective_key_columns_json"]) == ["ID"]


def test_cdc_group_table_keys_normalizes_names(monkeypatch):
    monkeypatch.setattr(
        connector_groups,
        "get_group_tables",
        lambda group_id: [
            {"source_schema": "tcbpay", "source_table": "allorders"},
            {"source_schema": "TCBPAY", "source_table": "Merchants#Orders"},
        ],
    )

    assert migrations._cdc_group_table_keys("gid") == {
        ("TCBPAY", "ALLORDERS"),
        ("TCBPAY", "MERCHANTS#ORDERS"),
    }


def test_schema_migration_starts_each_created_cdc_queue_position(monkeypatch):
    calls = []

    def fake_start(plan_id, **kwargs):
        calls.append((plan_id, kwargs))
        return {"batch": kwargs["batch_order"], "started": [f"mid-{kwargs['batch_order']}"]}

    import routes.planner as planner

    monkeypatch.setattr(planner, "_start_next_plan_batch", fake_start)

    result = schema_migrations._start_created_cdc_plan_batches(
        42,
        [
            {"batch_order": 3},
            {"batch_order": 1},
            {"batch_order": 3},
        ],
    )

    assert result == [
        {"batch": 1, "started": ["mid-1"]},
        {"batch": 3, "started": ["mid-3"]},
    ]
    assert calls == [
        (42, {
            "actor": "SYSTEM",
            "batch_order": 1,
            "allow_cdc_queue_when_blocked": True,
        }),
        (42, {
            "actor": "SYSTEM",
            "batch_order": 3,
            "allow_cdc_queue_when_blocked": True,
        }),
    ]


def test_schema_migration_queues_created_cdc_items_when_connector_not_running():
    assert schema_migrations._should_start_created_cdc_plan_batches(
        "STOPPED",
        "Kafka Connect unavailable",
    )
    assert schema_migrations._should_start_created_cdc_plan_batches(
        "TOPICS_CREATING",
        "topic creation failed",
    )
    assert schema_migrations._should_start_created_cdc_plan_batches(
        None,
        "Oracle source is not configured",
    )


def test_schema_migration_does_not_queue_running_cdc_items_after_sync_error():
    assert not schema_migrations._should_start_created_cdc_plan_batches(
        "RUNNING",
        "bad table.include.list",
    )
    assert schema_migrations._should_start_created_cdc_plan_batches("RUNNING", None)


def test_schema_migration_cdc_group_endpoint_uses_schema_group_without_plan(monkeypatch):
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params):
            self.query = query
            self.params = params

        def fetchone(self):
            return ("gid-schema", None)

    class Conn:
        def __init__(self):
            self.cursor_obj = Cursor()
            self.closed = False

        def cursor(self):
            return self.cursor_obj

        def close(self):
            self.closed = True

    conn = Conn()
    monkeypatch.setitem(schema_migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(schema_migrations._state, "get_conn", lambda: conn)
    monkeypatch.setattr(
        schema_migrations,
        "_load_cdc_connector_summary",
        lambda group_id: {
            "group_id": group_id,
            "tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}],
            "table_include_list": "TCBPAY.ALLORDERS",
        },
    )

    app = Flask(__name__)
    app.register_blueprint(schema_migrations.bp)

    res = app.test_client().get("/api/schema-migrations/sm-1/cdc-group")

    assert res.status_code == 200
    assert res.get_json()["group_id"] == "gid-schema"
    assert conn.cursor_obj.params == ("sm-1",)
    assert conn.closed


def test_schema_migration_add_items_rejects_payload_without_table_names(monkeypatch):
    monkeypatch.setitem(schema_migrations._state, "db_available", {"value": True})

    app = Flask(__name__)
    app.register_blueprint(schema_migrations.bp)

    res = app.test_client().post(
        "/api/schema-migrations/sm-1/plan/items",
        json={
            "strategy": "CDC_DIRECT",
            "tables": [{}, {"source_table": "  "}],
        },
    )

    assert res.status_code == 400
    assert res.get_json()["error"] == "at least one table name is required"


def test_schema_migration_add_items_rejects_conflicting_payload_cdc_group(monkeypatch):
    calls = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split())[:80], params))

        def fetchone(self):
            return ("sm-1", "TCBPAY->PAY", "TCBPAY", "PAY", 42, "gid-schema", "gid-schema")

    class Conn:
        def cursor(self):
            return Cursor()

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setitem(schema_migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(schema_migrations._state, "get_conn", lambda: Conn())
    monkeypatch.setattr(
        schema_migrations,
        "_source_oracle_conn",
        lambda: (_ for _ in ()).throw(AssertionError("Oracle should not be opened")),
    )

    app = Flask(__name__)
    app.register_blueprint(schema_migrations.bp)

    res = app.test_client().post(
        "/api/schema-migrations/sm-1/plan/items",
        json={
            "strategy": "CDC_DIRECT",
            "connector_group_id": "gid-other",
            "tables": [{"source_table": "ALLORDERS"}],
        },
    )

    assert res.status_code == 400
    assert "only one CDC pack" in res.get_json()["error"]
    assert [call[0] for call in calls] == ["execute", "rollback", "close"]


def test_schema_migration_add_items_route_returns_created_item_states(monkeypatch):
    calls = []

    class Cursor:
        def __init__(self):
            self.fetchone_results = [
                ("sm-1", "TCBPAY->TCBPAY", "TCBPAY", "TCBPAY", 42, "gid-1", "gid-1"),
                ("RUNNING", "topic.prefix", None),
                (0,),
                None,
                None,
                (7,),
            ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split())[:80], params))

        def fetchone(self):
            return self.fetchone_results.pop(0)

    class Conn:
        def __init__(self):
            self.cur = Cursor()

        def cursor(self):
            return self.cur

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    class OracleConn:
        def close(self):
            calls.append(("oracle-close",))

    monkeypatch.setitem(schema_migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(schema_migrations._state, "get_conn", lambda: Conn())
    monkeypatch.setitem(schema_migrations._state, "broadcast", lambda event: calls.append(("broadcast", event["type"])))
    monkeypatch.setattr(schema_migrations, "_source_oracle_conn", lambda: OracleConn())
    monkeypatch.setattr(
        oracle_browser,
        "get_table_info",
        lambda _conn, _schema, _table: {
            "pk_columns": ["ID"],
            "uk_constraints": [],
            "columns": [{"name": "ID"}],
            "supplemental_log_data_all": "YES",
        },
    )
    monkeypatch.setattr(
        schema_migrations,
        "_autostart_created_cdc_items",
        lambda group_id, status, plan_id, created: calls.append(("autostart", group_id, status, plan_id, created)) or {
            "connector_start": {"group_id": group_id, "status": "RUNNING"},
            "connector_start_error": None,
            "plan_start": {"batch": 1, "started": [created[0]["migration_id"]]},
            "plan_starts": [{"batch": 1, "started": [created[0]["migration_id"]]}],
            "plan_start_error": None,
        },
    )
    monkeypatch.setattr(
        schema_migrations,
        "_load_created_plan_item_states",
        lambda _conn, created: calls.append(("states", created)) or [{
            "item_id": created[0]["item_id"],
            "table": created[0]["table"],
            "migration_id": created[0]["migration_id"],
            "batch_order": created[0]["batch_order"],
            "status": "RUNNING",
            "phase": "NEW",
            "queue_position": None,
            "error_text": None,
        }],
    )
    monkeypatch.setattr(
        schema_migrations,
        "_load_cdc_connector_summary",
        lambda group_id: {"group_id": group_id, "tables": [], "table_include_list": "TCBPAY.ALLORDERS"},
    )

    app = Flask(__name__)
    app.register_blueprint(schema_migrations.bp)

    res = app.test_client().post(
        "/api/schema-migrations/sm-1/plan/items",
        json={
            "strategy": "CDC_DIRECT",
            "tables": [{"source_table": "ALLORDERS"}],
            "truncate_target": True,
            "max_parallel_workers": 4,
        },
    )

    assert res.status_code == 201
    body = res.get_json()
    assert body["plan_id"] == 42
    assert body["connector_group_id"] == "gid-1"
    assert body["connector_start"] == {"group_id": "gid-1", "status": "RUNNING"}
    assert body["plan_starts"] == [{"batch": 1, "started": [body["items"][0]["migration_id"]]}]
    assert body["item_states"] == [{
        "item_id": 7,
        "table": "ALLORDERS",
        "migration_id": body["items"][0]["migration_id"],
        "batch_order": 1,
        "status": "RUNNING",
        "phase": "NEW",
        "queue_position": None,
        "error_text": None,
    }]
    assert [call[0] for call in calls if call[0] in ("commit", "autostart", "states", "broadcast")] == [
        "commit",
        "autostart",
        "states",
        "broadcast",
    ]
    assert ("oracle-close",) in calls
    assert ("close",) in calls


def test_schema_migration_add_items_can_prune_cdc_pack_during_add(monkeypatch):
    calls = []

    class Cursor:
        def __init__(self):
            self.fetchone_results = [
                ("sm-1", "TCBPAY->TCBPAY", "TCBPAY", "TCBPAY", 42, "gid-1", "gid-1"),
                ("RUNNING", "topic.prefix", None),
                (0,),
                None,
                None,
                (7,),
            ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchone(self):
            return self.fetchone_results.pop(0)

    class Conn:
        def __init__(self):
            self.cur = Cursor()

        def cursor(self):
            return self.cur

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    class OracleConn:
        def close(self):
            calls.append(("oracle-close",))

    monkeypatch.setitem(schema_migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(schema_migrations._state, "get_conn", lambda: Conn())
    monkeypatch.setitem(schema_migrations._state, "broadcast", lambda event: calls.append(("broadcast", event["type"])))
    monkeypatch.setattr(schema_migrations, "_source_oracle_conn", lambda: OracleConn())
    monkeypatch.setattr(
        schema_migrations,
        "_prune_cdc_group_tables_tx",
        lambda cur, group_id, keep_tables: calls.append(("prune", group_id, keep_tables)) or [{
            "id": "old-1",
            "source_schema": "TCBPAY",
            "source_table": "OLDORDERS",
            "target_schema": "TCBPAY",
            "target_table": "OLDORDERS",
        }],
    )
    monkeypatch.setattr(
        oracle_browser,
        "get_table_info",
        lambda _conn, _schema, _table: {
            "pk_columns": ["ID"],
            "uk_constraints": [],
            "columns": [{"name": "ID"}],
            "supplemental_log_data_all": "YES",
        },
    )
    monkeypatch.setattr(
        schema_migrations,
        "_autostart_created_cdc_items",
        lambda group_id, status, plan_id, created: {
            "connector_start": {"group_id": group_id, "status": "RUNNING"},
            "connector_start_error": None,
            "plan_start": {"batch": 1, "started": [created[0]["migration_id"]]},
            "plan_starts": [{"batch": 1, "started": [created[0]["migration_id"]]}],
            "plan_start_error": None,
        },
    )
    monkeypatch.setattr(schema_migrations, "_load_created_plan_item_states", lambda _conn, created: [])
    monkeypatch.setattr(
        schema_migrations,
        "_load_cdc_connector_summary",
        lambda group_id: {"group_id": group_id, "tables": [{"source_table": "ALLORDERS"}], "table_include_list": "TCBPAY.ALLORDERS"},
    )

    app = Flask(__name__)
    app.register_blueprint(schema_migrations.bp)

    res = app.test_client().post(
        "/api/schema-migrations/sm-1/plan/items",
        json={
            "strategy": "CDC_DIRECT",
            "prune_cdc_pack": True,
            "tables": [{"source_table": "ALLORDERS"}],
        },
    )

    assert res.status_code == 201
    body = res.get_json()
    assert body["cdc_pruned_tables"] == [{
        "id": "old-1",
        "source_schema": "TCBPAY",
        "source_table": "OLDORDERS",
        "target_schema": "TCBPAY",
        "target_table": "OLDORDERS",
    }]
    assert ("prune", "gid-1", {"TCBPAY.ALLORDERS"}) in calls
    labels = [call[0] for call in calls]
    assert labels.index("prune") < labels.index("commit")
    assert ("rollback",) not in calls
    assert ("oracle-close",) in calls


def test_schema_migration_add_items_prune_failure_aborts_before_new_cdc_row(monkeypatch):
    calls = []

    class Cursor:
        def __init__(self):
            self.fetchone_results = [
                ("sm-1", "TCBPAY->TCBPAY", "TCBPAY", "TCBPAY", 42, "gid-1", "gid-1"),
                ("RUNNING", "topic.prefix", None),
            ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchone(self):
            return self.fetchone_results.pop(0)

    class Conn:
        def __init__(self):
            self.cur = Cursor()

        def cursor(self):
            return self.cur

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setitem(schema_migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(schema_migrations._state, "get_conn", lambda: Conn())
    monkeypatch.setitem(schema_migrations._state, "broadcast", lambda event: calls.append(("broadcast", event["type"])))
    monkeypatch.setattr(
        schema_migrations,
        "_source_oracle_conn",
        lambda: (_ for _ in ()).throw(AssertionError("Oracle should not be opened")),
    )
    monkeypatch.setattr(
        schema_migrations,
        "_prune_cdc_group_tables_tx",
        lambda cur, group_id, keep_tables: calls.append(("prune", group_id, keep_tables)) or (
            (_ for _ in ()).throw(
                ValueError("Cannot remove TCBPAY.OLDORDERS from CDC pack: active migration mid-old is in phase CDC_APPLYING")
            )
        ),
    )
    monkeypatch.setattr(
        schema_migrations,
        "_autostart_created_cdc_items",
        lambda *_args: calls.append(("autostart",)),
    )

    app = Flask(__name__)
    app.register_blueprint(schema_migrations.bp)

    res = app.test_client().post(
        "/api/schema-migrations/sm-1/plan/items",
        json={
            "strategy": "CDC_DIRECT",
            "prune_cdc_pack": True,
            "tables": [{"source_table": "ALLORDERS"}],
        },
    )

    assert res.status_code == 400
    assert "active migration mid-old is in phase CDC_APPLYING" in res.get_json()["error"]
    assert ("prune", "gid-1", {"TCBPAY.ALLORDERS"}) in calls
    assert ("rollback",) in calls
    assert ("commit",) not in calls
    assert ("autostart",) not in calls
    assert not any(
        call[0] == "execute" and "INSERT INTO migrations" in call[1]
        for call in calls
    )
    assert ("close",) in calls


def test_schema_migration_prune_cdc_pack_requires_boolean_true(monkeypatch):
    calls = []

    class Cursor:
        def __init__(self):
            self.fetchone_results = [
                ("sm-1", "TCBPAY->TCBPAY", "TCBPAY", "TCBPAY", 42, "gid-1", "gid-1"),
                ("RUNNING", "topic.prefix", None),
                (0,),
                None,
                None,
                (7,),
            ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchone(self):
            return self.fetchone_results.pop(0)

    class Conn:
        def __init__(self):
            self.cur = Cursor()

        def cursor(self):
            return self.cur

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    class OracleConn:
        def close(self):
            calls.append(("oracle-close",))

    monkeypatch.setitem(schema_migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(schema_migrations._state, "get_conn", lambda: Conn())
    monkeypatch.setitem(schema_migrations._state, "broadcast", lambda event: calls.append(("broadcast", event["type"])))
    monkeypatch.setattr(schema_migrations, "_source_oracle_conn", lambda: OracleConn())
    monkeypatch.setattr(
        schema_migrations,
        "_prune_cdc_group_tables_tx",
        lambda *_args: calls.append(("prune",)),
    )
    monkeypatch.setattr(
        oracle_browser,
        "get_table_info",
        lambda _conn, _schema, _table: {
            "pk_columns": ["ID"],
            "uk_constraints": [],
            "columns": [{"name": "ID"}],
            "supplemental_log_data_all": "YES",
        },
    )
    monkeypatch.setattr(
        schema_migrations,
        "_autostart_created_cdc_items",
        lambda group_id, status, plan_id, created: {
            "connector_start": {"group_id": group_id, "status": "RUNNING"},
            "connector_start_error": None,
            "plan_start": {"batch": 1, "started": [created[0]["migration_id"]]},
            "plan_starts": [{"batch": 1, "started": [created[0]["migration_id"]]}],
            "plan_start_error": None,
        },
    )
    monkeypatch.setattr(schema_migrations, "_load_created_plan_item_states", lambda _conn, created: [])
    monkeypatch.setattr(schema_migrations, "_load_cdc_connector_summary", lambda group_id: {"group_id": group_id})

    app = Flask(__name__)
    app.register_blueprint(schema_migrations.bp)

    res = app.test_client().post(
        "/api/schema-migrations/sm-1/plan/items",
        json={
            "strategy": "CDC_DIRECT",
            "prune_cdc_pack": "false",
            "tables": [{"source_table": "ALLORDERS"}],
        },
    )

    assert res.status_code == 201
    assert "prune" not in [call[0] for call in calls]
    assert ("commit",) in calls
    assert ("rollback",) not in calls


def test_schema_migration_add_first_cdc_item_creates_group_plan_and_autostarts(monkeypatch):
    calls = []
    ids = iter(["gid-new", "mid-new", "gt-new"])

    class Cursor:
        def __init__(self):
            self.fetchone_results = [
                ("sm-1", "TCBPAY->PAY", "TCBPAY", "PAY", None, None, None),
                ("PENDING", "topic.prefix", ""),
                (42,),
                (0,),
                None,
                None,
                (7,),
            ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchone(self):
            return self.fetchone_results.pop(0)

    class Conn:
        def __init__(self):
            self.cur = Cursor()

        def cursor(self):
            return self.cur

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    class OracleConn:
        def close(self):
            calls.append(("oracle-close",))

    monkeypatch.setitem(schema_migrations._state, "db_available", {"value": True})
    monkeypatch.setitem(schema_migrations._state, "get_conn", lambda: Conn())
    monkeypatch.setitem(schema_migrations._state, "broadcast", lambda event: calls.append(("broadcast", event["type"])))
    monkeypatch.setattr(schema_migrations.uuid, "uuid4", lambda: next(ids))
    monkeypatch.setattr(schema_migrations, "_source_oracle_conn", lambda: OracleConn())
    monkeypatch.setattr(
        oracle_browser,
        "get_table_info",
        lambda _conn, _schema, _table: {
            "pk_columns": ["ID"],
            "uk_constraints": [],
            "columns": [{"name": "ID"}],
            "supplemental_log_data_all": "YES",
        },
    )
    monkeypatch.setattr(
        schema_migrations,
        "_autostart_created_cdc_items",
        lambda group_id, status, plan_id, created: calls.append(("autostart", group_id, status, plan_id, created)) or {
            "connector_start": {"group_id": group_id, "status": "TOPICS_CREATING"},
            "connector_start_error": None,
            "plan_start": {"batch": 1, "started": [created[0]["migration_id"]]},
            "plan_starts": [{"batch": 1, "started": [created[0]["migration_id"]]}],
            "plan_start_error": None,
        },
    )
    monkeypatch.setattr(
        schema_migrations,
        "_load_created_plan_item_states",
        lambda _conn, created: [{
            "item_id": created[0]["item_id"],
            "table": created[0]["table"],
            "migration_id": created[0]["migration_id"],
            "batch_order": created[0]["batch_order"],
            "status": "RUNNING",
            "phase": "NEW",
            "queue_position": None,
            "error_text": None,
        }],
    )
    monkeypatch.setattr(
        schema_migrations,
        "_load_cdc_connector_summary",
        lambda group_id: {"group_id": group_id, "tables": [{"source_table": "ALLORDERS"}], "table_include_list": "TCBPAY.ALLORDERS"},
    )

    app = Flask(__name__)
    app.register_blueprint(schema_migrations.bp)

    res = app.test_client().post(
        "/api/schema-migrations/sm-1/plan/items",
        json={
            "strategy": "CDC_DIRECT",
            "tables": [{"source_table": "ALLORDERS"}],
        },
    )

    assert res.status_code == 201
    body = res.get_json()
    assert body["plan_id"] == 42
    assert body["connector_group_id"] == "gid-new"
    assert body["connector_start"] == {"group_id": "gid-new", "status": "TOPICS_CREATING"}
    assert body["plan_starts"] == [{"batch": 1, "started": ["mid-new"]}]
    assert body["item_states"] == [{
        "item_id": 7,
        "table": "ALLORDERS",
        "migration_id": "mid-new",
        "batch_order": 1,
        "status": "RUNNING",
        "phase": "NEW",
        "queue_position": None,
        "error_text": None,
    }]
    assert ("autostart", "gid-new", "PENDING", 42, [{
        "item_id": 7,
        "table": "ALLORDERS",
        "migration_id": "mid-new",
        "batch_order": 1,
    }]) in calls
    sqls = [call[1] for call in calls if call[0] == "execute"]
    assert any("INSERT INTO connector_groups" in sql for sql in sqls)
    assert any("SET group_id = %s" in sql for sql in sqls)
    assert any("INSERT INTO migration_plans" in sql for sql in sqls)
    assert any("SET plan_id = %s" in sql for sql in sqls)
    assert any("INSERT INTO group_tables" in sql for sql in sqls)
    assert any("INSERT INTO migrations" in sql for sql in sqls)
    group_table_params = next(
        params for _, sql, params in calls
        if "INSERT INTO group_tables" in sql
    )
    assert group_table_params == (
        "gt-new",
        "gid-new",
        "TCBPAY",
        "ALLORDERS",
        "PAY",
        "ALLORDERS",
        "PRIMARY_KEY",
        '["ID"]',
        True,
        False,
        "topic.prefix.TCBPAY.ALLORDERS",
    )
    migration_params = next(
        params for _, sql, params in calls
        if "INSERT INTO migrations" in sql
    )
    assert migration_params[0] == "mid-new"
    assert migration_params[3:7] == ("TCBPAY", "ALLORDERS", "PAY", "ALLORDERS")
    assert migration_params[14:20] == (
        "PRIMARY_KEY",
        "PK",
        '["ID"]',
        "CDC_DIRECT",
        True,
        "gid-new",
    )
    plan_item_params = next(
        params for _, sql, params in calls
        if "INSERT INTO migration_plan_items" in sql
    )
    assert plan_item_params[:5] == (42, "ALLORDERS", "CDC", 1, 0)
    assert plan_item_params[6] == "mid-new"
    labels = [call[0] for call in calls]
    assert labels.index("commit") < labels.index("autostart")
    assert ("commit",) in calls
    assert ("oracle-close",) in calls
    assert ("close",) in calls


def test_schema_migration_add_items_response_includes_cdc_autostart_snapshot(monkeypatch):
    monkeypatch.setattr(
        schema_migrations,
        "_load_cdc_connector_summary",
        lambda group_id: {
            "group_id": group_id,
            "tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}],
            "table_include_list": "TCBPAY.ALLORDERS",
        },
    )

    payload = schema_migrations._build_add_plan_items_response_payload(
        plan_id=42,
        created=[{"item_id": 7, "table": "ALLORDERS", "migration_id": "mid-1", "batch_order": 3}],
        strategy=schema_migrations.Strategy.CDC_DIRECT,
        connector_group_id="gid-1",
        item_states=[{"migration_id": "mid-1", "status": "RUNNING", "phase": "NEW"}],
        connector_start={"group_id": "gid-1", "status": "RUNNING"},
        plan_starts=[{"batch": 3, "started": ["mid-1"]}],
    )

    assert payload["plan_id"] == 42
    assert payload["connector_group_id"] == "gid-1"
    assert payload["item_states"] == [{"migration_id": "mid-1", "status": "RUNNING", "phase": "NEW"}]
    assert payload["cdc_group"]["table_include_list"] == "TCBPAY.ALLORDERS"
    assert payload["connector_start"]["status"] == "RUNNING"
    assert payload["plan_starts"] == [{"batch": 3, "started": ["mid-1"]}]
    assert payload["cdc_next_action"] == {
        "level": "ok",
        "code": "STARTING",
        "message": "CDC-строки запущены и ожидают назначения worker.",
    }


def test_schema_migration_add_items_response_omits_cdc_snapshot_for_bulk(monkeypatch):
    calls = []
    monkeypatch.setattr(
        schema_migrations,
        "_load_cdc_connector_summary",
        lambda group_id: calls.append(group_id) or {"group_id": group_id},
    )

    payload = schema_migrations._build_add_plan_items_response_payload(
        plan_id=42,
        created=[],
        strategy=schema_migrations.Strategy.BULK_DIRECT,
        connector_group_id="gid-1",
    )

    assert payload["connector_group_id"] is None
    assert payload["cdc_group"] is None
    assert payload["item_states"] == []
    assert payload["plan_starts"] == []
    assert payload["cdc_next_action"] is None
    assert calls == []


def test_schema_migration_cdc_next_action_reports_queued():
    result = schema_migrations._build_cdc_next_action(
        strategy=schema_migrations.Strategy.CDC_DIRECT,
        item_states=[{
            "status": "RUNNING",
            "phase": "NEW",
            "queue_position": 3,
            "cdc_worker_heartbeat": None,
        }],
        connector_start={"status": "RUNNING"},
        connector_start_error=None,
        plan_start_error=None,
        cdc_queue_kicked=True,
    )

    assert result == {
        "level": "ok",
        "code": "QUEUED",
        "message": "CDC-строки поставлены в очередь.",
    }


def test_schema_migration_cdc_next_action_reports_waiting_worker():
    result = schema_migrations._build_cdc_next_action(
        strategy=schema_migrations.Strategy.CDC_DIRECT,
        item_states=[{
            "status": "RUNNING",
            "phase": "CDC_APPLYING",
            "queue_position": None,
            "cdc_worker_heartbeat": None,
        }],
        connector_start={"status": "RUNNING"},
        connector_start_error=None,
        plan_start_error=None,
        cdc_queue_kicked=True,
    )

    assert result == {
        "level": "warn",
        "code": "WAITING_WORKER",
        "message": "CDC-строки готовы к apply и ждут universal worker.",
    }


def test_schema_migration_cdc_next_action_reports_connector_error_with_waiting_rows():
    result = schema_migrations._build_cdc_next_action(
        strategy=schema_migrations.Strategy.CDC_DIRECT,
        item_states=[{
            "status": "RUNNING",
            "phase": "NEW",
            "queue_position": None,
            "cdc_worker_heartbeat": None,
        }],
        connector_start=None,
        connector_start_error="Oracle source is not configured",
        plan_start_error=None,
        cdc_queue_kicked=True,
    )

    assert result == {
        "level": "warn",
        "code": "CONNECTOR_START_FAILED_WAITING",
        "message": (
            "CDC-строки добавлены в очередь ожидания. "
            "Исправьте CDC-пачку и запустите её: Oracle source is not configured"
        ),
    }


def test_schema_migration_loads_created_plan_item_states_in_request_order():
    class Cursor:
        description = [
            ("item_id",),
            ("table",),
            ("migration_id",),
            ("batch_order",),
            ("status",),
            ("phase",),
            ("queue_position",),
            ("error_text",),
            ("cdc_worker_heartbeat",),
        ]

        def __init__(self):
            self.params = None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _sql, params):
            self.params = params

        def fetchall(self):
            return [
                (8, "PAYMENTS", "mid-2", 4, "PENDING", "DRAFT", None, None, None),
                (7, "ALLORDERS", "mid-1", 3, "RUNNING", "NEW", None, None, datetime(2026, 6, 25, 0, 0, tzinfo=timezone.utc)),
            ]

    class Conn:
        def __init__(self):
            self.cur = Cursor()

        def cursor(self):
            return self.cur

    conn = Conn()
    states = schema_migrations._load_created_plan_item_states(conn, [
        {"item_id": 7, "table": "ALLORDERS", "migration_id": "mid-1", "batch_order": 3},
        {"item_id": 9, "table": "MISSING", "migration_id": "mid-missing", "batch_order": 5},
        {"item_id": 8, "table": "PAYMENTS", "migration_id": "mid-2", "batch_order": 4},
    ])

    assert conn.cur.params == (["mid-1", "mid-missing", "mid-2"],)
    assert states == [
        {
            "item_id": 7,
            "table": "ALLORDERS",
            "migration_id": "mid-1",
            "batch_order": 3,
            "status": "RUNNING",
            "phase": "NEW",
            "queue_position": None,
            "error_text": None,
            "cdc_worker_heartbeat": "2026-06-25T00:00:00Z",
        },
        {
            "item_id": 9,
            "table": "MISSING",
            "migration_id": "mid-missing",
            "batch_order": 5,
            "status": None,
            "phase": None,
            "queue_position": None,
            "error_text": None,
            "cdc_worker_heartbeat": None,
        },
        {
            "item_id": 8,
            "table": "PAYMENTS",
            "migration_id": "mid-2",
            "batch_order": 4,
            "status": "PENDING",
            "phase": "DRAFT",
            "queue_position": None,
            "error_text": None,
            "cdc_worker_heartbeat": None,
        },
    ]


def test_schema_migration_prunes_cdc_group_tables_in_current_transaction():
    calls = []

    class Cursor:
        def __init__(self):
            self.fetchall_result = [
                ("id-1", "TCBPAY", "ALLORDERS", "TCBPAY", "ALLORDERS"),
                ("id-2", "TCBPAY", "OLDORDERS", "TCBPAY", "OLDORDERS"),
            ]

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchall(self):
            return self.fetchall_result

        def fetchone(self):
            return None

    cur = Cursor()
    removed = schema_migrations._prune_cdc_group_tables_tx(
        cur,
        "gid-1",
        {"TCBPAY.ALLORDERS"},
    )

    assert removed == [{
        "id": "id-2",
        "source_schema": "TCBPAY",
        "source_table": "OLDORDERS",
        "target_schema": "TCBPAY",
        "target_table": "OLDORDERS",
    }]
    sql = [call[1] for call in calls]
    assert any("SELECT id, source_schema, source_table" in text for text in sql)
    assert any("DELETE FROM group_tables" in text for text in sql)
    assert calls[-1][2] == ("gid-1", "TCBPAY", "OLDORDERS")


def test_schema_migration_prune_rejects_active_removed_cdc_table():
    calls = []

    class Cursor:
        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchall(self):
            return [("id-2", "TCBPAY", "OLDORDERS", "TCBPAY", "OLDORDERS")]

        def fetchone(self):
            return ("mid-active", "CDC_APPLYING")

    with pytest.raises(ValueError) as exc:
        schema_migrations._prune_cdc_group_tables_tx(
            Cursor(),
            "gid-1",
            {"TCBPAY.ALLORDERS"},
        )

    assert "active migration mid-active is in phase CDC_APPLYING" in str(exc.value)
    assert not any("DELETE FROM group_tables" in call[1] for call in calls)


def test_schema_migration_records_stopped_connector_start_error_as_failed(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups,
        "transition_group",
        lambda group_id, status, message=None, error_text=None: calls.append({
            "group_id": group_id,
            "status": status,
            "message": message,
            "error_text": error_text,
        }),
    )

    assert schema_migrations._record_cdc_connector_start_error(
        "gid-1",
        "STOPPED",
        "Oracle source is not configured",
    ) == "FAILED"
    assert calls == [{
        "group_id": "gid-1",
        "status": "FAILED",
        "message": "CDC runtime autostart failed: Oracle source is not configured",
        "error_text": "Oracle source is not configured",
    }]


def test_schema_migration_records_running_connector_sync_error_without_stopping(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups,
        "transition_group",
        lambda group_id, status, message=None, error_text=None: calls.append((group_id, status, error_text)),
    )

    assert schema_migrations._record_cdc_connector_start_error(
        "gid-1",
        "RUNNING",
        "bad table.include.list",
    ) == "RUNNING"
    assert calls == [("gid-1", "RUNNING", "bad table.include.list")]


def test_schema_migration_clears_connector_start_error_after_success(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups,
        "clear_group_error",
        lambda group_id: calls.append(group_id),
    )

    schema_migrations._clear_cdc_connector_start_error("gid-1")

    assert calls == ["gid-1"]


def test_schema_migration_kicks_cdc_group_after_queue_start(monkeypatch):
    calls = []

    monkeypatch.setattr(orchestrator, "_update_queue_positions", lambda: calls.append(("queue",)))
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )

    assert schema_migrations._kick_cdc_group_best_effort("gid-1") is True

    assert calls == [
        ("queue",),
        ("kick", "gid-1"),
    ]


def test_schema_migration_queue_kick_reports_failure(monkeypatch):
    monkeypatch.setattr(
        orchestrator,
        "_update_queue_positions",
        lambda: (_ for _ in ()).throw(RuntimeError("state db down")),
    )
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda _group_id: (_ for _ in ()).throw(AssertionError("must not kick after queue failure")),
    )

    assert schema_migrations._kick_cdc_group_best_effort("gid-1") is False


def test_schema_migration_autostart_contract_starts_connector_and_plan(monkeypatch):
    calls = []

    monkeypatch.setattr(
        schema_migrations,
        "_sync_and_request_cdc_connector_start",
        lambda group_id, status: calls.append(("connector", group_id, status)) or {
            "group_id": group_id,
            "status": "RUNNING",
        },
    )
    monkeypatch.setattr(
        schema_migrations,
        "_clear_cdc_connector_start_error",
        lambda group_id: calls.append(("clear", group_id)),
    )
    monkeypatch.setattr(
        schema_migrations,
        "_start_created_cdc_plan_batches",
        lambda plan_id, created: calls.append(("plan", plan_id, created)) or [
            {"batch": 2, "started": ["mid-1"]},
        ],
    )
    monkeypatch.setattr(
        schema_migrations,
        "_kick_cdc_group_best_effort",
        lambda group_id: calls.append(("kick", group_id)) or True,
    )
    monkeypatch.setattr(
        schema_migrations,
        "_kick_cdc_group_lifecycle_best_effort",
        lambda group_id: calls.append(("lifecycle", group_id)) or True,
    )
    monkeypatch.setitem(
        schema_migrations._state,
        "broadcast",
        lambda event: calls.append(("broadcast", event["type"], event["status"])),
    )

    result = schema_migrations._autostart_created_cdc_items(
        "gid-1",
        "STOPPED",
        42,
        [{"migration_id": "mid-1", "batch_order": 2}],
    )

    assert result == {
        "connector_start": {"group_id": "gid-1", "status": "RUNNING"},
        "connector_start_error": None,
        "plan_start": {"batch": 2, "started": ["mid-1"]},
        "plan_starts": [{"batch": 2, "started": ["mid-1"]}],
        "plan_start_error": None,
        "cdc_queue_kicked": True,
    }
    assert calls == [
        ("connector", "gid-1", "STOPPED"),
        ("clear", "gid-1"),
        ("broadcast", "connector_group_status", "RUNNING"),
        ("lifecycle", "gid-1"),
        ("plan", 42, [{"migration_id": "mid-1", "batch_order": 2}]),
        ("kick", "gid-1"),
    ]


def test_schema_migration_autostart_queues_items_when_stopped_connector_start_fails(monkeypatch):
    calls = []

    monkeypatch.setattr(
        schema_migrations,
        "_sync_and_request_cdc_connector_start",
        lambda *_args: (_ for _ in ()).throw(ValueError("Oracle source is not configured")),
    )
    monkeypatch.setattr(
        schema_migrations,
        "_record_cdc_connector_start_error",
        lambda group_id, status, error: calls.append(("record", group_id, status, error)) or "FAILED",
    )
    monkeypatch.setattr(
        schema_migrations,
        "_start_created_cdc_plan_batches",
        lambda plan_id, created: calls.append(("plan", plan_id, created)) or [
            {"batch": created[0]["batch_order"], "started": [created[0]["migration_id"]]},
        ],
    )
    monkeypatch.setattr(
        schema_migrations,
        "_kick_cdc_group_best_effort",
        lambda group_id: calls.append(("kick", group_id)) or True,
    )
    monkeypatch.setitem(
        schema_migrations._state,
        "broadcast",
        lambda event: calls.append(("broadcast", event["type"], event["status"])),
    )

    result = schema_migrations._autostart_created_cdc_items(
        "gid-1",
        "STOPPED",
        42,
        [{"migration_id": "mid-1", "batch_order": 2}],
    )

    assert result == {
        "connector_start": None,
        "connector_start_error": "Oracle source is not configured",
        "plan_start": {"batch": 2, "started": ["mid-1"]},
        "plan_starts": [{"batch": 2, "started": ["mid-1"]}],
        "plan_start_error": None,
        "cdc_queue_kicked": True,
    }
    assert calls == [
        ("record", "gid-1", "STOPPED", "Oracle source is not configured"),
        ("broadcast", "connector_group_status", "FAILED"),
        ("plan", 42, [{"migration_id": "mid-1", "batch_order": 2}]),
        ("kick", "gid-1"),
    ]


def test_schema_migration_autostart_running_sync_error_does_not_start_plan(monkeypatch):
    calls = []

    monkeypatch.setattr(
        schema_migrations,
        "_sync_and_request_cdc_connector_start",
        lambda *_args: (_ for _ in ()).throw(ValueError("bad table.include.list")),
    )
    monkeypatch.setattr(
        schema_migrations,
        "_record_cdc_connector_start_error",
        lambda group_id, status, error: calls.append(("record", group_id, status, error)) or "RUNNING",
    )
    monkeypatch.setattr(
        schema_migrations,
        "_start_created_cdc_plan_batches",
        lambda *_args: calls.append(("plan",)),
    )
    monkeypatch.setitem(
        schema_migrations._state,
        "broadcast",
        lambda event: calls.append(("broadcast", event["status"])),
    )

    result = schema_migrations._autostart_created_cdc_items(
        "gid-1",
        "RUNNING",
        42,
        [{"migration_id": "mid-1", "batch_order": 2}],
    )

    assert result["connector_start"] is None
    assert result["connector_start_error"] == "bad table.include.list"
    assert result["plan_starts"] == []
    assert result["cdc_queue_kicked"] is False
    assert calls == [
        ("record", "gid-1", "RUNNING", "bad table.include.list"),
        ("broadcast", "RUNNING"),
    ]


def test_schema_migration_ensures_cdc_group_topics(monkeypatch):
    from services import connector_groups

    monkeypatch.setattr(
        connector_groups,
        "create_group_topics",
        lambda group_id: [{"topic_name": "topic-1", "status": "ok"}],
    )

    assert schema_migrations._ensure_cdc_group_topics("gid-1") == [
        {"topic_name": "topic-1", "status": "ok"},
    ]


def test_schema_migration_ensure_cdc_group_topics_raises_on_error(monkeypatch):
    from services import connector_groups

    monkeypatch.setattr(
        connector_groups,
        "create_group_topics",
        lambda group_id: [{"topic_name": "topic-1", "status": "error", "error": "boom"}],
    )

    try:
        schema_migrations._ensure_cdc_group_topics("gid-1")
    except ValueError as exc:
        assert "CDC topic creation failed" in str(exc)
        assert "topic-1: boom" in str(exc)
    else:
        raise AssertionError("expected CDC topic creation failure")


def test_connector_group_key_columns_include_unique_keys(monkeypatch):
    monkeypatch.setattr(
        connector_groups,
        "get_group_tables",
        lambda group_id: [
            {
                "source_schema": "TCBPAY",
                "source_table": "ALLORDERS",
                "source_pk_exists": False,
                "source_uk_exists": True,
                "effective_key_columns_json": '["ORDER_ID", "MERCHANT_ID"]',
            },
            {
                "source_schema": "TCBPAY",
                "source_table": "PAYMENTS",
                "source_pk_exists": True,
                "source_uk_exists": False,
                "effective_key_columns_json": '["PAYMENT_ID"]',
            },
            {
                "source_schema": "TCBPAY",
                "source_table": "MANUAL_KEY_TABLE",
                "source_pk_exists": False,
                "source_uk_exists": False,
                "effective_key_columns_json": ["ID"],
            },
            {
                "source_schema": "TCBPAY",
                "source_table": "MERCHANTS#ORDERS",
                "source_pk_exists": False,
                "source_uk_exists": False,
                "effective_key_columns_json": ["MERCHANT_ID", "ORDER_ID"],
            },
        ],
    )

    assert (
        connector_groups._build_key_columns("gid-1")
        == "TCBPAY.ALLORDERS:ORDER_ID,MERCHANT_ID;"
        "TCBPAY.MANUAL_KEY_TABLE:ID;"
        "TCBPAY.MERCHANTS#ORDERS:MERCHANT_ID,ORDER_ID"
    )


def test_connector_group_topic_creation_uses_active_run_topic_names(monkeypatch):
    from services import kafka_topics

    created_topics = []

    class CursorStub:
        description = [
            ("id",),
            ("group_id",),
            ("source_schema",),
            ("source_table",),
            ("target_schema",),
            ("target_table",),
            ("effective_key_type",),
            ("effective_key_columns_json",),
            ("source_pk_exists",),
            ("source_uk_exists",),
            ("topic_name",),
            ("created_at",),
        ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args):
            pass

        def fetchall(self):
            return [(
                "row-1",
                "gid-1",
                "TCBPAY",
                "MERCHANTS#ORDERS",
                "TCBPAY",
                "MERCHANTS#ORDERS",
                "PRIMARY_KEY",
                '["ID"]',
                True,
                False,
                "stale.topic.TCBPAY.MERCHANTS_ORDERS",
                None,
            )]

    class ConnStub:
        def cursor(self):
            return CursorStub()

        def close(self):
            pass

    monkeypatch.setattr(
        connector_groups,
        "get_group",
        lambda group_id: {
            "group_id": group_id,
            "topic_prefix": "base.topic",
            "run_id": "r123ab",
        },
    )
    monkeypatch.setitem(connector_groups._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(connector_groups._state, "row_to_dict", lambda cur, row: {
        desc[0]: value for desc, value in zip(cur.description, row)
    })
    monkeypatch.setitem(connector_groups._state, "load_configs", lambda: {"kafka": {"bootstrap_servers": "k:9092"}})
    monkeypatch.setattr(
        kafka_topics,
        "create_topic",
        lambda bootstrap_servers, topic_name: created_topics.append((bootstrap_servers, topic_name)),
    )

    assert connector_groups.create_group_topics("gid-1") == [
        {"topic_name": "base.topic.r123ab.TCBPAY.MERCHANTS_ORDERS", "status": "ok"},
    ]
    assert created_topics == [
        (["k:9092"], "base.topic.r123ab.TCBPAY.MERCHANTS_ORDERS"),
    ]


def test_connector_group_request_start_syncs_persisted_topic_names(monkeypatch):
    calls = []

    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", params))

    class ConnStub:
        def cursor(self):
            return CursorStub()

        def commit(self):
            calls.append(("commit",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(
        connector_groups,
        "get_group",
        lambda group_id: {
            "group_id": group_id,
            "status": "PENDING",
            "source_connection_id": "oracle_source",
            "topic_prefix": "base.topic",
        },
    )
    monkeypatch.setattr(connector_groups, "_build_table_include_list", lambda group_id: "TCBPAY.ALLORDERS")
    monkeypatch.setattr(connector_groups, "_oracle_cfg", lambda source_connection_id: {"host": "oracle"})
    monkeypatch.setattr(connector_groups, "_gen_run_id", lambda: "r123ab")
    monkeypatch.setattr(connector_groups, "_conn", lambda: ConnStub())
    monkeypatch.setattr(
        connector_groups,
        "_sync_persisted_topic_names",
        lambda cur, group_id, prefix: calls.append(("sync", group_id, prefix)),
    )
    monkeypatch.setattr(
        connector_groups,
        "transition_group",
        lambda group_id, status, message=None: calls.append(("transition", group_id, status, message)),
    )

    assert connector_groups.request_start("gid-1") == {
        "group_id": "gid-1",
        "status": "TOPICS_CREATING",
        "run_id": "r123ab",
    }
    assert ("sync", "gid-1", "base.topic.r123ab") in calls
    assert any(call[:3] == ("transition", "gid-1", "TOPICS_CREATING") for call in calls)


class CursorStub:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.row


def test_schema_migration_active_cdc_table_lookup_finds_non_terminal():
    cur = CursorStub(("mid-1", "NEW"))

    assert schema_migrations._active_cdc_migration_for_group_table(
        cur, "gid-1", "tcbpay", "allorders",
    ) == ("mid-1", "NEW")
    sql, params = cur.executed[0]
    assert params == ("gid-1", "tcbpay", "allorders")
    assert "LEFT(COALESCE(strategy, ''), 4) = 'CDC_'" in sql
    assert "COALESCE(phase, '') NOT IN" in sql


def test_schema_migration_active_cdc_table_lookup_allows_absent():
    cur = CursorStub(None)

    assert schema_migrations._active_cdc_migration_for_group_table(
        cur, "gid-1", "TCBPAY", "ALLORDERS",
    ) is None


def test_schema_migration_reuses_plan_cdc_group_when_schema_group_is_empty():
    assert schema_migrations._resolve_cdc_connector_group_id(
        sm_group_id=None,
        plan_group_id="plan-gid",
        payload_group_id=None,
    ) == "plan-gid"


def test_schema_migration_accepts_matching_payload_cdc_group():
    assert schema_migrations._resolve_cdc_connector_group_id(
        sm_group_id="schema-gid",
        plan_group_id="schema-gid",
        payload_group_id="schema-gid",
    ) == "schema-gid"


def test_schema_migration_uses_payload_cdc_group_when_schema_has_none():
    assert schema_migrations._resolve_cdc_connector_group_id(
        sm_group_id=None,
        plan_group_id=None,
        payload_group_id="payload-gid",
    ) == "payload-gid"


def test_schema_migration_rejects_conflicting_cdc_groups():
    try:
        schema_migrations._resolve_cdc_connector_group_id(
            sm_group_id="schema-gid",
            plan_group_id="plan-gid",
            payload_group_id=None,
        )
    except ValueError as exc:
        assert "only one CDC pack" in str(exc)
    else:
        raise AssertionError("expected CDC pack conflict")


def test_schema_migration_rejects_conflicting_payload_cdc_group():
    try:
        schema_migrations._resolve_cdc_connector_group_id(
            sm_group_id="schema-gid",
            plan_group_id="schema-gid",
            payload_group_id="payload-gid",
        )
    except ValueError as exc:
        assert "only one CDC pack" in str(exc)
    else:
        raise AssertionError("expected CDC pack conflict")


def test_schema_migration_accepts_existing_manual_cdc_key_columns():
    assert schema_migrations._validate_manual_cdc_key_columns(
        {"columns": [{"name": "ID"}, {"name": "DATE_ID"}]},
        ["ID", "DATE_ID"],
    ) == []


def test_schema_migration_rejects_missing_manual_cdc_key_columns():
    assert schema_migrations._validate_manual_cdc_key_columns(
        {"columns": [{"name": "ID"}]},
        ["ID", "MISSING_COL"],
    ) == ["MISSING_COL"]


def test_schema_migration_manual_cdc_key_requires_missing_pk_uk():
    try:
        schema_migrations._effective_cdc_key_info(
            {
                "columns": [{"name": "ID"}, {"name": "ALT_ID"}],
                "pk_columns": ["ID"],
                "uk_constraints": [],
            },
            ["ALT_ID"],
            "TCBPAY",
            "ALLORDERS",
        )
    except ValueError as exc:
        assert "allowed only when PK/UK is missing" in str(exc)
    else:
        raise AssertionError("expected manual CDC key rejection for PK table")


def test_schema_migration_manual_cdc_key_for_no_key_table_is_user_defined():
    assert schema_migrations._effective_cdc_key_info(
        {
            "columns": [{"name": "ID"}, {"name": "MERCHANT_ID"}],
            "pk_columns": [],
            "uk_constraints": [],
        },
        ["ID", "MERCHANT_ID"],
        "TCBPAY",
        "ALLORDERS",
    ) == ("USER_DEFINED", "USER", ["ID", "MERCHANT_ID"], False, False)


def test_schema_migration_requires_known_cdc_supplemental_logging():
    schema_migrations._require_cdc_supplemental_logging(
        {"supplemental_log_data_all": "YES"},
        "TCBPAY",
        "ALLORDERS",
    )

    for value, expected in [
        ("NO", "does not have ALL COLUMNS supplemental logging"),
        (None, "supplemental logging could not be checked"),
    ]:
        try:
            schema_migrations._require_cdc_supplemental_logging(
                {"supplemental_log_data_all": value},
                "TCBPAY",
                "ALLORDERS",
            )
        except ValueError as exc:
            assert expected in str(exc)
        else:
            raise AssertionError("expected supplemental logging rejection")


def test_schema_migration_normalizes_manual_cdc_key_columns_from_csv():
    assert schema_migrations._normalize_manual_cdc_key_columns(
        " id, ID, merchant_id, "
    ) == ["ID", "MERCHANT_ID"]


def test_schema_migration_normalizes_manual_cdc_key_columns_from_list():
    assert schema_migrations._normalize_manual_cdc_key_columns(
        ["id", " ", "ID", "date_id"]
    ) == ["ID", "DATE_ID"]


def test_orchestrator_treats_steady_state_as_plan_done():
    assert orchestrator._plan_item_status_for_phase("STEADY_STATE") == "DONE"


def test_orchestrator_index_enable_phase_releases_load_slot():
    assert "INDEXES_ENABLING" not in orchestrator._HEAVY_PHASES
    assert orchestrator._plan_item_status_for_phase("INDEXES_ENABLING") == "DONE"


def test_orchestrator_baseline_phases_release_load_slot():
    # Baseline writes only to the target Oracle, so it must NOT hold the load
    # slot — the next migration's bulk load (source read) should start as soon as
    # the current migration reaches baseline. Source-read phases keep the slot.
    for phase in (
        "BASELINE_PUBLISHING",
        "BASELINE_LOADING",
        "BASELINE_PUBLISHED",
        "STAGE_DROPPING",
    ):
        assert phase not in orchestrator._HEAVY_PHASES
    for phase in ("CHUNKING", "BULK_LOADING", "STAGE_VALIDATING"):
        assert phase in orchestrator._HEAVY_PHASES


def test_orchestrator_keeps_plan_running_when_no_pending_but_active_items():
    assert orchestrator._plan_status_without_pending(
        active_count=1,
        failed_count=0,
        cancelled_count=0,
    ) == "RUNNING"


def test_orchestrator_closes_plan_when_no_pending_or_active_items():
    assert orchestrator._plan_status_without_pending(
        active_count=0,
        failed_count=0,
        cancelled_count=0,
    ) == "DONE"


def test_orchestrator_kicks_cdc_group_after_auto_starting_next_plan_batch(monkeypatch):
    calls = []

    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=()):
            self.sql = sql
            self.params = params
            calls.append(("execute", " ".join(sql.split())[:80], params))
            self.rowcount = 0
            if "UPDATE migrations" in sql and "phase = 'DRAFT'" in sql:
                self.rowcount = 1

        def fetchone(self):
            if "RETURNING plan_id, batch_order" in self.sql:
                return (42, 1)
            if "SELECT COUNT(*)" in self.sql:
                return (0,)
            if "SELECT batch_order" in self.sql:
                return (2,)
            return None

        def fetchall(self):
            if "SELECT i.item_id, i.migration_id" in self.sql:
                return [(7, "mid-next", "DRAFT", "gid-1", "CDC_DIRECT")]
            return []

    class ConnStub:
        def __init__(self):
            self.cursor_stub = CursorStub()

        def cursor(self):
            return self.cursor_stub

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setitem(orchestrator._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(
        orchestrator._state,
        "broadcast",
        lambda event: calls.append(("broadcast", event["migration_id"], event["phase"])),
    )
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )

    orchestrator._sync_plan_after_transition("mid-done", "STEADY_STATE")

    assert ("broadcast", "mid-next", "NEW") in calls
    assert ("kick", "gid-1") in calls
    assert ("rollback",) not in calls


def test_orchestrator_syncs_cdc_runtime_context_from_group(monkeypatch):
    updates = {}
    monkeypatch.setattr(
        orchestrator,
        "_update",
        lambda migration_id, fields: updates.update({"migration_id": migration_id, **fields}),
    )

    result = orchestrator._sync_cdc_runtime_context(
        "mid-1",
        {
            "source_schema": "TCBPAY",
            "source_table": "ALLORDERS",
            "connector_name": "",
            "topic_prefix": "",
            "consumer_group": "",
        },
        {
            "connector_name": "sm_tcbpay_pay_connector",
            "topic_prefix": "sm.tcbpay.pay",
            "consumer_group_prefix": "sm.tcbpay.pay",
            "run_id": "r123ab",
        },
    )

    assert updates == {
        "migration_id": "mid-1",
        "connector_name": "sm_tcbpay_pay_connector_r123ab",
        "topic_prefix": "sm.tcbpay.pay.r123ab",
        "consumer_group": "sm.tcbpay.pay_TCBPAY_ALLORDERS",
    }
    assert result["topic_prefix"] == "sm.tcbpay.pay.r123ab"


def test_orchestrator_creates_manual_trigger_job_when_cdc_catches_up(monkeypatch):
    calls = []
    monkeypatch.setattr(
        orchestrator,
        "_safe_transition",
        lambda mid, expected, target, **kwargs: calls.append(
            ("transition", mid, expected, target, kwargs.get("message"))
        ) or True,
    )
    monkeypatch.setattr(orchestrator, "_ensure_trigger_job", lambda mid: calls.append(("trigger-job", mid)))

    orchestrator._handle_cdc_caught_up("mid-cdc", {"migration_id": "mid-cdc"})

    assert calls[0][:4] == ("transition", "mid-cdc", "CDC_CAUGHT_UP", "STEADY_STATE")
    assert calls[0][4]
    assert calls[1] == ("trigger-job", "mid-cdc")


def test_orchestrator_skips_trigger_job_when_cdc_catchup_phase_changed(monkeypatch):
    calls = []
    monkeypatch.setattr(
        orchestrator,
        "_safe_transition",
        lambda mid, expected, target, **kwargs: calls.append(
            ("transition", mid, expected, target)
        ) and False,
    )
    monkeypatch.setattr(orchestrator, "_ensure_trigger_job", lambda mid: calls.append(("trigger-job", mid)))

    orchestrator._handle_cdc_caught_up("mid-cdc", {"migration_id": "mid-cdc"})

    assert calls == [("transition", "mid-cdc", "CDC_CAUGHT_UP", "STEADY_STATE")]


def test_orchestrator_waits_for_real_cdc_lag_checkin_before_caught_up(monkeypatch):
    calls = []

    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args):
            pass

        def fetchone(self):
            return (0, None, None)

    class ConnStub:
        def cursor(self):
            return CursorStub()

        def close(self):
            calls.append(("close",))

    monkeypatch.setitem(orchestrator._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(orchestrator._state, "broadcast", lambda event: calls.append(("broadcast", event)))
    monkeypatch.setattr(orchestrator, "_update", lambda *_args, **_kwargs: calls.append(("update",)))
    monkeypatch.setattr(orchestrator, "_transition", lambda *_args, **_kwargs: calls.append(("transition",)))

    orchestrator._handle_cdc_catching_up("mid-cdc", {"migration_id": "mid-cdc"})

    assert calls == [("close",)]


def test_orchestrator_transitions_cdc_caught_up_after_lag_checkin(monkeypatch):
    calls = []

    class UpdatedAt:
        def isoformat(self):
            return "2026-06-25T00:00:00"

    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args):
            pass

        def fetchone(self):
            return (0, {}, UpdatedAt())

    class ConnStub:
        def cursor(self):
            return CursorStub()

        def close(self):
            calls.append(("close",))

    monkeypatch.setitem(orchestrator._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(orchestrator._state, "broadcast", lambda event: calls.append(("broadcast", event["type"], event["total_lag"])))
    monkeypatch.setattr(orchestrator, "_update", lambda mid, fields: calls.append(("update", mid, fields["kafka_lag"])))
    monkeypatch.setattr(orchestrator, "_transition", lambda mid, phase, **kwargs: calls.append(("transition", mid, phase)))

    orchestrator._handle_cdc_catching_up("mid-cdc", {"migration_id": "mid-cdc"})

    assert ("update", "mid-cdc", 0) in calls
    assert ("broadcast", "kafka_lag", 0) in calls
    assert ("transition", "mid-cdc", "CDC_CAUGHT_UP") in calls


def test_orchestrator_broadcasts_created_pending_trigger_job(monkeypatch):
    calls = []

    class ConnStub:
        def close(self):
            calls.append(("close",))

    job = {
        "job_id": "job-1",
        "migration_id": "mid-cdc",
        "state": "PENDING",
        "enabled_count": 0,
        "created": True,
    }

    monkeypatch.setitem(orchestrator._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(orchestrator._state, "broadcast", lambda event: calls.append(("broadcast", event)))
    monkeypatch.setattr(
        orchestrator.target_trigger_jobs,
        "ensure_pending_job",
        lambda conn, mid, requested_by=None: calls.append(("ensure", mid, requested_by)) or job,
    )

    orchestrator._ensure_trigger_job("mid-cdc", requested_by="orchestrator")

    assert calls[0] == ("ensure", "mid-cdc", "orchestrator")
    assert calls[1][0] == "broadcast"
    assert calls[1][1]["type"] == "target_trigger_job"
    assert calls[1][1]["migration_id"] == "mid-cdc"
    assert calls[1][1]["job_id"] == "job-1"
    assert calls[1][1]["state"] == "PENDING"
    assert calls[2] == ("close",)


def test_orchestrator_skips_existing_trigger_job_broadcast(monkeypatch):
    calls = []

    class ConnStub:
        def close(self):
            calls.append(("close",))

    job = {
        "job_id": "job-1",
        "migration_id": "mid-cdc",
        "state": "PENDING",
        "enabled_count": 0,
        "created": False,
    }

    monkeypatch.setitem(orchestrator._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(orchestrator._state, "broadcast", lambda event: calls.append(("broadcast", event)))
    monkeypatch.setattr(
        orchestrator.target_trigger_jobs,
        "ensure_pending_job",
        lambda conn, mid, requested_by=None: calls.append(("ensure", mid, requested_by)) or job,
    )

    result = orchestrator.create_trigger_job("mid-cdc", requested_by="user")

    assert result == job
    assert calls == [
        ("ensure", "mid-cdc", "user"),
        ("close",),
    ]


def test_orchestrator_refreshes_queue_when_group_becomes_running(monkeypatch):
    calls = []

    class ImmediateThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    monkeypatch.setattr(orchestrator.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "do_start_connector",
        lambda group_id: calls.append(("start", group_id)) or {"name": "cdc-1"},
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)),
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "transition_group",
        lambda group_id, status, message=None: calls.append(("transition", group_id, status)),
    )
    monkeypatch.setattr(
        orchestrator,
        "_update_queue_positions",
        lambda: calls.append(("queue",)),
    )
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )
    monkeypatch.setitem(orchestrator._state, "broadcast", lambda event: calls.append(("broadcast", event["status"])))
    orchestrator._group_in_progress.clear()

    orchestrator._handle_group_connector_starting("gid-1")

    assert calls == [
        ("start", "gid-1"),
        ("refresh", "gid-1"),
        ("transition", "gid-1", "RUNNING"),
        ("queue",),
        ("kick", "gid-1"),
        ("broadcast", "RUNNING"),
    ]


def test_orchestrator_chains_group_topics_to_connector_start(monkeypatch):
    calls = []

    class ImmediateThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    monkeypatch.setattr(orchestrator.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "do_create_topics",
        lambda group_id: calls.append(("topics", group_id)) or [
            {"topic_name": "topic-1", "status": "ok"},
        ],
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "transition_group",
        lambda group_id, status, message=None: calls.append(("transition", group_id, status, message)),
    )
    monkeypatch.setattr(
        orchestrator,
        "_handle_group_connector_starting",
        lambda group_id: calls.append(("connector-start", group_id, group_id in orchestrator._group_in_progress)),
    )
    monkeypatch.setitem(orchestrator._state, "broadcast", lambda event: calls.append(("broadcast", event["status"])))
    orchestrator._group_in_progress.clear()

    orchestrator._handle_group_topics_creating("gid-1")

    assert calls == [
        ("topics", "gid-1"),
        ("transition", "gid-1", "CONNECTOR_STARTING", "Создано 1 топиков"),
        ("broadcast", "CONNECTOR_STARTING"),
        ("connector-start", "gid-1", False),
    ]


def test_orchestrator_kicks_first_new_cdc_migration_for_running_group(monkeypatch):
    calls = []

    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            calls.append(("query", params))

        def fetchall(self):
            return [{
                "migration_id": "mid-1",
                "group_id": "gid-1",
                "phase": "NEW",
                "strategy": "CDC_DIRECT",
            }]

    class ConnStub:
        def cursor(self):
            return CursorStub()

        def close(self):
            calls.append(("close",))

    monkeypatch.setitem(orchestrator._state, "get_conn", lambda: ConnStub())
    monkeypatch.setattr(orchestrator, "row_to_dict", lambda _cur, row: dict(row))
    monkeypatch.setattr(
        orchestrator,
        "_handle_new",
        lambda migration_id, migration: calls.append(("handle", migration_id, migration["group_id"])),
    )

    orchestrator._kick_new_migrations_for_group("gid-1")

    assert calls == [
        ("query", ("gid-1",)),
        ("close",),
        ("handle", "mid-1", "gid-1"),
    ]


def test_orchestrator_marks_group_failed_when_connector_start_fails(monkeypatch):
    calls = []

    class ImmediateThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    monkeypatch.setattr(orchestrator.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "do_start_connector",
        lambda group_id: calls.append(("start", group_id)) or (
            (_ for _ in ()).throw(RuntimeError("connect failed"))
        ),
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "transition_group",
        lambda group_id, status, message=None: calls.append(("transition", group_id, status)),
    )
    monkeypatch.setitem(orchestrator._state, "broadcast", lambda event: calls.append(("broadcast", event["status"])))
    orchestrator._group_in_progress.clear()

    orchestrator._handle_group_connector_starting("gid-1")

    assert calls == [
        ("start", "gid-1"),
        ("transition", "gid-1", "FAILED"),
        ("broadcast", "FAILED"),
    ]


def test_orchestrator_kicks_first_new_cdc_migration_for_group(monkeypatch):
    executed = []
    handled = []
    row = {
        "migration_id": "mid-1",
        "group_id": "gid-1",
        "phase": "NEW",
        "strategy": "CDC_DIRECT",
        "state_changed_at": "2026-06-25T00:00:00Z",
    }

    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params=None):
            executed.append((query, params))

        def fetchall(self):
            return [row]

    class ConnStub:
        def cursor(self):
            return CursorStub()

        def close(self):
            pass

    monkeypatch.setitem(orchestrator._state, "get_conn", lambda: ConnStub())
    monkeypatch.setattr(orchestrator, "row_to_dict", lambda _cur, value: dict(value))
    monkeypatch.setattr(orchestrator, "_handle_new", lambda mid, migration: handled.append((mid, migration)))

    orchestrator._kick_new_migrations_for_group("gid-1")

    assert handled == [("mid-1", row)]
    query, params = executed[0]
    assert params == ("gid-1",)
    assert "m.phase = 'NEW'" in query
    assert "LEFT(COALESCE(m.strategy, ''), 4) = 'CDC_'" in query
    assert "LIMIT  1" in query


def test_orchestrator_keeps_new_cdc_migration_waiting_until_group_running(monkeypatch):
    calls = []
    migration = {
        "migration_id": "mid-1",
        "group_id": "gid-1",
        "phase": "NEW",
        "strategy": "CDC_DIRECT",
        "source_pk_exists": True,
        "source_uk_exists": False,
        "effective_key_columns_json": "[]",
    }

    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "get_group",
        lambda group_id: calls.append(("get-group", group_id)) or {
            "group_id": group_id,
            "status": "TOPICS_CREATING",
        },
    )
    monkeypatch.setattr(orchestrator, "_update", lambda mid, values: calls.append(("update", mid, values)))
    monkeypatch.setattr(orchestrator, "_update_queue_positions", lambda: calls.append(("queue",)))
    monkeypatch.setattr(orchestrator, "_fail", lambda *args, **kwargs: calls.append(("fail", args, kwargs)))
    monkeypatch.setattr(orchestrator, "_mark_in_prog", lambda mid: calls.append(("mark", mid)))
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "get_connector_status",
        lambda group_id: calls.append(("runtime-status", group_id)) or "CONNECTOR_STARTING",
    )
    monkeypatch.setattr(
        orchestrator,
        "kick_connector_group_lifecycle",
        lambda group_id: calls.append(("kick-lifecycle", group_id)) or True,
    )

    orchestrator._handle_new("mid-1", migration)

    assert calls == [
        ("get-group", "gid-1"),
        ("runtime-status", "gid-1"),
        ("update", "mid-1", {"queue_position": None}),
        ("kick-lifecycle", "gid-1"),
        ("queue",),
    ]


def test_orchestrator_requests_cdc_group_start_when_new_migration_waits_on_pending_group(monkeypatch):
    calls = []
    migration = {
        "migration_id": "mid-1",
        "group_id": "gid-1",
        "phase": "NEW",
        "strategy": "CDC_DIRECT",
        "source_pk_exists": True,
        "source_uk_exists": False,
        "effective_key_columns_json": "[]",
    }

    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "get_group",
        lambda group_id: calls.append(("get-group", group_id)) or {
            "group_id": group_id,
            "status": "PENDING",
        },
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "request_start",
        lambda group_id: calls.append(("request-start", group_id)) or {
            "group_id": group_id,
            "status": "TOPICS_CREATING",
        },
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "get_connector_status",
        lambda group_id: calls.append(("runtime-status", group_id)) or "NOT_FOUND",
    )
    monkeypatch.setattr(
        orchestrator,
        "kick_connector_group_lifecycle",
        lambda group_id: calls.append(("kick-lifecycle", group_id)) or True,
    )
    monkeypatch.setattr(orchestrator, "_broadcast", lambda event: calls.append(("broadcast", event["status"])))
    monkeypatch.setattr(orchestrator, "_update", lambda mid, values: calls.append(("update", mid, values)))
    monkeypatch.setattr(orchestrator, "_update_queue_positions", lambda: calls.append(("queue",)))
    monkeypatch.setattr(orchestrator, "_fail", lambda *args, **kwargs: calls.append(("fail", args, kwargs)))
    monkeypatch.setattr(orchestrator, "_mark_in_prog", lambda mid: calls.append(("mark", mid)))

    orchestrator._handle_new("mid-1", migration)

    assert calls == [
        ("get-group", "gid-1"),
        ("runtime-status", "gid-1"),
        ("update", "mid-1", {"queue_position": None}),
        ("request-start", "gid-1"),
        ("broadcast", "TOPICS_CREATING"),
        ("kick-lifecycle", "gid-1"),
        ("queue",),
    ]


def test_orchestrator_syncs_cdc_group_running_from_kafka_connect(monkeypatch):
    calls = []
    starting_group = {
        "group_id": "gid-1",
        "status": "CONNECTOR_STARTING",
        "connector_name": "cdc-main",
        "topic_prefix": "cdc.topic",
        "run_id": "r123ab",
    }
    running_group = {**starting_group, "status": "RUNNING"}

    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "get_connector_status",
        lambda group_id: calls.append(("runtime-status", group_id)) or "RUNNING",
    )
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "get_group",
        lambda group_id: calls.append(("get-group", group_id)) or running_group,
    )
    monkeypatch.setattr(orchestrator, "_broadcast", lambda event: calls.append(("broadcast", event["status"])))

    result = orchestrator._sync_cdc_group_runtime_status("mid-1", "gid-1", starting_group)

    assert result["status"] == "RUNNING"
    assert calls == [
        ("runtime-status", "gid-1"),
        ("get-group", "gid-1"),
        ("broadcast", "RUNNING"),
    ]


def test_orchestrator_starts_new_cdc_migration_when_group_running(monkeypatch):
    calls = []
    migration = {
        "migration_id": "mid-1",
        "group_id": "gid-1",
        "phase": "NEW",
        "strategy": "CDC_DIRECT",
        "source_connection_id": "oracle_source",
        "target_connection_id": "oracle_target",
        "source_schema": "TCBPAY",
        "source_table": "ALLORDERS",
        "target_schema": "TCBPAY",
        "target_table": "ALLORDERS",
        "stage_table_name": "STG_TCBPAY_ALLORDERS",
        "stage_tablespace": "",
        "source_pk_exists": True,
        "source_uk_exists": False,
        "effective_key_columns_json": "[]",
        "truncate_target": True,
    }

    class CursorStub:
        def __init__(self, rows):
            self.rows = list(rows)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("query", " ".join(sql.split()), params))

        def fetchone(self):
            return self.rows.pop(0) if self.rows else None

    class ConnStub:
        def __init__(self):
            self.cursors = [
                CursorStub([None]),          # no heavy slot busy
                CursorStub([("mid-1",)]),    # this migration is first runnable NEW
            ]

        def cursor(self):
            return self.cursors.pop(0)

        def close(self):
            calls.append(("close",))

    class ImmediateThread:
        def __init__(self, target, **_kwargs):
            self.target = target

        def start(self):
            self.target()

    monkeypatch.setitem(orchestrator._state, "get_conn", lambda: ConnStub())
    monkeypatch.setattr(orchestrator.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(
        orchestrator.connector_groups_svc,
        "get_group",
        lambda group_id: calls.append(("get-group", group_id)) or {
            "group_id": group_id,
            "status": "RUNNING",
            "connector_name": "cdc-main",
            "topic_prefix": "cdc.topic",
            "consumer_group_prefix": "cdc.consumer",
            "run_id": "r123ab",
        },
    )
    monkeypatch.setattr(
        orchestrator,
        "_sync_cdc_runtime_context",
        lambda mid, m, group: calls.append(("runtime", mid, group["run_id"])) or {
            **m,
            "connector_name": "cdc-main_r123ab",
            "topic_prefix": "cdc.topic.r123ab",
            "consumer_group": "cdc.consumer_TCBPAY_ALLORDERS",
        },
    )
    monkeypatch.setattr(orchestrator, "_update", lambda mid, values: calls.append(("update", mid, values)))
    monkeypatch.setattr(orchestrator.oracle_scn, "check_supplemental_logging", lambda *_args: True)
    monkeypatch.setattr(orchestrator, "_oracle_cfg", lambda conn_id: {"conn_id": conn_id})
    monkeypatch.setattr(
        orchestrator,
        "_prepare_target_for_direct_load",
        lambda mid, m, dst_cfg, msg_parts: calls.append(("prepare-target", mid, dst_cfg["conn_id"])) or msg_parts.append("target prepared"),
    )
    monkeypatch.setattr(
        orchestrator,
        "_safe_transition",
        lambda mid, expected, to_phase, message=None, **_kwargs: calls.append(("transition", mid, expected, to_phase, message)),
    )
    orchestrator._in_progress.clear()

    orchestrator._handle_new("mid-1", migration)

    assert ("runtime", "mid-1", "r123ab") in calls
    assert ("update", "mid-1", {"queue_position": None}) in calls
    assert ("prepare-target", "mid-1", "oracle_target") in calls
    assert any(
        call[0] == "transition"
        and call[1:4] == ("mid-1", "NEW", "TOPIC_CREATING")
        and "create Kafka topic" in (call[4] or "")
        for call in calls
    )
