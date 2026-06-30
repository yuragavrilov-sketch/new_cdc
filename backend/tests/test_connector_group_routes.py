from flask import Flask

from routes import connector_groups
from routes import planner
from services import orchestrator
from services import connector_groups as connector_groups_svc


def test_legacy_connector_group_migration_error_points_to_schema_screen():
    payload = connector_groups._legacy_cdc_migration_error()

    assert "Legacy direct CDC migration flow is disabled" in payload["error"]
    assert "schema migration screen" in payload["error"]
    assert "queued and autostarted" in payload["error"]


def test_legacy_connector_group_membership_error_points_to_schema_screen():
    payload = connector_groups._legacy_cdc_membership_error()

    assert "Direct CDC pack table edits are disabled" in payload["error"]
    assert "schema migration screen" in payload["error"]
    assert "enters the queue" in payload["error"]
    assert "autostarted" in payload["error"]


def test_create_connector_group_rejects_direct_group_creation(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "create_group",
        lambda **kwargs: calls.append(("create", kwargs)),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post(
        "/api/connector-groups",
        json={
            "group_name": "CDC",
            "connector_name": "cdc_connector",
            "topic_prefix": "cdc",
        },
    )

    assert res.status_code == 400
    assert "Direct CDC pack creation is disabled" in res.get_json()["error"]
    assert "schema migration screen" in res.get_json()["error"]
    assert calls == []


def test_remove_group_table_returns_warning_when_sync_fails_after_delete(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "remove_table",
        lambda group_id, schema, table: calls.append(("remove", group_id, schema, table)),
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)) or (
            (_ for _ in ()).throw(RuntimeError("connect unavailable"))
        ),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().delete("/api/connector-groups/gid-1/tables/TCBPAY/ALLORDERS")

    assert res.status_code == 200
    assert res.get_json() == {
        "removed": True,
        "sync_error": "CDC runtime config sync failed: connect unavailable",
    }
    assert calls == [
        ("remove", "gid-1", "TCBPAY", "ALLORDERS"),
        ("refresh", "gid-1"),
    ]


def test_prune_group_tables_syncs_debezium_once(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "prune_tables",
        lambda group_id, keep_tables: calls.append(("prune", group_id, keep_tables)) or [
            {"source_schema": "TCBPAY", "source_table": "OLDORDERS"},
        ],
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post(
        "/api/connector-groups/gid-1/tables/prune",
        json={"keep_tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}]},
    )

    assert res.status_code == 200
    assert res.get_json() == {
        "removed": [{"source_schema": "TCBPAY", "source_table": "OLDORDERS"}],
        "removed_count": 1,
        "synced": True,
    }
    assert calls == [
        ("prune", "gid-1", [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}]),
        ("refresh", "gid-1"),
    ]


def test_prune_group_tables_returns_warning_when_sync_fails(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "prune_tables",
        lambda group_id, keep_tables: calls.append(("prune", group_id, keep_tables)) or [
            {"source_schema": "TCBPAY", "source_table": "OLDORDERS"},
        ],
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)) or (
            (_ for _ in ()).throw(RuntimeError("connect unavailable"))
        ),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post(
        "/api/connector-groups/gid-1/tables/prune",
        json={"keep_tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}]},
    )

    assert res.status_code == 200
    assert res.get_json() == {
        "removed": [{"source_schema": "TCBPAY", "source_table": "OLDORDERS"}],
        "removed_count": 1,
        "sync_error": "CDC runtime config sync failed: connect unavailable",
    }
    assert calls == [
        ("prune", "gid-1", [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}]),
        ("refresh", "gid-1"),
    ]


def test_add_group_tables_rejects_direct_membership_edits(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "add_tables",
        lambda group_id, tables: calls.append(("add", group_id, tables)) or [
            {"source_schema": "TCBPAY", "source_table": "ALLORDERS"},
        ],
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)) or (
            (_ for _ in ()).throw(RuntimeError("connect unavailable"))
        ),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post(
        "/api/connector-groups/gid-1/tables",
        json={"tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}]},
    )

    assert res.status_code == 400
    assert "Direct CDC pack table edits are disabled" in res.get_json()["error"]
    assert calls == []


def test_get_connector_group_includes_persisted_debezium_lists(monkeypatch):
    monkeypatch.setattr(
        connector_groups_svc,
        "get_group",
        lambda group_id: {
            "group_id": group_id,
            "group_name": "CDC",
            "source_connection_id": "oracle_source",
            "connector_name": "cdc-main",
            "topic_prefix": "cdc.main",
            "consumer_group_prefix": "cdc.main",
            "status": "RUNNING",
            "error_text": None,
        },
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "get_group_tables",
        lambda group_id: [
            {"source_schema": "TCBPAY", "source_table": "ALLORDERS"},
            {"source_schema": "TCBPAY", "source_table": "MERCHANTS#ORDERS"},
        ],
    )
    monkeypatch.setattr(connector_groups_svc, "get_group_migrations", lambda group_id: [])
    monkeypatch.setattr(
        connector_groups_svc,
        "_build_table_include_list",
        lambda group_id: "TCBPAY.ALLORDERS,TCBPAY.MERCHANTS#ORDERS",
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "_build_key_columns",
        lambda group_id: "TCBPAY.ALLORDERS:ID",
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().get("/api/connector-groups/gid-1")

    assert res.status_code == 200
    body = res.get_json()
    assert body["table_include_list"] == "TCBPAY.ALLORDERS,TCBPAY.MERCHANTS#ORDERS"
    assert body["message_key_columns"] == "TCBPAY.ALLORDERS:ID"
    assert [t["source_table"] for t in body["tables"]] == ["ALLORDERS", "MERCHANTS#ORDERS"]


def test_debezium_sync_status_route_returns_comparison(monkeypatch):
    monkeypatch.setattr(
        connector_groups_svc,
        "get_debezium_sync_status",
        lambda group_id: {
            "connector_name": "cdc-main",
            "exists": True,
            "in_sync": False,
            "desired_table_include_list": "TCBPAY.ALLORDERS",
            "actual_table_include_list": "TCBPAY.ALLORDERS,TCBPAY.MERCHANTS#ORDERS",
            "desired_message_key_columns": "",
            "actual_message_key_columns": "",
            "missing_tables": [],
            "extra_tables": ["TCBPAY.MERCHANTS#ORDERS"],
            "key_columns_match": True,
        },
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().get("/api/connector-groups/gid-1/debezium-sync-status")

    assert res.status_code == 200
    body = res.get_json()
    assert body["in_sync"] is False
    assert body["extra_tables"] == ["TCBPAY.MERCHANTS#ORDERS"]


def test_connector_group_wizard_rejects_direct_membership_edits():
    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post(
        "/api/connector-groups/wizard",
        json={
            "group_name": "CDC",
            "connector_name": "cdc_connector",
            "topic_prefix": "cdc",
            "tables": [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}],
        },
    )

    assert res.status_code == 400
    assert "Direct CDC pack table edits are disabled" in res.get_json()["error"]


def test_refresh_tables_starts_pending_cdc_batches_after_sync(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)),
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "get_group",
        lambda group_id: calls.append(("get-group", group_id)) or {"group_id": group_id, "status": "RUNNING"},
    )
    monkeypatch.setattr(
        connector_groups,
        "_start_pending_cdc_plan_batches_for_group",
        lambda group_id: calls.append(("start-pending", group_id)) or [
            {"batch": 4, "started": ["mid-1"]},
        ],
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post("/api/connector-groups/gid-1/refresh-tables")

    assert res.status_code == 200
    assert res.get_json() == {
        "ok": True,
        "status": "RUNNING",
        "plan_starts": [{"batch": 4, "started": ["mid-1"]}],
        "plan_start_error": None,
        "cdc_queue_kicked": True,
        "cdc_next_action": {
            "level": "ok",
            "code": "QUEUED",
            "message": "CDC-строки поставлены в очередь: 1.",
        },
    }
    assert calls == [
        ("refresh", "gid-1"),
        ("get-group", "gid-1"),
        ("start-pending", "gid-1"),
    ]


def test_start_group_starts_pending_cdc_batches_after_request_start(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "request_start",
        lambda group_id: calls.append(("request-start", group_id)) or {
            "group_id": group_id,
            "status": "RUNNING",
        },
    )
    monkeypatch.setattr(
        connector_groups,
        "_start_pending_cdc_plan_batches_for_group",
        lambda group_id: calls.append(("start-pending", group_id)) or [
            {"batch": 2, "started": ["mid-1"]},
        ],
    )
    monkeypatch.setattr(
        connector_groups,
        "_kick_group_lifecycle_best_effort",
        lambda group_id: calls.append(("lifecycle", group_id)) or True,
    )
    monkeypatch.setitem(
        connector_groups._state,
        "broadcast",
        lambda event: calls.append(("broadcast", event["group_id"], event["status"])),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post("/api/connector-groups/gid-1/start")

    assert res.status_code == 200
    assert res.get_json() == {
        "group_id": "gid-1",
        "status": "RUNNING",
        "plan_starts": [{"batch": 2, "started": ["mid-1"]}],
        "plan_start_error": None,
        "cdc_queue_kicked": True,
        "cdc_next_action": {
            "level": "ok",
            "code": "QUEUED",
            "message": "CDC-строки поставлены в очередь: 1.",
        },
    }
    assert calls == [
        ("request-start", "gid-1"),
        ("broadcast", "gid-1", "RUNNING"),
        ("lifecycle", "gid-1"),
        ("start-pending", "gid-1"),
    ]


def test_start_group_kicks_existing_new_cdc_when_already_running(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "request_start",
        lambda group_id: calls.append(("request-start", group_id)) or {
            "group_id": group_id,
            "status": "RUNNING",
        },
    )
    monkeypatch.setattr(
        connector_groups,
        "_start_pending_cdc_plan_batches_for_group",
        lambda group_id: calls.append(("start-pending", group_id)) or [],
    )
    monkeypatch.setattr(
        connector_groups,
        "_kick_group_lifecycle_best_effort",
        lambda group_id: calls.append(("lifecycle", group_id)) or True,
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "get_group",
        lambda group_id: calls.append(("get-group", group_id)) or {"group_id": group_id, "status": "RUNNING"},
    )
    monkeypatch.setattr(
        connector_groups,
        "_has_existing_new_cdc_rows_for_group",
        lambda group_id: calls.append(("has-new", group_id)) or True,
    )
    monkeypatch.setattr(orchestrator, "_update_queue_positions", lambda: calls.append(("queue",)))
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )
    monkeypatch.setitem(
        connector_groups._state,
        "broadcast",
        lambda event: calls.append(("broadcast", event["group_id"], event["status"])),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post("/api/connector-groups/gid-1/start")

    assert res.status_code == 200
    assert res.get_json() == {
        "group_id": "gid-1",
        "status": "RUNNING",
        "plan_starts": [],
        "plan_start_error": None,
        "cdc_queue_kicked": True,
        "cdc_next_action": {
            "level": "ok",
            "code": "QUEUE_KICKED",
            "message": "CDC-очередь проверена и продолжена.",
        },
    }
    assert calls == [
        ("request-start", "gid-1"),
        ("broadcast", "gid-1", "RUNNING"),
        ("lifecycle", "gid-1"),
        ("start-pending", "gid-1"),
        ("get-group", "gid-1"),
        ("has-new", "gid-1"),
        ("queue",),
        ("kick", "gid-1"),
    ]


def test_refresh_tables_kicks_existing_new_cdc_when_group_running(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "refresh_connector_tables",
        lambda group_id: calls.append(("refresh", group_id)),
    )
    monkeypatch.setattr(
        connector_groups,
        "_start_pending_cdc_plan_batches_for_group",
        lambda group_id: calls.append(("start-pending", group_id)) or [],
    )
    monkeypatch.setattr(
        connector_groups_svc,
        "get_group",
        lambda group_id: calls.append(("get-group", group_id)) or {"group_id": group_id, "status": "RUNNING"},
    )
    monkeypatch.setattr(
        connector_groups,
        "_has_existing_new_cdc_rows_for_group",
        lambda group_id: calls.append(("has-new", group_id)) or True,
    )
    monkeypatch.setattr(orchestrator, "_update_queue_positions", lambda: calls.append(("queue",)))
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )

    app = Flask(__name__)
    app.register_blueprint(connector_groups.bp)

    res = app.test_client().post("/api/connector-groups/gid-1/refresh-tables")

    assert res.status_code == 200
    assert res.get_json() == {
        "ok": True,
        "status": "RUNNING",
        "plan_starts": [],
        "plan_start_error": None,
        "cdc_queue_kicked": True,
        "cdc_next_action": {
            "level": "ok",
            "code": "QUEUE_KICKED",
            "message": "CDC-очередь проверена и продолжена.",
        },
    }
    assert calls == [
        ("refresh", "gid-1"),
        ("get-group", "gid-1"),
        ("start-pending", "gid-1"),
        ("get-group", "gid-1"),
        ("has-new", "gid-1"),
        ("queue",),
        ("kick", "gid-1"),
    ]


def test_kick_existing_new_cdc_returns_false_when_no_new_rows(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups_svc,
        "get_group",
        lambda group_id: calls.append(("get-group", group_id)) or {"group_id": group_id, "status": "RUNNING"},
    )
    monkeypatch.setattr(
        connector_groups,
        "_has_existing_new_cdc_rows_for_group",
        lambda group_id: calls.append(("has-new", group_id)) or False,
    )
    monkeypatch.setattr(orchestrator, "_update_queue_positions", lambda: calls.append(("queue",)))
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )

    assert connector_groups._kick_existing_new_cdc_for_running_group("gid-1") is False
    assert calls == [
        ("get-group", "gid-1"),
        ("has-new", "gid-1"),
    ]


def test_connector_group_next_action_reports_plan_start_error():
    assert connector_groups._build_cdc_group_next_action(
        status="RUNNING",
        plan_starts=[],
        plan_start_error="state db busy",
        cdc_queue_kicked=False,
    ) == {
        "level": "error",
        "code": "PLAN_START_FAILED",
        "message": "CDC-пачка обработана, но очередь не продолжена: state db busy",
    }


def test_has_existing_new_cdc_rows_filters_by_group_phase_and_strategy(monkeypatch):
    calls = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchone(self):
            return (1,)

    class Conn:
        def cursor(self):
            return Cursor()

        def close(self):
            calls.append(("close",))

    monkeypatch.setitem(connector_groups._state, "get_conn", lambda: Conn())

    assert connector_groups._has_existing_new_cdc_rows_for_group("gid-1") is True
    sql = calls[0][1]
    assert "WHERE group_id = %s AND phase = 'NEW'" in sql
    assert "LEFT(COALESCE(strategy, ''), 4) = 'CDC_'" in sql
    assert calls[0][2] == ("gid-1",)
    assert ("close",) in calls


def test_start_pending_cdc_plan_batches_kicks_group(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups,
        "_pending_cdc_plan_batches_for_group",
        lambda group_id: calls.append(("load", group_id)) or [(42, 2), (42, 3)],
    )
    monkeypatch.setattr(
        planner,
        "_start_next_plan_batch",
        lambda plan_id, **kwargs: calls.append(("start", plan_id, kwargs)) or {
            "batch": kwargs["batch_order"],
            "started": [f"mid-{kwargs['batch_order']}"],
        },
    )
    monkeypatch.setattr(orchestrator, "_update_queue_positions", lambda: calls.append(("queue",)))
    monkeypatch.setattr(
        orchestrator,
        "_kick_new_migrations_for_group",
        lambda group_id: calls.append(("kick", group_id)),
    )

    assert connector_groups._start_pending_cdc_plan_batches_for_group("gid-1") == [
        {"batch": 2, "started": ["mid-2"]},
        {"batch": 3, "started": ["mid-3"]},
    ]
    assert calls == [
        ("load", "gid-1"),
        ("start", 42, {
            "actor": "SYSTEM",
            "batch_order": 2,
            "allow_cdc_queue_when_blocked": True,
        }),
        ("start", 42, {
            "actor": "SYSTEM",
            "batch_order": 3,
            "allow_cdc_queue_when_blocked": True,
        }),
        ("queue",),
        ("kick", "gid-1"),
    ]
