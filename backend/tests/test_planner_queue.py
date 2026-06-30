from __future__ import annotations

from flask import Flask

from routes import planner


def test_can_start_cdc_batch_while_cdc_is_running():
    assert planner._can_start_plan_batch(
        running_items=[("CDC", "CDC_DIRECT")],
        pending_items=[("CDC", "CDC_DIRECT")],
    )


def test_cdc_queue_rule_accepts_strategy_when_mode_is_missing():
    assert planner._can_start_plan_batch(
        running_items=[("", "CDC_STAGE")],
        pending_items=[("", "CDC_DIRECT")],
    )


def test_running_bulk_allows_cdc_batch():
    assert planner._can_start_plan_batch(
        running_items=[("BULK", "BULK_DIRECT")],
        pending_items=[("CDC", "CDC_DIRECT")],
    )


def test_running_cdc_allows_pending_bulk_batch():
    assert planner._can_start_plan_batch(
        running_items=[("CDC", "CDC_DIRECT")],
        pending_items=[("BULK", "BULK_DIRECT")],
    )


def test_running_bulk_blocks_pending_bulk_batch():
    assert not planner._can_start_plan_batch(
        running_items=[("BULK", "BULK_DIRECT")],
        pending_items=[("BULK", "BULK_DIRECT")],
    )


def test_index_enabling_counts_as_done_for_pack_queue():
    assert planner._plan_item_status_for_phase("INDEXES_ENABLING") == "DONE"


def test_can_force_queue_explicit_cdc_batch():
    assert planner._can_force_queue_cdc_batch(
        batch_order=7,
        pending_items=[("CDC", "CDC_DIRECT")],
    )


def test_cannot_force_queue_implicit_or_bulk_batch():
    assert not planner._can_force_queue_cdc_batch(
        batch_order=None,
        pending_items=[("CDC", "CDC_DIRECT")],
    )
    assert not planner._can_force_queue_cdc_batch(
        batch_order=7,
        pending_items=[("BULK", "BULK_DIRECT")],
    )


def test_legacy_payload_detects_cdc_mode():
    assert planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "mode": "CDC", "overrides": {}}]}],
        defaults={},
    )


def test_legacy_payload_detects_default_cdc_strategy():
    assert planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "overrides": {}}]}],
        defaults={"strategy": "CDC_STAGE"},
    )


def test_legacy_payload_detects_implicit_cdc_defaults():
    assert planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "overrides": {}}]}],
        defaults={},
    )


def test_legacy_payload_allows_bulk():
    assert not planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "mode": "BULK", "overrides": {"strategy": "BULK_STAGE"}}]}],
        defaults={},
    )


def test_legacy_payload_allows_bulk_default_strategy_without_mode():
    assert not planner._legacy_payload_has_cdc(
        batches=[{"tables": [{"table": "T1", "overrides": {}}]}],
        defaults={"strategy": "BULK_STAGE"},
    )


def test_planner_execute_rejects_cdc_payload_before_db(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(planner.bp)

    def fail_get_conn():
        raise AssertionError("legacy planner CDC reject must not open state DB connection")

    monkeypatch.setitem(planner._state, "get_conn", fail_get_conn)

    response = app.test_client().post("/api/planner/execute", json={
        "src_schema": "TCBPAY",
        "tgt_schema": "TCBPAY",
        "name": "CDC plan",
        "create_connector_group": {
            "group_name": "CDC",
            "connector_name": "cdc-main",
            "topic_prefix": "cdc.main",
        },
        "defaults": {"strategy": "CDC_DIRECT"},
        "batches": [{"order": 1, "tables": [{"table": "ALLORDERS"}]}],
    })

    assert response.status_code == 400
    payload = response.get_json()
    assert "Legacy CDC queue flow is disabled" in payload["error"]
    assert "schema migration screen" in payload["error"]


def test_planner_add_items_rejects_cdc_payload_before_db(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(planner.bp)

    def fail_get_conn():
        raise AssertionError("legacy planner CDC reject must not open state DB connection")

    monkeypatch.setitem(planner._state, "get_conn", fail_get_conn)

    response = app.test_client().post("/api/planner/plans/42/items", json={
        "defaults": {"strategy": "CDC_STAGE"},
        "batches": [{"order": 1, "tables": [{"table": "ALLORDERS"}]}],
    })

    assert response.status_code == 400
    payload = response.get_json()
    assert "Legacy CDC queue flow is disabled" in payload["error"]
    assert "schema migration screen" in payload["error"]


def test_plan_item_status_for_active_phase_is_running():
    assert planner._plan_item_status_for_phase("NEW") == "RUNNING"
    assert planner._plan_item_status_for_phase("CDC_APPLYING") == "RUNNING"


def test_plan_item_status_for_terminal_phase():
    assert planner._plan_item_status_for_phase("COMPLETED") == "DONE"
    assert planner._plan_item_status_for_phase("STEADY_STATE") == "DONE"
    assert planner._plan_item_status_for_phase("FAILED") == "FAILED"
    assert planner._plan_item_status_for_phase("CANCELLED") == "CANCELLED"


def test_queue_position_refresh_is_best_effort(monkeypatch):
    from services import orchestrator

    monkeypatch.setattr(
        orchestrator,
        "_update_queue_positions",
        lambda: (_ for _ in ()).throw(RuntimeError("db down")),
    )

    planner._refresh_queue_positions_best_effort()


def test_group_table_include_list_is_whole_cdc_pack():
    tables = [
        {"source_schema": "tcbpay", "source_table": "allorders"},
        {"source_schema": "TCBPAY", "source_table": "MERCHANTS#ORDERS"},
        {"source_schema": "TCBPAY", "source_table": "ALLORDERS"},
    ]

    assert (
        planner._group_table_include_list(tables)
        == "TCBPAY.ALLORDERS,TCBPAY.MERCHANTS#ORDERS"
    )


def test_group_message_key_columns_uses_non_pk_tables():
    tables = [
        {
            "source_schema": "TCBPAY",
            "source_table": "ALLORDERS",
            "source_pk_exists": False,
            "source_uk_exists": False,
            "effective_key_columns_json": '["ID", "MERCHANT_ID"]',
        },
        {
            "source_schema": "TCBPAY",
            "source_table": "MERCHANTS",
            "source_pk_exists": False,
            "source_uk_exists": True,
            "effective_key_columns_json": '["MERCHANT_ID"]',
        },
        {
            "source_schema": "TCBPAY",
            "source_table": "PAYMENTS",
            "source_pk_exists": True,
            "source_uk_exists": False,
            "effective_key_columns_json": '["ID"]',
        },
    ]

    assert (
        planner._group_message_key_columns(tables)
        == "TCBPAY.ALLORDERS:ID,MERCHANT_ID;TCBPAY.MERCHANTS:MERCHANT_ID"
    )


def test_cdc_group_snapshot_uses_active_run_topic_names(monkeypatch):
    class CursorStub:
        def __init__(self):
            self.query = ""

        def execute(self, sql, *_args):
            self.query = sql

        def fetchone(self):
            if "FROM   connector_groups" in self.query:
                return {
                    "group_id": "gid",
                    "group_name": "CDC",
                    "status": "RUNNING",
                    "connector_name": "base_connector",
                    "topic_prefix": "base.topic",
                    "consumer_group_prefix": "base.consumer",
                    "run_id": "r123ab",
                    "error_text": None,
                }
            return None

        def fetchall(self):
            if "FROM   group_tables" in self.query:
                return [{
                    "id": "row-1",
                    "source_schema": "TCBPAY",
                    "source_table": "ALLORDERS",
                    "target_schema": "TCBPAY",
                    "target_table": "ALLORDERS",
                    "effective_key_type": "PRIMARY_KEY",
                    "effective_key_columns_json": '["ID"]',
                    "source_pk_exists": True,
                    "source_uk_exists": False,
                    "topic_name": "stale.topic.TCBPAY.ALLORDERS",
                    "created_at": None,
                }]
            return []

    monkeypatch.setitem(planner._state, "row_to_dict", lambda _cur, row: dict(row))

    snapshot = planner._load_cdc_group_snapshot(CursorStub(), "gid")

    assert snapshot["active_connector_name"] == "base_connector_r123ab"
    assert snapshot["active_topic_prefix"] == "base.topic.r123ab"
    assert snapshot["tables"][0]["topic_name"] == "base.topic.r123ab.TCBPAY.ALLORDERS"


def test_get_plan_includes_cdc_worker_state(monkeypatch):
    executed: list[str] = []

    class CursorStub:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, *_args):
            executed.append(sql)

        def fetchone(self):
            return {
                "plan_id": 42,
                "name": "TCBPAY",
                "connector_group_id": "gid-1",
                "status": "RUNNING",
            }

        def fetchall(self):
            return [{
                "item_id": 1,
                "plan_id": 42,
                "table_name": "ALLORDERS",
                "mode": "CDC",
                "batch_order": 1,
                "sort_order": 0,
                "migration_id": "mid-1",
                "status": "RUNNING",
                "phase": "CDC_APPLYING",
                "cdc_total_lag": 7,
                "cdc_rows_applied": 123,
                "cdc_worker_heartbeat": "2026-06-25T00:00:00Z",
            }]

    class ConnStub:
        def __init__(self):
            self.cursor_stub = CursorStub()

        def cursor(self):
            return self.cursor_stub

        def close(self):
            pass

    app = Flask(__name__)
    app.register_blueprint(planner.bp)

    monkeypatch.setitem(planner._state, "get_conn", lambda: ConnStub())
    monkeypatch.setitem(planner._state, "row_to_dict", lambda _cur, row: dict(row))
    monkeypatch.setattr(planner, "_load_cdc_group_snapshot", lambda _cur, group_id: {"group_id": group_id})

    response = app.test_client().get("/api/planner/plans/42")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["items"][0]["cdc_total_lag"] == 7
    assert payload["items"][0]["cdc_rows_applied"] == 123
    assert payload["items"][0]["cdc_worker_heartbeat"] == "2026-06-25T00:00:00Z"
    assert any("LEFT JOIN migration_cdc_state cs" in sql for sql in executed)
