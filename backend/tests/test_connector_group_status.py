from __future__ import annotations

import pytest

from services import connector_groups


def test_status_poll_preserves_start_lifecycle_when_connector_not_created():
    assert connector_groups._normalize_polled_connector_status(
        "TOPICS_CREATING",
        "NOT_FOUND",
    ) == ("TOPICS_CREATING", None)

    assert connector_groups._normalize_polled_connector_status(
        "CONNECTOR_STARTING",
        "NOT_FOUND",
    ) == ("CONNECTOR_STARTING", None)


def test_status_poll_preserves_stopping_lifecycle():
    assert connector_groups._normalize_polled_connector_status(
        "STOPPING",
        "RUNNING",
    ) == ("STOPPING", None)


def test_status_poll_syncs_start_lifecycle_when_connector_is_running():
    assert connector_groups._normalize_polled_connector_status(
        "CONNECTOR_STARTING",
        "RUNNING",
    ) == ("RUNNING", "RUNNING")


def test_status_poll_marks_missing_stable_connector_stopped():
    assert connector_groups._normalize_polled_connector_status(
        "RUNNING",
        "NOT_FOUND",
    ) == ("STOPPED", "STOPPED")


def test_status_poll_writes_real_status_for_stable_group():
    assert connector_groups._normalize_polled_connector_status(
        "RUNNING",
        "FAILED",
    ) == ("FAILED", "FAILED")


def test_refresh_connector_tables_marks_missing_running_connector_stopped(monkeypatch):
    calls = []

    monkeypatch.setattr(
        connector_groups,
        "get_group",
        lambda group_id: {
            "group_id": group_id,
            "status": "RUNNING",
            "connector_name": "cdc-main",
            "topic_prefix": "cdc.topic",
            "run_id": None,
        },
    )
    monkeypatch.setattr(
        connector_groups.debezium,
        "get_connector_status",
        lambda connector_name: "NOT_FOUND",
    )
    monkeypatch.setattr(
        connector_groups,
        "update_group_status",
        lambda group_id, status, error_text=None: calls.append((group_id, status, error_text)),
    )

    try:
        connector_groups.refresh_connector_tables("gid-1")
    except ValueError as exc:
        assert "marked RUNNING but is missing" in str(exc)
    else:
        raise AssertionError("expected missing Kafka Connect connector error")

    assert calls == [
        ("gid-1", "STOPPED", "CDC runtime cdc-main is missing in Kafka Connect"),
    ]


class CursorStub:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.row


def test_active_migration_for_group_table_finds_non_terminal_phase():
    cur = CursorStub(("mid-1", "CDC_APPLYING"))

    assert connector_groups._active_migration_for_group_table(
        cur, "gid", "tcbpay", "allorders",
    ) == ("mid-1", "CDC_APPLYING")
    assert "COALESCE(phase, '') NOT IN" in cur.executed[0][0]


@pytest.mark.parametrize("phase", ["DRAFT", "NEW", "CDC_APPLYING", "STEADY_STATE"])
def test_active_migration_for_group_table_treats_cdc_membership_as_active(phase):
    cur = CursorStub(("mid-1", phase))

    assert connector_groups._active_migration_for_group_table(
        cur, "gid", "TCBPAY", "ALLORDERS",
    ) == ("mid-1", phase)


def test_active_migration_for_group_table_allows_terminal_phase_absent():
    cur = CursorStub(None)

    assert connector_groups._active_migration_for_group_table(
        cur, "gid", "TCBPAY", "ALLORDERS",
    ) is None


def test_prune_tables_removes_only_tables_not_in_keep_list(monkeypatch):
    calls = []

    class Cur:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchall(self):
            return [
                {"source_schema": "TCBPAY", "source_table": "ALLORDERS"},
                {"source_schema": "TCBPAY", "source_table": "OLDORDERS"},
            ]

        def fetchone(self):
            return None

    class Conn:
        def cursor(self):
            return Cur()

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(connector_groups, "_conn", lambda: Conn())
    monkeypatch.setattr(connector_groups, "_r2d", lambda _cur, row: row)

    removed = connector_groups.prune_tables(
        "gid-1",
        [{"source_schema": "tcbpay", "source_table": "allorders"}],
    )

    assert removed == [{"source_schema": "TCBPAY", "source_table": "OLDORDERS"}]
    executed_sql = [call[1] for call in calls if call[0] == "execute"]
    assert any("DELETE FROM group_tables" in sql for sql in executed_sql)
    assert ("commit",) in calls
    assert ("rollback",) not in calls
    assert ("close",) in calls


def test_prune_tables_rolls_back_when_removed_table_has_active_migration(monkeypatch):
    calls = []

    class Cur:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchall(self):
            return [{"source_schema": "TCBPAY", "source_table": "OLDORDERS"}]

        def fetchone(self):
            return ("mid-1", "CDC_APPLYING")

    class Conn:
        def cursor(self):
            return Cur()

        def commit(self):
            calls.append(("commit",))

        def rollback(self):
            calls.append(("rollback",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(connector_groups, "_conn", lambda: Conn())
    monkeypatch.setattr(connector_groups, "_r2d", lambda _cur, row: row)

    with pytest.raises(ValueError) as exc:
        connector_groups.prune_tables(
            "gid-1",
            [{"source_schema": "TCBPAY", "source_table": "ALLORDERS"}],
        )

    assert "active migration mid-1 is in phase CDC_APPLYING" in str(exc.value)
    executed_sql = [call[1] for call in calls if call[0] == "execute"]
    assert not any("DELETE FROM group_tables" in sql for sql in executed_sql)
    assert ("commit",) not in calls
    assert ("rollback",) in calls
    assert ("close",) in calls


def test_delete_group_treats_draft_cdc_migration_as_active(monkeypatch):
    calls = []

    class Cur:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append(("execute", " ".join(sql.split()), params))

        def fetchall(self):
            return [("mid-draft", "DRAFT")]

    class Conn:
        def cursor(self):
            return Cur()

        def commit(self):
            calls.append(("commit",))

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(
        connector_groups,
        "get_group",
        lambda group_id: {
            "group_id": group_id,
            "connector_name": "cdc-main",
            "topic_prefix": "cdc.main",
        },
    )
    monkeypatch.setattr(connector_groups, "_conn", lambda: Conn())

    try:
        connector_groups.delete_group("gid-1", force=False)
    except ValueError as exc:
        assert "активных миграций" in str(exc)
    else:
        raise AssertionError("expected active migration rejection")

    sql = calls[0][1]
    assert "COALESCE(phase, '') NOT IN ('COMPLETED', 'CANCELLED', 'FAILED')" in sql
    assert ("commit",) not in calls
    assert ("close",) in calls


def test_topic_count_consumer_uses_supported_timeout_configs(monkeypatch):
    captured = {}

    class FakeKafkaConsumer:
        def __init__(self, **configs):
            captured.update(configs)

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", FakeKafkaConsumer)

    assert isinstance(
        connector_groups._new_topic_count_consumer(["broker:9092"]),
        FakeKafkaConsumer,
    )
    assert captured == {
        "bootstrap_servers": ["broker:9092"],
        "request_timeout_ms": 5000,
        "connections_max_idle_ms": 8000,
        "enable_auto_commit": False,
    }


def test_debezium_sync_status_reports_extra_actual_tables(monkeypatch):
    monkeypatch.setattr(
        connector_groups,
        "get_group",
        lambda group_id: {
            "group_id": group_id,
            "connector_name": "cdc-main",
            "topic_prefix": "cdc.main",
            "run_id": None,
        },
    )
    monkeypatch.setattr(
        connector_groups,
        "_build_table_include_list",
        lambda group_id: "TCBPAY.ALLORDERS",
    )
    monkeypatch.setattr(
        connector_groups,
        "_build_key_columns",
        lambda group_id: "",
    )
    monkeypatch.setattr(
        connector_groups.debezium,
        "get_connector_config",
        lambda connector_name: {
            "table.include.list": "TCBPAY.ALLORDERS,TCBPAY.MERCHANTS#ORDERS",
        },
    )

    result = connector_groups.get_debezium_sync_status("gid-1")

    assert result["connector_name"] == "cdc-main"
    assert result["exists"] is True
    assert result["in_sync"] is False
    assert result["missing_tables"] == []
    assert result["extra_tables"] == ["TCBPAY.MERCHANTS#ORDERS"]


def test_debezium_sync_status_reports_missing_connector(monkeypatch):
    monkeypatch.setattr(
        connector_groups,
        "get_group",
        lambda group_id: {
            "group_id": group_id,
            "connector_name": "cdc-main",
            "topic_prefix": "cdc.main",
            "run_id": "run1",
        },
    )
    monkeypatch.setattr(
        connector_groups,
        "_build_table_include_list",
        lambda group_id: "TCBPAY.ALLORDERS",
    )
    monkeypatch.setattr(
        connector_groups,
        "_build_key_columns",
        lambda group_id: "TCBPAY.ALLORDERS:ID",
    )
    monkeypatch.setattr(
        connector_groups.debezium,
        "get_connector_config",
        lambda connector_name: None,
    )

    result = connector_groups.get_debezium_sync_status("gid-1")

    assert result["connector_name"] == "cdc-main_run1"
    assert result["exists"] is False
    assert result["in_sync"] is False
    assert result["missing_tables"] == ["TCBPAY.ALLORDERS"]
    assert result["actual_table_include_list"] is None
    assert result["key_columns_match"] is False
