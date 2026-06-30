from __future__ import annotations

import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace


WORKERS_DIR = Path(__file__).resolve().parents[2] / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import worker  # noqa: E402


class CursorStub:
    def __init__(self, calls):
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params or []))


class ConnStub:
    def __init__(self):
        self.calls = []

    def cursor(self):
        return CursorStub(self.calls)

    def close(self):
        self.calls.append(("close", []))


def test_parse_debezium_unwrapped_upsert_strips_meta_and_coerces_temporal_values():
    event = worker._parse_debezium(json.dumps({
        "schema": {
            "type": "struct",
            "fields": [
                {"field": "ID", "type": "int64"},
                {"field": "AMOUNT", "type": "float"},
                {
                    "field": "CREATED_AT",
                    "type": "int64",
                    "name": "io.debezium.time.Timestamp",
                },
                {
                    "field": "BUSINESS_DATE",
                    "type": "int32",
                    "name": "io.debezium.time.Date",
                },
                {"field": "__op", "type": "string"},
                {"field": "__table", "type": "string"},
                {"field": "__deleted", "type": "string"},
            ],
        },
        "payload": {
            "ID": 7,
            "AMOUNT": 100.5,
            "CREATED_AT": 1000,
            "BUSINESS_DATE": 1,
            "__op": "u",
            "__table": "ALLORDERS",
            "__deleted": "false",
        },
    }).encode("utf-8"))

    assert event == {
        "op": "u",
        "before": None,
        "after": {
            "ID": 7,
            "AMOUNT": 100.5,
            "CREATED_AT": datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
            "BUSINESS_DATE": date(1970, 1, 2),
        },
    }


def test_parse_debezium_unwrapped_delete_uses_before_record_and_skips_tombstone():
    event = worker._parse_debezium(json.dumps({
        "schema": {
            "type": "struct",
            "fields": [
                {"field": "ID", "type": "int64"},
                {"field": "AMOUNT", "type": "float"},
                {"field": "__op", "type": "string"},
                {"field": "__deleted", "type": "string"},
            ],
        },
        "payload": {
            "ID": 7,
            "AMOUNT": 100.5,
            "__op": "d",
            "__deleted": "true",
        },
    }).encode("utf-8"))

    assert event == {
        "op": "d",
        "before": {"ID": 7, "AMOUNT": 100.5},
        "after": None,
    }
    assert worker._parse_debezium(None) is None


def test_cdc_apply_upsert_requires_key_columns_before_sql():
    conn = ConnStub()

    try:
        worker._apply_event(
            conn,
            {"op": "u", "after": {"AMOUNT": 100}},
            "TCBPAY",
            "ALLORDERS",
            ["ID"],
        )
    except ValueError as exc:
        assert "missing key columns: ID" in str(exc)
    else:
        raise AssertionError("expected missing CDC key error")

    assert conn.calls == []


def test_cdc_apply_delete_requires_key_columns_before_sql():
    conn = ConnStub()

    try:
        worker._apply_event(
            conn,
            {"op": "d", "before": {"AMOUNT": 100}},
            "TCBPAY",
            "ALLORDERS",
            ["ID"],
        )
    except ValueError as exc:
        assert "missing key columns: ID" in str(exc)
    else:
        raise AssertionError("expected missing CDC key error")

    assert conn.calls == []


def test_cdc_apply_upsert_uses_key_columns_when_present():
    conn = ConnStub()

    worker._apply_event(
        conn,
        {"op": "u", "after": {"ID": 7, "AMOUNT": 100}},
        "TCBPAY",
        "ALLORDERS",
        ["ID"],
    )

    assert len(conn.calls) == 1
    sql, params = conn.calls[0]
    assert 'MERGE INTO "TCBPAY"."ALLORDERS"' in sql
    assert 't."ID" = s."ID"' in sql
    assert params == [7, 100]


def _migration():
    return {
        "migration_id": "mid-1",
        "target_schema": "TCBPAY",
        "target_table": "ALLORDERS",
        "source_schema": "TCBPAY",
        "source_table": "ALLORDERS",
        "topic_prefix": "cdc.run1",
        "consumer_group": "cdc_group",
        "effective_key_columns_json": '["ID"]',
        "target_connection_id": "oracle_target",
    }


def test_cdc_thread_marks_failed_when_kafka_consumer_start_fails(monkeypatch):
    calls = []

    class PgConn:
        def close(self):
            calls.append(("pg-close",))

    class FailingKafkaConsumer:
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("kafka unavailable")

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", FailingKafkaConsumer)
    monkeypatch.setattr(worker.db, "get_pg_conn", lambda: calls.append(("pg",)) or PgConn())
    monkeypatch.setattr(worker.db, "load_configs", lambda _pg: {"kafka": {"bootstrap_servers": "broker:9092"}})
    monkeypatch.setattr(
        worker.db,
        "fail_cdc_migration",
        lambda pg, mid, code, detail: calls.append(("fail", mid, code, detail)),
    )

    worker.cdc_thread(_migration(), stop_event=type("Stop", (), {"is_set": lambda self: True})())

    assert ("fail", "mid-1", "CDC_WORKER_START_FAILED", "RuntimeError: kafka unavailable") in calls
    assert ("pg-close",) in calls


def test_cdc_thread_marks_failed_when_target_oracle_start_fails(monkeypatch):
    calls = []

    class PgConn:
        def close(self):
            calls.append(("pg-close",))

    class KafkaConsumer:
        def __init__(self, *_args, **_kwargs):
            pass

        def close(self):
            calls.append(("consumer-close",))

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", KafkaConsumer)
    monkeypatch.setattr(worker.db, "get_pg_conn", lambda: calls.append(("pg",)) or PgConn())
    monkeypatch.setattr(worker.db, "load_configs", lambda _pg: {"kafka": {"bootstrap_servers": "broker:9092"}})
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_args: (_ for _ in ()).throw(RuntimeError("oracle down")))
    monkeypatch.setattr(
        worker.db,
        "fail_cdc_migration",
        lambda pg, mid, code, detail: calls.append(("fail", mid, code, detail)),
    )

    worker.cdc_thread(_migration(), stop_event=type("Stop", (), {"is_set": lambda self: True})())

    assert ("fail", "mid-1", "CDC_WORKER_START_FAILED", "RuntimeError: oracle down") in calls
    assert ("consumer-close",) in calls
    assert ("pg-close",) in calls


def test_cdc_thread_uses_heartbeat_only_until_lag_partitions_exist(monkeypatch):
    calls = []

    class PgConn:
        def close(self):
            calls.append(("pg-close",))

    class OracleConn:
        def close(self):
            calls.append(("oracle-close",))

    class KafkaConsumer:
        def __init__(self, *_args, **_kwargs):
            pass

        def poll(self, timeout_ms=None):
            calls.append(("poll", timeout_ms))
            return {}

        def close(self):
            calls.append(("consumer-close",))

    class StopEvent:
        def __init__(self):
            self.calls = 0

        def is_set(self):
            self.calls += 1
            return self.calls > 1

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", KafkaConsumer)
    monkeypatch.setattr(worker, "CDC_CHECKIN_SEC", 0)
    monkeypatch.setattr(worker.db, "get_pg_conn", lambda: PgConn())
    monkeypatch.setattr(worker.db, "load_configs", lambda _pg: {"kafka": {"bootstrap_servers": "broker:9092"}})
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_args: OracleConn())
    monkeypatch.setattr(worker, "_calc_lag", lambda *_args: (0, {}))
    monkeypatch.setattr(worker.db, "cdc_checkin", lambda *_args, **_kwargs: calls.append(("checkin",)))
    monkeypatch.setattr(worker.db, "cdc_heartbeat", lambda _pg, mid: calls.append(("heartbeat", mid)))
    monkeypatch.setattr(time, "sleep", lambda *_args: None)

    worker.cdc_thread(_migration(), stop_event=StopEvent())

    assert ("heartbeat", "mid-1") in calls
    assert ("checkin",) not in calls
    assert ("consumer-close",) in calls
    assert ("oracle-close",) in calls


def test_cdc_thread_triggers_caught_up_when_measured_lag_is_zero(monkeypatch):
    calls = []

    class PgConn:
        def close(self):
            calls.append(("pg-close",))

    class OracleConn:
        def close(self):
            calls.append(("oracle-close",))

    class KafkaConsumer:
        def __init__(self, *_args, **_kwargs):
            pass

        def poll(self, timeout_ms=None):
            calls.append(("poll", timeout_ms))
            return {}

        def close(self):
            calls.append(("consumer-close",))

    class StopEvent:
        def __init__(self):
            self.calls = 0

        def is_set(self):
            self.calls += 1
            return self.calls > 1

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", KafkaConsumer)
    monkeypatch.setattr(worker, "CDC_CHECKIN_SEC", 0)
    monkeypatch.setattr(worker.db, "get_pg_conn", lambda: PgConn())
    monkeypatch.setattr(worker.db, "load_configs", lambda _pg: {"kafka": {"bootstrap_servers": "broker:9092"}})
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_args: OracleConn())
    monkeypatch.setattr(worker, "_calc_lag", lambda *_args: (0, {"topic-0": 0}))
    monkeypatch.setattr(
        worker.db,
        "cdc_checkin",
        lambda _pg, mid, lag, rows, **kwargs: calls.append(("checkin", mid, lag, rows, kwargs)),
    )
    monkeypatch.setattr(worker.db, "trigger_lag_zero", lambda _pg, mid: calls.append(("lag-zero", mid)))
    monkeypatch.setattr(worker.db, "cdc_heartbeat", lambda _pg, mid: calls.append(("heartbeat", mid)))
    monkeypatch.setattr(time, "sleep", lambda *_args: None)

    worker.cdc_thread(_migration(), stop_event=StopEvent())

    assert (
        "checkin",
        "mid-1",
        0,
        0,
        {"lag_by_partition": {"topic-0": 0}},
    ) in calls
    assert ("lag-zero", "mid-1") in calls
    assert ("heartbeat", "mid-1") not in calls
    assert ("consumer-close",) in calls
    assert ("oracle-close",) in calls


def test_cdc_thread_marks_failed_after_repeated_poll_errors(monkeypatch):
    calls = []

    class PgConn:
        def close(self):
            calls.append(("pg-close",))

    class OracleConn:
        def close(self):
            calls.append(("oracle-close",))

    class KafkaConsumer:
        def __init__(self, *_args, **_kwargs):
            pass

        def poll(self, timeout_ms=None):
            calls.append(("poll", timeout_ms))
            raise RuntimeError("broker connection lost")

        def close(self):
            calls.append(("consumer-close",))

    class StopEvent:
        def is_set(self):
            return False

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", KafkaConsumer)
    monkeypatch.setattr(worker, "CDC_POLL_ERROR_THRESHOLD", 2)
    monkeypatch.setattr(worker.db, "get_pg_conn", lambda: PgConn())
    monkeypatch.setattr(worker.db, "load_configs", lambda _pg: {"kafka": {"bootstrap_servers": "broker:9092"}})
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_args: OracleConn())
    monkeypatch.setattr(worker.db, "cdc_heartbeat", lambda _pg, mid: calls.append(("heartbeat", mid)))
    monkeypatch.setattr(
        worker.db,
        "fail_cdc_migration",
        lambda pg, mid, code, detail: calls.append(("fail", mid, code, detail)),
    )
    monkeypatch.setattr(time, "sleep", lambda *_args: None)

    worker.cdc_thread(_migration(), stop_event=StopEvent())

    assert calls.count(("poll", worker.CDC_POLL_MS)) == 2
    assert calls.count(("heartbeat", "mid-1")) == 2
    assert (
        "fail",
        "mid-1",
        "CDC_POLL_FAILED",
        "RuntimeError: broker connection lost",
    ) in calls
    assert ("consumer-close",) in calls
    assert ("oracle-close",) in calls
    assert ("pg-close",) in calls


def test_cdc_thread_marks_failed_on_runtime_fatal_error(monkeypatch):
    calls = []

    class PgConn:
        def close(self):
            calls.append(("pg-close",))

    class OracleConn:
        def commit(self):
            raise RuntimeError("target commit down")

        def close(self):
            calls.append(("oracle-close",))

    class KafkaConsumer:
        def __init__(self, *_args, **_kwargs):
            pass

        def poll(self, timeout_ms=None):
            calls.append(("poll", timeout_ms))
            return {
                "tp0": [
                    SimpleNamespace(
                        value=b"{}",
                        topic="cdc.run1.TCBPAY.ALLORDERS",
                        partition=0,
                        offset=12,
                    )
                ]
            }

        def commit(self):
            calls.append(("consumer-commit",))

        def close(self):
            calls.append(("consumer-close",))

    class StopEvent:
        def is_set(self):
            return False

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", KafkaConsumer)
    monkeypatch.setattr(worker.db, "get_pg_conn", lambda: PgConn())
    monkeypatch.setattr(worker.db, "load_configs", lambda _pg: {"kafka": {"bootstrap_servers": "broker:9092"}})
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_args: OracleConn())
    monkeypatch.setattr(worker, "_parse_debezium", lambda _value: {"op": "u", "after": {"ID": 7}})
    monkeypatch.setattr(worker, "_apply_event", lambda *_args: calls.append(("apply",)))
    monkeypatch.setattr(
        worker.db,
        "fail_cdc_migration",
        lambda pg, mid, code, detail: calls.append(("fail", mid, code, detail)),
    )

    worker.cdc_thread(_migration(), stop_event=StopEvent())

    assert ("apply",) in calls
    assert ("consumer-commit",) not in calls
    assert (
        "fail",
        "mid-1",
        "CDC_WORKER_FATAL",
        "RuntimeError: target commit down",
    ) in calls
    assert ("consumer-close",) in calls
    assert ("oracle-close",) in calls
    assert ("pg-close",) in calls
