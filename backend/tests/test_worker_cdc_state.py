from __future__ import annotations

import sys
import types
from pathlib import Path


WORKERS_DIR = Path(__file__).resolve().parents[2] / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import common as worker_common  # noqa: E402


class CursorStub:
    def __init__(self, row=None):
        self.row = row
        self.executed: list[tuple[str, tuple]] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params or ()))
        self.rowcount = 0

    def fetchone(self):
        return self.row


class ConnStub:
    def __init__(self, row=None):
        self.cur = CursorStub(row)
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class RowcountCursorStub(CursorStub):
    def __init__(self, rowcounts):
        super().__init__()
        self.rowcounts = list(rowcounts)

    def execute(self, sql, params=None):
        self.executed.append((sql, params or ()))
        self.rowcount = self.rowcounts.pop(0) if self.rowcounts else 0


class RowcountConnStub(ConnStub):
    def __init__(self, rowcounts):
        self.cur = RowcountCursorStub(rowcounts)
        self.committed = False
        self.rolled_back = False


class SequenceCursorStub(CursorStub):
    def __init__(self, rows):
        super().__init__()
        self.rows = list(rows)

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


class SequenceConnStub(ConnStub):
    def __init__(self, rows):
        self.cur = SequenceCursorStub(rows)
        self.committed = False
        self.rolled_back = False


def test_cdc_topic_name_sanitizes_hash_table_names():
    assert (
        worker_common.cdc_topic_name("sm.tcbpay.pay.r123ab", "tcbpay", "merchants#orders")
        == "sm.tcbpay.pay.r123ab.TCBPAY.MERCHANTS_ORDERS"
    )


def test_open_oracle_applies_session_context(monkeypatch):
    class OracleConnStub:
        module = ""
        action = ""
        client_identifier = ""

    conn = OracleConnStub()
    fake_oracledb = types.SimpleNamespace(
        DB_TYPE_TIMESTAMP=object(),
        connect=lambda **_kwargs: conn,
    )
    monkeypatch.setitem(sys.modules, "oracledb", fake_oracledb)

    result = worker_common.open_oracle(
        "oracle_target",
        {
            "oracle_target": {
                "host": "db-host",
                "port": 1521,
                "service_name": "svc",
                "user": "usr",
                "password": "pwd",
            }
        },
        {
            "module": "new_cdc.worker",
            "action": "bulk chunk-1",
            "client_identifier": "worker-1 migration=mid-1 chunk=chunk-1",
        },
    )

    assert result is conn
    assert conn.module == "new_cdc.worker"
    assert conn.action == "bulk chunk-1"
    assert conn.client_identifier == "worker-1 migration=mid-1 chunk=chunk-1"


def test_chunk_is_active_false_when_migration_is_cancelling():
    conn = ConnStub(row=("RUNNING", "CANCELLING"))

    assert worker_common.chunk_is_active(conn, "chunk-1") is False
    sql, params = conn.cur.executed[0]
    assert "JOIN   migrations m ON m.migration_id = c.migration_id" in sql
    assert params == ("chunk-1",)


def test_cancel_chunk_marks_active_chunk_cancelled():
    conn = ConnStub()

    worker_common.cancel_chunk(conn, "chunk-1", "migration cancelled")

    sql, params = conn.cur.executed[0]
    assert "SET    status       = 'CANCELLED'" in sql
    assert params == ("migration cancelled", "chunk-1")
    assert conn.committed is True


def test_claim_cdc_migration_persists_exact_worker_topic(monkeypatch):
    monkeypatch.setattr(worker_common, "WORKER_ID", "worker-1")
    row = (
        "mid-1",
        "oracle_target",
        "TCBPAY",
        "MERCHANTS#ORDERS",
        "TCBPAY",
        "MERCHANTS#ORDERS",
        "sm.tcbpay.pay.r123ab",
        "sm.tcbpay.pay_TCBPAY_MERCHANTS#ORDERS",
        '["ID"]',
    )
    conn = ConnStub(row)

    migration = worker_common.claim_cdc_migration(conn)

    assert migration is not None
    assert conn.committed is True
    reserve_sql, reserve_params = conn.cur.executed[1]
    assert "INSERT INTO migration_cdc_state" in reserve_sql
    assert reserve_params == (
        "sm.tcbpay.pay.r123ab.TCBPAY.MERCHANTS_ORDERS",
        "worker-1",
        "mid-1",
    )
    assert "topic            = EXCLUDED.topic" in reserve_sql


def test_claim_cdc_migration_excludes_active_ids_without_heartbeat_refresh(monkeypatch):
    conn = ConnStub(row=None)

    migration = worker_common.claim_cdc_migration(
        conn,
        exclude_migration_ids=["11111111-1111-1111-1111-111111111111"],
    )

    assert migration is None
    assert conn.rolled_back is True
    assert conn.committed is False
    assert len(conn.cur.executed) == 1
    select_sql, select_params = conn.cur.executed[0]
    assert "m.migration_id = ANY(%s::uuid[])" in select_sql
    assert select_params == (
        worker_common.CDC_HEARTBEAT_STALE_MINUTES,
        ["11111111-1111-1111-1111-111111111111"],
    )


def test_claim_chunk_skips_paused_migrations(monkeypatch):
    conn = ConnStub(row=None)

    chunk = worker_common.claim_chunk(conn)

    assert chunk is None
    select_sql, _params = conn.cur.executed[0]
    assert "COALESCE(m.paused, FALSE) = FALSE" in select_sql


def test_claim_cdc_migration_skips_paused_migrations(monkeypatch):
    conn = ConnStub(row=None)

    migration = worker_common.claim_cdc_migration(conn)

    assert migration is None
    select_sql, _params = conn.cur.executed[0]
    assert "COALESCE(m.paused, FALSE) = FALSE" in select_sql


def test_cdc_migration_should_run_false_when_paused():
    conn = ConnStub(row=("CDC_CATCHING_UP", True))

    assert worker_common.cdc_migration_should_run(conn, "mid-1") is False

    sql, params = conn.cur.executed[0]
    assert "SELECT phase, paused" in sql
    assert params == ("mid-1",)


def test_cdc_migration_should_run_true_when_active_and_unpaused():
    conn = ConnStub(row=("STEADY_STATE", False))

    assert worker_common.cdc_migration_should_run(conn, "mid-1") is True


def test_cdc_checkin_recomputes_topic_from_migration_columns(monkeypatch):
    monkeypatch.setattr(worker_common, "WORKER_ID", "worker-1")
    conn = ConnStub()

    worker_common.cdc_checkin(conn, "mid-1", total_lag=7, rows_applied=3)

    sql, params = conn.cur.executed[0]
    assert "INSERT INTO migration_cdc_state" in sql
    assert "UPPER(source_schema) || '.' || UPPER(source_table)" in sql
    assert "topic            = EXCLUDED.topic" in sql
    assert params == (7, "{}", 3, "worker-1", "mid-1")
    assert conn.committed is True


def test_cdc_heartbeat_does_not_write_lag(monkeypatch):
    monkeypatch.setattr(worker_common, "WORKER_ID", "worker-1")
    conn = ConnStub()

    worker_common.cdc_heartbeat(conn, "mid-1")

    sql, params = conn.cur.executed[0]
    assert "UPDATE migration_cdc_state" in sql
    assert "total_lag" not in sql
    assert "lag_by_partition" not in sql
    assert params == ("worker-1", "mid-1")
    assert conn.committed is True


def test_worker_heartbeat_upserts_process_liveness(monkeypatch):
    monkeypatch.setattr(worker_common, "WORKER_ID", "worker-1")
    conn = ConnStub()

    worker_common.worker_heartbeat(conn, role="universal", capabilities=["bulk", "cdc"])

    sql, params = conn.cur.executed[0]
    assert "INSERT INTO worker_heartbeats" in sql
    assert "ON CONFLICT (worker_id) DO UPDATE" in sql
    assert params == ("worker-1", "universal", '["bulk", "cdc"]')
    assert conn.committed is True


def test_claim_ddl_apply_job_returns_job_type(monkeypatch):
    monkeypatch.setattr(worker_common, "WORKER_ID", "worker-1")
    conn = SequenceConnStub([
        ("job-1", "sm-1", "sync_diff", "SYNC_INDEX", "INDEX", "IX_ORDERS_ID"),
        ("SRC", "TGT"),
    ])

    job = worker_common.claim_ddl_apply_job(conn)

    assert job == {
        "job_id": "job-1",
        "schema_migration_id": "sm-1",
        "action": "sync_diff",
        "job_type": "SYNC_INDEX",
        "object_type": "INDEX",
        "object_name": "IX_ORDERS_ID",
        "src_schema": "SRC",
        "tgt_schema": "TGT",
    }
    claim_sql, claim_params = conn.cur.executed[0]
    assert "action, job_type" in claim_sql
    assert claim_params == ("worker-1",)
    assert conn.committed is True


def test_fail_cdc_migration_marks_plan_item_and_plan_failed():
    conn = RowcountConnStub([1, 1, 1])

    worker_common.fail_cdc_migration(
        conn,
        "mid-1",
        "CDC_APPLY_FAILED",
        "poison event",
    )

    sqls = [sql for sql, _params in conn.cur.executed]
    assert "UPDATE migrations" in sqls[0]
    assert "UPDATE migration_plan_items" in sqls[1]
    assert "UPDATE migration_plans" in sqls[1]
    assert "UPDATE migration_cdc_state" in sqls[2]
    assert conn.cur.executed[1][1] == ("mid-1",)
    assert conn.committed is True


def test_fail_cdc_migration_does_not_touch_plan_when_phase_not_changed():
    conn = RowcountConnStub([0, 1])

    worker_common.fail_cdc_migration(
        conn,
        "mid-1",
        "CDC_APPLY_FAILED",
        "already terminal",
    )

    sqls = [sql for sql, _params in conn.cur.executed]
    assert len(sqls) == 2
    assert "UPDATE migrations" in sqls[0]
    assert "UPDATE migration_cdc_state" in sqls[1]
    assert all("migration_plan_items" not in sql for sql in sqls)
    assert conn.committed is True
