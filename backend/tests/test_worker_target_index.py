from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
WORKERS_DIR = ROOT / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import worker  # noqa: E402


class CursorStub:
    def __init__(self, conn):
        self.conn = conn
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.conn.executed.append((sql, params))

    def fetchone(self):
        if "FROM   all_tables" in self.sql:
            return ("N",)
        return None

    def fetchall(self):
        if "FROM   all_indexes" in self.sql:
            return []
        if "owner = :s" in self.sql and "table_name = :t" in self.sql and "status = 'DISABLED'" in self.sql:
            return []
        if "FROM   all_constraints fk" in self.sql:
            return [("TGT", "CHILD", "FK_CHILD_PARENT")]
        return []


class ConnStub:
    def __init__(self):
        self.executed = []
        self.committed = False

    def cursor(self):
        return CursorStub(self)

    def commit(self):
        self.committed = True


def test_enable_target_indexes_enables_referencing_fk_novalidate():
    conn = ConnStub()

    result = worker._enable_target_indexes(conn, "TGT", "PARENT")

    assert 'ALTER TABLE "TGT"."CHILD" ENABLE NOVALIDATE CONSTRAINT "FK_CHILD_PARENT"' in [
        sql for sql, _params in conn.executed
    ]
    assert result["enabled"]["referencing_fk_novalidate"] == ["TGT.CHILD.FK_CHILD_PARENT"]
    assert result["errors"] == {"indexes": [], "constraints": []}
    assert result["deferred_fk"] == []
    assert conn.committed


# ---------------------------------------------------------------------------
# Deferred-FK behaviour: FK enable failures must NOT fail the migration; index
# and PK/UK/CHECK failures must stay fatal.
# ---------------------------------------------------------------------------

class _FlexCursor:
    """Cursor stub driven by table attributes on its connection."""

    def __init__(self, conn):
        self.conn = conn
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.conn.executed.append((sql, params))
        if "ENABLE" in sql and sql.strip().startswith("ALTER TABLE"):
            for frag in self.conn.fail_on:
                if frag in sql:
                    raise Exception("ORA-02298: cannot validate - parent keys not found")

    def fetchone(self):
        if "FROM   all_tables" in self.sql:
            return ("N",)
        return None

    def fetchall(self):
        if "FROM   all_indexes" in self.sql:
            return list(self.conn.unusable_indexes)
        if "constraint_type IN ('P','U','R','C')" in self.sql:
            return list(self.conn.own_constraints)
        if "FROM   all_constraints fk" in self.sql:
            return list(self.conn.referencing)
        return []


class _FlexConn:
    def __init__(self, *, own_constraints=(), referencing=(),
                 unusable_indexes=(), fail_on=()):
        self.own_constraints = own_constraints
        self.referencing = referencing
        self.unusable_indexes = unusable_indexes
        self.fail_on = fail_on
        self.executed = []
        self.committed = False

    def cursor(self):
        return _FlexCursor(self)

    def commit(self):
        self.committed = True

    def close(self):
        pass


def test_enable_target_indexes_defers_failed_own_fk():
    # Own outbound FK can't enable because the parent PK isn't enabled yet.
    conn = _FlexConn(
        own_constraints=[("FK_SELF_PARENT", "R"), ("PK_SELF", "P")],
        fail_on=['CONSTRAINT "FK_SELF_PARENT"'],
    )

    result = worker._enable_target_indexes(conn, "TGT", "CHILD")

    # FK failure is deferred, not fatal.
    assert [d["name"] for d in result["deferred_fk"]] == ["FK_SELF_PARENT"]
    assert result["errors"] == {"indexes": [], "constraints": []}
    # The PK still enabled successfully.
    assert "PK_SELF" in result["enabled"]["constraints"]


def test_enable_target_indexes_defers_failed_referencing_fk():
    conn = _FlexConn(
        referencing=[("TGT", "CHILD", "FK_CHILD_PARENT")],
        fail_on=['CONSTRAINT "FK_CHILD_PARENT"'],
    )

    result = worker._enable_target_indexes(conn, "TGT", "PARENT")

    assert [d["name"] for d in result["deferred_fk"]] == ["TGT.CHILD.FK_CHILD_PARENT"]
    assert result["errors"] == {"indexes": [], "constraints": []}


def test_enable_target_indexes_rebuilds_indexes_parallel(monkeypatch):
    monkeypatch.setattr(worker, "INDEX_REBUILD_PARALLEL", True)
    conn = _FlexConn(unusable_indexes=[("IDX1",)])

    result = worker._enable_target_indexes(conn, "TGT", "T")

    sqls = [sql for sql, _params in conn.executed]
    assert 'ALTER INDEX "TGT"."IDX1" REBUILD NOLOGGING PARALLEL' in sqls
    # Degree reset afterwards so future DML doesn't go parallel.
    assert 'ALTER INDEX "TGT"."IDX1" NOPARALLEL' in sqls
    assert result["enabled"]["indexes"] == ["IDX1"]


def test_enable_target_indexes_serial_rebuild_when_disabled(monkeypatch):
    monkeypatch.setattr(worker, "INDEX_REBUILD_PARALLEL", False)
    conn = _FlexConn(unusable_indexes=[("IDX1",)])

    worker._enable_target_indexes(conn, "TGT", "T")

    sqls = [sql for sql, _params in conn.executed]
    assert 'ALTER INDEX "TGT"."IDX1" REBUILD NOLOGGING' in sqls
    assert not any("NOPARALLEL" in s for s in sqls)


def test_enable_target_indexes_pk_failure_is_fatal():
    conn = _FlexConn(
        own_constraints=[("PK_SELF", "P")],
        fail_on=['CONSTRAINT "PK_SELF"'],
    )

    result = worker._enable_target_indexes(conn, "TGT", "CHILD")

    assert [e["name"] for e in result["errors"]["constraints"]] == ["PK_SELF"]
    assert result["deferred_fk"] == []


def test_enable_target_indexes_stops_on_rebuild_connection_loss():
    class DropCursor:
        def __init__(self, conn):
            self.conn = conn
            self.sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, sql, params=None):
            self.sql = sql
            self.conn.executed.append((sql, params))
            if sql.startswith('ALTER INDEX "TGT"."IDX1" REBUILD'):
                raise RuntimeError("DPY-4011: the database or network closed the connection")

        def fetchone(self):
            if "FROM   all_tables" in self.sql:
                return ("N",)
            return None

        def fetchall(self):
            if "FROM   all_indexes" in self.sql:
                return [("IDX1",)]
            if "constraint_type IN ('P','U','R','C')" in self.sql:
                self.conn.constraints_queried = True
            return []

    class DropConn:
        def __init__(self):
            self.executed = []
            self.constraints_queried = False

        def cursor(self):
            return DropCursor(self)

        def commit(self):
            raise AssertionError("commit must not run after connection loss")

    conn = DropConn()

    with pytest.raises(RuntimeError, match="DPY-4011"):
        worker._enable_target_indexes(conn, "TGT", "T")

    assert conn.constraints_queried is False


def test_process_target_index_job_retries_serial_after_parallel_connection_loss(monkeypatch, capsys):
    class RebuildCursor:
        def __init__(self, conn):
            self.conn = conn
            self.sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, sql, params=None):
            self.sql = sql
            self.conn.executed.append((sql, params))
            if self.conn.fail_parallel and sql == 'ALTER INDEX "TGT"."IDX1" REBUILD NOLOGGING PARALLEL':
                raise RuntimeError("DPY-4011: the database or network closed the connection")

        def fetchone(self):
            if "FROM   all_tables" in self.sql:
                return ("N",)
            return None

        def fetchall(self):
            if "FROM   all_indexes" in self.sql:
                return [("IDX1",)]
            if "constraint_type IN ('P','U','R','C')" in self.sql:
                return []
            if "FROM   all_constraints fk" in self.sql:
                return []
            return []

    class RebuildConn:
        def __init__(self, *, fail_parallel=False):
            self.fail_parallel = fail_parallel
            self.executed = []
            self.closed = False
            self.committed = False

        def cursor(self):
            return RebuildCursor(self)

        def commit(self):
            self.committed = True

        def close(self):
            self.closed = True

    first = RebuildConn(fail_parallel=True)
    second = RebuildConn()
    opened = [first, second]
    completed, failed = [], []
    monkeypatch.setattr(worker, "INDEX_REBUILD_PARALLEL", True)
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_a: opened.pop(0))
    monkeypatch.setattr(worker.db, "complete_target_index_job",
                        lambda _pg, job_id, result: completed.append((job_id, result)))
    monkeypatch.setattr(worker.db, "fail_target_index_job",
                        lambda _pg, job_id, err: failed.append((job_id, err)))

    job = {
        "job_id": "job-retry",
        "migration_id": "migration-1",
        "target_connection_id": "oracle_target",
        "target_schema": "TGT",
        "target_table": "T",
    }

    worker.process_target_index_job(job, object(), {})

    out = capsys.readouterr().out
    second_sql = [sql for sql, _params in second.executed]
    assert "DPY-4011 during parallel index enable; retrying once with serial rebuild" in out
    assert first.closed is True
    assert 'ALTER INDEX "TGT"."IDX1" REBUILD NOLOGGING' in second_sql
    assert 'ALTER INDEX "TGT"."IDX1" REBUILD NOLOGGING PARALLEL' not in second_sql
    assert completed and completed[0][0] == "job-retry"
    assert completed[0][1]["enabled"]["indexes"] == ["IDX1"]
    assert failed == []


def test_process_target_index_job_completes_when_only_fk_deferred(monkeypatch):
    conn = _FlexConn(
        own_constraints=[("FK_SELF_PARENT", "R")],
        fail_on=['CONSTRAINT "FK_SELF_PARENT"'],
    )
    completed, failed = [], []
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_a: conn)
    monkeypatch.setattr(worker.db, "complete_target_index_job",
                        lambda _pg, job_id, result: completed.append((job_id, result)))
    monkeypatch.setattr(worker.db, "fail_target_index_job",
                        lambda _pg, job_id, err: failed.append((job_id, err)))

    job = {
        "job_id": "job-1",
        "target_connection_id": "oracle_target",
        "target_schema": "TGT",
        "target_table": "CHILD",
    }
    worker.process_target_index_job(job, object(), {})

    assert failed == []
    assert len(completed) == 1
    assert [d["name"] for d in completed[0][1]["deferred_fk"]] == ["FK_SELF_PARENT"]


def test_process_target_index_job_fails_on_pk_error(monkeypatch):
    conn = _FlexConn(
        own_constraints=[("PK_SELF", "P")],
        fail_on=['CONSTRAINT "PK_SELF"'],
    )
    completed, failed = [], []
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_a: conn)
    monkeypatch.setattr(worker.db, "complete_target_index_job",
                        lambda _pg, job_id, result: completed.append((job_id, result)))
    monkeypatch.setattr(worker.db, "fail_target_index_job",
                        lambda _pg, job_id, err: failed.append((job_id, err)))

    job = {
        "job_id": "job-2",
        "target_connection_id": "oracle_target",
        "target_schema": "TGT",
        "target_table": "CHILD",
    }
    worker.process_target_index_job(job, object(), {})

    assert completed == []
    assert len(failed) == 1


def test_process_target_index_job_logs_dpy1001_diagnostics(monkeypatch, capsys):
    class BrokenConn:
        closed = False

        def cursor(self):
            raise RuntimeError("DPY-1001: not connected to database")

        def close(self):
            self.closed = True

    conn = BrokenConn()
    completed, failed = [], []
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_a: conn)
    monkeypatch.setattr(worker.db, "complete_target_index_job",
                        lambda _pg, job_id, result: completed.append((job_id, result)))
    monkeypatch.setattr(worker.db, "fail_target_index_job",
                        lambda _pg, job_id, err: failed.append((job_id, err)))

    job = {
        "job_id": "job-dpy",
        "migration_id": "migration-1",
        "target_connection_id": "oracle_target",
        "target_schema": "TCBPAY",
        "target_table": "ISS#ORDERS",
    }

    worker.process_target_index_job(job, object(), {})

    out = capsys.readouterr().out
    assert "TCBPAY.ISS#ORDERS/job-dpy started" in out
    assert "stage=temporary-probe table=TCBPAY.ISS#ORDERS" in out
    assert "FAILED stage=temporary-probe err=RuntimeError: DPY-1001" in out
    assert "diagnostics err=RuntimeError: DPY-1001" in out
    assert "stage=enable-target-indexes" in out
    assert "DPY-1001 target_conn present=true type=BrokenConn closed_attr=False" in out
    assert "select1=failed:RuntimeError:DPY-1001: not connected to database" in out
    assert "FAILED: RuntimeError: DPY-1001: not connected to database" in out
    assert completed == []
    assert failed == [("job-dpy", "RuntimeError: DPY-1001: not connected to database")]
    assert conn.closed is True
