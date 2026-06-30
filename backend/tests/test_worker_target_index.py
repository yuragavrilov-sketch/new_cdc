from __future__ import annotations

import sys
from pathlib import Path


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
    assert conn.committed
