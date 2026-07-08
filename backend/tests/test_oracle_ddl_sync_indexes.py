from __future__ import annotations

from services import oracle_ddl_sync


class IndexCursorStub:
    def __init__(self, conn):
        self.conn = conn
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.conn.executed.append((self.sql, params))

    def fetchall(self):
        if self.conn.role == "source":
            if "FROM all_indexes ai" in self.sql and "ai.index_type" in self.sql:
                return [
                    (
                        "IX_ORDERS_UPPER_CARD",
                        "NONUNIQUE",
                        "FUNCTION-BASED NORMAL",
                        "SYS_NC00005$",
                    )
                ]
            if "FROM all_ind_expressions" in self.sql:
                return [("IX_ORDERS_UPPER_CARD", 1, 'UPPER("CARD_ID")')]
            if "FROM all_constraints" in self.sql:
                return []
        if self.conn.role == "target":
            if self.sql.startswith("SELECT"):
                return []
        return []


class IndexConnStub:
    def __init__(self, role: str):
        self.role = role
        self.executed = []
        self.commits = 0

    def cursor(self):
        return IndexCursorStub(self)

    def commit(self):
        self.commits += 1


def test_sync_indexes_creates_function_based_normal_index():
    source = IndexConnStub("source")
    target = IndexConnStub("target")
    out = {"added": [], "skipped": [], "errors": []}

    oracle_ddl_sync._sync_indexes(
        source,
        target,
        "SRC",
        "ORDERS",
        "TGT",
        "ORDERS",
        out,
    )

    target_sql = [sql for sql, _params in target.executed]
    assert (
        'CREATE INDEX "TGT"."IX_ORDERS_UPPER_CARD" '
        'ON "TGT"."ORDERS" (UPPER("CARD_ID"))'
    ) in target_sql
    assert out == {
        "added": ["IX_ORDERS_UPPER_CARD"],
        "skipped": [],
        "errors": [],
    }
    assert target.commits == 1
