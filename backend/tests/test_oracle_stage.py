from services import oracle_stage


class CursorStub:
    def __init__(self, kind, calls):
        self.kind = kind
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        compact_sql = " ".join(sql.split())
        self.calls.append((self.kind, compact_sql, params))
        if self.kind == "dst" and compact_sql.startswith("CREATE TABLE"):
            raise Exception("ORA-00955: name is already used by an existing object")

    def fetchall(self):
        return [("ID", "NUMBER", None, 10, 0, "N", "B")]

    def fetchone(self):
        return ("PAYSTAGE",)


class ConnStub:
    def __init__(self, kind, calls):
        self.kind = kind
        self.calls = calls

    def cursor(self):
        return CursorStub(self.kind, self.calls)

    def commit(self):
        self.calls.append((self.kind, "commit", None))

    def close(self):
        self.calls.append((self.kind, "close", None))


def test_create_stage_table_truncates_existing_table_in_requested_tablespace(monkeypatch):
    calls = []
    conns = [ConnStub("src", calls), ConnStub("dst", calls)]

    monkeypatch.setattr(oracle_stage, "open_oracle_conn", lambda _cfg: conns.pop(0))

    oracle_stage.create_stage_table(
        {"name": "source"},
        {"name": "target"},
        "TCBPAY",
        "ALLORDERS",
        "TCBPAY",
        "STG_TCBPAY_ALLORDERS",
        tablespace="PAYSTAGE",
    )

    assert not any("MOVE TABLESPACE" in sql for _kind, sql, _params in calls)
    assert any(
        kind == "dst" and "FROM all_tables" in sql
        for kind, sql, _params in calls
    )
    assert any(
        kind == "dst" and sql == 'TRUNCATE TABLE "TCBPAY"."STG_TCBPAY_ALLORDERS"'
        for kind, sql, _params in calls
    )
