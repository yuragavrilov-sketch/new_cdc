from __future__ import annotations

from services import ddl_sync_extended


class CursorStub:
    def __init__(self, executed: list[str]):
        self.executed = executed

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql: str, *_args, **_kwargs):
        self.executed.append(sql)


class ConnStub:
    def __init__(self):
        self.executed: list[str] = []
        self.commits = 0

    def cursor(self):
        return CursorStub(self.executed)

    def commit(self):
        self.commits += 1


def test_sync_index_uses_target_schema(monkeypatch):
    monkeypatch.setattr(
        ddl_sync_extended,
        "get_index_info",
        lambda *_args: {
            "table_name": "ORDERS",
            "uniqueness": "NONUNIQUE",
            "index_type": "NORMAL",
            "columns": [{"name": "ID", "descending": False}],
        },
    )
    target = ConnStub()

    result = ddl_sync_extended.sync_to_target(
        object(),
        target,
        "SRC",
        "IX_ORDERS_ID",
        "INDEX",
        "create_missing",
        target_schema="TGT",
    )

    assert result["action"] == "created"
    assert 'CREATE INDEX "TGT"."IX_ORDERS_ID" ON "TGT"."ORDERS" ("ID")' in target.executed


def test_sync_trigger_uses_target_schema(monkeypatch):
    monkeypatch.setattr(
        ddl_sync_extended,
        "get_trigger_info",
        lambda *_args: {
            "trigger_type": "BEFORE EACH ROW",
            "triggering_event": "INSERT",
            "table_name": "ORDERS",
            "when_clause": None,
            "trigger_body": "BEGIN NULL; END;",
        },
    )
    target = ConnStub()

    result = ddl_sync_extended.sync_to_target(
        object(),
        target,
        "SRC",
        "TRG_ORDERS_BI",
        "TRIGGER",
        "sync_diff",
        target_schema="TGT",
    )

    assert result["action"] == "created"
    assert target.executed == [
        'CREATE OR REPLACE TRIGGER "TGT"."TRG_ORDERS_BI"\n'
        'BEFORE EACH ROW INSERT\n'
        'ON "TGT"."ORDERS"\n'
        'BEGIN NULL; END;'
    ]
