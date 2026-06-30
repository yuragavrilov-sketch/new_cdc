from __future__ import annotations

from db import oracle_browser


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
        self.committed = False

    def cursor(self):
        return CursorStub(self.executed)

    def commit(self):
        self.committed = True


class RefFkCursorStub:
    def __init__(self, conn):
        self.conn = conn
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql: str, params=None, **_kwargs):
        self.sql = sql
        self.conn.executed.append((sql, params))

    def fetchall(self):
        if "FROM   all_constraints fk" in self.sql:
            return [("TGT", "CHILD", "FK_CHILD_PARENT", "ENABLED")]
        return []


class RefFkConnStub:
    def __init__(self):
        self.executed = []
        self.committed = False

    def cursor(self):
        return RefFkCursorStub(self)

    def commit(self):
        self.committed = True


class TruncateCursorStub:
    def __init__(self, conn):
        self.conn = conn
        self.sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql: str, params=None, **_kwargs):
        self.sql = sql
        self.conn.executed.append((sql, params))

    def fetchall(self):
        if "FROM   all_constraints fk" in self.sql:
            return [("TGT", "CHILD", "FK_CHILD_PARENT", "ENABLED")]
        if "constraint_type IN ('P', 'U')" in self.sql:
            return [("PARENT_PK", "P")]
        return []


class TruncateConnStub:
    def __init__(self):
        self.executed = []
        self.commits = 0

    def cursor(self):
        return TruncateCursorStub(self)

    def commit(self):
        self.commits += 1


class TableInfoCursorStub:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql: str, *_args, **_kwargs):
        self.sql = sql.lower()

    def fetchall(self):
        if "all_tab_columns" in self.sql:
            return [("ID", "NUMBER", "N")]
        if "constraint_type = 'p'" in self.sql:
            return [("ID",)]
        return []

    def fetchone(self):
        if "v$database" in self.sql:
            return ("NO",)
        if "all_log_groups" in self.sql:
            return (1,)
        return None


class TableInfoConnStub:
    def cursor(self):
        return TableInfoCursorStub()


def test_get_table_info_reports_table_supplemental_logging():
    info = oracle_browser.get_table_info(TableInfoConnStub(), "SRC", "T")

    assert info["columns"] == [{"name": "ID", "type": "NUMBER", "nullable": False}]
    assert info["pk_columns"] == ["ID"]
    assert info["supplemental_log_data_all"] == "YES"


def test_enable_all_disabled_objects_enables_fk_novalidate(monkeypatch):
    monkeypatch.setattr(oracle_browser, "is_temporary_table", lambda *_args: False)
    monkeypatch.setattr(
        oracle_browser,
        "get_full_ddl_info",
        lambda *_args: {
            "indexes": [{"name": "IX_CHILD", "status": "UNUSABLE"}],
            "constraints": [
                {"name": "CHK_CHILD", "type_code": "C", "status": "DISABLED"},
                {"name": "FK_CHILD_PARENT", "type_code": "R", "status": "DISABLED"},
            ],
            "triggers": [{"name": "TRG_CHILD", "status": "DISABLED"}],
        },
    )
    conn = ConnStub()

    result = oracle_browser.enable_all_disabled_objects(conn, "TGT", "CHILD")

    assert 'ALTER INDEX "TGT"."IX_CHILD" REBUILD NOLOGGING' in conn.executed
    assert 'ALTER TABLE "TGT"."CHILD" ENABLE CONSTRAINT "CHK_CHILD"' in conn.executed
    assert 'ALTER TABLE "TGT"."CHILD" ENABLE NOVALIDATE CONSTRAINT "FK_CHILD_PARENT"' in conn.executed
    assert result["enabled"]["constraints"] == ["CHK_CHILD"]
    assert result["enabled"]["fk_novalidate"] == ["FK_CHILD_PARENT"]
    assert result["errors"] == {"indexes": [], "constraints": []}
    assert not any("ALTER TRIGGER" in sql for sql in conn.executed)
    assert conn.committed


def test_disable_referencing_foreign_keys_disables_child_constraints():
    conn = RefFkConnStub()

    disabled = oracle_browser.disable_referencing_foreign_keys(conn, "TGT", "PARENT")

    assert disabled == ["TGT.CHILD.FK_CHILD_PARENT"]
    assert any(
        sql == 'ALTER TABLE "TGT"."CHILD" DISABLE CONSTRAINT "FK_CHILD_PARENT"'
        for sql, _params in conn.executed
    )
    assert any(
        "fk.status = :status" in sql and params["status"] == "ENABLED"
        for sql, params in conn.executed
        if params
    )
    assert conn.committed


def test_truncate_table_for_load_disables_child_fk_and_parent_key_cascade():
    conn = TruncateConnStub()

    result = oracle_browser.truncate_table_for_load(conn, "TGT", "PARENT")

    sqls = [sql for sql, _params in conn.executed]
    assert 'ALTER TABLE "TGT"."CHILD" DISABLE CONSTRAINT "FK_CHILD_PARENT"' in sqls
    assert 'ALTER TABLE "TGT"."PARENT" DISABLE CONSTRAINT "PARENT_PK" CASCADE' in sqls
    assert 'TRUNCATE TABLE "TGT"."PARENT"' in sqls
    assert sqls.index('ALTER TABLE "TGT"."PARENT" DISABLE CONSTRAINT "PARENT_PK" CASCADE') < sqls.index(
        'TRUNCATE TABLE "TGT"."PARENT"'
    )
    assert result == {
        "referencing_fk": ["TGT.CHILD.FK_CHILD_PARENT"],
        "key_constraints": ["P:PARENT_PK"],
    }
    assert conn.commits >= 3
