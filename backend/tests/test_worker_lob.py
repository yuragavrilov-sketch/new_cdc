from __future__ import annotations

import base64
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKERS_DIR = ROOT / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import worker  # noqa: E402
from routes import data_compare  # noqa: E402


# ---------------------------------------------------------------------------
# CDC LOB sanitisation
# ---------------------------------------------------------------------------

def test_sanitize_strips_unavailable_placeholder():
    # An UPDATE that didn't touch the CLOB carries the sentinel, not the value.
    row = {"ID": 1, "DOC": worker.CDC_UNAVAILABLE_PLACEHOLDER, "NAME": "x"}
    out = worker._sanitize_lob_values(row, set())
    assert out == {"ID": 1, "NAME": "x"}          # DOC dropped → LOB preserved


def test_sanitize_strips_binary_placeholder_b64():
    row = {"PHOTO": worker._CDC_UNAVAILABLE_PLACEHOLDER_B64}
    out = worker._sanitize_lob_values(row, {"PHOTO"})
    assert "PHOTO" not in out


def test_sanitize_base64_decodes_binary_column():
    raw = b"\x00\x01\x02hello"
    row = {"ID": 1, "PHOTO": base64.b64encode(raw).decode("ascii")}
    out = worker._sanitize_lob_values(row, {"PHOTO"})
    assert out["PHOTO"] == raw                     # bytes, not base64 text


def test_sanitize_passes_clob_text_through():
    row = {"ID": 5, "DOC": "real clob text"}
    out = worker._sanitize_lob_values(row, set())
    assert out == {"ID": 5, "DOC": "real clob text"}


# ---------------------------------------------------------------------------
# Bulk insert binds LOB columns as LOBs
# ---------------------------------------------------------------------------

def test_lob_batch_size_defaults_to_bulk_batch_size():
    assert worker.BULK_LOB_BATCH_SIZE == worker.BULK_BATCH_SIZE


def test_build_insert_marks_lob_binds(monkeypatch):
    monkeypatch.setattr(worker, "_LOB_DBTYPES", ("CLOBT", "BLOBT"))
    desc = [
        ("ID", "NUMT", None, None),
        ("DOC", "CLOBT", None, None),
        ("PIC", "BLOBT", None, None),
    ]
    sql, binds, lob = worker._build_insert(desc, "TGT", "STG")
    assert binds == ["c0", "c1", "c2"]
    assert lob == {"c1": "CLOBT", "c2": "BLOBT"}
    assert "APPEND_VALUES" in sql


def test_build_insert_uses_source_lob_metadata_when_fetch_reports_long(monkeypatch):
    monkeypatch.setattr(
        worker,
        "_LOB_BIND_DBTYPE_BY_DATA_TYPE",
        {"CLOB": "CLOBT", "NCLOB": "NCLOBT", "BLOB": "BLOBT"},
    )
    desc = [
        ("ORDERID", "NUMT", None, None),
        ("GOOGLEDATA", "LONGT", None, None),
        ("FINGERPRINTDATA", "LONGT", None, None),
    ]

    sql, binds, lob = worker._build_insert(
        desc,
        "TCBPAY",
        "FORM#FINGERPRINTS",
        {"GOOGLEDATA": "CLOB", "FINGERPRINTDATA": "CLOB"},
    )

    assert binds == ["c0", "c1", "c2"]
    assert lob == {"c1": "CLOBT", "c2": "CLOBT"}
    assert '"GOOGLEDATA"' in sql
    assert '"FINGERPRINTDATA"' in sql


def test_build_insert_no_lobs_is_empty():
    monkeypatch_free_desc = [("ID", "NUMT", None, None)]
    # _LOB_DBTYPES is () when oracledb is absent; force that for determinism.
    import worker as w
    saved = w._LOB_DBTYPES
    w._LOB_DBTYPES = ()
    try:
        _sql, _binds, lob = w._build_insert(monkeypatch_free_desc, "TGT", "STG")
    finally:
        w._LOB_DBTYPES = saved
    assert lob == {}


def test_source_table_lob_probe_uses_non_reserved_bind_name():
    class Cursor:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            self.conn.calls.append((" ".join(sql.split()), params))

        def fetchall(self):
            return [("DOC", "CLOB")]

    class Conn:
        def __init__(self):
            self.calls = []

        def cursor(self):
            return Cursor(self)

    conn = Conn()

    assert worker._source_table_has_lob(conn, "TCBPAY", "FORM#FINGERPRINTS") is True

    sql, params = conn.calls[0]
    assert "table_name = :table_name" in sql
    assert ":table " not in f"{sql} "
    assert params == {
        "owner": "TCBPAY",
        "table_name": "FORM#FINGERPRINTS",
    }


def test_bulk_lob_batch_summary_reports_types_and_lengths_without_values():
    batch = [
        {"c0": 1, "c1": "secret-clob-data"},
        {"c0": 2, "c1": None},
        {"c0": 3, "c1": b"\x00\x01"},
    ]

    summary = worker._bulk_lob_batch_summary(
        batch,
        {"c1": "DOC"},
        {"c1": "CLOBT"},
    )

    assert "c1/DOC" in summary
    assert "dbtype=CLOBT" in summary
    assert "py_types=NoneType:1,bytes:1,str:1" in summary
    assert "nulls=1/3" in summary
    assert "max_len=16" in summary
    assert "total_len=18" in summary
    assert "secret-clob-data" not in summary


def test_bulk_chunk_uses_lob_fetch_batch_for_lob_source_table(monkeypatch):
    class SourceCursor:
        description = [("ID", "NUMT", None, None), ("DOC", "LONGT", None, None)]

        def __init__(self, conn):
            self.conn = conn
            self.arraysize = None
            self.prefetchrows = None
            self.data_query = False
            self.fetchmany_sizes = []
            self._rows_fetched = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(sql.split())
            self.conn.executed.append((compact, params))
            self.data_query = "ROWID BETWEEN" in compact

        def fetchall(self):
            return [("DOC", "CLOB")]

        def fetchmany(self, size):
            self.fetchmany_sizes.append(size)
            if self._rows_fetched:
                return []
            self._rows_fetched = True
            return [(1, "x" * 100)]

    class SourceConn:
        def __init__(self):
            self.executed = []
            self.cursors = []
            self.closed = False

        def cursor(self):
            cursor = SourceCursor(self)
            self.cursors.append(cursor)
            return cursor

        def rollback(self):
            pass

        def close(self):
            self.closed = True

    class TargetCursor:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def setinputsizes(self, **kwargs):
            self.conn.inputsizes.append(kwargs)

        def executemany(self, sql, batch):
            self.conn.batches.append((sql, batch))

    class TargetConn:
        def __init__(self):
            self.inputsizes = []
            self.batches = []
            self.commits = 0
            self.closed = False

        def cursor(self):
            return TargetCursor(self)

        def commit(self):
            self.commits += 1

        def rollback(self):
            pass

        def close(self):
            self.closed = True

    src_conn = SourceConn()
    dst_conn = TargetConn()
    chunk = {
        "chunk_id": "chunk-1",
        "migration_id": "mid-1",
        "source_connection_id": "oracle_source",
        "target_connection_id": "oracle_target",
        "source_schema": "SRC",
        "source_table": "FORM#FINGERPRINTS",
        "target_schema": "TGT",
        "target_table": "FORM#FINGERPRINTS",
        "stage_table": "",
        "strategy": "CDC_DIRECT",
        "start_scn": None,
        "rowid_start": "AAAA",
        "rowid_end": "BBBB",
    }

    monkeypatch.setattr(worker, "BULK_BATCH_SIZE", 20_000)
    monkeypatch.setattr(worker, "BULK_LOB_BATCH_SIZE", 20_000, raising=False)
    monkeypatch.setattr(worker, "_LOB_DBTYPES", ())
    monkeypatch.setattr(worker, "_LOB_BIND_DBTYPE_BY_DATA_TYPE", {"CLOB": "CLOBT"})
    monkeypatch.setattr(worker.db, "chunk_is_active", lambda *_args: True)
    monkeypatch.setattr(worker.db, "update_chunk_progress", lambda *_args: None)
    monkeypatch.setattr(
        worker.db,
        "open_oracle",
        lambda conn_id, *_args: src_conn if conn_id == "oracle_source" else dst_conn,
    )

    rows_loaded = worker._process_bulk_chunk(chunk, "pg-conn", {})

    data_cursor = next(cursor for cursor in src_conn.cursors if cursor.data_query)
    assert rows_loaded == 1
    assert data_cursor.arraysize == 20_000
    assert data_cursor.prefetchrows == 20_000
    assert data_cursor.fetchmany_sizes == [20_000, 20_000]
    assert dst_conn.inputsizes == [{"c1": "CLOBT"}]
    assert dst_conn.commits == 1


def test_bulk_chunk_falls_back_to_conventional_split_after_protocol_error(monkeypatch, capsys):
    class SourceCursor:
        description = [("ID", "NUMT", None, None), ("DOC", "CLOBT", None, None)]

        def __init__(self):
            self.arraysize = None
            self.prefetchrows = None
            self.data_query = False
            self._rows_fetched = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            self.data_query = "ROWID BETWEEN" in " ".join(sql.split())

        def fetchall(self):
            return [("DOC", "CLOB")]

        def fetchmany(self, _size):
            if self._rows_fetched:
                return []
            self._rows_fetched = True
            return [
                (1, "doc-1"),
                (2, "doc-2"),
                (3, "doc-3"),
                (4, "doc-4"),
            ]

    class SourceConn:
        def __init__(self):
            self.cursor_obj = SourceCursor()

        def cursor(self):
            return self.cursor_obj

        def rollback(self):
            pass

        def close(self):
            pass

    class TargetCursor:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def setinputsizes(self, **kwargs):
            self.conn.inputsizes.append(kwargs)

        def executemany(self, sql, batch):
            self.conn.calls.append((sql, list(batch)))
            if self.conn.fail_protocol:
                raise RuntimeError("ORA-03106: fatal two-task communication protocol error")

    class TargetConn:
        def __init__(self, *, fail_protocol=False):
            self.fail_protocol = fail_protocol
            self.calls = []
            self.inputsizes = []
            self.commits = 0
            self.rollbacks = 0
            self.closed = False

        def cursor(self):
            return TargetCursor(self)

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

        def close(self):
            self.closed = True

    src_conn = SourceConn()
    fast_target = TargetConn(fail_protocol=True)
    fallback_target = TargetConn()
    target_conns = [fast_target, fallback_target]
    progress = []
    chunk = {
        "chunk_id": "chunk-fallback",
        "chunk_seq": 7,
        "migration_id": "migration-1",
        "source_connection_id": "oracle_source",
        "target_connection_id": "oracle_target",
        "source_schema": "TCBPAY",
        "source_table": "FORM#FINGERPRINTS",
        "target_schema": "TCBPAY",
        "target_table": "FORM#FINGERPRINTS",
        "stage_table": "",
        "strategy": "CDC_DIRECT",
        "start_scn": None,
        "rowid_start": "AAAA",
        "rowid_end": "BBBB",
    }

    def open_oracle(conn_id, *_args):
        if conn_id == "oracle_source":
            return src_conn
        return target_conns.pop(0)

    monkeypatch.setattr(worker, "BULK_BATCH_SIZE", 20_000)
    monkeypatch.setattr(worker, "BULK_LOB_BATCH_SIZE", 20_000, raising=False)
    monkeypatch.setattr(worker, "BULK_FALLBACK_BATCH_SIZE", 2, raising=False)
    monkeypatch.setattr(worker, "_LOB_DBTYPES", ("CLOBT",))
    monkeypatch.setattr(worker, "_LOB_BIND_DBTYPE_BY_DATA_TYPE", {"CLOB": "CLOBT"})
    monkeypatch.setattr(worker.db, "chunk_is_active", lambda *_args: True)
    monkeypatch.setattr(worker.db, "update_chunk_progress", lambda _pg, cid, rows: progress.append((cid, rows)))
    monkeypatch.setattr(worker.db, "open_oracle", open_oracle)

    rows_loaded = worker._process_bulk_chunk(chunk, "pg-conn", {})

    assert rows_loaded == 4
    assert fast_target.rollbacks == 1
    assert fast_target.closed is True
    assert fallback_target.commits == 1
    assert fallback_target.closed is True
    assert len(fallback_target.calls) == 2
    assert [len(batch) for _sql, batch in fallback_target.calls] == [2, 2]
    assert all("APPEND_VALUES" not in sql for sql, _batch in fallback_target.calls)
    assert all("INSERT INTO" in sql for sql, _batch in fallback_target.calls)
    assert fallback_target.inputsizes == [{"c1": "CLOBT"}, {"c1": "CLOBT"}]
    assert progress == [("chunk-fallback", 4)]

    out = capsys.readouterr().out
    assert "[bulk:chunk-fa] fallback start reason=ORA_PROTOCOL_ERROR" in out
    assert "append_values=false" in out
    assert "fallback done rows=4" in out


def test_bulk_chunk_logs_flush_context_on_oracle_protocol_error(monkeypatch, capsys):
    class SourceCursor:
        description = [("ID", "NUMT", None, None), ("DOC", "CLOBT", None, None)]

        def __init__(self, conn):
            self.conn = conn
            self.arraysize = None
            self.prefetchrows = None
            self.data_query = False
            self._rows_fetched = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            compact = " ".join(sql.split())
            self.conn.executed.append((compact, params))
            self.data_query = "ROWID BETWEEN" in compact

        def fetchall(self):
            return [("DOC", "CLOB")]

        def fetchmany(self, _size):
            if self._rows_fetched:
                return []
            self._rows_fetched = True
            return [(1, "secret-clob-data")]

    class SourceConn:
        def __init__(self):
            self.executed = []

        def cursor(self):
            return SourceCursor(self)

        def rollback(self):
            pass

        def close(self):
            pass

    class TargetCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def setinputsizes(self, **_kwargs):
            pass

        def executemany(self, _sql, _batch):
            raise RuntimeError("ORA-03106: fatal two-task communication protocol error")

    class TargetConn:
        def cursor(self):
            return TargetCursor()

        def commit(self):
            raise AssertionError("commit must not run after failed executemany")

        def rollback(self):
            pass

        def close(self):
            pass

    src_conn = SourceConn()
    dst_conn = TargetConn()
    chunk = {
        "chunk_id": "chunk-ora-03106",
        "chunk_seq": 42,
        "migration_id": "migration-1",
        "source_connection_id": "oracle_source",
        "target_connection_id": "oracle_target",
        "source_schema": "TCBPAY",
        "source_table": "FORM#FINGERPRINTS",
        "target_schema": "TCBPAY",
        "target_table": "FORM#FINGERPRINTS",
        "stage_table": "",
        "strategy": "CDC_DIRECT",
        "start_scn": None,
        "rowid_start": "AAAA",
        "rowid_end": "BBBB",
    }

    monkeypatch.setattr(worker, "BULK_BATCH_SIZE", 20_000)
    monkeypatch.setattr(worker, "BULK_LOB_BATCH_SIZE", 25, raising=False)
    monkeypatch.setattr(worker, "_LOB_DBTYPES", ("CLOBT",))
    monkeypatch.setattr(worker.db, "chunk_is_active", lambda *_args: True)
    monkeypatch.setattr(
        worker.db,
        "open_oracle",
        lambda conn_id, *_args: src_conn if conn_id == "oracle_source" else dst_conn,
    )

    try:
        worker._process_bulk_chunk(chunk, "pg-conn", {})
    except RuntimeError as exc:
        assert "ORA-03106" in str(exc)
    else:
        raise AssertionError("expected ORA-03106 failure")

    out = capsys.readouterr().out
    assert "[bulk:chunk-or] FAILED stage=flush batch=1" in out
    assert "src=TCBPAY.FORM#FINGERPRINTS" in out
    assert "dest=TCBPAY.FORM#FINGERPRINTS" in out
    assert "rows_loaded=0" in out
    assert "batch_rows=1" in out
    assert "lob_summary=c1/DOC" in out
    assert "py_types=str:1" in out
    assert "max_len=16" in out
    assert "secret-clob-data" not in out


# ---------------------------------------------------------------------------
# LOBs are now compared by length (worker and route must agree)
# ---------------------------------------------------------------------------

def test_lob_columns_are_compared_by_length():
    for ctype in ("BLOB", "CLOB", "NCLOB"):
        assert "DBMS_LOB.GETLENGTH" in worker._cmp_col_expr("DOC", ctype)
        assert "DBMS_LOB.GETLENGTH" in data_compare.col_expr("DOC", ctype)
        assert ctype not in worker._CMP_SKIP_TYPES
        assert ctype not in data_compare._SKIP_TYPES


def test_compare_skip_sets_match_between_worker_and_route():
    assert worker._CMP_SKIP_TYPES == data_compare._SKIP_TYPES
    assert worker._CMP_LOB_TYPES == data_compare._LOB_TYPES
