from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
WORKERS_DIR = ROOT / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import worker  # noqa: E402


def _chunk() -> dict:
    return {
        "chunk_id": "chunk-1",
        "target_connection_id": "oracle_target",
        "target_schema": "TGT",
        "target_table": "PARENT",
        "stage_table": "STG_PARENT",
        "rowid_start": "AAAA",
        "rowid_end": "BBBB",
    }


class Ora00001Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.fetchone_value = None

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql, params=None):
        compact = " ".join(sql.split())
        self.conn.executed.append((compact, params))
        if compact.startswith("INSERT"):
            raise Exception("ORA-00001: unique constraint violated")
        if compact.startswith('SELECT COUNT(*) FROM "TGT"."STG_PARENT"'):
            self.fetchone_value = self.conn.stage_rows
        elif compact.startswith('SELECT COUNT(*) FROM "TGT"."PARENT"'):
            self.fetchone_value = self.conn.target_rows

    def fetchone(self):
        return (self.fetchone_value,)


class Ora00001Conn:
    def __init__(self, stage_rows: int, target_rows: int):
        self.stage_rows = stage_rows
        self.target_rows = target_rows
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return Ora00001Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_baseline_ora00001_fails_when_target_does_not_contain_stage_rows(monkeypatch):
    conn = Ora00001Conn(stage_rows=124, target_rows=0)
    progress_calls = []
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_args: conn)
    monkeypatch.setattr(worker.db, "update_chunk_progress", lambda *args: progress_calls.append(args))

    with pytest.raises(RuntimeError, match="target has 0 rows"):
        worker._process_baseline_chunk(_chunk(), object(), {})

    assert progress_calls == []
    assert conn.rollbacks == 1
    assert conn.closed


def test_baseline_ora00001_is_idempotent_when_target_already_has_rows(monkeypatch):
    conn = Ora00001Conn(stage_rows=62, target_rows=62)
    progress_calls = []
    monkeypatch.setattr(worker.db, "open_oracle", lambda *_args: conn)
    monkeypatch.setattr(worker.db, "update_chunk_progress", lambda *args: progress_calls.append(args))

    rows_loaded = worker._process_baseline_chunk(_chunk(), object(), {})

    assert rows_loaded == 62
    assert len(progress_calls) == 1
    assert conn.rollbacks == 0
    assert conn.closed
