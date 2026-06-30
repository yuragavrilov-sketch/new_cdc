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
