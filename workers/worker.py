"""
Universal Worker — bulk loading + CDC apply, both via direct PostgreSQL access.

No HTTP calls to Flask.  Workers read configs and job queue straight from the
state DB using SELECT … FOR UPDATE SKIP LOCKED.

Usage:
    python worker.py

Environment variables (see .env.example):
    STATE_DB_DSN       PostgreSQL DSN           (default: postgres://postgres:postgres@localhost:5432/migration_state)
    WORKER_ID          Unique identifier        (default: hostname:pid)
    BULK_BATCH_SIZE    Rows per INSERT batch    (default: 5000)
    BULK_LOB_BATCH_SIZE Optional rows per INSERT batch cap for LOB tables (default: BULK_BATCH_SIZE)
    BULK_FALLBACK_BATCH_SIZE Rows per conventional fallback INSERT batch (default: 1000)
    BULK_POLL_INTERVAL Seconds between polls    (default: 5)
    CDC_BATCH_SIZE     Kafka records per cycle  (default: 500)
    CDC_CHECKIN_SEC    Seconds between checkins (default: 30)
    CDC_POLL_MS        Kafka poll timeout ms    (default: 1000)
    CDC_SCAN_INTERVAL  Seconds between CDC scans (default: 3)
    WORKER_HEARTBEAT_SEC Seconds between worker liveness writes (default: 10)
"""

import base64
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Optional

# Load .env from the workers directory (if it exists) before anything else
_HERE = Path(__file__).parent
_env_file = _HERE / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=False)   # override=False: OS env takes priority
        print(f"[worker] loaded env from {_env_file}")
    except ImportError:
        # dotenv not installed — parse manually (key=value, skip comments/blanks)
        with open(_env_file) as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _k, _, _v = _line.partition("=")
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k and _k not in os.environ:   # don't override OS env
                    os.environ[_k] = _v
        print(f"[worker] loaded env from {_env_file} (manual parser)")

sys.path.insert(0, str(_HERE))
import common as db
from common import WORKER_ID

# Fetch LOBs (CLOB/BLOB/NCLOB) as Python str/bytes directly — LOB locators
# from AS OF SCN flashback cursors are invalid outside the fetching cursor,
# which causes ORA-64219.  Setting this globally is safe for bulk copy.
_LOB_DBTYPES: tuple = ()
_LOB_BIND_DBTYPE_BY_DATA_TYPE: dict = {}
try:
    import oracledb
    oracledb.defaults.fetch_lobs = False
    # LOB column DB types — used to bind bulk inserts as LOBs (streamed) instead
    # of letting oracledb preallocate a full per-batch char/byte buffer, which
    # would OOM on multi-MB LOBs at BULK_BATCH_SIZE rows.
    _LOB_DBTYPES = (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB, oracledb.DB_TYPE_BLOB)
    _LOB_BIND_DBTYPE_BY_DATA_TYPE = {
        "CLOB": oracledb.DB_TYPE_CLOB,
        "NCLOB": oracledb.DB_TYPE_NCLOB,
        "BLOB": oracledb.DB_TYPE_BLOB,
    }
except ImportError:
    pass

BULK_BATCH_SIZE    = int(os.environ.get("BULK_BATCH_SIZE",    20_000))
BULK_LOB_BATCH_SIZE = max(1, int(os.environ.get("BULK_LOB_BATCH_SIZE", BULK_BATCH_SIZE)))
BULK_FALLBACK_BATCH_SIZE = max(1, int(os.environ.get("BULK_FALLBACK_BATCH_SIZE", 1_000)))
BULK_POLL_INTERVAL = int(os.environ.get("BULK_POLL_INTERVAL", 5))
CDC_BATCH_SIZE     = int(os.environ.get("CDC_BATCH_SIZE",     500))
CDC_CHECKIN_SEC    = int(os.environ.get("CDC_CHECKIN_SEC",    30))
CDC_POLL_MS        = int(os.environ.get("CDC_POLL_MS",        1_000))
CDC_POLL_ERROR_THRESHOLD = max(1, int(os.environ.get("CDC_POLL_ERROR_THRESHOLD", 3)))
CDC_SCAN_INTERVAL  = int(os.environ.get("CDC_SCAN_INTERVAL",  3))
CMP_POLL_INTERVAL  = int(os.environ.get("CMP_POLL_INTERVAL",  5))
WORKER_HEARTBEAT_SEC = int(os.environ.get("WORKER_HEARTBEAT_SEC", 10))
# Rebuild post-load indexes with PARALLEL (degree reset to NOPARALLEL after) —
# large index rebuilds are much faster in parallel. Disable for tiny tables.
INDEX_REBUILD_PARALLEL = os.environ.get("INDEX_REBUILD_PARALLEL", "true").lower() == "true"

# Sentinel Debezium emits for a LOB column that was NOT changed by an UPDATE
# (only changed columns are mined for LOBs). Must match the connector's
# unavailable.value.placeholder (see services/debezium.py). The CDC apply path
# DROPS columns equal to this sentinel so an unchanged LOB is not overwritten.
CDC_UNAVAILABLE_PLACEHOLDER = os.environ.get(
    "CDC_UNAVAILABLE_PLACEHOLDER", "__debezium_unavailable_value"
)
# Same sentinel as it appears for a binary (BLOB/RAW) column under
# binary.handling.mode=base64 — the bytes of the placeholder, base64-encoded.
_CDC_UNAVAILABLE_PLACEHOLDER_B64 = base64.b64encode(
    CDC_UNAVAILABLE_PLACEHOLDER.encode("utf-8")
).decode("ascii")


class ChunkCancelled(RuntimeError):
    """Raised when a worker notices that an active chunk was cancelled."""


def _session_context(action: str, *, migration_id: str | None = None,
                     chunk_id: str | None = None, table: str | None = None) -> dict:
    parts = []
    if migration_id:
        parts.append(f"mig={migration_id[:8]}")
    if chunk_id:
        parts.append(f"chunk={chunk_id[:8]}")
    parts.append(f"worker={WORKER_ID}")
    if table:
        parts.append(f"table={table}")
    return {
        "module": "new_cdc.worker",
        "action": action,
        "client_identifier": " ".join(parts),
    }


def _ensure_chunk_active(pg_conn, chunk_id: str) -> None:
    if not db.chunk_is_active(pg_conn, chunk_id):
        raise ChunkCancelled(f"chunk {chunk_id} cancelled or no longer active")


# ══════════════════════════════════════════════════════════════════════════════
# BULK LOADING
# ══════════════════════════════════════════════════════════════════════════════

def _source_lob_column_types(conn, schema: str, table: str) -> dict:
    """Return source LOB column names mapped to Oracle dictionary data types."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM   all_tab_columns
            WHERE  owner = :owner
              AND  table_name = :table_name
              AND  data_type IN ('BLOB', 'CLOB', 'NCLOB')
        """, {"owner": schema.upper(), "table_name": table.upper()})
        return {
            str(column_name).upper(): str(data_type).upper()
            for column_name, data_type in cur.fetchall()
        }


def _source_table_has_lob(conn, schema: str, table: str) -> bool:
    """Return True when source table contains LOB columns."""
    return bool(_source_lob_column_types(conn, schema, table))


def _build_insert(cursor_description, target_schema: str,
                  stage_table: str, source_lob_column_types: dict | None = None) -> tuple:
    """
    Returns (sql, bind_names).

    Uses APPEND_VALUES hint for direct-path (no undo) insert and
    safe :c0, :c1 bind names (oracledb requires names to start with a letter).
    executemany is called with a list of dicts keyed by bind_names.
    """
    col_names  = [d[0] for d in cursor_description]
    bind_names = [f"c{i}" for i in range(len(col_names))]
    cols   = ", ".join(f'"{c}"' for c in col_names)
    params = ", ".join(f":{b}" for b in bind_names)
    sql = (
        f'INSERT /*+ APPEND_VALUES */ INTO '
        f'"{target_schema.upper()}"."{stage_table.upper()}" '
        f'({cols}) VALUES ({params})'
    )
    # Bind LOB columns explicitly as LOBs so oracledb streams them instead of
    # sizing a full per-batch buffer from the data (OOM risk on large LOBs).
    # With fetch_lobs=False, CLOB/NCLOB may appear in cursor.description as
    # DB_TYPE_LONG, so prefer all_tab_columns metadata when it is available.
    source_lob_column_types = {
        str(name).upper(): str(data_type).upper()
        for name, data_type in (source_lob_column_types or {}).items()
    }
    lob_inputsizes = {}
    for i, d in enumerate(cursor_description):
        bind_name = bind_names[i]
        source_lob_type = source_lob_column_types.get(str(d[0]).upper())
        metadata_dbtype = _LOB_BIND_DBTYPE_BY_DATA_TYPE.get(source_lob_type)
        if metadata_dbtype is not None:
            lob_inputsizes[bind_name] = metadata_dbtype
        elif _LOB_DBTYPES and d[1] in _LOB_DBTYPES:
            lob_inputsizes[bind_name] = d[1]
    return sql, bind_names, lob_inputsizes


def _compact_sql(sql: str, limit: int = 1200) -> str:
    compact = " ".join(str(sql).split())
    return compact if len(compact) <= limit else compact[:limit - 3] + "..."


def _quote_oracle_ident(name: str) -> str:
    escaped = str(name).upper().replace('"', '""')
    return f'"{escaped}"'


def _oracle_table_ref(schema: str, table: str) -> str:
    return f"{_quote_oracle_ident(schema)}.{_quote_oracle_ident(table)}"


def _dbtype_name(dbtype) -> str:
    return getattr(dbtype, "name", None) or str(dbtype)


def _safe_value_len(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray, memoryview)):
        return len(value)
    size = getattr(value, "size", None)
    if callable(size):
        try:
            return int(size())
        except Exception:
            return None
    return None


def _bulk_lob_batch_summary(batch: list, lob_bind_columns: dict,
                            lob_inputsizes: dict | None) -> str:
    """Summarise LOB bind values without logging actual column data."""
    if not lob_inputsizes:
        return "none"
    parts = []
    for bind_name in sorted(lob_inputsizes):
        type_counts: dict[str, int] = {}
        nulls = 0
        measured = 0
        unknown_len = 0
        max_len = None
        total_len = 0
        for row in batch:
            value = row.get(bind_name)
            type_name = type(value).__name__
            type_counts[type_name] = type_counts.get(type_name, 0) + 1
            if value is None:
                nulls += 1
                continue
            value_len = _safe_value_len(value)
            if value_len is None:
                unknown_len += 1
                continue
            measured += 1
            total_len += value_len
            max_len = value_len if max_len is None else max(max_len, value_len)
        type_summary = ",".join(
            f"{name}:{count}" for name, count in sorted(type_counts.items())
        ) or "none"
        max_text = str(max_len) if max_len is not None else "n/a"
        total_text = str(total_len) if measured else "n/a"
        unknown_text = f" unknown_len={unknown_len}" if unknown_len else ""
        parts.append(
            f"{bind_name}/{lob_bind_columns.get(bind_name, '?')} "
            f"dbtype={_dbtype_name(lob_inputsizes[bind_name])} "
            f"py_types={type_summary} nulls={nulls}/{len(batch)} "
            f"max_len={max_text} total_len={total_text}{unknown_text}"
        )
    return "; ".join(parts)


def _cursor_description_summary(description, limit: int = 2000) -> str:
    parts = []
    for col in description or []:
        name = col[0] if len(col) > 0 else "?"
        dbtype = _dbtype_name(col[1]) if len(col) > 1 else "?"
        parts.append(f"{name}:{dbtype}")
    text = ", ".join(parts)
    return text if len(text) <= limit else text[:limit - 3] + "..."


_ORACLE_PROTOCOL_ERRORS = ("ORA-03106", "ORA-03113", "ORA-03114")
_ORACLE_CONNECTION_ERRORS = (
    "DPY-1001",
    "DPY-4011",
    "ORA-03106",
    "ORA-03113",
    "ORA-03114",
    "ORA-03135",
)
_ORA00001_CONSTRAINT_RE = re.compile(
    r"unique constraint\s*\((?P<constraint>[^)]+)\)\s*violated",
    re.IGNORECASE,
)


def _is_oracle_protocol_error(exc: Exception) -> bool:
    text = str(exc)
    return any(code in text for code in _ORACLE_PROTOCOL_ERRORS)


def _is_ora00001(exc: Exception) -> bool:
    return "ORA-00001" in str(exc)


def _is_dpy_not_connected(exc: Exception) -> bool:
    text = str(exc)
    lowered = text.lower()
    return (
        any(code in text for code in _ORACLE_CONNECTION_ERRORS)
        or "not connected to database" in lowered
        or "database or network closed the connection" in lowered
    )


def _oracle_connection_error_code(exc: Exception) -> str:
    text = str(exc)
    for code in _ORACLE_CONNECTION_ERRORS:
        if code in text:
            return code
    if "not connected to database" in text.lower():
        return "DPY-1001"
    return "CONNECTION_LOST"


def _parse_ora00001_constraint(exc: Exception, default_owner: str) -> tuple[str | None, str | None]:
    match = _ORA00001_CONSTRAINT_RE.search(str(exc))
    if not match:
        return (None, None)
    raw = match.group("constraint").strip().replace('"', "")
    if "." in raw:
        owner, constraint_name = raw.split(".", 1)
    else:
        owner, constraint_name = default_owner, raw
    return (owner.upper(), constraint_name.upper())


def _conventional_insert_sql(insert_sql: str) -> str:
    return insert_sql.replace("INSERT /*+ APPEND_VALUES */", "INSERT", 1)


def _split_columns(csv_text) -> list[str]:
    if not csv_text:
        return []
    return [part.strip().upper() for part in str(csv_text).split(",") if part.strip()]


def _read_unique_definition(conn, owner: str | None, constraint_name: str | None) -> dict:
    result = {"constraint": None, "index": None, "columns": []}
    if not owner or not constraint_name:
        return result

    owner = owner.upper()
    constraint_name = constraint_name.upper()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.owner, c.constraint_name, c.constraint_type, c.status,
                   c.validated, c.index_owner, c.index_name,
                   LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) AS key_columns
            FROM   all_constraints c
            JOIN   all_cons_columns cc
              ON   cc.owner = c.owner
             AND   cc.constraint_name = c.constraint_name
            WHERE  c.owner = :owner
              AND  c.constraint_name = :constraint_name
            GROUP  BY c.owner, c.constraint_name, c.constraint_type, c.status,
                      c.validated, c.index_owner, c.index_name
        """, {"owner": owner, "constraint_name": constraint_name})
        row = cur.fetchone()

    index_owner = owner
    index_name = constraint_name
    if row:
        (
            c_owner,
            c_name,
            c_type,
            c_status,
            c_validated,
            c_index_owner,
            c_index_name,
            c_columns,
        ) = row
        key_columns = _split_columns(c_columns)
        result["constraint"] = {
            "owner": c_owner,
            "name": c_name,
            "type": c_type,
            "status": c_status,
            "validated": c_validated,
            "index_owner": c_index_owner,
            "index_name": c_index_name,
        }
        result["columns"] = key_columns
        index_owner = str(c_index_owner or owner).upper()
        index_name = str(c_index_name or constraint_name).upper()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT i.owner, i.index_name, i.uniqueness, i.status,
                   LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) AS key_columns
            FROM   all_indexes i
            JOIN   all_ind_columns ic
              ON   ic.index_owner = i.owner
             AND   ic.index_name = i.index_name
            WHERE  i.owner = :owner
              AND  i.index_name = :index_name
            GROUP  BY i.owner, i.index_name, i.uniqueness, i.status
        """, {"owner": index_owner, "index_name": index_name})
        row = cur.fetchone()
    if row:
        i_owner, i_name, uniqueness, status, i_columns = row
        index_columns = _split_columns(i_columns)
        result["index"] = {
            "owner": i_owner,
            "name": i_name,
            "uniqueness": uniqueness,
            "status": status,
        }
        if not result["columns"]:
            result["columns"] = index_columns

    return result


def _read_target_table_snapshot(conn, schema: str, table: str) -> dict:
    snapshot = {
        "has_rows": None,
        "sample_rows": None,
        "stats_num_rows": None,
        "stats_blocks": None,
        "last_analyzed": None,
    }
    with conn.cursor() as cur:
        cur.execute("""
            SELECT num_rows, blocks, last_analyzed
            FROM   all_tables
            WHERE  owner = :owner
              AND  table_name = :table_name
        """, {"owner": schema.upper(), "table_name": table.upper()})
        row = cur.fetchone()
    if row:
        snapshot["stats_num_rows"] = row[0]
        snapshot["stats_blocks"] = row[1]
        snapshot["last_analyzed"] = row[2]
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {_oracle_table_ref(schema, table)} WHERE ROWNUM <= 1")
        row = cur.fetchone()
    sample_rows = int(row[0] or 0)
    snapshot["sample_rows"] = sample_rows
    snapshot["has_rows"] = sample_rows > 0
    return snapshot


def _flashback_suffix(start_scn: int | None) -> str:
    return " AS OF SCN :p_scn" if start_scn else ""


def _rowid_params(rowid_start: str, rowid_end: str, start_scn: int | None) -> dict:
    params = {"p_start": rowid_start, "p_end": rowid_end}
    if start_scn:
        params["p_scn"] = start_scn
    return params


def _read_source_chunk_key_stats(
    conn,
    schema: str,
    table: str,
    rowid_start: str,
    rowid_end: str,
    start_scn: int | None,
    key_columns: list[str],
) -> dict:
    table_ref = _oracle_table_ref(schema, table)
    flashback = _flashback_suffix(start_scn)
    params = _rowid_params(rowid_start, rowid_end, start_scn)
    rowid_where = "ROWID BETWEEN CHARTOROWID(:p_start) AND CHARTOROWID(:p_end)"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM {table_ref}{flashback} WHERE {rowid_where}",
            params,
        )
        rows_count = int(cur.fetchone()[0] or 0)

    stats = {
        "rows": rows_count,
        "distinct_key_groups": None,
        "duplicate_key_groups": None,
    }
    if not key_columns:
        return stats

    key_expr = ", ".join(_quote_oracle_ident(col) for col in key_columns)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM ("
            f"SELECT {key_expr} FROM {table_ref}{flashback} "
            f"WHERE {rowid_where} GROUP BY {key_expr})",
            params,
        )
        stats["distinct_key_groups"] = int(cur.fetchone()[0] or 0)
        cur.execute(
            f"SELECT COUNT(*) FROM ("
            f"SELECT {key_expr} FROM {table_ref}{flashback} "
            f"WHERE {rowid_where} GROUP BY {key_expr} HAVING COUNT(*) > 1)",
            params,
        )
        stats["duplicate_key_groups"] = int(cur.fetchone()[0] or 0)
    return stats


def _hashable_diagnostic_value(value):
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    try:
        hash(value)
        return value
    except Exception:
        return repr(value)


def _batch_key_stats(batch: list, bind_names: list[str], cursor_description, key_columns: list[str]) -> dict:
    result = {
        "rows": len(batch),
        "key_columns": key_columns,
        "missing_columns": [],
        "null_key_rows": None,
        "duplicate_key_rows": None,
    }
    if not key_columns or not cursor_description:
        return result

    column_to_bind = {
        str(col[0]).upper(): bind_names[i]
        for i, col in enumerate(cursor_description)
        if i < len(bind_names)
    }
    bind_key = []
    missing = []
    for column in key_columns:
        bind_name = column_to_bind.get(column.upper())
        if bind_name:
            bind_key.append(bind_name)
        else:
            missing.append(column)
    result["missing_columns"] = missing
    if missing:
        return result

    null_key_rows = 0
    keys = []
    for row in batch:
        key = tuple(_hashable_diagnostic_value(row.get(bind_name)) for bind_name in bind_key)
        if any(row.get(bind_name) is None for bind_name in bind_key):
            null_key_rows += 1
        keys.append(key)
    result["null_key_rows"] = null_key_rows
    result["duplicate_key_rows"] = len(keys) - len(set(keys))
    return result


def _log_pg_chunk_state(pg_conn, tag: str, chunk_id: str) -> None:
    try:
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT status, retry_count, rows_loaded, worker_id,
                       claimed_at, started_at, completed_at
                FROM   migration_chunks
                WHERE  chunk_id = %s
            """, (chunk_id,))
            row = cur.fetchone()
    except Exception as exc:
        print(f"[bulk:{tag}] ORA-00001 pg_chunk_state unavailable err={type(exc).__name__}: {exc}")
        return
    if not row:
        print(f"[bulk:{tag}] ORA-00001 pg_chunk_state missing")
        return
    status, retry_count, rows_loaded, worker_id, claimed_at, started_at, completed_at = row
    print(
        f"[bulk:{tag}] ORA-00001 pg_chunk_state status={status} "
        f"retry_count={retry_count} rows_loaded={rows_loaded} worker_id={worker_id} "
        f"claimed_at={claimed_at} started_at={started_at} completed_at={completed_at}"
    )


def _connection_health(conn) -> str:
    if conn is None:
        return "present=false"
    parts = [f"present=true type={type(conn).__name__}"]
    closed = getattr(conn, "closed", None)
    if closed is not None:
        parts.append(f"closed_attr={closed}")
    ping = getattr(conn, "ping", None)
    if callable(ping):
        try:
            ping()
            parts.append("ping=ok")
            return " ".join(parts)
        except Exception as exc:
            parts.append(f"ping=failed:{type(exc).__name__}:{exc}")
            return " ".join(parts)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM dual")
            cur.fetchone()
        parts.append("select1=ok")
    except Exception as exc:
        parts.append(f"select1=failed:{type(exc).__name__}:{exc}")
    return " ".join(parts)


def _log_oracle_connection_error_diagnostics(
    *,
    tag: str,
    exc: Exception,
    chunk: dict,
    stage: str,
    src_label: str,
    dest_label: str,
    rows_loaded: int,
    batch_no: int,
    batch_rows: int,
    source_has_lob: bool,
    fetch_batch_size: int,
    fallback_mode: bool,
    last_lob_summary: str,
    src_conn,
    dst_conn,
) -> None:
    print(
        f"[bulk:{tag}] DPY-1001 diagnostics begin err={type(exc).__name__}: {exc} "
        f"stage={stage} src={src_label} dest={dest_label} "
        f"chunk_seq={chunk.get('chunk_seq')} chunk_retry={chunk.get('retry_count', 'n/a')} "
        f"chunk_rows_loaded_pg={chunk.get('rows_loaded', 'n/a')} "
        f"rows_loaded={rows_loaded} batch={batch_no} batch_rows={batch_rows} "
        f"source_has_lob={source_has_lob} fetch_batch={fetch_batch_size} "
        f"fallback_mode={fallback_mode} lob_summary={last_lob_summary}"
    )
    print(f"[bulk:{tag}] DPY-1001 source_conn {_connection_health(src_conn)}")
    print(f"[bulk:{tag}] DPY-1001 target_conn {_connection_health(dst_conn)}")


def _log_bulk_unique_violation_diagnostics(
    *,
    tag: str,
    exc: Exception,
    chunk: dict,
    pg_conn,
    src_conn,
    open_target_conn,
    dest_table: str,
    start_scn: int | None,
    batch: list,
    bind_names: list[str],
    cursor_description,
) -> None:
    chunk_id = chunk["chunk_id"]
    src_schema = chunk["source_schema"]
    src_table = chunk["source_table"]
    tgt_schema = chunk["target_schema"]
    owner, constraint_name = _parse_ora00001_constraint(exc, tgt_schema)
    print(
        f"[bulk:{tag}] ORA-00001 diagnostics begin constraint="
        f"{owner + '.' + constraint_name if owner and constraint_name else 'unknown'} "
        f"src={src_schema}.{src_table} dest={tgt_schema}.{dest_table} "
        f"chunk_seq={chunk.get('chunk_seq')} chunk_retry={chunk.get('retry_count', 'n/a')} "
        f"chunk_rows_loaded_pg={chunk.get('rows_loaded', 'n/a')} "
        f"rowid={chunk.get('rowid_start')}..{chunk.get('rowid_end')} "
        f"batch_rows={len(batch)} start_scn={start_scn or 'current'}"
    )
    _log_pg_chunk_state(pg_conn, tag, chunk_id)

    key_columns: list[str] = []
    target_conn = None
    try:
        target_conn = open_target_conn()
        definition = _read_unique_definition(target_conn, owner, constraint_name)
        key_columns = definition.get("columns") or []
        constraint = definition.get("constraint")
        if constraint:
            print(
                f"[bulk:{tag}] ORA-00001 target_constraint owner={constraint['owner']} "
                f"name={constraint['name']} type={constraint['type']} "
                f"status={constraint['status']} validated={constraint['validated']} "
                f"index={constraint.get('index_owner')}.{constraint.get('index_name')} "
                f"columns={','.join(key_columns) or 'unknown'}"
            )
        else:
            print(
                f"[bulk:{tag}] ORA-00001 target_constraint not_found "
                f"owner={owner or 'unknown'} name={constraint_name or 'unknown'}"
            )
        index = definition.get("index")
        if index:
            print(
                f"[bulk:{tag}] ORA-00001 target_index owner={index['owner']} "
                f"name={index['name']} uniqueness={index['uniqueness']} "
                f"status={index['status']} columns={','.join(key_columns) or 'unknown'}"
            )
        target_snapshot = _read_target_table_snapshot(target_conn, tgt_schema, dest_table)
        print(
            f"[bulk:{tag}] ORA-00001 target_table table={tgt_schema}.{dest_table} "
            f"has_rows={target_snapshot['has_rows']} sample_rows={target_snapshot['sample_rows']} "
            f"stats_num_rows={target_snapshot['stats_num_rows']} "
            f"stats_blocks={target_snapshot['stats_blocks']} "
            f"last_analyzed={target_snapshot['last_analyzed']}"
        )
    except Exception as diag_exc:
        print(
            f"[bulk:{tag}] ORA-00001 target_diagnostics_failed "
            f"err={type(diag_exc).__name__}: {diag_exc}"
        )
    finally:
        if target_conn is not None:
            try:
                target_conn.close()
            except Exception:
                pass

    batch_stats = _batch_key_stats(batch, bind_names, cursor_description, key_columns)
    print(
        f"[bulk:{tag}] ORA-00001 batch_key_stats rows={batch_stats['rows']} "
        f"key_columns={','.join(batch_stats['key_columns']) or 'unknown'} "
        f"missing_columns={','.join(batch_stats['missing_columns']) or 'none'} "
        f"null_key_rows={batch_stats['null_key_rows']} "
        f"duplicate_key_rows={batch_stats['duplicate_key_rows']}"
    )

    try:
        source_stats = _read_source_chunk_key_stats(
            src_conn,
            src_schema,
            src_table,
            chunk["rowid_start"],
            chunk["rowid_end"],
            start_scn,
            key_columns,
        )
        print(
            f"[bulk:{tag}] ORA-00001 source_chunk_stats table={src_schema}.{src_table} "
            f"rows={source_stats['rows']} key_columns={','.join(key_columns) or 'unknown'} "
            f"distinct_key_groups={source_stats['distinct_key_groups']} "
            f"duplicate_key_groups={source_stats['duplicate_key_groups']}"
        )
    except Exception as diag_exc:
        print(
            f"[bulk:{tag}] ORA-00001 source_diagnostics_failed "
            f"err={type(diag_exc).__name__}: {diag_exc}"
        )


def _execute_batch_no_commit(dst_conn, insert_sql: str, batch: list,
                             lob_inputsizes: dict | None = None) -> None:
    with dst_conn.cursor() as ic:
        if lob_inputsizes:
            ic.setinputsizes(**lob_inputsizes)
        ic.executemany(insert_sql, batch)


def _flush_batch(dst_conn, insert_sql: str, batch: list,
                 lob_inputsizes: dict | None = None) -> None:
    """Execute one batch insert and commit."""
    _execute_batch_no_commit(dst_conn, insert_sql, batch, lob_inputsizes)
    dst_conn.commit()


def _flush_batch_fallback(
    open_target_conn,
    insert_sql: str,
    batch: list,
    lob_inputsizes: dict | None,
    *,
    tag: str,
    batch_no: int,
    lob_summary: str,
) -> None:
    """Retry a failed bulk batch conventionally, splitting on protocol errors.

    The whole original batch is committed only after every fallback slice
    succeeds. If a protocol error breaks the connection mid-attempt, the
    attempt is rolled back/closed and retried from the beginning with a smaller
    slice size, avoiding partial commits inside the chunk.
    """
    fallback_sql = _conventional_insert_sql(insert_sql)
    slice_size = min(len(batch), BULK_FALLBACK_BATCH_SIZE)
    while True:
        print(
            f"[bulk:{tag}] fallback start reason=ORA_PROTOCOL_ERROR "
            f"batch={batch_no} rows={len(batch)} fallback_batch_rows={slice_size} "
            f"append_values=false lob_summary={lob_summary}"
        )
        conn = open_target_conn()
        try:
            for offset in range(0, len(batch), slice_size):
                _execute_batch_no_commit(
                    conn,
                    fallback_sql,
                    batch[offset:offset + slice_size],
                    lob_inputsizes,
                )
            conn.commit()
            print(
                f"[bulk:{tag}] fallback done rows={len(batch)} "
                f"fallback_batch_rows={slice_size}"
            )
            return
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            if _is_oracle_protocol_error(exc) and slice_size > 1:
                next_size = max(1, slice_size // 2)
                print(
                    f"[bulk:{tag}] fallback split after protocol error "
                    f"batch={batch_no} fallback_batch_rows={slice_size}->{next_size} "
                    f"err={type(exc).__name__}: {exc}"
                )
                slice_size = next_size
                continue
            print(
                f"[bulk:{tag}] fallback failed batch={batch_no} "
                f"fallback_batch_rows={slice_size} err={type(exc).__name__}: {exc}"
            )
            if _is_dpy_not_connected(exc):
                print(f"[bulk:{tag}] DPY-1001 fallback_conn {_connection_health(conn)}")
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass


def _process_bulk_chunk(chunk: dict, pg_conn, configs: dict) -> None:
    """BULK/DIRECT: read from source, write to stage or target.

    Historical no-CDC migrations normally leave start_scn NULL and read current data.
    If start_scn is explicitly set, use a flashback query AS OF SCN.
    """
    chunk_id    = chunk["chunk_id"]
    src_schema  = chunk["source_schema"]
    src_table   = chunk["source_table"]
    tgt_schema  = chunk["target_schema"]
    strategy    = chunk.get("strategy", "CDC_STAGE")
    uses_stage  = strategy.endswith("_STAGE")
    dest_table  = chunk["stage_table"] if uses_stage else chunk["target_table"]
    raw_scn     = chunk.get("start_scn")
    start_scn   = int(raw_scn) if raw_scn else None
    rowid_start = chunk["rowid_start"]
    rowid_end   = chunk["rowid_end"]
    migration_id = str(chunk.get("migration_id") or "")
    tag = chunk_id[:8]
    src_label = f"{src_schema}.{src_table}"
    dest_label = f"{tgt_schema}.{dest_table}"
    source_has_lob = False
    fetch_batch_size = BULK_BATCH_SIZE
    stage = "start"
    batch_no = 0
    batch_rows = 0
    batch = []
    insert_sql = ""
    bind_names: list[str] = []
    cursor_description = []
    last_lob_summary = "none"
    lob_bind_columns: dict = {}
    lob_inputsizes: dict = {}
    source_lob_column_types: dict = {}
    fallback_mode = False

    src_conn = db.open_oracle(
        chunk["source_connection_id"],
        configs,
        _session_context(
            f"bulk-src {chunk_id[:8]}",
            migration_id=migration_id,
            chunk_id=chunk_id,
            table=f"{src_schema}.{src_table}",
        ),
    )

    def _open_target_conn(action: str = ""):
        return db.open_oracle(
            chunk["target_connection_id"],
            configs,
            _session_context(
                action or f"bulk-dst {chunk_id[:8]}",
                migration_id=migration_id,
                chunk_id=chunk_id,
                table=f"{tgt_schema}.{dest_table}",
            ),
        )

    dst_conn = _open_target_conn()
    rows_loaded = 0
    try:
        print(
            f"[bulk:{tag}] start seq={chunk.get('chunk_seq')} strategy={strategy} "
            f"src={src_label} dest={dest_label} uses_stage={uses_stage} "
            f"start_scn={start_scn or 'current'} rowid={rowid_start}..{rowid_end} "
            f"bulk_batch={BULK_BATCH_SIZE} lob_batch={BULK_LOB_BATCH_SIZE} "
            f"chunk_retry={chunk.get('retry_count', 'n/a')} "
            f"chunk_rows_loaded_pg={chunk.get('rows_loaded', 'n/a')}"
        )
        stage = "chunk-active"
        _ensure_chunk_active(pg_conn, chunk_id)
        stage = "lob-probe"
        source_lob_column_types = _source_lob_column_types(src_conn, src_schema, src_table)
        source_has_lob = bool(source_lob_column_types)
        fetch_batch_size = (
            min(BULK_BATCH_SIZE, BULK_LOB_BATCH_SIZE)
            if source_has_lob else BULK_BATCH_SIZE
        )
        if source_has_lob:
            lob_columns_text = ",".join(
                f"{name}:{data_type}"
                for name, data_type in sorted(source_lob_column_types.items())
            )
            batch_note = (
                f"bulk batch capped at {fetch_batch_size}"
                if fetch_batch_size < BULK_BATCH_SIZE
                else f"bulk batch={fetch_batch_size}"
            )
            print(
                f"[worker] chunk {chunk_id}: source has LOB columns; "
                f"{batch_note}; lob_columns={lob_columns_text}"
            )
        with src_conn.cursor() as cur:
            cur.arraysize = fetch_batch_size
            cur.prefetchrows = fetch_batch_size if source_has_lob else fetch_batch_size + 1
            print(
                f"[bulk:{tag}] source_query mode="
                f"{'flashback' if start_scn else 'current'} "
                f"arraysize={cur.arraysize} prefetchrows={cur.prefetchrows}"
            )
            if start_scn:
                # Consistent snapshot via flashback query
                stage = "source-query flashback"
                cur.execute(
                    f'SELECT * FROM "{src_schema.upper()}"."{src_table.upper()}" '
                    f'AS OF SCN :p_scn '
                    f'WHERE ROWID BETWEEN CHARTOROWID(:p_start) AND CHARTOROWID(:p_end)',
                    {"p_scn": start_scn, "p_start": rowid_start, "p_end": rowid_end},
                )
            else:
                # Read current data without flashback.
                stage = "source-query current"
                cur.execute(
                    f'SELECT * FROM "{src_schema.upper()}"."{src_table.upper()}" '
                    f'WHERE ROWID BETWEEN CHARTOROWID(:p_start) AND CHARTOROWID(:p_end)',
                    {"p_start": rowid_start, "p_end": rowid_end},
                )
            stage = "build-insert"
            cursor_description = list(cur.description or [])
            insert_sql, bind_names, lob_inputsizes = _build_insert(
                cursor_description, tgt_schema, dest_table, source_lob_column_types)
            lob_bind_columns = {
                bind_names[i]: col[0]
                for i, col in enumerate(cursor_description)
                if bind_names[i] in lob_inputsizes
            }
            print(
                f"[bulk:{tag}] source_describe columns={len(cursor_description)} "
                f"desc={_cursor_description_summary(cursor_description)}"
            )
            print(
                f"[bulk:{tag}] target_insert bind_count={len(bind_names)} "
                f"lob_binds={','.join(sorted(lob_inputsizes)) or 'none'} "
                f"sql={_compact_sql(insert_sql)}"
            )
            while True:
                stage = f"fetch batch={batch_no + 1}"
                _ensure_chunk_active(pg_conn, chunk_id)
                rows = cur.fetchmany(fetch_batch_size)
                if not rows:
                    break
                _ensure_chunk_active(pg_conn, chunk_id)
                batch_no += 1
                batch_rows = len(rows)
                batch = [dict(zip(bind_names, row)) for row in rows]
                last_lob_summary = _bulk_lob_batch_summary(
                    batch, lob_bind_columns, lob_inputsizes
                )
                if lob_inputsizes:
                    print(
                        f"[bulk:{tag}] flush batch={batch_no} rows={batch_rows} "
                        f"rows_loaded={rows_loaded} lob_summary={last_lob_summary}"
                    )
                stage = f"flush batch={batch_no}"
                if fallback_mode:
                    _flush_batch_fallback(
                        lambda: _open_target_conn(f"bulk-fallback {chunk_id[:8]}"),
                        insert_sql,
                        batch,
                        lob_inputsizes,
                        tag=tag,
                        batch_no=batch_no,
                        lob_summary=last_lob_summary,
                    )
                else:
                    try:
                        _flush_batch(dst_conn, insert_sql, batch, lob_inputsizes)
                    except Exception as exc:
                        if not _is_oracle_protocol_error(exc):
                            raise
                        print(
                            f"[bulk:{tag}] protocol error on fast flush; "
                            f"switching chunk to fallback mode err={type(exc).__name__}: {exc}"
                        )
                        try:
                            dst_conn.rollback()
                        except Exception:
                            pass
                        try:
                            dst_conn.close()
                        except Exception:
                            pass
                        dst_conn = None
                        fallback_mode = True
                        _flush_batch_fallback(
                            lambda: _open_target_conn(f"bulk-fallback {chunk_id[:8]}"),
                            insert_sql,
                            batch,
                            lob_inputsizes,
                            tag=tag,
                            batch_no=batch_no,
                            lob_summary=last_lob_summary,
                        )
                rows_loaded += len(batch)
                db.update_chunk_progress(pg_conn, chunk_id, rows_loaded)
                print(f"  → {rows_loaded} rows")
    except ChunkCancelled:
        for c in (src_conn, dst_conn):
            try: c.rollback()
            except Exception: pass
        raise
    except Exception as exc:
        if _is_dpy_not_connected(exc):
            _log_oracle_connection_error_diagnostics(
                tag=tag,
                exc=exc,
                chunk=chunk,
                stage=stage,
                src_label=src_label,
                dest_label=dest_label,
                rows_loaded=rows_loaded,
                batch_no=batch_no,
                batch_rows=batch_rows,
                source_has_lob=source_has_lob,
                fetch_batch_size=fetch_batch_size,
                fallback_mode=fallback_mode,
                last_lob_summary=last_lob_summary,
                src_conn=src_conn,
                dst_conn=dst_conn,
            )
        if _is_ora00001(exc):
            _log_bulk_unique_violation_diagnostics(
                tag=tag,
                exc=exc,
                chunk=chunk,
                pg_conn=pg_conn,
                src_conn=src_conn,
                open_target_conn=lambda: _open_target_conn(f"bulk-ora00001 {chunk_id[:8]}"),
                dest_table=dest_table,
                start_scn=start_scn,
                batch=batch,
                bind_names=bind_names,
                cursor_description=cursor_description,
            )
        print(
            f"[bulk:{tag}] FAILED stage={stage} err={type(exc).__name__}: {exc} "
            f"src={src_label} dest={dest_label} strategy={strategy} "
            f"rows_loaded={rows_loaded} batch={batch_no} batch_rows={batch_rows} "
            f"source_has_lob={source_has_lob} fetch_batch={fetch_batch_size} "
            f"start_scn={start_scn or 'current'} rowid={rowid_start}..{rowid_end} "
            f"fallback_mode={fallback_mode} lob_summary={last_lob_summary}"
        )
        if insert_sql:
            print(f"[bulk:{tag}] failed_insert_sql={_compact_sql(insert_sql)}")
        raise
    finally:
        for c in (src_conn, dst_conn):
            try: c.close()
            except Exception: pass
    return rows_loaded


def _process_baseline_chunk(chunk: dict, pg_conn, configs: dict) -> int:
    """BASELINE: INSERT INTO target SELECT * FROM stage WHERE ROWID BETWEEN ... (no SCN).

    Oracle commit and PG progress update are intentionally separated:
    if PG fails after Oracle commits, the exception propagates without
    attempting to rollback an already-committed Oracle transaction.
    If the chunk is retried after a partial commit, ORA-00001 is caught
    and treated as idempotent success (rows already present from prior attempt).
    """
    chunk_id    = chunk["chunk_id"]
    tgt_schema  = chunk["target_schema"]
    tgt_table   = chunk["target_table"]
    stg_table   = chunk["stage_table"]
    rowid_start = chunk["rowid_start"]
    rowid_end   = chunk["rowid_end"]
    migration_id = str(chunk.get("migration_id") or "")

    tgt = f'"{tgt_schema.upper()}"."{tgt_table.upper()}"'
    stg = f'"{tgt_schema.upper()}"."{stg_table.upper()}"'

    dst_conn = db.open_oracle(
        chunk["target_connection_id"],
        configs,
        _session_context(
            f"baseline {chunk_id[:8]}",
            migration_id=migration_id,
            chunk_id=chunk_id,
            table=f"{tgt_schema}.{tgt_table}",
        ),
    )
    rows_loaded = 0
    try:
        _ensure_chunk_active(pg_conn, chunk_id)
        with dst_conn.cursor() as cur:
            # Conventional (non-direct-path) INSERT so multiple workers write into
            # the target table CONCURRENTLY.
            #
            # A direct-path insert (/*+ APPEND */ or parallel DML) takes an
            # EXCLUSIVE table lock (TM enqueue, mode 6) held until COMMIT, which
            # serialises every baseline chunk across workers and silently defeats
            # baseline_parallel_degree — only one chunk inserts at a time even
            # though several are CLAIMED.  See services/oracle_baseline.py for the
            # same reasoning.
            #
            # PARALLEL stays on the SELECT only: a parallel *query* speeds the
            # stage scan and does NOT take the exclusive insert lock, so it is
            # safe under concurrent writers.
            #
            # Trade-off: conventional INSERT generates redo even though
            # baseline_publishing set the table NOLOGGING (NOLOGGING only affects
            # direct-path operations).  Concurrency across workers is preferred
            # over the redo savings of a single serialised direct-path load.
            cur.execute(
                f'INSERT INTO {tgt} tgt '
                f'SELECT /*+ PARALLEL(stg, DEFAULT) */ * FROM {stg} stg '
                f'WHERE stg.ROWID BETWEEN CHARTOROWID(:rs) AND CHARTOROWID(:re)',
                {"rs": rowid_start, "re": rowid_end},
            )
            rows_loaded = cur.rowcount if cur.rowcount >= 0 else 0
        dst_conn.commit()
    except ChunkCancelled:
        try:
            dst_conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        if "ORA-00001" in str(exc):
            # A retry after Oracle committed but before PG progress was stored can hit
            # ORA-00001. Treat it as idempotent success only if target already
            # contains at least this chunk's row count. Otherwise this is likely
            # duplicate data in stage, and marking DONE would hide data loss.
            try:
                with dst_conn.cursor() as cur:
                    cur.execute(
                        f'SELECT COUNT(*) FROM {stg} stg '
                        f'WHERE stg.ROWID BETWEEN CHARTOROWID(:rs) AND CHARTOROWID(:re)',
                        {"rs": rowid_start, "re": rowid_end},
                    )
                    stage_rows = cur.fetchone()[0]
                    cur.execute(f"SELECT COUNT(*) FROM {tgt}")
                    target_rows = cur.fetchone()[0]
                if stage_rows > 0 and target_rows >= stage_rows:
                    rows_loaded = stage_rows
                    print(
                        f"[worker] chunk {chunk_id} ORA-00001 — target already has "
                        f"{target_rows} rows, treating retry as done ({rows_loaded} rows)"
                    )
                else:
                    try:
                        dst_conn.rollback()
                    except Exception:
                        pass
                    raise RuntimeError(
                        f"Baseline insert hit ORA-00001, but target has {target_rows} rows "
                        f"for stage chunk size {stage_rows}; likely duplicate rows in stage. "
                        "Restart after clearing stage."
                    ) from exc
            except Exception as verify_exc:
                if isinstance(verify_exc, RuntimeError):
                    raise
                try:
                    dst_conn.rollback()
                except Exception:
                    pass
                raise RuntimeError(
                    f"Baseline insert hit ORA-00001 and idempotency check failed: {verify_exc}"
                ) from exc
        else:
            try: dst_conn.rollback()
            except Exception: pass
            raise
    finally:
        try: dst_conn.close()
        except Exception: pass

    # Oracle committed — update PG outside try/except so a PG failure here
    # does NOT trigger rollback of the already-committed Oracle transaction.
    db.update_chunk_progress(pg_conn, chunk_id, rows_loaded)
    return rows_loaded


def process_chunk(chunk: dict, pg_conn, configs: dict) -> None:
    chunk_id   = chunk["chunk_id"]
    chunk_type = chunk.get("chunk_type", "BULK")

    print(
        f"[worker] chunk {chunk_id} seq={chunk['chunk_seq']}"
        f" type={chunk_type} retry={chunk.get('retry_count', 'n/a')}"
        f" rows_loaded_pg={chunk.get('rows_loaded', 'n/a')}"
        f" ({chunk['rowid_start']}..{chunk['rowid_end']})"
    )

    try:
        if chunk_type == "BASELINE":
            rows_loaded = _process_baseline_chunk(chunk, pg_conn, configs)
        else:
            rows_loaded = _process_bulk_chunk(chunk, pg_conn, configs)

        db.complete_chunk(pg_conn, chunk_id, rows_loaded)
        print(f"[worker] chunk {chunk_id} DONE — {rows_loaded} rows")

    except ChunkCancelled as exc:
        err = str(exc)
        print(f"[worker] chunk {chunk_id} CANCELLED: {err}")
        try:
            db.cancel_chunk(pg_conn, chunk_id, err)
        except Exception:
            pass

    except Exception as exc:
        err = str(exc)
        print(f"[worker] chunk {chunk_id} FAILED: {err}")
        try:
            if "ORA-01555" in err:
                print(f"[worker] chunk {chunk_id} permanent fail (ORA-01555)")
                db.fail_chunk_permanent(pg_conn, chunk_id, err)
            else:
                db.fail_chunk(pg_conn, chunk_id, err)
        except Exception:
            pass
        raise


def bulk_loop() -> None:
    """Main thread: continuously claim + process bulk/baseline chunks."""
    print(f"[bulk] loop started (worker_id={WORKER_ID})")
    pg = db.get_pg_conn_with_retry()
    try:
        while True:
            try:
                chunk = db.claim_chunk(pg)
                if chunk is None:
                    time.sleep(BULK_POLL_INTERVAL)
                    continue
                configs = db.load_configs(pg)
                process_chunk(chunk, pg, configs)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[bulk] error: {exc}")
                try:
                    pg.close()
                except Exception:
                    pass
                pg = db.get_pg_conn()
                time.sleep(BULK_POLL_INTERVAL)
    finally:
        try:
            pg.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# CDC APPLY
# ══════════════════════════════════════════════════════════════════════════════

def _calc_lag(consumer, consumer_group: str, bootstrap: list) -> tuple[int, dict]:
    """Возвращает (total_lag, by_partition={"topic-partition": lag})."""
    from kafka import KafkaAdminClient
    total_lag = 0
    by_partition: dict = {}
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            request_timeout_ms=5_000,
            connections_max_idle_ms=8_000,
        )
        try:
            offsets   = admin.list_consumer_group_offsets(consumer_group)
            committed = {tp: om.offset for tp, om in offsets.items() if om.offset >= 0}
            partitions = set(committed.keys())
            try:
                partitions.update(consumer.assignment() or [])
            except Exception:
                pass

            if not partitions:
                return 0, {}

            end = consumer.end_offsets(list(partitions))
            missing_position = [tp for tp in partitions if tp not in committed]
            try:
                beginning = consumer.beginning_offsets(missing_position) if missing_position else {}
            except Exception:
                beginning = {}

            for tp in partitions:
                if tp in committed:
                    off = committed[tp]
                else:
                    try:
                        off = consumer.position(tp)
                    except Exception:
                        off = None
                    if off is None or off < 0:
                        off = beginning.get(tp, end.get(tp, 0))
                lag = max(0, end.get(tp, off) - off)
                total_lag += lag
                by_partition[f"{tp.topic}-{tp.partition}"] = lag
        finally:
            admin.close()
    except Exception as exc:
        print(f"[cdc] lag error: {exc}")
    return total_lag, by_partition


def _merge_upsert(conn, schema: str, table: str, row: dict, key_cols: list) -> None:
    columns  = list(row.keys())
    non_keys = [c for c in columns if c not in key_cols]
    key_conds   = " AND ".join(f't."{c}" = s."{c}"' for c in key_cols)
    src_cols    = ", ".join(f':{i + 1} "{c}"' for i, c in enumerate(columns))
    insert_cols = ", ".join(f'"{c}"' for c in columns)
    insert_vals = ", ".join(f's."{c}"' for c in columns)
    update_set  = ", ".join(f't."{c}" = s."{c}"' for c in non_keys)

    sql = (
        f'MERGE INTO "{schema.upper()}"."{table.upper()}" t '
        f'USING (SELECT {src_cols} FROM DUAL) s ON ({key_conds}) '
        + (f'WHEN MATCHED THEN UPDATE SET {update_set} ' if update_set else '')
        + f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
    )
    with conn.cursor() as cur:
        cur.execute(sql, list(row.values()))


def _delete_row(conn, schema: str, table: str, key_data: dict, key_cols: list) -> None:
    where  = " AND ".join(f'"{c}" = :{i + 1}' for i, c in enumerate(key_cols))
    with conn.cursor() as cur:
        cur.execute(
            f'DELETE FROM "{schema.upper()}"."{table.upper()}" WHERE {where}',
            [key_data.get(c) for c in key_cols],
        )


def _require_event_keys(row: dict, key_cols: list, op: str) -> None:
    missing = [col for col in key_cols if col not in row]
    if missing:
        raise ValueError(
            f"CDC {op} event is missing key columns: {', '.join(missing)}"
        )


def _apply_event(oracle_conn, event: dict, target_schema: str,
                 target_table: str, key_cols: list) -> None:
    op = event["op"]
    if op in ("c", "r", "u"):
        row = event.get("after") or {}
        if row:
            _require_event_keys(row, key_cols, op)
            _merge_upsert(oracle_conn, target_schema, target_table, row, key_cols)
    elif op == "d":
        row = event.get("before") or {}
        if row and key_cols:
            _require_event_keys(row, key_cols, op)
            _delete_row(oracle_conn, target_schema, target_table, row, key_cols)


# Fields injected by ExtractNewRecordState — not real table columns
_DEBEZIUM_META = frozenset({"__op", "__table", "__source_ts_ms", "__deleted",
                            "__db", "__schema"})

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# Debezium / Kafka Connect logical type names for temporal fields
# (appear as "name" inside the field schema)
_LTYPE_TS_MS = frozenset({          # int64 → milliseconds since epoch → datetime
    "org.apache.kafka.connect.data.Timestamp",
    "io.debezium.time.Timestamp",
})
_LTYPE_TS_US = frozenset({          # int64 → microseconds since epoch → datetime
    "io.debezium.time.MicroTimestamp",
})
_LTYPE_TS_NS = frozenset({          # int64 → nanoseconds since epoch → datetime
    "io.debezium.time.NanoTimestamp",
})
_LTYPE_DATE = frozenset({           # int32 → days since epoch → date
    "org.apache.kafka.connect.data.Date",
    "io.debezium.time.Date",
})
_LTYPE_TIME_MS = frozenset({        # int32/int64 → time-of-day ms → skip (pass as-is)
    "org.apache.kafka.connect.data.Time",
    "io.debezium.time.Time",
})


def _build_type_map(schema: dict) -> dict:
    """
    Walk a Debezium struct schema and return {field_name: logical_type_name}
    for every field that has a logical type.  Fields without a 'name' (logical
    type) are omitted — they need no special coercion.
    """
    if not schema or schema.get("type") != "struct":
        return {}
    return {
        f["field"]: f["name"]
        for f in schema.get("fields", [])
        if f.get("field") and f.get("name")
    }


def _coerce_row(row: dict, type_map: dict) -> dict:
    """
    Convert Debezium-encoded temporal values to Python datetime / date objects
    so that oracledb accepts them for Oracle DATE / TIMESTAMP columns.
    Values that are None, or whose logical type is unknown, pass through unchanged.
    """
    if not type_map:
        return row
    out: dict = {}
    for col, val in row.items():
        ltype = type_map.get(col)
        if val is None or ltype is None:
            out[col] = val
        elif ltype in _LTYPE_TS_MS:
            out[col] = _EPOCH + timedelta(milliseconds=int(val))
        elif ltype in _LTYPE_TS_US:
            out[col] = _EPOCH + timedelta(microseconds=int(val))
        elif ltype in _LTYPE_TS_NS:
            out[col] = _EPOCH + timedelta(microseconds=int(val) // 1000)
        elif ltype in _LTYPE_DATE:
            out[col] = (_EPOCH + timedelta(days=int(val))).date()
        else:
            out[col] = val
    return out


def _parse_debezium(msg_value: bytes) -> Optional[dict]:
    """
    Parse a message produced by the ExtractNewRecordState (unwrap) transform.

    With value.converter.schemas.enable=true the wire format is:
        { "schema": {...}, "payload": { col1: v1, ..., "__op": "c"|"u"|"d"|"r",
                                        "__deleted": "true"|"false" } }

    Temporal columns (Oracle DATE / TIMESTAMP) arrive as integers
    (ms / µs / days since epoch).  _coerce_row converts them to Python
    datetime / date so that oracledb accepts them without ORA-00932.

    Delete events use delete.handling.mode=rewrite: the value payload contains
    the *before* record with __deleted=true and __op=d.
    Tombstones (null value) are skipped.
    """
    if msg_value is None:
        return None  # tombstone — skip
    try:
        envelope = json.loads(msg_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return None

    # schemas.enable=true → {schema:…, payload:{…}}; =false → payload IS the root
    has_schema = isinstance(envelope, dict) and "schema" in envelope
    payload    = envelope.get("payload") if has_schema else envelope
    if not isinstance(payload, dict):
        return None

    type_map = _build_type_map(envelope.get("schema") if has_schema else {})

    op      = payload.get("__op")
    deleted = payload.get("__deleted") == "true"

    # Strip meta fields → actual table columns only
    row = _coerce_row(
        {k: v for k, v in payload.items() if k not in _DEBEZIUM_META},
        type_map,
    )

    if deleted or op == "d":
        return {"op": "d", "before": row, "after": None}
    if op in ("c", "r", "u"):
        return {"op": op, "before": None, "after": row}
    return None


# Oracle binary column types. Under binary.handling.mode=base64 their values
# arrive as base64 strings and must be decoded back to bytes before binding.
_BINARY_COL_TYPES = frozenset({"BLOB", "RAW", "LONG RAW"})


def _binary_columns(conn, schema: str, table: str) -> set:
    """Names of binary (BLOB/RAW) columns on the target table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
        """, {"s": schema.upper(), "t": table.upper()})
        return {r[0] for r in cur.fetchall() if r[1] in _BINARY_COL_TYPES}


def _sanitize_lob_values(row: dict, binary_cols: set) -> dict:
    """Prepare a Debezium row for binding into Oracle.

    1. DROP columns equal to the unavailable-value placeholder: an UPDATE that
       did not touch a LOB column carries this sentinel, not the value. Dropping
       the column omits it from the MERGE so the existing LOB is preserved
       instead of being overwritten with the sentinel string.
    2. base64-decode binary (BLOB/RAW) columns back to bytes.
    """
    if not row:
        return row
    out: dict = {}
    for col, val in row.items():
        if isinstance(val, str) and val in (
            CDC_UNAVAILABLE_PLACEHOLDER, _CDC_UNAVAILABLE_PLACEHOLDER_B64,
        ):
            continue  # unchanged LOB — leave target value untouched
        if col in binary_cols and isinstance(val, str):
            try:
                out[col] = base64.b64decode(val)
            except Exception:
                out[col] = val
            continue
        out[col] = val
    return out


def cdc_thread(migration: dict, stop_event: threading.Event) -> None:
    """Long-running thread: apply Debezium events for one migration."""
    migration_id = migration["migration_id"]
    tag = migration_id[:8]
    pg = None
    consumer = None
    oracle_conn = None

    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        detail = f"kafka-python not installed: {exc}"
        print(f"[cdc:{tag}] {detail}")
        try:
            pg = db.get_pg_conn()
            db.fail_cdc_migration(pg, migration_id, "CDC_WORKER_START_FAILED", detail)
        except Exception as fail_exc:
            print(f"[cdc:{tag}] mark-FAILED error: {fail_exc}")
        finally:
            try:
                pg.close()
            except Exception:
                pass
        return

    target_schema  = migration["target_schema"]
    target_table   = migration["target_table"]
    source_schema  = migration["source_schema"]
    source_table   = migration["source_table"]
    topic_prefix   = migration["topic_prefix"]
    consumer_group = migration["consumer_group"]
    key_cols       = json.loads(migration.get("effective_key_columns_json") or "[]")
    topic          = db.cdc_topic_name(topic_prefix, source_schema, source_table)
    tag            = migration_id[:8]

    print(f"[cdc:{tag}] thread started  topic={topic}  group={consumer_group}")

    try:
        pg      = db.get_pg_conn()
        configs = db.load_configs(pg)
        kafka_cfg   = configs.get("kafka") or {}
        bootstrap   = [s.strip() for s in (kafka_cfg.get("bootstrap_servers") or "localhost:9092").split(",")]

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id=consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=None,
            consumer_timeout_ms=CDC_POLL_MS,
            max_poll_records=CDC_BATCH_SIZE,
        )
        oracle_conn     = db.open_oracle(
            migration["target_connection_id"],
            configs,
            _session_context(
                f"cdc {migration_id[:8]}",
                migration_id=migration_id,
                table=f"{target_schema}.{target_table}",
            ),
        )
        binary_cols     = _binary_columns(oracle_conn, target_schema, target_table)
        last_checkin_ts = time.time()
        rows_applied    = 0
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        print(f"[cdc:{tag}] startup fatal: {detail}")
        try:
            if pg is None:
                pg = db.get_pg_conn()
            db.fail_cdc_migration(pg, migration_id, "CDC_WORKER_START_FAILED", detail)
        except Exception as fail_exc:
            print(f"[cdc:{tag}] mark-FAILED error: {fail_exc}")
        finally:
            for obj in (oracle_conn, consumer, pg):
                try:
                    obj.close()
                except Exception:
                    pass
        return

    # Защита от бесконечного reclaim-цикла на «ядовитом» событии: если один и
    # тот же offset падает >=POISON_THRESHOLD раз, миграция помечается FAILED.
    fail_offsets: dict = {}        # (topic, partition, offset) → count
    POISON_THRESHOLD = 3
    consecutive_poll_errors = 0

    try:
        while not stop_event.is_set():
            try:
                if not db.cdc_migration_should_run(pg, migration_id):
                    print(f"[cdc:{tag}] stopping: migration paused or no longer active")
                    return
            except Exception as exc:
                print(f"[cdc:{tag}] pause check error: {exc}")

            try:
                raw_msgs = consumer.poll(timeout_ms=CDC_POLL_MS)
            except Exception as exc:
                consecutive_poll_errors += 1
                detail = f"{type(exc).__name__}: {exc}"
                print(
                    f"[cdc:{tag}] poll error "
                    f"({consecutive_poll_errors}/{CDC_POLL_ERROR_THRESHOLD}): {detail}"
                )
                try:
                    db.cdc_heartbeat(pg, migration_id)
                except Exception as heartbeat_exc:
                    print(f"[cdc:{tag}] heartbeat after poll error failed: {heartbeat_exc}")
                if consecutive_poll_errors >= CDC_POLL_ERROR_THRESHOLD:
                    try:
                        db.fail_cdc_migration(pg, migration_id, "CDC_POLL_FAILED", detail)
                    except Exception as fail_exc:
                        print(f"[cdc:{tag}] mark-FAILED error: {fail_exc}")
                    return
                time.sleep(5)
                continue
            consecutive_poll_errors = 0

            polled = sum(len(v) for v in raw_msgs.values())
            applied_in_batch = 0
            batch_failed     = False

            for _tp, messages in raw_msgs.items():
                for msg in messages:
                    event = _parse_debezium(msg.value)
                    if event is None:
                        continue
                    if event.get("after"):
                        event["after"] = _sanitize_lob_values(event["after"], binary_cols)
                    try:
                        _apply_event(oracle_conn, event,
                                     target_schema, target_table, key_cols)
                        rows_applied     += 1
                        applied_in_batch += 1
                    except Exception as exc:
                        offset_key = (msg.topic, msg.partition, msg.offset)
                        cnt = fail_offsets.get(offset_key, 0) + 1
                        fail_offsets[offset_key] = cnt
                        err = f"{type(exc).__name__}: {exc}"
                        print(f"[cdc:{tag}] apply error at {msg.topic}:{msg.partition}@{msg.offset} "
                              f"(attempt {cnt}/{POISON_THRESHOLD}) event_op={event.get('op')} "
                              f"keys={key_cols} err={err}")
                        try:
                            oracle_conn.rollback()
                        except Exception:
                            pass
                        batch_failed = True

                        if cnt >= POISON_THRESHOLD:
                            detail = (f"poison event {msg.topic}:{msg.partition}@{msg.offset} "
                                      f"op={event.get('op')} key_cols={key_cols} → {err}")
                            print(f"[cdc:{tag}] FATAL {detail}; transitioning migration to FAILED")
                            try:
                                db.fail_cdc_migration(pg, migration_id,
                                                      "CDC_APPLY_FAILED", detail)
                            except Exception as fail_exc:
                                print(f"[cdc:{tag}] mark-FAILED error: {fail_exc}")
                            return
                        break        # bail out of inner-loop, do not commit this batch
                if batch_failed:
                    break

                # Весь батч по этому TP применился — коммитим Oracle и Kafka.
                # Не используем consumer.commit() без аргументов на success-path,
                # т.к. мы итерируемся по TP и хотим коммитить только этот TP.
                oracle_conn.commit()
                consumer.commit()

            if polled > 0:
                print(f"[cdc:{tag}] poll: applied={applied_in_batch}/{polled}"
                      f"{' (rollback)' if batch_failed else ''}")

            # Periodic checkin
            if time.time() - last_checkin_ts >= CDC_CHECKIN_SEC:
                try:
                    total_lag, by_partition = _calc_lag(consumer, consumer_group, bootstrap)
                    if by_partition:
                        db.cdc_checkin(pg, migration_id, total_lag, rows_applied,
                                       lag_by_partition=by_partition)
                        if total_lag == 0:
                            db.trigger_lag_zero(pg, migration_id)
                        print(f"[cdc:{tag}] checkin lag={total_lag} rows={rows_applied} parts={len(by_partition)}")
                    else:
                        db.cdc_heartbeat(pg, migration_id)
                        print(f"[cdc:{tag}] heartbeat only: no Kafka partitions assigned yet")
                except Exception as exc:
                    print(f"[cdc:{tag}] checkin error: {exc}")
                    # Reconnect pg on error
                    try:
                        pg.close()
                    except Exception:
                        pass
                    pg = db.get_pg_conn()
                last_checkin_ts = time.time()

    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        print(f"[cdc:{tag}] fatal: {detail}")
        try:
            if pg is None:
                pg = db.get_pg_conn()
            db.fail_cdc_migration(pg, migration_id, "CDC_WORKER_FATAL", detail)
        except Exception as fail_exc:
            print(f"[cdc:{tag}] mark-FAILED error: {fail_exc}")
    finally:
        print(f"[cdc:{tag}] thread stopping")
        try:
            if pg is not None:
                db.release_cdc_migration(pg, migration_id)
        except Exception as exc:
            print(f"[cdc:{tag}] release heartbeat error: {exc}")
        for obj in (oracle_conn, consumer):
            try:
                obj.close()
            except Exception:
                pass
        try:
            pg.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# CDC MANAGER — background thread that claims CDC migrations
# ══════════════════════════════════════════════════════════════════════════════

def worker_heartbeat_loop(stop_event: threading.Event) -> None:
    capabilities = [
        "bulk", "baseline", "cdc", "compare", "ddl",
        "ddl:index", "ddl:view", "ddl:mview", "ddl:code",
        "ddl:trigger", "ddl:sequence", "ddl:synonym", "ddl:dblink", "ddl:job",
        "target:index",
    ]
    pg = db.get_pg_conn_with_retry()
    try:
        while not stop_event.is_set():
            try:
                db.worker_heartbeat(pg, role="universal", capabilities=capabilities)
            except Exception as exc:
                print(f"[worker] heartbeat error: {exc}")
                try:
                    pg.close()
                except Exception:
                    pass
                pg = db.get_pg_conn_with_retry()
            stop_event.wait(WORKER_HEARTBEAT_SEC)
    finally:
        try:
            pg.close()
        except Exception:
            pass

def cdc_manager(stop_event: threading.Event) -> None:
    """
    Periodically scans for CDC migrations needing a worker and starts
    a cdc_thread for each one.  Reaps finished threads.
    """
    # migration_id → (thread, stop_event)
    active: dict = {}
    print(f"[cdc_manager] started (scan every {CDC_SCAN_INTERVAL}s)")

    pg = db.get_pg_conn_with_retry()
    try:
        while not stop_event.is_set():
            # Reap dead threads
            for mid in list(active):
                t, _ = active[mid]
                if not t.is_alive():
                    print(f"[cdc_manager] thread {mid[:8]} exited")
                    del active[mid]

            # Claim a new CDC migration if available
            try:
                migration = db.claim_cdc_migration(pg, exclude_migration_ids=list(active))
                if migration:
                    mid = migration["migration_id"]
                    if mid not in active:
                        se = threading.Event()
                        t  = threading.Thread(
                            target=cdc_thread,
                            args=(migration, se),
                            name=f"cdc-{mid[:8]}",
                            daemon=True,
                        )
                        t.start()
                        active[mid] = (t, se)
                        print(f"[cdc_manager] started thread for {mid[:8]}")
            except Exception as exc:
                print(f"[cdc_manager] scan error: {exc}")
                try:
                    pg.close()
                except Exception:
                    pass
                pg = db.get_pg_conn()

            time.sleep(CDC_SCAN_INTERVAL)
    finally:
        # Signal all CDC threads to stop
        for mid, (t, se) in active.items():
            se.set()
        for mid, (t, _) in active.items():
            t.join(timeout=10)
            print(f"[cdc_manager] joined {mid[:8]}")
        try:
            pg.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# DATA COMPARISON
# ══════════════════════════════════════════════════════════════════════════════

# Column types to skip in hash computation (must match routes/data_compare.py)
_CMP_SKIP_TYPES = frozenset({
    "BFILE", "LONG", "LONG RAW",
    "XMLTYPE", "SDO_GEOMETRY", "ANYDATA", "URITYPE",
})

# LOBs ARE compared, by length — catches placeholder corruption / base64 bloat /
# truncation without DBMS_CRYPTO. Must match routes/data_compare.py (_LOB_TYPES).
_CMP_LOB_TYPES = frozenset({"BLOB", "CLOB", "NCLOB"})


def _cmp_col_expr(col_name: str, col_type: str) -> str:
    q = f'"{col_name}"'
    if col_type in _CMP_LOB_TYPES:
        return f"NVL(TO_CHAR(DBMS_LOB.GETLENGTH({q})), CHR(0))"
    if col_type == "DATE":
        return f"NVL(TO_CHAR({q}, 'YYYY-MM-DD HH24:MI:SS'), CHR(0))"
    if col_type.startswith("TIMESTAMP"):
        return f"NVL(TO_CHAR({q}, 'YYYY-MM-DD HH24:MI:SS.FF6'), CHR(0))"
    return f"NVL(TO_CHAR({q}), CHR(0))"


def _get_comparable_columns(conn, schema: str, table: str) -> list:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        return [
            {"name": r[0], "data_type": r[1]}
            for r in cur.fetchall()
            if r[1] not in _CMP_SKIP_TYPES
        ]


def process_compare_chunk(chunk: dict, pg_conn, configs: dict) -> None:
    """Process one data-compare chunk: COUNT(*) + SUM(hash) for a ROWID range."""
    chunk_id    = chunk["chunk_id"]
    side        = chunk["side"]
    schema      = chunk["schema"]
    table       = chunk["table"]
    rowid_start = chunk["rowid_start"]
    rowid_end   = chunk["rowid_end"]

    print(f"[compare] chunk {chunk_id[:8]} side={side} seq={chunk['chunk_seq']}"
          f" ({rowid_start}..{rowid_end})")

    try:
        ora_conn = db.open_oracle(
            chunk["connection_id"],
            configs,
            _session_context(
                f"compare {chunk_id[:8]}",
                chunk_id=chunk_id,
                table=f"{schema}.{table}",
            ),
        )
        try:
            columns = _get_comparable_columns(ora_conn, schema, table)
            hash_parts = [f"ORA_HASH({_cmp_col_expr(c['name'], c['data_type'])})"
                          for c in columns]
            row_hash = " + ".join(hash_parts) if hash_parts else "0"

            sql = (
                f'SELECT COUNT(*) AS cnt, SUM({row_hash}) AS hash_sum '
                f'FROM "{schema}"."{table}" '
                f'WHERE ROWID BETWEEN CHARTOROWID(:rs) AND CHARTOROWID(:re)'
            )
            with ora_conn.cursor() as cur:
                cur.execute(sql, {"rs": rowid_start, "re": rowid_end})
                row_count, hash_sum = cur.fetchone()
        finally:
            try:
                ora_conn.close()
            except Exception:
                pass

        task_id = db.complete_compare_chunk(pg_conn, chunk_id, row_count or 0, hash_sum)
        print(f"[compare] chunk {chunk_id[:8]} DONE — {row_count} rows")

        # Try to aggregate (check if all chunks are done)
        if task_id:
            _try_aggregate_from_worker(task_id, pg_conn)

    except Exception as exc:
        err = str(exc)
        print(f"[compare] chunk {chunk_id[:8]} FAILED: {err}")
        try:
            db.fail_compare_chunk(pg_conn, chunk_id, err)
        except Exception:
            pass
        raise


def _try_aggregate_from_worker(task_id: str, pg_conn) -> None:
    """Worker-side aggregation: check if all chunks done and finalize task."""
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT status, chunks_total FROM data_compare_tasks "
                "WHERE task_id = %s FOR UPDATE", (task_id,))
            row = cur.fetchone()
            if not row or row[0] != 'RUNNING':
                pg_conn.rollback()
                return

            # Count statuses
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'DONE')    AS done,
                    COUNT(*) FILTER (WHERE status = 'FAILED')  AS failed,
                    COUNT(*) FILTER (WHERE status IN ('PENDING', 'CLAIMED')) AS active
                FROM data_compare_chunks
                WHERE task_id = %s
            """, (task_id,))
            done, failed, active = cur.fetchone()

            cur.execute(
                "UPDATE data_compare_tasks SET chunks_done = %s WHERE task_id = %s",
                (done, task_id))

            if active > 0:
                pg_conn.commit()
                return

            if failed > 0:
                cur.execute("""
                    UPDATE data_compare_tasks
                    SET    status = 'FAILED', error_text = %s, completed_at = NOW()
                    WHERE  task_id = %s
                """, (f"{failed} chunk(s) failed", task_id))
                pg_conn.commit()
                return

            # All done — aggregate per side
            cur.execute("""
                SELECT side, SUM(COALESCE(row_count, 0)), SUM(COALESCE(hash_sum, 0))
                FROM   data_compare_chunks
                WHERE  task_id = %s AND status = 'DONE'
                GROUP BY side
            """, (task_id,))
            side_data = {}
            for side, rc, hs in cur.fetchall():
                side_data[side] = {"count": int(rc), "hash": hs}

            src = side_data.get("source", {"count": 0, "hash": 0})
            tgt = side_data.get("target", {"count": 0, "hash": 0})

            counts_match = src["count"] == tgt["count"]
            hash_match = src["hash"] == tgt["hash"]

            cur.execute("""
                UPDATE data_compare_tasks
                SET    status = 'DONE',
                       source_count = %s, target_count = %s,
                       source_hash  = %s, target_hash  = %s,
                       counts_match = %s, hash_match   = %s,
                       chunks_done  = %s,
                       completed_at = NOW()
                WHERE  task_id = %s
            """, (src["count"], tgt["count"],
                  str(src["hash"]), str(tgt["hash"]),
                  counts_match, hash_match, done, task_id))
        pg_conn.commit()

        print(f"[compare] task {task_id[:8]} DONE: "
              f"src={src['count']} tgt={tgt['count']} "
              f"counts={'OK' if counts_match else 'MISMATCH'} "
              f"hash={'OK' if hash_match else 'MISMATCH'}")

    except Exception as exc:
        print(f"[compare] aggregate error: {exc}")
        try:
            pg_conn.rollback()
        except Exception:
            pass


def compare_loop(stop_event: threading.Event) -> None:
    """Background thread: continuously claim + process data-compare chunks."""
    print(f"[compare] loop started (worker_id={WORKER_ID})")
    pg = db.get_pg_conn_with_retry()
    try:
        while not stop_event.is_set():
            try:
                chunk = db.claim_compare_chunk(pg)
                if chunk is None:
                    time.sleep(CMP_POLL_INTERVAL)
                    continue
                configs = db.load_configs(pg)
                process_compare_chunk(chunk, pg, configs)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[compare] error: {exc}")
                try:
                    pg.close()
                except Exception:
                    pass
                pg = db.get_pg_conn()
                time.sleep(CMP_POLL_INTERVAL)
    finally:
        try:
            pg.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

# =============================================================================
# TARGET INDEX JOBS
# =============================================================================

def _is_temporary_table(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT temporary
            FROM   all_tables
            WHERE  owner = :s AND table_name = :t
        """, {"s": schema.upper(), "t": table.upper()})
        row = cur.fetchone()
    return bool(row and str(row[0]).upper() == "Y")


def _referencing_foreign_keys(conn, schema: str, table: str, status: str) -> list[tuple[str, str, str]]:
    s = schema.upper()
    t = table.upper()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT fk.owner, fk.table_name, fk.constraint_name
            FROM   all_constraints fk
            JOIN   all_constraints pk
                   ON pk.owner = fk.r_owner
                  AND pk.constraint_name = fk.r_constraint_name
            WHERE  fk.constraint_type = 'R'
              AND  pk.owner = :s
              AND  pk.table_name = :t
              AND  fk.status = :status
            ORDER BY fk.owner, fk.table_name, fk.constraint_name
        """, {"s": s, "t": t, "status": status.upper()})
    return [(row[0], row[1], row[2]) for row in cur.fetchall()]


def _target_index_log(tag: str | None, message: str) -> None:
    if tag:
        print(f"[target_index] {tag} {message}", flush=True)


def _enable_target_indexes(
    conn,
    schema: str,
    table: str,
    *,
    tag: str | None = None,
    parallel_rebuild_override: bool | None = None,
) -> dict:
    s = schema.upper()
    t = table.upper()
    stage = "temporary-probe"
    try:
        _target_index_log(tag, f"stage={stage} table={s}.{t}")
        is_temp = _is_temporary_table(conn, s, t)
        # PARALLEL only for real tables (GTT indexes can't be NOLOGGING/parallel here).
        parallel_enabled = (
            INDEX_REBUILD_PARALLEL
            if parallel_rebuild_override is None
            else bool(parallel_rebuild_override)
        )
        parallel_rebuild = parallel_enabled and not is_temp
        if is_temp:
            rebuild_clause = "REBUILD"
        elif parallel_rebuild:
            rebuild_clause = "REBUILD NOLOGGING PARALLEL"
        else:
            rebuild_clause = "REBUILD NOLOGGING"
        _target_index_log(
            tag,
            f"table temporary={is_temp} parallel_rebuild={parallel_rebuild} "
            f"parallel_override={parallel_rebuild_override} rebuild_clause={rebuild_clause}",
        )
        enabled = {"indexes": [], "constraints": [], "fk_novalidate": [], "referencing_fk_novalidate": []}
        errors = {"indexes": [], "constraints": []}
        # FK enable failures are NOT fatal: enabling an FK (even NOVALIDATE) requires
        # the referenced parent's PK/UK to be ENABLED. When the parent table is still
        # mid-migration (its PK temporarily disabled by baseline), the FK can't enable
        # yet. We collect those here and let the parent's own INDEXES_ENABLING pass (or
        # the orchestrator FK-reconcile) enable them once the parent is ready, instead
        # of failing the whole migration.
        deferred_fk: list[dict] = []

        with conn.cursor() as cur:
            stage = "alter-table-logging"
            _target_index_log(tag, f"stage={stage} sql=ALTER TABLE {s}.{t} LOGGING")
            cur.execute(f'ALTER TABLE "{s}"."{t}" LOGGING')

            stage = "list-unusable-indexes"
            _target_index_log(tag, f"stage={stage}")
            cur.execute("""
                SELECT index_name
                FROM   all_indexes
                WHERE  owner = :s
                  AND  table_name = :t
                  AND  status = 'UNUSABLE'
                ORDER BY index_name
            """, {"s": s, "t": t})
            indexes = [row[0] for row in cur.fetchall()]
            _target_index_log(tag, f"unusable_indexes count={len(indexes)} names={','.join(indexes) or 'none'}")
            for index_name in indexes:
                stage = f"rebuild-index {index_name}"
                try:
                    _target_index_log(
                        tag,
                        f"stage={stage} sql=ALTER INDEX {s}.{index_name} {rebuild_clause}",
                    )
                    cur.execute(f'ALTER INDEX "{s}"."{index_name}" {rebuild_clause}')
                    if parallel_rebuild:
                        # Reset the degree so later DML/queries don't silently go
                        # parallel just because we rebuilt with PARALLEL.
                        try:
                            _target_index_log(tag, f"stage=noparallel-reset index={index_name}")
                            cur.execute(f'ALTER INDEX "{s}"."{index_name}" NOPARALLEL')
                        except Exception as np_exc:
                            print(
                                f"[target_index] {s}.{index_name} NOPARALLEL reset failed: {np_exc}",
                                flush=True,
                            )
                            if _is_dpy_not_connected(np_exc):
                                raise
                    enabled["indexes"].append(index_name)
                    _target_index_log(tag, f"index {index_name} rebuilt")
                except Exception as exc:
                    _target_index_log(tag, f"index {index_name} failed err={type(exc).__name__}: {exc}")
                    if _is_dpy_not_connected(exc):
                        raise
                    errors["indexes"].append({"name": index_name, "error": str(exc)})

            stage = "list-disabled-constraints"
            _target_index_log(tag, f"stage={stage}")
            cur.execute("""
                SELECT constraint_name, constraint_type
                FROM   all_constraints
                WHERE  owner = :s
                  AND  table_name = :t
                  AND  status = 'DISABLED'
                  AND  constraint_type IN ('P','U','R','C')
                ORDER BY constraint_type, constraint_name
            """, {"s": s, "t": t})
            constraints = [(row[0], row[1]) for row in cur.fetchall()]
            constraint_summary = ",".join(f"{name}:{ctype}" for name, ctype in constraints) or "none"
            _target_index_log(tag, f"disabled_constraints count={len(constraints)} names={constraint_summary}")
            for constraint_name, constraint_type in constraints:
                stage = f"enable-constraint {constraint_name}"
                try:
                    if constraint_type == "R":
                        _target_index_log(
                            tag,
                            f"stage={stage} type=R mode=NOVALIDATE",
                        )
                        cur.execute(
                            f'ALTER TABLE "{s}"."{t}" ENABLE NOVALIDATE CONSTRAINT "{constraint_name}"'
                        )
                        enabled["fk_novalidate"].append(constraint_name)
                    else:
                        _target_index_log(
                            tag,
                            f"stage={stage} type={constraint_type} mode=VALIDATE",
                        )
                        cur.execute(
                            f'ALTER TABLE "{s}"."{t}" ENABLE CONSTRAINT "{constraint_name}"'
                        )
                        enabled["constraints"].append(constraint_name)
                except Exception as exc:
                    _target_index_log(
                        tag,
                        f"constraint {constraint_name} failed err={type(exc).__name__}: {exc}",
                    )
                    if _is_dpy_not_connected(exc):
                        raise
                    if constraint_type == "R":
                        # FK failure → defer (parent PK may not be enabled yet).
                        deferred_fk.append({"name": constraint_name, "error": str(exc)})
                    else:
                        # PK/UK/CHECK failure → fatal.
                        errors["constraints"].append({"name": constraint_name, "error": str(exc)})

            stage = "list-referencing-fk"
            _target_index_log(tag, f"stage={stage}")
            referencing = _referencing_foreign_keys(conn, s, t, "DISABLED")
            _target_index_log(tag, f"referencing_fk count={len(referencing)}")
            for owner, child_table, constraint_name in referencing:
                display_name = f"{owner}.{child_table}.{constraint_name}"
                stage = f"enable-referencing-fk {display_name}"
                try:
                    _target_index_log(tag, f"stage={stage} mode=NOVALIDATE")
                    cur.execute(
                        f'ALTER TABLE "{owner}"."{child_table}" ENABLE NOVALIDATE CONSTRAINT "{constraint_name}"'
                    )
                    enabled["referencing_fk_novalidate"].append(display_name)
                except Exception as exc:
                    _target_index_log(
                        tag,
                        f"referencing_fk {display_name} failed err={type(exc).__name__}: {exc}",
                    )
                    if _is_dpy_not_connected(exc):
                        raise
                    # Referencing FK failure → defer, never fatal.
                    deferred_fk.append({"name": display_name, "error": str(exc)})

        stage = "commit"
        _target_index_log(tag, f"stage={stage}")
        conn.commit()
        return {"enabled": enabled, "deferred_fk": deferred_fk, "errors": errors}
    except Exception as exc:
        _target_index_log(tag, f"FAILED stage={stage} err={type(exc).__name__}: {exc}")
        raise


def _log_target_index_failure_diagnostics(
    *,
    tag: str,
    exc: Exception,
    job: dict,
    target_connection_id: str,
    stage: str,
    ora_conn,
) -> None:
    print(
        f"[target_index] {tag} diagnostics err={type(exc).__name__}: {exc} "
        f"stage={stage} job_id={job.get('job_id')} migration_id={job.get('migration_id')} "
        f"target_connection_id={target_connection_id} table={job.get('target_schema')}.{job.get('target_table')}",
        flush=True,
    )
    if _is_dpy_not_connected(exc):
        print(
            f"[target_index] {tag} {_oracle_connection_error_code(exc)} target_conn "
            f"{_connection_health(ora_conn)}",
            flush=True,
        )


def _close_quietly(obj) -> None:
    if obj is None:
        return
    try:
        obj.close()
    except Exception:
        pass


def process_target_index_job(job: dict, pg_conn, configs: dict) -> None:
    job_id = job["job_id"]
    target_connection_id = job["target_connection_id"] or "oracle_target"
    schema = job["target_schema"]
    table = job["target_table"]
    tag = f"{schema}.{table}/{job_id[:8]}"
    print(
        f"[target_index] {tag} started migration_id={job.get('migration_id')} "
        f"target_connection_id={target_connection_id}",
        flush=True,
    )

    ora_conn = None
    stage = "open-oracle"
    try:
        ora_conn = db.open_oracle(
            target_connection_id,
            configs,
            _session_context(
                f"target-index {job_id[:8]}",
                table=f"{schema}.{table}",
            ),
        )
        stage = "enable-target-indexes"
        try:
            result = _enable_target_indexes(ora_conn, schema, table, tag=tag)
        except Exception as exc:
            code = _oracle_connection_error_code(exc)
            serial_retry_codes = {"DPY-4011", "ORA-03106", "ORA-03113", "ORA-03114", "ORA-03135"}
            if INDEX_REBUILD_PARALLEL and code in serial_retry_codes:
                print(
                    f"[target_index] {tag} {code} during parallel index enable; "
                    "retrying once with serial rebuild",
                    flush=True,
                )
                _close_quietly(ora_conn)
                ora_conn = db.open_oracle(
                    target_connection_id,
                    configs,
                    _session_context(
                        f"target-index-serial {job_id[:8]}",
                        table=f"{schema}.{table}",
                    ),
                )
                result = _enable_target_indexes(
                    ora_conn,
                    schema,
                    table,
                    tag=tag,
                    parallel_rebuild_override=False,
                )
            else:
                raise
        err_count = len(result["errors"]["indexes"]) + len(result["errors"]["constraints"])
        if err_count:
            names = [e["name"] for e in result["errors"]["indexes"]]
            names += [e["name"] for e in result["errors"]["constraints"]]
            raise RuntimeError(
                f"Could not enable target indexes/constraints: {', '.join(names)}. {result['errors']}"
            )
        deferred = result.get("deferred_fk") or []
        if deferred:
            # Non-fatal: FKs whose parent key isn't enabled yet. The orchestrator
            # FK-reconcile (and the parent's own INDEXES_ENABLING) will enable them
            # once the parent is ready.
            print(
                f"[target_index] {tag} DONE with {len(deferred)} deferred FK(s) "
                f"(parent key not enabled yet): "
                f"{', '.join(d['name'] for d in deferred)}",
                flush=True,
            )
        stage = "complete-state"
        db.complete_target_index_job(pg_conn, job_id, result)
        print(f"[target_index] {tag} DONE", flush=True)
    except Exception as exc:
        _log_target_index_failure_diagnostics(
            tag=tag,
            exc=exc,
            job=job,
            target_connection_id=target_connection_id,
            stage=stage,
            ora_conn=ora_conn,
        )
        try:
            db.fail_target_index_job(pg_conn, job_id, f"{type(exc).__name__}: {exc}")
        except Exception as fail_exc:
            print(
                f"[target_index] {tag} mark-FAILED error: {type(fail_exc).__name__}: {fail_exc}",
                flush=True,
            )
        print(f"[target_index] {tag} FAILED: {type(exc).__name__}: {exc}", flush=True)
    finally:
        if ora_conn is not None:
            try:
                ora_conn.close()
            except Exception:
                pass


def target_index_loop(stop_event: threading.Event, poll_interval: int = 5) -> None:
    print(f"[target_index] loop started (worker_id={WORKER_ID})", flush=True)
    pg = db.get_pg_conn_with_retry()
    try:
        while not stop_event.is_set():
            try:
                job = db.claim_target_index_job(pg)
                if job is None:
                    time.sleep(poll_interval)
                    continue
                print(
                    f"[target_index] claimed job_id={job.get('job_id')} "
                    f"migration_id={job.get('migration_id')} table={job.get('target_schema')}.{job.get('target_table')} "
                    f"worker_id={WORKER_ID}",
                    flush=True,
                )
                configs = db.load_configs(pg)
                process_target_index_job(job, pg, configs)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[target_index] loop error: {type(exc).__name__}: {exc}", flush=True)
                try:
                    pg.close()
                except Exception:
                    pass
                pg = db.get_pg_conn()
                time.sleep(poll_interval)
    finally:
        try:
            pg.close()
        except Exception:
            pass

def main() -> None:
    print(f"[worker] started  worker_id={WORKER_ID}")
    print(f"[worker] state_db={db.STATE_DB_DSN}")
    print(f"[worker] bulk_batch={BULK_BATCH_SIZE}  cdc_batch={CDC_BATCH_SIZE}"
          f"  cdc_scan={CDC_SCAN_INTERVAL}s")
    print("[worker] diagnostics=target_index_v2 function_based_index_sync")

    main_stop = threading.Event()

    mgr = threading.Thread(
        target=cdc_manager, args=(main_stop,),
        name="cdc-manager", daemon=True,
    )
    mgr.start()

    hb = threading.Thread(
        target=worker_heartbeat_loop, args=(main_stop,),
        name="worker-heartbeat", daemon=True,
    )
    hb.start()

    cmp = threading.Thread(
        target=compare_loop, args=(main_stop,),
        name="compare-loop", daemon=True,
    )
    cmp.start()

    from ddl_apply_worker import ddl_apply_loop
    ddl = threading.Thread(
        target=ddl_apply_loop, args=(main_stop,),
        name="ddl-apply", daemon=True,
    )
    ddl.start()

    idx = threading.Thread(
        target=target_index_loop, args=(main_stop,),
        name="target-index", daemon=True,
    )
    idx.start()

    try:
        bulk_loop()
    except KeyboardInterrupt:
        print("[worker] shutting down…")
        main_stop.set()
        hb.join(timeout=5)
        mgr.join(timeout=15)
        cmp.join(timeout=10)
        ddl.join(timeout=10)
        idx.join(timeout=10)
        print("[worker] stopped")


if __name__ == "__main__":
    main()
