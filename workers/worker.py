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
    BULK_LOB_BATCH_SIZE Rows per INSERT batch for tables with LOB columns (default: 100)
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
try:
    import oracledb
    oracledb.defaults.fetch_lobs = False
    # LOB column DB types — used to bind bulk inserts as LOBs (streamed) instead
    # of letting oracledb preallocate a full per-batch char/byte buffer, which
    # would OOM on multi-MB LOBs at BULK_BATCH_SIZE rows.
    _LOB_DBTYPES = (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB, oracledb.DB_TYPE_BLOB)
except ImportError:
    pass

BULK_BATCH_SIZE    = int(os.environ.get("BULK_BATCH_SIZE",    20_000))
BULK_LOB_BATCH_SIZE = max(1, int(os.environ.get("BULK_LOB_BATCH_SIZE", 100)))
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

def _source_table_has_lob(conn, schema: str, table: str) -> bool:
    """Return True when source table contains LOB columns."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM   all_tab_columns
            WHERE  owner = :owner
              AND  table_name = :table_name
              AND  data_type IN ('BLOB', 'CLOB', 'NCLOB')
              AND  ROWNUM = 1
        """, {"owner": schema.upper(), "table_name": table.upper()})
        return bool(cur.fetchall())


def _build_insert(cursor_description, target_schema: str,
                  stage_table: str) -> tuple:
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
    lob_inputsizes = {
        bind_names[i]: d[1]
        for i, d in enumerate(cursor_description)
        if _LOB_DBTYPES and d[1] in _LOB_DBTYPES
    }
    return sql, bind_names, lob_inputsizes


def _compact_sql(sql: str, limit: int = 1200) -> str:
    compact = " ".join(str(sql).split())
    return compact if len(compact) <= limit else compact[:limit - 3] + "..."


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


def _flush_batch(dst_conn, insert_sql: str, batch: list,
                 lob_inputsizes: dict | None = None) -> None:
    """Execute one batch insert and commit."""
    with dst_conn.cursor() as ic:
        if lob_inputsizes:
            ic.setinputsizes(**lob_inputsizes)
        ic.executemany(insert_sql, batch)
    dst_conn.commit()


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
    insert_sql = ""
    last_lob_summary = "none"
    lob_bind_columns: dict = {}
    lob_inputsizes: dict = {}

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
    dst_conn = db.open_oracle(
        chunk["target_connection_id"],
        configs,
        _session_context(
            f"bulk-dst {chunk_id[:8]}",
            migration_id=migration_id,
            chunk_id=chunk_id,
            table=f"{tgt_schema}.{dest_table}",
        ),
    )
    rows_loaded = 0
    try:
        print(
            f"[bulk:{tag}] start seq={chunk.get('chunk_seq')} strategy={strategy} "
            f"src={src_label} dest={dest_label} uses_stage={uses_stage} "
            f"start_scn={start_scn or 'current'} rowid={rowid_start}..{rowid_end} "
            f"bulk_batch={BULK_BATCH_SIZE} lob_batch={BULK_LOB_BATCH_SIZE}"
        )
        stage = "chunk-active"
        _ensure_chunk_active(pg_conn, chunk_id)
        stage = "lob-probe"
        source_has_lob = _source_table_has_lob(src_conn, src_schema, src_table)
        fetch_batch_size = (
            min(BULK_BATCH_SIZE, BULK_LOB_BATCH_SIZE)
            if source_has_lob else BULK_BATCH_SIZE
        )
        if source_has_lob:
            print(
                f"[worker] chunk {chunk_id}: source has LOB columns; "
                f"bulk batch capped at {fetch_batch_size}"
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
            insert_sql, bind_names, lob_inputsizes = _build_insert(
                cur.description, tgt_schema, dest_table)
            lob_bind_columns = {
                bind_names[i]: col[0]
                for i, col in enumerate(cur.description or [])
                if bind_names[i] in lob_inputsizes
            }
            print(
                f"[bulk:{tag}] source_describe columns={len(cur.description or [])} "
                f"desc={_cursor_description_summary(cur.description)}"
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
                _flush_batch(dst_conn, insert_sql, batch, lob_inputsizes)
                rows_loaded += len(batch)
                db.update_chunk_progress(pg_conn, chunk_id, rows_loaded)
                print(f"  → {rows_loaded} rows")
    except ChunkCancelled:
        for c in (src_conn, dst_conn):
            try: c.rollback()
            except Exception: pass
        raise
    except Exception as exc:
        print(
            f"[bulk:{tag}] FAILED stage={stage} err={type(exc).__name__}: {exc} "
            f"src={src_label} dest={dest_label} strategy={strategy} "
            f"rows_loaded={rows_loaded} batch={batch_no} batch_rows={batch_rows} "
            f"source_has_lob={source_has_lob} fetch_batch={fetch_batch_size} "
            f"start_scn={start_scn or 'current'} rowid={rowid_start}..{rowid_end} "
            f"lob_summary={last_lob_summary}"
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

    print(f"[worker] chunk {chunk_id} seq={chunk['chunk_seq']}"
          f" type={chunk_type} ({chunk['rowid_start']}..{chunk['rowid_end']})")

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


def _enable_target_indexes(conn, schema: str, table: str) -> dict:
    s = schema.upper()
    t = table.upper()
    is_temp = _is_temporary_table(conn, s, t)
    # PARALLEL only for real tables (GTT indexes can't be NOLOGGING/parallel here).
    parallel_rebuild = INDEX_REBUILD_PARALLEL and not is_temp
    if is_temp:
        rebuild_clause = "REBUILD"
    elif parallel_rebuild:
        rebuild_clause = "REBUILD NOLOGGING PARALLEL"
    else:
        rebuild_clause = "REBUILD NOLOGGING"
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
        cur.execute(f'ALTER TABLE "{s}"."{t}" LOGGING')

        cur.execute("""
            SELECT index_name
            FROM   all_indexes
            WHERE  owner = :s
              AND  table_name = :t
              AND  status = 'UNUSABLE'
            ORDER BY index_name
        """, {"s": s, "t": t})
        indexes = [row[0] for row in cur.fetchall()]
        for index_name in indexes:
            try:
                cur.execute(f'ALTER INDEX "{s}"."{index_name}" {rebuild_clause}')
                if parallel_rebuild:
                    # Reset the degree so later DML/queries don't silently go
                    # parallel just because we rebuilt with PARALLEL.
                    try:
                        cur.execute(f'ALTER INDEX "{s}"."{index_name}" NOPARALLEL')
                    except Exception as np_exc:
                        print(f"[target_index] {s}.{index_name} NOPARALLEL reset failed: {np_exc}")
                enabled["indexes"].append(index_name)
            except Exception as exc:
                errors["indexes"].append({"name": index_name, "error": str(exc)})

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
        for constraint_name, constraint_type in constraints:
            try:
                if constraint_type == "R":
                    cur.execute(
                        f'ALTER TABLE "{s}"."{t}" ENABLE NOVALIDATE CONSTRAINT "{constraint_name}"'
                    )
                    enabled["fk_novalidate"].append(constraint_name)
                else:
                    cur.execute(
                        f'ALTER TABLE "{s}"."{t}" ENABLE CONSTRAINT "{constraint_name}"'
                    )
                    enabled["constraints"].append(constraint_name)
            except Exception as exc:
                if constraint_type == "R":
                    # FK failure → defer (parent PK may not be enabled yet).
                    deferred_fk.append({"name": constraint_name, "error": str(exc)})
                else:
                    # PK/UK/CHECK failure → fatal.
                    errors["constraints"].append({"name": constraint_name, "error": str(exc)})

        for owner, child_table, constraint_name in _referencing_foreign_keys(conn, s, t, "DISABLED"):
            display_name = f"{owner}.{child_table}.{constraint_name}"
            try:
                cur.execute(
                    f'ALTER TABLE "{owner}"."{child_table}" ENABLE NOVALIDATE CONSTRAINT "{constraint_name}"'
                )
                enabled["referencing_fk_novalidate"].append(display_name)
            except Exception as exc:
                # Referencing FK failure → defer, never fatal.
                deferred_fk.append({"name": display_name, "error": str(exc)})

    conn.commit()
    return {"enabled": enabled, "deferred_fk": deferred_fk, "errors": errors}


def process_target_index_job(job: dict, pg_conn, configs: dict) -> None:
    job_id = job["job_id"]
    target_connection_id = job["target_connection_id"] or "oracle_target"
    schema = job["target_schema"]
    table = job["target_table"]
    tag = f"{schema}.{table}/{job_id[:8]}"
    print(f"[target_index] {tag} started")

    ora_conn = None
    try:
        ora_conn = db.open_oracle(
            target_connection_id,
            configs,
            _session_context(
                f"target-index {job_id[:8]}",
                table=f"{schema}.{table}",
            ),
        )
        result = _enable_target_indexes(ora_conn, schema, table)
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
            print(f"[target_index] {tag} DONE with {len(deferred)} deferred FK(s) "
                  f"(parent key not enabled yet): "
                  f"{', '.join(d['name'] for d in deferred)}")
        db.complete_target_index_job(pg_conn, job_id, result)
        print(f"[target_index] {tag} DONE")
    except Exception as exc:
        db.fail_target_index_job(pg_conn, job_id, f"{type(exc).__name__}: {exc}")
        print(f"[target_index] {tag} FAILED: {type(exc).__name__}: {exc}")
    finally:
        if ora_conn is not None:
            try:
                ora_conn.close()
            except Exception:
                pass


def target_index_loop(stop_event: threading.Event, poll_interval: int = 5) -> None:
    print(f"[target_index] loop started (worker_id={WORKER_ID})")
    pg = db.get_pg_conn_with_retry()
    try:
        while not stop_event.is_set():
            try:
                job = db.claim_target_index_job(pg)
                if job is None:
                    time.sleep(poll_interval)
                    continue
                configs = db.load_configs(pg)
                process_target_index_job(job, pg, configs)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[target_index] loop error: {exc}")
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
