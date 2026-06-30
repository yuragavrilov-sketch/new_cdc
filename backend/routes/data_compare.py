"""Data comparison API — compare row counts and data hashes between source and target.

Full-table mode: ROWID-based chunks are created via DBMS_PARALLEL_EXECUTE
for both source and target.  Workers claim chunks and compute COUNT + HASH
per ROWID range.  Results are aggregated when all chunks complete.

Last-N mode: runs directly in a coordinator thread (bounded by N rows).
"""

import threading
import traceback

from flask import Blueprint, jsonify, request
from db.oracle_browser import get_oracle_conn
from services.oracle_scn import open_oracle_conn

bp = Blueprint("data_compare", __name__)

_state: dict = {}

# Column types to skip when computing row hashes (spatial, legacy LONG, etc.)
# NOTE: keep in sync with workers/worker.py (_CMP_SKIP_TYPES / _CMP_LOB_TYPES).
_SKIP_TYPES = frozenset({
    "BFILE", "LONG", "LONG RAW",
    "XMLTYPE", "SDO_GEOMETRY", "ANYDATA", "URITYPE",
})

# LOBs that ARE compared — by length (see col_expr). A full-content hash needs
# DBMS_CRYPTO privileges and is expensive; length already catches the realistic
# corruption modes: unavailable-value placeholder, base64 bloat, truncation.
_LOB_TYPES = frozenset({"BLOB", "CLOB", "NCLOB"})


def init(get_conn_fn, load_configs_fn, broadcast_fn):
    _state["get_conn"] = get_conn_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn


# ---------------------------------------------------------------------------
# Shared hash helpers (also used by workers — importable)
# ---------------------------------------------------------------------------

def col_expr(col_name: str, col_type: str) -> str:
    """Build NVL(TO_CHAR(...), CHR(0)) expression for a single column."""
    q = f'"{col_name}"'
    if col_type in _LOB_TYPES:
        # LOBs can't be TO_CHAR'd / compared directly — hash their length.
        return f"NVL(TO_CHAR(DBMS_LOB.GETLENGTH({q})), CHR(0))"
    if col_type == "DATE":
        return f"NVL(TO_CHAR({q}, 'YYYY-MM-DD HH24:MI:SS'), CHR(0))"
    if col_type.startswith("TIMESTAMP"):
        return f"NVL(TO_CHAR({q}, 'YYYY-MM-DD HH24:MI:SS.FF6'), CHR(0))"
    return f"NVL(TO_CHAR({q}), CHR(0))"


def get_comparable_columns(conn, schema: str, table: str) -> list[dict]:
    """Return columns suitable for hash comparison (name + data_type)."""
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
            if r[1] not in _SKIP_TYPES
        ]


def build_hash_expr(columns: list[dict]) -> str:
    """Build a per-row hash expression: SUM of ORA_HASH of each column."""
    parts = [f"ORA_HASH({col_expr(c['name'], c['data_type'])})" for c in columns]
    return " + ".join(parts) if parts else "0"


# ---------------------------------------------------------------------------
# Chunk creation (DBMS_PARALLEL_EXECUTE)
# ---------------------------------------------------------------------------

def _create_rowid_chunks(ora_cfg: dict, schema: str, table: str,
                         chunk_size: int, task_id: str) -> list[tuple[str, str]]:
    """Use DBMS_PARALLEL_EXECUTE to partition a table by ROWID.
    Returns [(rowid_start, rowid_end), ...].
    """
    task_name = f"CMP_{task_id.replace('-', '')[:30]}"
    conn = open_oracle_conn(ora_cfg)
    try:
        # Drop stale task
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    BEGIN
                      DBMS_PARALLEL_EXECUTE.DROP_TASK(task_name => :tn);
                    EXCEPTION WHEN OTHERS THEN NULL;
                    END;
                """, {"tn": task_name})
        except Exception:
            pass

        with conn.cursor() as cur:
            cur.execute("""
                BEGIN
                  DBMS_PARALLEL_EXECUTE.CREATE_TASK(task_name => :tn);
                  DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(
                    task_name   => :tn,
                    table_owner => :owner,
                    table_name  => :tbl,
                    by_row      => TRUE,
                    chunk_size  => :cs
                  );
                END;
            """, {
                "tn":    task_name,
                "owner": schema.upper(),
                "tbl":   table.upper(),
                "cs":    chunk_size,
            })
            cur.execute("""
                SELECT start_rowid, end_rowid
                FROM   user_parallel_execute_chunks
                WHERE  task_name = :tn
                ORDER BY chunk_id
            """, {"tn": task_name})
            rows = [(str(r[0]), str(r[1])) for r in cur.fetchall()]

        # Cleanup
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    BEGIN
                      DBMS_PARALLEL_EXECUTE.DROP_TASK(task_name => :tn);
                    EXCEPTION WHEN OTHERS THEN NULL;
                    END;
                """, {"tn": task_name})
        except Exception:
            pass

        return rows
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Background: create chunks for full-table comparison
# ---------------------------------------------------------------------------

def _create_chunks_and_start(task_id: str, configs: dict,
                             src_schema: str, src_table: str,
                             tgt_schema: str, tgt_table: str,
                             chunk_size: int):
    """Daemon thread: create ROWID chunks for source + target, then set RUNNING."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE data_compare_tasks SET status = 'CHUNKING', started_at = NOW() "
                "WHERE task_id = %s", (task_id,))
        conn.commit()

        _state["broadcast"]({"type": "data_compare", "task_id": task_id, "status": "CHUNKING"})

        src_cfg = configs.get("oracle_source", {})
        tgt_cfg = configs.get("oracle_target", {})

        # Chunk source
        src_chunks = _create_rowid_chunks(src_cfg, src_schema, src_table, chunk_size, task_id + "S")
        # Chunk target
        tgt_chunks = _create_rowid_chunks(tgt_cfg, tgt_schema, tgt_table, chunk_size, task_id + "T")

        total = len(src_chunks) + len(tgt_chunks)

        # Bulk-insert chunks
        with conn.cursor() as cur:
            for i, (rs, re) in enumerate(src_chunks):
                cur.execute("""
                    INSERT INTO data_compare_chunks
                        (task_id, side, chunk_seq, rowid_start, rowid_end)
                    VALUES (%s, 'source', %s, %s, %s)
                """, (task_id, i, rs, re))

            for i, (rs, re) in enumerate(tgt_chunks):
                cur.execute("""
                    INSERT INTO data_compare_chunks
                        (task_id, side, chunk_seq, rowid_start, rowid_end)
                    VALUES (%s, 'target', %s, %s, %s)
                """, (task_id, i, rs, re))

            cur.execute("""
                UPDATE data_compare_tasks
                SET    status = 'RUNNING', chunks_total = %s
                WHERE  task_id = %s
            """, (total, task_id))
        conn.commit()

        print(f"[data_compare] task {task_id[:8]} chunked: "
              f"{len(src_chunks)} src + {len(tgt_chunks)} tgt = {total}")
        _state["broadcast"]({"type": "data_compare", "task_id": task_id, "status": "RUNNING"})

        # Пустая таблица: 0 чанков → воркеры не запускаются → try_aggregate
        # никто не дёргает и task висит в RUNNING вечно. Финализируем сразу:
        # обе стороны пустые, counts/hash совпадают по определению.
        if total == 0:
            try_aggregate(task_id)

    except Exception as exc:
        traceback.print_exc()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE data_compare_tasks SET status = 'FAILED', "
                    "error_text = %s, completed_at = NOW() WHERE task_id = %s",
                    (str(exc)[:2000], task_id))
            conn.commit()
        except Exception:
            pass
        _state["broadcast"]({"type": "data_compare", "task_id": task_id, "status": "FAILED"})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Background: direct comparison for last_n mode (no chunks)
# ---------------------------------------------------------------------------

def _run_last_n_comparison(task_id: str, configs: dict,
                           src_schema: str, src_table: str,
                           tgt_schema: str, tgt_table: str,
                           last_n: int, order_column: str):
    """Background thread: run last-N comparison directly (no chunking needed)."""
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE data_compare_tasks SET status = 'RUNNING', started_at = NOW() "
                "WHERE task_id = %s", (task_id,))
        conn.commit()

        src_conn = get_oracle_conn("source", configs)
        tgt_conn = get_oracle_conn("target", configs)
        try:
            src_cols = get_comparable_columns(src_conn, src_schema, src_table)
            tgt_cols = get_comparable_columns(tgt_conn, tgt_schema, tgt_table)
            tgt_names = {c["name"] for c in tgt_cols}
            common_cols = [c for c in src_cols if c["name"] in tgt_names]

            hash_expr = build_hash_expr(common_cols)
            n = int(last_n)

            def _last_n_sql(schema, table):
                return (
                    f'SELECT COUNT(*) AS cnt, SUM({hash_expr}) AS hash_sum FROM ('
                    f'SELECT * FROM "{schema}"."{table}" '
                    f'ORDER BY "{order_column}" DESC '
                    f'FETCH FIRST {n} ROWS ONLY)'
                )

            with src_conn.cursor() as cur:
                cur.execute(_last_n_sql(src_schema, src_table))
                src_cnt, src_hash = cur.fetchone()

            with tgt_conn.cursor() as cur:
                cur.execute(_last_n_sql(tgt_schema, tgt_table))
                tgt_cnt, tgt_hash = cur.fetchone()
        finally:
            src_conn.close()
            tgt_conn.close()

        counts_match = src_cnt == tgt_cnt
        hash_match = src_hash == tgt_hash

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE data_compare_tasks
                SET    status = 'DONE',
                       source_count = %s, target_count = %s,
                       source_hash  = %s, target_hash  = %s,
                       counts_match = %s, hash_match   = %s,
                       completed_at = NOW()
                WHERE  task_id = %s
            """, (src_cnt, tgt_cnt,
                  str(src_hash) if src_hash else None,
                  str(tgt_hash) if tgt_hash else None,
                  counts_match, hash_match, task_id))
        conn.commit()
        _state["broadcast"]({"type": "data_compare", "task_id": task_id, "status": "DONE"})

    except Exception as exc:
        traceback.print_exc()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE data_compare_tasks SET status = 'FAILED', "
                    "error_text = %s, completed_at = NOW() WHERE task_id = %s",
                    (str(exc)[:2000], task_id))
            conn.commit()
        except Exception:
            pass
        _state["broadcast"]({"type": "data_compare", "task_id": task_id, "status": "FAILED"})
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Aggregation: called when all chunks are DONE
# ---------------------------------------------------------------------------

def try_aggregate(task_id: str) -> None:
    """Check if all chunks are DONE for a task and aggregate results.
    Called by the worker after completing each compare chunk.
    Safe to call from any process (coordinator or worker).
    """
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            # Lock the task row
            cur.execute(
                "SELECT status, chunks_total FROM data_compare_tasks "
                "WHERE task_id = %s FOR UPDATE", (task_id,))
            row = cur.fetchone()
            if not row or row[0] != 'RUNNING':
                conn.rollback()
                return

            chunks_total = row[1]

            # Count chunk statuses
            cur.execute("""
                SELECT status, COUNT(*), SUM(COALESCE(row_count, 0)), SUM(COALESCE(hash_sum, 0))
                FROM   data_compare_chunks
                WHERE  task_id = %s
                GROUP BY status
            """, (task_id,))
            stats = {}
            for status, cnt, rc, hs in cur.fetchall():
                stats[status] = {"count": cnt, "row_count": rc, "hash_sum": hs}

            done_count = stats.get("DONE", {}).get("count", 0)
            failed_count = stats.get("FAILED", {}).get("count", 0)
            pending_count = stats.get("PENDING", {}).get("count", 0)
            claimed_count = stats.get("CLAIMED", {}).get("count", 0)

            # Update chunks_done
            cur.execute(
                "UPDATE data_compare_tasks SET chunks_done = %s WHERE task_id = %s",
                (done_count, task_id))

            # Not all done yet
            if pending_count + claimed_count > 0:
                conn.commit()
                return

            # All chunks finished (done or failed)
            if failed_count > 0:
                cur.execute("""
                    UPDATE data_compare_tasks
                    SET    status = 'FAILED', error_text = %s, completed_at = NOW()
                    WHERE  task_id = %s
                """, (f"{failed_count} chunk(s) failed", task_id))
                conn.commit()
                return

            # All DONE — aggregate per side
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
                  counts_match, hash_match, done_count, task_id))
        conn.commit()

        print(f"[data_compare] task {task_id[:8]} DONE: "
              f"src={src['count']} tgt={tgt['count']} "
              f"counts={'OK' if counts_match else 'MISMATCH'} "
              f"hash={'OK' if hash_match else 'MISMATCH'}")
        _state["broadcast"]({"type": "data_compare", "task_id": task_id, "status": "DONE"})

    except Exception as exc:
        print(f"[data_compare] aggregate error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@bp.get("/api/data-compare/tasks")
def list_tasks():
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM data_compare_tasks ORDER BY created_at DESC LIMIT 100"
            )
            from db.state_db import row_to_dict
            return jsonify([row_to_dict(cur, r) for r in cur.fetchall()])
    finally:
        conn.close()


@bp.post("/api/data-compare/run")
def run_compare():
    data = request.json or {}
    src_schema   = data.get("source_schema", "").strip().upper()
    src_table    = data.get("source_table", "").strip().upper()
    tgt_schema   = data.get("target_schema", "").strip().upper()
    tgt_table    = data.get("target_table", "").strip().upper()
    mode         = data.get("compare_mode", "full")
    last_n       = data.get("last_n")
    order_column = data.get("order_column", "").strip().upper() or None
    chunk_size   = int(data.get("chunk_size", 100_000))

    if not all([src_schema, src_table, tgt_schema, tgt_table]):
        return jsonify({"error": "source_schema, source_table, target_schema, target_table required"}), 400
    if mode not in ("full", "last_n"):
        return jsonify({"error": "compare_mode must be 'full' or 'last_n'"}), 400
    if mode == "last_n":
        if not last_n or not order_column:
            return jsonify({"error": "last_n and order_column required for last_n mode"}), 400
        last_n = int(last_n)

    configs = _state["load_configs"]()

    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_compare_tasks
                    (source_schema, source_table, target_schema, target_table,
                     compare_mode, last_n, order_column, chunk_size, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
                RETURNING task_id
            """, (src_schema, src_table, tgt_schema, tgt_table,
                  mode, last_n, order_column, chunk_size))
            task_id = str(cur.fetchone()[0])
        conn.commit()
    finally:
        conn.close()

    if mode == "full":
        # Chunk the tables and let workers process
        threading.Thread(
            target=_create_chunks_and_start,
            args=(task_id, configs, src_schema, src_table, tgt_schema, tgt_table, chunk_size),
            daemon=True,
            name=f"cmp-chunk-{task_id[:8]}",
        ).start()
    else:
        # Last-N: run directly in coordinator thread
        threading.Thread(
            target=_run_last_n_comparison,
            args=(task_id, configs, src_schema, src_table, tgt_schema, tgt_table,
                  last_n, order_column),
            daemon=True,
            name=f"cmp-lastn-{task_id[:8]}",
        ).start()

    return jsonify({"task_id": task_id, "status": "PENDING"}), 201


@bp.get("/api/data-compare/column-diff/<task_id>")
def column_diff(task_id: str):
    """Per-column hash breakdown to show which columns differ between source and target."""
    pg = _state["get_conn"]()
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT source_schema, source_table, target_schema, target_table, "
                "compare_mode, last_n, order_column, status "
                "FROM data_compare_tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "task not found"}), 404
    finally:
        pg.close()

    src_schema, src_table, tgt_schema, tgt_table, mode, last_n, order_col, status = row
    if status != "DONE":
        return jsonify({"error": "task not DONE"}), 400

    configs = _state["load_configs"]()
    src_conn = get_oracle_conn("source", configs)
    tgt_conn = get_oracle_conn("target", configs)
    try:
        src_cols = get_comparable_columns(src_conn, src_schema, src_table)
        tgt_cols = get_comparable_columns(tgt_conn, tgt_schema, tgt_table)
        tgt_names = {c["name"] for c in tgt_cols}
        common_cols = [c for c in src_cols if c["name"] in tgt_names]

        if not common_cols:
            return jsonify({"columns": []})

        # Build per-column SUM(ORA_HASH(...)) selects
        col_parts = []
        for c in common_cols:
            expr = col_expr(c["name"], c["data_type"])
            col_parts.append(f'SUM(ORA_HASH({expr})) AS "{c["name"]}"')
        select_list = ", ".join(col_parts)

        if mode == "last_n" and last_n and order_col:
            def _sql(schema, table):
                return (
                    f"SELECT {select_list} FROM ("
                    f'SELECT * FROM "{schema}"."{table}" '
                    f'ORDER BY "{order_col}" DESC '
                    f"FETCH FIRST {int(last_n)} ROWS ONLY)"
                )
        else:
            def _sql(schema, table):
                return f'SELECT {select_list} FROM "{schema}"."{table}"'

        with src_conn.cursor() as cur:
            cur.execute(_sql(src_schema, src_table))
            src_row = cur.fetchone()

        with tgt_conn.cursor() as cur:
            cur.execute(_sql(tgt_schema, tgt_table))
            tgt_row = cur.fetchone()

        result = []
        for i, c in enumerate(common_cols):
            s_hash = src_row[i] if src_row else None
            t_hash = tgt_row[i] if tgt_row else None
            result.append({
                "column": c["name"],
                "data_type": c["data_type"],
                "source_hash": str(s_hash) if s_hash is not None else None,
                "target_hash": str(t_hash) if t_hash is not None else None,
                "match": s_hash == t_hash,
            })

        return jsonify({"columns": result})
    finally:
        src_conn.close()
        tgt_conn.close()


@bp.delete("/api/data-compare/tasks/<task_id>")
def delete_task(task_id: str):
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM data_compare_tasks WHERE task_id = %s", (task_id,))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()
