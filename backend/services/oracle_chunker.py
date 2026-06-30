"""
ROWID-based chunking via Oracle DBMS_PARALLEL_EXECUTE.

Requires the Oracle user to have EXECUTE privilege on DBMS_PARALLEL_EXECUTE.
Privilege grant (run as DBA):
  GRANT EXECUTE ON DBMS_PARALLEL_EXECUTE TO <migration_user>;
"""

from dataclasses import dataclass

from services.oracle_scn import open_oracle_conn


@dataclass
class ChunkRange:
    chunk_seq: int
    rowid_start: str
    rowid_end: str


def create_chunks(
    src_cfg: dict,
    source_schema: str,
    source_table: str,
    chunk_size: int,
    migration_id: str,
) -> list[ChunkRange]:
    """
    Partition *source_schema.source_table* into ROWID ranges of *chunk_size* rows.
    Uses DBMS_PARALLEL_EXECUTE on the source Oracle instance.
    Returns a list of ChunkRange objects sorted by chunk_seq.

    Idempotent — if the task already exists it is dropped and recreated.
    """
    # Task name: max 128 chars in Oracle; use short prefix + migration_id
    task_name = f"MIG_{migration_id.replace('-', '')[:30]}"

    conn = open_oracle_conn(src_cfg)
    try:
        # Drop stale task if it exists (ignore errors)
        _drop_task(conn, task_name)

        # Create task and partition by ROWID
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
                "owner": source_schema.upper(),
                "tbl":   source_table.upper(),
                "cs":    chunk_size,
            })

            # Read the generated chunks
            cur.execute("""
                SELECT chunk_id, start_rowid, end_rowid
                FROM   user_parallel_execute_chunks
                WHERE  task_name = :tn
                ORDER BY chunk_id
            """, {"tn": task_name})
            rows = cur.fetchall()

        chunks = [
            ChunkRange(chunk_seq=i, rowid_start=str(r[1]), rowid_end=str(r[2]))
            for i, r in enumerate(rows)
        ]
        return chunks

    finally:
        # Always clean up the Oracle task
        _drop_task(conn, task_name)
        conn.close()


def _drop_task(conn, task_name: str) -> None:
    """Drop a DBMS_PARALLEL_EXECUTE task; silently ignore if not found."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                BEGIN
                  DBMS_PARALLEL_EXECUTE.DROP_TASK(task_name => :tn);
                EXCEPTION
                  WHEN OTHERS THEN NULL;
                END;
            """, {"tn": task_name})
    except Exception:
        pass
