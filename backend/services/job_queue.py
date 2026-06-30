"""Chunk job-queue helpers used by the orchestrator and monitoring API."""

_STALE_AFTER_MINUTES = 10


def get_chunk_stats(conn, migration_id: str, chunk_type: str = "BULK") -> dict:
    """
    Return counts per status and total rows_loaded for a migration's chunks.
    {total, pending, claimed, running, done, failed, rows_loaded}
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT status, COUNT(*), COALESCE(SUM(rows_loaded), 0)
            FROM   migration_chunks
            WHERE  migration_id = %s
              AND  COALESCE(chunk_type, 'BULK') = %s
            GROUP BY status
        """, (migration_id, chunk_type))
        counts = {}
        total_rows = 0
        for status, cnt, rows in cur.fetchall():
            counts[status] = cnt
            total_rows += int(rows)
    return {
        "total":       sum(counts.values()),
        "pending":     counts.get("PENDING",  0),
        "claimed":     counts.get("CLAIMED",  0),
        "running":     counts.get("RUNNING",  0),
        "done":        counts.get("DONE",     0),
        "failed":      counts.get("FAILED",   0),
        "rows_loaded": total_rows,
    }


def reset_stale_chunks(conn, stale_minutes: int = _STALE_AFTER_MINUTES) -> int:
    """
    Reset CLAIMED/RUNNING chunks whose claimed_at is older than stale_minutes
    back to PENDING. Returns the number of chunks reset.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE migration_chunks c
            SET    status     = 'PENDING',
                   worker_id  = NULL,
                   claimed_at = NULL,
                   started_at = NULL
            WHERE  c.status IN ('CLAIMED', 'RUNNING')
              AND  c.claimed_at < NOW() - INTERVAL '%s minutes'
              AND  EXISTS (
                  SELECT 1 FROM migrations m
                  WHERE  m.migration_id = c.migration_id
                    AND  m.phase IN ('BULK_LOADING', 'BASELINE_LOADING')
              )
        """, (stale_minutes,))
        count = cur.rowcount
    conn.commit()
    if count:
        print(f"[job_queue] reset {count} stale chunk(s) to PENDING")
    return count


def save_chunks(conn, migration_id: str, chunks: list) -> None:
    """
    Bulk-insert chunk ranges into migration_chunks.
    chunks is a list of ChunkRange objects or dicts with chunk_seq,
    rowid_start, rowid_end.
    """
    with conn.cursor() as cur:
        for ch in chunks:
            seq = ch.chunk_seq if hasattr(ch, "chunk_seq") else ch["chunk_seq"]
            start = ch.rowid_start if hasattr(ch, "rowid_start") else ch["rowid_start"]
            end = ch.rowid_end if hasattr(ch, "rowid_end") else ch["rowid_end"]
            cur.execute("""
                INSERT INTO migration_chunks
                    (migration_id, chunk_seq, rowid_start, rowid_end)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (migration_id, chunk_type, chunk_seq) DO NOTHING
            """, (migration_id, seq, start, end))
    conn.commit()


def list_chunks(
    conn,
    migration_id: str,
    chunk_type: str = "BULK",
    *,
    page: int = 1,
    page_size: int = 100,
    status_filter: str = "",
) -> dict:
    """Return paginated chunks for a migration filtered by chunk_type."""
    from db.state_db import row_to_dict

    where = "migration_id = %s AND COALESCE(chunk_type, 'BULK') = %s"
    params: list = [migration_id, chunk_type]

    if status_filter:
        where += " AND status = %s"
        params.append(status_filter)

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM migration_chunks WHERE {where}", params)
        total = cur.fetchone()[0]

        offset = (max(1, page) - 1) * page_size
        cur.execute(f"""
            SELECT chunk_id, migration_id, chunk_seq,
                   rowid_start, rowid_end, status,
                   rows_loaded, worker_id,
                   claimed_at, started_at, completed_at,
                   error_text, retry_count, created_at,
                   COALESCE(chunk_type, 'BULK') AS chunk_type
            FROM   migration_chunks
            WHERE  {where}
            ORDER BY chunk_seq
            LIMIT %s OFFSET %s
        """, params + [page_size, offset])
        chunks = [row_to_dict(cur, r) for r in cur.fetchall()]

    return {"chunks": chunks, "total": total, "page": page, "page_size": page_size}
