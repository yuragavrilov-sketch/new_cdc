"""Baseline publish: TRUNCATE target + parallel INSERT SELECT from stage.

Stage table is re-chunked on the TARGET Oracle via DBMS_PARALLEL_EXECUTE,
then each chunk is inserted by a dedicated thread using its own connection.
Data never leaves Oracle — no Python data movement, no TEMP pressure.

Why no APPEND hint on parallel inserts:
  INSERT /*+ APPEND */ acquires an exclusive table lock for the whole
  transaction, so concurrent APPEND inserts serialize — defeating the
  purpose. Regular INSERT lets multiple threads write simultaneously.
"""

import concurrent.futures
import threading

from db import oracle_browser
from services.oracle_chunker import ChunkRange, create_chunks
from services.oracle_scn import open_oracle_conn


def publish_baseline(
    dst_cfg: dict,
    target_schema: str,
    target_table: str,
    stage_table: str,
    migration_id: str,
    parallel_degree: int = 4,
    chunk_size: int = 500_000,
) -> int:
    """
    1. TRUNCATE target table.
    2. Chunk the stage table on the target Oracle (DBMS_PARALLEL_EXECUTE).
    3. Insert each chunk via INSERT SELECT WHERE ROWID BETWEEN … in parallel.
    4. Return total rows inserted.

    parallel_degree  — number of concurrent Oracle connections / threads.
    chunk_size       — rows per chunk (controls per-INSERT transaction size).
    """
    tgt = f'"{target_schema.upper()}"."{target_table.upper()}"'
    stg = f'"{target_schema.upper()}"."{stage_table.upper()}"'

    # ── 1. TRUNCATE ───────────────────────────────────────────────────────────
    conn = open_oracle_conn(dst_cfg)
    try:
        truncate_result = oracle_browser.truncate_table_for_load(
            conn, target_schema, target_table,
        )
        disabled_fk = truncate_result.get("referencing_fk") or []
        disabled_keys = truncate_result.get("key_constraints") or []
        if disabled_fk:
            print(f"[baseline] disabled referencing FKs: {disabled_fk}")
        if disabled_keys:
            print(f"[baseline] disabled key constraints CASCADE: {disabled_keys}")
        print(f"[baseline] truncated {tgt}")
    finally:
        conn.close()

    # ── 2. Chunk stage table on target ────────────────────────────────────────
    # Use a "BAS_" prefixed task name so it doesn't collide with the source
    # bulk-loading task (same migration_id, possibly same Oracle instance).
    task_id = f"BAS_{migration_id}"
    chunks = create_chunks(dst_cfg, target_schema, stage_table, chunk_size, task_id)

    if not chunks:
        print("[baseline] stage table is empty — nothing to insert")
        return 0

    print(f"[baseline] {len(chunks)} chunks × ≤{chunk_size} rows, "
          f"parallel_degree={parallel_degree}")

    # ── 3. Parallel INSERT SELECT per chunk ───────────────────────────────────
    total_rows = 0
    total_lock = threading.Lock()
    errors: list[tuple[ChunkRange, Exception]] = []
    errors_lock = threading.Lock()

    def process_chunk(chunk: ChunkRange) -> int:
        chunk_conn = open_oracle_conn(dst_cfg)
        try:
            with chunk_conn.cursor() as cur:
                cur.execute(
                    f'INSERT INTO {tgt} '
                    f'SELECT * FROM {stg} '
                    f'WHERE ROWID BETWEEN CHARTOROWID(:rs) AND CHARTOROWID(:re)',
                    {"rs": chunk.rowid_start, "re": chunk.rowid_end},
                )
                n = cur.rowcount if cur.rowcount >= 0 else 0
            chunk_conn.commit()
        except Exception:
            chunk_conn.rollback()
            raise
        finally:
            chunk_conn.close()

        with total_lock:
            nonlocal total_rows
            total_rows += n
            print(f"[baseline] chunk {chunk.chunk_seq:>4} done — "
                  f"{n:>8} rows  ({total_rows} total)")
        return n

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=parallel_degree, thread_name_prefix="baseline"
    ) as pool:
        future_to_chunk = {pool.submit(process_chunk, c): c for c in chunks}
        for fut in concurrent.futures.as_completed(future_to_chunk):
            chunk = future_to_chunk[fut]
            try:
                fut.result()
            except Exception as exc:
                with errors_lock:
                    errors.append((chunk, exc))
                print(f"[baseline] chunk {chunk.chunk_seq} FAILED: {exc}")

    if errors:
        details = "; ".join(
            f"chunk {c.chunk_seq}: {e}" for c, e in errors
        )
        raise RuntimeError(
            f"Baseline publish failed: {len(errors)}/{len(chunks)} chunks errored. "
            f"Details: {details}"
        )

    print(f"[baseline] complete — {total_rows} rows inserted into {tgt}")
    return total_rows
