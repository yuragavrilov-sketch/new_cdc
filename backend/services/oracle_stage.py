"""Stage-table lifecycle on the target Oracle database."""

from services.oracle_scn import open_oracle_conn


# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

def _col_type_str(data_type: str, data_length, data_precision, data_scale,
                  char_used: str) -> str:
    """Convert all_tab_columns metadata to a DDL type string."""
    dt = data_type.upper()
    if dt == "NUMBER":
        if data_precision is not None:
            return f"NUMBER({data_precision},{data_scale or 0})"
        return "NUMBER"
    if dt in ("VARCHAR2", "NVARCHAR2"):
        unit = " CHAR" if char_used == "C" else ""
        return f"{dt}({data_length}{unit})"
    if dt in ("CHAR", "NCHAR"):
        unit = " CHAR" if char_used == "C" else ""
        return f"{dt}({data_length}{unit})"
    if dt == "FLOAT":
        return f"FLOAT({data_precision})" if data_precision else "FLOAT"
    if dt == "RAW":
        return f"RAW({data_length})"
    # DATE, CLOB, BLOB, XMLTYPE, TIMESTAMP*, INTERVAL* — keep as-is
    return data_type


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_target_table_like_source(
    src_cfg: dict,
    dst_cfg: dict,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
) -> None:
    """
    Create a regular table on the target Oracle matching the source column
    structure.  Unlike create_stage_table this creates a normal (LOGGING)
    table without a custom tablespace.

    Raises if the table already exists (caller should check first).
    """
    src_conn = open_oracle_conn(src_cfg)
    try:
        with src_conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, data_length,
                       data_precision, data_scale, nullable, char_used
                FROM   all_tab_columns
                WHERE  owner      = :s
                  AND  table_name = :t
                ORDER BY column_id
            """, {"s": source_schema.upper(), "t": source_table.upper()})
            columns = cur.fetchall()
    finally:
        src_conn.close()

    if not columns:
        raise ValueError(
            f"Исходная таблица {source_schema}.{source_table} не найдена "
            "или нет прав на all_tab_columns"
        )

    col_defs = []
    for col_name, data_type, data_length, data_precision, data_scale, nullable, char_used in columns:
        type_str = _col_type_str(data_type, data_length, data_precision,
                                 data_scale, char_used or "B")
        null_str = "" if nullable == "Y" else " NOT NULL"
        col_defs.append(f'  "{col_name}" {type_str}{null_str}')

    tgt_full = f'"{target_schema.upper()}"."{target_table.upper()}"'
    ddl = (
        f'CREATE TABLE {tgt_full} (\n'
        + ",\n".join(col_defs)
        + "\n)"
    )

    dst_conn = open_oracle_conn(dst_cfg)
    try:
        with dst_conn.cursor() as cur:
            cur.execute(ddl)
            dst_conn.commit()
            print(f"[oracle_stage] created target table {tgt_full}")
    finally:
        dst_conn.close()


def create_stage_table(
    src_cfg: dict,
    dst_cfg: dict,
    source_schema: str,
    source_table: str,
    target_schema: str,
    stage_table: str,
    tablespace: str = "",
) -> None:
    """
    Create the stage table on the target Oracle with the same column structure
    as the source table. Idempotent — silent no-op if the table already exists.
    """
    src_conn = open_oracle_conn(src_cfg)
    try:
        with src_conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, data_length,
                       data_precision, data_scale, nullable, char_used
                FROM   all_tab_columns
                WHERE  owner      = :s
                  AND  table_name = :t
                ORDER BY column_id
            """, {"s": source_schema.upper(), "t": source_table.upper()})
            columns = cur.fetchall()
    finally:
        src_conn.close()

    if not columns:
        raise ValueError(
            f"Исходная таблица {source_schema}.{source_table} не найдена "
            "или нет прав на all_tab_columns"
        )

    col_defs = []
    for col_name, data_type, data_length, data_precision, data_scale, nullable, char_used in columns:
        type_str = _col_type_str(data_type, data_length, data_precision,
                                 data_scale, char_used or "B")
        null_str = "" if nullable == "Y" else " NOT NULL"
        col_defs.append(f'  "{col_name}" {type_str}{null_str}')

    ts_clause = f' TABLESPACE "{tablespace.strip().upper()}"' if tablespace.strip() else ""
    tgt_full = f'"{target_schema.upper()}"."{stage_table.upper()}"'
    ddl = (
        f'CREATE TABLE {tgt_full} (\n'
        + ",\n".join(col_defs)
        + "\n) NOLOGGING" + ts_clause
    )

    dst_conn = open_oracle_conn(dst_cfg)
    try:
        with dst_conn.cursor() as cur:
            try:
                cur.execute(ddl)
                dst_conn.commit()
                print(f"[oracle_stage] created {tgt_full}"
                      + (f" in tablespace {tablespace.strip().upper()}" if tablespace.strip() else ""))
            except Exception as exc:
                # ORA-00955: name is already used by an existing object
                if "ORA-00955" in str(exc):
                    # Table exists — move it to the requested tablespace if needed
                    if tablespace.strip():
                        desired_ts = tablespace.strip().upper()
                        try:
                            cur.execute("""
                                SELECT tablespace_name
                                FROM   all_tables
                                WHERE  owner = :owner
                                  AND  table_name = :table_name
                            """, {
                                "owner": target_schema.upper(),
                                "table_name": stage_table.upper(),
                            })
                            row = cur.fetchone()
                            current_ts = (row[0] if row else "") or ""
                            if current_ts.upper() == desired_ts:
                                print(
                                    f"[oracle_stage] {tgt_full} already exists "
                                    f"in tablespace {desired_ts}"
                                )
                            else:
                                cur.execute(
                                    f'ALTER TABLE {tgt_full} MOVE TABLESPACE "{desired_ts}"'
                                )
                                dst_conn.commit()
                                print(f"[oracle_stage] {tgt_full} already exists, moved to tablespace {desired_ts}")
                        except Exception as move_exc:
                            print(f"[oracle_stage] {tgt_full} already exists, MOVE TABLESPACE failed: {move_exc}")
                    else:
                        print(f"[oracle_stage] {tgt_full} already exists")
                    cur.execute(f"TRUNCATE TABLE {tgt_full}")
                    dst_conn.commit()
                    # Ensure NOLOGGING so the bulk APPEND_VALUES direct-path load
                    # skips redo (stage is throwaway — re-truncated/re-chunked on
                    # restart, and stage validation catches any lost blocks).
                    try:
                        cur.execute(f'ALTER TABLE {tgt_full} NOLOGGING')
                        dst_conn.commit()
                    except Exception as nolog_exc:
                        print(f"[oracle_stage] {tgt_full} NOLOGGING failed: {nolog_exc}")
                    print(f"[oracle_stage] truncated existing stage table {tgt_full}")
                    return
                raise
    finally:
        dst_conn.close()


def drop_stage_table(dst_cfg: dict, target_schema: str, stage_table: str) -> None:
    """Drop the stage table on the target Oracle (PURGE). Silent if not found."""
    dst_conn = open_oracle_conn(dst_cfg)
    try:
        with dst_conn.cursor() as cur:
            try:
                cur.execute(
                    f'DROP TABLE "{target_schema.upper()}"."{stage_table.upper()}" PURGE'
                )
                dst_conn.commit()
            except Exception as exc:
                if "ORA-00942" in str(exc):  # table or view does not exist
                    return
                raise
    finally:
        dst_conn.close()


def sync_target_columns(
    src_cfg: dict,
    dst_cfg: dict,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
) -> dict:
    """
    Bring the target table column set in line with the source table.

    Actions:
    - Columns present in source but missing in target → ALTER TABLE ADD
    - Columns with mismatched type/length → logged as warning only (no auto-ALTER)
    - Extra columns in target not in source → ALTER TABLE DROP COLUMN

    Returns a summary:
      {
        "added":       [{"column": str, "type": str}, ...],
        "dropped":     [str, ...],
        "drop_errors": [{"column": str, "error": str}, ...],
        "warnings":    [{"column": str, "source_type": str, "target_type": str}, ...],
      }
    """
    src_conn = open_oracle_conn(src_cfg)
    try:
        with src_conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, data_length,
                       data_precision, data_scale, nullable, char_used
                FROM   all_tab_columns
                WHERE  owner      = :s
                  AND  table_name = :t
                ORDER BY column_id
            """, {"s": source_schema.upper(), "t": source_table.upper()})
            src_cols = {
                r[0]: r for r in cur.fetchall()
            }
    finally:
        src_conn.close()

    dst_conn = open_oracle_conn(dst_cfg)
    try:
        with dst_conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, data_length,
                       data_precision, data_scale, nullable, char_used
                FROM   all_tab_columns
                WHERE  owner      = :s
                  AND  table_name = :t
                ORDER BY column_id
            """, {"s": target_schema.upper(), "t": target_table.upper()})
            dst_cols = {r[0]: r for r in cur.fetchall()}

        added: list    = []
        dropped: list  = []
        drop_errors: list = []
        warnings: list = []
        extra_cols = [c for c in dst_cols if c not in src_cols]

        # Drop extra columns from target that don't exist in source
        tgt_full = f'"{target_schema.upper()}"."{target_table.upper()}"'
        for col_name in extra_cols:
            try:
                with dst_conn.cursor() as cur:
                    cur.execute(f'ALTER TABLE {tgt_full} DROP COLUMN "{col_name}"')
                dropped.append(col_name)
            except Exception as exc:
                drop_errors.append({"column": col_name, "error": str(exc)})

        # LONG / LONG RAW columns cannot be added via ALTER TABLE without
        # restrictions (ORA-00997 / ORA-01703) — skip them.
        _SKIP_TYPES = {"LONG", "LONG RAW"}

        for col_name, src_row in src_cols.items():
            _, data_type, data_length, data_precision, data_scale, nullable, char_used = src_row

            if data_type.upper() in _SKIP_TYPES:
                if col_name not in dst_cols:
                    warnings.append({
                        "column":      col_name,
                        "source_type": data_type,
                        "target_type": "—",
                        "note":        f"{data_type} columns cannot be added automatically",
                    })
                continue

            type_str  = _col_type_str(data_type, data_length, data_precision,
                                      data_scale, char_used or "B")
            null_str  = "" if nullable == "Y" else " NOT NULL"

            if col_name not in dst_cols:
                ddl = (
                    f'ALTER TABLE "{target_schema.upper()}"."{target_table.upper()}" '
                    f'ADD ("{col_name}" {type_str}{null_str})'
                )
                with dst_conn.cursor() as cur:
                    cur.execute(ddl)
                added.append({"column": col_name, "type": type_str})
            else:
                dst_row = dst_cols[col_name]
                dst_type_str = _col_type_str(
                    dst_row[1], dst_row[2], dst_row[3], dst_row[4], dst_row[6] or "B"
                )
                if dst_type_str != type_str:
                    warnings.append({
                        "column":      col_name,
                        "source_type": type_str,
                        "target_type": dst_type_str,
                    })

        dst_conn.commit()
    finally:
        dst_conn.close()

    if warnings:
        print(
            f"[oracle_stage] sync_target_columns: type mismatches (not auto-fixed): "
            + ", ".join(
                f"{w['column']} src={w['source_type']} dst={w['target_type']}"
                for w in warnings
            )
        )

    return {"added": added, "dropped": dropped, "drop_errors": drop_errors, "warnings": warnings}


def count_stage(dst_cfg: dict, target_schema: str, stage_table: str) -> int:
    """Return the row count of the stage table."""
    dst_conn = open_oracle_conn(dst_cfg)
    try:
        with dst_conn.cursor() as cur:
            cur.execute(
                f'SELECT COUNT(*) FROM "{target_schema.upper()}"."{stage_table.upper()}"'
            )
            return int(cur.fetchone()[0])
    finally:
        dst_conn.close()


def count_source_as_of_scn(
    src_cfg: dict, source_schema: str, source_table: str, scn: int
) -> int:
    """Return the row count of the source table AS OF SCN."""
    src_conn = open_oracle_conn(src_cfg)
    try:
        with src_conn.cursor() as cur:
            cur.execute(
                f'SELECT COUNT(*) FROM "{source_schema.upper()}"."{source_table.upper()}"'
                f" AS OF SCN :scn",
                {"scn": scn},
            )
            return int(cur.fetchone()[0])
    finally:
        src_conn.close()
