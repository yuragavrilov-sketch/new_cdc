"""Oracle DB browser — schemas, tables, columns/keys for migration wizard."""

_SYSTEM_SCHEMAS = frozenset([
    "SYS", "SYSTEM", "OUTLN", "DIP", "ORACLE_OCM", "DBSNMP", "APPQOSSYS",
    "WMSYS", "EXFSYS", "CTXSYS", "XDB", "ANONYMOUS", "ORDSYS", "ORDPLUGINS",
    "SI_INFORMTN_SCHEMA", "MDSYS", "OLAPSYS", "MDDATA", "XS$NULL",
    "APEX_PUBLIC_USER", "FLOWS_FILES", "DVSYS", "LBACSYS", "OJVMSYS",
    "GSMADMIN_INTERNAL", "GSMUSER", "AUDSYS", "DVF", "DBSFWUSER",
    "REMOTE_SCHEDULER_AGENT", "SYS$UMF", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC",
])

_CATALOG_TYPES = frozenset([
    "TABLE", "VIEW", "MATERIALIZED VIEW",
    "FUNCTION", "PROCEDURE", "PACKAGE",
    "SEQUENCE", "SYNONYM", "TYPE",
    "TRIGGER", "INDEX", "DATABASE LINK", "JOB",
])


def get_oracle_conn(db: str, configs: dict, *, prefer_owner: bool = False):
    """Open an Oracle connection. db = 'source' or 'target'.

    If prefer_owner=True and owner_user is configured, use schema owner
    credentials instead of the default (Debezium) user. This gives full
    visibility of DDL objects via all_objects / all_source.
    """
    cfg = configs.get(f"oracle_{db}", {})
    host         = cfg.get("host", "").strip()
    port         = cfg.get("port", 1521)
    service_name = cfg.get("service_name", "").strip()

    # Use owner credentials when requested and available
    if prefer_owner and cfg.get("owner_user", "").strip():
        user     = cfg["owner_user"].strip()
        password = cfg.get("owner_password", "")
    else:
        user     = cfg.get("user", "").strip()
        password = cfg.get("password", "")
    if not host or not service_name or not user:
        raise ValueError(f"Oracle {db} не настроен — проверьте Настройки")
    try:
        import oracledb
    except ImportError:
        raise ImportError("oracledb не установлен (pip install oracledb)")
    return oracledb.connect(user=user, password=password, dsn=f"{host}:{port}/{service_name}")


def list_schemas(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT owner FROM all_tables ORDER BY owner")
        return [r[0] for r in cur.fetchall() if r[0] not in _SYSTEM_SCHEMAS]


_SYSMETRIC_NAMES = (
    "Host CPU Utilization (%)",
    "Network Traffic Volume Per Sec",   # B/s
    "Redo Generated Per Sec",           # B/s
    "Physical Reads Per Sec",
    "Physical Writes Per Sec",
)


def get_v_sysmetric(conn) -> dict:
    """Return current value per metric_name from V$SYSMETRIC (60-second window).
    Returns {} if the user lacks SELECT on V$SYSMETRIC."""
    try:
        with conn.cursor() as cur:
            placeholders = ", ".join(f":m{i}" for i in range(len(_SYSMETRIC_NAMES)))
            binds = {f"m{i}": n for i, n in enumerate(_SYSMETRIC_NAMES)}
            cur.execute(f"""
                SELECT metric_name, value
                FROM   v$sysmetric
                WHERE  metric_name IN ({placeholders})
                  AND  group_id = 2
            """, binds)
            return {r[0]: float(r[1] or 0) for r in cur.fetchall()}
    except Exception as exc:
        print(f"[metrics] V$SYSMETRIC unavailable: {exc}")
        return {}


def get_v_sysmetric_history(conn, limit_per_metric: int = 10) -> dict:
    """Return up to `limit_per_metric` recent values per metric from
    V$SYSMETRIC_HISTORY, oldest-first (so a sparkline reads left → right
    in time order)."""
    try:
        with conn.cursor() as cur:
            placeholders = ", ".join(f":m{i}" for i in range(len(_SYSMETRIC_NAMES)))
            binds = {f"m{i}": n for i, n in enumerate(_SYSMETRIC_NAMES)}
            cur.execute(f"""
                SELECT metric_name, value, begin_time
                FROM   v$sysmetric_history
                WHERE  metric_name IN ({placeholders})
                  AND  group_id   = 2
                ORDER  BY begin_time DESC
                FETCH  FIRST 200 ROWS ONLY
            """, binds)
            buckets: dict[str, list] = {}
            for r in cur.fetchall():
                buckets.setdefault(r[0], []).append(float(r[1] or 0))
            for k in buckets:
                buckets[k].reverse()
                if len(buckets[k]) > limit_per_metric:
                    buckets[k] = buckets[k][-limit_per_metric:]
            return buckets
    except Exception as exc:
        print(f"[metrics] V$SYSMETRIC_HISTORY unavailable: {exc}")
        return {}


def get_oracle_version(conn) -> dict:
    """Return Oracle version info. Tries product_component_version → V$VERSION.
    Falls back to {short: 'unknown'} on any failure (don't crash on auth)."""
    try:
        with conn.cursor() as cur:
            # product_component_version is queryable by any user; V$VERSION needs privs
            cur.execute("""
                SELECT product, version FROM product_component_version
                WHERE product LIKE 'Oracle%'
                FETCH FIRST 1 ROWS ONLY
            """)
            row = cur.fetchone()
            if row:
                product, version = row
                return {"banner": f"{product} {version}", "short": version}
    except Exception:
        pass
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT banner FROM v$version WHERE banner LIKE 'Oracle%' FETCH FIRST 1 ROWS ONLY")
            row = cur.fetchone()
            if row:
                banner = row[0]
                # Extract version e.g. "19.21" from "Oracle Database 19c Enterprise Edition Release 19.21.0.0.0 - Production"
                import re
                m = re.search(r"\b(\d+\.\d+(?:\.\d+)?)\b", banner)
                short = m.group(1) if m else banner.split()[2] if len(banner.split()) > 2 else "unknown"
                return {"banner": banner, "short": short}
    except Exception:
        pass
    return {"banner": "unknown", "short": "unknown"}


def list_tables(conn, schema: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM all_tables WHERE owner = :s ORDER BY table_name",
            {"s": schema},
        )
        return [r[0] for r in cur.fetchall()]


def list_all_objects(conn, schema: str) -> list[dict]:
    """List all DDL objects in schema (excluding PACKAGE BODY / TYPE BODY).

    Skips Oracle-internal object names that always differ between databases:
      • SYS_C%   — auto-generated names for PK/UK/CHECK constraints (and their
                   underlying indexes). Recreated implicitly when the parent
                   constraint is recreated, so synchronising them is pointless.
      • SYS_IL% — auto-generated LOB column indexes. Same deal — created when
                   the parent table/column is created.
      • SYS_LOB% — LOB segment objects (storage artifacts, not DDL).
      • ISEQ$$% — identity-column sequences (managed by Oracle).
      • BIN$%   — recycle-bin (already-dropped) objects.
      • DR$%    — Oracle Text auxiliary objects.
    """
    placeholders = ",".join(f":t{i}" for i in range(len(_CATALOG_TYPES)))
    binds = {"s": schema}
    binds.update({f"t{i}": t for i, t in enumerate(sorted(_CATALOG_TYPES))})
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT object_name, object_type, status, last_ddl_time
            FROM   all_objects
            WHERE  owner = :s
              AND  object_type IN ({placeholders})
              AND  object_name NOT LIKE 'SYS_C%'
              AND  object_name NOT LIKE 'SYS_IL%'
              AND  object_name NOT LIKE 'SYS_LOB%'
              AND  object_name NOT LIKE 'ISEQ$$%'
              AND  object_name NOT LIKE 'BIN$%'
              AND  object_name NOT LIKE 'DR$%'
            ORDER  BY object_type, object_name
        """, binds)
        return [
            {
                "object_name": r[0],
                "object_type": r[1],
                "status": r[2],
                "last_ddl_time": r[3].isoformat() if r[3] else None,
            }
            for r in cur.fetchall()
        ]


def get_view_info(conn, schema: str, name: str) -> dict:
    """Get view definition and columns."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT text FROM all_views WHERE owner = :s AND view_name = :n",
            {"s": schema, "n": name},
        )
        row = cur.fetchone()
        sql_text = row[0] if row else None

        cur.execute("""
            SELECT column_name, data_type, data_length, nullable
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :n
            ORDER  BY column_id
        """, {"s": schema, "n": name})
        columns = [
            {"name": r[0], "data_type": r[1], "data_length": r[2], "nullable": r[3] == "Y"}
            for r in cur.fetchall()
        ]

        cur.execute(
            "SELECT status FROM all_objects WHERE owner = :s AND object_name = :n AND object_type = 'VIEW'",
            {"s": schema, "n": name},
        )
        status_row = cur.fetchone()

    return {
        "sql_text": sql_text,
        "columns": columns,
        "status": status_row[0] if status_row else "UNKNOWN",
    }


def get_mview_info(conn, schema: str, name: str) -> dict:
    """Get materialized view definition, columns, and refresh info."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT query, refresh_mode, refresh_method, last_refresh_date
            FROM   all_mviews
            WHERE  owner = :s AND mview_name = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
        if not row:
            return {"sql_text": None, "columns": [], "refresh_type": None, "last_refresh": None}

        cur.execute("""
            SELECT column_name, data_type, data_length, nullable
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :n
            ORDER  BY column_id
        """, {"s": schema, "n": name})
        columns = [
            {"name": r[0], "data_type": r[1], "data_length": r[2], "nullable": r[3] == "Y"}
            for r in cur.fetchall()
        ]

    return {
        "sql_text": row[0],
        "columns": columns,
        "refresh_type": f"{row[1]}/{row[2]}",
        "last_refresh": row[3].isoformat() if row[3] else None,
    }


def get_source_code(conn, schema: str, name: str, obj_type: str) -> str | None:
    """Get source code for FUNCTION, PROCEDURE, PACKAGE, PACKAGE BODY, TYPE, TYPE BODY."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT text
            FROM   all_source
            WHERE  owner = :s AND name = :n AND type = :t
            ORDER  BY line
        """, {"s": schema, "n": name, "t": obj_type})
        lines = [r[0] for r in cur.fetchall()]
    return "".join(lines) if lines else None


def get_code_info(conn, schema: str, name: str, obj_type: str) -> dict:
    """Get full info for a code object (function, procedure, package, type)."""
    result: dict = {"status": "UNKNOWN"}

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM all_objects WHERE owner = :s AND object_name = :n AND object_type = :t",
            {"s": schema, "n": name, "t": obj_type},
        )
        row = cur.fetchone()
        if row:
            result["status"] = row[0]

    if obj_type == "PACKAGE":
        result["spec_source"] = get_source_code(conn, schema, name, "PACKAGE")
        result["body_source"] = get_source_code(conn, schema, name, "PACKAGE BODY")
    elif obj_type == "TYPE":
        result["source"] = get_source_code(conn, schema, name, "TYPE")
        result["body_source"] = get_source_code(conn, schema, name, "TYPE BODY")
    else:
        result["source_code"] = get_source_code(conn, schema, name, obj_type)

    if obj_type in ("FUNCTION", "PROCEDURE"):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM all_arguments
                WHERE owner = :s AND object_name = :n AND argument_name IS NOT NULL
            """, {"s": schema, "n": name})
            result["argument_count"] = cur.fetchone()[0]

    return result


def get_sequence_info(conn, schema: str, name: str) -> dict:
    """Get sequence parameters."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT min_value, max_value, increment_by, cache_size, last_number
            FROM   all_sequences
            WHERE  sequence_owner = :s AND sequence_name = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
    if not row:
        return {}
    return {
        "min_value": str(row[0]) if row[0] is not None else None,
        "max_value": str(row[1]) if row[1] is not None else None,
        "increment_by": str(row[2]) if row[2] is not None else None,
        "cache_size": row[3],
        "last_number": str(row[4]) if row[4] is not None else None,
    }


def get_trigger_info(conn, schema: str, name: str) -> dict:
    """Get trigger header + body."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT trigger_type, triggering_event, table_owner, table_name,
                   status, when_clause, trigger_body, base_object_type,
                   action_type
            FROM   all_triggers
            WHERE  owner = :s AND trigger_name = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
    if not row:
        return {}
    # trigger_body is CLOB
    body = row[6]
    if body is not None and not isinstance(body, str):
        try:
            body = body.read()
        except Exception:
            body = str(body)
    when_clause = row[5]
    if when_clause is not None and not isinstance(when_clause, str):
        try:
            when_clause = when_clause.read()
        except Exception:
            when_clause = str(when_clause)
    return {
        "trigger_type":     row[0],
        "triggering_event": row[1],
        "table_owner":      row[2],
        "table_name":       row[3],
        "status":           row[4],
        "when_clause":      when_clause,
        "trigger_body":     body,
        "base_object_type": row[7],
        "action_type":      row[8],
    }


# DBMS-side and ALL_OBJECTS-side type names for the same logical object differ
# in one place: PACKAGE BODY → "PACKAGE BODY" in ALL_OBJECTS but is queried as
# its own row in all_errors with TYPE='PACKAGE BODY'. We pass through as-is.
def get_compilation_errors(conn, schema: str, object_type: str, object_name: str) -> list[dict]:
    """Return rows from all_errors for an INVALID PL/SQL object.

    Most useful for VIEW / PACKAGE / PACKAGE BODY / PROCEDURE / FUNCTION /
    TYPE / TYPE BODY / TRIGGER — Oracle stores compilation errors with
    line/position/text. For non-compilable types (TABLE / INDEX / SEQUENCE)
    all_errors is normally empty, so this returns [].
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT type, sequence, line, position, text, attribute, message_number
            FROM   all_errors
            WHERE  owner = :s
              AND  name  = :n
              AND  type  = :t
            ORDER  BY sequence
        """, {"s": schema.upper(), "n": object_name.upper(), "t": object_type.upper()})
        out: list[dict] = []
        for r in cur.fetchall():
            out.append({
                "type":           r[0],
                "sequence":       r[1],
                "line":           r[2],
                "position":       r[3],
                "text":           r[4],
                "attribute":      r[5],
                "message_number": r[6],
            })
        return out


def get_index_info(conn, schema: str, name: str) -> dict:
    """Get index columns + properties."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_owner, table_name, uniqueness, index_type,
                   status, partitioned, tablespace_name
            FROM   all_indexes
            WHERE  owner = :s AND index_name = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
        if not row:
            return {}
        cur.execute("""
            SELECT column_name, column_position, descend
            FROM   all_ind_columns
            WHERE  index_owner = :s AND index_name = :n
            ORDER  BY column_position
        """, {"s": schema, "n": name})
        cols = [
            {"name": c[0], "position": c[1], "descending": c[2] == "DESC"}
            for c in cur.fetchall()
        ]
    return {
        "table_owner":    row[0],
        "table_name":     row[1],
        "uniqueness":     row[2],
        "index_type":     row[3],
        "status":         row[4],
        "partitioned":    row[5] == "YES",
        "tablespace":     row[6],
        "columns":        cols,
    }


def get_db_link_info(conn, schema: str, name: str) -> dict:
    """Get database-link target info. NB: in Oracle, all_db_links is keyed
    by owner = SCHEMA + 'PUBLIC'. We filter by owner = schema."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT db_link, username, host, created
            FROM   all_db_links
            WHERE  owner = :s AND db_link = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
    if not row:
        return {}
    return {
        "db_link":  row[0],
        "username": row[1],
        "host":     row[2],
        "created":  row[3].isoformat() if row[3] else None,
    }


def get_job_info(conn, schema: str, name: str) -> dict:
    """Get scheduler job parameters (subset)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT job_name, job_type, job_action, schedule_type,
                   start_date, repeat_interval, enabled, state
            FROM   all_scheduler_jobs
            WHERE  owner = :s AND job_name = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
    if not row:
        return {}
    action = row[2]
    if action is not None and not isinstance(action, str):
        try:
            action = action.read()
        except Exception:
            action = str(action)
    return {
        "job_name":        row[0],
        "job_type":        row[1],
        "job_action":      action,
        "schedule_type":   row[3],
        "start_date":      row[4].isoformat() if row[4] else None,
        "repeat_interval": row[5],
        "enabled":         row[6] == "TRUE",
        "state":           row[7],
    }


def get_synonym_info(conn, schema: str, name: str) -> dict:
    """Get synonym target info."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_owner, table_name, db_link
            FROM   all_synonyms
            WHERE  owner = :s AND synonym_name = :n
        """, {"s": schema, "n": name})
        row = cur.fetchone()
    if not row:
        return {}
    return {
        "table_owner": row[0],
        "table_name": row[1],
        "db_link": row[2],
    }


def get_table_info(conn, schema: str, table: str) -> dict:
    supp_log = None
    with conn.cursor() as cur:
        # Columns
        cur.execute("""
            SELECT column_name, data_type, nullable
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        columns = [{"name": r[0], "type": r[1], "nullable": r[2] == "Y"}
                   for r in cur.fetchall()]

        # PK columns
        cur.execute("""
            SELECT acc.column_name
            FROM   all_constraints  ac
            JOIN   all_cons_columns acc
                   ON  ac.constraint_name = acc.constraint_name
                   AND ac.owner           = acc.owner
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type = 'P'
            ORDER BY acc.position
        """, {"s": schema, "t": table})
        pk_columns = [r[0] for r in cur.fetchall()]

        # UK constraints
        cur.execute("""
            SELECT ac.constraint_name, acc.column_name
            FROM   all_constraints  ac
            JOIN   all_cons_columns acc
                   ON  ac.constraint_name = acc.constraint_name
                   AND ac.owner           = acc.owner
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type = 'U'
            ORDER BY ac.constraint_name, acc.position
        """, {"s": schema, "t": table})
        uk_map: dict = {}
        for cname, col in cur.fetchall():
            uk_map.setdefault(cname, []).append(col)

        # Unique indexes that are NOT already covered by a UK constraint
        # (CREATE UNIQUE INDEX without a matching CONSTRAINT UNIQUE)
        constraint_backed: set = set()
        cur.execute("""
            SELECT index_name
            FROM   all_constraints
            WHERE  owner      = :s
              AND  table_name = :t
              AND  constraint_type IN ('P', 'U')
              AND  index_name IS NOT NULL
        """, {"s": schema, "t": table})
        for row in cur.fetchall():
            constraint_backed.add(row[0])

        cur.execute("""
            SELECT ai.index_name,
                   LISTAGG(aic.column_name, ',')
                       WITHIN GROUP (ORDER BY aic.column_position) AS cols
            FROM   all_indexes     ai
            JOIN   all_ind_columns aic
                   ON  ai.index_name = aic.index_name
                   AND ai.owner      = aic.index_owner
            WHERE  ai.owner      = :s
              AND  ai.table_name = :t
              AND  ai.uniqueness = 'UNIQUE'
            GROUP BY ai.index_name
            ORDER BY ai.index_name
        """, {"s": schema, "t": table})
        for idx_name, cols_csv in cur.fetchall():
            if idx_name not in constraint_backed and idx_name not in uk_map:
                uk_map[idx_name] = cols_csv.split(",")

        uk_constraints = [{"name": k, "columns": v} for k, v in uk_map.items()]

        try:
            cur.execute("SELECT supplemental_log_data_all FROM v$database")
            row = cur.fetchone()
            db_level = (row[0] or "").upper() if row else None
        except Exception:
            db_level = None

        if db_level == "YES":
            supp_log = "YES"
        else:
            try:
                cur.execute("""
                    SELECT COUNT(*) FROM all_log_groups
                    WHERE  owner = :s AND table_name = :t
                      AND  log_group_type = 'ALL COLUMN LOGGING'
                """, {"s": schema, "t": table})
                row = cur.fetchone()
                supp_log = "YES" if row and (row[0] or 0) > 0 else "NO"
            except Exception:
                supp_log = None

    return {
        "columns":        columns,
        "pk_columns":     pk_columns,
        "uk_constraints": uk_constraints,
        "supplemental_log_data_all": supp_log,
    }


# ── Full DDL info (for target-prep tab) ────────────────────────────────────

def get_full_ddl_info(conn, schema: str, table: str) -> dict:
    """Return full DDL snapshot: columns, constraints, indexes, triggers."""
    # Supplemental logging at table level — нужен для CDC (LogMiner).
    # all_tables.supplemental_log_data_all присутствует не во всех версиях /
    # доступен только DBA, поэтому проверяем через ALL_LOG_GROUPS (есть у
    # любого юзера, видит свои таблицы) + v$database для database-wide
    # настройки. Результат — 'YES' / 'NO' / None (если оба запроса упали).
    supp_log = None
    db_level = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT supplemental_log_data_all FROM v$database")
            row = cur.fetchone()
            db_level = (row[0] or "").upper() if row else None
    except Exception:
        db_level = None
    if db_level == "YES":
        supp_log = "YES"
    else:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM all_log_groups
                    WHERE  owner = :s AND table_name = :t
                      AND  log_group_type = 'ALL COLUMN LOGGING'
                """, {"s": schema, "t": table})
                row = cur.fetchone()
                supp_log = "YES" if row and (row[0] or 0) > 0 else "NO"
        except Exception:
            supp_log = None

    with conn.cursor() as cur:
        # Fetch columns without data_default (LONG type — cannot be used in
        # expressions; fetched separately below to avoid ORA-00997).
        cur.execute("""
            SELECT column_name, data_type, data_length, data_precision, data_scale,
                   nullable, column_id
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        columns = [
            {
                "name":           r[0],
                "data_type":      r[1],
                "data_length":    r[2],
                "data_precision": r[3],
                "data_scale":     r[4],
                "nullable":       r[5] == "Y",
                "data_default":   None,   # populated below
                "column_id":      r[6],
            }
            for r in cur.fetchall()
        ]

    # Fetch data_default (LONG) one row at a time — the only safe way to read
    # Oracle LONG columns without risking ORA-00997.
    col_index = {c["name"]: c for c in columns}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_default
            FROM   all_tab_columns
            WHERE  owner = :s AND table_name = :t
            ORDER BY column_id
        """, {"s": schema, "t": table})
        for row in cur:
            col_name, default_val = row
            if col_name in col_index and default_val is not None:
                try:
                    col_index[col_name]["data_default"] = str(default_val).strip()
                except Exception:
                    pass   # ignore unreadable LONG values

    with conn.cursor() as cur:

        # Constraints (PK / UK / FK / CHECK) — subquery avoids outer-join NULLs
        cur.execute("""
            SELECT ac.constraint_name,
                   ac.constraint_type,
                   ac.status,
                   (SELECT LISTAGG(acc.column_name, ',')
                            WITHIN GROUP (ORDER BY acc.position)
                    FROM   all_cons_columns acc
                    WHERE  acc.constraint_name = ac.constraint_name
                      AND  acc.owner           = ac.owner) AS cols
            FROM   all_constraints ac
            WHERE  ac.owner           = :s
              AND  ac.table_name      = :t
              AND  ac.constraint_type IN ('P','U','R','C')
            ORDER BY ac.constraint_type, ac.constraint_name
        """, {"s": schema, "t": table})
        _ctype = {"P": "PRIMARY KEY", "U": "UNIQUE", "R": "FOREIGN KEY", "C": "CHECK"}
        constraints = [
            {
                "name":      r[0],
                "type":      _ctype.get(r[1], r[1]),
                "type_code": r[1],
                "status":    r[2],          # ENABLED / DISABLED
                "columns":   r[3].split(",") if r[3] else [],
            }
            for r in cur.fetchall()
        ]

        # Indexes
        # Note: partitioned indexes have status = 'N/A' in all_indexes;
        # real per-partition status lives in all_ind_partitions.
        # We treat 'N/A' as 'VALID' for comparison purposes.
        cur.execute("""
            SELECT ai.index_name, ai.uniqueness, ai.index_type,
                   ai.status, ai.partitioned,
                   LISTAGG(aic.column_name, ',')
                       WITHIN GROUP (ORDER BY aic.column_position) AS cols
            FROM   all_indexes     ai
            JOIN   all_ind_columns aic
                   ON  ai.index_name  = aic.index_name
                   AND ai.owner       = aic.index_owner
            WHERE  ai.owner      = :s
              AND  ai.table_name = :t
            GROUP BY ai.index_name, ai.uniqueness, ai.index_type,
                     ai.status, ai.partitioned
            ORDER BY ai.index_name
        """, {"s": schema, "t": table})
        indexes = [
            {
                "name":       r[0],
                "unique":     r[1] == "UNIQUE",
                "index_type": r[2],
                "status":     "VALID" if r[3] == "N/A" else r[3],
                "columns":    r[5].split(",") if r[5] else [],
            }
            for r in cur.fetchall()
        ]

        # Triggers
        cur.execute("""
            SELECT trigger_name, trigger_type, triggering_event, status
            FROM   all_triggers
            WHERE  owner = :s AND table_name = :t
            ORDER BY trigger_name
        """, {"s": schema, "t": table})
        triggers = [
            {"name": r[0], "trigger_type": r[1], "event": r[2], "status": r[3]}
            for r in cur.fetchall()
        ]

    return {
        "schema":      schema,
        "table":       table,
        "columns":     columns,
        "constraints": constraints,
        "indexes":     indexes,
        "triggers":    triggers,
        "supplemental_log_data_all": supp_log,
    }


def execute_target_action(conn, action: str, schema: str, table: str, object_name: str) -> None:
    """Execute a DDL management action on the target database."""
    _valid = {
        "disable_index", "enable_index",
        "disable_trigger", "enable_trigger",
        "disable_constraint", "enable_constraint",
    }
    if action not in _valid:
        raise ValueError(f"Unknown action: {action}")

    # Build DDL statement — identifiers are double-quoted to preserve case
    s, t, o = schema.upper(), table.upper(), object_name
    if action == "disable_index":
        ddl = f'ALTER INDEX "{s}"."{o}" UNUSABLE'
    elif action == "enable_index":
        ddl = f'ALTER INDEX "{s}"."{o}" REBUILD'
    elif action == "disable_trigger":
        ddl = f'ALTER TRIGGER "{s}"."{o}" DISABLE'
    elif action == "enable_trigger":
        ddl = f'ALTER TRIGGER "{s}"."{o}" ENABLE'
    elif action == "disable_constraint":
        ddl = f'ALTER TABLE "{s}"."{t}" DISABLE CONSTRAINT "{o}"'
    else:  # enable_constraint
        ddl = f'ALTER TABLE "{s}"."{t}" ENABLE CONSTRAINT "{o}"'

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def is_temporary_table(conn, schema: str, table: str) -> bool:
    """Return True if {schema}.{table} is a Global Temporary Table.

    Temp tables не поддерживают LOGGING/NOLOGGING (ORA-14451), а у их
    индексов нет UNUSABLE-состояния в обычном смысле — поэтому везде
    скипаем перестроение и переключение логирования.
    """
    s, t = schema.upper(), table.upper()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT temporary FROM all_tables WHERE owner = :s AND table_name = :t",
            {"s": s, "t": t},
        )
        row = cur.fetchone()
    return bool(row) and (row[0] or "N").upper() == "Y"


def set_table_logging(conn, schema: str, table: str, nologging: bool) -> None:
    """Switch the target table between NOLOGGING and LOGGING mode.

    NOLOGGING before baseline load skips redo generation for direct-path
    inserts (APPEND hint), dramatically reducing I/O.  Restore to LOGGING
    afterwards so normal DML is protected.

    Skips silently for Global Temporary Tables — Oracle отклоняет
    ALTER TABLE LOGGING/NOLOGGING на GTT с ORA-14451.
    """
    s, t = schema.upper(), table.upper()
    if is_temporary_table(conn, s, t):
        print(f"[oracle_browser] set_table_logging skipped: {s}.{t} is a temporary table")
        return
    mode = "NOLOGGING" if nologging else "LOGGING"
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{s}"."{t}" {mode}')
    conn.commit()


def _constraint_backing_indexes(conn, schema: str, table: str) -> set[str]:
    """Return upper-cased names of indexes that back a PK or UNIQUE constraint.

    Uses the authoritative ``all_constraints.index_name`` column — this is
    Oracle's own mapping and covers all cases including non-unique indexes
    created with ``USING INDEX`` on a UNIQUE constraint.

    Also includes every index with ``uniqueness = 'UNIQUE'`` as a safety net.

    Oracle refuses to skip such indexes during INSERT even with
    SKIP_UNUSABLE_INDEXES = TRUE.  Marking them UNUSABLE causes ORA-26026.
    """
    s = schema.upper()
    t = table.upper()
    protected: set[str] = set()

    with conn.cursor() as cur:
        # Direct authoritative lookup
        cur.execute("""
            SELECT index_name
            FROM   all_constraints
            WHERE  owner = :s AND table_name = :t
              AND  constraint_type IN ('P', 'U')
              AND  index_name IS NOT NULL
        """, {"s": s, "t": t})
        for row in cur.fetchall():
            protected.add(row[0].upper())

        # Safety net: any index flagged UNIQUE in all_indexes
        cur.execute("""
            SELECT index_name
            FROM   all_indexes
            WHERE  owner = :s AND table_name = :t
              AND  uniqueness = 'UNIQUE'
        """, {"s": s, "t": t})
        for row in cur.fetchall():
            protected.add(row[0].upper())

    print(f"[oracle_browser] protected indexes for {s}.{t}: {protected}")
    return protected


def rebuild_unusable_constraint_indexes(conn, schema: str, table: str) -> list[str]:
    """Rebuild any UNUSABLE indexes that back PK/UNIQUE constraints.

    Recovers from a previous failed attempt that left such indexes in
    UNUSABLE state.  ORA-26026 is raised by INSERT if they are UNUSABLE,
    so they MUST be VALID before the load starts.

    Returns the list of index names that were rebuilt.
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    protected = _constraint_backing_indexes(conn, schema, table)
    # NOLOGGING не поддерживается на индексах GTT (ORA-14451).
    rebuild_clause = "REBUILD" if is_temporary_table(conn, s, table) else "REBUILD NOLOGGING"
    rebuilt: list[str] = []
    with conn.cursor() as cur:
        for idx in info["indexes"]:
            if idx["name"].upper() in protected and idx["status"] == "UNUSABLE":
                try:
                    cur.execute(f'ALTER INDEX "{s}"."{idx["name"]}" {rebuild_clause}')
                    rebuilt.append(idx["name"])
                except Exception as exc:
                    print(f"[oracle_browser] could not rebuild {idx['name']}: {exc}")
    conn.commit()
    return rebuilt


def mark_indexes_unusable(conn, schema: str, table: str, skip_pk: bool = True) -> list[str]:
    """Mark indexes on *table* as UNUSABLE so Oracle skips index maintenance
    during the subsequent bulk INSERT.

    SKIP_UNUSABLE_INDEXES = TRUE (Oracle session default) causes the INSERT to
    bypass UNUSABLE indexes — but ONLY for indexes that do NOT back a PK or
    UNIQUE constraint.  Marking such an index UNUSABLE causes ORA-26026.

    Detection uses ``all_constraints.index_name`` — the authoritative Oracle
    mapping — plus the ``uniqueness`` flag as a safety net.

    Returns the list of index names that were marked UNUSABLE.
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    protected = _constraint_backing_indexes(conn, schema, table)

    marked: list[str] = []
    with conn.cursor() as cur:
        for idx in info["indexes"]:
            if idx["status"] != "VALID":
                continue
            if idx["name"].upper() in protected:
                continue
            try:
                cur.execute(f'ALTER INDEX "{s}"."{idx["name"]}" UNUSABLE')
                marked.append(idx["name"])
            except Exception as exc:
                print(f"[oracle_browser] could not mark {idx['name']} UNUSABLE: {exc}")
    conn.commit()
    return marked


def disable_triggers(conn, schema: str, table: str) -> list[str]:
    """Disable all ENABLED triggers on *table* so they don't fire during bulk
    INSERT.  Returns the list of trigger names that were disabled.
    Re-enable them separately with enable_triggers(), normally from a manual
    target-trigger job after CDC apply has caught up.
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    disabled: list[str] = []
    with conn.cursor() as cur:
        for trg in info["triggers"]:
            if trg["status"] == "ENABLED":
                try:
                    cur.execute(f'ALTER TRIGGER "{s}"."{trg["name"]}" DISABLE')
                    disabled.append(trg["name"])
                except Exception as exc:
                    print(f"[oracle_browser] could not disable trigger {trg['name']}: {exc}")
    conn.commit()
    return disabled


def get_referencing_foreign_keys(conn, schema: str, table: str, status: str | None = None) -> list[dict]:
    """Return foreign keys in other tables that reference *schema.table*.

    Oracle blocks TRUNCATE on a parent table while such FK constraints are
    enabled (ORA-02266), even if the child tables are not being loaded now.
    """
    s = schema.upper()
    t = table.upper()
    params = {"s": s, "t": t}
    status_filter = ""
    if status:
        status_filter = "AND fk.status = :status"
        params["status"] = status.upper()

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT fk.owner, fk.table_name, fk.constraint_name, fk.status
            FROM   all_constraints fk
            JOIN   all_constraints pk
                   ON pk.owner = fk.r_owner
                  AND pk.constraint_name = fk.r_constraint_name
            WHERE  fk.constraint_type = 'R'
              AND  pk.owner = :s
              AND  pk.table_name = :t
              {status_filter}
            ORDER BY fk.owner, fk.table_name, fk.constraint_name
        """, params)
        rows = cur.fetchall()

    return [
        {
            "owner": row[0],
            "table_name": row[1],
            "constraint_name": row[2],
            "status": row[3],
        }
        for row in rows
    ]


def disable_referencing_foreign_keys(conn, schema: str, table: str) -> list[str]:
    """Disable enabled child FKs that reference *schema.table*.

    Returns display names in OWNER.TABLE.CONSTRAINT form.
    """
    disabled: list[str] = []
    with conn.cursor() as cur:
        for fk in get_referencing_foreign_keys(conn, schema, table, status="ENABLED"):
            owner = fk["owner"]
            child_table = fk["table_name"]
            constraint_name = fk["constraint_name"]
            cur.execute(
                f'ALTER TABLE "{owner}"."{child_table}" DISABLE CONSTRAINT "{constraint_name}"'
            )
            disabled.append(f"{owner}.{child_table}.{constraint_name}")
    conn.commit()
    return disabled


def disable_key_constraints_cascade(conn, schema: str, table: str) -> list[str]:
    """Disable enabled PK/UK constraints on *schema.table* with CASCADE.

    This is the robust guard for ORA-02266. If Oracle still sees an enabled
    child FK, disabling the referenced PK/UK with CASCADE disables the dependent
    FK constraints as well.
    """
    s = schema.upper()
    t = table.upper()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT constraint_name, constraint_type
            FROM   all_constraints
            WHERE  owner = :s
              AND  table_name = :t
              AND  status = 'ENABLED'
              AND  constraint_type IN ('P', 'U')
            ORDER BY constraint_type, constraint_name
        """, {"s": s, "t": t})
        constraints = [(row[0], row[1]) for row in cur.fetchall()]

        disabled: list[str] = []
        for constraint_name, constraint_type in constraints:
            cur.execute(
                f'ALTER TABLE "{s}"."{t}" DISABLE CONSTRAINT "{constraint_name}" CASCADE'
            )
            disabled.append(f"{constraint_type}:{constraint_name}")
    conn.commit()
    return disabled


def truncate_table_for_load(conn, schema: str, table: str) -> dict:
    """Prepare and truncate a target table for reload.

    Returns details about disabled objects. Raises a RuntimeError with visible
    blocking FK names if Oracle still reports ORA-02266.
    """
    s = schema.upper()
    t = table.upper()
    disabled_ref_fk = disable_referencing_foreign_keys(conn, s, t)
    disabled_keys = disable_key_constraints_cascade(conn, s, t)
    qname = f'"{s}"."{t}"'
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {qname}")
        conn.commit()
    except Exception as exc:
        if "ORA-02266" not in str(exc):
            raise
        try:
            blockers = get_referencing_foreign_keys(conn, s, t, status="ENABLED")
            visible = [
                f"{fk['owner']}.{fk['table_name']}.{fk['constraint_name']}"
                for fk in blockers
            ]
        except Exception as lookup_exc:
            visible = [f"<lookup failed: {lookup_exc}>"]
        raise RuntimeError(
            f"ORA-02266 while truncating {s}.{t}; "
            f"disabled_ref_fk={disabled_ref_fk}; disabled_keys={disabled_keys}; "
            f"visible_enabled_ref_fk={visible}"
        ) from exc
    return {
        "referencing_fk": disabled_ref_fk,
        "key_constraints": disabled_keys,
    }


def enable_all_disabled_objects(conn, schema: str, table: str) -> dict:
    """
    Rebuild UNUSABLE indexes and re-enable DISABLED constraints on *table*.
    Foreign keys are enabled NOVALIDATE so per-table migration does not fail
    on ORA-02298 while the referenced parent rows are loaded by another table
    in the same schema pack.

    Triggers are NOT touched here — they must be re-enabled separately
    (e.g. via enable_triggers()) after CDC apply has fully caught up,
    otherwise triggers would fire on every replayed DML event.

    Returns a summary dict:
      { "enabled": {"indexes": [...], "constraints": [...]},
        "errors":  {"indexes": [...], "constraints": [...]} }

    Errors list items: {"name": str, "error": str}
    A single commit is issued after all statements.  Any per-object error is
    collected and returned rather than raised so the caller decides policy.
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    t = table.upper()
    # NOLOGGING недопустим на индексах GTT (ORA-14451). Для temp-таблиц
    # используем обычный REBUILD без NOLOGGING.
    is_temp = is_temporary_table(conn, s, t)
    rebuild_clause = "REBUILD" if is_temp else "REBUILD NOLOGGING"
    enabled: dict = {"indexes": [], "constraints": [], "fk_novalidate": []}
    errors:  dict = {"indexes": [], "constraints": []}

    with conn.cursor() as cur:
        for idx in info["indexes"]:
            if idx["status"] == "UNUSABLE":
                try:
                    cur.execute(f'ALTER INDEX "{s}"."{idx["name"]}" {rebuild_clause}')
                    enabled["indexes"].append(idx["name"])
                except Exception as exc:
                    errors["indexes"].append({"name": idx["name"], "error": str(exc)})

        for con in info["constraints"]:
            if con["status"] == "DISABLED":
                try:
                    if con.get("type_code") == "R":
                        cur.execute(
                            f'ALTER TABLE "{s}"."{t}" ENABLE NOVALIDATE CONSTRAINT "{con["name"]}"'
                        )
                        enabled["fk_novalidate"].append(con["name"])
                        continue
                    cur.execute(
                        f'ALTER TABLE "{s}"."{t}" ENABLE CONSTRAINT "{con["name"]}"'
                    )
                    enabled["constraints"].append(con["name"])
                except Exception as exc:
                    errors["constraints"].append({"name": con["name"], "error": str(exc)})

    conn.commit()
    return {"enabled": enabled, "errors": errors}


def enable_triggers(conn, schema: str, table: str) -> dict:
    """Re-enable all DISABLED triggers on *table*.

    Call this only after CDC apply has caught up — otherwise triggers fire
    on every replayed row.  Counterpart to disable_triggers().

    Returns {"enabled": [...], "errors": [...]}.
    """
    info = get_full_ddl_info(conn, schema, table)
    s = schema.upper()
    enabled: list[str] = []
    errors: list[dict] = []

    with conn.cursor() as cur:
        for trg in info["triggers"]:
            if trg["status"] == "DISABLED":
                try:
                    cur.execute(f'ALTER TRIGGER "{s}"."{trg["name"]}" ENABLE')
                    enabled.append(trg["name"])
                except Exception as exc:
                    errors.append({"name": trg["name"], "error": str(exc)})

    conn.commit()
    return {"enabled": enabled, "errors": errors}


def enable_disabled_foreign_keys(conn, schema: str, table: str) -> dict:
    """Best-effort ENABLE NOVALIDATE of every currently-DISABLED foreign key that
    touches *schema.table* — the table's own outbound FKs and the child FKs that
    reference it.

    Used by the orchestrator to reconcile FKs that INDEXES_ENABLING left disabled
    because a referenced parent's PK was not yet enabled. Idempotent and never
    raises per FK; an FK that still can't enable (parent PK still down) is reported
    in ``still_disabled`` for the next pass.

    Returns {"enabled": [display...], "still_disabled": [{"name","error"}...]}.
    """
    s = schema.upper()
    t = table.upper()
    enabled: list[str] = []
    still: list[dict] = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT constraint_name
            FROM   all_constraints
            WHERE  owner = :s
              AND  table_name = :t
              AND  constraint_type = 'R'
              AND  status = 'DISABLED'
            ORDER BY constraint_name
        """, {"s": s, "t": t})
        targets = [(s, t, row[0]) for row in cur.fetchall()]

    targets += [
        (fk["owner"], fk["table_name"], fk["constraint_name"])
        for fk in get_referencing_foreign_keys(conn, s, t, status="DISABLED")
    ]

    with conn.cursor() as cur:
        for owner, tbl, cname in targets:
            display = f"{owner}.{tbl}.{cname}"
            try:
                cur.execute(
                    f'ALTER TABLE "{owner}"."{tbl}" ENABLE NOVALIDATE CONSTRAINT "{cname}"'
                )
                enabled.append(display)
            except Exception as exc:
                still.append({"name": display, "error": str(exc)})

    conn.commit()
    return {"enabled": enabled, "still_disabled": still}
