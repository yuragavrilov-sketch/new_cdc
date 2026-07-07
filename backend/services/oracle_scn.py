"""Oracle SCN helpers and low-level connection factory."""


_ORACLE_CONTEXT_LIMITS = {
    "module": 48,
    "action": 32,
    "client_identifier": 64,
}


def _apply_session_context(conn, context: dict | None) -> None:
    if not context:
        return
    for key, limit in _ORACLE_CONTEXT_LIMITS.items():
        value = context.get(key)
        if not value:
            continue
        try:
            setattr(conn, key, str(value)[:limit])
        except Exception:
            pass


def open_oracle_conn(cfg: dict, session_context: dict | None = None):
    """
    Open an Oracle connection from a service-config dict.
    cfg keys: host, port (opt, default 1521), service_name, user, password.
    """
    try:
        import oracledb
    except ImportError:
        raise ImportError("oracledb не установлен (pip install oracledb)")
    host         = cfg.get("host", "").strip()
    port         = int(cfg.get("port", 1521))
    service_name = cfg.get("service_name", "").strip()
    user         = cfg.get("user", "").strip()
    password     = cfg.get("password", "")
    if not host or not service_name or not user:
        raise ValueError("Oracle connection не настроен — проверьте Настройки")
    conn = oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:{port}/{service_name}",
    )
    _apply_session_context(conn, session_context)
    return conn


def check_supplemental_logging(
    cfg: dict,
    schema: str,
    table: str,
    session_context: dict | None = None,
) -> bool:
    """
    Return True if the table has at least ALL COLUMNS supplemental logging enabled.
    Required for Debezium LogMiner connector to capture full row images.
    """
    conn = open_oracle_conn(cfg, session_context)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT log_mode FROM v$database
            """)
            row = cur.fetchone()
            if not row or row[0] != "ARCHIVELOG":
                raise RuntimeError(
                    f"Oracle не в режиме ARCHIVELOG (текущий: {row[0] if row else '?'}). "
                    "Debezium LogMiner требует ARCHIVELOG."
                )
            # Check supplemental logging — сначала database-wide (v$database
            # уже доступна, мы выше log_mode читали), потом конкретная
            # таблица через ALL_LOG_GROUPS. all_tables.supplemental_log_data_all
            # отсутствует в части версий и доступен только DBA, поэтому не
            # используем.
            cur.execute("SELECT supplemental_log_data_all FROM v$database")
            row = cur.fetchone()
            if row and (row[0] or "").upper() == "YES":
                return True
            cur.execute("""
                SELECT COUNT(*) FROM all_log_groups
                WHERE  owner = :s AND table_name = :t
                  AND  log_group_type = 'ALL COLUMN LOGGING'
            """, {"s": schema.upper(), "t": table.upper()})
            row = cur.fetchone()
            return bool(row and (row[0] or 0) > 0)
    finally:
        conn.close()
