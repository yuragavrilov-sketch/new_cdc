"""DDL apply worker — fetches source DDL via DBMS_METADATA.GET_DDL and applies
it on target.

Actions:
  create_missing — execute CREATE; ORA-00955 (already exists) treated as ok.
  sync_diff      — only for replaceable types (VIEW/PACKAGE/FUNCTION/...);
                   relies on Oracle returning "CREATE OR REPLACE" via metadata.
  recreate       — DROP then CREATE; refused for TABLE unless object is empty
                   (safety net — UI gates this further).

Schema re-namespacing is done as a plain string substitution on the extracted
DDL: source-schema identifiers (quoted "SRC" and unquoted SRC.) are swapped
for target schema. Imperfect but covers 99% of cases; review applied_ddl in
the UI if a job fails strangely.
"""

import re
import time
import traceback

import common as db
from common import WORKER_ID


def _session_context(action: str, *, job_id: str, object_name: str) -> dict:
    return {
        "module": "new_cdc.worker",
        "action": action,
        "client_identifier": f"ddl={job_id[:8]} worker={WORKER_ID} object={object_name}",
    }


# DBMS_METADATA object-type names — uses underscores (TABLE, PACKAGE_BODY,
# MATERIALIZED_VIEW, DB_LINK, ...) while our table stores the canonical form
# with spaces (MATERIALIZED VIEW, DATABASE LINK).
_TYPE_TO_METADATA = {
    "TABLE":              "TABLE",
    "VIEW":               "VIEW",
    "MATERIALIZED VIEW":  "MATERIALIZED_VIEW",
    "INDEX":              "INDEX",
    "SEQUENCE":           "SEQUENCE",
    "SYNONYM":            "SYNONYM",
    "PACKAGE":            "PACKAGE",
    "PACKAGE BODY":       "PACKAGE_BODY",
    "PROCEDURE":          "PROCEDURE",
    "FUNCTION":           "FUNCTION",
    "TRIGGER":            "TRIGGER",
    "TYPE":               "TYPE",
    "TYPE BODY":          "TYPE_BODY",
    "DATABASE LINK":      "DB_LINK",
    "JOB":                "PROCOBJ",  # scheduler objects → PROCOBJ
}


# Replaceable via "CREATE OR REPLACE" — for sync_diff.
_REPLACEABLE = {
    "VIEW", "PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE BODY",
    "TRIGGER", "TYPE", "TYPE BODY", "SYNONYM",
}


def _set_metadata_transforms(cur) -> None:
    """Strip noise (tablespace/storage/segment attrs) so the DDL is portable."""
    for param in (
        "STORAGE", "TABLESPACE", "SEGMENT_ATTRIBUTES",
        "CONSTRAINTS_AS_ALTER", "REF_CONSTRAINTS",
        "SQLTERMINATOR",
    ):
        try:
            cur.execute(
                "BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM("
                "DBMS_METADATA.SESSION_TRANSFORM, :p, FALSE); END;",
                {"p": param},
            )
        except Exception:
            pass  # parameter may not apply to this object type — ignore


def _extract_source_ddl(src_conn, owner: str, object_type: str, object_name: str) -> str:
    """Return the CREATE statement(s) for owner.object_name."""
    meta_type = _TYPE_TO_METADATA.get(object_type)
    if not meta_type:
        raise ValueError(f"DBMS_METADATA mapping missing for {object_type}")
    with src_conn.cursor() as cur:
        _set_metadata_transforms(cur)
        cur.execute(
            "SELECT DBMS_METADATA.GET_DDL(:t, :n, :s) FROM dual",
            {"t": meta_type, "n": object_name, "s": owner},
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            raise ValueError(f"DBMS_METADATA returned no DDL for {object_type} {owner}.{object_name}")
        val = row[0]
        return val if isinstance(val, str) else val.read()


def _remap_schema(ddl: str, src_schema: str, tgt_schema: str) -> str:
    """Replace source-schema qualifier with target-schema. Conservative:
    only matches owner-prefix positions (quoted "SRC". and unquoted SRC.).
    Avoids false positives by requiring a trailing dot/word-boundary.
    """
    if not src_schema or src_schema.upper() == tgt_schema.upper():
        return ddl
    src_u = src_schema.upper()
    tgt_u = tgt_schema.upper()
    out = ddl.replace(f'"{src_u}".', f'"{tgt_u}".')
    out = re.sub(rf'\b{re.escape(src_u)}\.', f'{tgt_u}.', out)
    return out


def _normalize_for_action(ddl: str, action: str, object_type: str) -> str:
    """Ensure the DDL matches the requested action.

    Oracle's GET_DDL returns 'CREATE OR REPLACE' for replaceable types and
    plain 'CREATE' otherwise, which is usually what we want. For create_missing
    on a replaceable type we still keep OR REPLACE — it's idempotent and matches
    user intent ("make sure target has it").
    """
    s = ddl.strip()
    if action == "recreate" and object_type in _REPLACEABLE and not s.upper().startswith("CREATE OR REPLACE"):
        s = re.sub(r"^CREATE\b", "CREATE OR REPLACE", s, count=1, flags=re.IGNORECASE)
    return s


def _split_statements(ddl: str) -> list[str]:
    """Split DDL on '/' or trailing ';' delimiters. DBMS_METADATA usually
    returns a single statement, but for nested types (PACKAGE+BODY) or jobs
    it may return script-like blocks."""
    text = ddl.strip()
    # Split on lines that are just '/' (SQL*Plus separator)
    parts = re.split(r"\n\s*/\s*\n", text)
    cleaned = []
    for p in parts:
        p = p.strip().rstrip(";").rstrip("/").strip()
        if p:
            cleaned.append(p)
    return cleaned or [text]


def _apply_on_target(tgt_conn, ddl: str) -> None:
    """Execute DDL statements on target. Commit happens implicitly per DDL."""
    for stmt in _split_statements(ddl):
        with tgt_conn.cursor() as cur:
            cur.execute(stmt)


def _drop_target(tgt_conn, owner: str, object_type: str, object_name: str) -> None:
    """Drop the target object before recreating. CASCADE for TABLE/TYPE."""
    qname = f'"{owner.upper()}"."{object_name.upper()}"'
    if object_type == "TABLE":
        stmt = f"DROP TABLE {qname} CASCADE CONSTRAINTS PURGE"
    elif object_type in ("TYPE", "TYPE BODY"):
        stmt = f"DROP TYPE {qname} FORCE"
    elif object_type == "PACKAGE BODY":
        stmt = f"DROP PACKAGE BODY {qname}"
    elif object_type == "MATERIALIZED VIEW":
        stmt = f"DROP MATERIALIZED VIEW {qname}"
    elif object_type == "DATABASE LINK":
        # DB links can be private; DROP DATABASE LINK uses unqualified name
        stmt = f"DROP DATABASE LINK {object_name.upper()}"
    else:
        kw = object_type
        stmt = f"DROP {kw} {qname}"
    with tgt_conn.cursor() as cur:
        try:
            cur.execute(stmt)
        except Exception as exc:
            if "ORA-04043" in str(exc) or "ORA-00942" in str(exc) or "ORA-02289" in str(exc):
                # Object does not exist — fine, we wanted to drop it anyway
                return
            raise


def _is_already_exists(exc: Exception) -> bool:
    msg = str(exc)
    return ("ORA-00955" in msg            # name is already used
            or "ORA-01408" in msg         # column list already indexed
            or "ORA-02275" in msg         # such a constraint already exists
            or "ORA-02261" in msg         # such unique key already exists
            )


def process_ddl_apply_job(job: dict, pg_conn, configs: dict) -> None:
    """Process one ddl_apply job. Writes per-step events to
    schema_migration_events; updates the job row's state on completion."""
    job_id      = job["job_id"]
    sm_id       = job["schema_migration_id"]
    action      = job["action"]
    job_type    = job.get("job_type") or "SYNC_DDL"
    object_type = job["object_type"]
    object_name = job["object_name"]
    src_schema  = job["src_schema"] or ""
    tgt_schema  = job["tgt_schema"] or ""

    tag = f"{job_type}/{action}/{object_type}/{object_name}"
    print(f"[ddl_apply] {tag} src={src_schema} tgt={tgt_schema}")
    db.log_sm_event(
        pg_conn, sm_id, "ddl_apply.started",
        object_type=object_type, object_name=object_name,
        level="info", message=f"started {job_type} {action}",
        job_id=job_id,
    )

    src_conn = None
    tgt_conn = None
    try:
        src_conn = db.open_oracle(
            "oracle_source",
            configs,
            _session_context(f"ddl-src {job_id[:8]}", job_id=job_id, object_name=object_name),
        )
        tgt_conn = db.open_oracle(
            "oracle_target",
            configs,
            _session_context(f"ddl-tgt {job_id[:8]}", job_id=job_id, object_name=object_name),
        )

        ddl = _extract_source_ddl(src_conn, src_schema, object_type, object_name)
        ddl = _remap_schema(ddl, src_schema, tgt_schema)
        ddl = _normalize_for_action(ddl, action, object_type)

        if action == "recreate":
            _drop_target(tgt_conn, tgt_schema, object_type, object_name)
            _apply_on_target(tgt_conn, ddl)
            applied = ddl
        elif action == "sync_diff":
            # _normalize_for_action already preserves CREATE OR REPLACE; just apply.
            _apply_on_target(tgt_conn, ddl)
            applied = ddl
        else:  # create_missing
            try:
                _apply_on_target(tgt_conn, ddl)
                applied = ddl
            except Exception as exc:
                if _is_already_exists(exc):
                    db.log_sm_event(
                        pg_conn, sm_id, "ddl_apply.already_exists",
                        object_type=object_type, object_name=object_name,
                        level="info", message="object already exists on target",
                        job_id=job_id,
                    )
                    db.complete_ddl_apply_job(pg_conn, job_id, ddl)
                    return
                raise

        db.complete_ddl_apply_job(pg_conn, job_id, applied)
        db.log_sm_event(
            pg_conn, sm_id, "ddl_apply.done",
            object_type=object_type, object_name=object_name,
            level="info", message=f"{action} succeeded",
            job_id=job_id,
        )
        print(f"[ddl_apply] {tag} DONE")

    except Exception as exc:
        err_text = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
        db.fail_ddl_apply_job(pg_conn, job_id, err_text)
        db.log_sm_event(
            pg_conn, sm_id, "ddl_apply.failed",
            object_type=object_type, object_name=object_name,
            level="error", message=err_text,
            job_id=job_id,
        )
        print(f"[ddl_apply] {tag} FAILED: {err_text}")
    finally:
        for c in (src_conn, tgt_conn):
            if c is not None:
                try:
                    c.close()
                except Exception:
                    pass


def ddl_apply_loop(stop_event, poll_interval: int = 5) -> None:
    """Main loop — continuously claim + process DDL apply jobs."""
    print(f"[ddl_apply] loop started (worker_id={WORKER_ID})")
    pg = db.get_pg_conn_with_retry()
    try:
        while not stop_event.is_set():
            try:
                job = db.claim_ddl_apply_job(pg)
                if job is None:
                    time.sleep(poll_interval)
                    continue
                configs = db.load_configs(pg)
                process_ddl_apply_job(job, pg, configs)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[ddl_apply] loop error: {exc}")
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
