"""Schema-migration aggregator.

A schema_migration is the operational unit shown on the Dashboard: one source
schema being migrated to a target schema as a single coordinated effort.
Persistent state lives in `schema_migrations` (metadata only); operational
state — status / stage / progress / KPIs — is *computed* from the child
`migrations` rows (joined via plan_id → migration_plan_items.migration_id) at
read time. This keeps the orchestrator unchanged and avoids sync issues.
"""

import json
from typing import Any

# ── Phase → dashboard-stage map (6 buckets) ─────────────────────────────────
#
# The orchestrator has 30+ phases; the dashboard collapses them into 6
# canonical stages. We take the *furthest-along* stage across all child
# migrations as the schema-level stage.
_STAGE_ORDER = ["assess", "schema", "bulk", "cdc", "validate", "cutover"]

_PHASE_TO_STAGE: dict[str, str] = {
    # assess — precheck / draft
    "DRAFT":               "assess",
    "NEW":                 "assess",
    # schema — DDL prep before data movement
    "TOPIC_CREATING":      "schema",
    # bulk — data loading + stage validation + baseline publish
    "CHUNKING":            "bulk",
    "BULK_LOADING":        "bulk",
    "BULK_LOADED":         "bulk",
    "STAGE_VALIDATING":    "bulk",
    "STAGE_VALIDATED":     "bulk",
    "BASELINE_PUBLISHING": "bulk",
    "BASELINE_LOADING":    "bulk",
    "BASELINE_PUBLISHED":  "bulk",
    # cdc — change-data-capture apply
    "CDC_APPLY_STARTING":  "cdc",
    "CDC_APPLYING":        "cdc",
    "CDC_CATCHING_UP":     "cdc",
    "CDC_CAUGHT_UP":       "cdc",
    "STEADY_STATE":        "cdc",
    # validate — post-load checks, drop stage, enable constraints/indexes
    "STAGE_DROPPING":      "validate",
    "INDEXES_ENABLING":    "validate",
    "DATA_VERIFYING":      "validate",
    "DATA_MISMATCH":       "validate",
    # cutover — terminal
    "COMPLETED":           "cutover",
    "FAILED":              "cutover",
    "CANCELLING":          "cutover",
    "CANCELLED":           "cutover",
    "PAUSED":              "schema",   # paused mid-flow → keep prior stage
}

# Phases considered "active" for KPI counting
_ACTIVE_PHASES = {
    "NEW", "TOPIC_CREATING",
    "CHUNKING", "BULK_LOADING", "BULK_LOADED",
    "STAGE_VALIDATING", "STAGE_VALIDATED",
    "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
    "STAGE_DROPPING", "INDEXES_ENABLING",
    "DATA_VERIFYING", "DATA_MISMATCH",
    "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
    "STEADY_STATE",
}

_DONE_PHASES   = {"COMPLETED"}
_FAILED_PHASES = {"FAILED", "CANCELLED"}


def _phase_to_object_status(phase: str, has_error: bool) -> str:
    """Map a per-migration phase to the dashboard's ObjectStatus enum.

    Matches frontend dashboard/types.ts ObjectStatus.
    """
    if has_error and phase != "COMPLETED":
        return "error"
    if phase == "COMPLETED":
        return "done"
    if phase in ("FAILED",):
        return "error"
    if phase in ("CANCELLED", "CANCELLING"):
        return "skipped"
    if phase == "PAUSED":
        return "paused"
    if phase in ("DATA_VERIFYING", "STAGE_VALIDATING"):
        return "validating"
    if phase in ("DRAFT", "NEW"):
        return "queued"
    if phase in _ACTIVE_PHASES:
        return "running"
    return "queued"


def _aggregate_stage(children_phases: list[str], any_failed: bool) -> str:
    """Furthest-along stage across children; if all done → cutover."""
    if not children_phases:
        return "assess"
    if all(p in _DONE_PHASES for p in children_phases):
        return "cutover"
    indices = [_STAGE_ORDER.index(_PHASE_TO_STAGE.get(p, "assess"))
               for p in children_phases]
    return _STAGE_ORDER[max(indices)]


def _aggregate_status(children_phases: list[str], any_failed: bool, paused: bool) -> str:
    """Frontend SchemaStatus: running/cdc/paused/error/validating/done/queued."""
    if paused:
        return "paused"
    if any_failed:
        return "error"
    if not children_phases:
        return "queued"
    if all(p in _DONE_PHASES for p in children_phases):
        return "done"
    # If any child in CDC phase, consider whole schema in CDC
    cdc_phases = {"CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE"}
    if any(p in cdc_phases for p in children_phases):
        return "cdc"
    if any(p in {"DATA_VERIFYING", "STAGE_VALIDATING"} for p in children_phases):
        return "validating"
    if any(p in _ACTIVE_PHASES for p in children_phases):
        return "running"
    return "queued"


# ── Object table: TABLE migrations from `migrations` table ──────────────────

# Oracle object_type (as stored in ddl_objects) → frontend ObjectType enum
_DDL_TYPE_MAP = {
    "TABLE":             "TABLE",
    "INDEX":             "INDEX",
    "MATERIALIZED VIEW": "MVIEW",
    "PACKAGE":           "PACKAGE",
    "PROCEDURE":         "PROCEDURE",
    "FUNCTION":          "FUNCTION",
    "VIEW":              "VIEW",
    "TRIGGER":           "TRIGGER",
    "SEQUENCE":          "SEQUENCE",
    "SYNONYM":           "SYNONYM",
    "TYPE":              "TYPE",
    "DATABASE LINK":     "DBLINK",
    "JOB":               "JOB",
}

# match_status → (ObjectStatus, compat%, warn_count, err_count, note_prefix)
_MATCH_MAP = {
    "MATCH":   ("done",   100, 0, 0, ""),
    "DIFF":    ("warn",    85, 1, 0, "DDL отличается"),
    "MISSING": ("warn",    90, 1, 0, "нет в target — нужно создать"),
    "EXTRA":   ("skipped",100, 0, 0, "только в target"),
    "ERROR":   ("error",   70, 0, 1, ""),
    "UNKNOWN": ("queued", 100, 0, 0, ""),
}


def _pk_uk_from_meta(meta) -> tuple[bool, bool]:
    """Извлечь наличие PK/UK из metadata.constraints (TABLE).

    get_full_ddl_info кладёт constraints с type_code = 'P'/'U'/'R'/'C'. PK
    считаем по 'P'; UK — по 'U' (включая UNIQUE indexes, которые
    get_table_info добавляет в uk_constraints, но в full_ddl_info их нет —
    fallback на uk_constraints на случай старых снапшотов).
    """
    import json
    if not meta:
        return (False, False)
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            return (False, False)
    if not isinstance(meta, dict):
        return (False, False)
    has_pk = False
    has_uk = False
    for c in meta.get("constraints") or []:
        tcode = (c.get("type_code") or "").upper()
        if tcode == "P":
            has_pk = True
        elif tcode == "U":
            has_uk = True
    if not has_uk and meta.get("uk_constraints"):
        has_uk = True
    if not has_pk and meta.get("pk_columns"):
        has_pk = True
    return (has_pk, has_uk)


def _supp_log_from_meta(meta) -> bool | None:
    """Извлечь supplemental_log_data_all из metadata.

    Поле кладём в TABLE-metadata в get_full_ddl_info: 'YES'/'NO'/None.
    Возвращаем True / False / None (последнее — значит мета без поля,
    скорее всего старый снапшот).
    """
    import json
    if not meta:
        return None
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            return None
    if not isinstance(meta, dict):
        return None
    val = meta.get("supplemental_log_data_all")
    if val is None:
        return None
    val = str(val).upper().strip()
    if val == "YES":
        return True
    if val == "NO":
        return False
    return None


def _ddl_id(fe_type: str, object_name: str) -> str:
    """URL-safe id for DDL object — uses frontend alias (e.g. MVIEW, no spaces)."""
    return f"ddl-{fe_type}-{object_name}"


# Reverse map: frontend type → Oracle canonical (for lookups by id)
_FE_TYPE_TO_ORACLE = {v: k for k, v in _DDL_TYPE_MAP.items()}


def _build_ddl_object(
    object_type: str, object_name: str,
    src_status: str | None, tgt_status: str | None,
    match_status: str, diff_json,
) -> dict:
    """Convert a (ddl_objects + ddl_compare_results) row → SchemaObject.

    src_status / tgt_status: Oracle `oracle_status` для source и target.
    Когда оба INVALID — pre-existing breakage, переводим в warn (не блокирует
    миграцию, но требует человеческого решения). Только source INVALID —
    error: миграция перенесёт сломанный DDL.
    """
    fe_type = _DDL_TYPE_MAP.get(object_type, object_type)
    status, compat, warn, err, note = _MATCH_MAP.get(match_status, _MATCH_MAP["UNKNOWN"])

    src_invalid = (src_status or "").upper() == "INVALID"
    tgt_invalid = (tgt_status or "").upper() == "INVALID"
    if src_invalid and tgt_invalid:
        status = "warn" if status in ("done", "queued") else status
        warn = max(warn, 1)
        note = "INVALID в обоих — проверить" if not note else f"INVALID в обоих; {note}"
    elif src_invalid:
        status = "error"
        err = max(err, 1)
        note = "INVALID в source — миграция перенесёт ошибку"
    elif tgt_invalid:
        status = "warn" if status == "done" else status
        warn = max(warn, 1)
        note = "INVALID в target — требуется пересоздать"

    # Light note from diff (first failing field)
    if not note and isinstance(diff_json, dict):
        if not diff_json.get("ok", True):
            for k, v in diff_json.items():
                if k == "ok" or not v:
                    continue
                if isinstance(v, bool) and v is False:
                    note = f"{k.replace('_match', '').replace('_', ' ')} mismatch"
                    break

    return {
        "id":         _ddl_id(fe_type, object_name),
        "type":       fe_type,
        "name":       object_name,
        "rows":       None,
        "rowsDone":   None,
        "sizeMb":     0,
        "status":     status,
        "progress":   100 if status == "done" else 0,
        "rowsPerSec": 0,
        "mbPerSec":   0,
        "compat":     compat,
        "warn":       warn,
        "err":        err,
        "eta":        "—",
        "dur":        "<1s",
        "note":       note,
        "srcStatus":  src_status or "",
        "tgtStatus":  tgt_status or "",
    }


def _build_object(row: dict) -> dict:
    """Convert a `migrations` row into a dashboard SchemaObject."""
    has_error = bool(row.get("error_text"))
    status    = _phase_to_object_status(row.get("phase", ""), has_error)
    rows_loaded = row.get("rows_loaded") or 0
    total_rows  = row.get("total_rows")
    progress = 0.0
    if total_rows and total_rows > 0:
        progress = min(100.0, (rows_loaded / total_rows) * 100.0)
    elif status == "done":
        progress = 100.0
    return {
        "id":        str(row.get("migration_id")),
        "type":      "TABLE",
        "name":      row.get("source_table") or row.get("migration_name") or "—",
        "rows":      total_rows,
        "rowsDone":  rows_loaded,
        "sizeMb":    0,                            # not yet captured in `migrations`
        "status":    status,
        "progress":  round(progress, 1),
        "rowsPerSec": 0,                           # would come from worker metrics
        "mbPerSec":  0,
        "compat":    100,                          # tables don't have PL/SQL compat
        "warn":      0,
        "err":       1 if has_error else 0,
        "eta":       "—",
        "dur":       "—",
        "note":      row.get("error_text") or "",
        "strategy":   row.get("strategy")            or "",
        "keyType":    row.get("effective_key_type")  or "",
        "hasPk":      bool(row.get("source_pk_exists")),
        "hasUk":      bool(row.get("source_uk_exists")),
    }


# ── Public API ──────────────────────────────────────────────────────────────

def list_schema_migrations(conn) -> list[dict]:
    """List all schema_migrations with computed status/stage/progress."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                sm.schema_migration_id, sm.name, sm.src_schema, sm.tgt_schema,
                sm.source_host, sm.source_version, sm.target_host, sm.target_version,
                sm.priority, sm.owner, sm.plan_id, sm.group_id, sm.paused,
                sm.started_at, sm.window_at, sm.completed_at,
                sm.created_at, sm.updated_at
            FROM schema_migrations sm
            ORDER BY sm.created_at DESC
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        result = []
        for r in rows:
            sm = dict(zip(cols, r))
            child = _load_children(conn, sm["plan_id"],
                                   sm.get("src_schema"), sm.get("tgt_schema"))
            result.append(_to_dashboard_view(sm, child))
        return result


def get_schema_migration(conn, sm_id: str) -> dict | None:
    """Load one schema_migration (without objects/events — those have own endpoints)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                sm.schema_migration_id, sm.name, sm.src_schema, sm.tgt_schema,
                sm.source_host, sm.source_version, sm.target_host, sm.target_version,
                sm.priority, sm.owner, sm.description, sm.plan_id, sm.group_id,
                sm.paused, sm.started_at, sm.window_at, sm.completed_at,
                sm.created_at, sm.updated_at
            FROM schema_migrations sm
            WHERE sm.schema_migration_id = %s
        """, (sm_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        sm = dict(zip(cols, row))
        child = _load_children(conn, sm["plan_id"],
                               sm.get("src_schema"), sm.get("tgt_schema"))
        return _to_dashboard_view(sm, child)


def get_objects(conn, sm_id: str) -> list[dict]:
    """Return SchemaObject[]: union of
       (a) TABLE rows from migrations linked to this schema_migration's plan,
       (b) all source-side objects from the latest ddl_snapshot for this
           schema pair — TABLE rows already covered by (a) are skipped to
           avoid duplicates, non-TABLE DDL is always included.
    Tables present in the snapshot but not yet in any migration appear as
    `queued` — they're known to exist on source but not planned yet."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src_schema, tgt_schema FROM schema_migrations WHERE schema_migration_id = %s",
            (sm_id,),
        )
        row = cur.fetchone()
        if not row:
            return []
        src_schema, tgt_schema = row

    # (a) TABLE objects driven by migrations — they have phase/progress/errors.
    # Project only the columns _build_object actually reads — avoids dragging
    # 30+ migration metadata columns over the wire on every 5s poll.
    #
    # Источники миграций: (1) явно прилинкованные через plan_id →
    # migration_plan_items (планнер); (2) созданные напрямую через
    # POST /api/migrations с совпадающей парой schema (source_schema/target_schema =
    # sm.src_schema/tgt_schema) — такие миграции могут быть orphan-row без plan_id,
    # но логически принадлежат этому schema_migration.
    tables: list[dict] = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.migration_id, m.migration_name, m.phase, m.error_text,
                   m.source_table, m.total_rows, m.rows_loaded,
                   m.strategy, m.effective_key_type,
                   m.source_pk_exists, m.source_uk_exists
            FROM migrations m
            WHERE m.migration_id IN (
                SELECT mpi.migration_id
                FROM   migration_plan_items mpi
                JOIN   schema_migrations sm ON sm.plan_id = mpi.plan_id
                WHERE  sm.schema_migration_id = %s
            )
               OR (UPPER(m.source_schema) = UPPER(%s)
                   AND UPPER(m.target_schema) = UPPER(%s))
            ORDER BY m.created_at
        """, (sm_id, src_schema or "", tgt_schema or ""))
        cols = [d[0] for d in cur.description]
        tables = [_build_object(dict(zip(cols, r))) for r in cur.fetchall()]
    migrated_table_names = {t["name"].upper() for t in tables}

    # Подгрузим supplemental_log_data_all для уже прилинкованных миграций,
    # чтобы рядом с CDC-чипом можно было показать SUPP/NO SUPP. Один батч-
    # запрос к ddl_objects.metadata по последнему snapshot для данной пары
    # схем — то же, что используется ниже для orphan DDL.
    if tables:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT o.object_name, o.metadata
                FROM   ddl_objects o
                WHERE  o.snapshot_id = (
                            SELECT snapshot_id FROM ddl_snapshots
                            WHERE  src_schema = %s AND tgt_schema = %s
                            ORDER  BY loaded_at DESC
                            FETCH  FIRST 1 ROWS ONLY)
                  AND  o.db_side    = 'source'
                  AND  o.object_type = 'TABLE'
                  AND  o.object_name = ANY(%s)
            """, (src_schema, tgt_schema, [t["name"].upper() for t in tables]))
            meta_by_name = {r[0]: r[1] for r in cur.fetchall()}
        for tbl in tables:
            meta = meta_by_name.get(tbl["name"].upper())
            tbl["hasSuppLog"] = _supp_log_from_meta(meta)

    # (b) DDL objects from the most recent snapshot
    ddl_objects: list[dict] = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT snapshot_id FROM ddl_snapshots
            WHERE src_schema = %s AND tgt_schema = %s
            ORDER BY loaded_at DESC
            FETCH FIRST 1 ROWS ONLY
        """, (src_schema, tgt_schema))
        snap = cur.fetchone()
        if snap:
            snapshot_id = snap[0]
            # Oracle-internal names (SYS_C / SYS_IL / SYS_LOB / ISEQ$$ / BIN$
            # / DR$) are auto-generated and always differ between databases —
            # synchronising them is pointless. Existing snapshots from before
            # the catalog-side filter still contain them, so we filter again
            # here as a defensive measure.
            # Skip ddl_compare_results.diff (heavy JSONB blob) in the listing —
            # it's only ever consumed by /objects/:id/detail. Saves up to MBs
            # of payload per poll for schemas with thousands of objects.
            cur.execute("""
                SELECT s.object_type, s.object_name,
                       s.oracle_status        AS src_status,
                       tg.oracle_status       AS tgt_status,
                       COALESCE(c.match_status, 'UNKNOWN') AS match_status,
                       s.metadata             AS src_meta
                FROM   ddl_objects s
                LEFT   JOIN ddl_objects tg
                       ON tg.snapshot_id = s.snapshot_id
                       AND tg.db_side = 'target'
                       AND tg.object_type = s.object_type
                       AND tg.object_name = s.object_name
                LEFT   JOIN ddl_compare_results c
                       ON c.snapshot_id = s.snapshot_id
                       AND c.object_type = s.object_type
                       AND c.object_name = s.object_name
                WHERE  s.snapshot_id = %s
                  AND  s.db_side = 'source'
                  AND  s.object_name NOT LIKE 'SYS\\_C%%' ESCAPE '\\'
                  AND  s.object_name NOT LIKE 'SYS\\_IL%%' ESCAPE '\\'
                  AND  s.object_name NOT LIKE 'SYS\\_LOB%%' ESCAPE '\\'
                  AND  s.object_name NOT LIKE 'ISEQ$$%%'
                  AND  s.object_name NOT LIKE 'BIN$%%'
                  AND  s.object_name NOT LIKE 'DR$%%'
                ORDER BY s.object_type, s.object_name
            """, (snapshot_id,))
            for r in cur.fetchall():
                otype, oname, src_status, tgt_status, match_status, src_meta = r
                # Skip TABLE rows already represented by a migration
                if otype == "TABLE" and oname.upper() in migrated_table_names:
                    continue
                obj = _build_ddl_object(
                    otype, oname, src_status, tgt_status, match_status, None,
                )
                # Для TABLE без миграции совпадение DDL не означает перенос
                # данных — таблица может быть пустой или хранить устаревшие
                # данные. Понижаем "done" до "queued", сохраняя error/warn
                # для сломанных/расходящихся DDL.
                if otype == "TABLE" and obj["status"] == "done":
                    obj["status"]   = "queued"
                    obj["progress"] = 0
                    obj["note"]     = obj["note"] or "миграция данных не запущена"
                # Достаём наличие PK/UK из метаданных source-side TABLE,
                # чтобы дашборд мог показать чип ключа ещё до создания
                # миграции.
                if otype == "TABLE":
                    has_pk, has_uk = _pk_uk_from_meta(src_meta)
                    obj["hasPk"] = has_pk
                    obj["hasUk"] = has_uk
                    obj["hasSuppLog"] = _supp_log_from_meta(src_meta)
                ddl_objects.append(obj)

    # Stable ID space: TABLE migrations use UUID strings; DDL objects get
    # synthetic "ddl-<TYPE>-<NAME>" IDs to avoid collisions.
    return tables + ddl_objects


def get_object_detail(conn, sm_id: str, obj_id: str) -> dict | None:
    """Detailed view of one object for the Drawer.

    obj_id formats:
      - "ddl-<TYPE>-<NAME>" → DDL object: returns src_meta + tgt_meta + diff +
        match_status from latest snapshot.
      - "<uuid>"            → migration row: returns full migrations row +
        recent state_history + (optional) DDL diff from snapshot if any.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src_schema, tgt_schema FROM schema_migrations WHERE schema_migration_id = %s",
            (sm_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        src_schema, tgt_schema = row

    if obj_id.startswith("ddl-"):
        # Format: "ddl-<FE_TYPE>-<NAME>" — FE_TYPE is the frontend alias
        # (TABLE/VIEW/MVIEW/PACKAGE/...) without spaces. Oracle object names
        # never contain '-' unless quoted, so partition on first '-' is safe.
        rest = obj_id[len("ddl-"):]
        fe_type, _, oname = rest.partition("-")
        otype = _FE_TYPE_TO_ORACLE.get(fe_type, fe_type)
        return _load_ddl_detail(conn, src_schema, tgt_schema, otype, oname)
    else:
        return _load_migration_detail(conn, src_schema, tgt_schema, obj_id)


def _load_ddl_detail(conn, src_schema: str, tgt_schema: str,
                     object_type: str, object_name: str) -> dict | None:
    import json
    with conn.cursor() as cur:
        cur.execute("""
            SELECT snapshot_id FROM ddl_snapshots
            WHERE src_schema = %s AND tgt_schema = %s
            ORDER BY loaded_at DESC FETCH FIRST 1 ROWS ONLY
        """, (src_schema, tgt_schema))
        snap = cur.fetchone()
        if not snap:
            return {"kind": "ddl", "found": False, "object_type": object_type, "object_name": object_name}
        snapshot_id = snap[0]
        cur.execute("""
            SELECT db_side, metadata, oracle_status, last_ddl_time FROM ddl_objects
            WHERE snapshot_id = %s AND object_type = %s AND object_name = %s
        """, (snapshot_id, object_type, object_name))
        result: dict = {"kind": "ddl", "found": True,
                        "object_type": object_type, "object_name": object_name,
                        "source": None, "target": None}
        for r in cur.fetchall():
            side, meta, ostatus, last_ddl = r
            meta_obj = meta if isinstance(meta, dict) else (json.loads(meta) if meta else {})
            result[side] = {
                "metadata":      meta_obj,
                "oracle_status": ostatus,
                "last_ddl_time": last_ddl.isoformat() if last_ddl else None,
            }
        cur.execute("""
            SELECT match_status, diff FROM ddl_compare_results
            WHERE snapshot_id = %s AND object_type = %s AND object_name = %s
        """, (snapshot_id, object_type, object_name))
        cr = cur.fetchone()
        if cr:
            result["match_status"] = cr[0]
            result["diff"] = cr[1] if isinstance(cr[1], dict) else (json.loads(cr[1]) if cr[1] else {})
        else:
            result["match_status"] = "UNKNOWN"
            result["diff"] = {}
        return result


def _load_migration_detail(conn, src_schema: str, tgt_schema: str, migration_id: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM migrations WHERE migration_id = %s", (migration_id,))
        row = cur.fetchone()
        if not row:
            return {"kind": "migration", "found": False, "migration_id": migration_id}
        cols = [d[0] for d in cur.description]
        m = dict(zip(cols, row))
        cur.execute("""
            SELECT id, from_phase, to_phase, transition_status, transition_reason,
                   message, actor_type, actor_id, created_at
            FROM migration_state_history
            WHERE migration_id = %s
            ORDER BY created_at DESC
            LIMIT 50
        """, (migration_id,))
        hist_cols = [d[0] for d in cur.description]
        history = []
        for r in cur.fetchall():
            h = dict(zip(hist_cols, r))
            if h.get("created_at"):
                h["created_at"] = h["created_at"].isoformat()
            history.append(h)

    # Strip non-JSON-serialisable types
    def _clean(v):
        if hasattr(v, "isoformat"):
            return v.isoformat()
        if isinstance(v, (bytes, bytearray, memoryview)):
            return None
        return v
    m_clean = {k: _clean(v) for k, v in m.items()}
    m_clean["migration_id"] = str(m_clean.get("migration_id") or "")
    if m_clean.get("group_id"):
        m_clean["group_id"] = str(m_clean["group_id"])

    # Optional DDL diff for the same table in latest snapshot
    table_name = m.get("source_table") or ""
    ddl_diff = None
    if table_name:
        ddl_diff = _load_ddl_detail(conn, src_schema, tgt_schema, "TABLE", table_name)

    return {
        "kind":      "migration",
        "found":     True,
        "migration": m_clean,
        "history":   history,
        "ddl_diff":  ddl_diff,
    }


def _matched_migrations_subquery() -> str:
    """SQL-фрагмент: миграции, относящиеся к schema_migration <sm_id>.

    Возвращает подзапрос (без скобок), пригодный для использования как:
        m.migration_id IN ({_matched_migrations_subquery()})

    Принимает 4 параметра (в этом порядке): sm_id, sm_id, src_schema, tgt_schema.

    Подбор по трём путям:
      1) plan-flow:   schema_migrations.plan_id → migration_plan_items
      2) group-flow:  schema_migrations.group_id → migrations.group_id
      3) schema-pair: совпадение src_schema/tgt_schema (case-insensitive)
    Любой путь даёт hit — нужный для случаев, когда у sm пустые src/tgt,
    либо у миграции нет ни plan_id, ни group_id.
    """
    return """
        SELECT mpi.migration_id
        FROM   migration_plan_items mpi
        JOIN   schema_migrations sm ON sm.plan_id = mpi.plan_id
        WHERE  sm.schema_migration_id = %s
        UNION
        SELECT m2.migration_id
        FROM   migrations m2
        JOIN   schema_migrations sm2 ON sm2.group_id = m2.group_id
        WHERE  sm2.schema_migration_id = %s
          AND  m2.group_id IS NOT NULL
        UNION
        SELECT m3.migration_id
        FROM   migrations m3
        WHERE  %s <> '' AND %s <> ''
          AND  UPPER(m3.source_schema) = UPPER(%s)
          AND  UPPER(m3.target_schema) = UPPER(%s)
    """


def get_events(conn, sm_id: str, limit: int = 100) -> list[dict]:
    """Merge two event streams (per-migration phase history + schema-level
    events such as DDL apply jobs), ordered by time, newest first."""
    events: list[tuple] = []  # (created_at, obj, level, msg)

    # Get schema pair to also pick up orphan migrations matched by schema.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src_schema, tgt_schema FROM schema_migrations WHERE schema_migration_id = %s",
            (sm_id,),
        )
        row = cur.fetchone()
        src_schema, tgt_schema = (row[0], row[1]) if row else ("", "")

    src = src_schema or ""
    tgt = tgt_schema or ""

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                m.source_table AS obj,
                msh.created_at,
                msh.from_phase, msh.to_phase, msh.transition_status,
                msh.transition_reason, msh.message
            FROM migrations m
            JOIN migration_state_history msh ON msh.migration_id = m.migration_id
            WHERE m.migration_id IN ({_matched_migrations_subquery()})
            ORDER BY msh.created_at DESC
            LIMIT %s
        """, (sm_id, sm_id, src, tgt, src, tgt, limit))
        for r in cur.fetchall():
            obj, created_at, from_phase, to_phase, status, _reason, message = r
            level = "error" if status == "FAILED" else "warn" if to_phase == "DATA_MISMATCH" else "info"
            msg = message or f"{from_phase or '—'} → {to_phase}"
            events.append((created_at, obj or "—", level, msg))

        cur.execute("""
            SELECT created_at, object_type, object_name, event_type, level, message
            FROM   schema_migration_events
            WHERE  schema_migration_id = %s
            ORDER BY created_at DESC
            LIMIT  %s
        """, (sm_id, limit))
        for r in cur.fetchall():
            created_at, otype, oname, event_type, lvl, message = r
            obj = oname or otype or "—"
            msg = message or event_type
            events.append((created_at, obj, lvl or "info", msg))

    # Колонки created_at в двух исходных таблицах разного типа:
    # migration_state_history — TIMESTAMP (naive), schema_migration_events —
    # TIMESTAMPTZ (aware). При сортировке Python падает с
    # "can't compare offset-naive and offset-aware datetimes". Приводим
    # к единому формату — naive UTC — на этапе ключа сортировки.
    from datetime import datetime
    def _sort_key(e):
        v = e[0]
        if v is None:
            return datetime.min
        if hasattr(v, "tzinfo") and v.tzinfo is not None:
            return v.replace(tzinfo=None)
        return v

    events.sort(key=_sort_key, reverse=True)
    out = []
    for created_at, obj, level, msg in events[:limit]:
        out.append({
            "t":     created_at.strftime("%H:%M:%S") if created_at else "",
            "obj":   obj,
            "level": level,
            "msg":   msg,
        })
    return out


def _collect_side_metrics(side: str) -> dict:
    """Open `oracle_<side>` and pull current + historical V$SYSMETRIC.
    Silent fallback to zeros on any failure (V$ permissions, network, etc)."""
    out = {
        "cpu": 0.0, "net_mb_s": 0.0, "redo_bps": 0,
        "cpu_hist": [], "net_hist": [], "redo_hist": [],
    }
    try:
        from db.oracle_browser import get_oracle_conn, get_v_sysmetric, get_v_sysmetric_history
        from db.state_db import load_configs

        configs = load_configs(True)
        ora = get_oracle_conn(side, configs)
        try:
            cur_vals  = get_v_sysmetric(ora)
            hist_vals = get_v_sysmetric_history(ora, limit_per_metric=10)
        finally:
            try: ora.close()
            except Exception: pass

        out["cpu"]      = round(float(cur_vals.get("Host CPU Utilization (%)") or 0), 1)
        net_bps         = float(cur_vals.get("Network Traffic Volume Per Sec") or 0)
        out["net_mb_s"] = round(net_bps / (1024 * 1024), 2)
        out["redo_bps"] = int(cur_vals.get("Redo Generated Per Sec") or 0)
        out["cpu_hist"]  = [round(float(v), 1) for v in hist_vals.get("Host CPU Utilization (%)") or []]
        out["net_hist"]  = [round(float(v) / (1024 * 1024), 2)
                            for v in hist_vals.get("Network Traffic Volume Per Sec") or []]
        out["redo_hist"] = [int(v) for v in hist_vals.get("Redo Generated Per Sec") or []]
    except Exception as exc:
        print(f"[metrics] {side}-side metrics failed: {exc}")
    return out


def get_metrics(conn, sm_id: str) -> dict:
    """Live metrics: V$SYSMETRIC from both source and target + CDC lag.
    Failures degrade silently to zeros so the dashboard never crashes."""
    src = _collect_side_metrics("source")
    tgt = _collect_side_metrics("target")

    # Резолвим связанные миграции тем же путём, что и get_events:
    # plan_id, group_id или schema-pair (см. _matched_migrations_subquery).
    cdc_lag_total = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT src_schema, tgt_schema FROM schema_migrations WHERE schema_migration_id = %s",
                (sm_id,),
            )
            sm_row = cur.fetchone()
            sm_src = (sm_row[0] if sm_row else "") or ""
            sm_tgt = (sm_row[1] if sm_row else "") or ""

            cur.execute(f"""
                SELECT COALESCE(SUM(cs.total_lag), 0)
                FROM   migration_cdc_state cs
                WHERE  cs.migration_id IN ({_matched_migrations_subquery()})
            """, (sm_id, sm_id, sm_src, sm_tgt, sm_src, sm_tgt))
            row = cur.fetchone()
            cdc_lag_total = int(row[0] or 0) if row else 0
    except Exception as exc:
        print(f"[metrics] CDC lag query failed: {exc}")

    def _pad10(arr: list) -> list:
        return ([0] * (10 - len(arr)) + arr)[-10:] if arr else [0] * 10

    return {
        # Source
        "sourceCpu":  src["cpu"],
        "network":    src["net_mb_s"],
        "redoPerSec": src["redo_bps"],
        "cpuSpark":   _pad10(src["cpu_hist"]),
        "netSpark":   _pad10(src["net_hist"]),
        "redoSpark":  _pad10(src["redo_hist"]),
        # Target
        "targetCpu":        tgt["cpu"],
        "targetNetwork":    tgt["net_mb_s"],
        "targetRedoPerSec": tgt["redo_bps"],
        "targetCpuSpark":   _pad10(tgt["cpu_hist"]),
        "targetNetSpark":   _pad10(tgt["net_hist"]),
        "targetRedoSpark":  _pad10(tgt["redo_hist"]),
        # CDC: суммарный consumer-group lag по всем CDC-миграциям схемы,
        # в сообщениях (offset_end − committed). Не миллисекунды.
        "cdcLag":   cdc_lag_total,
        "lagSpark": [0] * 10,
    }


def create_schema_migration(conn, payload: dict) -> str:
    """Create a new schema_migration row. Returns its UUID."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO schema_migrations (
                name, src_schema, tgt_schema,
                source_host, source_version, target_host, target_version,
                priority, owner, description, plan_id, group_id, window_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING schema_migration_id
        """, (
            payload.get("name") or payload.get("src_schema") or "",
            payload.get("src_schema") or "",
            payload.get("tgt_schema") or "",
            payload.get("source_host"),
            payload.get("source_version"),
            payload.get("target_host"),
            payload.get("target_version"),
            payload.get("priority", "P2"),
            payload.get("owner"),
            payload.get("description"),
            payload.get("plan_id"),
            payload.get("group_id"),
            payload.get("window_at"),
        ))
        new_id = cur.fetchone()[0]
        conn.commit()
        return str(new_id)


def set_paused(conn, sm_id: str, paused: bool) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE schema_migrations
            SET paused = %s, updated_at = NOW()
            WHERE schema_migration_id = %s
        """, (paused, sm_id))
        affected = cur.rowcount
        conn.commit()
        return affected > 0


# ── Internal helpers ────────────────────────────────────────────────────────

def _load_children(conn, plan_id: int | None,
                   src_schema: str | None = None,
                   tgt_schema: str | None = None) -> list[dict]:
    """Fetch child migrations: union of (1) plan-linked via migration_plan_items
    and (2) orphan migrations matched by (source_schema, target_schema) pair."""
    has_plan = plan_id is not None
    has_pair = bool(src_schema and tgt_schema)
    if not has_plan and not has_pair:
        return []

    if has_plan and has_pair:
        sql = """
            SELECT m.migration_id, m.phase, m.error_text,
                   m.total_rows, m.rows_loaded, m.source_table
            FROM migrations m
            WHERE m.migration_id IN (
                SELECT mpi.migration_id FROM migration_plan_items mpi
                WHERE mpi.plan_id = %s
            )
               OR (UPPER(m.source_schema) = UPPER(%s)
                   AND UPPER(m.target_schema) = UPPER(%s))
        """
        params = (plan_id, src_schema, tgt_schema)
    elif has_plan:
        sql = """
            SELECT m.migration_id, m.phase, m.error_text,
                   m.total_rows, m.rows_loaded, m.source_table
            FROM migration_plan_items mpi
            JOIN migrations m ON m.migration_id = mpi.migration_id
            WHERE mpi.plan_id = %s
        """
        params = (plan_id,)
    else:
        sql = """
            SELECT m.migration_id, m.phase, m.error_text,
                   m.total_rows, m.rows_loaded, m.source_table
            FROM migrations m
            WHERE UPPER(m.source_schema) = UPPER(%s)
              AND UPPER(m.target_schema) = UPPER(%s)
        """
        params = (src_schema, tgt_schema)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _to_dashboard_view(sm: dict, children: list[dict]) -> dict:
    phases = [c["phase"] for c in children]
    any_failed = any(c.get("error_text") for c in children) or any(p == "FAILED" for p in phases)
    paused = bool(sm.get("paused"))

    status = _aggregate_status(phases, any_failed, paused)
    stage  = _aggregate_stage(phases, any_failed)

    total_rows = sum((c.get("total_rows") or 0) for c in children)
    rows_done  = sum((c.get("rows_loaded") or 0) for c in children)
    progress   = (rows_done / total_rows * 100.0) if total_rows > 0 else 0.0

    done_count = sum(1 for p in phases if p in _DONE_PHASES)
    err_count  = sum(1 for c in children if c.get("error_text"))

    def _iso(v: Any) -> str | None:
        if v is None:
            return None
        if hasattr(v, "isoformat"):
            return v.isoformat() + ("Z" if v.tzinfo is None else "")
        return str(v)

    return {
        "id":        str(sm["schema_migration_id"]),
        "name":      sm["name"],
        "source":    {
            "host":    sm.get("source_host") or "",
            "version": sm.get("source_version") or "",
            "tns":     sm.get("src_schema") or "",
        },
        "target":    {
            "host":    sm.get("target_host") or "",
            "version": sm.get("target_version") or "",
            "tns":     sm.get("tgt_schema") or "",
        },
        "src_schema": sm.get("src_schema"),
        "tgt_schema": sm.get("tgt_schema"),
        "owner":      sm.get("owner") or "",
        "priority":   sm.get("priority") or "P2",
        "status":     status,
        "stage":      stage,
        "startedAt":  _iso(sm.get("started_at")) or _iso(sm.get("created_at")),
        "windowAt":   _iso(sm.get("window_at")),
        "schemaCompat": 100.0,
        "sizeGb":     0,
        "totals":     { "rowsPerSec": 0, "mbPerSec": 0 },
        "kpi":        {
            "totalObjects":  len(children),
            "doneObjects":   done_count,
            "errorObjects":  err_count,
            "totalRows":     total_rows,
            "rowsDone":      rows_done,
            "progress":      round(progress, 1),
        },
        "planId":   sm.get("plan_id"),
        "groupId":  str(sm["group_id"]) if sm.get("group_id") else None,
        "paused":   paused,
    }
