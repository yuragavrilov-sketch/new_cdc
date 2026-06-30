"""
DDL Catalog API — cache Oracle DDL objects in PostgreSQL, compare, sync.
"""
import json
from flask import Blueprint, request, jsonify
from db.oracle_browser import (
    get_oracle_conn, list_all_objects, get_full_ddl_info,
    get_view_info, get_mview_info, get_code_info,
    get_sequence_info, get_synonym_info,
    get_trigger_info, get_index_info, get_db_link_info, get_job_info,
)
from services.ddl_compare import compare_object
from services.ddl_sync_extended import sync_to_target

bp = Blueprint("catalog", __name__)
_state: dict = {}


def init(*, get_conn_fn, row_to_dict_fn, load_configs_fn, broadcast_fn):
    _state["get_conn"] = get_conn_fn
    _state["row_to_dict"] = row_to_dict_fn
    _state["load_configs"] = load_configs_fn
    _state["broadcast"] = broadcast_fn


_META_FETCHERS = {
    "TABLE":             lambda conn, schema, name: get_full_ddl_info(conn, schema, name),
    "VIEW":              lambda conn, schema, name: get_view_info(conn, schema, name),
    "MATERIALIZED VIEW": lambda conn, schema, name: get_mview_info(conn, schema, name),
    "FUNCTION":          lambda conn, schema, name: get_code_info(conn, schema, name, "FUNCTION"),
    "PROCEDURE":         lambda conn, schema, name: get_code_info(conn, schema, name, "PROCEDURE"),
    "PACKAGE":           lambda conn, schema, name: get_code_info(conn, schema, name, "PACKAGE"),
    "TYPE":              lambda conn, schema, name: get_code_info(conn, schema, name, "TYPE"),
    "SEQUENCE":          lambda conn, schema, name: get_sequence_info(conn, schema, name),
    "SYNONYM":           lambda conn, schema, name: get_synonym_info(conn, schema, name),
    "TRIGGER":           lambda conn, schema, name: get_trigger_info(conn, schema, name),
    "INDEX":             lambda conn, schema, name: get_index_info(conn, schema, name),
    "DATABASE LINK":     lambda conn, schema, name: get_db_link_info(conn, schema, name),
    "JOB":               lambda conn, schema, name: get_job_info(conn, schema, name),
}


def _fetch_metadata(conn, schema: str, obj_type: str, obj_name: str) -> dict:
    fetcher = _META_FETCHERS.get(obj_type)
    if not fetcher:
        return {}
    try:
        return fetcher(conn, schema, obj_name)
    except Exception as exc:
        return {"_error": str(exc)}


@bp.get("/api/catalog/snapshots")
def list_snapshots():
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT snapshot_id, src_schema, tgt_schema, loaded_at
                FROM   ddl_snapshots
                ORDER  BY loaded_at DESC
                LIMIT  20
            """)
            rows = [_state["row_to_dict"](cur, r) for r in cur.fetchall()]
        return jsonify(rows)
    finally:
        conn.close()


@bp.get("/api/catalog/snapshots/latest")
def latest_snapshot():
    """Return the most recent snapshot for a schema pair with object_counts.
    Used by Dashboard's LoadSnapshotBanner to show 'last loaded X ago · N objects'.
    Returns 200 with null when no snapshot exists for the pair."""
    src_schema = (request.args.get("src_schema") or "").upper()
    tgt_schema = (request.args.get("tgt_schema") or "").upper()
    if not src_schema or not tgt_schema:
        return jsonify({"error": "src_schema and tgt_schema required"}), 400
    conn = _state["get_conn"]()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT snapshot_id, loaded_at
                FROM   ddl_snapshots
                WHERE  src_schema = %s AND tgt_schema = %s
                ORDER  BY loaded_at DESC
                FETCH  FIRST 1 ROWS ONLY
            """, (src_schema, tgt_schema))
            row = cur.fetchone()
            if not row:
                return jsonify(None)
            snapshot_id, loaded_at = row
            # Per-type counts on source side
            cur.execute("""
                SELECT object_type, COUNT(*) FROM ddl_objects
                WHERE snapshot_id = %s AND db_side = 'source'
                GROUP BY object_type
                ORDER BY object_type
            """, (snapshot_id,))
            counts = {r[0]: r[1] for r in cur.fetchall()}
            # Match status summary
            cur.execute("""
                SELECT match_status, COUNT(*) FROM ddl_compare_results
                WHERE snapshot_id = %s
                GROUP BY match_status
            """, (snapshot_id,))
            match_summary = {r[0]: r[1] for r in cur.fetchall()}
        return jsonify({
            "snapshot_id":   snapshot_id,
            "loaded_at":     loaded_at.isoformat() + "Z" if loaded_at and loaded_at.tzinfo is None else (loaded_at.isoformat() if loaded_at else None),
            "object_counts": counts,
            "match_summary": match_summary,
            "total":         sum(counts.values()),
        })
    finally:
        conn.close()


@bp.post("/api/catalog/load")
def load_catalog():
    """Load full DDL catalog for a schema pair into cache."""
    data = request.get_json(force=True)
    src_schema = (data.get("src_schema") or "").upper()
    tgt_schema = (data.get("tgt_schema") or "").upper()
    if not src_schema or not tgt_schema:
        return jsonify({"error": "src_schema and tgt_schema required"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs, prefer_owner=True)
        tgt_conn = get_oracle_conn("target", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    pg = _state["get_conn"]()
    try:
        with pg.cursor() as cur:
            cur.execute(
                "INSERT INTO ddl_snapshots (src_schema, tgt_schema) VALUES (%s, %s) RETURNING snapshot_id",
                (src_schema, tgt_schema),
            )
            snapshot_id = cur.fetchone()[0]

        broadcast = _state["broadcast"]
        def _progress(phase: str, done: int, total: int, current: str = ""):
            broadcast({
                "type":       "ddl_snapshot.progress",
                "src_schema": src_schema,
                "tgt_schema": tgt_schema,
                "snapshot_id": snapshot_id,
                "phase":      phase,
                "done":       done,
                "total":      total,
                "current":    current,
            })

        _progress("listing", 0, 0, "перечисляем объекты источника…")
        src_objects = list_all_objects(src_conn, src_schema)
        src_types = {}
        for o in src_objects:
            src_types[o["object_type"]] = src_types.get(o["object_type"], 0) + 1
        print(f"[catalog] source {src_schema}: {src_types}")

        _progress("listing", 0, 0, "перечисляем объекты таргета…")
        tgt_objects = list_all_objects(tgt_conn, tgt_schema)

        total_objects = len(src_objects) + len(tgt_objects)
        object_counts: dict[str, int] = {}
        src_meta_cache: dict[tuple, dict] = {}
        done_count = 0

        for obj in src_objects:
            otype = obj["object_type"]
            oname = obj["object_name"]
            object_counts[otype] = object_counts.get(otype, 0) + 1

            meta = _fetch_metadata(src_conn, src_schema, otype, oname)
            src_meta_cache[(otype, oname)] = meta

            with pg.cursor() as cur:
                cur.execute("""
                    INSERT INTO ddl_objects (snapshot_id, db_side, object_type, object_name,
                                            oracle_status, last_ddl_time, metadata)
                    VALUES (%s, 'source', %s, %s, %s, %s, %s)
                """, (snapshot_id, otype, oname, obj["status"], obj["last_ddl_time"],
                      json.dumps(meta, default=str)))

            done_count += 1
            if done_count % 5 == 0 or done_count == len(src_objects):
                _progress("source", done_count, total_objects, f"{otype} {oname}")

        tgt_meta_cache: dict[tuple, dict] = {}
        for obj in tgt_objects:
            otype = obj["object_type"]
            oname = obj["object_name"]
            meta = _fetch_metadata(tgt_conn, tgt_schema, otype, oname)
            tgt_meta_cache[(otype, oname)] = meta

            with pg.cursor() as cur:
                cur.execute("""
                    INSERT INTO ddl_objects (snapshot_id, db_side, object_type, object_name,
                                            oracle_status, last_ddl_time, metadata)
                    VALUES (%s, 'target', %s, %s, %s, %s, %s)
                """, (snapshot_id, otype, oname, obj["status"], obj["last_ddl_time"],
                      json.dumps(meta, default=str)))

            done_count += 1
            if done_count % 5 == 0 or done_count == total_objects:
                _progress("target", done_count, total_objects, f"{otype} {oname}")

        _progress("comparing", total_objects, total_objects, "сравниваем…")
        all_keys = set(src_meta_cache.keys()) | set(tgt_meta_cache.keys())
        for (otype, oname) in all_keys:
            src_m = src_meta_cache.get((otype, oname))
            tgt_m = tgt_meta_cache.get((otype, oname))

            if src_m and tgt_m:
                diff = compare_object(otype, src_m, tgt_m)
                status = "MATCH" if diff.get("ok") else "DIFF"
            elif src_m and not tgt_m:
                diff = {}
                status = "MISSING"
            else:
                diff = {}
                status = "EXTRA"

            with pg.cursor() as cur:
                cur.execute("""
                    INSERT INTO ddl_compare_results (snapshot_id, object_type, object_name,
                                                     match_status, diff)
                    VALUES (%s, %s, %s, %s, %s)
                """, (snapshot_id, otype, oname, status, json.dumps(diff, default=str)))

        pg.commit()
        broadcast({
            "type":         "ddl_snapshot.complete",
            "src_schema":   src_schema,
            "tgt_schema":   tgt_schema,
            "snapshot_id":  snapshot_id,
            "object_counts": object_counts,
            "src_total":    len(src_objects),
            "tgt_total":    len(tgt_objects),
        })

        return jsonify({
            "snapshot_id": snapshot_id,
            "object_counts": object_counts,
            "src_total": len(src_objects),
            "tgt_total": len(tgt_objects),
        })

    except Exception as exc:
        pg.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        tgt_conn.close()
        pg.close()


@bp.get("/api/catalog/objects")
def list_objects():
    """List objects of given type from a snapshot, with compare status and migration status."""
    snapshot_id = request.args.get("snapshot_id", type=int)
    obj_type = (request.args.get("type") or "").upper()
    if not snapshot_id or not obj_type:
        return jsonify({"error": "snapshot_id and type required"}), 400

    if obj_type == "MVIEW":
        obj_type = "MATERIALIZED VIEW"

    pg = _state["get_conn"]()
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT src_schema FROM ddl_snapshots WHERE snapshot_id = %s", (snapshot_id,))
            snap = cur.fetchone()
            if not snap:
                return jsonify({"error": "Snapshot not found"}), 404
            src_schema = snap[0]

            cur.execute("""
                SELECT o.object_name, o.oracle_status, o.last_ddl_time, o.metadata,
                       COALESCE(c.match_status, 'UNKNOWN') AS match_status,
                       c.diff
                FROM   ddl_objects o
                LEFT   JOIN ddl_compare_results c
                       ON c.snapshot_id = o.snapshot_id
                       AND c.object_type = o.object_type
                       AND c.object_name = o.object_name
                WHERE  o.snapshot_id = %s
                  AND  o.db_side = 'source'
                  AND  o.object_type = %s
                ORDER  BY o.object_name
            """, (snapshot_id, obj_type))
            rows = []
            for r in cur.fetchall():
                row = {
                    "object_name": r[0],
                    "oracle_status": r[1],
                    "last_ddl_time": r[2].isoformat() + "Z" if r[2] else None,
                    "metadata": r[3] if isinstance(r[3], dict) else json.loads(r[3]) if r[3] else {},
                    "match_status": r[4],
                    "diff": r[5] if isinstance(r[5], dict) else json.loads(r[5]) if r[5] else {},
                    "migration_status": "NONE",
                }
                rows.append(row)

            if obj_type == "TABLE" and rows:
                table_names = [r["object_name"] for r in rows]
                placeholders = ",".join(["%s"] * len(table_names))
                cur.execute(f"""
                    SELECT source_table, phase
                    FROM   migrations
                    WHERE  source_schema = %s
                      AND  source_table IN ({placeholders})
                    ORDER  BY created_at DESC
                """, [src_schema] + table_names)
                phase_map: dict[str, str] = {}
                for mr in cur.fetchall():
                    if mr[0] not in phase_map:
                        phase_map[mr[0]] = mr[1]

                _PLANNED = {"DRAFT", "NEW", "PREPARING"}
                _DONE = {"COMPLETED"}
                _FAILED = {"FAILED", "CANCELLED"}
                for row in rows:
                    phase = phase_map.get(row["object_name"])
                    if not phase:
                        row["migration_status"] = "NONE"
                    elif phase in _PLANNED:
                        row["migration_status"] = "PLANNED"
                    elif phase in _DONE:
                        row["migration_status"] = "COMPLETED"
                    elif phase in _FAILED:
                        row["migration_status"] = "FAILED"
                    else:
                        row["migration_status"] = "IN_PROGRESS"

        return jsonify(rows)
    finally:
        pg.close()


@bp.get("/api/catalog/objects/<name>/detail")
def object_detail(name: str):
    """Full metadata for a single object (source + target side)."""
    snapshot_id = request.args.get("snapshot_id", type=int)
    obj_type = (request.args.get("type") or "").upper()
    if not snapshot_id or not obj_type:
        return jsonify({"error": "snapshot_id and type required"}), 400
    if obj_type == "MVIEW":
        obj_type = "MATERIALIZED VIEW"

    name = name.upper()
    pg = _state["get_conn"]()
    try:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT db_side, metadata FROM ddl_objects
                WHERE snapshot_id = %s AND object_type = %s AND object_name = %s
            """, (snapshot_id, obj_type, name))
            result = {"source": {}, "target": {}}
            for r in cur.fetchall():
                side = r[0]
                meta = r[1] if isinstance(r[1], dict) else json.loads(r[1]) if r[1] else {}
                result[side] = meta

            cur.execute("""
                SELECT match_status, diff FROM ddl_compare_results
                WHERE snapshot_id = %s AND object_type = %s AND object_name = %s
            """, (snapshot_id, obj_type, name))
            cr = cur.fetchone()
            result["match_status"] = cr[0] if cr else "UNKNOWN"
            result["diff"] = (cr[1] if isinstance(cr[1], dict) else json.loads(cr[1]) if cr[1] else {}) if cr else {}

        return jsonify(result)
    finally:
        pg.close()


@bp.post("/api/catalog/compare")
def compare_objects():
    """Re-compare specific objects with target (refresh comparison)."""
    data = request.get_json(force=True)
    snapshot_id = data.get("snapshot_id")
    src_schema = (data.get("src_schema") or "").upper()
    tgt_schema = (data.get("tgt_schema") or "").upper()
    objects = data.get("objects", [])
    if not snapshot_id or not src_schema or not tgt_schema or not objects:
        return jsonify({"error": "snapshot_id, src_schema, tgt_schema, objects required"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs, prefer_owner=True)
        tgt_conn = get_oracle_conn("target", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    pg = _state["get_conn"]()
    results = []
    try:
        for obj_ref in objects:
            obj_type, obj_name = obj_ref.split(":", 1)
            obj_type = obj_type.upper()
            obj_name = obj_name.upper()

            src_meta = _fetch_metadata(src_conn, src_schema, obj_type, obj_name)
            tgt_meta = _fetch_metadata(tgt_conn, tgt_schema, obj_type, obj_name)

            if src_meta and tgt_meta:
                diff = compare_object(obj_type, src_meta, tgt_meta)
                status = "MATCH" if diff.get("ok") else "DIFF"
            elif src_meta:
                diff = {}
                status = "MISSING"
            else:
                diff = {}
                status = "EXTRA"

            with pg.cursor() as cur:
                cur.execute("""
                    DELETE FROM ddl_compare_results
                    WHERE snapshot_id = %s AND object_type = %s AND object_name = %s
                """, (snapshot_id, obj_type, obj_name))
                cur.execute("""
                    INSERT INTO ddl_compare_results (snapshot_id, object_type, object_name, match_status, diff)
                    VALUES (%s, %s, %s, %s, %s)
                """, (snapshot_id, obj_type, obj_name, status, json.dumps(diff, default=str)))

                cur.execute("""
                    UPDATE ddl_objects SET metadata = %s
                    WHERE snapshot_id = %s AND db_side = 'source' AND object_type = %s AND object_name = %s
                """, (json.dumps(src_meta, default=str), snapshot_id, obj_type, obj_name))

            results.append({"object": f"{obj_type}:{obj_name}", "match_status": status, "diff": diff})

        pg.commit()
        return jsonify(results)
    except Exception as exc:
        pg.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        tgt_conn.close()
        pg.close()


@bp.post("/api/catalog/refresh")
def refresh_objects():
    """Refresh metadata from source Oracle for specific objects."""
    data = request.get_json(force=True)
    snapshot_id = data.get("snapshot_id")
    src_schema = (data.get("src_schema") or "").upper()
    objects = data.get("objects", [])
    if not snapshot_id or not src_schema or not objects:
        return jsonify({"error": "snapshot_id, src_schema, objects required"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs, prefer_owner=True)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    pg = _state["get_conn"]()
    results = []
    try:
        for obj_ref in objects:
            obj_type, obj_name = obj_ref.split(":", 1)
            obj_type = obj_type.upper()
            obj_name = obj_name.upper()

            meta = _fetch_metadata(src_conn, src_schema, obj_type, obj_name)
            with pg.cursor() as cur:
                cur.execute("""
                    UPDATE ddl_objects SET metadata = %s, last_ddl_time = now()
                    WHERE snapshot_id = %s AND db_side = 'source' AND object_type = %s AND object_name = %s
                """, (json.dumps(meta, default=str), snapshot_id, obj_type, obj_name))

            results.append({"object": f"{obj_type}:{obj_name}", "refreshed": True})

        pg.commit()
        return jsonify(results)
    except Exception as exc:
        pg.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        pg.close()


@bp.post("/api/catalog/sync-to-target")
def sync_object_to_target():
    """Create or sync a single object on target."""
    data = request.get_json(force=True)
    src_schema = (data.get("src_schema") or "").upper()
    tgt_schema = (data.get("tgt_schema") or "").upper()
    obj_type = (data.get("object_type") or "").upper()
    obj_name = (data.get("object_name") or "").upper()
    action = data.get("action", "create")

    if not all([src_schema, tgt_schema, obj_type, obj_name]):
        return jsonify({"error": "src_schema, tgt_schema, object_type, object_name required"}), 400

    if obj_type == "TABLE":
        return jsonify({"error": "Use /api/target-prep/* endpoints for table sync"}), 400

    configs = _state["load_configs"]()
    try:
        src_conn = get_oracle_conn("source", configs, prefer_owner=True)
        tgt_conn = get_oracle_conn("target", configs)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503

    try:
        result = sync_to_target(src_conn, tgt_conn, src_schema, obj_name, obj_type, action, target_schema=tgt_schema)
        if "error" in result:
            return jsonify(result), 400
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    finally:
        src_conn.close()
        tgt_conn.close()
