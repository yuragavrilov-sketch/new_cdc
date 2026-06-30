"""Checklist routes — migration table catalog stored in PostgreSQL."""

from flask import Blueprint, jsonify, request

bp = Blueprint("checklist", __name__)

_state: dict = {}


def init(get_conn_fn, db_available_ref):
    _state["get_conn"] = get_conn_fn
    _state["db_available"] = db_available_ref


def _conn():
    return _state["get_conn"]()


# ── Lists ─────────────────────────────────────────────────────────────────────

@bp.get("/api/checklists")
def get_lists():
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT list_id, name, created_at FROM checklist_lists ORDER BY list_id"
            )
            lists = []
            for row in cur.fetchall():
                list_id, name, created_at = row
                cur2 = conn.cursor()
                cur2.execute(
                    "SELECT item_id, schema_name, table_name, decision, status "
                    "FROM checklist_items WHERE list_id = %s ORDER BY item_id",
                    (list_id,),
                )
                items = [
                    {"item_id": r[0], "schema": r[1], "table": r[2],
                     "decision": r[3], "status": r[4]}
                    for r in cur2.fetchall()
                ]
                cur2.close()
                lists.append({
                    "list_id": list_id,
                    "name": name,
                    "created_at": created_at.isoformat() + "Z" if created_at else None,
                    "tables": items,
                })
        return jsonify(lists)
    finally:
        conn.close()


@bp.post("/api/checklists")
def create_list():
    body = request.get_json(force=True)
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO checklist_lists (name) VALUES (%s) RETURNING list_id",
                (name,),
            )
            list_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({"list_id": list_id, "name": name, "tables": []}), 201
    except Exception as e:
        conn.rollback()
        if "unique" in str(e).lower():
            return jsonify({"error": "List with this name already exists"}), 409
        raise
    finally:
        conn.close()


@bp.delete("/api/checklists/<int:list_id>")
def delete_list(list_id: int):
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM checklist_lists WHERE list_id = %s", (list_id,))
        conn.commit()
        return "", 204
    finally:
        conn.close()


# ── Items ─────────────────────────────────────────────────────────────────────

@bp.post("/api/checklists/<int:list_id>/items")
def add_items(list_id: int):
    """Add one or many items. Body: {"items": [{"schema": "X", "table": "Y"}, ...]}"""
    body = request.get_json(force=True)
    items = body.get("items", [])
    if not items:
        return jsonify({"error": "items required"}), 400

    conn = _conn()
    try:
        added = []
        with conn.cursor() as cur:
            for item in items:
                schema = (item.get("schema") or "").strip().upper()
                table = (item.get("table") or "").strip().upper()
                decision = item.get("decision", "migrate")
                status = item.get("status", "pending")
                if not table:
                    continue
                try:
                    cur.execute(
                        "INSERT INTO checklist_items (list_id, schema_name, table_name, decision, status) "
                        "VALUES (%s, %s, %s, %s, %s) "
                        "ON CONFLICT (list_id, schema_name, table_name) DO NOTHING "
                        "RETURNING item_id",
                        (list_id, schema, table, decision, status),
                    )
                    row = cur.fetchone()
                    if row:
                        added.append({"item_id": row[0], "schema": schema, "table": table,
                                      "decision": decision, "status": status})
                except Exception:
                    pass
        conn.commit()
        return jsonify({"added": added}), 201
    finally:
        conn.close()


@bp.patch("/api/checklists/<int:list_id>/items/<int:item_id>")
def update_item(list_id: int, item_id: int):
    body = request.get_json(force=True)
    allowed = {"decision", "status"}
    fields = {k: v for k, v in body.items() if k in allowed}
    if not fields:
        return jsonify({"error": "nothing to update"}), 400

    conn = _conn()
    try:
        set_parts = [f"{k} = %s" for k in fields]
        values = list(fields.values()) + [item_id, list_id]
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE checklist_items SET {', '.join(set_parts)} "
                f"WHERE item_id = %s AND list_id = %s",
                values,
            )
        conn.commit()
        return "", 204
    finally:
        conn.close()


@bp.delete("/api/checklists/<int:list_id>/items/<int:item_id>")
def delete_item(list_id: int, item_id: int):
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM checklist_items WHERE item_id = %s AND list_id = %s",
                (item_id, list_id),
            )
        conn.commit()
        return "", 204
    finally:
        conn.close()
