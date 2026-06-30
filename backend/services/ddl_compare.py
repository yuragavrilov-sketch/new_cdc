"""
Compare DDL objects between source and target snapshots.
"""
import re

_SQL_PUNCT = re.compile(r'\s*([,<>=!()]+)\s*')


def _normalize_sql(text: str | None) -> str:
    """Normalize SQL for comparison.

    - Lowercase
    - Collapse all whitespace to single space
    - Normalize spacing around commas, operators, parens:
      'a","b"' → 'a", "b"'   and   'x<y' → 'x < y'
    """
    if not text:
        return ""
    s = " ".join(text.lower().split())
    # Ensure single space around punctuation: commas, <>=!(), etc.
    s = _SQL_PUNCT.sub(r' \1 ', s)
    # Re-collapse any double spaces introduced
    return " ".join(s.split())


def _normalize_code(text: str | None) -> str:
    """Normalize PL/SQL code for comparison.

    Same approach as _normalize_sql: collapse to single line, normalize
    punctuation spacing, lowercase. This handles formatting-only differences
    like indentation, line breaks, spaces around operators/commas.
    """
    if not text:
        return ""
    # Collapse everything into one line, lowercase, normalize punctuation
    s = " ".join(text.lower().split())
    s = _SQL_PUNCT.sub(r' \1 ', s)
    return " ".join(s.split())


def _diff_table(src_meta: dict, tgt_meta: dict) -> dict:
    """Compare table DDL. Uses same logic as planner._diff_summary."""
    src_cols = {c["name"]: c for c in src_meta.get("columns", [])}
    tgt_cols = {c["name"]: c for c in tgt_meta.get("columns", [])}

    cols_missing = [n for n in src_cols if n not in tgt_cols]
    cols_extra = [n for n in tgt_cols if n not in src_cols]
    cols_type = [
        n for n in src_cols
        if n in tgt_cols and src_cols[n].get("data_type") != tgt_cols[n].get("data_type")
    ]

    src_idx = {i["name"]: i for i in src_meta.get("indexes", [])}
    tgt_idx = {i["name"]: i for i in tgt_meta.get("indexes", [])}
    tgt_idx_keys = {(i["unique"], ",".join(i["columns"])) for i in tgt_meta.get("indexes", [])}
    idx_missing = [
        n for n, i in src_idx.items()
        if n not in tgt_idx and (i["unique"], ",".join(i["columns"])) not in tgt_idx_keys
    ]
    idx_disabled = [n for n, i in tgt_idx.items() if i.get("status") != "VALID"]

    src_con_keys = {(c["type_code"], ",".join(c["columns"])): c["name"] for c in src_meta.get("constraints", [])}
    tgt_con_keys = {(c["type_code"], ",".join(c["columns"])) for c in tgt_meta.get("constraints", [])}
    con_missing = [name for key, name in src_con_keys.items() if key not in tgt_con_keys]
    con_disabled = [
        c["name"] for c in tgt_meta.get("constraints", [])
        if c.get("status") == "DISABLED" and c.get("type_code") != "P"
    ]

    src_trg = {t["name"] for t in src_meta.get("triggers", [])}
    tgt_trg = {t["name"] for t in tgt_meta.get("triggers", [])}
    trg_missing = [n for n in src_trg if n not in tgt_trg]

    total = len(cols_missing) + len(cols_extra) + len(cols_type) + len(idx_missing) + len(idx_disabled) + len(con_missing) + len(con_disabled) + len(trg_missing)
    return {
        "ok": total == 0,
        "cols_missing": cols_missing,
        "cols_extra": cols_extra,
        "cols_type": cols_type,
        "idx_missing": idx_missing,
        "idx_disabled": idx_disabled,
        "con_missing": con_missing,
        "con_disabled": con_disabled,
        "trg_missing": trg_missing,
    }


def _diff_view(src_meta: dict, tgt_meta: dict) -> dict:
    src_sql = _normalize_sql(src_meta.get("sql_text"))
    tgt_sql = _normalize_sql(tgt_meta.get("sql_text"))
    sql_match = src_sql == tgt_sql
    status_match = src_meta.get("status") == tgt_meta.get("status")
    return {"ok": sql_match and status_match, "sql_match": sql_match, "status_match": status_match}


def _diff_mview(src_meta: dict, tgt_meta: dict) -> dict:
    src_sql = _normalize_sql(src_meta.get("sql_text"))
    tgt_sql = _normalize_sql(tgt_meta.get("sql_text"))
    sql_match = src_sql == tgt_sql
    refresh_match = src_meta.get("refresh_type") == tgt_meta.get("refresh_type")
    return {"ok": sql_match and refresh_match, "sql_match": sql_match, "refresh_match": refresh_match}


def _diff_code(src_meta: dict, tgt_meta: dict, obj_type: str) -> dict:
    if obj_type == "PACKAGE":
        spec_match = _normalize_code(src_meta.get("spec_source")) == _normalize_code(tgt_meta.get("spec_source"))
        body_match = _normalize_code(src_meta.get("body_source")) == _normalize_code(tgt_meta.get("body_source"))
        return {"ok": spec_match and body_match, "spec_match": spec_match, "body_match": body_match}
    elif obj_type == "TYPE":
        src_match = _normalize_code(src_meta.get("source")) == _normalize_code(tgt_meta.get("source"))
        body_match = _normalize_code(src_meta.get("body_source")) == _normalize_code(tgt_meta.get("body_source"))
        return {"ok": src_match and body_match, "source_match": src_match, "body_match": body_match}
    else:
        code_match = _normalize_code(src_meta.get("source_code")) == _normalize_code(tgt_meta.get("source_code"))
        return {"ok": code_match, "code_match": code_match}


def _diff_sequence(src_meta: dict, tgt_meta: dict) -> dict:
    fields = ["min_value", "max_value", "increment_by", "cache_size"]
    diffs = {f: (src_meta.get(f), tgt_meta.get(f)) for f in fields if src_meta.get(f) != tgt_meta.get(f)}
    return {"ok": len(diffs) == 0, "field_diffs": diffs}


def _diff_synonym(src_meta: dict, tgt_meta: dict) -> dict:
    fields = ["table_owner", "table_name", "db_link"]
    diffs = {f: (src_meta.get(f), tgt_meta.get(f)) for f in fields if src_meta.get(f) != tgt_meta.get(f)}
    return {"ok": len(diffs) == 0, "field_diffs": diffs}


def _diff_trigger(src_meta: dict, tgt_meta: dict) -> dict:
    header_fields = ["trigger_type", "triggering_event", "status",
                     "base_object_type", "action_type", "table_name"]
    field_diffs = {
        f: (src_meta.get(f), tgt_meta.get(f))
        for f in header_fields if src_meta.get(f) != tgt_meta.get(f)
    }
    when_match = _normalize_code(src_meta.get("when_clause")) == _normalize_code(tgt_meta.get("when_clause"))
    body_match = _normalize_code(src_meta.get("trigger_body")) == _normalize_code(tgt_meta.get("trigger_body"))
    return {
        "ok": not field_diffs and when_match and body_match,
        "field_diffs": field_diffs,
        "when_match":  when_match,
        "body_match":  body_match,
    }


def _diff_index(src_meta: dict, tgt_meta: dict) -> dict:
    header_fields = ["uniqueness", "index_type", "status", "table_name", "partitioned"]
    field_diffs = {
        f: (src_meta.get(f), tgt_meta.get(f))
        for f in header_fields if src_meta.get(f) != tgt_meta.get(f)
    }
    src_cols = [(c.get("name"), c.get("descending")) for c in (src_meta.get("columns") or [])]
    tgt_cols = [(c.get("name"), c.get("descending")) for c in (tgt_meta.get("columns") or [])]
    cols_match = src_cols == tgt_cols
    return {
        "ok": not field_diffs and cols_match,
        "field_diffs": field_diffs,
        "cols_match":  cols_match,
        "src_cols":    src_cols,
        "tgt_cols":    tgt_cols,
    }


def _diff_db_link(src_meta: dict, tgt_meta: dict) -> dict:
    fields = ["username", "host"]
    diffs = {f: (src_meta.get(f), tgt_meta.get(f)) for f in fields if src_meta.get(f) != tgt_meta.get(f)}
    return {"ok": len(diffs) == 0, "field_diffs": diffs}


def _diff_job(src_meta: dict, tgt_meta: dict) -> dict:
    header_fields = ["job_type", "schedule_type", "repeat_interval", "enabled", "state"]
    field_diffs = {
        f: (src_meta.get(f), tgt_meta.get(f))
        for f in header_fields if src_meta.get(f) != tgt_meta.get(f)
    }
    action_match = _normalize_code(src_meta.get("job_action")) == _normalize_code(tgt_meta.get("job_action"))
    return {
        "ok": not field_diffs and action_match,
        "field_diffs":  field_diffs,
        "action_match": action_match,
    }


# ── Public API ───────────────────────────────────────────────────────────────

_COMPARATORS = {
    "TABLE":             _diff_table,
    "VIEW":              _diff_view,
    "MATERIALIZED VIEW": _diff_mview,
    "FUNCTION":          lambda s, t: _diff_code(s, t, "FUNCTION"),
    "PROCEDURE":         lambda s, t: _diff_code(s, t, "PROCEDURE"),
    "PACKAGE":           lambda s, t: _diff_code(s, t, "PACKAGE"),
    "TYPE":              lambda s, t: _diff_code(s, t, "TYPE"),
    "SEQUENCE":          _diff_sequence,
    "SYNONYM":           _diff_synonym,
    "TRIGGER":           _diff_trigger,
    "INDEX":             _diff_index,
    "DATABASE LINK":     _diff_db_link,
    "JOB":               _diff_job,
}


def compare_object(object_type: str, src_meta: dict, tgt_meta: dict) -> dict:
    """Compare a single object. Returns {ok: bool, ...diff_details}."""
    comparator = _COMPARATORS.get(object_type)
    if not comparator:
        return {"ok": True}
    return comparator(src_meta, tgt_meta)
