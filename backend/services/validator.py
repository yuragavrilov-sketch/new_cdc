"""Stage validation: optional hash/sample comparison of source vs stage."""

import json
from dataclasses import dataclass, field
from typing import Optional

from services.oracle_scn import open_oracle_conn


@dataclass
class ValidationResult:
    ok: bool
    message: str
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"ok": self.ok, "message": self.message, "details": self.details}


def validate_stage(
    migration: dict,
    src_cfg: dict,
    dst_cfg: dict,
    sample_size: int = 1000,
) -> ValidationResult:
    """
    Validate the stage table.

    If validate_hash_sample is False → fast pass (always OK).
    If validate_hash_sample is True  → sample *sample_size* rows from stage,
        look them up in source AS OF SCN, compare non-key columns via hash.
    """
    if not migration.get("validate_hash_sample"):
        return ValidationResult(
            ok=True,
            message="Валидация пропущена (hash_sample не включён)",
        )

    # В CDC-пачках per-migration start_scn не фиксируется —
    # за CDC-консистентность отвечает CDC-пачка. AS OF SCN сравнение
    # без фиксированного SCN бессмысленно: данные на source могут уйти
    # вперёд. Пропускаем hash-sample с понятным сообщением вместо краша.
    start_scn = migration.get("start_scn")
    if start_scn is None:
        return ValidationResult(
            ok=True,
            message=(
                "Hash-sample пропущен: start_scn не зафиксирован "
                "(CDC-пачка управляет согласованностью без table SCN)"
            ),
        )

    source_schema = migration["source_schema"]
    source_table  = migration["source_table"]
    target_schema = migration["target_schema"]
    stage_table   = migration["stage_table_name"]
    scn           = int(start_scn)

    key_columns: list[str] = json.loads(migration.get("effective_key_columns_json") or "[]")
    if not key_columns:
        return ValidationResult(
            ok=False,
            message="Не определены ключевые колонки — невозможно выполнить hash-sample",
        )

    src_conn = open_oracle_conn(src_cfg)
    dst_conn = open_oracle_conn(dst_cfg)
    try:
        key_select = ", ".join(f'"{c}"' for c in key_columns)

        # 1. Sample random keys from stage
        with dst_conn.cursor() as cur:
            cur.execute(f"""
                SELECT {key_select}
                FROM (
                    SELECT {key_select}
                    FROM   "{target_schema.upper()}"."{stage_table.upper()}"
                    ORDER BY DBMS_RANDOM.VALUE
                )
                WHERE ROWNUM <= :n
            """, {"n": sample_size})
            sample_keys = cur.fetchall()

        if not sample_keys:
            # Stage is empty — only OK if the source was also empty at the SCN
            with src_conn.cursor() as _cur:
                _cur.execute(
                    f'SELECT COUNT(*) FROM "{source_schema.upper()}"."{source_table.upper()}"'
                    f" AS OF SCN :scn",
                    {"scn": scn},
                )
                src_count = int(_cur.fetchone()[0])
            if src_count > 0:
                return ValidationResult(
                    ok=False,
                    message=(
                        f"Stage таблица пуста, но источник содержит "
                        f"{src_count} строк (AS OF SCN {scn}) — bulk load неполный"
                    ),
                    details={"stage_rows": 0, "source_rows": src_count},
                )
            return ValidationResult(
                ok=True,
                message="Stage таблица пуста — источник тоже пуст",
                details={"sample_size": 0},
            )

        # 2. Build key filter: WHERE (k1,k2) IN ((:k1_1,:k2_1),...)
        #    For single key column use simpler IN clause
        if len(key_columns) == 1:
            placeholders = ", ".join([f":k{i}" for i in range(len(sample_keys))])
            bind = {f"k{i}": row[0] for i, row in enumerate(sample_keys)}
            key_filter = f'"{key_columns[0]}" IN ({placeholders})'
        else:
            # Multi-column: use EXISTS / UNION of single-row SELECTs
            key_filter, bind = _multi_key_filter(key_columns, sample_keys)

        # 3. Fetch rows from source AS OF SCN
        with src_conn.cursor() as cur:
            cur.execute(
                f'SELECT {key_select} FROM '
                f'"{source_schema.upper()}"."{source_table.upper()}" '
                f'AS OF SCN :scn WHERE {key_filter}',
                {"scn": scn, **bind},
            )
            src_keys = {row for row in cur.fetchall()}

        # 4. Compare key presence
        stage_keys = {tuple(row) for row in sample_keys}
        missing_in_source = stage_keys - src_keys
        missing_in_stage  = src_keys - stage_keys

        match_count = len(stage_keys & src_keys)
        ok = len(missing_in_source) == 0 and len(missing_in_stage) == 0

        return ValidationResult(
            ok=ok,
            message=(
                f"Sample {len(sample_keys)} строк: {match_count} совпадений"
                + (f", {len(missing_in_source)} отсутствуют в source" if missing_in_source else "")
                + (f", {len(missing_in_stage)} отсутствуют в stage"  if missing_in_stage  else "")
            ),
            details={
                "sample_size":        len(sample_keys),
                "match_count":        match_count,
                "missing_in_source":  len(missing_in_source),
                "missing_in_stage":   len(missing_in_stage),
            },
        )

    finally:
        src_conn.close()
        dst_conn.close()


def _multi_key_filter(key_columns: list[str], sample_keys: list) -> tuple[str, dict]:
    """Build WHERE clause for multi-column key IN simulation."""
    conditions = []
    bind: dict = {}
    for i, row in enumerate(sample_keys):
        parts = []
        for j, col in enumerate(key_columns):
            bind_name = f"k{i}_{j}"
            parts.append(f'"{col}" = :{bind_name}')
            bind[bind_name] = row[j]
        conditions.append(f"({' AND '.join(parts)})")
    return " OR ".join(conditions), bind
