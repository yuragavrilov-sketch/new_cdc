"""Connector-group lifecycle management.

A connector group is a single Debezium connector that captures CDC events
for multiple tables.  One LogMiner session serves all tables in the group.

Tables are stored in the `group_tables` table, decoupled from migrations.
"""

import json
import logging
import os
import random
import string
import uuid
from datetime import datetime

from . import debezium
from .strategy import Strategy

log = logging.getLogger(__name__)
_LIFECYCLE_STATUSES = {"TOPICS_CREATING", "CONNECTOR_STARTING", "STOPPING"}


# ---------------------------------------------------------------------------
# Initialisation (called once from app.py)
# ---------------------------------------------------------------------------

_state: dict = {}


def init(*, get_conn_fn, row_to_dict_fn, load_configs_fn) -> None:
    _state["get_conn"] = get_conn_fn
    _state["row_to_dict"] = row_to_dict_fn
    _state["load_configs"] = load_configs_fn


def _conn():
    return _state["get_conn"]()


def _r2d(cur, row):
    return _state["row_to_dict"](cur, row)


def _gen_run_id() -> str:
    """Short random id for each start cycle (e.g. 'a7x9b2')."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


def _active_connector_name(group: dict) -> str:
    """Debezium connector name for the current run: base_name + '_' + run_id."""
    run_id = group.get("run_id") or ""
    base = group["connector_name"]
    return f"{base}_{run_id}" if run_id else base


def _active_topic_prefix(group: dict) -> str:
    """Topic prefix for the current run: base_prefix + '.' + run_id."""
    run_id = group.get("run_id") or ""
    base = group["topic_prefix"]
    return f"{base}.{run_id}" if run_id else base


def _schema_topic_name(group: dict) -> str:
    """Schema history topic for the current run."""
    return f"schema-changes.{_active_connector_name(group)}"


# ---------------------------------------------------------------------------
# CRUD — groups
# ---------------------------------------------------------------------------

def create_group(
    group_name: str,
    source_connection_id: str,
    connector_name: str,
    topic_prefix: str,
    consumer_group_prefix: str = "",
) -> dict:
    gid = str(uuid.uuid4())
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO connector_groups
                    (group_id, group_name, source_connection_id,
                     connector_name, topic_prefix, consumer_group_prefix)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
            """, (gid, group_name, source_connection_id,
                  connector_name, topic_prefix, consumer_group_prefix))
            row = _r2d(cur, cur.fetchone())
        conn.commit()
        return row
    finally:
        conn.close()


def get_group(group_id: str) -> dict | None:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM connector_groups WHERE group_id = %s", (group_id,))
            row = cur.fetchone()
            return _r2d(cur, row) if row else None
    finally:
        conn.close()


def list_groups() -> list[dict]:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM connector_groups ORDER BY created_at DESC")
            return [_r2d(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()


def delete_group(group_id: str, force: bool = False) -> None:
    """Delete group: cleanup Debezium connector + Kafka topics + DB records.

    Если *force=True* — все активные миграции группы предварительно
    переводятся в CANCELLED (с записью в migration_state_history).
    Без *force* группа с активными миграциями удалить нельзя — ValueError.
    """
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT migration_id, phase FROM migrations
                WHERE  group_id = %s
                  AND  COALESCE(phase, '') NOT IN ('COMPLETED', 'CANCELLED', 'FAILED')
            """, (group_id,))
            active = cur.fetchall()
            if active and not force:
                raise ValueError(
                    f"Нельзя удалить группу: {len(active)} активных миграций"
                )

            for mid, prev_phase in active:
                cur.execute("""
                    UPDATE migrations
                    SET    phase            = 'CANCELLED',
                           state_changed_at = NOW(),
                           updated_at       = NOW(),
                           error_code       = COALESCE(error_code, 'GROUP_DELETED'),
                           error_text       = COALESCE(error_text, 'Группа удалена пользователем')
                    WHERE  migration_id = %s
                """, (mid,))
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, %s, 'CANCELLED', 'Группа удалена (force)', 'USER')
                """, (mid, prev_phase))
                # Освобождаем worker-heartbeat, чтобы CDC-менеджер не reclaim-ил
                cur.execute("""
                    UPDATE migration_cdc_state
                    SET    worker_id = NULL, worker_heartbeat = NULL, updated_at = NOW()
                    WHERE  migration_id = %s
                """, (mid,))

            # FK migrations.group_id / schema_migrations.group_id у нас без
            # ON DELETE — поэтому перед удалением группы обнуляем ссылки на
            # неё во всех связанных записях (включая уже завершённые миграции,
            # чтобы сохранилась история).
            cur.execute(
                "UPDATE migrations SET group_id = NULL WHERE group_id = %s",
                (group_id,),
            )
            cur.execute(
                "UPDATE schema_migrations SET group_id = NULL WHERE group_id = %s",
                (group_id,),
            )

            # group_tables + group_state_history deleted by ON DELETE CASCADE
            cur.execute("DELETE FROM connector_groups WHERE group_id = %s", (group_id,))
        conn.commit()
    finally:
        conn.close()

    # Cleanup: delete Debezium connector + all Kafka topics
    try:
        debezium.delete_connector(_active_connector_name(group))
    except Exception as exc:
        log.warning("cleanup connector error: %s", exc)
    try:
        _delete_group_topics(group)
    except Exception as exc:
        log.warning("cleanup topics error: %s", exc)


def transition_group(group_id: str, to_status: str,
                     message: str | None = None,
                     error_text: str | None = None) -> None:
    """Atomically update group status and record history."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM connector_groups WHERE group_id = %s",
                (group_id,))
            row = cur.fetchone()
            from_status = row[0] if row else None

            cur.execute("""
                UPDATE connector_groups
                SET    status = %s, error_text = %s, updated_at = NOW()
                WHERE  group_id = %s
            """, (to_status, error_text, group_id))

            cur.execute("""
                INSERT INTO group_state_history
                    (group_id, from_status, to_status, message)
                VALUES (%s, %s, %s, %s)
            """, (group_id, from_status, to_status, message))
        conn.commit()
    finally:
        conn.close()


def get_group_history(group_id: str) -> list[dict]:
    """Return state transition history for a group."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, group_id, from_status, to_status, message, created_at
                FROM   group_state_history
                WHERE  group_id = %s
                ORDER BY created_at DESC
                LIMIT 50
            """, (group_id,))
            return [_r2d(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()


# Backward compat alias
def update_group_status(group_id: str, status: str, error_text: str | None = None) -> None:
    transition_group(group_id, status, error_text=error_text)


def clear_group_error(group_id: str) -> None:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE connector_groups
                SET    error_text = NULL,
                       updated_at = NOW()
                WHERE  group_id = %s
            """, (group_id,))
        conn.commit()
    finally:
        conn.close()


def _normalize_polled_connector_status(group_status: str | None,
                                       connector_status: str) -> tuple[str, str | None]:
    """Return (reported_status, db_status_to_write).

    Lifecycle states are owned by the orchestrator. A UI/status poll must not
    turn TOPICS_CREATING or CONNECTOR_STARTING into STOPPED just because the
    Debezium connector has not been created yet.
    """
    if group_status in ("TOPICS_CREATING", "CONNECTOR_STARTING"):
        if connector_status in ("RUNNING", "FAILED"):
            return connector_status, connector_status
        return group_status, None
    if group_status in _LIFECYCLE_STATUSES:
        return group_status, None
    if connector_status == "NOT_FOUND":
        return "STOPPED", "STOPPED"
    return connector_status, connector_status


# ---------------------------------------------------------------------------
# CRUD — group_tables
# ---------------------------------------------------------------------------

def add_tables(group_id: str, tables: list[dict]) -> list[dict]:
    """Add tables to a group.  Returns inserted rows."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    topic_prefix = _active_topic_prefix(group)
    conn = _conn()
    try:
        rows = []
        with conn.cursor() as cur:
            for t in tables:
                tid = str(uuid.uuid4())
                src_schema = t["source_schema"]
                src_table = t["source_table"]
                tgt_schema = t.get("target_schema", src_schema)
                tgt_table = t.get("target_table", src_table)
                ekt = t.get("effective_key_type", "NONE")
                ekc = json.dumps(t.get("effective_key_columns", []))
                pk = t.get("source_pk_exists", False)
                uk = t.get("source_uk_exists", False)
                topic = f"{topic_prefix}.{src_schema.upper()}.{src_table.upper()}".replace("#", "_")

                cur.execute("""
                    INSERT INTO group_tables
                        (id, group_id, source_schema, source_table,
                         target_schema, target_table,
                         effective_key_type, effective_key_columns_json,
                         source_pk_exists, source_uk_exists, topic_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (group_id, source_schema, source_table) DO NOTHING
                    RETURNING *
                """, (tid, group_id, src_schema, src_table,
                      tgt_schema, tgt_table,
                      ekt, ekc, pk, uk, topic))
                row = cur.fetchone()
                if row:
                    rows.append(_r2d(cur, row))
        conn.commit()
        return rows
    finally:
        conn.close()


def get_group_tables(group_id: str) -> list[dict]:
    """Return all tables in a group with correct topic_name (includes run_id)."""
    group = get_group(group_id)
    prefix = _active_topic_prefix(group) if group else ""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM group_tables
                WHERE  group_id = %s
                ORDER BY source_schema, source_table
            """, (group_id,))
            rows = [_r2d(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()
    # Always compute topic_name from prefix (DB value may be stale)
    for r in rows:
        r["topic_name"] = _topic_name(prefix, r["source_schema"], r["source_table"])
    return rows


def _active_migration_for_group_table(cur, group_id: str, source_schema: str, source_table: str):
    cur.execute("""
        SELECT migration_id, phase
        FROM   migrations
        WHERE  group_id = %s
          AND  UPPER(source_schema) = UPPER(%s)
          AND  UPPER(source_table) = UPPER(%s)
          AND  COALESCE(phase, '') NOT IN ('CANCELLED', 'FAILED', 'COMPLETED')
        LIMIT  1
    """, (group_id, source_schema, source_table))
    return cur.fetchone()


def remove_table(group_id: str, source_schema: str, source_table: str) -> None:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            active = _active_migration_for_group_table(cur, group_id, source_schema, source_table)
            if active:
                raise ValueError(
                    f"Cannot remove {source_schema}.{source_table} from CDC pack: "
                    f"active migration {active[0]} is in phase {active[1]}"
                )
            cur.execute("""
                DELETE FROM group_tables
                WHERE group_id = %s
                  AND UPPER(source_schema) = UPPER(%s)
                  AND UPPER(source_table) = UPPER(%s)
            """, (group_id, source_schema, source_table))
        conn.commit()
    finally:
        conn.close()


def prune_tables(group_id: str, keep_tables: list[dict]) -> list[dict]:
    """Remove group tables that are not present in keep_tables.

    Active CDC migrations are protected the same way as remove_table().
    The caller is expected to refresh Debezium config after this transaction.
    """
    keep_keys = set()
    for item in keep_tables:
        source_schema = str(item.get("source_schema") or "").strip()
        source_table = str(item.get("source_table") or "").strip()
        if not source_schema or not source_table:
            raise ValueError("Each keep table must include source_schema and source_table")
        keep_keys.add((source_schema.upper(), source_table.upper()))

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM   group_tables
                WHERE  group_id = %s
                ORDER BY source_schema, source_table
            """, (group_id,))
            rows = [_r2d(cur, row) for row in cur.fetchall()]
            to_remove = [
                row for row in rows
                if (str(row["source_schema"]).upper(), str(row["source_table"]).upper()) not in keep_keys
            ]

            for row in to_remove:
                active = _active_migration_for_group_table(
                    cur,
                    group_id,
                    row["source_schema"],
                    row["source_table"],
                )
                if active:
                    raise ValueError(
                        f"Cannot remove {row['source_schema']}.{row['source_table']} from CDC pack: "
                        f"active migration {active[0]} is in phase {active[1]}"
                    )

            for row in to_remove:
                cur.execute("""
                    DELETE FROM group_tables
                    WHERE group_id = %s
                      AND UPPER(source_schema) = UPPER(%s)
                      AND UPPER(source_table) = UPPER(%s)
                """, (group_id, row["source_schema"], row["source_table"]))
        conn.commit()
        return to_remove
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Legacy: group members via migrations (for backward compat)
# ---------------------------------------------------------------------------

def get_group_migrations(group_id: str) -> list[dict]:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM migrations
                WHERE  group_id = %s
                ORDER BY created_at
            """, (group_id,))
            return [_r2d(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Debezium connector helpers
# ---------------------------------------------------------------------------

def _build_table_include_list(group_id: str) -> str:
    """Collect SCHEMA.TABLE from group_tables."""
    tables = get_group_tables(group_id)
    entries = []
    for t in tables:
        entry = f"{t['source_schema'].upper()}.{t['source_table'].upper()}"
        if entry not in entries:
            entries.append(entry)
    return ",".join(entries)


def _build_key_columns(group_id: str) -> str:
    """Build message.key.columns for tables whose key is not a source PK.

    Format: "SCHEMA.T1:col1,col2;SCHEMA.T2:colA,colB"
    Tables with a source PK are omitted; UK/manual keys are sent explicitly.
    """
    tables = get_group_tables(group_id)
    parts = []
    for t in tables:
        if t.get("source_pk_exists"):
            continue
        key_cols_json = t.get("effective_key_columns_json") or "[]"
        key_cols = json.loads(key_cols_json) if isinstance(key_cols_json, str) else key_cols_json
        if key_cols:
            schema = t["source_schema"].upper()
            table = t["source_table"].upper()
            cols_csv = ",".join(c.upper() for c in key_cols)
            parts.append(f"{schema}.{table}:{cols_csv}")
    return ";".join(parts)


def _split_include_list(value: str | None) -> list[str]:
    entries = []
    for part in str(value or "").split(","):
        entry = part.strip().upper()
        if entry and entry not in entries:
            entries.append(entry)
    return entries


def _topic_name(topic_prefix: str, schema: str, table: str) -> str:
    """Build topic name matching Debezium convention: {prefix}.{SCHEMA}.{TABLE} with # → _."""
    return f"{topic_prefix}.{schema.upper()}.{table.upper()}".replace("#", "_")


def _sync_persisted_topic_names(cur, group_id: str, topic_prefix: str) -> None:
    cur.execute("""
        UPDATE group_tables
        SET    topic_name = REPLACE(
                   %s || '.' || UPPER(source_schema) || '.' || UPPER(source_table),
                   '#',
                   '_'
               )
        WHERE  group_id = %s
    """, (topic_prefix, group_id))


def _build_topic_names(group_id: str) -> list[str]:
    """Generate topic names from group's topic_prefix + table names."""
    tables = get_group_tables(group_id)
    return [t["topic_name"] for t in tables if t.get("topic_name")]


def _oracle_cfg(source_connection_id: str) -> dict:
    configs = _state["load_configs"]()
    return configs.get(source_connection_id, {})


def _kafka_bootstrap() -> list[str]:
    """Get Kafka bootstrap servers from UI settings, fall back to env var."""
    configs = _state["load_configs"]()
    servers = configs.get("kafka", {}).get("bootstrap_servers", "").strip()
    if not servers:
        servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return [s.strip() for s in servers.split(",")]


# ---------------------------------------------------------------------------
# Config preview (no side-effects)
# ---------------------------------------------------------------------------

def build_connector_config(group_id: str) -> dict:
    """Build the Debezium connector config JSON without sending it anywhere."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    oracle_cfg = _oracle_cfg(group["source_connection_id"])
    table_list = _build_table_include_list(group_id)
    key_columns = _build_key_columns(group_id)

    connector_name = _active_connector_name(group)
    topic_prefix = _active_topic_prefix(group)
    bootstrap = ""
    try:
        from . import debezium as _deb
        bootstrap = _deb._kafka_bootstrap()
    except Exception:
        bootstrap = "(not configured)"

    lob_enabled = os.getenv("DEBEZIUM_LOB_ENABLED", "true").lower() == "true"

    config = {
        "connector.class":              "io.debezium.connector.oracle.OracleConnector",
        "tasks.max":                    "1",
        "snapshot.mode":                "no_data",
        "snapshot.locking.mode":        "none",
        "log.mining.strategy":          "online_catalog",
        "database.connection.adapter":  "logminer",
        "log.mining.continuous.mine":          "true",
        "log.mining.batch.size.max":           os.getenv("DEBEZIUM_LOG_MINING_BATCH_SIZE_MAX", "5000"),
        "log.mining.sleep.time.increment.ms":  os.getenv("DEBEZIUM_LOG_MINING_SLEEP_TIME_INCREMENT_MS", "400"),
        "log.mining.sleep.time.max.ms":        os.getenv("DEBEZIUM_LOG_MINING_SLEEP_TIME_MAX_MS", "1000"),
        "heartbeat.interval.ms":               "30000",

        "topic.prefix":      topic_prefix,
        "database.hostname": oracle_cfg.get("host", ""),
        "database.port":     str(oracle_cfg.get("port", 1521)),
        "database.user":     oracle_cfg.get("user", ""),
        "database.password": "********",
        "database.dbname":   oracle_cfg.get("service_name", ""),

        "table.include.list": table_list,

        "topic.creation.default.replication.factor": os.getenv("DEBEZIUM_TOPIC_REPLICATION_FACTOR", "1"),
        "topic.creation.default.partitions":         os.getenv("DEBEZIUM_TOPIC_PARTITIONS", "1"),
        "topic.creation.default.cleanup.policy":     os.getenv("DEBEZIUM_TOPIC_CLEANUP_POLICY", "delete"),
        "topic.creation.default.retention.ms":       os.getenv("DEBEZIUM_TOPIC_RETENTION_MS", "604800000"),
        "topic.creation.default.compression.type":   os.getenv("DEBEZIUM_TOPIC_COMPRESSION_TYPE", "snappy"),

        "lob.enabled":           "true" if lob_enabled else "false",
        "lob.fetch.size":        os.getenv("DEBEZIUM_LOB_FETCH_SIZE", "0"),
        "lob.fetch.buffer.size": os.getenv("DEBEZIUM_LOB_FETCH_BUFFER_SIZE", "0"),

        "schema.history.internal.kafka.bootstrap.servers": bootstrap,
        "schema.history.internal.kafka.topic":             f"schema-changes.{connector_name}",
        "include.schema.changes": "false",

        "key.converter":                 "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable":  "true",
        "value.converter":               "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",

        "decimal.handling.mode": "double",
        "time.precision.mode":   "connect",

        "provide.transaction.metadata": "false",
        "tombstones.on.delete":          "true",
        "skipped.operations":            "none",

        "transforms":                              "unwrap,route",
        "transforms.unwrap.type":                  "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.delete.handling.mode":  "rewrite",
        "transforms.unwrap.add.fields":            "op,table,source.ts_ms",
        "transforms.unwrap.add.fields.prefix":     "__",
        "transforms.route.type":                   "io.debezium.transforms.ByLogicalTableRouter",
        "transforms.route.topic.regex":            f"({topic_prefix}\\..*)",
        "transforms.route.topic.replacement":      "$1",
    }

    if key_columns:
        config["message.key.columns"] = key_columns

    return {"name": connector_name, "config": config}


# ---------------------------------------------------------------------------
# Kafka topics — pre-create and monitor
# ---------------------------------------------------------------------------

def create_group_topics(group_id: str) -> list[dict]:
    """Pre-create Kafka topics for all tables in the group.
    Returns list of {topic_name, status} dicts.
    """
    from . import kafka_topics

    bootstrap = _kafka_bootstrap()
    topics = _build_topic_names(group_id)
    results = []
    for topic in topics:
        try:
            kafka_topics.create_topic(bootstrap_servers=bootstrap, topic_name=topic)
            results.append({"topic_name": topic, "status": "ok"})
        except Exception as exc:
            results.append({"topic_name": topic, "status": "error", "error": str(exc)})
    return results


def _delete_group_topics(group: dict) -> None:
    """Delete all Kafka topics for a group: data topics + schema-changes topic."""
    from . import kafka_topics

    bootstrap = _kafka_bootstrap()
    group_id = group["group_id"]
    # Data topics
    topics = _build_topic_names(group_id)
    for topic in topics:
        try:
            kafka_topics.delete_topic(bootstrap_servers=bootstrap, topic_name=topic)
        except Exception as exc:
            log.warning("failed to delete topic %s: %s", topic, exc)

    # Schema-changes topic
    schema_topic = _schema_topic_name(group)
    try:
        kafka_topics.delete_topic(bootstrap_servers=bootstrap, topic_name=schema_topic)
    except Exception as exc:
        log.warning("failed to delete schema topic %s: %s", schema_topic, exc)


def _new_topic_count_consumer(bootstrap: list[str]):
    from kafka import KafkaConsumer

    return KafkaConsumer(
        bootstrap_servers=bootstrap,
        request_timeout_ms=5000,
        connections_max_idle_ms=8000,
        enable_auto_commit=False,
    )


def get_topic_message_counts(group_id: str) -> list[dict]:
    """Return message counts for each topic in the group."""
    from kafka.structs import TopicPartition

    topics = _build_topic_names(group_id)
    if not topics:
        return []

    bootstrap = _kafka_bootstrap()

    results = []
    consumer = None
    try:
        consumer = _new_topic_count_consumer(bootstrap)
        for topic_name in topics:
            try:
                partitions = consumer.partitions_for_topic(topic_name)
                if partitions is None:
                    results.append({"topic_name": topic_name, "count": -1, "exists": False})
                    continue
                tps = [TopicPartition(topic_name, p) for p in partitions]
                end_offsets = consumer.end_offsets(tps)
                begin_offsets = consumer.beginning_offsets(tps)
                total = sum(end_offsets[tp] - begin_offsets[tp] for tp in tps)
                results.append({"topic_name": topic_name, "count": total, "exists": True})
            except Exception:
                results.append({"topic_name": topic_name, "count": -1, "exists": False})
    except Exception:
        for topic_name in topics:
            results.append({"topic_name": topic_name, "count": -1, "exists": False})
    finally:
        if consumer:
            consumer.close()
    return results


# ---------------------------------------------------------------------------
# Connector lifecycle
# ---------------------------------------------------------------------------

def create_migrations_for_group_tables(
    group_id: str,
    table_ids: list[str] | None,
    *,
    strategy: Strategy,
    stage_tablespace: str = "PAYSTAGE",
    truncate_target: bool = True,
    chunk_size: int = 1_000_000,
    max_parallel_workers: int = 1,
    baseline_parallel_degree: int = 4,
    baseline_batch_size: int = 500_000,
    validate_hash_sample: bool = False,
) -> list[dict]:
    """Create one migration per group_tables row.

    table_ids — restrict to a subset; None means «all tables in the group».
    Skips tables that already have a non-terminal migration in the group.

    Returns list of {"table": "SCHEMA.NAME", "migration_id": ...,
                     "skipped": "reason" | None}.
    """
    if strategy.uses_stage and not truncate_target:
        raise ValueError(
            "STAGE-стратегия требует TRUNCATE target — поведение неизменяемо")

    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    all_tables = get_group_tables(group_id)
    if table_ids is not None:
        wanted = set(table_ids)
        tables = [t for t in all_tables if t["id"] in wanted]
    else:
        tables = all_tables
    if not tables:
        return []

    connector_name = group["connector_name"]
    topic_prefix   = _active_topic_prefix(group)
    prefix         = group.get("consumer_group_prefix") or group["topic_prefix"]

    key_source_map = {
        "PRIMARY_KEY":  "PK",
        "UNIQUE_KEY":   "UK",
        "USER_DEFINED": "USER",
        "NONE":         "NONE",
    }

    results: list[dict] = []
    conn = _conn()
    try:
        with conn.cursor() as cur:
            for tbl in tables:
                src_schema = tbl["source_schema"].upper()
                src_table  = tbl["source_table"].upper()
                tgt_schema = tbl["target_schema"].upper()
                tgt_table  = tbl["target_table"].upper()
                full_name  = f"{src_schema}.{src_table}"

                # Skip if there's already an active migration for this table
                cur.execute("""
                    SELECT migration_id, phase FROM migrations
                    WHERE  group_id = %s
                      AND  source_schema = %s AND source_table = %s
                      AND  phase NOT IN ('CANCELLED', 'FAILED', 'COMPLETED')
                    LIMIT 1
                """, (group_id, src_schema, src_table))
                dup = cur.fetchone()
                if dup:
                    results.append({
                        "table":        full_name,
                        "migration_id": dup[0],
                        "skipped":      f"already active ({dup[1]})",
                    })
                    continue

                ekt = tbl.get("effective_key_type", "NONE") or "NONE"
                ekc_json = tbl.get("effective_key_columns_json") or "[]"
                pk = bool(tbl.get("source_pk_exists", False))
                uk = bool(tbl.get("source_uk_exists", False))

                stage_name = f"STG_{src_schema}_{src_table}" if strategy.uses_stage else ""
                stg_ts     = stage_tablespace.strip().upper() if strategy.uses_stage else ""
                consumer_group = f"{prefix}_{src_schema}_{src_table}"
                migration_name = f"{src_schema}.{src_table} → {tgt_schema}.{tgt_table}"

                mid = str(uuid.uuid4())
                now = datetime.utcnow()

                cur.execute("""
                    INSERT INTO migrations (
                        migration_id, migration_name, phase, state_changed_at,
                        source_connection_id, target_connection_id,
                        source_schema, source_table, target_schema, target_table,
                        stage_table_name, stage_tablespace,
                        connector_name, topic_prefix, consumer_group,
                        chunk_size, max_parallel_workers, baseline_parallel_degree,
                        baseline_batch_size,
                        validate_hash_sample,
                        source_pk_exists, source_uk_exists,
                        effective_key_type, effective_key_source, effective_key_columns_json,
                        strategy,
                        truncate_target,
                        group_id,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, 'NEW', %s,
                        %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s,
                        %s,
                        %s, %s,
                        %s, %s, %s,
                        %s,
                        %s,
                        %s,
                        %s, %s
                    )
                """, (
                    mid, migration_name, now,
                    group["source_connection_id"], "oracle_target",
                    src_schema, src_table, tgt_schema, tgt_table,
                    stage_name, stg_ts,
                    connector_name, topic_prefix, consumer_group,
                    chunk_size,
                    max(1, max_parallel_workers),
                    max(1, baseline_parallel_degree),
                    max(1000, baseline_batch_size),
                    validate_hash_sample,
                    pk, uk,
                    ekt, key_source_map.get(ekt, "NONE"), ekc_json,
                    strategy.value,
                    truncate_target,
                    group_id,
                    now, now,
                ))
                cur.execute("""
                    INSERT INTO migration_state_history
                        (migration_id, from_phase, to_phase, message, actor_type)
                    VALUES (%s, NULL, 'NEW', 'Created from CDC pack', 'USER')
                """, (mid,))
                results.append({
                    "table":        full_name,
                    "migration_id": mid,
                    "skipped":      None,
                })
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return results


def check_cdc_readiness(source_connection_id: str,
                        tables: list[dict]) -> dict:
    """Check whether the source DB/tables are ready for Debezium CDC.

    Returns a dict:
      {
        "archivelog": bool,
        "db_level_supp": bool,    # supplemental_log_data_all on v$database
        "tables": [
          {"source_schema": ..., "source_table": ..., "supp_log": bool}
        ]
      }
    """
    from . import oracle_scn

    oracle_cfg = _oracle_cfg(source_connection_id)
    if not oracle_cfg.get("host"):
        raise ValueError(
            "Oracle source не настроен — заполните настройки соединения")

    conn = oracle_scn.open_oracle_conn(oracle_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT log_mode FROM v$database")
            row = cur.fetchone()
            archivelog = bool(row and (row[0] or "").upper() == "ARCHIVELOG")

            cur.execute("SELECT supplemental_log_data_all FROM v$database")
            row = cur.fetchone()
            db_supp = bool(row and (row[0] or "").upper() == "YES")

            results = []
            if db_supp:
                for t in tables:
                    results.append({
                        "source_schema": t["source_schema"].upper(),
                        "source_table":  t["source_table"].upper(),
                        "supp_log":      True,
                    })
            else:
                for t in tables:
                    s = t["source_schema"].upper()
                    n = t["source_table"].upper()
                    cur.execute("""
                        SELECT COUNT(*) FROM all_log_groups
                        WHERE  owner = :s AND table_name = :t
                          AND  log_group_type = 'ALL COLUMN LOGGING'
                    """, {"s": s, "t": n})
                    cnt = (cur.fetchone() or [0])[0] or 0
                    results.append({
                        "source_schema": s,
                        "source_table":  n,
                        "supp_log":      cnt > 0,
                    })
    finally:
        conn.close()

    return {
        "archivelog":    archivelog,
        "db_level_supp": db_supp,
        "tables":        results,
    }


def request_start(group_id: str) -> dict:
    """Validate and initiate the async start lifecycle.

    Generates a unique run_id for this start cycle so that connector name
    and schema-changes topic don't collide with previous runs.
    Sets group to TOPICS_CREATING — the orchestrator picks it up from there.
    """
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    status = group.get("status")
    if status in ("RUNNING", "TOPICS_CREATING", "CONNECTOR_STARTING"):
        return {
            "group_id": group_id,
            "status": status,
            "run_id": group.get("run_id"),
            "already_started": True,
        }
    if status == "STOPPING":
        raise ValueError("CDC-пачка сейчас останавливается - повторите запуск после STOPPED")

    table_list = _build_table_include_list(group_id)
    if not table_list:
        raise ValueError("В группе нет таблиц — добавьте таблицы перед запуском")

    oracle_cfg = _oracle_cfg(group["source_connection_id"])
    if not oracle_cfg.get("host"):
        raise ValueError("Oracle source не настроен — проверьте Настройки")

    # Generate unique run_id for this start cycle
    run_id = _gen_run_id()
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE connector_groups SET run_id = %s, updated_at = NOW()
                WHERE group_id = %s
            """, (run_id, group_id))
            _sync_persisted_topic_names(
                cur,
                group_id,
                f"{group['topic_prefix']}.{run_id}",
            )
        conn.commit()
    finally:
        conn.close()

    transition_group(group_id, "TOPICS_CREATING",
                     f"Запуск инициирован (run_id={run_id})")
    return {"group_id": group_id, "status": "TOPICS_CREATING", "run_id": run_id}


def do_create_topics(group_id: str) -> list[dict]:
    """Phase handler: create Kafka topics for group tables."""
    return create_group_topics(group_id)


def do_start_connector(group_id: str) -> dict:
    """Phase handler: create Debezium connector with run_id in name."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    connector_name = _active_connector_name(group)
    oracle_cfg = _oracle_cfg(group["source_connection_id"])
    table_list = _build_table_include_list(group_id)
    key_columns = _build_key_columns(group_id)

    return debezium.create_group_connector(
        connector_name=connector_name,
        topic_prefix=_active_topic_prefix(group),
        oracle_cfg=oracle_cfg,
        table_include_list=table_list,
        key_columns=key_columns,
    )


def request_stop(group_id: str) -> None:
    """Initiate async stop — orchestrator handles the actual stop."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")
    transition_group(group_id, "STOPPING", "Остановка инициирована пользователем")


def do_stop_connector(group_id: str) -> None:
    """Phase handler: delete Debezium connector + all Kafka topics."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    connector_name = _active_connector_name(group)

    # 1. Delete Debezium connector
    debezium.delete_connector(connector_name)

    # 2. Delete all data topics + schema-changes topic
    _delete_group_topics(group)


def refresh_connector_tables(group_id: str) -> None:
    """Re-read group_tables and update Debezium table.include.list + key columns.

    Called when tables are added/removed from the group while
    the connector is already running.
    """
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Группа {group_id} не найдена")

    connector_name = _active_connector_name(group)
    status = debezium.get_connector_status(connector_name)
    if status == "NOT_FOUND":
        if group.get("status") == "RUNNING":
            update_group_status(
                group_id,
                "STOPPED",
                f"CDC runtime {connector_name} is missing in Kafka Connect",
            )
            raise ValueError(
                f"CDC runtime {connector_name} is marked RUNNING but is missing in Kafka Connect"
            )
        return  # connector not running, nothing to update

    table_list = _build_table_include_list(group_id)
    key_columns = _build_key_columns(group_id)

    debezium.update_connector_tables(
        connector_name=connector_name,
        table_include_list=table_list,
        key_columns=key_columns,
    )


def get_debezium_sync_status(group_id: str) -> dict:
    """Compare desired connector table config from state DB with Kafka Connect."""
    group = get_group(group_id)
    if not group:
        raise ValueError(f"Р“СЂСѓРїРїР° {group_id} РЅРµ РЅР°Р№РґРµРЅР°")

    connector_name = _active_connector_name(group)
    desired_table_list = _build_table_include_list(group_id)
    desired_key_columns = _build_key_columns(group_id)
    desired_tables = _split_include_list(desired_table_list)

    actual_config = debezium.get_connector_config(connector_name)
    if actual_config is None:
        return {
            "connector_name": connector_name,
            "exists": False,
            "in_sync": False,
            "desired_table_include_list": desired_table_list,
            "actual_table_include_list": None,
            "desired_message_key_columns": desired_key_columns,
            "actual_message_key_columns": None,
            "missing_tables": desired_tables,
            "extra_tables": [],
            "key_columns_match": desired_key_columns == "",
        }

    actual_table_list = str(actual_config.get("table.include.list") or "")
    actual_key_columns = str(actual_config.get("message.key.columns") or "")
    actual_tables = _split_include_list(actual_table_list)
    missing_tables = [entry for entry in desired_tables if entry not in actual_tables]
    extra_tables = [entry for entry in actual_tables if entry not in desired_tables]
    key_columns_match = desired_key_columns.strip() == actual_key_columns.strip()

    return {
        "connector_name": connector_name,
        "exists": True,
        "in_sync": not missing_tables and not extra_tables and key_columns_match,
        "desired_table_include_list": desired_table_list,
        "actual_table_include_list": actual_table_list,
        "desired_message_key_columns": desired_key_columns,
        "actual_message_key_columns": actual_key_columns,
        "missing_tables": missing_tables,
        "extra_tables": extra_tables,
        "key_columns_match": key_columns_match,
    }


def get_connector_status(group_id: str) -> str:
    """Return current Debezium connector status for the group."""
    group = get_group(group_id)
    if not group:
        return "NOT_FOUND"
    try:
        connector_status = debezium.get_connector_status(_active_connector_name(group))
        reported_status, db_status = _normalize_polled_connector_status(
            group.get("status"),
            connector_status,
        )
        if db_status and db_status != group.get("status"):
            update_group_status(group_id, db_status)
        return reported_status
    except Exception:
        return "UNKNOWN"
