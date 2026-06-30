# Oracle → Oracle Migration System — Постановка и план реализации

## Контекст
Стек: Python Flask + React. State DB: PostgreSQL (уже существует).
Внешние сервисы (уже развёрнуты): Kafka, Kafka Connect (Debezium), Oracle source, Oracle target.
Kafka Connect REST API — без аутентификации.

## Ключевые решения
- Одна миграция = одна таблица
- Масштаб данных: TB
- Чанкинг: по ROWID (DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID), размер фиксированный (настройка chunk_size)
- Workers: отдельные Python-процессы, запускаются внешним оркестратором (supervisord/k8s/вручную), клеймят джобы через HTTP API Flask
- CDC apply worker: читает Kafka самостоятельно (Python consumer), применяет напрямую в финальную таблицу target Oracle
- lag=0 измеряется по Kafka consumer group lag
- Публикация baseline: TRUNCATE + INSERT /*+ APPEND PARALLEL */ (самый быстрый вариант)
- При рестарте фазы BASELINE_PUBLISHING: TRUNCATE + повтор
- Таблицы без PK: пользователь указывает custom key columns при создании; без указания — блокируем запуск
- Custom key задаётся один раз при создании миграции
- Валидация stage: count source AS OF SCN vs count stage; опционально hash/sample (toggle при создании)
- Фазы идемпотентны (рестарт с той же точки)
- Нет требований к rollback target

---

## Архитектура системы

```
┌─────────────────────────────────────────────────────────────┐
│                        React UI                              │
│  MigrationList │ MigrationDetail │ Progress │ Connector UI  │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP + SSE
┌──────────────────────────▼──────────────────────────────────┐
│                     Flask API                                │
│  routes/  │  services/  │  orchestrator (background thread) │
└──────┬────────────┬──────────────────────┬──────────────────┘
       │            │                      │
  PostgreSQL    Oracle src/dst        Kafka Connect REST
  (State DB)    (oracledb)            (Debezium mgmt)
       │
  ┌────▼──────────────────────────────────────┐
  │         migration_chunks (job queue)       │
  └──────┬───────────────────────┬────────────┘
         │                       │
  bulk_worker.py          cdc_apply_worker.py
  (N процессов)           (1 процесс на миграцию)
  Oracle src AS OF SCN    Kafka consumer → Oracle dst
  → stage table
```

---

## State DB: новые таблицы и колонки

### Добавить в таблицу `migrations`

```sql
ALTER TABLE migrations ADD COLUMN total_rows           BIGINT;
ALTER TABLE migrations ADD COLUMN total_chunks         INTEGER;
ALTER TABLE migrations ADD COLUMN chunks_done          INTEGER DEFAULT 0;
ALTER TABLE migrations ADD COLUMN chunks_failed        INTEGER DEFAULT 0;
ALTER TABLE migrations ADD COLUMN validate_hash_sample BOOLEAN DEFAULT FALSE;
ALTER TABLE migrations ADD COLUMN validation_result    JSONB;
ALTER TABLE migrations ADD COLUMN connector_status     VARCHAR(50);
ALTER TABLE migrations ADD COLUMN kafka_lag            BIGINT;
ALTER TABLE migrations ADD COLUMN kafka_lag_checked_at TIMESTAMPTZ;
```

### Новая таблица `migration_chunks`

```sql
CREATE TABLE migration_chunks (
    chunk_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    migration_id    UUID NOT NULL REFERENCES migrations(migration_id) ON DELETE CASCADE,
    chunk_seq       INTEGER NOT NULL,
    rowid_start     VARCHAR(20) NOT NULL,   -- Oracle extended ROWID (18 chars)
    rowid_end       VARCHAR(20) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    -- PENDING | CLAIMED | RUNNING | DONE | FAILED
    rows_loaded     BIGINT DEFAULT 0,
    worker_id       VARCHAR(200),           -- hostname:pid
    claimed_at      TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error_text      TEXT,
    retry_count     INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (migration_id, chunk_seq)
);
CREATE INDEX idx_chunks_migration_status
    ON migration_chunks (migration_id, status);
CREATE INDEX idx_chunks_pending
    ON migration_chunks (status, created_at)
    WHERE status = 'PENDING';
```

### Новая таблица `migration_cdc_state`

```sql
CREATE TABLE migration_cdc_state (
    migration_id      UUID PRIMARY KEY REFERENCES migrations(migration_id) ON DELETE CASCADE,
    consumer_group    VARCHAR(200) NOT NULL,
    topic             VARCHAR(200) NOT NULL,
    total_lag         BIGINT DEFAULT 0,
    lag_by_partition  JSONB,                 -- {"0": 1234, "1": 567}
    last_event_scn    NUMERIC,
    last_event_ts     TIMESTAMPTZ,
    apply_rate_rps    NUMERIC(10,2),         -- rows/sec (EMA)
    worker_id         VARCHAR(200),
    worker_heartbeat  TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Граф фаз (state machine)

```
DRAFT → NEW → PREPARING → SCN_FIXED → CONNECTOR_STARTING → CDC_BUFFERING
→ CHUNKING → BULK_LOADING → BULK_LOADED → STAGE_VALIDATING → STAGE_VALIDATED
→ BASELINE_PUBLISHING → BASELINE_PUBLISHED → CDC_APPLY_STARTING
→ CDC_CATCHING_UP → CDC_CAUGHT_UP → STEADY_STATE

Из любой активной фазы → CANCELLING → CANCELLED
При ошибке            → FAILED (error_code, error_text, failed_phase)
```

### Оркестратор (`services/orchestrator.py`) — тик каждые 5с

| Фаза | Действие |
|------|---------|
| `NEW` | Validate metadata, PK/UK/custom key check → `PREPARING` |
| `PREPARING` | Проверить/создать stage table, зафиксировать start_scn → `SCN_FIXED` |
| `SCN_FIXED` | `debezium.create_connector()` → `CONNECTOR_STARTING` |
| `CONNECTOR_STARTING` | Poll connector status; RUNNING → `CDC_BUFFERING` |
| `CDC_BUFFERING` | `create_chunks()` → записать в migration_chunks → `CHUNKING` |
| `CHUNKING` | Проверить chunks записаны → `BULK_LOADING` |
| `BULK_LOADING` | `chunks_done==total_chunks` → `BULK_LOADED`; `chunks_failed>retry_limit` → `FAILED` |
| `BULK_LOADED` | → `STAGE_VALIDATING` |
| `STAGE_VALIDATING` | `validator.validate()` → `STAGE_VALIDATED` или `FAILED` |
| `STAGE_VALIDATED` | → `BASELINE_PUBLISHING` |
| `BASELINE_PUBLISHING` | TRUNCATE + INSERT APPEND → `BASELINE_PUBLISHED` |
| `BASELINE_PUBLISHED` | → `CDC_APPLY_STARTING` |
| `CDC_APPLY_STARTING` | Ждать heartbeat от cdc_apply_worker → `CDC_CATCHING_UP` |
| `CDC_CATCHING_UP` | Обновлять lag из checkin; lag==0 → `CDC_CAUGHT_UP` → `STEADY_STATE` |
| `STEADY_STATE` | Мониторинг lag, heartbeat |

---

## Сервисные модули (`backend/services/`)

| Модуль | Описание |
|--------|---------|
| `debezium.py` | Kafka Connect REST: create/delete/get_status connector |
| `oracle_scn.py` | `get_current_scn()` из `v$database` |
| `oracle_chunker.py` | `DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID` → list[(rowid_start, rowid_end)] |
| `oracle_stage.py` | create/drop stage table, count, sample_hash |
| `oracle_baseline.py` | TRUNCATE + `INSERT /*+ APPEND PARALLEL */ INTO final SELECT FROM stage` |
| `kafka_lag.py` | `KafkaAdminClient` consumer group lag |
| `validator.py` | count match + optional hash/sample |
| `job_queue.py` | claim/complete/fail chunk (`SELECT FOR UPDATE SKIP LOCKED`) |
| `orchestrator.py` | background phase handler thread |

---

## Протокол воркеров

Workers общаются только через HTTP API Flask (не напрямую к State DB).

### `bulk_worker.py`

```
POST /api/worker/chunks/claim
  → { chunk_id, rowid_start, rowid_end, start_scn, source_conn, target_conn,
      source_schema, source_table, stage_table }

  SELECT * FROM {schema}.{table} AS OF SCN :scn
  WHERE ROWID BETWEEN CHARTOROWID(:start) AND CHARTOROWID(:end)

  INSERT batch в stage_<table> на target Oracle

POST /api/worker/chunks/<id>/progress  { rows_loaded }
POST /api/worker/chunks/<id>/complete  { rows_loaded }
POST /api/worker/chunks/<id>/fail      { error_text }
```

### `cdc_apply_worker.py`

```
KafkaConsumer(topic, group_id=consumer_group, auto_offset_reset='earliest')

Применяет события (I/U/D) напрямую в target Oracle final table:
  - I  → INSERT (on conflict key → UPDATE)
  - U  → MERGE по effective_key_columns
  - D  → DELETE по effective_key_columns

POST /api/worker/cdc/checkin  { migration_id, lag, rows_applied, last_event_ts }
При lag==0: POST /api/migrations/<id>/action  { action: "lag_zero" }
Heartbeat каждые 30с; считается мёртвым через 90с без heartbeat
```

---

## API endpoints (новые)

### Мониторинг
```
GET  /api/migrations/<id>/chunks      — список чанков с прогрессом
GET  /api/migrations/<id>/connector   — статус Debezium коннектора (proxy к Kafka Connect)
GET  /api/migrations/<id>/lag         — Kafka consumer group lag
GET  /api/migrations/<id>/validation  — результат валидации
```

### Действия пользователя
```
POST /api/migrations/<id>/action
  body: { "action": "run" | "pause" | "resume" | "cancel" }
```

### Worker endpoints
```
POST /api/worker/chunks/claim
POST /api/worker/chunks/<chunk_id>/progress
POST /api/worker/chunks/<chunk_id>/complete
POST /api/worker/chunks/<chunk_id>/fail
POST /api/worker/cdc/checkin
```

### SSE — новые типы событий
```
chunk_progress    { migration_id, chunks_done, total_chunks, rows_loaded }
connector_status  { migration_id, status, connector_name }
kafka_lag         { migration_id, total_lag, updated_at }
```

---

## UI компоненты (новые)

| Компонент | Описание |
|-----------|---------|
| `BulkLoadProgress.tsx` | Прогресс-бар chunks done/total + rows/sec + оценка времени |
| `ChunkTable.tsx` | Таблица чанков: seq, rowid range, status, rows, worker, время |
| `ConnectorPanel.tsx` | Статус Debezium: status badge, topic, offset, кнопка refresh |
| `KafkaLagGauge.tsx` | Lag значение + тренд, partition breakdown |
| `ValidationResult.tsx` | count match, hash match, детали |
| `PhaseActionsBar.tsx` | Кнопки действий в зависимости от текущей фазы |

### Изменения в существующих компонентах
- `CreateMigrationModal.tsx` — добавить: chunk_size, max_parallel_workers, validate_hash_sample toggle, custom key columns UI
- `MigrationDetail.tsx` — добавить фазо-зависимые панели: BulkLoadProgress, ConnectorPanel, KafkaLagGauge, ValidationResult
- `useSSE.ts` — обработка новых событий: chunk_progress, connector_status, kafka_lag

---

## Этапы реализации

### Этап 1 — Backend foundation
1. Расширить State DB (ALTER migrations, CREATE migration_chunks, migration_cdc_state)
2. `services/debezium.py`
3. `services/oracle_scn.py`, `oracle_stage.py`, `oracle_chunker.py`
4. `services/kafka_lag.py`
5. `services/job_queue.py`
6. `services/validator.py`, `oracle_baseline.py`

### Этап 2 — Orchestrator
7. `services/orchestrator.py` — phase handler thread
8. Подключить к `app.py` (аналогично status_poller)
9. Расширить `routes/migrations.py` — action endpoint, worker endpoints

### Этап 3 — Workers
10. `workers/common.py` — общий HTTP клиент, Oracle helper, Kafka helper
11. `workers/bulk_worker.py`
12. `workers/cdc_apply_worker.py`
13. `workers/requirements.txt`

### Этап 4 — API extensions
14. `/api/migrations/<id>/chunks`, `/connector`, `/lag`, `/validation`
15. SSE новые event types

### Этап 5 — UI
16. Новые компоненты: BulkLoadProgress, ChunkTable, ConnectorPanel, KafkaLagGauge, ValidationResult, PhaseActionsBar
17. Расширить MigrationDetail, CreateMigrationModal, useSSE

### Этап 6 — Интеграционное тестирование
18. End-to-end тест (маленькая таблица)
19. Проверка идемпотентности (рестарт на каждой фазе)
20. TB stress test (chunking + bulk load)

---

## Важные нюансы

- **Kafka offset при рестарте после BASELINE_PUBLISHING:** сохранять `baseline_kafka_offset` в `migration_cdc_state` перед публикацией, чтобы CDC worker стартовал с правильной точки.
- **Stale chunks при рестарте:** при старте оркестратора все CLAIMED/RUNNING чанки старше timeout → сбрасывать в PENDING.
- **Oracle Supplemental Logging:** проверять в фазе PREPARING (`ALTER TABLE ... ADD SUPPLEMENTAL LOG DATA`).
- **Debezium коннектор:** `snapshot.mode=no_data`, `log.mining.start.scn=<start_scn>`, `table.include.list=<schema>.<table>`, `topic.prefix=<topic_prefix>`.
