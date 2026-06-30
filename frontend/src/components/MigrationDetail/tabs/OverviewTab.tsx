import React, { useState } from "react";
import type { MigrationDetail } from "../../../types/migration";
import { strategyLabel } from "../../../types/migration";
import type { SSEEvent } from "../../../hooks/useSSE";
import { t } from "../../../theme";
import { fmtTs, fmtNum } from "../../../utils/format";
import { InfoRow, SectionHeader } from "../../ui";
import {
  BulkProgressPanel, ConnectorPanel, KafkaLagPanel, ValidationPanel,
} from "../../MigrationPanels";
import {
  BULK_PHASES, CONNECTOR_PHASES, LAG_PHASES, VALIDATION_PHASES, isCdcMode,
} from "../helpers";
import { WorkerCountEditor, DataVerifyCard } from "../actions";

interface Props {
  detail:      MigrationDetail;
  migrationId: string;
  sseEvents:   SSEEvent[];
  phase:       string;
  loadDetail:  () => void;
}

function EnsureChip({ label, color, bg }: { label: string; color: string; bg: string }) {
  return (
    <span style={{
      background: bg, color, border: `1px solid ${color}44`,
      borderRadius: t.radius.sm, fontSize: t.size.xs, fontWeight: 700,
      padding: "2px 8px", display: "inline-block",
    }}>{label}</span>
  );
}

function InfoGrid({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={{ fontSize: t.size.base, marginBottom: t.space[4] }}>
      <SectionHeader>{title}</SectionHeader>
      <div style={{
        display: "grid",
        gridTemplateColumns: "minmax(160px, auto) 1fr",
        gap: "5px 16px",
      }}>
        {children}
      </div>
    </div>
  );
}

export function OverviewTab({
  detail, migrationId, sseEvents, phase, loadDetail,
}: Props) {
  const [ensureBusy,   setEnsureBusy]   = useState(false);
  const [ensureResult, setEnsureResult] = useState<any>(null);
  const [ensureErr,    setEnsureErr]    = useState("");

  const ensureTargetTable = () => {
    setEnsureBusy(true);
    setEnsureErr("");
    setEnsureResult(null);
    fetch("/api/target-prep/ensure-table", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        src_schema: detail.source_schema,
        src_table:  detail.source_table,
        tgt_schema: detail.target_schema,
        tgt_table:  detail.target_table,
      }),
    })
      .then(r => r.json())
      .then(d => {
        if (d.error) setEnsureErr(d.error);
        else setEnsureResult(d);
      })
      .catch(e => setEnsureErr(String(e)))
      .finally(() => setEnsureBusy(false));
  };

  return (
    <>
      {/* Queue position indicator */}
      {phase === "NEW" && detail.queue_position != null && (
        <div style={{
          background: t.amber.bg, border: `1px solid ${t.amber.dim}`,
          borderRadius: t.radius.md, padding: "8px 12px", marginBottom: t.space[4],
          display: "flex", alignItems: "center", gap: t.space[3],
        }}>
          <span style={{ fontSize: 22, fontWeight: 800, color: t.amber.fg }}>
            #{detail.queue_position}
          </span>
          <div>
            <div style={{ fontSize: t.size.base, color: t.amber.fg, fontWeight: 700 }}>
              В очереди на загрузку
            </div>
            <div style={{ fontSize: t.size.sm, color: t.amber.dim }}>
              Ожидание своей позиции в очереди. Для исторических таблиц и CDC-пачки SCN не фиксируется.
            </div>
          </div>
        </div>
      )}

      {/* Error block */}
      {detail.error_code && (
        <div style={{
          background: t.red.bg, border: `1px solid ${t.red.border}`,
          borderRadius: t.radius.md, padding: "8px 12px", marginBottom: t.space[4],
          fontSize: t.size.base,
        }}>
          <span style={{ color: t.red.fg, fontWeight: 700 }}>{detail.error_code}</span>
          {detail.failed_phase && (
            <span style={{ color: t.text.secondary, marginLeft: 8 }}>
              в фазе {detail.failed_phase}
            </span>
          )}
          {detail.error_text && (
            <div style={{ color: t.red.fg, marginTop: 4, opacity: 0.85 }}>
              {detail.error_text}
            </div>
          )}
        </div>
      )}

      {(phase === "DATA_VERIFYING" || phase === "DATA_MISMATCH") && detail.data_compare_task_id && (
        <DataVerifyCard taskId={detail.data_compare_task_id} phase={phase} />
      )}

      {/* Phase-specific panels */}
      {CONNECTOR_PHASES.has(phase) && isCdcMode(detail) && (
        <ConnectorPanel migrationId={migrationId} sseEvents={sseEvents} />
      )}
      {BULK_PHASES.has(phase) && (
        <BulkProgressPanel
          migrationId={migrationId}
          sseEvents={sseEvents}
          chunkType={phase === "BASELINE_LOADING" ? "BASELINE" : "BULK"}
        />
      )}
      {VALIDATION_PHASES.has(phase) && (
        <ValidationPanel migrationId={migrationId} />
      )}
      {LAG_PHASES.has(phase) && isCdcMode(detail) && (
        <KafkaLagPanel migrationId={migrationId} sseEvents={sseEvents} />
      )}

      {/* Info grids */}
      <InfoGrid title="Основное">
        <InfoRow label="ID" value={
          <span style={{ fontFamily: t.font.mono, fontSize: t.size.sm }}>
            {detail.migration_id}
          </span>
        } />
        <InfoRow label="Создана"       value={fmtTs(detail.created_at)} />
        <InfoRow label="Автор"         value={detail.created_by} />
        <InfoRow label="Описание"      value={detail.description} />
        <InfoRow label="Фаза изменена" value={fmtTs(detail.state_changed_at)} />
        <InfoRow label="Обновлена"     value={fmtTs(detail.updated_at)} />
        <InfoRow label="Повторов"      value={
          detail.retry_count > 0
            ? <span style={{ color: t.amber.base }}>{detail.retry_count}</span>
            : "0"
        } />
      </InfoGrid>

      <InfoGrid title="Источник → Цель">
        <InfoRow label="Source connection" value={detail.source_connection_id} />
        <InfoRow label="Source table"      value={`${detail.source_schema}.${detail.source_table}`} />
        <InfoRow label="Target connection" value={detail.target_connection_id} />
        <InfoRow label="Target table"      value={`${detail.target_schema}.${detail.target_table}`} />
        <InfoRow label="Stage table"       value={detail.stage_table_name} />
      </InfoGrid>

      <div style={{
        marginBottom: t.space[4], display: "flex", flexDirection: "column", gap: 6,
      }}>
        <button
          disabled={ensureBusy}
          onClick={ensureTargetTable}
          style={{
            padding: "6px 14px", borderRadius: t.radius.md,
            cursor: ensureBusy ? "not-allowed" : "pointer",
            border: `1px solid ${t.green.dim}`, background: t.green.bg, color: t.green.fg,
            fontSize: t.size.base, fontWeight: 600,
            opacity: ensureBusy ? 0.5 : 1, alignSelf: "flex-start",
          }}
        >
          {ensureBusy ? "Синхронизация…" : "Привести target в соответствие source"}
        </button>
        {ensureErr && (
          <div style={{ color: t.red.fg, fontSize: t.size.sm }}>{ensureErr}</div>
        )}
        {ensureResult && (
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
            {ensureResult.created && (
              <EnsureChip label="Таблица создана" color={t.green.fg} bg={t.green.bg} />
            )}
            {ensureResult.columns?.added?.length > 0 && (
              <EnsureChip label={`+${ensureResult.columns.added.length} колонок`} color={t.green.fg} bg={t.green.bg} />
            )}
            {ensureResult.columns?.dropped?.length > 0 && (
              <EnsureChip label={`−${ensureResult.columns.dropped.length} лишних колонок`} color={t.amber.dim} bg={t.red.bg} />
            )}
            {ensureResult.columns?.drop_errors?.length > 0 && (
              <EnsureChip label={`${ensureResult.columns.drop_errors.length} ошибок удаления колонок`} color={t.red.fg} bg={t.red.bg} />
            )}
            {ensureResult.columns?.warnings?.length > 0 && (
              <EnsureChip label={`${ensureResult.columns.warnings.length} расхождений типов`} color={t.amber.base} bg={t.amber.bg} />
            )}
            {(ensureResult.objects?.constraints?.added?.length > 0 ||
              ensureResult.objects?.indexes?.added?.length > 0 ||
              ensureResult.objects?.triggers?.added?.length > 0) && (
              <EnsureChip label={`+${
                (ensureResult.objects.constraints?.added?.length || 0) +
                (ensureResult.objects.indexes?.added?.length || 0) +
                (ensureResult.objects.triggers?.added?.length || 0)
              } объектов`} color={t.green.fg} bg={t.green.bg} />
            )}
            {(ensureResult.objects?.constraints?.errors?.length > 0 ||
              ensureResult.objects?.indexes?.errors?.length > 0 ||
              ensureResult.objects?.triggers?.errors?.length > 0) && (
              <EnsureChip label={`${
                (ensureResult.objects.constraints?.errors?.length || 0) +
                (ensureResult.objects.indexes?.errors?.length || 0) +
                (ensureResult.objects.triggers?.errors?.length || 0)
              } ошибок`} color={t.red.fg} bg={t.red.bg} />
            )}
            {!ensureResult.created &&
              ensureResult.columns?.added?.length === 0 &&
              ensureResult.columns?.dropped?.length === 0 &&
              ensureResult.columns?.warnings?.length === 0 &&
              (ensureResult.objects?.constraints?.added?.length || 0) +
              (ensureResult.objects?.indexes?.added?.length || 0) +
              (ensureResult.objects?.triggers?.added?.length || 0) === 0 &&
              (ensureResult.objects?.constraints?.errors?.length || 0) +
              (ensureResult.objects?.indexes?.errors?.length || 0) +
              (ensureResult.objects?.triggers?.errors?.length || 0) === 0 && (
              <EnsureChip label="Таблицы идентичны" color={t.green.fg} bg={t.green.bg} />
            )}
          </div>
        )}
      </div>

      <InfoGrid title="Режим миграции">
        <InfoRow label="Режим" value={
          <span style={{
            fontWeight: 700,
            color: isCdcMode(detail) ? t.purple.fg : t.green.fg,
          }}>
            {isCdcMode(detail) ? "CDC (Debezium)" : "Разовая переливка"}
          </span>
        } />
        <InfoRow label="Стратегия" value={strategyLabel(detail.strategy)} />
        <InfoRow label="TRUNCATE target" value={detail.truncate_target ? "да" : "нет"} />
      </InfoGrid>

      {isCdcMode(detail) && (
        <InfoGrid title="CDC-пачка / Kafka">
          <InfoRow label="Debezium runtime" value={detail.connector_name} />
          <InfoRow label="Topic prefix"   value={detail.topic_prefix} />
          <InfoRow label="Consumer group" value={detail.consumer_group} />
        </InfoGrid>
      )}

      <InfoGrid title="Параметры загрузки">
        <InfoRow label="Размер чанка" value={detail.chunk_size?.toLocaleString()} />
        <InfoRow label="Воркеры bulk"  value={
          <WorkerCountEditor
            migrationId={detail.migration_id}
            field="max_parallel_workers"
            value={detail.max_parallel_workers}
            onSaved={loadDetail}
          />
        } />
        <InfoRow label="Воркеры baseline" value={
          <WorkerCountEditor
            migrationId={detail.migration_id}
            field="baseline_parallel_degree"
            value={detail.baseline_parallel_degree}
            onSaved={loadDetail}
          />
        } />
        <InfoRow label="Размер порции baseline" value={
          <WorkerCountEditor
            migrationId={detail.migration_id}
            field="baseline_batch_size"
            value={detail.baseline_batch_size}
            minValue={1000}
            onSaved={loadDetail}
          />
        } />
        <InfoRow label="Total rows"   value={detail.total_rows != null ? fmtNum(detail.total_rows) : "—"} />
        <InfoRow label="Total chunks" value={detail.total_chunks ?? "—"} />
        <InfoRow label="Chunks done"  value={detail.chunks_done} />
        <InfoRow label="Rows loaded"  value={fmtNum(detail.rows_loaded)} />
        {(detail.start_scn || detail.scn_fixed_at) && (
          <>
            <InfoRow label="Start SCN"    value={detail.start_scn} />
            <InfoRow label="SCN fixed at" value={fmtTs(detail.scn_fixed_at)} />
          </>
        )}
        <InfoRow label="Hash/sample validate" value={
          detail.validate_hash_sample ? "включено" : "выключено"
        } />
      </InfoGrid>

      <InfoGrid title="Ключ">
        <InfoRow label="Key type"    value={detail.effective_key_type} />
        <InfoRow label="Key source"  value={detail.effective_key_source} />
        <InfoRow label="Key columns" value={
          <span style={{ fontFamily: t.font.mono, fontSize: t.size.sm }}>
            {detail.effective_key_columns_json}
          </span>
        } />
        <InfoRow label="PK exists"   value={detail.source_pk_exists ? "да" : "нет"} />
        <InfoRow label="UK exists"   value={detail.source_uk_exists ? "да" : "нет"} />
      </InfoGrid>
    </>
  );
}
