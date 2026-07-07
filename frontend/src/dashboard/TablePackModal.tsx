import React, { useEffect, useMemo, useState } from "react";
import { t } from "../theme";
import { S } from "./PackModalStyles";
import { Section, Field } from "./PackModalUi";
import { useApi } from "../hooks/useApi";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import { cdcRuntimeStatusLabel } from "./displayLabels";
import { PACK_DEFINITIONS, type TablePackAddMode } from "./packModel";
import { PackModeSegment } from "./PackModeSegment";
import {
  addSchemaPlanItems,
  type AddPlanItemsPayload,
  type AddPlanItemsResp,
  type MigrationPlanCdcGroup,
  type ServicesMetrics,
  type WorkerStatus,
} from "./api";

export interface BulkTable {
  source_schema: string;
  source_table:  string;
  target_schema: string;
  target_table?: string;
}

interface TableInfo {
  columns:        Array<{ name: string; type: string; nullable: boolean }>;
  pk_columns:     string[];
  uk_constraints: Array<{ name: string; columns: string[] }>;
  supplemental_log_data_all?: string | null;
  error?:         string;
}

type CdcPackAction = "append" | "replace";

interface Props {
  schemaMigrationId: string;
  tables: BulkTable[];
  initialMode?: TablePackAddMode;
  cdcGroup?: MigrationPlanCdcGroup | null;
  cdcGroupLoading?: boolean;
  cdcGroupError?: string | null;
  modeLocked?: boolean;
  onClose: () => void;
  onReloadCdcGroup?: () => void | Promise<void>;
  onDone: (packQueueId: number, count: number, response: AddPlanItemsResp) => void | Promise<void>;
}

export function TablePackModal({
  schemaMigrationId,
  tables,
  initialMode = "historical",
  cdcGroup,
  cdcGroupLoading = false,
  cdcGroupError = null,
  modeLocked = false,
  onClose,
  onReloadCdcGroup,
  onDone,
}: Props) {
  const [mode, setMode] = useState<TablePackAddMode>(initialMode);
  const [strategy, setStrategy] = useState<AddPlanItemsPayload["strategy"]>(
    initialMode === "cdc" ? "CDC_DIRECT" : "BULK_DIRECT",
  );
  const [sequential, setSequential] = useState(true);
  const [truncateTarget, setTruncateTarget] = useState(true);
  const [chunkSize, setChunkSize] = useState(1_000_000);
  const [workers, setWorkers] = useState(1);
  const [baselinePd, setBaselinePd] = useState(4);
  const [stageTablespace, setStageTablespace] = useState("PAYSTAGE");
  const [tableInfos, setTableInfos] = useState<Record<string, TableInfo>>({});
  const [keyColumns, setKeyColumns] = useState<Record<string, string[]>>({});
  const [infoLoading, setInfoLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [cdcPackAction, setCdcPackAction] = useState<CdcPackAction>("append");
  const [err, setErr] = useState("");
  const workerStatusApi = useApi<WorkerStatus>(
    mode === "cdc" ? "/api/workers/status" : null,
    { intervalMs: 10000, enabled: mode === "cdc" },
  );
  const servicesMetricsApi = useApi<ServicesMetrics>(
    mode === "cdc" ? "/api/services/metrics" : null,
    { intervalMs: 10000, enabled: mode === "cdc" },
  );

  const usesStage = strategy.endsWith("_STAGE");
  const forcesTargetTruncate = strategy === "CDC_DIRECT";
  const truncateLocked = usesStage || forcesTargetTruncate;
  const tablesKey = useMemo(
    () => tables.map(x => `${x.source_schema}.${x.source_table}`).join("|"),
    [tables],
  );
  const selectedKeys = useMemo(
    () => new Set(tables.map(x => `${x.source_schema.toUpperCase()}.${x.source_table.toUpperCase()}`)),
    [tables],
  );
  const rawConnectorTables = cdcGroup?.tables || [];
  const connectorTables = rawConnectorTables;
  const connectorTableKeys = new Set(connectorTables.map(cdcTableKey));
  const connectorSelectedTables = connectorTables.filter(t => selectedKeys.has(cdcTableKey(t)));
  const connectorOtherTables = connectorTables.filter(t => !selectedKeys.has(cdcTableKey(t)));
  const connectorNewTables = tables.filter(t => !connectorTableKeys.has(rowKey(t)));
  const shouldPruneCdcPack = mode === "cdc" && cdcPackAction === "replace" && connectorOtherTables.length > 0;
  const projectedConnectorLabels = shouldPruneCdcPack
    ? tables.map(rowKey)
    : [
        ...connectorTables.map(cdcTableLabel),
        ...connectorNewTables.map(rowKey),
      ];
  const projectedPreview = projectedConnectorLabels.slice(0, 8);
  const projectedRest = Math.max(0, projectedConnectorLabels.length - projectedPreview.length);
  const connectorOtherPreview = connectorOtherTables.slice(0, 8).map(cdcTableLabel);
  const connectorOtherRest = Math.max(0, connectorOtherTables.length - connectorOtherPreview.length);
  const projectedConnectorCount = projectedConnectorLabels.length;
  const projectedIncludeList = projectedConnectorLabels.join(",");
  const cdcBaseSubmitLabel = shouldPruneCdcPack
    ? `Оставить ${tables.length} в CDC-пачке и добавить в очередь`
    : projectedConnectorCount > tables.length
      ? `Добавить ${tables.length} в очередь, синхронизировать ${projectedConnectorCount} в Debezium`
      : `Добавить ${tables.length} в CDC-пачку`;
  const cdcRuntimeLoading = mode === "cdc" && (workerStatusApi.loading || servicesMetricsApi.loading);
  const cdcRuntimeKnown = mode === "cdc" && !!workerStatusApi.data && !!servicesMetricsApi.data && !cdcRuntimeLoading;
  const cdcRuntimeIssues = cdcRuntimeKnown ? [
    workerStatusApi.data?.cdc_ready === true ? "" : "CDC worker",
    servicesMetricsApi.data?.oracle_source?.ok === true ? "" : "Oracle source",
    servicesMetricsApi.data?.oracle_target?.ok === true ? "" : "Oracle target",
    servicesMetricsApi.data?.kafka?.ok === true ? "" : "Kafka",
    servicesMetricsApi.data?.kafka_connect?.ok === true ? "" : "Kafka Connect",
  ].filter(Boolean) : [];
  const cdcRuntimeWillWait = cdcRuntimeKnown && cdcRuntimeIssues.length > 0;
  const cdcSubmitLabel = cdcRuntimeWillWait
    ? "Добавить в CDC-пачку: будет ждать runtime"
    : cdcBaseSubmitLabel;
  const submitDisabled = busy || (mode === "cdc" && (infoLoading || cdcGroupLoading || !!cdcGroupError));

  function rowKey(x: BulkTable) {
    return `${x.source_schema.toUpperCase()}.${x.source_table.toUpperCase()}`;
  }

  function hasSuppLog(info?: TableInfo) {
    const raw = String(info?.supplemental_log_data_all ?? "").trim().toUpperCase();
    if (!raw) return null;
    return raw === "YES" || raw === "TRUE" || raw === "1";
  }

  function cdcTableKey(x: { source_schema: string; source_table: string }) {
    return `${x.source_schema.toUpperCase()}.${x.source_table.toUpperCase()}`;
  }

  function cdcTableLabel(x: { source_schema: string; source_table: string }) {
    return cdcTableKey(x);
  }

  useEffect(() => {
    setPackMode(initialMode);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialMode]);

  useEffect(() => {
    if (truncateLocked && !truncateTarget) setTruncateTarget(true);
  }, [truncateLocked, truncateTarget]);

  useEffect(() => {
    if (mode !== "cdc") {
      setTableInfos({});
      setKeyColumns({});
      setInfoLoading(false);
      return;
    }

    let cancelled = false;
    setInfoLoading(true);
    setErr("");

    Promise.all(tables.map(async table => {
      const key = rowKey(table);
      const p = new URLSearchParams({
        schema: table.source_schema,
        table:  table.source_table,
      });
      try {
        const r = await fetch(`/api/db/source/table-info?${p.toString()}`);
        const d = await r.json().catch(() => ({}));
        if (!r.ok || d.error) {
          return [key, {
            columns: [],
            pk_columns: [],
            uk_constraints: [],
            error: d.error || `HTTP ${r.status}`,
          } as TableInfo] as const;
        }
        return [key, d as TableInfo] as const;
      } catch (e) {
        return [key, {
          columns: [],
          pk_columns: [],
          uk_constraints: [],
          error: e instanceof Error ? e.message : String(e),
        } as TableInfo] as const;
      }
    })).then(entries => {
      if (cancelled) return;
      const nextInfos = Object.fromEntries(entries);
      setTableInfos(nextInfos);
      setKeyColumns(prev => {
        const next: Record<string, string[]> = {};
        for (const table of tables) {
          const key = rowKey(table);
          const info = nextInfos[key];
          if (!info || info.pk_columns.length > 0) continue;
          next[key] = prev[key] ?? info.uk_constraints[0]?.columns ?? [];
        }
        return next;
      });
    }).finally(() => {
      if (!cancelled) setInfoLoading(false);
    });

    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, tablesKey]);

  function setPackMode(next: TablePackAddMode) {
    setMode(next);
    setErr("");
    setCdcPackAction("append");
    if (next === "historical") {
      setStrategy("BULK_DIRECT");
      setWorkers(1);
    } else {
      setStrategy("CDC_DIRECT");
      setWorkers(4);
      setSequential(true);
    }
  }

  async function submit() {
    if (busy) return;
    setBusy(true);
    setErr("");
    try {
      if (mode === "cdc") {
        if (cdcGroupLoading) {
          setErr("Дождитесь загрузки состава CDC-пачки.");
          return;
        }
        if (cdcGroupError) {
          setErr(`Не удалось загрузить состав CDC-пачки: ${cdcGroupError}`);
          return;
        }
        if (infoLoading) {
          setErr("Дождитесь загрузки ключей таблиц.");
          return;
        }
        for (const table of tables) {
          const key = rowKey(table);
          const info = tableInfos[key];
          if (!info) {
            setErr(`Не удалось получить ключи для ${key}.`);
            return;
          }
          if (info.error) {
            setErr(`${key}: ${info.error}`);
            return;
          }
          const suppLog = hasSuppLog(info);
          if (suppLog !== true) {
            setErr(suppLog === false
              ? `${key}: для CDC нужен supplemental logging ALL COLUMNS.`
              : `${key}: не удалось проверить supplemental logging ALL COLUMNS.`);
            return;
          }
          if (info.pk_columns.length === 0 && info.uk_constraints.length === 0 && (keyColumns[key] ?? []).length === 0) {
            setErr(`Для ${key} без PK нужно выбрать колонки CDC-ключа.`);
            return;
          }
        }
      }

      const payload: AddPlanItemsPayload = {
        tables: tables.map(t => ({
          source_table: t.source_table,
          target_table: t.target_table || t.source_table,
          key_columns: mode === "cdc"
            && (tableInfos[rowKey(t)]?.pk_columns.length ?? 0) === 0
            && (tableInfos[rowKey(t)]?.uk_constraints.length ?? 0) === 0
            ? keyColumns[rowKey(t)] ?? []
            : undefined,
        })),
        connector_group_id: mode === "cdc" ? cdcGroup?.group_id : undefined,
        strategy,
        sequential: mode === "cdc" ? true : sequential,
        truncate_target: truncateLocked ? true : truncateTarget,
        chunk_size: chunkSize,
        max_parallel_workers: workers,
        baseline_parallel_degree: baselinePd,
        stage_tablespace: usesStage ? stageTablespace.trim().toUpperCase() : undefined,
        prune_cdc_pack: shouldPruneCdcPack,
      };
      const res = await addSchemaPlanItems(schemaMigrationId, payload);
      await onDone(res.plan_id, res.items.length, res);
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={S.overlay} onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={{ ...S.modal, maxWidth: 640 }}>
        <div style={S.header}>
          <span style={{ fontSize: 15, fontWeight: 700, color: t.text.primary }}>
            {mode === "cdc" ? `Добавить в ${PACK_DEFINITIONS.cdc.title}` : `Добавить в ${PACK_DEFINITIONS.bulk.title.toLowerCase()}`}
          </span>
          <span style={{ fontSize: 12, color: t.text.muted, fontFamily: t.font.mono }}>
            {tables.length} таблиц
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: t.text.disabled,
            cursor: "pointer", fontSize: 20, lineHeight: 1, padding: "0 2px",
          }}>x</button>
        </div>

        <div style={S.body}>
          {err && (
            <div style={{
              padding: "8px 10px", borderRadius: t.radius.sm,
              background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
              color: t.red.fg, fontSize: 12,
            }}>
              {err}
            </div>
          )}

          <Section title={modeLocked ? "Пачка" : "Режим"}>
            <PackModeSegment value={mode} onChange={setPackMode} locked={modeLocked} disabled={busy} />
            <div style={{
              padding: "9px 12px",
              borderRadius: t.radius.md,
              background: mode === "cdc" ? t.blue.bg : t.amber.bg,
              border: `1px solid ${mode === "cdc" ? t.blue.dim : t.amber.dim}`,
              color: mode === "cdc" ? t.blue.fg : t.amber.fg,
              fontSize: 12,
              lineHeight: 1.45,
            }}>
              {mode === "cdc"
                ? "Таблицы попадут в единственную CDC-пачку этой миграции. Если её ещё нет, coordinator создаст её автоматически. После добавления пачка и перенос данных стартуют автоматически."
                : "SCN не фиксируется. Используйте только для таблиц, которые уже не меняются на source. Для DIRECT target будет подготовлен перед загрузкой: триггеры отключаются, вторичные индексы пересчитываются после переноса."}
            </div>
            <Field label="Стратегия">
              <select value={strategy} onChange={e => setStrategy(e.target.value as AddPlanItemsPayload["strategy"])} style={S.select}>
                {mode === "historical" ? (
                  <>
                    <option value="BULK_DIRECT">BULK_DIRECT</option>
                    <option value="BULK_STAGE">BULK_STAGE</option>
                  </>
                ) : (
                  <>
                    <option value="CDC_DIRECT">CDC_DIRECT</option>
                    <option value="CDC_STAGE">CDC_STAGE</option>
                  </>
                )}
              </select>
            </Field>
            {usesStage && (
              <Field label="Stage tablespace">
                <input value={stageTablespace} onChange={e => setStageTablespace(e.target.value)} style={S.input}/>
              </Field>
            )}
          </Section>

          {mode === "cdc" && (
            <Section title={PACK_DEFINITIONS.cdc.title}>
              {cdcGroupLoading ? (
                <div style={{
                  padding: "9px 10px",
                  border: `1px solid ${t.border.subtle}`,
                  borderRadius: t.radius.sm,
                  background: t.bg.s1,
                  color: t.text.muted,
                  fontSize: 12,
                }}>
                  Загружаю текущий состав CDC-пачки...
                </div>
              ) : cdcGroupError ? (
                <div style={{
                  padding: "9px 10px",
                  border: `1px solid ${t.red.border}`,
                  borderRadius: t.radius.sm,
                  background: `${t.red.border}22`,
                  color: t.red.fg,
                  fontSize: 12,
                }}>
                  Не удалось загрузить состав CDC-пачки: {cdcGroupError}
                </div>
              ) : cdcGroup ? (
                <div style={{
                  display: "grid",
                  gap: 8,
                  padding: "9px 10px",
                  border: `1px solid ${t.border.subtle}`,
                  borderRadius: t.radius.sm,
                  background: t.bg.s1,
                  fontSize: 12,
                  color: t.text.secondary,
                }}>
                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <span>Статус: <strong style={{ color: t.text.primary }}>{cdcRuntimeStatusLabel(cdcGroup.status)}</strong></span>
                    <span>Сейчас в CDC-пачке: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{connectorTables.length}</strong></span>
                    <span>Добавится новых: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{connectorNewTables.length}</strong></span>
                    <span>Будет в table.include.list: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{projectedConnectorCount}</strong></span>
                  </div>
                  <div style={{
                    padding: "7px 8px",
                    borderRadius: t.radius.sm,
                    border: `1px solid ${connectorOtherTables.length ? t.amber.dim : t.blue.dim}`,
                    background: connectorOtherTables.length ? t.amber.bg : t.blue.bg,
                    color: connectorOtherTables.length ? t.amber.fg : t.blue.fg,
                    lineHeight: 1.45,
                  }}>
                    CDC-пачка управляет Debezium runtime. После сохранения в Kafka Connect уйдет полный список пачки,
                    не только выбранные сейчас таблицы.
                    <div style={{ marginTop: 4, fontFamily: t.font.mono, color: t.text.primary, overflowWrap: "anywhere" }}>
                      {projectedPreview.length > 0 ? projectedPreview.join(", ") : "пока нет таблиц"}
                      {projectedRest > 0 && <span style={{ color: t.text.muted }}> +{projectedRest} еще</span>}
                    </div>
                  </div>
                  {connectorSelectedTables.length > 0 && (
                    <div style={{ color: t.amber.fg }}>
                      Уже есть в CDC-пачке, но строка очереди будет создана: {connectorSelectedTables.map(cdcTableLabel).join(", ")}
                    </div>
                  )}
                  {connectorOtherTables.length > 0 && (
                    <div style={{
                      display: "grid",
                      gap: 8,
                      padding: "8px",
                      borderRadius: t.radius.sm,
                      border: `1px solid ${t.amber.dim}`,
                      background: t.bg.s2,
                    }}>
                      <div style={{ color: t.amber.fg, lineHeight: 1.4 }}>
                        В CDC-пачке уже есть другие таблицы: {connectorOtherPreview.join(", ")}
                        {connectorOtherRest > 0 && <span style={{ color: t.text.muted }}> +{connectorOtherRest} еще</span>}
                      </div>
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                        <button
                          type="button"
                          disabled={busy}
                          onClick={() => setCdcPackAction("append")}
                          style={{
                            ...secondaryActionStyle(busy),
                            justifyContent: "flex-start",
                            borderColor: cdcPackAction === "append" ? t.blue.base : t.border.subtle,
                            background: cdcPackAction === "append" ? t.blue.bg : t.bg.s1,
                            color: cdcPackAction === "append" ? t.blue.fg : t.text.secondary,
                          }}
                        >
                          Добавить к пачке
                        </button>
                        <button
                          type="button"
                          disabled={busy}
                          onClick={() => setCdcPackAction("replace")}
                          style={{
                            ...secondaryActionStyle(busy),
                            justifyContent: "flex-start",
                            borderColor: cdcPackAction === "replace" ? t.amber.base : t.border.subtle,
                            background: cdcPackAction === "replace" ? t.amber.bg : t.bg.s1,
                            color: cdcPackAction === "replace" ? t.amber.fg : t.text.secondary,
                          }}
                        >
                          Оставить только выбранные
                        </button>
                      </div>
                      <div style={{
                        color: shouldPruneCdcPack ? t.amber.fg : t.text.secondary,
                        lineHeight: 1.4,
                      }}>
                        {shouldPruneCdcPack
                          ? "При сохранении остальные таблицы будут удалены из CDC-пачки в одной транзакции с созданием очереди. Активные миграции backend не даст удалить."
                          : "При сохранении выбранные таблицы добавятся к текущему составу. Все таблицы CDC-пачки останутся в Debezium table.include.list."}
                      </div>
                    </div>
                  )}
                  {connectorOtherTables.length > 0 && (
                    <div style={{
                      display: "grid",
                      gap: 4,
                      borderTop: `1px solid ${t.border.subtle}`,
                      paddingTop: 7,
                    }}>
                      {connectorOtherTables.map(table => {
                        const label = cdcTableLabel(table);
                        return (
                          <div key={table.id || label} style={{
                              fontFamily: t.font.mono,
                              color: t.text.primary,
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                              whiteSpace: "nowrap",
                            }}>
                              {label}
                          </div>
                        );
                      })}
                    </div>
                  )}
                  {projectedIncludeList && (
                    <div style={{
                      fontFamily: t.font.mono,
                      fontSize: 11,
                      color: t.text.muted,
                      overflowWrap: "anywhere",
                    }}>
                      После сохранения table.include.list: {projectedIncludeList}
                    </div>
                  )}
                </div>
              ) : (
                <div style={{
                  display: "grid",
                  gap: 6,
                  padding: "9px 10px",
                  border: `1px solid ${t.blue.dim}`,
                  borderRadius: t.radius.sm,
                  background: t.blue.bg,
                  color: t.blue.fg,
                  fontSize: 12,
                  lineHeight: 1.45,
                }}>
                  <div>
                    CDC-пачка еще не создана. Она будет создана автоматически для этой миграции.
                  </div>
                  {projectedIncludeList && (
                    <div style={{
                      fontFamily: t.font.mono,
                      color: t.text.primary,
                      overflowWrap: "anywhere",
                    }}>
                      table.include.list: {projectedIncludeList}
                    </div>
                  )}
                </div>
              )}
            </Section>
          )}

          {mode === "cdc" && (
            <Section title="Готовность запуска CDC">
              <div style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(145px, 1fr))",
                gap: 8,
              }}>
                <RuntimeTile
                  label="Worker"
                  ok={workerStatusApi.data?.cdc_ready === true}
                  loading={workerStatusApi.loading}
                  error={workerStatusApi.error || (!workerStatusApi.loading && workerStatusApi.data && !workerStatusApi.data.cdc_ready ? "CDC worker не активен" : "")}
                  detail={workerStatusApi.data ? `${workerStatusApi.data.active_count} активен` : ""}
                />
                <RuntimeTile
                  label="Oracle source"
                  ok={servicesMetricsApi.data?.oracle_source?.ok === true}
                  loading={servicesMetricsApi.loading}
                  error={servicesMetricsApi.error || servicesMetricsApi.data?.oracle_source?.error || ""}
                  detail={servicesMetricsApi.data?.oracle_source?.host || ""}
                />
                <RuntimeTile
                  label="Oracle target"
                  ok={servicesMetricsApi.data?.oracle_target?.ok === true}
                  loading={servicesMetricsApi.loading}
                  error={servicesMetricsApi.error || servicesMetricsApi.data?.oracle_target?.error || ""}
                  detail={servicesMetricsApi.data?.oracle_target?.host || ""}
                />
                <RuntimeTile
                  label="Kafka"
                  ok={servicesMetricsApi.data?.kafka?.ok === true}
                  loading={servicesMetricsApi.loading}
                  error={servicesMetricsApi.error || servicesMetricsApi.data?.kafka?.error || ""}
                  detail={servicesMetricsApi.data?.kafka?.bootstrap || ""}
                />
                <RuntimeTile
                  label="Kafka Connect"
                  ok={servicesMetricsApi.data?.kafka_connect?.ok === true}
                  loading={servicesMetricsApi.loading}
                  error={servicesMetricsApi.error || servicesMetricsApi.data?.kafka_connect?.error || ""}
                  detail={servicesMetricsApi.data?.kafka_connect?.url || ""}
                />
              </div>
              <div style={{
                marginTop: 8,
                fontSize: 12,
                lineHeight: 1.45,
                color: t.text.muted,
              }}>
                Зеленый статус означает, что после добавления таблицы coordinator сможет стартовать CDC-пачку и worker сможет забрать CDC apply. Если есть красный статус, строка останется в ожидании до исправления runtime.
              </div>
              {cdcRuntimeWillWait && (
                <div style={{
                  marginTop: 8,
                  padding: "7px 9px",
                  borderRadius: t.radius.sm,
                  border: `1px solid ${t.amber.dim}`,
                  background: t.amber.bg,
                  color: t.amber.fg,
                  fontSize: 12,
                  lineHeight: 1.4,
                }}>
                  При сохранении таблица добавится в CDC-пачку и очередь, но перенос не начнется до готовности: {cdcRuntimeIssues.join(", ")}.
                </div>
              )}
            </Section>
          )}

          {mode === "cdc" && (
            <Section title="Ключи CDC">
              <div style={{ display: "grid", gap: 8 }}>
                {infoLoading && (
                  <div style={{ fontSize: 12, color: t.text.muted }}>
                    Загружаю PK/UK и колонки выбранных таблиц...
                  </div>
                )}
                {!infoLoading && tables.map(table => {
                  const key = rowKey(table);
                  const info = tableInfos[key];
                  const selected = keyColumns[key] ?? [];
                  const supp = hasSuppLog(info);
                  if (!info) {
                    return (
                      <div key={key} style={{ fontSize: 12, color: t.text.muted }}>
                        {key}: metadata не загружена
                      </div>
                    );
                  }
                  if (info.error) {
                    return (
                      <div key={key} style={{
                        padding: "8px 10px",
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.red.border}`,
                        background: `${t.red.border}22`,
                        color: t.red.fg,
                        fontSize: 12,
                      }}>
                        {key}: {info.error}
                      </div>
                    );
                  }
                  if (info.pk_columns.length > 0) {
                    return (
                      <div key={key} style={{
                        padding: "8px 10px",
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.green.dim}`,
                        background: t.green.bg,
                        color: t.green.fg,
                        fontSize: 12,
                      }}>
                        <strong style={{ fontFamily: t.font.mono }}>{key}</strong>
                        <span style={{ marginLeft: 8 }}>PK: {info.pk_columns.join(", ")}</span>
                        <span style={{ marginLeft: 8, color: supp === false ? t.red.fg : t.green.fg }}>
                          {supp === false ? "NO SUPP" : supp === true ? "SUPP" : ""}
                        </span>
                      </div>
                    );
                  }
                  if (info.uk_constraints.length > 0) {
                    const uk = info.uk_constraints[0];
                    return (
                      <div key={key} style={{
                        padding: "8px 10px",
                        borderRadius: t.radius.sm,
                        border: `1px solid ${t.purple.base}`,
                        background: t.purple.bg,
                        color: t.purple.fg,
                        fontSize: 12,
                      }}>
                        <strong style={{ fontFamily: t.font.mono }}>{key}</strong>
                        <span style={{ marginLeft: 8 }}>UK: {uk.name} ({uk.columns.join(", ")})</span>
                        <span style={{ marginLeft: 8, color: supp === false ? t.red.fg : t.green.fg }}>
                          {supp === false ? "NO SUPP" : supp === true ? "SUPP" : ""}
                        </span>
                      </div>
                    );
                  }
                  return (
                    <div key={key} style={{
                      border: `1px solid ${selected.length ? t.blue.dim : t.red.border}`,
                      borderRadius: t.radius.sm,
                      background: t.bg.s1,
                      padding: 10,
                    }}>
                      <div style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 8,
                        marginBottom: 8,
                        color: t.text.primary,
                        fontSize: 12,
                      }}>
                        <strong style={{ fontFamily: t.font.mono }}>{key}</strong>
                        <span style={{ color: selected.length ? t.blue.fg : t.red.fg }}>
                          PK нет, выберите CDC-ключ
                        </span>
                        {supp !== null && (
                          <span style={{ color: supp ? t.green.fg : t.red.fg }}>
                            {supp ? "SUPP" : "NO SUPP"}
                          </span>
                        )}
                      </div>

                      <div style={{
                        display: "grid",
                        gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
                        gap: 6,
                      }}>
                        {info.columns.map(col => {
                          const checked = selected.includes(col.name);
                          return (
                            <label key={col.name} style={{
                              display: "flex",
                              alignItems: "center",
                              gap: 6,
                              padding: "5px 7px",
                              borderRadius: t.radius.sm,
                              border: `1px solid ${checked ? t.blue.dim : t.border.subtle}`,
                              background: checked ? t.blue.bg : t.bg.s2,
                              color: checked ? t.blue.fg : t.text.secondary,
                              fontSize: 11.5,
                              fontFamily: t.font.mono,
                              minWidth: 0,
                            }}>
                              <input
                                type="checkbox"
                                checked={checked}
                                onChange={e => {
                                  setKeyColumns(prev => {
                                    const cur = prev[key] ?? [];
                                    const next = e.target.checked
                                      ? [...cur, col.name]
                                      : cur.filter(x => x !== col.name);
                                    return { ...prev, [key]: next };
                                  });
                                }}
                              />
                              <span style={{ overflow: "hidden", textOverflow: "ellipsis" }}>
                                {col.name}
                              </span>
                            </label>
                          );
                        })}
                      </div>
                    </div>
                  );
                })}
              </div>
            </Section>
          )}

          <Section title="Порядок и нагрузка">
            <label style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13, color: t.text.primary }}>
              <input
                type="checkbox"
                checked={mode === "cdc" ? true : sequential}
                disabled={mode === "cdc"}
                onChange={e => setSequential(e.target.checked)}
              />
              <span>Каждую таблицу отдельным шагом очереди</span>
            </label>
            <label style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 13, color: t.text.primary }}>
              <input
                type="checkbox"
                checked={truncateLocked ? true : truncateTarget}
                disabled={truncateLocked}
                onChange={e => setTruncateTarget(e.target.checked)}
              />
              <span>
                TRUNCATE target перед загрузкой
                {usesStage ? " (обязательно для STAGE)" : forcesTargetTruncate ? " (обязательно для CDC_DIRECT)" : ""}
              </span>
            </label>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10 }}>
              <Field label="Размер чанка">
                <input type="number" value={chunkSize} onChange={e => setChunkSize(Number(e.target.value) || 1_000_000)} style={S.input}/>
              </Field>
              <Field label="Воркеры bulk">
                <input type="number" value={workers} onChange={e => setWorkers(Number(e.target.value) || 1)} style={S.input}/>
              </Field>
              <Field label="Воркеры baseline">
                <input type="number" value={baselinePd} onChange={e => setBaselinePd(Number(e.target.value) || 4)} style={S.input}/>
              </Field>
            </div>
          </Section>

          <Section title="Таблицы">
            <div style={{
              maxHeight: 180, overflowY: "auto",
              border: `1px solid ${t.border.subtle}`, borderRadius: t.radius.sm,
              fontFamily: t.font.mono, fontSize: 11.5,
            }}>
              {tables.map((x, i) => (
                <div key={`${x.source_table}-${i}`} style={{
                  padding: "5px 8px",
                  borderBottom: i === tables.length - 1 ? "none" : `1px solid ${t.bg.s2}`,
                  color: t.text.secondary,
                }}>
                  {x.source_schema}.{x.source_table} -&gt; {x.target_schema}.{x.target_table || x.source_table}
                </div>
              ))}
            </div>
          </Section>
        </div>

        <div style={S.footer}>
          <button onClick={onClose} disabled={busy} style={secondaryActionStyle(busy)}>Отмена</button>
          <button onClick={submit} disabled={submitDisabled} style={primaryActionStyle(submitDisabled)}>
            {busy ? "Добавление..." : mode === "cdc" ? cdcSubmitLabel : `Добавить в ${PACK_DEFINITIONS.bulk.title.toLowerCase()}`}
          </button>
        </div>
      </div>
    </div>
  );
}

function RuntimeTile({
  label,
  ok,
  loading,
  error,
  detail,
}: {
  label: string;
  ok: boolean;
  loading: boolean;
  error?: string;
  detail?: string;
}) {
  const tone = loading ? null : ok ? t.green : t.red;
  const value = loading ? "..." : ok ? "OK" : "нет";
  return (
    <div style={{
      minWidth: 0,
      padding: "8px 9px",
      borderRadius: t.radius.sm,
      border: `1px solid ${tone ? tone.dim : t.border.subtle}`,
      background: tone ? tone.bg : t.bg.s1,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
        <span style={{ fontSize: 12, color: t.text.primary, fontWeight: 700 }}>{label}</span>
        <span style={{
          fontSize: 11,
          fontFamily: t.font.mono,
          color: tone ? tone.fg : t.text.muted,
          fontWeight: 700,
        }}>
          {value}
        </span>
      </div>
      <div style={{
        marginTop: 4,
        fontSize: 11,
        color: ok ? t.text.muted : t.red.fg,
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}>
        {loading ? "проверка..." : ok ? (detail || "ready") : (error || "not ready")}
      </div>
    </div>
  );
}
