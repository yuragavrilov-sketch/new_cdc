import React, { useMemo } from "react";
import { t } from "../theme";
import type { SSEEvent } from "../hooks/useSSE";
import { useApi } from "../hooks/useApi";
import { ProgressBar } from "../components/ui";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import { PackDetailSection, PackItemGroup } from "./PackDetails";
import { PackWorkbench } from "./PackWorkbench";
import { cdcRuntimeStatusLabel, packQueueStatusLabel } from "./displayLabels";
import {
  PACK_DEFINITIONS,
  ddlJobPackCounts,
  isCdcPackItem,
  packItemCounts,
  packQueueGroups,
  type PackModel,
} from "./packModel";
import type {
  MigrationPlanCdcGroup,
  MigrationPlanCdcTable,
  MigrationPlanDetail,
  MigrationPlanItem,
  StartMigrationPlanResp,
  WorkerStatus,
  CdcNextAction,
  DdlJob,
} from "./api";

interface Props {
  packQueue: MigrationPlanDetail | null;
  loading: boolean;
  onStart: () => void;
  onReload: () => void;
  onOpenDetails?: () => void;
  busy: boolean;
  error: string;
  variant?: "overview" | "detail";
  cdcGroup?: MigrationPlanCdcGroup | null;
  sseEvents?: SSEEvent[];
  ddlJobs?: DdlJob[];
  ddlLoading?: boolean;
  ddlBusy?: boolean;
  ddlError?: string;
  ddlFeedback?: string;
  onStartDdlPack?: () => void;
}

const DONE = new Set(["DONE"]);
const BAD = new Set(["FAILED", "CANCELLED"]);
const ACTIVE_WORK_PHASES = new Set([
  "PREPARING",
  "SCN_FIXED",
  "CONNECTOR_STARTING",
  "CDC_BUFFERING",
  "TOPIC_CREATING",
  "CHUNKING",
  "BULK_LOADING",
  "BULK_LOADED",
  "STAGE_VALIDATING",
  "STAGE_VALIDATED",
  "BASELINE_PUBLISHING",
  "BASELINE_LOADING",
  "BASELINE_PUBLISHED",
  "STAGE_DROPPING",
  "INDEXES_ENABLING",
  "DATA_VERIFYING",
  "CDC_APPLY_STARTING",
  "CDC_APPLYING",
  "CDC_CATCHING_UP",
  "CDC_CAUGHT_UP",
]);

interface CdcConnectorActionResp {
  status?: string;
  error?: string;
  plan_starts?: StartMigrationPlanResp[];
  plan_start_error?: string | null;
  cdc_queue_kicked?: boolean;
  cdc_next_action?: CdcNextAction | null;
}

interface DebeziumSyncStatus {
  connector_name: string;
  exists: boolean;
  in_sync: boolean;
  desired_table_include_list: string;
  actual_table_include_list: string | null;
  desired_message_key_columns: string;
  actual_message_key_columns: string | null;
  missing_tables: string[];
  extra_tables: string[];
  key_columns_match: boolean;
}

interface TargetTriggerJob {
  job_id: string;
  state: "PENDING" | "RUNNING" | "DONE" | "FAILED";
  enabled_count: number;
  error_text: string | null;
}

export function PackPanel({
  packQueue,
  loading,
  onStart,
  onReload,
  onOpenDetails,
  busy,
  error,
  variant = "detail",
  cdcGroup: cdcGroupProp = null,
  sseEvents = [],
  ddlJobs = [],
  ddlLoading = false,
  ddlBusy = false,
  ddlError = "",
  ddlFeedback = "",
  onStartDdlPack,
}: Props) {
  const batches = useMemo(() => {
    const map = new Map<number, MigrationPlanItem[]>();
    for (const item of packQueue?.items || []) {
      const key = item.batch_order || 1;
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(item);
    }
    return Array.from(map.entries()).sort((a, b) => a[0] - b[0]);
  }, [packQueue]);
  const effectiveCdcGroup = packQueue?.cdc_group || cdcGroupProp || null;
  const [cdcActionBusy, setCdcActionBusy] = React.useState("");
  const [cdcActionErr, setCdcActionErr] = React.useState("");
  const [cdcActionInfo, setCdcActionInfo] = React.useState("");
  const [cdcSyncStatus, setCdcSyncStatus] = React.useState<DebeziumSyncStatus | null>(null);
  const [cdcSyncStatusErr, setCdcSyncStatusErr] = React.useState("");
  const [cdcSyncStatusLoading, setCdcSyncStatusLoading] = React.useState(false);
  const workerStatusApi = useApi<WorkerStatus>(
    effectiveCdcGroup ? "/api/workers/status" : null,
    { intervalMs: 10000, enabled: !!effectiveCdcGroup },
  );
  const cdcSyncFingerprint = [
    effectiveCdcGroup?.group_id || "",
    effectiveCdcGroup?.status || "",
    effectiveCdcGroup?.run_id || "",
    effectiveCdcGroup?.table_include_list || "",
    effectiveCdcGroup?.message_key_columns || "",
  ].join("|");

  const loadDebeziumSyncStatus = React.useCallback((groupId: string | null | undefined) => {
    if (!groupId) {
      setCdcSyncStatus(null);
      setCdcSyncStatusErr("");
      setCdcSyncStatusLoading(false);
      return;
    }
    setCdcSyncStatusErr("");
    setCdcSyncStatusLoading(true);
    fetch(`/api/connector-groups/${groupId}/debezium-sync-status`)
      .then(async r => {
        const body = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(body?.error || `HTTP ${r.status}`);
        return body as DebeziumSyncStatus;
      })
      .then(setCdcSyncStatus)
      .catch(e => {
        setCdcSyncStatus(null);
        setCdcSyncStatusErr(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setCdcSyncStatusLoading(false));
  }, []);

  React.useEffect(() => {
    loadDebeziumSyncStatus(effectiveCdcGroup?.group_id);
  }, [cdcSyncFingerprint, effectiveCdcGroup?.group_id, loadDebeziumSyncStatus]);

  async function syncCdcGroup(group: MigrationPlanCdcGroup) {
    setCdcActionBusy("sync");
    setCdcActionErr("");
    setCdcActionInfo("");
    try {
      const res = await fetch(`/api/connector-groups/${group.group_id}/refresh-tables`, { method: "POST" });
      const body = await res.json().catch(() => ({})) as CdcConnectorActionResp;
      if (!res.ok) {
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      onReload();
      loadDebeziumSyncStatus(group.group_id);
      const status = String(body.status || group.status || "").toUpperCase();
      const syncText = status && status !== "RUNNING"
        ? `CDC-пачка ${cdcRuntimeStatusLabel(status)}; Debezium синхронизируется после запуска`
        : "Debezium синхронизирован";
      if (body.plan_start_error) {
        setCdcActionErr(`${syncText}, но CDC очередь не продолжена: ${body.plan_start_error}`);
      } else if (body.cdc_next_action?.message) {
        if (body.cdc_next_action.level === "error") setCdcActionErr(body.cdc_next_action.message);
        else setCdcActionInfo(body.cdc_next_action.message);
      } else {
        const startedCount = (body.plan_starts || []).reduce(
          (sum: number, item: { started?: unknown[] }) => sum + (item.started?.length || 0),
          0,
        );
        setCdcActionInfo(startedCount
          ? `${syncText}, ${status === "RUNNING" ? "запущено CDC строк" : "CDC строк ждут CDC-пачку"}: ${startedCount}`
          : body.cdc_queue_kicked
            ? `${syncText}, очередь CDC продолжена`
            : syncText);
      }
    } catch (e) {
      setCdcActionErr(e instanceof Error ? e.message : String(e));
    } finally {
      setCdcActionBusy("");
    }
  }

  async function startCdcGroup(group: MigrationPlanCdcGroup) {
    setCdcActionBusy("start");
    setCdcActionErr("");
    setCdcActionInfo("");
    try {
      const res = await fetch(`/api/connector-groups/${group.group_id}/start`, { method: "POST" });
      const body = await res.json().catch(() => ({})) as CdcConnectorActionResp;
      if (!res.ok) {
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      onReload();
      loadDebeziumSyncStatus(group.group_id);
      const connectorStatus = String(body.status || "").toUpperCase();
      const connectorText = connectorStatus === "RUNNING"
        ? "CDC-пачка работает"
        : connectorStatus
          ? `Запуск CDC-пачки: ${cdcRuntimeStatusLabel(connectorStatus)}`
          : "Запуск CDC-пачки запрошен";
      if (body.plan_start_error) {
        setCdcActionErr(`${connectorText}, но очередь не продолжена: ${body.plan_start_error}`);
      } else if (body.cdc_next_action?.message) {
        if (body.cdc_next_action.level === "error") setCdcActionErr(body.cdc_next_action.message);
        else setCdcActionInfo(body.cdc_next_action.message);
      } else {
        const startedCount = (body.plan_starts || []).reduce(
          (sum: number, item: { started?: unknown[] }) => sum + (item.started?.length || 0),
          0,
        );
        const rowText = connectorStatus === "RUNNING"
          ? "запущено CDC строк"
          : "CDC строк переведено в ожидание CDC-пачки";
        setCdcActionInfo(startedCount
          ? `${connectorText}, ${rowText}: ${startedCount}`
          : body.cdc_queue_kicked
            ? `${connectorText}, очередь CDC продолжена`
          : connectorText);
      }
    } catch (e) {
      setCdcActionErr(e instanceof Error ? e.message : String(e));
    } finally {
      setCdcActionBusy("");
    }
  }

  async function removeCdcGroupTable(group: MigrationPlanCdcGroup, table: MigrationPlanCdcTable) {
    const label = tableLabel(table);
    if (!window.confirm(`Убрать ${label} из CDC-пачки? Debezium table.include.list будет обновлен.`)) return;
    setCdcActionBusy(label);
    setCdcActionErr("");
    setCdcActionInfo("");
    try {
      const res = await fetch(
        `/api/connector-groups/${group.group_id}/tables/${encodeURIComponent(table.source_schema)}/${encodeURIComponent(table.source_table)}`,
        { method: "DELETE" },
      );
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      const body = await res.json().catch(() => ({}));
      onReload();
      loadDebeziumSyncStatus(group.group_id);
      if (body.sync_error) {
        setCdcActionErr(`Таблица убрана из CDC-пачки, но Debezium не синхронизирован: ${body.sync_error}`);
      } else {
        setCdcActionInfo(`${label} убрана из CDC-пачки`);
      }
    } catch (e) {
      setCdcActionErr(e instanceof Error ? e.message : String(e));
    } finally {
      setCdcActionBusy("");
    }
  }

  if (!packQueue && loading) {
    return <Shell><Muted>Загрузка пачки...</Muted></Shell>;
  }
  if (!packQueue) {
    return (
      <Shell>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: effectiveCdcGroup ? 10 : 0 }}>
          <div>
            <Title>Пачки миграции</Title>
            <Muted>
              {effectiveCdcGroup
                ? "Очередь пачек ещё не создана, но CDC-пачка этой миграции уже содержит таблицы."
                : `Пока нет очереди пачек для этой миграции. Выделите таблицы и добавьте их в ${PACK_DEFINITIONS.bulk.title.toLowerCase()} или ${PACK_DEFINITIONS.cdc.title}.`}
            </Muted>
          </div>
        </div>
        {effectiveCdcGroup && (
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            <CdcConnectorCard
              group={effectiveCdcGroup}
              packItems={[]}
              busyAction={cdcActionBusy}
              syncStatus={cdcSyncStatus}
              syncStatusLoading={cdcSyncStatusLoading}
              syncStatusErr={cdcSyncStatusErr}
              workerStatus={workerStatusApi.data}
              workerStatusLoading={workerStatusApi.loading}
              workerStatusErr={workerStatusApi.error || ""}
              onSync={syncCdcGroup}
              onStart={startCdcGroup}
              showExtraTables={false}
            />
            <CdcConnectorDetails
              group={effectiveCdcGroup}
              packItems={[]}
              packSourceSchema=""
              busyKey={cdcActionBusy}
              onRemoveExtra={removeCdcGroupTable}
            />
          </div>
        )}
        <div style={{ marginTop: effectiveCdcGroup ? 8 : 10 }}>
          <DdlPackDetails
            jobs={ddlJobs}
            loading={ddlLoading}
            busy={ddlBusy}
            error={ddlError}
            feedback={ddlFeedback}
            onStart={onStartDdlPack}
          />
        </div>
        {cdcActionErr && (
          <div style={{
            marginTop: 10, padding: "7px 10px", borderRadius: t.radius.sm,
            background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
            color: t.red.fg, fontSize: 12,
          }}>
            {cdcActionErr}
          </div>
        )}
        {cdcActionInfo && (
          <div style={{
            marginTop: 10, padding: "7px 10px", borderRadius: t.radius.sm,
            background: t.green.bg, border: `1px solid ${t.green.dim}`,
            color: t.green.fg, fontSize: 12,
          }}>
            {cdcActionInfo}
          </div>
        )}
      </Shell>
    );
  }

  const total = packQueue.items.length;
  const done = packQueue.items.filter(isDoneItem).length;
  const failed = packQueue.items.filter(isFailedItem).length;
  const active = packQueue.items.filter(isActiveWorkItem).length;
  const running = packQueue.items.filter(isRunningItem).length;
  const pending = packQueue.items.filter(isQueuedItem).length;
  const actualPending = packQueue.items.filter(i => i.status === "PENDING").length;
  const progress = total ? done / total * 100 : 0;
  const hasPending = actualPending > 0;
  const nextPendingBatch = batches.find(([, items]) => items.some(i => i.status === "PENDING"));
  const nextPendingItems = nextPendingBatch?.[1].filter(i => i.status === "PENDING") || [];
  const runningItems = packQueue.items.filter(isRunningItem);
  const runningHasNonCdc = runningItems.some(i => !isCdcPackItem(i));
  const nextPendingHasNonCdc = nextPendingItems.some(i => !isCdcPackItem(i));
  const canStart = ["READY", "RUNNING"].includes(packQueue.status)
    && hasPending
    && !(runningHasNonCdc && nextPendingHasNonCdc);
  const currentBatch = batches.find(([, items]) => items.some(isActiveWorkItem))
    || batches.find(([, items]) => items.some(isRunningItem))
    || batches.find(([, items]) => items.some(i => i.status === "PENDING"))
    || batches[batches.length - 1];
  const detailPackModels: PackModel[] = (() => {
    const groups = packQueueGroups(packQueue.items);
    const bulk = groups.find(group => group.key === "bulk")?.items || [];
    const cdc = groups.find(group => group.key === "cdc")?.items || [];
    const ddlCounts = ddlJobPackCounts(ddlJobs);
    return [
      {
        key: PACK_DEFINITIONS.bulk.key,
        title: PACK_DEFINITIONS.bulk.title,
        subtitle: PACK_DEFINITIONS.bulk.subtitle,
        tone: PACK_DEFINITIONS.bulk.tone,
        ...packItemCounts(bulk),
        actions: [
          {
            label: busy ? "Запуск..." : "Запустить",
            onClick: onStart,
            disabled: !canStart || busy || bulk.length === 0,
            primary: true,
          },
        ],
      },
      {
        key: PACK_DEFINITIONS.cdc.key,
        title: PACK_DEFINITIONS.cdc.title,
        subtitle: effectiveCdcGroup?.status
          ? `Runtime CDC-пачки: ${cdcRuntimeStatusLabel(effectiveCdcGroup.status)}, таблиц ${effectiveCdcGroup.tables?.length || 0}`
          : PACK_DEFINITIONS.cdc.subtitle,
        tone: PACK_DEFINITIONS.cdc.tone,
        ...packItemCounts(cdc),
        total: Math.max(cdc.length, effectiveCdcGroup?.tables?.length || 0),
        actions: [
          {
            label: busy ? "Запуск..." : "Запустить",
            onClick: onStart,
            disabled: !canStart || busy || (cdc.length === 0 && !effectiveCdcGroup),
            primary: true,
          },
          {
            label: "Синхронизировать",
            onClick: () => effectiveCdcGroup && syncCdcGroup(effectiveCdcGroup),
            disabled: !effectiveCdcGroup || !!cdcActionBusy,
          },
        ],
      },
      {
        key: PACK_DEFINITIONS.ddl.key,
        title: PACK_DEFINITIONS.ddl.title,
        subtitle: PACK_DEFINITIONS.ddl.subtitle,
        tone: PACK_DEFINITIONS.ddl.tone,
        ...ddlCounts,
        actions: [
          {
            label: ddlBusy ? "Запуск..." : "Запустить",
            onClick: onStartDdlPack || (() => {}),
            disabled: !onStartDdlPack || ddlBusy || (ddlCounts.draft || 0) === 0,
            primary: true,
          },
        ],
        feedback: ddlError || ddlFeedback,
      },
    ];
  })();

  return (
    <Shell>
      <div style={{
        display: "flex", justifyContent: "space-between",
        alignItems: "flex-start", gap: 12, marginBottom: 12,
      }}>
        <div>
          <Title>Пачки миграции</Title>
          <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", marginTop: 5 }}>
            <Badge tone={failed ? "bad" : packQueue.status === "RUNNING" ? "run" : done === total ? "ok" : "idle"}>
              {packQueueStatusLabel(packQueue.status)}
            </Badge>
            <span style={{ fontFamily: t.font.mono, color: t.text.muted, fontSize: 12 }}>
              #{packQueue.plan_id} · {done}/{total} готово · {active} в работе
            </span>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          {variant === "overview" && onOpenDetails && (
            <button onClick={onOpenDetails} style={secondaryActionStyle(false)}>Детали</button>
          )}
          <button onClick={onReload} style={secondaryActionStyle(false)}>Обновить</button>
          <button
            onClick={onStart}
            disabled={!canStart || busy}
            style={{
              ...primaryActionStyle(busy),
              opacity: canStart && !busy ? 1 : 0.45,
              cursor: canStart && !busy ? "pointer" : "default",
            }}
          >
            {busy ? "Запуск..." : packQueue.status === "RUNNING" ? "Продолжить" : "Старт"}
          </button>
        </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
        <ProgressBar value={progress} tone={failed ? "error" : "info"} height={7}/>
        <span style={{ minWidth: 52, textAlign: "right", fontFamily: t.font.mono, fontSize: 12 }}>
          {progress.toFixed(0)}%
        </span>
      </div>

      {error && (
        <div style={{
          marginBottom: 10, padding: "7px 10px", borderRadius: t.radius.sm,
          background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
          color: t.red.fg, fontSize: 12,
        }}>
          {error}
        </div>
      )}
      {cdcActionErr && (
        <div style={{
          marginBottom: 10, padding: "7px 10px", borderRadius: t.radius.sm,
          background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
          color: t.red.fg, fontSize: 12,
        }}>
          {cdcActionErr}
        </div>
      )}
      {cdcActionInfo && (
        <div style={{
          marginBottom: 10, padding: "7px 10px", borderRadius: t.radius.sm,
          background: t.green.bg, border: `1px solid ${t.green.dim}`,
          color: t.green.fg, fontSize: 12,
        }}>
          {cdcActionInfo}
        </div>
      )}

      {variant === "overview" && (
        <PackQueueOverview
          batchCount={batches.length}
          total={total}
          done={done}
          running={active}
          pending={pending}
          failed={failed}
          currentBatch={currentBatch}
          items={packQueue.items}
          packSourceSchema={packQueue.src_schema}
          cdcGroup={effectiveCdcGroup}
          cdcActionBusy={cdcActionBusy}
          cdcSyncStatus={cdcSyncStatus}
          cdcSyncStatusLoading={cdcSyncStatusLoading}
          cdcSyncStatusErr={cdcSyncStatusErr}
          workerStatus={workerStatusApi.data}
          workerStatusLoading={workerStatusApi.loading}
          workerStatusErr={workerStatusApi.error || ""}
          onSyncCdcGroup={syncCdcGroup}
          onStartCdcGroup={startCdcGroup}
          canStart={canStart}
        />
      )}

      {variant === "detail" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          <PackWorkbench packs={detailPackModels} />
          {packQueueGroups(packQueue.items).map(pack => (
            <TablePackDetails
              key={pack.key}
              title={pack.title}
              kind={pack.key}
              items={pack.items}
              cdcGroupStatus={pack.key === "cdc" ? effectiveCdcGroup?.status : undefined}
              onReload={onReload}
              sseEvents={sseEvents}
            >
              {pack.key === "cdc" && effectiveCdcGroup && (
                <>
                  <CdcConnectorCard
                    group={effectiveCdcGroup}
                    packItems={pack.items}
                    packSourceSchema={packQueue.src_schema}
                    busyAction={cdcActionBusy}
                    syncStatus={cdcSyncStatus}
                    syncStatusLoading={cdcSyncStatusLoading}
                    syncStatusErr={cdcSyncStatusErr}
                    workerStatus={workerStatusApi.data}
                    workerStatusLoading={workerStatusApi.loading}
                    workerStatusErr={workerStatusApi.error || ""}
                    onSync={syncCdcGroup}
                    onStart={startCdcGroup}
                  />
                  <CdcConnectorDetails
                    group={effectiveCdcGroup}
                    packItems={pack.items}
                    packSourceSchema={packQueue.src_schema}
                    busyKey={cdcActionBusy}
                    onRemoveExtra={removeCdcGroupTable}
                  />
                </>
              )}
            </TablePackDetails>
          ))}
          <DdlPackDetails
            jobs={ddlJobs}
            loading={ddlLoading}
            busy={ddlBusy}
            error={ddlError}
            feedback={ddlFeedback}
            onStart={onStartDdlPack}
          />
        </div>
      )}
    </Shell>
  );
}

function TablePackDetails({
  title,
  kind,
  items,
  cdcGroupStatus,
  onReload,
  sseEvents,
  children,
}: {
  title: string;
  kind: "bulk" | "cdc";
  items: MigrationPlanItem[];
  cdcGroupStatus?: string;
  onReload: () => void;
  sseEvents: SSEEvent[];
  children?: React.ReactNode;
}) {
  const sortedItems = sortPackItems(items);
  const orderColumnTitle = kind === "cdc" ? "CDC-шаг" : "Шаг";
  return (
    <PackDetailSection
      title={title}
      countText={`${items.length} таблиц`}
      empty={items.length === 0}
      emptyText="Таблицы ещё не добавлены."
    >
      <PackItemGroup title="Таблицы в пачке" countText={`${sortedItems.length} таблиц`}>
        <PackTableHeader orderTitle={orderColumnTitle} />
        {sortedItems.map(item => (
          <PackQueueRow
            key={item.item_id}
            kind={kind}
            item={item}
            cdcGroupStatus={cdcGroupStatus}
            onReload={onReload}
            sseEvents={sseEvents}
          />
        ))}
      </PackItemGroup>
      {children}
    </PackDetailSection>
  );
}

function sortPackItems(items: MigrationPlanItem[]) {
  return [...items].sort((a, b) => {
    const byBatch = (a.batch_order || 1) - (b.batch_order || 1);
    if (byBatch !== 0) return byBatch;
    const bySort = (a.sort_order || 0) - (b.sort_order || 0);
    if (bySort !== 0) return bySort;
    return a.table_name.localeCompare(b.table_name);
  });
}

function PackTableHeader({ orderTitle }: { orderTitle: string }) {
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: packQueueRowGridTemplate,
      gap: 10,
      alignItems: "center",
      padding: "6px 10px",
      background: t.bg.s1,
      borderTop: `1px solid ${t.border.subtle}`,
      color: t.text.muted,
      fontSize: 10,
      fontWeight: 700,
      textTransform: "uppercase",
    }}>
      <span>{orderTitle}</span>
      <span>Таблица</span>
      <span>Статус</span>
      <span style={{ textAlign: "right" }}>Прогресс</span>
      <span style={{ textAlign: "right" }}>Действие</span>
    </div>
  );
}

function DdlPackDetails({
  jobs,
  loading,
  busy,
  error,
  feedback,
  onStart,
}: {
  jobs: DdlJob[];
  loading: boolean;
  busy: boolean;
  error: string;
  feedback?: string;
  onStart?: () => void;
}) {
  const draftCount = jobs.filter(job => job.state === "DRAFT").length;
  const sorted = [...jobs].sort((a, b) => {
    const rank: Record<string, number> = { DRAFT: 0, PENDING: 1, CLAIMED: 2, RUNNING: 3, FAILED: 4, DONE: 5, CANCELLED: 6 };
    const ra = rank[a.state] ?? 9;
    const rb = rank[b.state] ?? 9;
    if (ra !== rb) return ra - rb;
    return `${a.object_type}.${a.object_name}`.localeCompare(`${b.object_type}.${b.object_name}`);
  });
  return (
    <PackDetailSection
      title={PACK_DEFINITIONS.ddl.title}
      countText={`${jobs.length} объектов`}
      loading={loading}
      empty={jobs.length === 0}
      emptyText="DDL-объекты еще не добавлены."
      error={error}
      action={{
        label: busy ? "Запуск..." : `Запустить · ${draftCount}`,
        onClick: onStart,
        disabled: !onStart || busy || draftCount === 0,
      }}
    >
      {!error && feedback && (
        <div style={{
          padding: "7px 10px",
          borderRadius: t.radius.sm,
          background: t.bg.s2,
          border: `1px solid ${t.border.subtle}`,
          color: t.text.muted,
          fontSize: 12,
          fontFamily: t.font.mono,
        }}>
          {feedback}
        </div>
      )}
      {sorted.length > 0 && (
        <PackItemGroup title="Объекты DDL" countText={`${sorted.length} объектов`}>
          {sorted.map(job => <DdlJobRow key={job.job_id} job={job}/>)}
        </PackItemGroup>
      )}
    </PackDetailSection>
  );
}

function DdlJobRow({ job }: { job: DdlJob }) {
  const visual = ddlJobVisualState(job);
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "minmax(180px, 1fr) minmax(130px, auto) minmax(160px, 220px) minmax(120px, auto)",
      gap: 10,
      alignItems: "center",
      padding: "7px 10px",
      borderTop: `1px solid ${t.bg.s1}`,
      fontSize: 12,
    }}>
      <div style={{ minWidth: 0 }}>
        <div style={{
          fontFamily: t.font.mono,
          color: t.text.primary,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}>
          {job.object_type}.{job.object_name}
        </div>
        {job.error_text && (
          <div style={{ color: t.red.fg, fontSize: 11, marginTop: 2, overflow: "hidden", textOverflow: "ellipsis" }}>
            {job.error_text}
          </div>
        )}
      </div>
      <Badge tone={visual === "done" ? "ok" : visual === "running" ? "run" : visual === "failed" ? "bad" : "idle"}>
        {ddlJobStateLabel(job.state)}
      </Badge>
      <div style={{
        fontFamily: t.font.mono,
        color: t.text.muted,
        textAlign: "right",
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}>
        {ddlJobTypeLabel(job.job_type)} · {ddlActionLabel(job.action)}
      </div>
      <div style={{
        fontFamily: t.font.mono,
        color: t.text.muted,
        textAlign: "right",
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}>
        {job.completed_at ? "готово" : job.started_at ? "запущено" : "ожидает"}
      </div>
    </div>
  );
}

function ddlJobStateLabel(state: DdlJob["state"]) {
  switch (state) {
    case "DRAFT": return "черновик";
    case "PENDING": return "в очереди";
    case "CLAIMED": return "забрано";
    case "RUNNING": return "в работе";
    case "DONE": return "готово";
    case "FAILED": return "ошибка";
    case "CANCELLED": return "отменено";
    default: return state;
  }
}

function ddlJobTypeLabel(jobType: string) {
  const key = String(jobType || "").toLowerCase();
  if (key.includes("index")) return "индексы";
  if (key.includes("constraint")) return "ограничения";
  if (key.includes("trigger")) return "триггеры";
  if (key.includes("sequence")) return "сиквенсы";
  if (key.includes("view") || key.includes("mview")) return "представления";
  if (key.includes("grant")) return "права";
  if (key.includes("synonym")) return "синонимы";
  if (key.includes("code") || key.includes("plsql") || key.includes("package") || key.includes("procedure") || key.includes("function")) {
    return "PL/SQL";
  }
  return jobType || "DDL";
}

function ddlActionLabel(action: string) {
  switch (action) {
    case "create_missing": return "создать отсутствующие";
    case "sync_diff": return "синхронизировать отличия";
    case "recreate": return "пересоздать";
    case "enable": return "включить";
    case "disable": return "отключить";
    case "rebuild": return "пересчитать";
    default: return action || "действие";
  }
}

function ddlJobVisualState(job: DdlJob): "done" | "failed" | "queued" | "running" | "idle" {
  if (job.state === "DONE") return "done";
  if (job.state === "FAILED" || job.state === "CANCELLED") return "failed";
  if (job.state === "RUNNING") return "running";
  if (job.state === "DRAFT" || job.state === "PENDING" || job.state === "CLAIMED") return "queued";
  return "idle";
}

function PackQueueOverview({
  batchCount,
  total,
  done,
  running,
  pending,
  failed,
  currentBatch,
  items,
  packSourceSchema,
  cdcGroup,
  cdcActionBusy,
  cdcSyncStatus,
  cdcSyncStatusLoading,
  cdcSyncStatusErr,
  workerStatus,
  workerStatusLoading,
  workerStatusErr,
  onSyncCdcGroup,
  onStartCdcGroup,
  canStart,
}: {
  batchCount: number;
  total: number;
  done: number;
  running: number;
  pending: number;
  failed: number;
  currentBatch?: [number, MigrationPlanItem[]];
  items: MigrationPlanItem[];
  packSourceSchema: string;
  cdcGroup: MigrationPlanCdcGroup | null;
  cdcActionBusy: string;
  cdcSyncStatus: DebeziumSyncStatus | null;
  cdcSyncStatusLoading: boolean;
  cdcSyncStatusErr: string;
  workerStatus: WorkerStatus | null;
  workerStatusLoading: boolean;
  workerStatusErr: string;
  onSyncCdcGroup: (group: MigrationPlanCdcGroup) => void;
  onStartCdcGroup: (group: MigrationPlanCdcGroup) => void;
  canStart: boolean;
}) {
  const [batchNo, batchItems]: [number, MigrationPlanItem[]] = currentBatch || [0, []];
  const batchDone = batchItems.filter(isDoneItem).length;
  const batchRunning = batchItems.filter(isActiveWorkItem).length;
  const batchFailed = batchItems.filter(isFailedItem).length;
  const currentIsCdc = batchItems.length > 0 && batchItems.every(isCdcPackItem);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(5, minmax(92px, 1fr))",
        gap: 8,
      }}>
        <Stat label="Шагов" value={batchCount}/>
        <Stat label="Таблиц" value={total}/>
        <Stat label="Готово" value={done}/>
        <Stat label="В работе" value={running}/>
        <Stat label="Ошибки" value={failed}/>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        {packQueueGroups(items).map(pack => <PackCard key={pack.key} title={pack.title} items={pack.items}/>)}
      </div>

      {cdcGroup && (
        <CdcConnectorCard
          group={cdcGroup}
          packItems={items.filter(isCdcPackItem)}
          packSourceSchema={packSourceSchema}
          busyAction={cdcActionBusy}
          syncStatus={cdcSyncStatus}
          syncStatusLoading={cdcSyncStatusLoading}
          syncStatusErr={cdcSyncStatusErr}
          workerStatus={workerStatus}
          workerStatusLoading={workerStatusLoading}
          workerStatusErr={workerStatusErr}
          onSync={onSyncCdcGroup}
          onStart={onStartCdcGroup}
        />
      )}

      <div style={{
        display: "grid",
        gridTemplateColumns: "minmax(0, 1fr)",
        gap: 10,
      }}>
        <div style={{
          padding: "8px 10px",
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.md,
          background: t.bg.s2,
          minWidth: 0,
        }}>
          <div style={{ fontSize: 11, color: t.text.muted, marginBottom: 5 }}>
            {currentIsCdc ? "Текущий CDC-шаг" : "Текущий шаг запуска"}
          </div>
          {batchNo ? (
            <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
              <span style={{ fontSize: 13, fontWeight: 700, color: t.text.primary }}>
                {currentIsCdc ? `CDC-шаг ${batchNo}` : `Шаг ${batchNo}`}
              </span>
              <span style={{ fontSize: 12, color: t.text.muted }}>
                {batchDone}/{batchItems.length} готово · {batchRunning} в работе · {batchFailed} ошибок
              </span>
            </div>
          ) : (
            <div style={{ fontSize: 12, color: t.text.muted }}>Нет таблиц в пачке</div>
          )}
        </div>
      </div>

      {pending > 0 && canStart && failed === 0 && (
        <div style={{ fontSize: 12, color: t.text.muted }}>
          Готово к запуску: в очереди {pending} таблиц.
        </div>
      )}
    </div>
  );
}

function CdcConnectorCard({
  group,
  packItems,
  packSourceSchema = "",
  busyAction,
  syncStatus,
  syncStatusLoading,
  syncStatusErr,
  workerStatus,
  workerStatusLoading,
  workerStatusErr,
  onSync,
  onStart,
  showExtraTables = true,
}: {
  group: MigrationPlanCdcGroup;
  packItems: MigrationPlanItem[];
  packSourceSchema?: string;
  busyAction: string;
  syncStatus: DebeziumSyncStatus | null;
  syncStatusLoading: boolean;
  syncStatusErr: string;
  workerStatus: WorkerStatus | null;
  workerStatusLoading: boolean;
  workerStatusErr: string;
  onSync: (group: MigrationPlanCdcGroup) => void;
  onStart: (group: MigrationPlanCdcGroup) => void;
  showExtraTables?: boolean;
}) {
  const packKeys = new Set(packItems.map(item => packItemTableKey(item, packSourceSchema)));
  const connectorTables = group.tables || [];
  const extraTables = showExtraTables
    ? connectorTables.filter(tbl => !packKeys.has(cdcTableKey(tbl)))
    : [];
  const keyColsCount = connectorTables.filter(tbl => {
    if (tbl.source_pk_exists || tbl.source_uk_exists) return false;
    const raw = tbl.effective_key_columns_json;
    if (Array.isArray(raw)) return raw.length > 0;
    return String(raw || "[]") !== "[]";
  }).length;
  const connectorPreview = connectorTables.slice(0, 6).map(tableLabel);
  const connectorRest = Math.max(0, connectorTables.length - connectorPreview.length);
  const status = String(group.status || "").toUpperCase();
  const pendingDraftCdc = packItems.filter(item => {
    const itemStatus = String(item.status || "").toUpperCase();
    const phase = String(item.phase || "").toUpperCase();
    return itemStatus === "PENDING" || phase === "DRAFT";
  }).length;
  const waitingConnector = packItems.filter(item => isNewPhase(item) && status !== "RUNNING").length;
  const runnableNewCdc = packItems.filter(item => isNewPhase(item) && status === "RUNNING");
  const queuedCdc = runnableNewCdc.filter(item => item.queue_position != null).length;
  const readyCdc = runnableNewCdc.length - queuedCdc;
  const loadingCdc = packItems.filter(item => {
    const phase = String(item.phase || "").toUpperCase();
    return [
      "TOPIC_CREATING",
      "CHUNKING",
      "BULK_LOADING",
      "BULK_LOADED",
      "STAGE_VALIDATING",
      "STAGE_VALIDATED",
      "BASELINE_PUBLISHING",
      "BASELINE_LOADING",
      "BASELINE_PUBLISHED",
      "STAGE_DROPPING",
      "INDEXES_ENABLING",
    ].includes(phase);
  }).length;
  const waitingWorkerCdc = packItems.filter(item => {
    const phase = String(item.phase || "").toUpperCase();
    return (phase === "CDC_APPLY_STARTING" || phase === "CDC_APPLYING") && !item.cdc_worker_heartbeat;
  }).length;
  const applyingCdc = packItems.filter(item => {
    const phase = String(item.phase || "").toUpperCase();
    if (phase === "CDC_CATCHING_UP") return true;
    return (phase === "CDC_APPLY_STARTING" || phase === "CDC_APPLYING") && !!item.cdc_worker_heartbeat;
  }).length;
  const hasRawConfig = Boolean(
    group.table_include_list
    || group.active_topic_prefix
    || group.topic_prefix
    || group.message_key_columns,
  );
  const canStartConnector = !["RUNNING", "TOPICS_CREATING", "CONNECTOR_STARTING", "STOPPING"].includes(status);
  const syncBusy = busyAction === "sync";
  const startBusy = busyAction === "start";
  const syncProblem = Boolean(
    syncStatusErr
    || (syncStatus && (!syncStatus.exists || !syncStatus.in_sync)),
  );
  const activeWorkers = workerStatus?.active_count ?? 0;
  const cdcWorkerReady = workerStatus?.cdc_ready === true;

  return (
    <div style={{
      padding: "9px 10px",
      border: `1px solid ${extraTables.length ? t.amber.dim : t.border.subtle}`,
      borderRadius: t.radius.md,
      background: extraTables.length ? t.amber.bg : t.bg.s2,
      minWidth: 0,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        <div style={{ display: "flex", gap: 8, alignItems: "center", minWidth: 0, flexWrap: "wrap" }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: t.text.primary }}>CDC-пачка</div>
          <Badge tone={group.status === "RUNNING" ? "run" : group.status === "FAILED" ? "bad" : "idle"}>
            {cdcRuntimeStatusLabel(group.status)}
          </Badge>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", minWidth: 0 }}>
          <span style={{ fontFamily: t.font.mono, color: t.text.muted, fontSize: 11, overflow: "hidden", textOverflow: "ellipsis" }}>
            {group.active_connector_name || group.connector_name}
          </span>
          {canStartConnector && (
            <button
              onClick={() => onStart(group)}
              disabled={!!busyAction}
              style={{
                ...primaryActionStyle(!!busyAction),
                padding: "3px 8px",
                fontSize: 11,
                opacity: startBusy ? 0.55 : 1,
              }}
            >
              {startBusy ? "Запуск..." : "Запустить"}
            </button>
          )}
          <button
            onClick={() => onSync(group)}
            disabled={!!busyAction}
            style={{
              ...secondaryActionStyle(false),
              padding: "3px 8px",
              fontSize: 11,
              opacity: syncBusy ? 0.55 : 1,
            }}
          >
            {syncBusy ? "Синхронизация..." : "Синхронизировать"}
          </button>
        </div>
      </div>
      <div style={{ marginTop: 7, display: "flex", gap: 12, flexWrap: "wrap", fontSize: 12, color: t.text.muted }}>
        <span>Таблиц в CDC-пачке: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{connectorTables.length}</strong></span>
        <span>Строк CDC в пачке: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{packItems.length}</strong></span>
        <span>Не запущены: <strong style={{ color: pendingDraftCdc ? t.amber.fg : t.text.primary, fontFamily: t.font.mono }}>{pendingDraftCdc}</strong></span>
        <span>Ждут CDC-пачку: <strong style={{ color: waitingConnector ? t.amber.fg : t.text.primary, fontFamily: t.font.mono }}>{waitingConnector}</strong></span>
        <span>Стартуют: <strong style={{ color: readyCdc ? t.blue.fg : t.text.primary, fontFamily: t.font.mono }}>{readyCdc}</strong></span>
        <span>В очереди: <strong style={{ color: queuedCdc ? t.blue.fg : t.text.primary, fontFamily: t.font.mono }}>{queuedCdc}</strong></span>
        <span>Переносятся: <strong style={{ color: loadingCdc ? t.blue.fg : t.text.primary, fontFamily: t.font.mono }}>{loadingCdc}</strong></span>
        <span>Ждут worker: <strong style={{ color: waitingWorkerCdc ? t.amber.fg : t.text.primary, fontFamily: t.font.mono }}>{waitingWorkerCdc}</strong></span>
        <span>Применяются: <strong style={{ color: applyingCdc ? t.green.fg : t.text.primary, fontFamily: t.font.mono }}>{applyingCdc}</strong></span>
        <span>Worker: <strong style={{ color: cdcWorkerReady ? t.green.fg : t.amber.fg, fontFamily: t.font.mono }}>{workerStatusLoading ? "..." : workerStatusErr ? "?" : cdcWorkerReady ? `${activeWorkers} активен` : "не активен"}</strong></span>
        <span>Ручных ключей: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{keyColsCount}</strong></span>
      </div>
      {(workerStatusErr || (!workerStatusLoading && workerStatus && !cdcWorkerReady)) && (
        <div style={{
          marginTop: 7,
          padding: "6px 8px",
          borderRadius: t.radius.sm,
          border: `1px solid ${t.amber.dim}`,
          background: t.amber.bg,
          color: t.amber.fg,
          fontSize: 12,
          lineHeight: 1.4,
        }}>
          {workerStatusErr
            ? `Статус worker не прочитан: ${workerStatusErr}`
            : "CDC worker не активен. Таблицы можно добавить в пачку, но apply начнется после запуска universal worker.py."}
        </div>
      )}
      <div style={{ marginTop: 7, fontSize: 12, color: t.text.secondary, lineHeight: 1.45 }}>
        {connectorPreview.length > 0 ? (
          <>
            table.include.list строится из всего состава CDC-пачки: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{connectorPreview.join(", ")}</span>
            {connectorRest > 0 && <span style={{ color: t.text.muted }}> +{connectorRest} еще</span>}
          </>
        ) : (
          <span style={{ color: t.text.muted }}>В CDC-пачке пока нет таблиц.</span>
        )}
      </div>
      {extraTables.length > 0 && (
        <div style={{ marginTop: 7, fontSize: 12, color: t.amber.fg }}>
          В CDC-пачке уже есть таблицы без активной строки в очереди: {extraTables.map(tableLabel).join(", ")}
        </div>
      )}
      {(syncStatusLoading || syncStatusErr || syncStatus) && (
        <div style={{
          marginTop: 7,
          padding: "6px 8px",
          borderRadius: t.radius.sm,
          border: `1px solid ${
            syncStatusErr
              ? t.red.border
              : syncProblem
                ? t.amber.dim
                : t.green.dim
          }`,
          background: syncStatusErr
            ? `${t.red.border}22`
            : syncProblem
              ? t.amber.bg
              : t.green.bg,
          color: syncStatusErr
            ? t.red.fg
            : syncProblem
              ? t.amber.fg
              : t.green.fg,
          fontSize: 12,
          lineHeight: 1.4,
          overflowWrap: "anywhere",
        }}>
          {syncStatusLoading && <div>Проверяю фактический config Kafka Connect...</div>}
          {syncStatusErr && <div>Kafka Connect config не прочитан: {syncStatusErr}</div>}
          {syncStatus && (
            <>
              <div>
                Конфиг Runtime CDC-пачки: <strong>{syncStatus.exists ? (syncStatus.in_sync ? "совпадает" : "есть расхождение") : "runtime не создан"}</strong>
                {" "}({syncStatus.connector_name})
              </div>
              {syncStatus.missing_tables.length > 0 && (
                <div style={{ marginTop: 3 }}>
                  Нет в Kafka Connect: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{syncStatus.missing_tables.join(", ")}</span>
                </div>
              )}
              {syncStatus.extra_tables.length > 0 && (
                <div style={{ marginTop: 3 }}>
                  Лишние в Kafka Connect: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{syncStatus.extra_tables.join(", ")}</span>
                </div>
              )}
              {!syncStatus.key_columns_match && (
                <div style={{ marginTop: 3 }}>Расходятся CDC key columns.</div>
              )}
              {syncStatus.actual_table_include_list && (
                <div style={{ marginTop: 3, fontFamily: t.font.mono, color: t.text.primary }}>
                  actual table.include.list: {syncStatus.actual_table_include_list}
                </div>
              )}
            </>
          )}
        </div>
      )}
      {group.error_text && (
        <div style={{
          marginTop: 7,
          padding: "6px 8px",
          borderRadius: t.radius.sm,
          border: `1px solid ${t.red.border}`,
          background: `${t.red.border}22`,
          color: t.red.fg,
          fontSize: 12,
          lineHeight: 1.35,
          overflowWrap: "anywhere",
        }}>
          {group.error_text}
        </div>
      )}
      {group.status !== "RUNNING" && packItems.some(item => String(item.phase || "").toUpperCase() === "NEW") && (
        <div style={{ marginTop: 7, fontSize: 12, color: t.text.muted }}>
          CDC-строки ждут запуска CDC-пачки и продолжат работу после перехода runtime в состояние "работает".
        </div>
      )}
      {waitingWorkerCdc > 0 && (
        <div style={{ marginTop: 7, fontSize: 12, color: t.amber.fg, lineHeight: 1.4 }}>
          {waitingWorkerCdc} CDC-строк готовы к apply и ждут worker. Если счетчик не уменьшается, проверьте процесс worker.py и подключение к state DB/Kafka.
        </div>
      )}
      {pendingDraftCdc > 0 && (
        <div style={{ marginTop: 7, fontSize: 12, color: t.amber.fg, lineHeight: 1.4 }}>
          Есть CDC-строки, которые еще не переведены в NEW. Обычно это значит, что Debezium не синхронизировался при добавлении; нажмите "Синхронизировать" или "Запустить" после проверки ошибки runtime CDC-пачки.
        </div>
      )}
      {hasRawConfig && (
        <details style={{ marginTop: 6 }}>
          <summary style={{ cursor: "pointer", color: t.text.muted, fontSize: 11 }}>
            Диагностика Debezium
          </summary>
          {group.table_include_list && (
            <MonoLine>table.include.list: {group.table_include_list}</MonoLine>
          )}
          {(group.active_topic_prefix || group.topic_prefix) && (
            <MonoLine>topic.prefix: {group.active_topic_prefix || group.topic_prefix}</MonoLine>
          )}
          {group.message_key_columns && (
            <MonoLine>message.key.columns: {group.message_key_columns}</MonoLine>
          )}
        </details>
      )}
    </div>
  );
}

function CdcConnectorDetails({
  group,
  packItems,
  packSourceSchema,
  busyKey,
  onRemoveExtra,
}: {
  group: MigrationPlanCdcGroup;
  packItems: MigrationPlanItem[];
  packSourceSchema: string;
  busyKey: string;
  onRemoveExtra: (group: MigrationPlanCdcGroup, table: MigrationPlanCdcTable) => void;
}) {
  const packItemsByTable = new Map<string, MigrationPlanItem[]>();
  for (const item of packItems) {
    const key = packItemTableKey(item, packSourceSchema);
    if (!packItemsByTable.has(key)) packItemsByTable.set(key, []);
    packItemsByTable.get(key)!.push(item);
  }
  const rows = group.tables || [];
  if (rows.length === 0) return null;
  return (
    <div style={{
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.md,
      overflow: "hidden",
      background: t.bg.s2,
    }}>
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        padding: "7px 10px",
        borderBottom: `1px solid ${t.border.subtle}`,
      }}>
        <span style={{ fontSize: 12, fontWeight: 700, color: t.text.primary }}>Фактический состав CDC-пачки</span>
        <span style={{ fontSize: 11, color: t.text.muted }}>table.include.list: {rows.length} таблиц</span>
      </div>
      {rows.map(tbl => {
        const tablePackItems = packItemsByTable.get(cdcTableKey(tbl)) || [];
        const inPack = tablePackItems.length > 0;
        const hasActivePackItem = tablePackItems.some(isActiveCdcPackItem);
        const canRemove = !hasActivePackItem;
        return (
          <div key={tbl.id} style={{
            display: "grid",
            gridTemplateColumns: "minmax(170px, 1fr) 100px minmax(150px, 1fr) 92px",
            gap: 10,
            alignItems: "center",
            padding: "7px 10px",
            borderTop: `1px solid ${t.bg.s1}`,
            fontSize: 12,
          }}>
            <div style={{ fontFamily: t.font.mono, color: t.text.primary, overflow: "hidden", textOverflow: "ellipsis" }}>
              {tableLabel(tbl)}
            </div>
            <Badge tone={inPack ? "ok" : "idle"}>{inPack ? "в пачке" : "только runtime"}</Badge>
            <div style={{ fontFamily: t.font.mono, color: t.text.muted, overflow: "hidden", textOverflow: "ellipsis" }}>
              {tbl.topic_name || "-"}
            </div>
            <div style={{ textAlign: "right" }}>
              {canRemove && (
                <button
                  onClick={() => onRemoveExtra(group, tbl)}
                  disabled={busyKey === tableLabel(tbl)}
                  style={{
                    ...secondaryActionStyle(false),
                    padding: "3px 8px",
                    fontSize: 11,
                    opacity: busyKey === tableLabel(tbl) ? 0.55 : 1,
                  }}
                >
                  {busyKey === tableLabel(tbl) ? "..." : "Убрать"}
                </button>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function PackCard({ title, items }: { title: string; items: MigrationPlanItem[] }) {
  const done = items.filter(isDoneItem).length;
  const running = items.filter(isActiveWorkItem).length;
  const failed = items.filter(isFailedItem).length;
  const queued = items.filter(isQueuedItem).length;
  return (
    <div style={{
      padding: "9px 10px",
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.md,
      background: t.bg.s2,
      minWidth: 0,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: t.text.primary }}>{title}</div>
        <Badge tone={failed ? "bad" : running ? "run" : done && done === items.length ? "ok" : "idle"}>
          {items.length ? `${done}/${items.length}` : "пусто"}
        </Badge>
      </div>
      <div style={{ marginTop: 7, display: "flex", gap: 12, flexWrap: "wrap", fontSize: 12, color: t.text.muted }}>
        <span>Таблиц: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{items.length}</strong></span>
        <span>В очереди: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{queued}</strong></span>
        <span>В работе: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{running}</strong></span>
        <span>Ошибки: <strong style={{ color: t.text.primary, fontFamily: t.font.mono }}>{failed}</strong></span>
      </div>
    </div>
  );
}

function tableLabel(tbl: { source_schema: string; source_table: string }) {
  return `${tbl.source_schema}.${tbl.source_table}`;
}

function cdcTableKey(tbl: { source_schema: string; source_table: string }) {
  return `${tbl.source_schema}.${tbl.source_table}`.toUpperCase();
}

function packItemTableKey(item: MigrationPlanItem, sourceSchema: string) {
  return `${sourceSchema}.${item.table_name}`.toUpperCase();
}

function MonoLine({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      marginTop: 6,
      fontFamily: t.font.mono,
      fontSize: 11,
      color: t.text.muted,
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
    }}>
      {children}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div style={{
      padding: "8px 10px",
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.md,
      background: t.bg.s2,
    }}>
      <div style={{ fontSize: 10.5, color: t.text.muted, marginBottom: 3 }}>{label}</div>
      <div style={{ fontSize: 16, fontWeight: 700, fontFamily: t.font.mono, color: t.text.primary }}>
        {value}
      </div>
    </div>
  );
}

function isNewPhase(item: MigrationPlanItem) {
  return String(item.phase || "").toUpperCase() === "NEW";
}

const packQueueRowGridTemplate = "minmax(90px, 110px) minmax(160px, 1fr) minmax(145px, auto) minmax(150px, 180px) minmax(190px, auto)";

function PackQueueRow({
  kind,
  item,
  cdcGroupStatus,
  onReload,
  sseEvents,
}: {
  kind: "bulk" | "cdc";
  item: MigrationPlanItem;
  cdcGroupStatus?: string;
  onReload: () => void;
  sseEvents: SSEEvent[];
}) {
  const rowsLoaded = item.rows_loaded || 0;
  const totalRows = item.total_rows || 0;
  const progress = totalRows ? rowsLoaded / totalRows * 100 : undefined;
  const status = itemStatusLabel(item, cdcGroupStatus);
  const progressText = itemProgressText(item, progress, cdcGroupStatus);
  const visual = itemVisualState(item);
  const showTriggerJob = shouldShowTriggerJob(item);
  const orderText = kind === "cdc"
    ? `CDC-шаг ${item.batch_order || 1}`
    : `Шаг ${item.batch_order || 1}`;
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: packQueueRowGridTemplate,
      gap: 10,
      alignItems: "center",
      padding: "7px 10px",
      borderTop: `1px solid ${t.bg.s1}`,
      fontSize: 12,
    }}>
      <div style={{ fontFamily: t.font.mono, color: t.text.muted, whiteSpace: "nowrap" }}>
        {orderText}
      </div>
      <div style={{ minWidth: 0 }}>
        <div style={{ fontFamily: t.font.mono, color: t.text.primary, overflow: "hidden", textOverflow: "ellipsis" }}>
          {item.table_name}
        </div>
        {item.error_text && (
          <div style={{ color: t.red.fg, fontSize: 11, marginTop: 2, overflow: "hidden", textOverflow: "ellipsis" }}>
            {item.error_text}
          </div>
        )}
      </div>
      <Badge tone={visual === "done" ? "ok" : visual === "running" ? "run" : visual === "failed" ? "bad" : "idle"}>
        {status}
      </Badge>
      <div style={{
        fontFamily: t.font.mono,
        color: t.text.muted,
        textAlign: "right",
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}>
        {progressText}
      </div>
      <div style={{ display: "flex", justifyContent: "flex-end", alignItems: "center", gap: 6, minWidth: 0, flexWrap: "wrap" }}>
        {item.migration_id ? (
          <FullRestartInlineAction migrationId={item.migration_id} tableName={item.table_name} onDone={onReload} />
        ) : null}
        {showTriggerJob ? (
          <TriggerJobInlineAction migrationId={item.migration_id!} onDone={onReload} sseEvents={sseEvents} />
        ) : !item.migration_id ? (
          <span style={{ color: t.text.disabled, fontSize: 11 }}>-</span>
        ) : null}
      </div>
    </div>
  );
}

function FullRestartInlineAction({
  migrationId,
  tableName,
  onDone,
}: {
  migrationId: string;
  tableName: string;
  onDone: () => void;
}) {
  const [busy, setBusy] = React.useState<"" | "draft" | "start">("");
  const [err, setErr] = React.useState("");

  async function run(start: boolean) {
    const title = start ? "полностью перезапустить" : "сбросить в draft";
    const next = start
      ? "Состояние таблицы будет очищено, после чего она сразу вернется в очередь запуска."
      : "Состояние таблицы будет очищено, после чего ее можно будет запустить вручную.";
    if (!window.confirm(`${title} ${tableName}?\n\n${next}`)) return;

    setBusy(start ? "start" : "draft");
    setErr("");
    try {
      const res = await fetch(`/api/migrations/${migrationId}/full-restart`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ start }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      onDone();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  }

  const disabled = !!busy;

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 3, minWidth: 0 }}>
      <div style={{ display: "flex", gap: 4, justifyContent: "flex-end", flexWrap: "wrap" }}>
        <button
          type="button"
          onClick={() => run(false)}
          disabled={disabled}
          title="Очистить состояние таблицы и вернуть ее в DRAFT"
          style={{
            ...secondaryActionStyle(disabled),
            padding: "3px 7px",
            fontSize: 11,
            whiteSpace: "nowrap",
          }}
        >
          {busy === "draft" ? "..." : "Сброс"}
        </button>
        <button
          type="button"
          onClick={() => run(true)}
          disabled={disabled}
          title="Очистить состояние таблицы и сразу запустить заново"
          style={{
            ...primaryActionStyle(disabled, true),
            padding: "3px 7px",
            fontSize: 11,
            whiteSpace: "nowrap",
          }}
        >
          {busy === "start" ? "..." : "Рестарт"}
        </button>
      </div>
      {err ? (
        <span style={{
          color: t.red.fg,
          fontSize: 10.5,
          maxWidth: 190,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}>
          {err}
        </span>
      ) : null}
    </div>
  );
}

function TriggerJobInlineAction({
  migrationId,
  onDone,
  sseEvents,
}: {
  migrationId: string;
  onDone: () => void;
  sseEvents: SSEEvent[];
}) {
  const [jobs, setJobs] = React.useState<TargetTriggerJob[]>([]);
  const [busy, setBusy] = React.useState("");
  const [err, setErr] = React.useState("");
  const latest = jobs[0];

  const loadJobs = React.useCallback(async () => {
    const res = await fetch(`/api/migrations/${migrationId}/trigger-jobs`);
    if (res.ok) {
      setJobs(await res.json());
    }
  }, [migrationId]);

  React.useEffect(() => {
    let alive = true;
    async function tick() {
      const res = await fetch(`/api/migrations/${migrationId}/trigger-jobs`);
      if (alive && res.ok) setJobs(await res.json());
    }
    tick();
    const id = setInterval(tick, 5000);
    return () => { alive = false; clearInterval(id); };
  }, [migrationId]);

  React.useEffect(() => {
    const event = sseEvents[0];
    if (!event || event.type !== "target_trigger_job" || event.migration_id !== migrationId) return;
    loadJobs();
  }, [sseEvents, migrationId, loadJobs]);

  async function createJob() {
    setBusy("create");
    setErr("");
    try {
      const res = await fetch(`/api/migrations/${migrationId}/trigger-jobs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ requested_by: "ui" }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      await loadJobs();
      onDone();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  }

  async function runJob(jobId: string) {
    setBusy("run");
    setErr("");
    try {
      const res = await fetch(`/api/migrations/${migrationId}/trigger-jobs/${jobId}/run`, { method: "POST" });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      await loadJobs();
      onDone();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy("");
    }
  }

  if (latest?.state === "DONE") {
    return (
      <span style={{ fontSize: 11, color: t.green.fg, fontWeight: 700, whiteSpace: "nowrap" }}>
        триггеры включены: {latest.enabled_count}
      </span>
    );
  }

  const failed = latest?.state === "FAILED";
  const running = latest?.state === "RUNNING";
  const pending = latest?.state === "PENDING";
  const label = busy
    ? "..."
    : running
      ? "job выполняется"
      : pending
        ? "Запустить триггеры"
        : "Создать job";

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 3, minWidth: 0 }}>
      <button
        onClick={pending ? () => runJob(latest.job_id) : createJob}
        disabled={!!busy || running}
        style={{
          ...secondaryActionStyle(false),
          padding: "3px 8px",
          fontSize: 11,
          opacity: busy || running ? 0.55 : 1,
          whiteSpace: "nowrap",
        }}
      >
        {label}
      </button>
      {(failed && latest?.error_text) || err ? (
        <span style={{
          color: t.red.fg,
          fontSize: 10.5,
          maxWidth: 180,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}>
          {err || latest?.error_text}
        </span>
      ) : null}
    </div>
  );
}

function shouldShowTriggerJob(item: MigrationPlanItem) {
  if (!item.migration_id) return false;
  const phase = String(item.phase || "").toUpperCase();
  if (isCdcPackItem(item)) {
    return ["CDC_CAUGHT_UP", "STEADY_STATE"].includes(phase);
  }
  return phase === "COMPLETED";
}

function itemProgressText(item: MigrationPlanItem, progress: number | undefined, cdcGroupStatus?: string) {
  const phase = String(item.phase || "").toUpperCase();
  const groupStatus = String(cdcGroupStatus || "").toUpperCase();
  if (isCdcPackItem(item) && phase === "NEW") {
    if (groupStatus && groupStatus !== "RUNNING") return `ждет ${cdcRuntimeStatusLabel(groupStatus)}`;
    if (item.queue_position != null) return `очередь #${item.queue_position}`;
    return "стартует";
  }
  if (phase === "NEW" && item.queue_position != null) {
    return `очередь #${item.queue_position}`;
  }
  if (isCdcPackItem(item) && ["CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP", "STEADY_STATE"].includes(phase)) {
    if ((phase === "CDC_APPLY_STARTING" || phase === "CDC_APPLYING") && !item.cdc_worker_heartbeat) {
      return "ждет worker";
    }
    const rows = item.cdc_rows_applied ?? null;
    const lag = item.cdc_total_lag ?? null;
    if (rows !== null || lag !== null) {
      const parts = [];
      if (rows !== null) parts.push(`cdc ${rows}`);
      if (lag !== null) parts.push(`lag ${lag}`);
      return parts.join(" · ");
    }
    return "cdc active";
  }
  return progress === undefined ? "rows n/a" : `${progress.toFixed(0)}%`;
}

function itemStatusLabel(item: MigrationPlanItem, cdcGroupStatus?: string) {
  const phase = String(item.phase || "").toUpperCase();
  const status = String(item.status || "").toUpperCase();
  const groupStatus = String(cdcGroupStatus || "").toUpperCase();
  if (isCdcPackItem(item) && status === "RUNNING" && phase === "NEW") {
    if (groupStatus && groupStatus !== "RUNNING") return `ЖДЕТ ${cdcRuntimeStatusLabel(groupStatus).toUpperCase()}`;
    if (item.queue_position != null) return "В ОЧЕРЕДИ";
    return "СТАРТУЕТ";
  }
  if (
    isCdcPackItem(item)
    && (phase === "CDC_APPLY_STARTING" || phase === "CDC_APPLYING")
    && !item.cdc_worker_heartbeat
  ) {
    return "ЖДЕТ WORKER";
  }
  return item.phase || item.status;
}

function itemVisualState(item: MigrationPlanItem): "done" | "failed" | "queued" | "running" | "idle" {
  const phase = String(item.phase || "").toUpperCase();
  const status = String(item.status || "").toUpperCase();
  if (status === "DONE" || phase === "COMPLETED" || phase === "STEADY_STATE") return "done";
  if (BAD.has(status) || phase === "FAILED" || phase === "CANCELLED") return "failed";
  if (status === "PENDING" || phase === "DRAFT" || phase === "NEW") return "queued";
  if (status === "RUNNING") return "running";
  return "idle";
}

function isDoneItem(item: MigrationPlanItem) {
  return itemVisualState(item) === "done";
}

function isFailedItem(item: MigrationPlanItem) {
  return itemVisualState(item) === "failed";
}

function isQueuedItem(item: MigrationPlanItem) {
  return itemVisualState(item) === "queued";
}

function isActiveWorkItem(item: MigrationPlanItem) {
  const phase = String(item.phase || "").toUpperCase();
  return ACTIVE_WORK_PHASES.has(phase);
}

function isRunningItem(item: MigrationPlanItem) {
  return itemVisualState(item) === "running";
}

function isActiveCdcPackItem(item: MigrationPlanItem) {
  const phase = String(item.phase || "").toUpperCase();
  const status = String(item.status || "").toUpperCase();
  if (phase === "FAILED" || phase === "CANCELLED" || phase === "COMPLETED") return false;
  if (status === "FAILED" || status === "CANCELLED") return false;
  return true;
}

function Shell({ children }: { children: React.ReactNode }) {
  return (
    <section style={{
      background: t.bg.s1,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding: 14,
      marginBottom: 12,
      boxShadow: t.shadow.s1,
    }}>
      {children}
    </section>
  );
}

function Title({ children }: { children: React.ReactNode }) {
  return <div style={{ fontSize: 14, fontWeight: 700, color: t.text.primary }}>{children}</div>;
}

function Muted({ children }: { children: React.ReactNode }) {
  return <div style={{ fontSize: 12, color: t.text.muted, marginTop: 4 }}>{children}</div>;
}

function Badge({ children, tone }: { children: React.ReactNode; tone: "ok" | "run" | "bad" | "idle" }) {
  const color = tone === "ok" ? t.green : tone === "run" ? t.blue : tone === "bad" ? t.red : null;
  return (
    <span style={{
      display: "inline-flex", justifyContent: "center",
      minWidth: 82,
      padding: "3px 8px",
      borderRadius: t.radius.sm,
      background: color ? color.bg : t.bg.s3,
      border: `1px solid ${color ? color.dim : t.border.subtle}`,
      color: color ? color.fg : t.text.muted,
      fontSize: 11,
      fontWeight: 700,
      fontFamily: t.font.mono,
      whiteSpace: "nowrap",
    }}>
      {children}
    </span>
  );
}
