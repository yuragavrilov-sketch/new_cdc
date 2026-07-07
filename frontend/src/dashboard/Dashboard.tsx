import React, { useCallback, useEffect, useMemo, useState } from "react";
import { ObjectFilters, type SortKey, type StatusFilter, type KeyFilter, type SuppFilter, type ColumnDiffFilter } from "./ObjectFilters";
import { ObjectTable } from "./ObjectTable";
import { ObjectDrawer } from "./ObjectDrawer";
import { AddToPackModal } from "./AddToPackModal";
import type { BulkTable } from "./TablePackModal";
import { PackWorkbench } from "./PackWorkbench";
import { SourceTablesBrowser } from "./SourceTablesBrowser";
import { LoadSnapshotBanner } from "./LoadSnapshotBanner";
import type { SyncGroup, SyncSelection } from "./SyncDdlDialog";
import { PACK_DEFINITIONS, ddlJobPackCounts, isCdcPackItem, packAddModeKind, packItemCounts, type PackAddMode, type PackModel, type TablePackAddMode } from "./packModel";
import { cdcRuntimeStatusLabel } from "./displayLabels";
import { secondaryActionStyle } from "./buttonStyles";
import { t } from "../theme";
import { useApi } from "../hooks/useApi";
import type { SSEEvent } from "../hooks/useSSE";
import { OBJECT_GROUPS, type ObjectGroupKey, type SchemaObject, type ObjectType, type MigrationEvent } from "./types";
import { hasColumnDiff } from "./tableDdl";
import {
  type SchemaMigrationListItem,
  type MigrationPlanDetail,
  type MigrationPlanCdcGroup,
  type AddPlanItemsResp,
  createSchemaMigration,
  startMigrationPlan,
  addDdlPackItems,
  startDdlPack,
  listDdlJobs,
  syncTargetColumns,
  loadCatalogSnapshot,
} from "./api";
import type { DdlApplyAction, DdlJob } from "./api";

const CDC_STARTED_PHASES = new Set([
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
  "CDC_APPLY_STARTING",
  "CDC_APPLYING",
  "CDC_CATCHING_UP",
  "CDC_CAUGHT_UP",
]);

const REPLACEABLE_DDL_TYPES = new Set([
  "VIEW", "PROCEDURE", "FUNCTION", "PACKAGE", "TRIGGER", "TYPE", "SYNONYM",
]);

const PACKABLE_DDL_TYPES = new Set([
  "INDEX", "MVIEW", "SEQUENCE", "VIEW", "PACKAGE", "PROCEDURE", "FUNCTION",
  "TRIGGER", "TYPE", "SYNONYM", "DBLINK", "JOB",
]);

function toOracleObjectType(type: string): string {
  if (type === "MVIEW") return "MATERIALIZED VIEW";
  if (type === "DBLINK") return "DATABASE LINK";
  return type;
}

function ddlObjectKey(type: string, name: string): string {
  return `${toOracleObjectType(type)}|${name.toUpperCase()}`;
}

function ddlActionForObject(o: SchemaObject): DdlApplyAction | null {
  if (!o.id.startsWith("ddl-")) return null;
  const srcInvalid = (o.srcStatus || "").toUpperCase() === "INVALID";
  const tgtInvalid = (o.tgtStatus || "").toUpperCase() === "INVALID";
  if (srcInvalid) return null;
  if (!o.tgtStatus) return "create_missing";
  if (tgtInvalid) return "recreate";
  if (o.status === "warn" || (o.note || "").toLowerCase().includes("ddl")) {
    return REPLACEABLE_DDL_TYPES.has(o.type) ? "sync_diff" : "recreate";
  }
  return null;
}

function ddlPackActionForObject(o: SchemaObject): DdlApplyAction | null {
  const automatic = ddlActionForObject(o);
  if (automatic) return automatic;
  if (!o.id.startsWith("ddl-")) return null;
  if (!PACKABLE_DDL_TYPES.has(o.type)) return null;
  if ((o.srcStatus || "").toUpperCase() === "INVALID") return null;
  if (!o.tgtStatus) return "create_missing";
  return REPLACEABLE_DDL_TYPES.has(o.type) ? "sync_diff" : "recreate";
}

function ddlSyncGroupsForObjects(objects: SchemaObject[]): SyncGroup[] {
  const buckets: Record<DdlApplyAction, SchemaObject[]> = {
    create_missing: [],
    sync_diff: [],
    recreate: [],
  };
  for (const obj of objects) {
    const action = ddlPackActionForObject(obj);
    if (action) buckets[action].push(obj);
  }
  const groups: SyncGroup[] = [
    {
      action: "create_missing",
      title: "Создать отсутствующие в target",
      description: "CREATE для объектов, которых нет в target.",
      items: buckets.create_missing,
      destructive: false,
    },
    {
      action: "sync_diff",
      title: "CREATE OR REPLACE",
      description: "Безопасная синхронизация replaceable DDL.",
      items: buckets.sync_diff,
      destructive: false,
    },
    {
      action: "recreate",
      title: "DROP + CREATE",
      description: "Для DDL, который нельзя заменить через CREATE OR REPLACE.",
      items: buckets.recreate,
      destructive: true,
    },
  ];
  return groups.filter(group => group.items.length > 0);
}

function cdcItemStateNote(response: AddPlanItemsResp, fallbackCount: number, connectorStatus = "") {
  const states = response.item_states || [];
  if (!states.length) return "";
  const normalizedConnectorStatus = connectorStatus.toUpperCase();
  const ready = states.filter(item =>
    String(item.status || "").toUpperCase() === "RUNNING"
    && String(item.phase || "").toUpperCase() === "NEW"
    && item.queue_position == null
    && (!normalizedConnectorStatus || normalizedConnectorStatus === "RUNNING")
  ).length;
  const waitingConnector = states.filter(item =>
    String(item.status || "").toUpperCase() === "RUNNING"
    && String(item.phase || "").toUpperCase() === "NEW"
    && item.queue_position == null
    && !!normalizedConnectorStatus
    && normalizedConnectorStatus !== "RUNNING"
  ).length;
  const queued = states.filter(item =>
    String(item.status || "").toUpperCase() === "RUNNING"
    && String(item.phase || "").toUpperCase() === "NEW"
    && item.queue_position != null
  ).length;
  const waitingWorker = states.filter(item => {
    const phase = String(item.phase || "").toUpperCase();
    return (
      (phase === "CDC_APPLY_STARTING" || phase === "CDC_APPLYING")
      && !item.cdc_worker_heartbeat
    );
  }).length;
  const active = states.filter(item => {
    const phase = String(item.phase || "").toUpperCase();
    if (
      (phase === "CDC_APPLY_STARTING" || phase === "CDC_APPLYING")
      && !item.cdc_worker_heartbeat
    ) {
      return false;
    }
    return CDC_STARTED_PHASES.has(phase);
  }).length;
  const pending = states.filter(item =>
    String(item.status || "").toUpperCase() === "PENDING"
    || String(item.phase || "").toUpperCase() === "DRAFT"
  ).length;
  const failed = states.filter(item =>
    String(item.status || "").toUpperCase() === "FAILED"
    || String(item.phase || "").toUpperCase() === "FAILED"
  ).length;
  const parts = [];
  if (active) parts.push(`в работе: ${active}`);
  if (waitingConnector) parts.push(`ждут CDC-пачку: ${waitingConnector}`);
  if (ready) parts.push(`стартуют: ${ready}`);
  if (queued) parts.push(`в очереди: ${queued}`);
  if (waitingWorker) parts.push(`ждут worker: ${waitingWorker}`);
  if (pending) parts.push(`ожидают: ${pending}`);
  if (failed) parts.push(`ошибки: ${failed}`);
  return parts.length
    ? ` · CDC: ${fallbackCount} таблиц (${parts.join(", ")})`
    : ` · CDC: ${fallbackCount} таблиц`;
}

function messageFromError(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

interface Props {
  selectedId:        string | null;
  schema:            SchemaMigrationListItem | null;
  packQueueId:            number | null;
  onCreated:         (newId: string) => void;
  onPackQueueChanged:     (packQueueId: number) => void;
  onOpenPacks:        () => void;
  /** When `true` and `schema` is null, render the empty state with CTA. */
  showEmptyState:    boolean;
  sseEvents:         SSEEvent[];
  objectGroup:       ObjectGroupKey;
}

interface SourceBrowserPackModal {
  schemaMigrationId: string;
  mode:              TablePackAddMode;
  tables:            BulkTable[];
}

export function Dashboard({
  selectedId,
  schema,
  packQueueId,
  onCreated,
  onPackQueueChanged,
  onOpenPacks,
  showEmptyState,
  sseEvents,
  objectGroup,
}: Props) {
  const [typeFilter,   setTypeFilter]   = useState<ObjectType | "all">("all");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [keyFilter,    setKeyFilter]    = useState<KeyFilter>("all");
  const [suppFilter,   setSuppFilter]   = useState<SuppFilter>("all");
  const [columnDiffFilter, setColumnDiffFilter] = useState<ColumnDiffFilter>("all");
  const [search,       setSearch]       = useState("");
  const [sort,         setSort]         = useState<SortKey>("priority");
  const [openObject,   setOpenObject]   = useState<SchemaObject | null>(null);
  const [page,         setPage]         = useState(1);
  const [pageSize,     setPageSize]     = useState(25);
  const [selectedIds,         setSelectedIds]         = useState<Set<string>>(() => new Set());
  const [packMode,            setPackMode]            = useState<PackAddMode | null>(null);
  const [sourcePackModal,     setSourcePackModal]     = useState<SourceBrowserPackModal | null>(null);
  const [ddlPackGroups,       setDdlPackGroups]       = useState<SyncGroup[]>([]);
  const [activePackQueueId,        setActivePackQueueId]        = useState<number | null>(packQueueId ?? schema?.planId ?? null);
  const [packQueueBusy,            setPackQueueBusy]            = useState(false);
  const [packQueueErr,             setPackQueueErr]             = useState("");
  const [toast,               setToast]               = useState<string>("");
  const [ddlSyncFeedback,     setDdlSyncFeedback]     = useState<string>("");
  const [ddlPackBusy,         setDdlPackBusy]         = useState(false);
  const [ddlColumnSyncBusy,   setDdlColumnSyncBusy]   = useState(false);
  const [ddlColumnSyncFeedback, setDdlColumnSyncFeedback] = useState("");

  // Fetch objects and events for this schema migration (auto-poll 5s)
  const objectsApi = useApi<SchemaObject[]>(
    selectedId ? `/api/schema-migrations/${selectedId}/objects` : null,
    { intervalMs: 5000 },
  );
  const eventsApi = useApi<MigrationEvent[]>(
    selectedId ? `/api/schema-migrations/${selectedId}/events?limit=200` : null,
    { intervalMs: 5000 },
  );
  const packQueueApi = useApi<MigrationPlanDetail>(
    activePackQueueId ? `/api/planner/plans/${activePackQueueId}` : null,
    { intervalMs: 5000 },
  );
  const cdcGroupApi = useApi<MigrationPlanCdcGroup>(
    selectedId ? `/api/schema-migrations/${selectedId}/cdc-group` : null,
    { intervalMs: 5000 },
  );
  const ddlJobsApi = useApi<DdlJob[]>(
    selectedId ? `/api/schema-migrations/${selectedId}/ddl-jobs?limit=500` : null,
    { intervalMs: 5000 },
  );

  const objects = objectsApi.data || [];
  const events  = eventsApi.data  || [];
  const activeGroup = useMemo(
    () => OBJECT_GROUPS.find(group => group.key === objectGroup) || OBJECT_GROUPS[0],
    [objectGroup],
  );
  const groupObjects = useMemo(() => {
    const typeSet = new Set(activeGroup.types);
    return objects.filter(o => typeSet.has(o.type));
  }, [objects, activeGroup]);
  const isTablesGroup = activeGroup.key === "tables";
  const cdcGroup = packQueueApi.data?.cdc_group || cdcGroupApi.data || null;

  useEffect(() => {
    setActivePackQueueId(packQueueId ?? schema?.planId ?? null);
  }, [schema?.id, schema?.planId, packQueueId]);

  useEffect(() => {
    const event = sseEvents[0];
    if (!event) return;

    if (event.type === "schema_migration.plan_items_added" && event.id === selectedId) {
      setActivePackQueueId(event.plan_id);
      objectsApi.reload();
      eventsApi.reload();
      cdcGroupApi.reload();
      packQueueApi.reload();
      return;
    }

    if (event.type === "connector_group_status") {
      cdcGroupApi.reload();
      if (activePackQueueId) packQueueApi.reload();
      return;
    }

    if (event.type === "migration_phase" && activePackQueueId) {
      objectsApi.reload();
      eventsApi.reload();
      cdcGroupApi.reload();
      packQueueApi.reload();
      return;
    }

    if (event.type === "ddl_apply_job" && event.sm_id === selectedId) {
      objectsApi.reload();
      eventsApi.reload();
      ddlJobsApi.reload();
    }

    if ((event.type === "ddl_pack.changed" || event.type === "ddl_pack.started") && event.sm_id === selectedId) {
      eventsApi.reload();
      ddlJobsApi.reload();
    }
  }, [
    sseEvents,
    selectedId,
    activePackQueueId,
    objectsApi.reload,
    eventsApi.reload,
    cdcGroupApi.reload,
    packQueueApi.reload,
    ddlJobsApi.reload,
  ]);

  // Filtered + sorted
  const filtered = useMemo(() => {
    let arr = groupObjects;
    if (!isTablesGroup && typeFilter !== "all") {
      arr = arr.filter(o => o.type === typeFilter);
    }
    if (statusFilter !== "all") {
      arr = arr.filter(o => {
        if (statusFilter === "issues") return o.err > 0 || o.warn > 0 || o.status === "error" || o.status === "warn";
        return o.status === statusFilter;
      });
    }
    // Фильтры PK/UK/NO KEY и SUPP/NO SUPP применяются только к таблицам;
    // DDL-объекты (INDEX, VIEW, PACKAGE...) при активном фильтре отсекаются
    // — иначе сегмент «NO KEY» показывал бы все view/package и т.п.
    if (isTablesGroup && keyFilter !== "all") {
      arr = arr.filter(o => {
        if (o.type !== "TABLE") return false;
        if (keyFilter === "pk")     return !!o.hasPk;
        if (keyFilter === "uk")     return !o.hasPk && !!o.hasUk;
        if (keyFilter === "no_key") return o.hasPk === false && o.hasUk === false;
        return true;
      });
    }
    if (isTablesGroup && suppFilter !== "all") {
      arr = arr.filter(o => {
        if (o.type !== "TABLE") return false;
        if (suppFilter === "supp")    return o.hasSuppLog === true;
        if (suppFilter === "no_supp") return o.hasSuppLog === false;
        return true;
      });
    }
    if (isTablesGroup && columnDiffFilter !== "all") {
      arr = arr.filter(hasColumnDiff);
    }
    if (search) {
      const q = search.toLowerCase();
      arr = arr.filter(o => o.name.toLowerCase().includes(q));
    }
    arr = [...arr].sort((a, b) => {
      if (sort === "priority") {
        const rank: Record<string, number> = {
          error: 0, warn: 1, running: 2, validating: 3, paused: 4, queued: 5, done: 6, skipped: 7,
        };
        const sa = rank[a.status] ?? 9;
        const sb = rank[b.status] ?? 9;
        if (sa !== sb) return sa - sb;
        return (b.sizeMb || 0) - (a.sizeMb || 0);
      }
      if (sort === "size")     return (b.sizeMb || 0) - (a.sizeMb || 0);
      if (sort === "progress") return b.progress - a.progress;
      if (sort === "name")     return a.name.localeCompare(b.name);
      return 0;
    });
    return arr;
  }, [groupObjects, isTablesGroup, typeFilter, statusFilter, keyFilter, suppFilter, columnDiffFilter, search, sort]);
  const ddlActiveJobKeys = useMemo(() => {
    const activeStates = new Set(["DRAFT", "PENDING", "CLAIMED", "RUNNING"]);
    const s = new Set<string>();
    for (const job of ddlJobsApi.data || []) {
      if (activeStates.has(job.state)) s.add(ddlObjectKey(job.object_type, job.object_name));
    }
    return s;
  }, [ddlJobsApi.data]);
  const ddlAvailableObjects = useMemo(
    () => isTablesGroup
      ? []
      : filtered.filter(o => ddlPackActionForObject(o) && !ddlActiveJobKeys.has(ddlObjectKey(o.type, o.name))),
    [filtered, isTablesGroup, ddlActiveJobKeys],
  );
  const ddlSyncGroups = useMemo(
    () => isTablesGroup ? [] : ddlSyncGroupsForObjects(ddlAvailableObjects),
    [ddlAvailableObjects, isTablesGroup],
  );
  const ddlSyncActionable = ddlSyncGroups.reduce((sum, group) => sum + group.items.length, 0);
  const ddlDraftJobs = useMemo(
    () => (ddlJobsApi.data || []).filter(job => job.state === "DRAFT"),
    [ddlJobsApi.data],
  );
  const ddlSelectableIds = useMemo(() => {
    const s = new Set<string>();
    if (isTablesGroup) return s;
    for (const o of ddlAvailableObjects) s.add(o.id);
    return s;
  }, [ddlAvailableObjects, isTablesGroup]);

  // Reset drawer when switching schemas
  useEffect(() => { setOpenObject(null); }, [selectedId]);
  // Reset to page 1 when filters change so the user always sees the matches
  useEffect(() => { setPage(1); }, [typeFilter, statusFilter, keyFilter, suppFilter, columnDiffFilter, search, sort, pageSize, selectedId, objectGroup]);
  // Clear bulk-selection when switching schemas
  useEffect(() => { setSelectedIds(new Set()); }, [selectedId]);
  useEffect(() => {
    setTypeFilter("all");
    setKeyFilter("all");
    setSuppFilter("all");
    setColumnDiffFilter("all");
    setSelectedIds(new Set());
  }, [objectGroup]);

  // Table packs can be adjusted repeatedly, so every TABLE row is selectable.
  const selectableIds = useMemo(() => {
    const s = new Set<string>();
    for (const o of objects) {
      if (o.type === "TABLE") s.add(o.id);
    }
    return s;
  }, [objects]);
  const activeSelectableIds = isTablesGroup ? selectableIds : ddlSelectableIds;
  // Keep the selection column visible in every object group. Rows that cannot
  // be added to the current pack stay disabled instead of hiding the controls.
  const selectionVisible = true;

  // Keep selection in sync if objects list changes (drop stale ids)
  useEffect(() => {
    setSelectedIds(prev => {
      let changed = false;
      const next = new Set<string>();
      prev.forEach(id => {
        if (activeSelectableIds.has(id)) next.add(id);
        else changed = true;
      });
      return changed ? next : prev;
    });
  }, [activeSelectableIds]);

  const toggleSelect = useCallback((id: string) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const selectAllPage = useCallback((ids: string[], allSelected: boolean) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (allSelected) ids.forEach(id => next.delete(id));
      else             ids.forEach(id => next.add(id));
      return next;
    });
  }, []);

  // Resolve selected ids → bulk-modal payload
  const selectedTables = useMemo(() => {
    if (selectedIds.size === 0 || !schema) return [];
    const byId = new Map(objects.map(o => [o.id, o]));
    const src = schema.src_schema || "";
    const tgt = schema.tgt_schema || "";
    const out: { source_schema: string; source_table: string; target_schema: string; target_table: string }[] = [];
    selectedIds.forEach(id => {
      const o = byId.get(id);
      if (!o) return;
      out.push({
        source_schema: src,
        source_table:  o.name,
        target_schema: tgt,
        target_table:  o.name,
      });
    });
    return out;
  }, [selectedIds, objects, schema]);

  const selectedDdlObjects = useMemo(() => {
    if (selectedIds.size === 0 || isTablesGroup) return [];
    const byId = new Map(objects.map(o => [o.id, o]));
    const out: SchemaObject[] = [];
    selectedIds.forEach(id => {
      const o = byId.get(id);
      if (o && ddlPackActionForObject(o)) out.push(o);
    });
    return out;
  }, [selectedIds, objects, isTablesGroup]);

  const handleSyncSelectedTableColumns = useCallback(async () => {
    if (!selectedTables.length || !schema) return;
    const ok = window.confirm(
      `Синхронизировать DDL колонок для ${selectedTables.length} таблиц?\n\n`
      + "Будут добавлены отсутствующие колонки и удалены лишние колонки на target.\n"
      + "Несовпадение типов будет показано предупреждением и автоматически не меняется.",
    );
    if (!ok) return;

    setDdlColumnSyncBusy(true);
    setDdlColumnSyncFeedback(`DDL колонки: синхронизация ${selectedTables.length} таблиц...`);
    let synced = 0;
    let added = 0;
    let dropped = 0;
    let warnings = 0;
    let dropErrors = 0;
    const errors: string[] = [];

    try {
      for (const table of selectedTables) {
        try {
          const result = await syncTargetColumns({
            src_schema: table.source_schema,
            src_table:  table.source_table,
            tgt_schema: table.target_schema,
            tgt_table:  table.target_table,
          });
          synced += 1;
          added += result.added?.length || 0;
          dropped += result.dropped?.length || 0;
          warnings += result.warnings?.length || 0;
          dropErrors += result.drop_errors?.length || 0;
        } catch (e) {
          errors.push(`${table.source_table}: ${messageFromError(e)}`);
        }
      }

      if (schema.src_schema && schema.tgt_schema) {
        try {
          await loadCatalogSnapshot(schema.src_schema, schema.tgt_schema);
        } catch (e) {
          errors.push(`snapshot: ${messageFromError(e)}`);
        }
      }

      const parts = [
        `DDL колонки: ${synced}/${selectedTables.length}`,
        `+${added}`,
        `-${dropped}`,
      ];
      if (warnings) parts.push(`типов: ${warnings}`);
      if (dropErrors) parts.push(`ошибок DROP: ${dropErrors}`);
      if (errors.length) parts.push(`ошибок: ${errors.length}`);
      const feedback = parts.join(", ");
      setDdlColumnSyncFeedback(feedback);
      setToast(errors.length ? `${feedback}: ${errors.slice(0, 2).join("; ")}` : feedback);
      if (!errors.length) setSelectedIds(new Set());
      objectsApi.reload();
      eventsApi.reload();
      setTimeout(() => setToast(""), 5000);
    } finally {
      setDdlColumnSyncBusy(false);
    }
  }, [selectedTables, schema, objectsApi, eventsApi]);

  // Stable handlers (so React.memo on ObjectRow can skip re-renders)
  const handleOpen = useCallback((o: SchemaObject) => setOpenObject(o), []);
  const handleRowAction = useCallback(
    (o: SchemaObject, a: "pause" | "retry" | "more") => console.log("object action", a, o.name),
    [],
  );
  const handleAddObjectToTablePack = useCallback((o: SchemaObject, mode: TablePackAddMode) => {
    if (!selectedId || !schema || o.type !== "TABLE") return;
    setSourcePackModal({
      schemaMigrationId: selectedId,
      mode,
      tables: [{
        source_schema: schema.src_schema || "",
        source_table:  o.name,
        target_schema: schema.tgt_schema || "",
        target_table:  o.name,
      }],
    });
  }, [selectedId, schema]);

  const handleStartPackQueue = useCallback(async () => {
    if (!activePackQueueId) return;
    setPackQueueBusy(true);
    setPackQueueErr("");
    try {
      await startMigrationPlan(activePackQueueId);
      packQueueApi.reload();
      objectsApi.reload();
      eventsApi.reload();
    } catch (e) {
      setPackQueueErr(String(e instanceof Error ? e.message : e));
    } finally {
      setPackQueueBusy(false);
    }
  }, [activePackQueueId, packQueueApi, objectsApi, eventsApi]);

  const handleSnapshotLoaded = useCallback(() => {
    objectsApi.reload();
    eventsApi.reload();
  }, [objectsApi, eventsApi]);

  const sourceBrowserRows = useCallback((
    sourceSchema: string,
    targetSchema: string,
    tables: string[],
  ): BulkTable[] => tables.map(table => ({
    source_schema: sourceSchema,
    source_table:  table,
    target_schema: targetSchema,
    target_table:  table,
  })), []);

  const openSourceBrowserPack = useCallback((
    schemaMigrationId: string,
    mode: TablePackAddMode,
    sourceSchema: string,
    targetSchema: string,
    tables: string[],
  ) => {
    if (!tables.length) return;
    setSourcePackModal({
      schemaMigrationId,
      mode,
      tables: sourceBrowserRows(sourceSchema, targetSchema, tables),
    });
  }, [sourceBrowserRows]);

  const sourcePackModalNode = sourcePackModal ? (
    <AddToPackModal
      mode={sourcePackModal.mode}
      schemaMigrationId={sourcePackModal.schemaMigrationId}
      tables={sourcePackModal.tables}
      cdcGroup={cdcGroup}
      cdcGroupLoading={
        sourcePackModal.mode === "cdc"
        && !!selectedId
        && !cdcGroup
        && (cdcGroupApi.loading || (!!activePackQueueId && packQueueApi.loading && !packQueueApi.data))
      }
      cdcGroupError={
        sourcePackModal.mode === "cdc" && !!selectedId && !cdcGroup
          ? (cdcGroupApi.error || (!!activePackQueueId && !packQueueApi.data ? packQueueApi.error : null))
          : null
      }
      onClose={() => setSourcePackModal(null)}
      onReloadCdcGroup={() => {
        cdcGroupApi.reload();
        packQueueApi.reload();
      }}
      onDone={async (packQueueId, count, response) => {
        const target = sourcePackModal.mode === "cdc"
          ? PACK_DEFINITIONS.cdc.title
          : PACK_DEFINITIONS.bulk.title.toLowerCase();
        setSourcePackModal(null);
        setActivePackQueueId(packQueueId);
        if (sourcePackModal.mode === "cdc") {
          cdcGroupApi.setData(response.cdc_group || null);
        }
        onPackQueueChanged(packQueueId);
        packQueueApi.reload();
        objectsApi.reload();
        eventsApi.reload();
        cdcGroupApi.reload();
        setToast(`Добавлено в ${target}: ${count}`);
      }}
    />
  ) : null;

  const pollDdlJobsAndRefresh = useCallback(async (jobIds: string[]) => {
    if (!selectedId || !schema) return;
    const wanted = new Set(jobIds);
    const terminal = new Set(["DONE", "FAILED", "CANCELLED"]);
    const started = Date.now();
    const timeoutMs = 10 * 60_000;
    while (Date.now() - started < timeoutMs) {
      await new Promise(resolve => setTimeout(resolve, 2500));
      const jobs = await listDdlJobs(selectedId, 500).catch(() => null);
      if (!jobs) continue;
      let seen = 0;
      let done = 0;
      let failed = 0;
      let active = 0;
      for (const job of jobs) {
        if (!wanted.has(job.job_id)) continue;
        seen += 1;
        if (job.state === "DONE") done += 1;
        else if (job.state === "FAILED") failed += 1;
        else if (!terminal.has(job.state)) active += 1;
      }
      setDdlSyncFeedback(active
        ? `DDL-пачка: ${done}/${jobIds.length} готово${failed ? `, ошибок: ${failed}` : ""}`
        : `DDL-пачка завершена: ${done}/${jobIds.length}${failed ? `, ошибок: ${failed}` : ""}`);
      if (seen === jobIds.length && active === 0) {
        if (schema.src_schema && schema.tgt_schema) {
          try {
            await fetch("/api/catalog/load", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ src_schema: schema.src_schema, tgt_schema: schema.tgt_schema }),
            });
          } catch {}
        }
        objectsApi.reload();
        eventsApi.reload();
        ddlJobsApi.reload();
        return;
      }
    }
    setDdlSyncFeedback("DDL-пачка: timeout, проверьте задания вручную");
  }, [selectedId, schema, objectsApi, eventsApi, ddlJobsApi]);

  const openDdlPackModal = useCallback((items: SchemaObject[]) => {
    const groups = ddlSyncGroupsForObjects(items);
    if (!groups.length) return;
    setDdlPackGroups(groups);
    setPackMode("ddl");
  }, []);

  const handleAddDdlSelectionToPack = useCallback(async (selection: SyncSelection[]) => {
    if (!selectedId) return;
    if (!selection.length) return;
    setDdlPackBusy(true);
    setDdlSyncFeedback("добавляем в DDL-пачку...");
    let queued = 0;
    let skipped = 0;
    try {
      for (const item of selection) {
        const result = await addDdlPackItems(
          selectedId,
          item.action,
          item.items.map(o => ({ type: o.type, name: o.name })),
        );
        queued += result.queued;
        skipped += result.skipped.length;
      }
      setDdlSyncFeedback(`DDL-пачка: добавлено ${queued}${skipped ? `, пропущено: ${skipped}` : ""}`);
      setSelectedIds(new Set());
      ddlJobsApi.reload();
      eventsApi.reload();
    } catch (e) {
      setDdlSyncFeedback(`DDL-пачка: ошибка добавления: ${e instanceof Error ? e.message : String(e)}`);
      throw e;
    } finally {
      setDdlPackBusy(false);
    }
  }, [selectedId, ddlJobsApi, eventsApi]);

  const handleAddFilteredDdlToPack = useCallback(() => {
    openDdlPackModal(ddlSyncGroups.flatMap(group => group.items));
  }, [ddlSyncGroups, openDdlPackModal]);

  const handleStartDdlPack = useCallback(async () => {
    if (!selectedId) return;
    const ids = ddlDraftJobs.map(job => job.job_id);
    if (!ids.length) return;
    setDdlPackBusy(true);
    setDdlSyncFeedback("запускаем DDL-пачку...");
    try {
      const result = await startDdlPack(selectedId, ids);
      setDdlSyncFeedback(`DDL-пачка запущена: ${result.started}`);
      ddlJobsApi.reload();
      eventsApi.reload();
      if (result.job_ids.length > 0) {
        void pollDdlJobsAndRefresh(result.job_ids);
      }
    } catch (e) {
      setDdlSyncFeedback(`DDL-пачка: ошибка запуска: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setDdlPackBusy(false);
    }
  }, [selectedId, ddlDraftJobs, ddlJobsApi, eventsApi, pollDdlJobsAndRefresh]);

  const packModels = useMemo<PackModel[]>(() => {
    const packItems = packQueueApi.data?.items || [];
    const bulkItems = packItems.filter(item => !isCdcPackItem(item));
    const cdcItems = packItems.filter(isCdcPackItem);
    const bulkCounts = packItemCounts(bulkItems);
    const cdcCounts = packItemCounts(cdcItems);
    const ddlCounts = ddlJobPackCounts(ddlJobsApi.data || []);
    const selectedTableCount = isTablesGroup ? selectedTables.length : 0;
    const selectedDdlCount = !isTablesGroup ? selectedDdlObjects.length : 0;

    return [
      {
        key: PACK_DEFINITIONS.bulk.key,
        title: PACK_DEFINITIONS.bulk.title,
        subtitle: PACK_DEFINITIONS.bulk.subtitle,
        tone: PACK_DEFINITIONS.bulk.tone,
        ...bulkCounts,
        selected: selectedTableCount,
        feedback: packQueueErr || packQueueApi.error || "",
        actions: [
          {
            label: selectedTableCount ? `В пачку · ${selectedTableCount}` : "В пачку",
            onClick: () => setPackMode("historical"),
            disabled: !isTablesGroup || selectedTableCount === 0,
            disabledReason: !isTablesGroup ? "Обычная пачка принимает только таблицы" : "Выберите таблицы",
          },
          {
            label: packQueueBusy ? "Запуск..." : "Запустить",
            onClick: handleStartPackQueue,
            disabled: packQueueBusy || !activePackQueueId || (bulkCounts.draft + bulkCounts.queued === 0),
            primary: true,
            disabledReason: !activePackQueueId ? "Очередь пачек ещё не создана" : "Нет строк обычной пачки в черновике или очереди",
          },
          {
            label: "Детали",
            onClick: onOpenPacks,
            disabled: !activePackQueueId,
          },
        ],
      },
      {
        key: PACK_DEFINITIONS.cdc.key,
        title: PACK_DEFINITIONS.cdc.title,
        subtitle: cdcGroup?.status
          ? `Runtime CDC-пачки: ${cdcRuntimeStatusLabel(cdcGroup.status)}, таблиц ${cdcGroup.tables?.length || 0}`
          : PACK_DEFINITIONS.cdc.subtitle,
        tone: PACK_DEFINITIONS.cdc.tone,
        ...cdcCounts,
        selected: selectedTableCount,
        feedback: packQueueErr || packQueueApi.error || "",
        actions: [
          {
            label: selectedTableCount ? `В пачку · ${selectedTableCount}` : "В пачку",
            onClick: () => setPackMode("cdc"),
            disabled: !isTablesGroup || selectedTableCount === 0,
            disabledReason: !isTablesGroup ? "CDC-пачка принимает только таблицы" : "Выберите таблицы",
          },
          {
            label: packQueueBusy ? "Запуск..." : "Запустить",
            onClick: handleStartPackQueue,
            disabled: packQueueBusy || !activePackQueueId || (cdcCounts.draft + cdcCounts.queued === 0 && !cdcGroup),
            primary: true,
            disabledReason: !activePackQueueId && !cdcGroup ? "CDC-пачка ещё не создана" : "Нет CDC-строк для запуска",
          },
          {
            label: "Детали",
            onClick: onOpenPacks,
            disabled: !activePackQueueId && !cdcGroup,
          },
        ],
      },
      {
        key: PACK_DEFINITIONS.ddl.key,
        title: PACK_DEFINITIONS.ddl.title,
        subtitle: isTablesGroup
          ? "Колонки таблиц: добавить отсутствующие, удалить лишние"
          : `Текущий раздел: ${activeGroup.label}`,
        tone: PACK_DEFINITIONS.ddl.tone,
        ...ddlCounts,
        selected: isTablesGroup ? selectedTableCount : selectedDdlCount,
        actions: [
          isTablesGroup
            ? {
              label: ddlColumnSyncBusy
                ? "Синхр..."
                : selectedTableCount
                  ? `Синхр. DDL · ${selectedTableCount}`
                  : "Синхр. DDL",
              onClick: handleSyncSelectedTableColumns,
              disabled: ddlColumnSyncBusy || selectedTableCount === 0,
              primary: selectedTableCount > 0,
              hint: "Добавить отсутствующие и удалить лишние колонки на target; типы не меняются автоматически",
              disabledReason: "Выберите таблицы для синхронизации колонок",
            }
            : {
              label: selectedDdlCount
                ? `В пачку · ${selectedDdlCount}`
                : ddlSyncActionable
                  ? `В пачку · ${ddlSyncActionable}`
                  : "В пачку",
              onClick: selectedDdlCount
                ? () => openDdlPackModal(selectedDdlObjects)
                : handleAddFilteredDdlToPack,
              disabled: ddlPackBusy || (selectedDdlCount === 0 && ddlSyncActionable === 0),
              disabledReason: "Нет DDL-объектов для добавления",
            },
          {
            label: ddlPackBusy ? "Запуск..." : "Запустить",
            onClick: handleStartDdlPack,
            disabled: ddlPackBusy || ddlDraftJobs.length === 0,
            primary: true,
            disabledReason: "Нет DDL-заданий в черновике",
          },
          {
            label: "Детали",
            onClick: onOpenPacks,
          },
        ],
        feedback: ddlColumnSyncFeedback || ddlSyncFeedback,
      },
    ];
  }, [
    packQueueApi.data,
    ddlJobsApi.data,
    selectedTables.length,
    selectedDdlObjects,
    isTablesGroup,
    packQueueBusy,
    activePackQueueId,
    cdcGroup,
    activeGroup.label,
    ddlSyncActionable,
    ddlPackBusy,
    ddlColumnSyncBusy,
    ddlDraftJobs,
    ddlSyncFeedback,
    ddlColumnSyncFeedback,
    handleStartPackQueue,
    onOpenPacks,
    openDdlPackModal,
    handleAddFilteredDdlToPack,
    handleStartDdlPack,
    handleSyncSelectedTableColumns,
  ]);

  // Empty state
  if (!schema) {
    return (
      <>
        {showEmptyState && (
          <SourceTablesBrowser
            onCreatePack={async (sourceSchema, targetSchema, meta, selectedTables, mode) => {
              const id = await createSchemaMigration({
                name:           sourceSchema || "—",
                src_schema:     sourceSchema,
                tgt_schema:     targetSchema,
                source_host:    meta.sourceHost,
                source_version: meta.sourceVersion,
                target_host:    meta.targetHost,
                target_version: meta.targetVersion,
              });
              onCreated(id);
              openSourceBrowserPack(id, mode, sourceSchema, targetSchema, selectedTables);
            }}
          />
        )}
        {sourcePackModalNode}
      </>
    );
  }

  if (!objectsApi.loading && objects.length === 0) {
    return (
      <>
        <LoadSnapshotBanner
          srcSchema={schema.src_schema || ""}
          tgtSchema={schema.tgt_schema || ""}
          sseEvents={sseEvents}
          onLoaded={handleSnapshotLoaded}
        />
        <SourceTablesBrowser
          initialSourceSchema={schema.src_schema || ""}
          initialTargetSchema={schema.tgt_schema || ""}
          createEnabled={false}
          onCreatePack={async () => {}}
          onAddSelected={async (sourceSchema, targetSchema, selectedTables, mode) => {
            openSourceBrowserPack(schema.id, mode, sourceSchema, targetSchema, selectedTables);
          }}
        />
        {sourcePackModalNode}
      </>
    );
  }

  return (
    <>
      <LoadSnapshotBanner
        srcSchema={schema.src_schema || ""}
        tgtSchema={schema.tgt_schema || ""}
        sseEvents={sseEvents}
        onLoaded={handleSnapshotLoaded}
      />

      <ObjectFilters
        objects={groupObjects}
        filtered={filtered}
        typeFilter={typeFilter}     onTypeFilter={setTypeFilter}
        statusFilter={statusFilter} onStatusFilter={setStatusFilter}
        keyFilter={keyFilter}       onKeyFilter={setKeyFilter}
        suppFilter={suppFilter}     onSuppFilter={setSuppFilter}
        columnDiffFilter={columnDiffFilter} onColumnDiffFilter={setColumnDiffFilter}
        search={search}             onSearch={setSearch}
        sort={sort}                 onSort={setSort}
        tablesOnly={isTablesGroup}
      />

      <PackWorkbench packs={packModels} />

      <ObjectTable
        objects={filtered}
        onOpen={handleOpen}
        onAction={handleRowAction}
        page={page}
        pageSize={pageSize}
        onPageChange={setPage}
        onPageSizeChange={setPageSize}
        selectableIds={selectionVisible ? activeSelectableIds : undefined}
        selectedIds={selectionVisible ? selectedIds : undefined}
        onToggleSelect={selectionVisible ? toggleSelect : undefined}
        onSelectAllPage={selectionVisible ? selectAllPage : undefined}
      />

      {isTablesGroup && selectedIds.size > 0 && (
        <BulkSelectionBar
          count={selectedIds.size}
          ddlBusy={ddlColumnSyncBusy}
          onClear={() => setSelectedIds(new Set())}
          onCdcPack={() => setPackMode("cdc")}
          onBulkPack={() => setPackMode("historical")}
          onSyncDdl={handleSyncSelectedTableColumns}
        />
      )}

      {!isTablesGroup && selectedDdlObjects.length > 0 && (
        <DdlSelectionBar
          count={selectedDdlObjects.length}
          busy={ddlPackBusy}
          onClear={() => setSelectedIds(new Set())}
          onAdd={() => openDdlPackModal(selectedDdlObjects)}
        />
      )}

      {sourcePackModalNode}

      {packMode === "ddl" && selectedId && ddlPackGroups.length > 0 && (
        <AddToPackModal
          mode="ddl"
          schemaMigrationId={selectedId}
          ddlGroups={ddlPackGroups}
          onClose={() => {
            setPackMode(null);
            setDdlPackGroups([]);
          }}
          onDdlSubmit={handleAddDdlSelectionToPack}
        />
      )}

      {packMode !== null && packMode !== "ddl" && selectedId && isTablesGroup && selectedTables.length > 0 && (
        <AddToPackModal
          mode={packMode}
          schemaMigrationId={selectedId}
          tables={selectedTables}
          cdcGroup={cdcGroup}
          cdcGroupLoading={packMode === "cdc" && !cdcGroup && (cdcGroupApi.loading || (!!activePackQueueId && packQueueApi.loading && !packQueueApi.data))}
          cdcGroupError={packMode === "cdc" && !cdcGroup ? (cdcGroupApi.error || (!!activePackQueueId && !packQueueApi.data ? packQueueApi.error : null)) : null}
          onClose={() => {
            setPackMode(null);
            setDdlPackGroups([]);
          }}
          onReloadCdcGroup={() => {
            cdcGroupApi.reload();
            packQueueApi.reload();
          }}
          onDone={async (packQueueId, count, response) => {
            const targetKind = packAddModeKind(packMode);
            const target = targetKind === "cdc"
              ? PACK_DEFINITIONS.cdc.title
              : PACK_DEFINITIONS.bulk.title.toLowerCase();
            let autoStartOk = true;
            const connectorStartError = response.connector_start_error || "";
            if (connectorStartError) {
              autoStartOk = false;
            }
            let startNote = "";
            if (packMode === "cdc") {
              const connectorCount = response.cdc_group?.tables?.length;
              const connectorStatus = String(response.connector_start?.status || response.cdc_group?.status || "").trim();
              const normalizedConnectorStatus = connectorStatus.toUpperCase();
              const stateNote = cdcItemStateNote(response, count, connectorStatus);
              if (response.plan_start_error) {
                autoStartOk = false;
                startNote = " · автозапуск не выполнен";
                setPackQueueErr(response.plan_start_error);
              } else if (response.plan_starts?.length) {
                const startedCount = response.plan_starts.reduce((sum, item) => sum + item.started.length, 0);
                startNote = startedCount
                  ? normalizedConnectorStatus === "RUNNING"
                    ? ` · CDC: ${count} таблиц (передано в очередь: ${startedCount})`
                    : ` · CDC: ${count} таблиц (ждут CDC-пачку: ${startedCount})`
                  : " · запуск уже обработан";
              } else if (response.plan_start) {
                const startedCount = response.plan_start.started.length;
                startNote = startedCount
                  ? normalizedConnectorStatus === "RUNNING"
                    ? ` · CDC: ${count} таблиц (передано в очередь: ${startedCount})`
                    : ` · CDC: ${count} таблиц (ждут CDC-пачку: ${startedCount})`
                  : " · запуск уже обработан";
              } else if (stateNote) {
                startNote = stateNote;
              } else if (response.cdc_queue_kicked) {
                startNote = ` · CDC: ${count} таблиц (очередь проверена)`;
              } else if (response.connector_start_error) {
                startNote = normalizedConnectorStatus === "RUNNING"
                  ? " · CDC: добавлено в пачку, но очередь не стартовала из-за ошибки синхронизации Debezium"
                  : " · ожидает запуска CDC-пачки";
              } else {
                try {
                  const started = await startMigrationPlan(packQueueId);
                  const startedCount = started.started.length;
                  startNote = startedCount
                    ? ` · CDC: ${count} таблиц (передано в очередь: ${startedCount})`
                    : " · запуск уже обработан";
                } catch (e) {
                  const msg = e instanceof Error ? e.message : String(e);
                  if (/already running/i.test(msg)) {
                    startNote = " · в очереди за текущей миграцией";
                  } else {
                    autoStartOk = false;
                    startNote = " · автозапуск не выполнен";
                    setPackQueueErr(msg);
                  }
                }
              }
              if (connectorCount !== undefined) {
                startNote += ` · CDC-пачка: ${connectorCount}`;
              }
              const prunedCount = response.cdc_pruned_tables?.length || 0;
              if (prunedCount > 0) {
                startNote += ` · убрано из CDC: ${prunedCount}`;
              }
              if (connectorStatus) {
                startNote += ` · Runtime CDC-пачки: ${cdcRuntimeStatusLabel(connectorStatus)}`;
              }
              if (response.cdc_next_action?.message) {
                const nextAction = response.cdc_next_action;
                if ((nextAction.level === "warn" || nextAction.level === "error") && !connectorStartError) {
                  setPackQueueErr(nextAction.message);
                } else if (!startNote.includes(nextAction.message)) {
                  startNote += ` · ${nextAction.message}`;
                }
              }
              if (connectorStartError) {
                const actionText = normalizedConnectorStatus === "RUNNING"
                  ? "Таблицы сохранены в CDC-пачке, но строка не запущена: сначала синхронизируйте Debezium."
                  : "Таблицы добавлены в очередь; запустите CDC-пачку вручную.";
                setPackQueueErr(`CDC-пачка не готова: ${connectorStartError}. ${actionText}`);
              }
            }
            setPackMode(null);
            setDdlPackGroups([]);
            setSelectedIds(new Set());
            setActivePackQueueId(packQueueId);
            if (packMode === "cdc") {
              cdcGroupApi.setData(response.cdc_group || null);
            }
            onPackQueueChanged(packQueueId);
            setToast(
              autoStartOk
                ? `Добавлено в ${target}: ${count}${startNote}`
                : `Добавлено в ${target}: ${count}${startNote} · CDC-пачка требует внимания`
            );
            objectsApi.reload();
            eventsApi.reload();
            cdcGroupApi.reload();
            packQueueApi.reload();
            setTimeout(() => setToast(""), 5000);
          }}
        />
      )}

      {toast && (
        <div style={{
          position: "fixed", bottom: 80, left: "50%",
          transform: "translateX(-50%)", zIndex: 1100,
          background: t.bg.s1, color: t.text.primary,
          border: `1px solid ${t.green.dim}`, borderRadius: t.radius.md,
          padding: "8px 14px", fontSize: 13,
          boxShadow: "0 8px 24px rgba(0,0,0,.35)",
        }}>{toast}</div>
      )}

      {openObject && selectedId && (
        <ObjectDrawer
          schemaMigrationId={selectedId}
          object={openObject}
          events={events}
          srcSchema={schema.src_schema || ""}
          tgtSchema={schema.tgt_schema || ""}
          onClose={() => setOpenObject(null)}
          onAction={(o, a) => console.log("drawer action", a, o.name)}
          onApplied={() => { objectsApi.reload(); eventsApi.reload(); }}
          onAddToPack={handleAddObjectToTablePack}
        />
      )}
    </>
  );
}

function DdlSelectionBar({ count, busy, onClear, onAdd }: {
  count: number;
  busy: boolean;
  onClear: () => void;
  onAdd: () => void;
}) {
  return (
    <div style={{
      position:   "fixed",
      bottom:     20,
      left:       "50%",
      transform:  "translateX(-50%)",
      zIndex:     900,
      display:    "flex",
      alignItems: "center",
      gap:        12,
      padding:    "10px 16px",
      background: t.bg.s1,
      border:     `1px solid ${t.border.base}`,
      borderRadius: t.radius.md,
      boxShadow:  "0 8px 24px rgba(0,0,0,.35)",
    }}>
      <span style={{ fontSize: 13, color: t.text.primary }}>
        Выбрано DDL: <strong style={{ fontFamily: t.font.mono }}>{count}</strong>
      </span>
      <button onClick={onClear} style={secondaryActionStyle()}>Очистить</button>
      <button onClick={onAdd} disabled={busy} style={secondaryActionStyle(busy)}>
        В DDL-пачку ({count})
      </button>
    </div>
  );
}

function BulkSelectionBar({ count, ddlBusy, onClear, onCdcPack, onBulkPack, onSyncDdl }: {
  count: number;
  ddlBusy: boolean;
  onClear: () => void;
  onCdcPack: () => void;
  onBulkPack: () => void;
  onSyncDdl: () => void;
}) {
  return (
    <div style={{
      position:   "fixed",
      bottom:     20,
      left:       "50%",
      transform:  "translateX(-50%)",
      zIndex:     900,
      display:    "flex",
      alignItems: "center",
      gap:        12,
      padding:    "10px 16px",
      background: t.bg.s1,
      border:     `1px solid ${t.border.base}`,
      borderRadius: t.radius.md,
      boxShadow:  "0 8px 24px rgba(0,0,0,.35)",
    }}>
      <span style={{ fontSize: 13, color: t.text.primary }}>
        Выбрано: <strong style={{ fontFamily: t.font.mono }}>{count}</strong>
      </span>
      <button onClick={onClear} style={secondaryActionStyle()}>Очистить</button>
      <button onClick={onCdcPack} style={secondaryActionStyle()}>
        В {PACK_DEFINITIONS.cdc.title} ({count})
      </button>
      <button onClick={onBulkPack} style={secondaryActionStyle()}>
        В {PACK_DEFINITIONS.bulk.title.toLowerCase()} ({count})
      </button>
      <button onClick={onSyncDdl} disabled={ddlBusy} style={secondaryActionStyle(ddlBusy)}>
        {ddlBusy ? "Синхр..." : `Синхр. DDL (${count})`}
      </button>
    </div>
  );
}
