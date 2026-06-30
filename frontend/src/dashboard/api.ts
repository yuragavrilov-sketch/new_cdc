/** API client for /api/schema-migrations/* endpoints. */
import type { SchemaInfo, SchemaObject, MigrationEvent, LiveMetrics } from "./types";

export interface SchemaMigrationListItem extends SchemaInfo {
  kpi: {
    totalObjects:  number;
    doneObjects:   number;
    errorObjects:  number;
    totalRows:     number;
    rowsDone:      number;
    progress:      number;
  };
  planId:  number | null;
  groupId: string | null;
  paused:  boolean;
}

export async function listSchemaMigrations(): Promise<SchemaMigrationListItem[]> {
  const r = await fetch("/api/schema-migrations");
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export async function getObjects(id: string): Promise<SchemaObject[]> {
  const r = await fetch(`/api/schema-migrations/${id}/objects`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export async function getEvents(id: string, limit = 100): Promise<MigrationEvent[]> {
  const r = await fetch(`/api/schema-migrations/${id}/events?limit=${limit}`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export async function getMetrics(id: string): Promise<LiveMetrics> {
  const r = await fetch(`/api/schema-migrations/${id}/metrics`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export interface CreatePayload {
  name?:           string;
  src_schema:      string;
  tgt_schema:      string;
  source_host?:    string;
  source_version?: string;
  target_host?:    string;
  target_version?: string;
  priority?:       "P0" | "P1" | "P2";
  owner?:          string;
  description?:    string;
  plan_id?:        number;
  group_id?:       string;
  window_at?:      string;
}

export async function createSchemaMigration(payload: CreatePayload): Promise<string> {
  const r = await fetch("/api/schema-migrations", {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify(payload),
  });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const j = await r.json();
  return j.schema_migration_id;
}

export async function pause(id: string, paused: boolean): Promise<void> {
  const r = await fetch(`/api/schema-migrations/${id}/${paused ? "pause" : "resume"}`, { method: "POST" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
}

export interface DdlSideInfo {
  metadata:      Record<string, unknown>;
  oracle_status: string | null;
  last_ddl_time: string | null;
}

export interface DdlDetailResp {
  kind:        "ddl";
  found:       boolean;
  object_type: string;       // canonical Oracle name (e.g. "MATERIALIZED VIEW")
  object_name: string;
  source?:     DdlSideInfo | null;
  target?:     DdlSideInfo | null;
  match_status?: "MATCH" | "DIFF" | "MISSING" | "EXTRA" | "UNKNOWN";
  diff?:       Record<string, unknown>;
}

export interface MigrationDetailResp {
  kind:         "migration";
  found:        boolean;
  migration_id?: string;
  migration?:   Record<string, unknown>;     // raw `migrations` row
  history?:     Array<Record<string, unknown>>;
  ddl_diff?:    DdlDetailResp | null;
}

export type ObjectDetailResp = DdlDetailResp | MigrationDetailResp;

export async function getObjectDetail(smId: string, objId: string): Promise<ObjectDetailResp> {
  const r = await fetch(`/api/schema-migrations/${smId}/objects/${encodeURIComponent(objId)}/detail`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export async function rollback(id: string): Promise<number> {
  const r = await fetch(`/api/schema-migrations/${id}/rollback`, { method: "POST" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const j = await r.json();
  return j.affected as number;
}

export type DdlApplyAction = "create_missing" | "sync_diff" | "recreate";

export interface DdlApplyResp {
  queued:   number;
  job_ids:  string[];
  skipped:  Array<{ type: string; name: string; reason: string }>;
  state?:   string;
}

export async function applyDdl(
  smId:    string,
  action:  DdlApplyAction,
  objects: Array<{ type: string; name: string }>,
  jobType?: string,
): Promise<DdlApplyResp> {
  const r = await fetch(`/api/schema-migrations/${smId}/ddl-apply`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ action, objects, job_type: jobType }),
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(t || `HTTP ${r.status}`);
  }
  return r.json();
}

export async function addDdlPackItems(
  smId:    string,
  action:  DdlApplyAction,
  objects: Array<{ type: string; name: string }>,
  jobType?: string,
): Promise<DdlApplyResp> {
  const r = await fetch(`/api/schema-migrations/${smId}/ddl-pack/items`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ action, objects, job_type: jobType }),
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(t || `HTTP ${r.status}`);
  }
  return r.json();
}

export interface DdlPackStartResp {
  started: number;
  job_ids: string[];
}

export async function startDdlPack(
  smId: string,
  jobIds?: string[],
): Promise<DdlPackStartResp> {
  const r = await fetch(`/api/schema-migrations/${smId}/ddl-pack/start`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify(jobIds && jobIds.length ? { job_ids: jobIds } : {}),
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(t || `HTTP ${r.status}`);
  }
  return r.json();
}

export interface DdlJob {
  job_id:       string;
  action:       DdlApplyAction;
  job_type:     string;
  object_type:  string;
  object_name:  string;
  state:        "DRAFT" | "PENDING" | "CLAIMED" | "RUNNING" | "DONE" | "FAILED" | "CANCELLED";
  error_text:   string | null;
  created_at:   string | null;
  started_at:   string | null;
  completed_at: string | null;
}

export async function listDdlJobs(smId: string, limit = 100): Promise<DdlJob[]> {
  const r = await fetch(`/api/schema-migrations/${smId}/ddl-jobs?limit=${limit}`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export interface MigrationPlanItem {
  item_id:          number;
  plan_id:          number;
  table_name:       string;
  mode:             string;
  batch_order:      number;
  sort_order:       number;
  overrides_json:   Record<string, unknown> | string;
  migration_id:     string | null;
  status:           string;
  phase?:           string | null;
  strategy?:        string | null;
  rows_loaded?:     number | null;
  total_rows?:      number | null;
  error_text?:      string | null;
  queue_position?:  number | null;
  state_changed_at?: string | null;
  cdc_total_lag?:   number | null;
  cdc_rows_applied?: number | null;
  cdc_worker_heartbeat?: string | null;
}

export interface MigrationPlanCdcTable {
  id:                         string;
  source_schema:              string;
  source_table:               string;
  target_schema:              string;
  target_table:               string;
  effective_key_type:         string;
  effective_key_columns_json: string | string[];
  source_pk_exists:           boolean;
  source_uk_exists:           boolean;
  topic_name:                 string;
  created_at?:                string | null;
}

export interface MigrationPlanCdcGroup {
  group_id:               string;
  group_name:             string;
  status:                 string;
  connector_name:         string;
  active_connector_name?: string;
  topic_prefix:           string;
  active_topic_prefix?:   string;
  consumer_group_prefix?: string | null;
  run_id?:                string | null;
  error_text?:            string | null;
  table_include_list:     string;
  message_key_columns:    string;
  tables:                 MigrationPlanCdcTable[];
}

export interface WorkerStatusWorker {
  worker_id: string;
  role: string;
  capabilities: string[];
  started_at: string | null;
  last_heartbeat: string | null;
  active: boolean;
}

export interface WorkerStatus {
  workers: WorkerStatusWorker[];
  active_count: number;
  cdc_ready: boolean;
  stale_after_seconds: number;
}

export interface OracleServiceMetric {
  ok: boolean;
  error?: string;
  host?: string;
  service_name?: string;
  version?: string;
  rtt_ms?: number;
}

export interface KafkaServiceMetric {
  ok: boolean;
  error?: string;
  bootstrap?: string;
  brokers?: number;
  topics?: number;
  cluster_id?: string | null;
  rtt_ms?: number;
}

export interface KafkaConnectServiceMetric {
  ok: boolean;
  error?: string;
  url?: string;
  version?: string;
  cluster_id?: string;
  connectors?: { total: number; running: number; failed: number; paused: number; unassigned: number };
  rtt_ms?: number;
}

export interface ServicesMetrics {
  oracle_source: OracleServiceMetric;
  oracle_target: OracleServiceMetric;
  kafka: KafkaServiceMetric;
  kafka_connect: KafkaConnectServiceMetric;
}

export async function getWorkerStatus(): Promise<WorkerStatus> {
  const r = await fetch("/api/workers/status");
  if (!r.ok) {
    const d = await r.json().catch(() => ({}));
    throw new Error(d.error || `HTTP ${r.status}`);
  }
  return r.json();
}

export interface MigrationPlanCdcPrunedTable {
  id?:            string;
  source_schema: string;
  source_table:  string;
  target_schema?: string;
  target_table?:  string;
}

export interface MigrationPlanDetail {
  plan_id:       number;
  name:          string;
  src_schema:    string;
  tgt_schema:    string;
  connector_group_id?: string | null;
  status:        string;
  defaults_json: Record<string, unknown> | string;
  created_at:    string | null;
  started_at:    string | null;
  completed_at:  string | null;
  items:         MigrationPlanItem[];
  cdc_group?:    MigrationPlanCdcGroup | null;
}

export async function getMigrationPlan(planId: number): Promise<MigrationPlanDetail> {
  const r = await fetch(`/api/planner/plans/${planId}`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export interface StartMigrationPlanResp {
  batch: number;
  started: string[];
}

export async function startMigrationPlan(planId: number): Promise<StartMigrationPlanResp> {
  const r = await fetch(`/api/planner/plans/${planId}/start`, { method: "POST" });
  if (!r.ok) {
    const d = await r.json().catch(() => ({}));
    throw new Error(d.error || `HTTP ${r.status}`);
  }
  return r.json();
}

export interface AddPlanItemsPayload {
  tables: Array<{ source_table: string; target_table?: string; key_columns?: string[] }>;
  strategy: "BULK_DIRECT" | "BULK_STAGE" | "CDC_DIRECT" | "CDC_STAGE";
  connector_group_id?: string;
  sequential: boolean;
  truncate_target: boolean;
  chunk_size: number;
  max_parallel_workers: number;
  baseline_parallel_degree: number;
  stage_tablespace?: string;
  prune_cdc_pack?: boolean;
}

export interface CdcNextAction {
  level: "ok" | "info" | "warn" | "error";
  code: string;
  message: string;
}

export interface AddPlanItemsResp {
  plan_id: number;
  items: Array<{ item_id: number; table: string; migration_id: string; batch_order: number }>;
  item_states?: Array<{
    item_id: number;
    table: string;
    migration_id: string;
    batch_order: number;
    status: string | null;
    phase: string | null;
    queue_position?: number | null;
    error_text?: string | null;
    cdc_worker_heartbeat?: string | null;
  }>;
  connector_group_id?: string | null;
  cdc_group?: MigrationPlanCdcGroup | null;
  connector_start?: Record<string, unknown> | null;
  connector_start_error?: string | null;
  plan_start?: StartMigrationPlanResp | null;
  plan_starts?: StartMigrationPlanResp[] | null;
  plan_start_error?: string | null;
  cdc_queue_kicked?: boolean;
  cdc_pruned_tables?: MigrationPlanCdcPrunedTable[];
  cdc_next_action?: CdcNextAction | null;
}

export async function addSchemaPlanItems(
  smId: string,
  payload: AddPlanItemsPayload,
): Promise<AddPlanItemsResp> {
  const r = await fetch(`/api/schema-migrations/${smId}/plan/items`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify(payload),
  });
  if (!r.ok) {
    const d = await r.json().catch(() => ({}));
    throw new Error(d.error || `HTTP ${r.status}`);
  }
  return r.json();
}
