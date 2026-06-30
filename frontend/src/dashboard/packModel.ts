import type { DdlJob, MigrationPlanItem } from "./api";

export type PackKind = "bulk" | "cdc" | "ddl";
export type TablePackAddMode = "historical" | "cdc";
export type PackAddMode = TablePackAddMode | "ddl";
export type PackBucket = "draft" | "queued" | "running" | "done" | "failed";

export interface PackDefinition {
  key: PackKind;
  title: string;
  subtitle: string;
  tone: PackKind;
}

export const PACK_DEFINITIONS = {
  bulk: {
    key: "bulk",
    title: "Обычная пачка",
    subtitle: "Исторические таблицы без CDC",
    tone: "bulk",
  },
  cdc: {
    key: "cdc",
    title: "CDC-пачка",
    subtitle: "Единая CDC-пачка",
    tone: "cdc",
  },
  ddl: {
    key: "ddl",
    title: "DDL-пачка",
    subtitle: "Индексы, сиквенсы, PL/SQL, права",
    tone: "ddl",
  },
} as const satisfies Record<PackKind, PackDefinition>;

export function packAddModeKind(mode: PackAddMode): PackKind {
  if (mode === "historical") return "bulk";
  return mode;
}

export interface PackCounts {
  total: number;
  draft: number;
  queued: number;
  running: number;
  done: number;
  failed: number;
}

export interface PackAction {
  label: string;
  onClick: () => void;
  disabled?: boolean;
  primary?: boolean;
  hint?: string;
  disabledReason?: string;
}

export interface PackModel extends PackCounts {
  key: PackKind;
  title: string;
  subtitle?: string;
  tone?: PackKind | "idle";
  selected?: number;
  feedback?: string;
  actions: PackAction[];
}

const DONE_PHASES = new Set(["COMPLETED", "STEADY_STATE"]);
const FAILED_STATES = new Set(["FAILED", "CANCELLED"]);
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

export function emptyPackCounts(total = 0): PackCounts {
  return { total, draft: 0, queued: 0, running: 0, done: 0, failed: 0 };
}

export function isCdcPackItem(item: MigrationPlanItem) {
  return item.mode === "CDC" || String(item.strategy || "").startsWith("CDC");
}

export function packItemBucket(item: MigrationPlanItem): PackBucket {
  const status = String(item.status || "").toUpperCase();
  const phase = String(item.phase || "").toUpperCase();
  if (status === "DONE" || DONE_PHASES.has(phase)) return "done";
  if (FAILED_STATES.has(status) || FAILED_STATES.has(phase)) return "failed";
  if (status === "PENDING" || phase === "DRAFT") return "draft";
  if (phase === "NEW" || item.queue_position != null) return "queued";
  if (status === "RUNNING" || ACTIVE_WORK_PHASES.has(phase)) return "running";
  return "queued";
}

export function packItemCounts(items: MigrationPlanItem[]): PackCounts {
  const counts = emptyPackCounts(items.length);
  for (const item of items) {
    counts[packItemBucket(item)] += 1;
  }
  return counts;
}

export function ddlJobPackCounts(jobs: DdlJob[]): PackCounts {
  const counts = emptyPackCounts(jobs.length);
  for (const job of jobs) {
    const state = String(job.state || "").toUpperCase();
    if (state === "DRAFT") counts.draft += 1;
    else if (state === "PENDING" || state === "CLAIMED") counts.queued += 1;
    else if (state === "RUNNING") counts.running += 1;
    else if (state === "DONE") counts.done += 1;
    else if (state === "FAILED" || state === "CANCELLED") counts.failed += 1;
  }
  return counts;
}

export function packQueueGroups(items: MigrationPlanItem[]) {
  return [
    { ...PACK_DEFINITIONS.bulk, items: items.filter(item => !isCdcPackItem(item)) },
    { ...PACK_DEFINITIONS.cdc, items: items.filter(isCdcPackItem) },
  ] as const;
}
