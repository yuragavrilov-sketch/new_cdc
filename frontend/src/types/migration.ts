import { t } from "../theme";

export type Strategy = "CDC_STAGE" | "CDC_DIRECT" | "BULK_STAGE" | "BULK_DIRECT";

export const hasCdc    = (s: Strategy): boolean => s.startsWith("CDC_");
export const usesStage = (s: Strategy): boolean => s.endsWith("_STAGE");
export const forcesTargetTruncate = (s: Strategy): boolean => s === "CDC_DIRECT";

export const strategyLabel = (s: Strategy): string =>
  `${hasCdc(s) ? "С CDC" : "Без CDC"} (${usesStage(s) ? "stage" : "direct"})`;

export const composeStrategy = (cdc: boolean, stage: boolean): Strategy =>
  `${cdc ? "CDC" : "BULK"}_${stage ? "STAGE" : "DIRECT"}` as Strategy;

export type MigrationPhase =
  | "DRAFT" | "NEW"
  | "TOPIC_CREATING"
  | "CHUNKING" | "BULK_LOADING" | "BULK_LOADED"
  | "STAGE_VALIDATING" | "STAGE_VALIDATED"
  | "BASELINE_PUBLISHING" | "BASELINE_LOADING" | "BASELINE_PUBLISHED"
  | "STAGE_DROPPING" | "INDEXES_ENABLING"
  | "DATA_VERIFYING" | "DATA_MISMATCH"
  | "CDC_APPLY_STARTING" | "CDC_APPLYING" | "CDC_CATCHING_UP" | "CDC_CAUGHT_UP"
  | "STEADY_STATE" | "PAUSED"
  | "CANCELLING" | "CANCELLED"
  | "COMPLETED" | "FAILED";

export interface Migration {
  migration_id: string;
  migration_name: string;
  phase: MigrationPhase;
  state_changed_at: string;
  source_connection_id: string;
  target_connection_id: string;
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  stage_table_name: string;
  stage_tablespace: string;
  connector_name: string;
  topic_prefix: string;
  consumer_group: string;
  chunk_strategy: string;
  chunk_size: number;
  apply_mode: string;
  source_pk_exists: boolean;
  source_uk_exists: boolean;
  effective_key_type: string;
  effective_key_source: string;
  effective_key_columns_json: string;
  key_uniqueness_validated: boolean;
  key_validation_status: string | null;
  key_validation_message: string | null;
  start_scn: string | null;
  scn_fixed_at: string | null;
  created_by: string | null;
  description: string | null;
  created_at: string;
  updated_at: string;
  locked_by: string | null;
  lock_until: string | null;
  error_code: string | null;
  error_text: string | null;
  failed_phase: string | null;
  retry_count: number;
  // ── Progress / monitoring (added stage 1) ────────────────────────────────
  total_rows: number | null;
  total_chunks: number | null;
  chunks_done: number;
  chunks_failed: number;
  validate_hash_sample: boolean;
  validation_result: Record<string, unknown> | null;
  connector_status: string | null;
  kafka_lag: number | null;
  kafka_lag_checked_at: string | null;
  rows_loaded: number;
  max_parallel_workers: number;
  baseline_parallel_degree: number;
  baseline_batch_size: number;
  strategy: Strategy;
  baseline_chunks_total: number | null;
  baseline_chunks_done: number;
  queue_position: number | null;
  group_id: string | null;
  data_compare_task_id?: string;
  truncate_target: boolean;
}

// ── Connector Groups ──────────────────────────────────────────────────────────

export type GroupStatus = "PENDING" | "TOPICS_CREATING" | "CONNECTOR_STARTING" | "RUNNING" | "STOPPING" | "STOPPED" | "FAILED";

export interface ConnectorGroup {
  group_id: string;
  group_name: string;
  source_connection_id: string;
  connector_name: string;
  topic_prefix: string;
  consumer_group_prefix: string;
  status: GroupStatus;
  error_text: string | null;
  table_include_list?: string;
  message_key_columns?: string;
  created_at: string;
  updated_at: string;
  migrations?: MigrationSummary[];
}

export interface MigrationSummary {
  migration_id: string;
  migration_name: string;
  phase: MigrationPhase;
  state_changed_at: string;
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  created_at: string;
  updated_at: string;
  error_code: string | null;
  error_text: string | null;
  failed_phase: string | null;
  retry_count: number;
  description: string | null;
  created_by: string | null;
  total_rows: number | null;
  total_chunks: number | null;
  chunks_done: number;
  chunks_failed: number;
  rows_loaded: number;
  strategy: Strategy;
  queue_position: number | null;
}

export interface StateHistoryEntry {
  id: number;
  migration_id: string;
  from_phase: string | null;
  to_phase: string;
  transition_status: string;
  transition_reason: string | null;
  message: string | null;
  actor_type: string;
  actor_id: string | null;
  correlation_id: string | null;
  created_at: string;
}

export interface MigrationDetail extends Migration {
  history: StateHistoryEntry[];
}

// ── Phase colours ────────────────────────────────────────────────────────────

interface PhaseColor { bg: string; text: string; border: string }

const PHASE_COLORS: Record<string, PhaseColor> = {
  DRAFT:               { bg: t.border.subtle, text: t.text.secondary, border: t.border.base },
  NEW:                 { bg: t.bg.s3, text: t.blue.fg, border: t.blue.dim },
  TOPIC_CREATING:      { bg: t.purple.bg, text: t.purple.fg, border: t.purple.base },
  CHUNKING:            { bg: t.amber.bg, text: t.amber.fg, border: t.amber.dim },
  BULK_LOADING:        { bg: t.amber.bg, text: t.amber.fg, border: t.amber.dim },
  BULK_LOADED:         { bg: t.amber.bg, text: t.amber.fg, border: t.amber.dim },
  STAGE_VALIDATING:    { bg: t.blue.bg, text: t.blue.fg, border: t.blue.dim },
  STAGE_VALIDATED:     { bg: t.blue.bg, text: t.blue.fg, border: t.blue.dim },
  BASELINE_PUBLISHING: { bg: t.purple.bg, text: t.purple.fg, border: t.purple.base },
  BASELINE_LOADING:    { bg: t.purple.bg, text: t.purple.fg, border: t.purple.base },
  BASELINE_PUBLISHED:  { bg: t.purple.bg, text: t.purple.fg, border: t.purple.base },
  STAGE_DROPPING:      { bg: t.bg.s2, text: t.green.fg, border: t.green.base },
  INDEXES_ENABLING:    { bg: t.bg.s2, text: t.green.fg, border: t.green.base },
  DATA_VERIFYING:      { bg: t.blue.bg, text: t.blue.fg, border: t.blue.dim },
  DATA_MISMATCH:       { bg: t.red.bg, text: t.amber.fg, border: t.amber.dim },
  CDC_APPLY_STARTING:  { bg: t.red.bg, text: t.amber.fg, border: t.amber.dim },
  CDC_APPLYING:        { bg: t.red.bg, text: t.amber.fg, border: t.amber.dim },
  CDC_CATCHING_UP:     { bg: t.red.bg, text: t.amber.fg, border: t.amber.dim },
  CDC_CAUGHT_UP:       { bg: t.red.bg, text: t.amber.fg, border: t.amber.dim },
  STEADY_STATE:        { bg: t.green.bg, text: t.green.fg, border: t.green.dim },
  PAUSED:              { bg: t.border.subtle, text: t.text.primary, border: t.text.disabled },
  CANCELLING:          { bg: t.red.bg, text: t.red.fg, border: t.red.dim },
  CANCELLED:           { bg: t.bg.s2, text: t.text.muted, border: t.text.disabled },
  COMPLETED:           { bg: t.green.bg, text: t.green.fg, border: t.green.dim },
  FAILED:              { bg: t.red.bg, text: t.red.fg, border: t.red.dim },
};

const FALLBACK_COLOR: PhaseColor = { bg: t.border.subtle, text: t.text.secondary, border: t.border.base };

export function phaseColor(phase: string): PhaseColor {
  return PHASE_COLORS[phase] ?? FALLBACK_COLOR;
}

export const ORDERED_PHASES: MigrationPhase[] = [
  "DRAFT", "NEW",
  "TOPIC_CREATING",
  "CHUNKING", "BULK_LOADING", "BULK_LOADED",
  "STAGE_VALIDATING", "STAGE_VALIDATED",
  "BASELINE_PUBLISHING", "BASELINE_LOADING", "BASELINE_PUBLISHED",
  "STAGE_DROPPING", "INDEXES_ENABLING",
  "DATA_VERIFYING", "DATA_MISMATCH",
  "CDC_APPLY_STARTING", "CDC_APPLYING", "CDC_CATCHING_UP", "CDC_CAUGHT_UP",
  "STEADY_STATE",
];
