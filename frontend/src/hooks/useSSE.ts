import { useCallback, useEffect, useRef, useState } from "react";

export type SSEStatus = "connecting" | "connected" | "error" | "closed";

export type ServiceName = "oracle_source" | "oracle_target" | "kafka" | "kafka_connect";
export type ServiceAvailability = "up" | "down" | "unknown";

export interface ServiceStatus {
  status: ServiceAvailability;
  message: string;
  checked_at?: string;
}

export type ServiceStatuses = Record<ServiceName, ServiceStatus>;

// ── SSE event shapes ──────────────────────────────────────────────────────────

export interface MigrationPhaseEvent {
  type: "migration_phase";
  migration_id: string;
  from_phase?: string;
  phase: string;
  ts: string;
}

export interface ChunkProgressEvent {
  type: "chunk_progress";
  migration_id: string;
  chunks_done: number;
  total_chunks: number;
  ts: string;
}

export interface ConnectorStatusEvent {
  type: "connector_status";
  migration_id: string;
  status: string;
  connector_name: string;
  ts: string;
}

export interface ConnectorGroupStatusEvent {
  type: "connector_group_status";
  group_id: string;
  status: string;
  ts?: string;
}

export interface SchemaPlanItemsAddedEvent {
  type: "schema_migration.plan_items_added";
  id: string;
  plan_id: number;
  count: number;
  ts?: string;
}

export interface KafkaLagEvent {
  type: "kafka_lag";
  migration_id: string;
  total_lag: number;
  updated_at: string | null;
  ts: string;
}

export interface BaselineProgressEvent {
  type: "baseline_progress";
  migration_id: string;
  baseline_chunks_done: number;
  baseline_chunks_total: number;
  ts: string;
}

export interface TargetTriggerJobEvent {
  type: "target_trigger_job";
  migration_id: string;
  job_id: string;
  state: "PENDING" | "RUNNING" | "DONE" | "FAILED";
  enabled_count?: number;
  error_text?: string;
  ts: string;
}

export interface DdlSnapshotProgressEvent {
  type: "ddl_snapshot.progress";
  src_schema: string;
  tgt_schema: string;
  snapshot_id: number;
  phase:   "listing" | "source" | "target" | "comparing";
  done:    number;
  total:   number;
  current: string;
  ts:      string;
}

export interface DdlSnapshotCompleteEvent {
  type: "ddl_snapshot.complete";
  src_schema: string;
  tgt_schema: string;
  snapshot_id: number;
  object_counts: Record<string, number>;
  src_total: number;
  tgt_total: number;
  ts:        string;
}

export interface DdlApplyJobEvent {
  type: "ddl_apply_job";
  sm_id: string;
  job_id: string;
  job_type: string;
  action: string;
  object_type: string;
  object_name: string;
  state: "PENDING" | "RUNNING" | "DONE" | "FAILED" | "CANCELLED";
  error_text?: string | null;
  ts: string;
}

export interface DdlPackChangedEvent {
  type: "ddl_pack.changed";
  sm_id: string;
  action: string;
  job_type?: string | null;
  queued: number;
  ts?: string;
}

export interface DdlPackStartedEvent {
  type: "ddl_pack.started";
  sm_id: string;
  started: number;
  ts?: string;
}

export type SSEEvent =
  | MigrationPhaseEvent
  | ChunkProgressEvent
  | ConnectorStatusEvent
  | ConnectorGroupStatusEvent
  | SchemaPlanItemsAddedEvent
  | KafkaLagEvent
  | BaselineProgressEvent
  | TargetTriggerJobEvent
  | DdlApplyJobEvent
  | DdlPackChangedEvent
  | DdlPackStartedEvent
  | DdlSnapshotProgressEvent
  | DdlSnapshotCompleteEvent;

// ── Defaults ──────────────────────────────────────────────────────────────────

const DEFAULT_STATUSES: ServiceStatuses = {
  oracle_source: { status: "unknown", message: "Not yet checked" },
  oracle_target: { status: "unknown", message: "Not yet checked" },
  kafka:         { status: "unknown", message: "Not yet checked" },
  kafka_connect: { status: "unknown", message: "Not yet checked" },
};

interface UseSSEOptions {
  url: string;
  maxEvents?: number;
}

export function useSSE({ url, maxEvents = 200 }: UseSSEOptions) {
  const [events,          setEvents]          = useState<SSEEvent[]>([]);
  const [status,          setStatus]          = useState<SSEStatus>("connecting");
  const [serviceStatuses, setServiceStatuses] = useState<ServiceStatuses>(DEFAULT_STATUSES);
  const [trigger,         setTrigger]         = useState(0); // bump to force reconnect
  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    setStatus("connecting");
    const es = new EventSource(url);
    sourceRef.current = es;

    es.addEventListener("connected", () => setStatus("connected"));

    es.onmessage = (e: MessageEvent) => {
      try {
        const parsed = JSON.parse(e.data);

        if (parsed.type === "service_status") {
          setServiceStatuses((prev) => ({
            ...prev,
            [parsed.service]: {
              status:     parsed.status,
              message:    parsed.message,
              checked_at: parsed.ts,
            },
          }));
        } else if (
          parsed.type === "migration_phase"  ||
          parsed.type === "chunk_progress"   ||
          parsed.type === "connector_status" ||
          parsed.type === "connector_group_status" ||
          parsed.type === "schema_migration.plan_items_added" ||
          parsed.type === "kafka_lag"        ||
          parsed.type === "baseline_progress" ||
          parsed.type === "target_trigger_job" ||
          parsed.type === "ddl_apply_job" ||
          parsed.type === "ddl_pack.changed" ||
          parsed.type === "ddl_pack.started" ||
          parsed.type === "ddl_snapshot.progress" ||
          parsed.type === "ddl_snapshot.complete"
        ) {
          setEvents((prev) => [parsed as SSEEvent, ...prev].slice(0, maxEvents));
        }

        setStatus("connected");
      } catch {
        // ignore malformed
      }
    };

    es.onerror = () => setStatus("error");

    return () => {
      es.close();
      sourceRef.current = null;
    };
  }, [url, maxEvents, trigger]); // eslint-disable-line

  const reconnect = useCallback(() => {
    sourceRef.current?.close();
    setTrigger(t => t + 1);
  }, []);

  return { events, status, serviceStatuses, reconnect };
}
