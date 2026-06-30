import React, { useCallback, useEffect, useState } from "react";
import { useApi } from "../hooks/useApi";
import type { SSEEvent } from "../hooks/useSSE";
import { t } from "../theme";
import { secondaryActionStyle } from "./buttonStyles";
import { PackPanel } from "./PackPanel";
import {
  startDdlPack,
  startMigrationPlan,
  type DdlJob,
  type MigrationPlanCdcGroup,
  type MigrationPlanDetail,
  type SchemaMigrationListItem,
} from "./api";

interface Props {
  schema: SchemaMigrationListItem | null;
  packQueueId: number | null;
  onBack: () => void;
  sseEvents: SSEEvent[];
}

export function PackDetailsPage({ schema, packQueueId, onBack, sseEvents }: Props) {
  const [busy, setBusy] = useState(false);
  const [ddlBusy, setDdlBusy] = useState(false);
  const [err, setErr] = useState("");
  const [ddlErr, setDdlErr] = useState("");
  const [ddlFeedback, setDdlFeedback] = useState("");
  const packQueueApi = useApi<MigrationPlanDetail>(
    packQueueId ? `/api/planner/plans/${packQueueId}` : null,
    { intervalMs: 5000 },
  );
  const ddlJobsApi = useApi<DdlJob[]>(
    schema?.id ? `/api/schema-migrations/${schema.id}/ddl-jobs?limit=500` : null,
    { intervalMs: 5000 },
  );
  const cdcGroupApi = useApi<MigrationPlanCdcGroup | null>(
    schema?.id ? `/api/schema-migrations/${schema.id}/cdc-group` : null,
    { intervalMs: 5000 },
  );

  const handleStartPackQueue = useCallback(async () => {
    if (!packQueueId) return;
    setBusy(true);
    setErr("");
    try {
      await startMigrationPlan(packQueueId);
      packQueueApi.reload();
    } catch (e) {
      setErr(String(e instanceof Error ? e.message : e));
    } finally {
      setBusy(false);
    }
  }, [packQueueId, packQueueApi]);

  const handleStartDdlPack = useCallback(async () => {
    if (!schema?.id) return;
    const ids = (ddlJobsApi.data || [])
      .filter(job => job.state === "DRAFT")
      .map(job => job.job_id);
    if (!ids.length) return;
    setDdlBusy(true);
    setDdlErr("");
    setDdlFeedback("");
    try {
      const result = await startDdlPack(schema.id, ids);
      setDdlFeedback(`DDL-пачка запущена: ${result.started}`);
      ddlJobsApi.reload();
    } catch (e) {
      setDdlErr(String(e instanceof Error ? e.message : e));
    } finally {
      setDdlBusy(false);
    }
  }, [schema?.id, ddlJobsApi]);

  useEffect(() => {
    const event = sseEvents[0];
    if (!event) return;
    if (
      packQueueId
      && (
        event.type === "migration_phase"
        || event.type === "target_trigger_job"
        || (event.type === "schema_migration.plan_items_added" && event.plan_id === packQueueId)
      )
    ) {
      packQueueApi.reload();
    }
    if (
      event.type === "connector_group_status"
      || (schema?.id && event.type === "schema_migration.plan_items_added" && event.id === schema.id)
    ) {
      cdcGroupApi.reload();
    }
    if (
      schema?.id
      && (
        (event.type === "ddl_apply_job" && event.sm_id === schema.id)
        || (event.type === "ddl_pack.changed" && event.sm_id === schema.id)
        || (event.type === "ddl_pack.started" && event.sm_id === schema.id)
      )
    ) {
      ddlJobsApi.reload();
    }
  }, [sseEvents, packQueueId, schema?.id, packQueueApi.reload, cdcGroupApi.reload, ddlJobsApi.reload]);

  return (
    <div>
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "flex-start",
        gap: 12,
        marginBottom: 14,
      }}>
        <div>
          <div style={{ fontSize: 20, fontWeight: 700, color: t.text.primary }}>Пачки миграции</div>
          <div style={{
            marginTop: 4,
            color: t.text.muted,
            fontSize: 12,
            fontFamily: t.font.mono,
          }}>
            {schema ? `${schema.src_schema || "-"} -> ${schema.tgt_schema || "-"} · ${schema.id.slice(0, 8)}` : "Миграция не выбрана"}
          </div>
        </div>
        <button onClick={onBack} style={secondaryActionStyle(false)}>К таблицам</button>
      </div>

      <PackPanel
        packQueue={packQueueId ? (packQueueApi.data || null) : null}
        loading={!!packQueueId && packQueueApi.loading}
        onStart={handleStartPackQueue}
        onReload={() => packQueueApi.reload()}
        busy={busy}
        error={err || packQueueApi.error || ""}
        variant="detail"
        cdcGroup={cdcGroupApi.data || null}
        sseEvents={sseEvents}
        ddlJobs={ddlJobsApi.data || []}
        ddlLoading={!!schema?.id && ddlJobsApi.loading}
        ddlBusy={ddlBusy}
        ddlError={ddlErr || ddlJobsApi.error || ""}
        ddlFeedback={ddlFeedback}
        onStartDdlPack={handleStartDdlPack}
      />
    </div>
  );
}
