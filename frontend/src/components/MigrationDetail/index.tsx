import React, { useEffect, useState } from "react";
import type { MigrationDetail } from "../../types/migration";
import type { SSEEvent } from "../../hooks/useSSE";
import { PhaseBadge } from "../PhaseBadge";
import { t } from "../../theme";
import { isCdcMode } from "./helpers";
import { OverviewTab }   from "./tabs/OverviewTab";
import { StatisticsTab } from "./tabs/StatisticsTab";
import { ChunksTab }     from "./tabs/ChunksTab";
import { ErrorsTab }     from "./tabs/ErrorsTab";
import { HistoryTab }    from "./tabs/HistoryTab";
import {
  EnableIndexesButton,
  EnableTriggersButton,
  RestartBaselineButton,
  DataMismatchButtons,
  StopDeleteButtons,
} from "./actions";

interface Props {
  migrationId: string;
  onClose:     () => void;
  sseEvents?:  SSEEvent[];
}

type Tab = "overview" | "stats" | "chunks" | "errors" | "history";

export function MigrationDetailPanel({ migrationId, onClose, sseEvents = [] }: Props) {
  const [detail,    setDetail]    = useState<MigrationDetail | null>(null);
  const [loading,   setLoading]   = useState(true);
  const [error,     setError]     = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<Tab>("overview");

  function loadDetail() {
    fetch(`/api/migrations/${migrationId}`)
      .then(r => r.ok ? r.json() : Promise.reject(`HTTP ${r.status}`))
      .then(data => { setDetail(data); setLoading(false); })
      .catch(e => { setError(String(e)); setLoading(false); });
  }

  useEffect(() => {
    setLoading(true);
    setError(null);
    loadDetail();
  }, [migrationId]);

  useEffect(() => {
    const last = sseEvents[0];
    if (!last || !("migration_id" in last) || last.migration_id !== migrationId) return;
    if (last.type === "migration_phase" || last.type === "baseline_progress" || last.type === "target_trigger_job") {
      loadDetail();
    }
  }, [sseEvents]); // eslint-disable-line

  const phase = detail?.phase ?? "";

  const errorCount    = detail?.error_code ? 1 : 0;
  const failedChunks  = detail?.chunks_failed ?? 0;

  const tabs: { id: Tab; label: React.ReactNode }[] = [
    { id: "overview", label: "Обзор" },
    { id: "stats",    label: "Статистика" },
    {
      id: "chunks",
      label: (
        <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
          Чанки
          {failedChunks > 0 && <CountBadge n={failedChunks} />}
        </span>
      ),
    },
    {
      id: "errors",
      label: (
        <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
          Ошибки
          {errorCount > 0 && <CountBadge n={errorCount} />}
        </span>
      ),
    },
    { id: "history",  label: `История${detail ? ` (${detail.history.length})` : ""}` },
  ];

  return (
    <div style={{
      background:    t.bg.app,
      border:        `1px solid ${t.border.subtle}`,
      borderRadius:  t.radius.lg,
      overflow:      "hidden",
      display:       "flex",
      flexDirection: "column",
      height:        "100%",
    }}>
      {/* Header */}
      <div style={{
        padding:      "12px 16px 0",
        borderBottom: `1px solid ${t.border.subtle}`,
        background:   t.bg.s1,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
          {detail && <PhaseBadge phase={detail.phase} />}
          <span style={{ fontWeight: 700, fontSize: t.size.lg, color: t.text.primary, flex: 1 }}>
            {detail?.migration_name ?? "Загрузка..."}
          </span>
          {(phase === "INDEXES_ENABLING" ||
            (phase === "FAILED" && detail?.error_code === "INDEXES_ENABLE_ERROR")) && (
            <EnableIndexesButton migrationId={migrationId} onDone={loadDetail} />
          )}
          {(phase === "BASELINE_LOADING" ||
            (phase === "FAILED" && (detail?.error_code === "BASELINE_PUBLISH_ERROR" || detail?.error_code === "BASELINE_LOAD_FAILED"))) && (
            <RestartBaselineButton migrationId={migrationId} onDone={loadDetail} />
          )}
          {phase === "DATA_MISMATCH" && (
            <DataMismatchButtons migrationId={migrationId} onDone={loadDetail} />
          )}
          {detail && (
            ((!isCdcMode(detail) && phase === "COMPLETED") ||
              (isCdcMode(detail) && (
                phase === "CDC_CAUGHT_UP" ||
                phase === "STEADY_STATE"
              ))) && (
            <EnableTriggersButton migrationId={migrationId} onDone={loadDetail} />
          ))}
          <StopDeleteButtons migrationId={migrationId} phase={phase} onDone={loadDetail} onDeleted={onClose} />
          <button onClick={onClose} style={{
            background: "none", border: "none", color: t.text.disabled,
            cursor: "pointer", fontSize: 18, lineHeight: 1, padding: "0 4px",
          }}>✕</button>
        </div>

        {/* Tab bar */}
        <div style={{ display: "flex", gap: 0 }}>
          {tabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              style={{
                background:   "none",
                border:       "none",
                borderBottom: `2px solid ${activeTab === tab.id ? t.blue.base : "transparent"}`,
                color:        activeTab === tab.id ? t.blue.fg : t.text.disabled,
                fontSize:     t.size.base,
                fontWeight:   activeTab === tab.id ? 700 : 400,
                cursor:       "pointer",
                padding:      "4px 14px 8px",
                transition:   "color 0.15s, border-color 0.15s",
              }}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Body */}
      {loading && (
        <div style={{ padding: 24, color: t.text.disabled, fontSize: t.size.md }}>Загрузка...</div>
      )}
      {error && (
        <div style={{ padding: 24, color: t.red.fg, fontSize: t.size.md }}>Ошибка: {error}</div>
      )}

      {detail && !loading && (
        <div style={{ flex: 1, overflow: "auto", padding: 16 }}>
          {activeTab === "overview" && (
            <OverviewTab
              detail={detail}
              migrationId={migrationId}
              sseEvents={sseEvents}
              phase={phase}
              loadDetail={loadDetail}
            />
          )}
          {activeTab === "stats"   && <StatisticsTab detail={detail} />}
          {activeTab === "chunks"  && <ChunksTab migrationId={migrationId} sseEvents={sseEvents} />}
          {activeTab === "errors"  && <ErrorsTab detail={detail} />}
          {activeTab === "history" && <HistoryTab history={detail.history} />}
        </div>
      )}
    </div>
  );
}

function CountBadge({ n }: { n: number }) {
  return (
    <span style={{
      fontSize:     t.size.xs,
      fontWeight:   700,
      background:   t.red.border,
      color:        t.red.fg,
      borderRadius: 10,
      padding:      "0 5px",
      lineHeight:   "16px",
    }}>
      {n}
    </span>
  );
}
