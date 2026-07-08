import React from "react";
import type { MigrationSummary } from "../../types/migration";
import { hasCdc, strategyLabel } from "../../types/migration";
import { PhaseBadge } from "../PhaseBadge";
import { t } from "../../theme";
import { fmtTs, fmtSpeed } from "../../utils/format";
import { ActionBtn } from "./ActionBtn";
import { BULK_PHASES, migrationRowActions, type MigrationRowAction } from "./helpers";

interface Props {
  m:        MigrationSummary;
  selected: boolean;
  compact:  boolean;
  busy:     boolean;
  speed?:   { chunksSec: number; rowsSec: number };
  onClick:  () => void;
  onAction: (id: string, action: MigrationRowAction) => void;
  /** Bulk-select state — независимо от выделения «строка раскрыта» */
  checked?:        boolean;
  onToggleCheck?:  (id: string) => void;
}

export function MigrationRow({
  m, selected, compact, busy, speed, onClick, onAction,
  checked, onToggleCheck,
}: Props) {
  const { canRun, canPause, canResume, canStop, canDelete } = migrationRowActions(m.phase, m.paused);

  const showProgress = BULK_PHASES.has(m.phase) && m.total_chunks != null && m.total_chunks > 0;
  const pct = showProgress ? Math.min(100, (m.chunks_done / m.total_chunks!) * 100) : 0;

  return (
    <div
      onClick={onClick}
      style={{
        padding:      compact ? "10px 14px" : "12px 16px",
        borderBottom: `1px solid ${t.bg.app}`,
        cursor:       "pointer",
        background:   selected ? t.bg.s2 : "transparent",
        borderLeft:   `3px solid ${selected ? t.blue.base : "transparent"}`,
        transition:   "background 0.1s",
      }}
      onMouseEnter={e => { if (!selected) (e.currentTarget as HTMLDivElement).style.background = t.bg.s2; }}
      onMouseLeave={e => { if (!selected) (e.currentTarget as HTMLDivElement).style.background = "transparent"; }}
    >
      {/* Name + phase + actions */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 5 }}>
        {onToggleCheck && (
          <input
            type="checkbox"
            checked={!!checked}
            onChange={() => onToggleCheck(m.migration_id)}
            onClick={e => e.stopPropagation()}
            style={{ flexShrink: 0, cursor: "pointer" }}
            title="Выбрать для массового действия"
          />
        )}
        <span style={{
          fontWeight: 700, fontSize: t.size.md, color: t.text.primary,
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          flex: 1, minWidth: 0,
        }}>
          {m.migration_name}
        </span>
        <PhaseBadge phase={m.phase} size="sm" />
        {m.paused && (
          <span style={{
            background: t.amber.bg, color: t.amber.fg,
            border: `1px solid ${t.amber.dim}`, borderRadius: t.radius.sm,
            fontSize: t.size.xs, fontWeight: 700, padding: "1px 6px",
          }}>
            Пауза
          </span>
        )}
        {m.strategy && (
          <span style={{
            marginLeft: 8, padding: "2px 6px", borderRadius: 4,
            fontSize: 11,
            background: hasCdc(m.strategy) ? t.purple.bg : t.green.bg,
            color: hasCdc(m.strategy) ? t.purple.fg : t.green.fg,
            border: `1px solid ${hasCdc(m.strategy) ? t.purple.base : t.green.dim}`,
          }}>
            {strategyLabel(m.strategy)}
          </span>
        )}
        {m.phase === "NEW" && m.queue_position != null && (
          <span style={{
            background: t.amber.bg, color: t.amber.fg,
            border: `1px solid ${t.amber.dim}`, borderRadius: t.radius.sm,
            fontSize: t.size.xs, fontWeight: 700, padding: "1px 6px",
          }}>
            #{m.queue_position} в очереди
          </span>
        )}
        <div
          style={{ display: "flex", gap: 4, flexShrink: 0 }}
          onClick={e => e.stopPropagation()}
        >
          {canRun && (
            <ActionBtn icon="▶" title="Запустить" color={t.green.fg} bg={t.green.bg}
              disabled={busy}
              onClick={() => onAction(m.migration_id, "run")} />
          )}
          {canPause && (
            <ActionBtn icon="Ⅱ" title="Пауза: не брать новые bulk chunks" color={t.amber.fg} bg={t.amber.bg}
              disabled={busy}
              onClick={() => onAction(m.migration_id, "pause")} />
          )}
          {canResume && (
            <ActionBtn icon="▶" title="Продолжить" color={t.green.fg} bg={t.green.bg}
              disabled={busy}
              onClick={() => onAction(m.migration_id, "resume")} />
          )}
          {canStop && (
            <ActionBtn icon="⏹" title="Остановить" color={t.red.fg} bg={t.red.bg}
              disabled={busy}
              onClick={() => onAction(m.migration_id, "stop")} />
          )}
          {canDelete && (
            <ActionBtn icon="✕" title="Удалить" color={t.text.secondary} bg={t.bg.s2}
              disabled={busy}
              onClick={() => onAction(m.migration_id, "delete")} />
          )}
          {busy && (
            <span style={{ fontSize: t.size.xs, color: t.text.disabled, alignSelf: "center" }}>…</span>
          )}
        </div>
      </div>

      {/* Source → Target */}
      <div style={{ fontSize: t.size.sm, color: t.text.disabled, marginBottom: 4 }}>
        <span style={{ color: t.text.muted }}>{m.source_schema}.{m.source_table}</span>
        <span style={{ color: t.text.faint, margin: "0 6px" }}>→</span>
        <span style={{ color: t.text.muted }}>{m.target_schema}.{m.target_table}</span>
      </div>

      {/* Progress (bulk phases) */}
      {showProgress && (
        <div style={{ marginBottom: 5 }}>
          <div style={{
            height: 4, borderRadius: 2,
            background: t.bg.s2, overflow: "hidden",
          }}>
            <div style={{
              height: "100%", borderRadius: 2,
              width: `${pct}%`,
              background: m.chunks_failed > 0 ? t.red.dim : t.blue.base,
              transition: "width 0.4s ease",
            }} />
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 3 }}>
            <span style={{ fontSize: t.size.xs, color: t.text.disabled }}>
              {m.chunks_done}
              {m.chunks_failed > 0 && <span style={{ color: t.red.base }}> / {m.chunks_failed} ✗</span>}
              {" "}/ {m.total_chunks} чанков
              {m.rows_loaded > 0 && (
                <span style={{ color: t.text.faint }}>
                  {" "}· {m.rows_loaded.toLocaleString("ru-RU")} строк
                </span>
              )}
            </span>
            {speed && (speed.chunksSec > 0 || speed.rowsSec > 0) && (
              <span style={{ fontSize: t.size.xs, color: t.text.muted }}>
                {speed.chunksSec > 0 && <>{fmtSpeed(speed.chunksSec)} чанк</>}
                {speed.chunksSec > 0 && speed.rowsSec > 0 && " · "}
                {speed.rowsSec > 0 && <>{fmtSpeed(speed.rowsSec)} строк/с</>}
              </span>
            )}
            <span style={{ fontSize: t.size.xs, color: t.text.disabled }}>{pct.toFixed(1)}%</span>
          </div>
        </div>
      )}

      {/* Error + meta */}
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        {m.error_code && (
          <span style={{
            background: t.red.bg, color: t.red.fg,
            border: `1px solid ${t.red.border}`, borderRadius: 3,
            fontSize: t.size.xs, padding: "1px 5px", fontWeight: 600,
          }}>
            {m.error_code}
          </span>
        )}
        {m.retry_count > 0 && (
          <span style={{ fontSize: t.size.xs, color: t.amber.base }}>↺ {m.retry_count}</span>
        )}
        <span style={{ marginLeft: "auto", fontSize: t.size.xs, color: t.text.faint }}>
          {fmtTs(m.state_changed_at)}
        </span>
      </div>
    </div>
  );
}
