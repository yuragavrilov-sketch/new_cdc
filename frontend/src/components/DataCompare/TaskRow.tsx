import { useState } from "react";
import { t } from "../../theme";
import { fmtNum } from "../../utils/format";
import type { CompareTask } from "./types";
import { ColumnDiffPanel } from "./ColumnDiffPanel";

const STATUS_COLORS: Record<string, { bg: string; text: string }> = {
  PENDING:  { bg: t.bg.s2,     text: t.text.secondary },
  CHUNKING: { bg: t.purple.bg, text: t.purple.fg },
  RUNNING:  { bg: t.bg.s3,     text: t.blue.fg },
  DONE:     { bg: t.green.bg,  text: t.green.fg },
  FAILED:   { bg: t.red.bg,    text: t.red.fg },
};

interface Props {
  task:     CompareTask;
  onDelete: (id: string) => void;
}

export function TaskRow({ task: task, onDelete }: Props) {
  const [expanded, setExpanded] = useState(false);
  const canExpand = task.status === "DONE" && task.hash_match === false;
  const sc        = STATUS_COLORS[task.status] || STATUS_COLORS.PENDING;

  const matchBadge = (match: boolean | null, label: string) => {
    if (match === null) return <span style={{ color: t.text.disabled, fontSize: t.size.sm }}>—</span>;
    return (
      <span style={{
        display: "inline-flex", alignItems: "center", gap: 4,
        fontSize: t.size.sm, fontWeight: 600,
        color: match ? t.green.base : t.red.base,
      }}>
        <span style={{
          width: 7, height: 7, borderRadius: "50%",
          background: match ? t.green.base : t.red.base,
          display: "inline-block",
        }} />
        {match ? "OK" : label}
      </span>
    );
  };

  const modeLabel = task.compare_mode === "full"
    ? "Вся таблица"
    : `Послед. ${task.last_n?.toLocaleString("ru-RU")}`;

  const elapsed = task.started_at && task.completed_at
    ? `${((new Date(task.completed_at).getTime() - new Date(task.started_at).getTime()) / 1000).toFixed(1)}с`
    : (task.status === "RUNNING" || task.status === "CHUNKING") ? "..." : "—";

  return (
    <>
      <tr style={{ borderBottom: expanded ? "none" : `1px solid ${t.bg.s2}` }}>
        <td style={{ padding: "6px 10px" }}>
          <div style={{ fontSize: t.size.base, color: t.text.primary }}>
            {task.source_schema}.{task.source_table}
          </div>
          {(task.source_schema !== task.target_schema || task.source_table !== task.target_table) && (
            <div style={{ fontSize: t.size.sm, color: t.text.muted }}>
              → {task.target_schema}.{task.target_table}
            </div>
          )}
        </td>

        <td style={{ padding: "6px 10px" }}>
          <span style={{ fontSize: t.size.sm, color: t.text.secondary }}>{modeLabel}</span>
          {task.compare_mode === "last_n" && task.order_column && (
            <div style={{ fontSize: t.size.xs, color: t.text.disabled }}>by {task.order_column}</div>
          )}
        </td>

        <td style={{ padding: "6px 10px" }}>
          <span style={{
            background: sc.bg, color: sc.text,
            padding: "2px 8px", borderRadius: t.radius.sm,
            fontSize: t.size.sm, fontWeight: 600,
            ...((task.status === "RUNNING" || task.status === "CHUNKING") ? { animation: "pulse 1.5s infinite" } : {}),
          }}>
            {task.status}
          </span>
          {task.status === "FAILED" && task.error_text && (
            <div
              style={{ fontSize: t.size.xs, color: t.red.fg, maxWidth: 200, marginTop: 3 }}
              title={task.error_text}
            >
              {task.error_text.length > 60 ? task.error_text.slice(0, 60) + "..." : task.error_text}
            </div>
          )}
          {(task.status === "RUNNING" || task.status === "CHUNKING") && task.chunks_total > 0 && (
            <div style={{ marginTop: 4 }}>
              <div style={{
                height: 4, borderRadius: 2, background: t.bg.s2, overflow: "hidden", width: 100,
              }}>
                <div style={{
                  height: "100%", borderRadius: 2,
                  background: t.blue.base,
                  width: `${Math.round((task.chunks_done / task.chunks_total) * 100)}%`,
                  transition: "width 0.3s",
                }} />
              </div>
              <div style={{ fontSize: t.size.xs, color: t.text.muted, marginTop: 2 }}>
                {task.chunks_done} / {task.chunks_total}
              </div>
            </div>
          )}
        </td>

        <td style={{
          padding: "6px 10px", fontSize: t.size.base,
          color: t.text.primary, fontVariantNumeric: "tabular-nums",
        }}>
          {fmtNum(task.source_count)}
        </td>
        <td style={{
          padding: "6px 10px", fontSize: t.size.base,
          color: t.text.primary, fontVariantNumeric: "tabular-nums",
        }}>
          {fmtNum(task.target_count)}
        </td>

        <td style={{ padding: "6px 10px" }}>
          {matchBadge(task.counts_match, task.source_count !== null && task.target_count !== null
            ? (Math.abs((task.source_count || 0) - (task.target_count || 0))).toLocaleString("ru-RU")
            : "Mismatch")}
        </td>

        <td style={{ padding: "6px 10px" }}>
          {canExpand ? (
            <span
              onClick={() => setExpanded(v => !v)}
              style={{
                display: "inline-flex", alignItems: "center", gap: 4,
                fontSize: t.size.sm, fontWeight: 600, color: t.red.base,
                cursor: "pointer", textDecoration: "underline", textDecorationStyle: "dotted",
              }}
              title="Показать разницу по колонкам"
            >
              <span style={{
                width: 7, height: 7, borderRadius: "50%",
                background: t.red.base, display: "inline-block",
              }} />
              Diff {expanded ? "▲" : "▼"}
            </span>
          ) : (
            matchBadge(task.hash_match, "Diff")
          )}
        </td>

        <td style={{ padding: "6px 10px", fontSize: t.size.sm, color: t.text.muted, whiteSpace: "nowrap" }}>
          {elapsed}
        </td>

        <td style={{ padding: "6px 10px" }}>
          <button
            onClick={() => onDelete(task.task_id)}
            title="Удалить"
            style={{
              background: "none", border: "none", color: t.text.disabled,
              cursor: "pointer", fontSize: t.size.md, padding: "2px 6px",
            }}
            onMouseEnter={e => (e.currentTarget.style.color = t.red.base)}
            onMouseLeave={e => (e.currentTarget.style.color = t.text.disabled)}
          >
            ✕
          </button>
        </td>
      </tr>
      {expanded && (
        <tr style={{ borderBottom: `1px solid ${t.bg.s2}` }}>
          <td colSpan={9} style={{ background: t.bg.s2, padding: 0 }}>
            <ColumnDiffPanel taskId={task.task_id} />
          </td>
        </tr>
      )}
    </>
  );
}
