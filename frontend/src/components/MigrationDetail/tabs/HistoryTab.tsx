import { useState } from "react";
import type { StateHistoryEntry } from "../../../types/migration";
import { PhaseBadge } from "../../PhaseBadge";
import { SectionHeader } from "../../ui";
import { fmtTs } from "../../../utils/format";
import { t } from "../../../theme";

// ── StatusDot (local helper) ──────────────────────────────────────────────────

function StatusDot({ status }: { status: string }) {
  const color =
    status === "SUCCESS" ? t.green.base
    : status === "FAILED" ? t.red.base
    : t.amber.base;
  return (
    <span style={{
      width: 7, height: 7, borderRadius: t.radius.pill, background: color,
      display: "inline-block", marginRight: 6, flexShrink: 0,
    }} />
  );
}

// ── HistoryRow (internal) ─────────────────────────────────────────────────────

function HistoryRow({ entry, isFirst }: { entry: StateHistoryEntry; isFirst: boolean }) {
  const [expanded, setExpanded] = useState(isFirst);

  return (
    <div style={{
      background: isFirst ? t.bg.s2 : t.bg.s1,
      border: `1px solid ${isFirst ? t.bg.s3 : t.border.subtle}`,
      borderRadius: 5,
      overflow: "hidden",
    }}>
      <div
        onClick={() => entry.message && setExpanded(e => !e)}
        style={{
          display: "flex",
          alignItems: "center",
          gap: t.space[2],
          padding: "7px 10px",
          cursor: entry.message ? "pointer" : "default",
          fontSize: t.size.base,
        }}
      >
        <StatusDot status={entry.transition_status} />
        <span style={{ color: t.text.muted, fontSize: t.size.sm }}>
          {entry.from_phase
            ? <><PhaseBadge phase={entry.from_phase} size="sm" />
                <span style={{ color: t.text.faint, margin: "0 4px" }}>→</span></>
            : null}
          <PhaseBadge phase={entry.to_phase} size="sm" />
        </span>
        {entry.transition_reason && (
          <span style={{ color: t.text.muted, fontSize: t.size.sm, marginLeft: 4 }}>
            [{entry.transition_reason}]
          </span>
        )}
        <span style={{ marginLeft: "auto", color: t.text.faint, fontSize: t.size.sm, whiteSpace: "nowrap" }}>
          {entry.actor_type}{entry.actor_id ? ` · ${entry.actor_id}` : ""}
        </span>
        <span style={{ color: t.text.faint, fontSize: t.size.sm, whiteSpace: "nowrap" }}>
          {fmtTs(entry.created_at, { withSeconds: true })}
        </span>
        {entry.message && (
          <span style={{ color: t.text.faint, fontSize: t.size.sm }}>{expanded ? "▲" : "▼"}</span>
        )}
      </div>
      {expanded && entry.message && (
        <div style={{
          padding: "4px 10px 8px 26px",
          color: t.text.secondary,
          fontSize: t.size.sm,
          borderTop: `1px solid ${t.border.subtle}`,
          whiteSpace: "pre-wrap",
          position: "relative",
        }}>
          <button
            onClick={() => navigator.clipboard?.writeText(entry.message!)}
            title="Копировать"
            style={{
              position: "absolute", top: 4, right: 8,
              background: "none", border: "none", color: t.text.faint,
              cursor: "pointer", fontSize: t.size.sm, padding: 2,
            }}
          >
            ⎘
          </button>
          {entry.message}
        </div>
      )}
    </div>
  );
}

// ── HistoryTab (exported) ─────────────────────────────────────────────────────

export function HistoryTab({ history }: { history: StateHistoryEntry[] }) {
  return (
    <div>
      <SectionHeader>История состояний ({history.length})</SectionHeader>
      {history.length === 0 ? (
        <div style={{ color: t.text.faint, fontSize: t.size.base }}>Нет записей</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 1 }}>
          {history.map((h, i) => (
            <HistoryRow key={h.id} entry={h} isFirst={i === 0} />
          ))}
        </div>
      )}
    </div>
  );
}
