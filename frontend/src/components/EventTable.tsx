import React from "react";
import { t } from "../theme";

interface CdcEvent {
  id: string; ts: string; schema: string; table: string;
  operation: string; data: unknown; old_data?: unknown;
}

const opColor: Record<string, string> = {
  INSERT: t.green.base,
  UPDATE: t.blue.base,
  DELETE: t.red.base,
  UNKNOWN: t.text.muted,
};

function OpBadge({ op }: { op: string }) {
  const color = opColor[op] ?? opColor.UNKNOWN;
  return (
    <span
      style={{
        padding: "1px 8px",
        borderRadius: 4,
        background: color + "22",
        color,
        fontWeight: 700,
        fontSize: 11,
        letterSpacing: 1,
        border: `1px solid ${color}44`,
      }}
    >
      {op}
    </span>
  );
}

interface Props {
  events: CdcEvent[];
  filter: string;
}

export function EventTable({ events, filter }: Props) {
  const lower = filter.toLowerCase();
  const visible = filter
    ? events.filter(
        (e) =>
          e.table.toLowerCase().includes(lower) ||
          e.operation.toLowerCase().includes(lower) ||
          e.schema.toLowerCase().includes(lower)
      )
    : events;

  if (visible.length === 0) {
    return (
      <div style={{ textAlign: "center", color: t.text.muted, padding: 48 }}>
        No events yet — waiting for CDC stream…
      </div>
    );
  }

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ background: t.bg.s2, color: t.text.secondary, textAlign: "left" }}>
            {["Time", "Schema", "Table", "Op", "Data", "Old Data"].map((h) => (
              <th key={h} style={{ padding: "8px 12px", fontWeight: 600, whiteSpace: "nowrap" }}>
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {visible.map((e, i) => (
            <tr
              key={e.id + i}
              style={{
                background: i % 2 === 0 ? t.bg.app : t.bg.s2,
                borderBottom: `1px solid ${t.border.subtle}`,
                animation: i === 0 ? "fadeIn 0.3s ease" : undefined,
              }}
            >
              <td style={{ padding: "6px 12px", color: t.text.muted, whiteSpace: "nowrap" }}>
                {new Date(e.ts).toLocaleTimeString()}
              </td>
              <td style={{ padding: "6px 12px", color: t.text.secondary }}>{e.schema}</td>
              <td style={{ padding: "6px 12px", color: t.text.primary, fontWeight: 600 }}>{e.table}</td>
              <td style={{ padding: "6px 12px" }}>
                <OpBadge op={e.operation} />
              </td>
              <td style={{ padding: "6px 12px", color: t.text.secondary, fontFamily: "monospace" }}>
                <code style={{ fontSize: 11 }}>{JSON.stringify(e.data)}</code>
              </td>
              <td style={{ padding: "6px 12px", color: t.text.disabled, fontFamily: "monospace" }}>
                {e.old_data ? (
                  <code style={{ fontSize: 11 }}>{JSON.stringify(e.old_data)}</code>
                ) : (
                  <span style={{ color: t.border.base }}>—</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
