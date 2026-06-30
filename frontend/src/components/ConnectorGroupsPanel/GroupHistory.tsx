import type { GroupStatus } from "../../types/migration";
import { t } from "../../theme";
import type { GroupHistoryEntry } from "./types";
import { STATUS_COLORS } from "./helpers";
import { cdcRuntimeStatusLabel } from "../../dashboard/displayLabels";

interface Props {
  history: GroupHistoryEntry[];
}

export function GroupHistory({ history }: Props) {
  if (history.length === 0) return null;
  return (
    <>
      <h4 style={{ color: t.text.muted, fontSize: t.size.base, margin: "12px 0 6px" }}>
        История ({history.length})
      </h4>
      <div style={{
        maxHeight: 180, overflowY: "auto",
        border: `1px solid ${t.border.subtle}`, borderRadius: t.radius.md,
      }}>
        {history.map((h, i) => {
          const hsc = STATUS_COLORS[(h.to_status as GroupStatus)] || STATUS_COLORS.PENDING;
          return (
            <div key={i} style={{
              display: "flex", alignItems: "center", gap: 8,
              padding: "5px 10px", fontSize: t.size.xs,
              borderBottom: i < history.length - 1 ? `1px solid ${t.border.subtle}` : "none",
            }}>
              <span style={{ color: t.text.disabled, fontFamily: t.font.mono, whiteSpace: "nowrap" }}>
                {new Date(h.created_at).toLocaleString()}
              </span>
              {h.from_status && (
                <span style={{ color: t.text.disabled }}>{cdcRuntimeStatusLabel(h.from_status)}</span>
              )}
              <span style={{ color: t.text.disabled }}>→</span>
              <span style={{
                background: hsc.bg, color: hsc.text,
                padding: "1px 6px", borderRadius: 3, fontWeight: 600,
              }}>
                {cdcRuntimeStatusLabel(h.to_status)}
              </span>
              {h.message && (
                <span style={{
                  color: t.text.muted, flex: 1,
                  overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                }}>
                  {h.message}
                </span>
              )}
            </div>
          );
        })}
      </div>
    </>
  );
}
