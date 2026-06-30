import React from "react";
import type { SSEStatus } from "../hooks/useSSE";
import { t } from "../theme";

const COLORS: Record<SSEStatus, string> = {
  connecting: t.amber.base,
  connected:  t.green.base,
  error:      t.red.base,
  closed:     t.text.muted,
};

export function StatusBadge({
  status,
  onReconnect,
}: {
  status: SSEStatus;
  onReconnect?: () => void;
}) {
  const c = COLORS[status];
  return (
    <span
      style={{
        display:      "inline-flex",
        alignItems:   "center",
        gap:          6,
        padding:      "2px 10px",
        borderRadius: t.radius.pill,
        background:   c + "22",
        color:        c,
        fontWeight:   600,
        fontSize:     t.size.md,
        border:       `1px solid ${c}55`,
      }}
    >
      <span
        style={{
          width:        8,
          height:       8,
          borderRadius: "50%",
          background:   c,
          display:      "inline-block",
          animation:    status === "connected" ? "pulse 2s infinite" : "none",
        }}
      />
      {status}
      {(status === "error" || status === "closed") && onReconnect && (
        <button
          onClick={onReconnect}
          title="Переподключиться"
          style={{
            background: "none",
            border:     "none",
            color:      "inherit",
            cursor:     "pointer",
            padding:    "0 2px",
            fontSize:   14,
            lineHeight: 1,
            opacity:    0.8,
          }}
        >
          ↺
        </button>
      )}
    </span>
  );
}
