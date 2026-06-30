import React from "react";
import { t } from "../../theme";

export interface Stage {
  key:   string;
  label: string;
}

interface Props {
  stages:  Stage[];
  current: string;
  status?: "running" | "error" | "done" | "paused";
}

/** Horizontal stage pipeline — dot + label per stage, with line connectors. */
export function StagePipeline({ stages, current, status = "running" }: Props) {
  const currentIdx = stages.findIndex(s => s.key === current);
  return (
    <div style={{ display: "flex", gap: 0, padding: "2px 0" }}>
      {stages.map((s, i) => {
        const state =
          i < currentIdx ? "done" :
          i === currentIdx ? (status === "error" ? "error" : "active") :
          "idle";
        return (
          <div key={s.key} style={{
            display: "flex", alignItems: "center", gap: 6,
            flex: 1, minWidth: 0, padding: "2px 0",
            position: "relative",
          }} title={s.label}>
            {i > 0 && (
              <span aria-hidden style={{
                position: "absolute", left: 0, top: "50%",
                width: 12, height: 1,
                background: t.border.subtle,
                transform: "translateX(-100%)",
              }}/>
            )}
            <span style={{
              width: 6, height: 6, borderRadius: "50%", flexShrink: 0,
              background:
                state === "done"   ? t.tone.ok :
                state === "active" ? t.tone.info :
                state === "error"  ? t.tone.error :
                                     t.bg.s3,
              border: `1px solid ${
                state === "done"   ? t.tone.ok :
                state === "active" ? t.tone.info :
                state === "error"  ? t.tone.error :
                                     t.border.strong
              }`,
              boxShadow:
                state === "active" ? `0 0 0 3px color-mix(in oklab, ${t.tone.info} 22%, transparent)` :
                state === "error"  ? `0 0 0 3px color-mix(in oklab, ${t.tone.error} 22%, transparent)` :
                                     "none",
            }}/>
            <span style={{
              fontSize:      "10px",
              textTransform: "uppercase",
              letterSpacing: "0.05em",
              fontWeight:    state === "active" || state === "error" ? 600 : 500,
              color:
                state === "active" ? t.tone.info :
                state === "error"  ? t.tone.error :
                state === "done"   ? t.text.secondary :
                                     t.text.muted,
            }}>
              {s.label}
            </span>
          </div>
        );
      })}
    </div>
  );
}
