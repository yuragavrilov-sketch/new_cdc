import React from "react";
import { t } from "../../theme";

export type Priority = "P0" | "P1" | "P2";

const TONES: Record<Priority, { bg: string; fg: string }> = {
  P0: { bg: t.tone.errorSoft, fg: t.tone.error },
  P1: { bg: t.tone.warnSoft,  fg: t.tone.warn  },
  P2: { bg: t.bg.s3,          fg: t.text.muted },
};

export function PriorityChip({ priority }: { priority: Priority }) {
  const c = TONES[priority];
  return (
    <span style={{
      fontFamily:    t.font.mono,
      fontSize:      "9.5px",
      fontWeight:    600,
      padding:       "1px 5px",
      borderRadius:  3,
      background:    c.bg,
      color:         c.fg,
      textTransform: "uppercase",
      letterSpacing: "0.04em",
    }}>
      {priority}
    </span>
  );
}
