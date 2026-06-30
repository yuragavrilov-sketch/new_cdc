import React from "react";
import { t } from "../../theme";

export type StatusTone = "info" | "ok" | "warn" | "error" | "muted";

const TONES: Record<StatusTone, { bg: string; fg: string; border: string; dotPulse: boolean }> = {
  info:  { bg: t.tone.infoSoft,  fg: t.tone.info,  border: t.tone.info,  dotPulse: true  },
  ok:    { bg: t.tone.okSoft,    fg: t.tone.ok,    border: t.tone.ok,    dotPulse: false },
  warn:  { bg: t.tone.warnSoft,  fg: t.tone.warn,  border: t.tone.warn,  dotPulse: true  },
  error: { bg: t.tone.errorSoft, fg: t.tone.error, border: t.tone.error, dotPulse: false },
  muted: { bg: t.bg.s2,          fg: t.text.secondary, border: t.border.subtle, dotPulse: false },
};

interface Props {
  tone:     StatusTone;
  label:    string;
  showDot?: boolean;
}

/** Pill status badge with optional pulsing dot. Matches CDC·Migrator design.  */
export function StatusBadge({ tone, label, showDot = true }: Props) {
  const c = TONES[tone];
  return (
    <span style={{
      display:       "inline-flex",
      alignItems:    "center",
      gap:           5,
      padding:       "1px 7px",
      borderRadius:  t.radius.pill,
      fontSize:      "10.5px",
      fontWeight:    500,
      background:    c.bg,
      color:         c.fg,
      border:        `1px solid color-mix(in oklab, ${c.border} 24%, transparent)`,
      whiteSpace:    "nowrap",
    }}>
      {showDot && (
        <span aria-hidden style={{
          width:        5,
          height:       5,
          borderRadius: "50%",
          background:   c.fg,
          animation:    c.dotPulse ? "pulse 1.6s infinite" : "none",
        }}/>
      )}
      {label}
    </span>
  );
}
