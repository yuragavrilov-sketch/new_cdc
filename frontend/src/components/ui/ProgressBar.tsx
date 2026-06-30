import React from "react";
import { t } from "../../theme";

export type Tone = "info" | "ok" | "warn" | "error" | "muted";

const FILL: Record<Tone, string> = {
  info:  t.tone.info,
  ok:    t.tone.ok,
  warn:  t.tone.warn,
  error: t.tone.error,
  muted: t.text.faint,
};

interface Props {
  value:    number;          // 0–100
  tone?:    Tone;
  height?:  number;          // px, defaults 5
  segments?: { v: number; tone: Tone }[];
  style?:   React.CSSProperties;
}

export function ProgressBar({ value, tone = "info", height = 5, segments, style }: Props) {
  const trackStyle: React.CSSProperties = {
    background:   t.bg.s3,
    borderRadius: t.radius.pill,
    height,
    overflow:     "hidden",
    display:      "flex",
    flex:         1,
    minWidth:     0,
    ...style,
  };
  if (segments) {
    const total = segments.reduce((a, s) => a + s.v, 0) || 1;
    return (
      <div style={trackStyle}>
        {segments.map((s, i) => (
          <div key={i} style={{
            background: FILL[s.tone],
            width:      `${(s.v / total) * 100}%`,
            height:     "100%",
            transition: "width 300ms ease-out",
          }}/>
        ))}
      </div>
    );
  }
  return (
    <div style={trackStyle}>
      <div style={{
        background: FILL[tone],
        width:      `${Math.max(0, Math.min(100, value))}%`,
        height:     "100%",
        transition: "width 300ms ease-out",
      }}/>
    </div>
  );
}
