import React from "react";
import { t } from "../../theme";

type Tone = "neutral" | "info" | "success" | "warn" | "danger" | "purple";

const TONES: Record<Tone, { bg: string; fg: string; border: string }> = {
  neutral: { bg: t.bg.s2,    fg: t.text.secondary, border: t.border.base },
  info:    { bg: t.blue.bg,  fg: t.blue.fg,         border: t.blue.dim },
  success: { bg: t.green.bg, fg: t.green.fg,        border: t.green.border },
  warn:    { bg: t.amber.bg, fg: t.amber.fg,        border: t.amber.dim },
  danger:  { bg: t.red.bg,   fg: t.red.fg,          border: t.red.border },
  purple:  { bg: t.purple.bg,fg: t.purple.fg,       border: t.purple.base },
};

interface Props {
  tone?: Tone;
  size?: "sm" | "md";
  children: React.ReactNode;
  style?: React.CSSProperties;
}

export function Badge({ tone = "neutral", size = "md", children, style }: Props) {
  const c = TONES[tone];
  return (
    <span style={{
      background:    c.bg,
      color:         c.fg,
      border:        `1px solid ${c.border}`,
      borderRadius:  t.radius.sm,
      fontSize:      size === "sm" ? t.size.xs : t.size.sm,
      fontWeight:    700,
      padding:       size === "sm" ? "2px 7px" : "3px 10px",
      letterSpacing: 0.3,
      display:       "inline-block",
      whiteSpace:    "nowrap",
      ...style,
    }}>
      {children}
    </span>
  );
}
