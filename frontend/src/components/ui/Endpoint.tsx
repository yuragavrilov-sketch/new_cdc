import React from "react";
import { t } from "../../theme";

interface Props {
  label: "SOURCE" | "TARGET" | string;
  value: string;
}

/** Source/Target endpoint pill — small box with uppercase label + mono value. */
export function Endpoint({ label, value }: Props) {
  return (
    <span style={{
      display:       "inline-flex",
      alignItems:    "center",
      gap:           8,
      padding:       "4px 10px",
      background:    t.bg.s2,
      border:        `1px solid ${t.border.subtle}`,
      borderRadius:  t.radius.sm,
    }}>
      <span style={{
        fontSize:      "9.5px",
        fontWeight:    600,
        letterSpacing: "0.08em",
        textTransform: "uppercase",
        color:         t.text.muted,
      }}>
        {label}
      </span>
      <span style={{ fontFamily: t.font.mono, fontSize: 12 }}>
        {value}
      </span>
    </span>
  );
}
