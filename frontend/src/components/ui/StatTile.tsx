import React from "react";
import { t } from "../../theme";

interface Props {
  label:  string;
  value:  React.ReactNode;
  sub?:   React.ReactNode;
  color?: string;
}

export function StatTile({ label, value, sub, color }: Props) {
  return (
    <div
      style={{
        background:   t.bg.s1,
        border:       `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.md,
        padding:      `${t.space[3]} ${t.space[4]}`,
        minWidth:     0,
      }}
    >
      <div style={{ fontSize: t.size.sm, color: t.text.disabled, marginBottom: t.space[1] }}>
        {label}
      </div>
      <div
        style={{
          fontSize:    t.size.xl,
          fontWeight:  800,
          color:       color ?? t.text.primary,
          fontVariantNumeric: "tabular-nums",
          lineHeight:  1,
        }}
      >
        {value}
      </div>
      {sub != null && (
        <div style={{ fontSize: t.size.xs, color: t.text.faint, marginTop: 3 }}>
          {sub}
        </div>
      )}
    </div>
  );
}
