import React from "react";
import { t } from "../../theme";

interface Props {
  title?:    React.ReactNode;
  accent?:   string;          // e.g. t.blue.base — colours the header & border
  right?:    React.ReactNode; // top-right slot (refresh button etc.)
  children:  React.ReactNode;
  style?:    React.CSSProperties;
}

/**
 * Bordered panel with optional accent-coloured header.
 * Replaces ad-hoc PanelWrap definitions across feature components.
 */
export function Panel({ title, accent, right, children, style }: Props) {
  const borderColor = accent ? `${accent}40` : t.border.subtle;
  return (
    <div
      style={{
        border:       `1px solid ${borderColor}`,
        borderRadius: t.radius.md,
        overflow:     "hidden",
        background:   t.bg.s1,
        ...style,
      }}
    >
      {title && (
        <div
          style={{
            padding:        `${t.space[2]} ${t.space[3]}`,
            background:     t.bg.s1,
            borderBottom:   `1px solid ${accent ? `${accent}30` : t.border.subtle}`,
            fontSize:       t.size.sm,
            fontWeight:     700,
            color:          accent ?? t.text.secondary,
            textTransform:  "uppercase",
            letterSpacing:  0.8,
            display:        "flex",
            alignItems:     "center",
            justifyContent: "space-between",
            gap:            t.space[2],
          }}
        >
          <span>{title}</span>
          {right}
        </div>
      )}
      <div style={{ padding: `${t.space[3]} ${t.space[3]}`, background: t.bg.deep }}>
        {children}
      </div>
    </div>
  );
}
