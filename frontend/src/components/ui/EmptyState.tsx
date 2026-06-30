import React from "react";
import { t } from "../../theme";

interface Props {
  icon?:        React.ReactNode;
  title:        string;
  description?: string;
}

export function EmptyState({ icon = "⇄", title, description }: Props) {
  return (
    <div style={{
      display: "flex", flexDirection: "column", alignItems: "center",
      justifyContent: "center", padding: `${t.space[8]} ${t.space[6]}`,
      color: t.text.faint,
    }}>
      <div style={{ fontSize: 40, marginBottom: t.space[3], opacity: 0.4 }}>{icon}</div>
      <div style={{ fontSize: t.size.lg, fontWeight: 600, color: t.text.disabled }}>
        {title}
      </div>
      {description && (
        <div style={{ fontSize: t.size.base, marginTop: 6, color: t.text.faint }}>
          {description}
        </div>
      )}
    </div>
  );
}
