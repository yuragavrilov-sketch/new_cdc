import React from "react";
import { t } from "../../theme";

export function SectionHeader({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      color: t.text.disabled, fontSize: t.size.sm, fontWeight: 700,
      textTransform: "uppercase", letterSpacing: 0.8, marginBottom: t.space[2],
    }}>
      {children}
    </div>
  );
}
