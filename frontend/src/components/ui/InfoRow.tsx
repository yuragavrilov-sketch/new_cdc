import React from "react";
import { t } from "../../theme";

/** Row for definition lists: label on the left, value on the right (or via grid display:contents). */
export function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{ display: "contents" }}>
      <span style={{ color: t.text.muted, fontSize: t.size.base }}>{label}</span>
      <span style={{ color: t.text.primary, fontSize: t.size.base, wordBreak: "break-all" }}>
        {value ?? <span style={{ color: t.text.faint }}>—</span>}
      </span>
    </div>
  );
}

/** Single-row label/value pair (flex layout, used inside Panel). */
export function Row({
  label, value, mono,
}: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div style={{
      display: "flex", justifyContent: "space-between", alignItems: "center",
      fontSize: t.size.base, marginBottom: 5,
    }}>
      <span style={{ color: t.text.muted }}>{label}</span>
      <span style={{
        color: t.text.primary,
        fontFamily: mono ? t.font.mono : undefined,
      }}>
        {value}
      </span>
    </div>
  );
}
