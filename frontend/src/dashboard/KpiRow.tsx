import React from "react";
import { t } from "../theme";
import { Sparkline } from "../components/ui";

interface KpiCardProps {
  label:   string;
  value:   React.ReactNode;
  sub?:    string;
  spark?:  number[];
  tone?:   "info" | "ok" | "warn" | "error";
  delta?:  number;
  mono?:   boolean;
}

export function KpiCard({ label, value, sub, spark, tone = "info", delta, mono = true }: KpiCardProps) {
  const sparkColor =
    tone === "info"  ? t.tone.info :
    tone === "ok"    ? t.tone.ok   :
    tone === "warn"  ? t.tone.warn :
                       t.tone.error;
  return (
    <div style={{
      background:   t.bg.s1,
      border:       `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding:      14,
      boxShadow:    t.shadow.s1,
      display:      "flex",
      flexDirection:"column",
      gap:          4,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span style={{
          fontSize: "10.5px", color: t.text.muted,
          textTransform: "uppercase", letterSpacing: "0.05em",
          fontWeight: 500,
        }}>
          {label}
        </span>
        {delta != null && (
          <span style={{
            fontSize: "10.5px", fontWeight: 600,
            fontFamily: t.font.mono,
            padding: "1px 5px", borderRadius: 4,
            background: delta >= 0 ? t.tone.okSoft : t.tone.errorSoft,
            color:      delta >= 0 ? t.tone.ok     : t.tone.error,
          }}>
            {delta >= 0 ? "+" : ""}{delta}%
          </span>
        )}
      </div>
      <div style={{
        fontSize: 24, fontWeight: 600,
        letterSpacing: "-0.025em",
        lineHeight: 1.1,
        fontFamily: mono ? t.font.mono : undefined,
      }}>
        {value}
      </div>
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "flex-end",
        gap: 8, marginTop: "auto", paddingTop: 4,
      }}>
        <span style={{ fontSize: 11, color: t.text.muted }}>{sub}</span>
        {spark && <Sparkline data={spark} color={sparkColor}/>}
      </div>
    </div>
  );
}

interface RowProps {
  children: React.ReactNode;
}

export function KpiRow({ children }: RowProps) {
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "repeat(5, minmax(0, 1fr))",
      gap: 12,
      marginBottom: 15,
    }}>
      {children}
    </div>
  );
}
