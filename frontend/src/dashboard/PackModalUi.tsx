import React from "react";
import { S } from "./PackModalStyles";

// ── Section wrapper ───────────────────────────────────────────────────────────

export function Section({
  title, accent, children,
}: {
  title: string; accent?: string; children: React.ReactNode;
}) {
  return (
    <div style={S.secWrap(accent)}>
      <div style={S.secHead(accent)}>{title}</div>
      <div style={S.secBody}>{children}</div>
    </div>
  );
}

// ── Field (label + control + hint/error) ──────────────────────────────────────

export function Field({
  label, required, error, hint, children,
}: {
  label: string; required?: boolean; error?: string; hint?: string;
  children: React.ReactNode;
}) {
  return (
    <div style={S.field}>
      <label style={S.label}>
        {label}{required && <span style={S.req}>*</span>}
      </label>
      {children}
      {hint  && !error && <div style={S.hint}>{hint}</div>}
      {error && <div style={S.err}>{error}</div>}
    </div>
  );
}

