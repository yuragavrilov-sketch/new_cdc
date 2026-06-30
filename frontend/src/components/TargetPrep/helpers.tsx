import React, { useState } from "react";
import { t } from "../../theme";
import type { ColInfo } from "./types";

// ── Helpers ──────────────────────────────────────────────────────────────────

export function fmtType(c: ColInfo): string {
  if (c.data_precision != null) {
    return c.data_scale != null && c.data_scale !== 0
      ? `${c.data_type}(${c.data_precision},${c.data_scale})`
      : `${c.data_type}(${c.data_precision})`;
  }
  const hasLen = ["VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR", "RAW"].includes(c.data_type);
  return hasLen && c.data_length != null ? `${c.data_type}(${c.data_length})` : c.data_type;
}

// ── Shared mini-components ────────────────────────────────────────────────────

export function Dot({ color }: { color: string }) {
  return (
    <span style={{
      width: 7, height: 7, borderRadius: "50%",
      background: color, display: "inline-block", flexShrink: 0,
    }} />
  );
}

export function StatusPill({ status, ok, warn }: { status: string; ok?: string; warn?: string }) {
  const color = status === ok ? t.green.base : status === warn ? t.amber.base : t.red.base;
  return (
    <span style={{
      background: color + "22", border: `1px solid ${color}55`,
      color, borderRadius: t.radius.sm, padding: "1px 7px",
      fontSize: t.size.sm, fontWeight: 600, whiteSpace: "nowrap",
    }}>
      {status}
    </span>
  );
}

export function ActionBtn({
  label, onClick, busy, variant = "danger",
}: {
  label: string; onClick: () => void; busy?: boolean; variant?: "danger" | "success";
}) {
  const c = variant === "success" ? t.green.base : t.red.base;
  return (
    <button
      onClick={onClick}
      disabled={busy}
      style={{
        background: c + "22", border: `1px solid ${c}55`,
        borderRadius: t.radius.sm, color: t.text.primary,
        padding: "3px 10px", fontSize: t.size.sm,
        cursor: busy ? "not-allowed" : "pointer",
        opacity: busy ? 0.55 : 1, fontWeight: 500, whiteSpace: "nowrap",
      }}
    >
      {busy ? "..." : label}
    </button>
  );
}

export function BulkDangerBtn({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      style={{
        background: `${t.red.border}33`, border: `1px solid ${t.red.border}88`,
        borderRadius: t.radius.sm, color: t.red.fg, padding: "3px 11px",
        fontSize: t.size.sm, cursor: "pointer", fontWeight: 500,
      }}
    >
      {label}
    </button>
  );
}

export function EmptyRow({ text }: { text: string }) {
  return (
    <div style={{
      padding: "20px 14px", textAlign: "center",
      color: t.text.disabled, fontSize: t.size.base,
    }}>
      {text}
    </div>
  );
}

// ── Collapsible Section ────────────────────────────────────────────────────────

export function Section({
  title, count, status, bulkAction, children,
}: {
  title: string; count: number; status: "ok" | "warn" | "info";
  bulkAction?: React.ReactNode; children: React.ReactNode;
}) {
  const [open, setOpen] = useState(true);
  const dotC = status === "ok" ? t.green.base : status === "warn" ? t.amber.base : t.text.muted;
  return (
    <div style={{ border: `1px solid ${t.border.subtle}`, borderRadius: t.radius.lg, overflow: "hidden" }}>
      <div
        style={{
          background: t.bg.s2, padding: "9px 14px",
          display: "flex", alignItems: "center", gap: 10,
          cursor: "pointer", userSelect: "none",
        }}
        onClick={() => setOpen(o => !o)}
      >
        <span style={{ color: t.text.disabled, fontSize: t.size.sm, width: 12 }}>{open ? "▼" : "▶"}</span>
        <span style={{ fontWeight: 600, fontSize: t.size.md, color: t.text.primary }}>{title}</span>
        <span style={{
          fontSize: t.size.sm, background: dotC + "22", color: dotC,
          padding: "1px 8px", borderRadius: 10,
        }}>{count}</span>
        {bulkAction && (
          <div style={{ marginLeft: "auto" }} onClick={e => e.stopPropagation()}>{bulkAction}</div>
        )}
      </div>
      {open && <div style={{ background: t.bg.s1 }}>{children}</div>}
    </div>
  );
}

// ── SyncObjResultBar ──────────────────────────────────────────────────────────

export function SyncObjResultBar({
  result, type,
}: {
  result: { type: string; added: string[]; skipped: string[]; errors: {name: string; error: string}[] } | null;
  type: string;
}) {
  if (!result || result.type !== type) return null;
  return (
    <div style={{
      padding: "8px 14px", borderTop: `1px solid ${t.border.subtle}`, fontSize: t.size.sm,
    }}>
      {result.added.length > 0 && (
        <div style={{ color: t.green.base, marginBottom: 3 }}>✓ Создано: {result.added.join(", ")}</div>
      )}
      {result.errors.length > 0 && (
        <div style={{ color: t.red.base, marginBottom: 3 }}>✕ Ошибки: {result.errors.map(e => `${e.name}: ${e.error}`).join("; ")}</div>
      )}
      {result.skipped.length > 0 && (
        <div style={{ color: t.text.disabled }}>— Пропущено: {result.skipped.join(", ")}</div>
      )}
      {result.added.length === 0 && result.errors.length === 0 && (
        <span style={{ color: t.text.disabled }}>Нет новых объектов</span>
      )}
    </div>
  );
}

// ── Table style constants ─────────────────────────────────────────────────────

export const TH: React.CSSProperties = {
  padding: "6px 10px", textAlign: "left",
  color: t.text.muted, fontWeight: 500, fontSize: t.size.base, whiteSpace: "nowrap",
};
export const TD: React.CSSProperties = { padding: "5px 10px", fontSize: t.size.base };
export const TR_BORDER: React.CSSProperties = { borderBottom: `1px solid ${t.bg.s2}` };

// ── DiffCell ─────────────────────────────────────────────────────────────────

export function DiffCell({ missing, disabled, comparing, compared }: {
  missing: number; disabled?: number; comparing: boolean; compared: boolean;
}) {
  if (comparing) return <span style={{ color: t.text.muted, fontSize: t.size.sm }}>…</span>;
  if (!compared) return <span style={{ color: t.text.faint }}>—</span>;
  const parts: React.ReactNode[] = [];
  if (missing > 0)         parts.push(<span key="m" style={{ color: t.red.base, fontSize: t.size.sm }}>✕ {missing}</span>);
  if ((disabled ?? 0) > 0) parts.push(<span key="d" style={{ color: t.amber.base, fontSize: t.size.sm }}>⚠ {disabled}</span>);
  if (parts.length === 0)  return <span style={{ color: t.green.base, fontSize: t.size.md }}>✓</span>;
  return <div style={{ display: "flex", flexDirection: "column", gap: 1 }}>{parts}</div>;
}
