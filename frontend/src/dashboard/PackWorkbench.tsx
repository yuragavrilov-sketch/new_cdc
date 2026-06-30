import React from "react";
import { t } from "../theme";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import type { PackKind, PackModel } from "./packModel";

export function PackWorkbench({ packs }: { packs: PackModel[] }) {
  return (
    <section style={{
      background: t.bg.s1,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding: 12,
      marginBottom: 12,
      boxShadow: t.shadow.s1,
    }}>
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        gap: 12,
        marginBottom: 10,
      }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: t.text.primary }}>Пачки</div>
      </div>
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))",
        gap: 10,
      }}>
        {packs.map(pack => <PackCard key={pack.key} pack={pack}/>)}
      </div>
    </section>
  );
}

function PackCard({ pack }: { pack: PackModel }) {
  const colors = packColors(pack.tone || "idle");
  const done = pack.done || 0;
  const failed = pack.failed || 0;
  const running = pack.running || 0;
  const progress = pack.total > 0 ? done / pack.total * 100 : 0;
  const metrics = [
    ...(pack.selected === undefined ? [] : [{ label: "выбрано", value: pack.selected }]),
    { label: "черновик", value: pack.draft },
    { label: "очередь", value: pack.queued },
    { label: "работа", value: pack.running },
    { label: "готово", value: pack.done },
    { label: "ошибки", value: pack.failed, danger: !!pack.failed },
  ];
  return (
    <div style={{
      minWidth: 0,
      display: "flex",
      flexDirection: "column",
      gap: 9,
      padding: "10px 11px",
      borderRadius: t.radius.md,
      border: `1px solid ${failed ? t.red.border : colors.border}`,
      background: failed ? `${t.red.border}18` : colors.bg,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "flex-start" }}>
        <div style={{ minWidth: 0 }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: t.text.primary }}>{pack.title}</div>
          {pack.subtitle && (
            <div style={{
              marginTop: 2,
              fontSize: 11.5,
              color: t.text.muted,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}>
              {pack.subtitle}
            </div>
          )}
        </div>
        <span style={{
          flexShrink: 0,
          minWidth: 58,
          textAlign: "center",
          padding: "3px 7px",
          borderRadius: t.radius.sm,
          border: `1px solid ${failed ? t.red.border : colors.border}`,
          background: t.bg.s1,
          color: failed ? t.red.fg : running ? t.blue.fg : colors.fg,
          fontFamily: t.font.mono,
          fontSize: 11,
          fontWeight: 700,
        }}>
          {pack.total || 0}
        </span>
      </div>

      <div style={{
        height: 5,
        borderRadius: 999,
        background: t.bg.s3,
        overflow: "hidden",
      }}>
        <div style={{
          width: `${Math.max(0, Math.min(100, progress))}%`,
          height: "100%",
          background: failed ? t.red.fg : colors.fg,
        }}/>
      </div>

      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
        gap: 6,
      }}>
        {metrics.map(metric => (
          <Metric
            key={metric.label}
            label={metric.label}
            value={metric.value}
            danger={metric.danger}
          />
        ))}
      </div>

      {pack.feedback && (
        <div style={{
          fontFamily: t.font.mono,
          fontSize: 11,
          color: t.text.muted,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}>
          {pack.feedback}
        </div>
      )}

      <div style={{ display: "flex", gap: 7, flexWrap: "wrap", marginTop: "auto" }}>
        {pack.actions.map(action => (
          <button
            key={action.label}
            onClick={action.onClick}
            disabled={action.disabled}
            title={action.disabled ? action.disabledReason : action.hint}
            style={{
              ...(action.primary ? primaryActionStyle(!!action.disabled) : secondaryActionStyle(!!action.disabled)),
              padding: "5px 9px",
              fontSize: 11.5,
            }}
          >
            {action.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function Metric({ label, value, danger = false }: { label: string; value?: number; danger?: boolean }) {
  return (
    <div style={{
      minWidth: 0,
      padding: "5px 6px",
      borderRadius: t.radius.sm,
      background: t.bg.s1,
      border: `1px solid ${t.border.subtle}`,
    }}>
      <div style={{ fontSize: 10, color: t.text.muted, whiteSpace: "nowrap" }}>{label}</div>
      <div style={{
        marginTop: 1,
        fontFamily: t.font.mono,
        fontSize: 12,
        fontWeight: 700,
        color: danger ? t.red.fg : t.text.primary,
      }}>
        {value ?? 0}
      </div>
    </div>
  );
}

function packColors(tone: PackKind | "idle") {
  if (tone === "bulk") return { bg: t.amber.bg, border: t.amber.dim, fg: t.amber.fg };
  if (tone === "cdc") return { bg: t.blue.bg, border: t.blue.dim, fg: t.blue.fg };
  if (tone === "ddl") return { bg: t.green.bg, border: t.green.dim, fg: t.green.fg };
  return { bg: t.bg.s2, border: t.border.subtle, fg: t.text.muted };
}
