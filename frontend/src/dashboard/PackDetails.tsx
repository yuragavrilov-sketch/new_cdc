import React from "react";
import { t } from "../theme";
import { secondaryActionStyle } from "./buttonStyles";

export function PackDetailSection({
  title,
  countText,
  loading = false,
  empty = false,
  emptyText,
  error,
  action,
  children,
}: {
  title: string;
  countText: string;
  loading?: boolean;
  empty?: boolean;
  emptyText: string;
  error?: string;
  action?: { label: string; onClick?: () => void; disabled?: boolean };
  children: React.ReactNode;
}) {
  const content = React.Children.toArray(children);
  const showContent = content.length > 0 && (!loading || !empty);
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        gap: 10,
        padding: "4px 2px",
      }}>
        <div style={{ display: "flex", gap: 8, alignItems: "baseline", minWidth: 0, flexWrap: "wrap" }}>
          <span style={{ fontSize: 12, fontWeight: 700, color: t.text.primary }}>{title}</span>
          <span style={{ fontSize: 11, color: t.text.muted }}>{countText}</span>
        </div>
        {action && (
          <button
            onClick={action.onClick}
            disabled={action.disabled}
            style={{
              ...secondaryActionStyle(!!action.disabled),
              padding: "3px 8px",
              fontSize: 11,
            }}
          >
            {action.label}
          </button>
        )}
      </div>
      {error && (
        <div style={{
          padding: "7px 10px",
          borderRadius: t.radius.sm,
          background: `${t.red.border}22`,
          border: `1px solid ${t.red.border}`,
          color: t.red.fg,
          fontSize: 12,
        }}>
          {error}
        </div>
      )}
      {loading && empty ? (
        <div style={{ fontSize: 12, color: t.text.muted, padding: "4px 2px" }}>
          Загрузка...
        </div>
      ) : empty ? (
        <div style={{ fontSize: 12, color: t.text.muted, padding: "4px 2px" }}>
          {emptyText}
        </div>
      ) : null}
      {showContent && content}
    </div>
  );
}

export function PackItemGroup({
  title,
  countText,
  children,
}: {
  title: string;
  countText: string;
  children: React.ReactNode;
}) {
  return (
    <div style={{
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.md,
      overflow: "hidden",
      background: t.bg.s2,
    }}>
      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        padding: "7px 10px",
        borderBottom: `1px solid ${t.border.subtle}`,
      }}>
        <span style={{ fontSize: 12, fontWeight: 700, color: t.text.primary }}>{title}</span>
        <span style={{ fontSize: 11, color: t.text.muted }}>{countText}</span>
      </div>
      {children}
    </div>
  );
}
