/** Shared style for the primary action buttons in the dashboard
 *  (apply-DDL, sync-all, dialog submit, drawer header).
 *
 *  Matches the existing `ActionBtn primary` pattern used elsewhere
 *  (SchemaHeader / ObjectDrawer): dark t.text.primary background with
 *  light t.text.inverse text — calm, mono-tone, doesn't fight the
 *  surrounding chrome.
 *
 *  Destructive variant swaps to t.tone.error (still a theme token, not
 *  a hardcoded #dc2626). No coloured drop-shadows.
 */
import type React from "react";
import { t } from "../theme";

export function primaryActionStyle(
  busy: boolean,
  destructive = false,
): React.CSSProperties {
  if (busy) {
    return {
      display: "inline-flex", alignItems: "center", gap: 6,
      padding: "6px 12px",
      borderRadius: t.radius.sm,
      fontSize: 12.5, fontWeight: 600,
      cursor: "default",
      background: t.bg.s2,
      color:      t.text.muted,
      border:     `1px solid ${t.border.subtle}`,
      opacity:    0.7,
    };
  }
  if (destructive) {
    return {
      display: "inline-flex", alignItems: "center", gap: 6,
      padding: "6px 12px",
      borderRadius: t.radius.sm,
      fontSize: 12.5, fontWeight: 600,
      cursor: "pointer",
      background: "#b04823",
      color:      "#fff",
      border:     "1px solid #7e3018",
      boxShadow:  "0 1px 2px rgba(20,20,20,0.18)",
    };
  }
  return {
    display: "inline-flex", alignItems: "center", gap: 6,
    padding: "6px 12px",
    borderRadius: t.radius.sm,
    fontSize: 12.5, fontWeight: 600,
    cursor: "pointer",
    background: t.text.primary,
    color:      t.text.inverse,
    border:     `1px solid ${t.text.primary}`,
    boxShadow:  "0 1px 2px rgba(20,20,20,0.12)",
  };
}

/** Cancel / secondary button — neutral surface, default fg. */
export function secondaryActionStyle(busy = false): React.CSSProperties {
  return {
    display: "inline-flex", alignItems: "center", gap: 6,
    padding: "6px 12px",
    borderRadius: t.radius.sm,
    fontSize: 12.5, fontWeight: 500,
    cursor: busy ? "default" : "pointer",
    background: t.bg.s2,
    color:      t.text.primary,
    border:     `1px solid ${t.border.subtle}`,
    opacity:    busy ? 0.7 : 1,
  };
}
