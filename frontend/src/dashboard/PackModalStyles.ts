import React from "react";
import { t } from "../theme";

/** Style tokens used across the modal. */
export const S = {
  overlay: {
    position:       "fixed"           as const,
    inset:          0,
    background:     "rgba(0,0,0,.72)",
    display:        "flex"            as const,
    alignItems:     "flex-start"      as const,
    justifyContent: "center"          as const,
    zIndex:         1000,
    overflowY:      "auto"            as const,
    padding:        "40px 16px 60px",
  } satisfies React.CSSProperties,

  modal: {
    background:     t.bg.app,
    border:         `1px solid ${t.border.subtle}`,
    borderRadius:   t.radius.lg,
    width:          "100%",
    maxWidth:       700,
    display:        "flex"   as const,
    flexDirection:  "column" as const,
    boxShadow:      "0 24px 48px rgba(0,0,0,.55)",
  } satisfies React.CSSProperties,

  header: {
    padding:      "14px 20px",
    borderBottom: `1px solid ${t.border.subtle}`,
    display:      "flex"   as const,
    alignItems:   "center" as const,
    gap:          12,
  } satisfies React.CSSProperties,

  body: {
    padding:       20,
    display:       "flex"   as const,
    flexDirection: "column" as const,
    gap:           16,
  } satisfies React.CSSProperties,

  footer: {
    padding:        "12px 20px",
    borderTop:      `1px solid ${t.border.subtle}`,
    display:        "flex"     as const,
    justifyContent: "flex-end" as const,
    gap:            8,
  } satisfies React.CSSProperties,

  secWrap: (accent?: string): React.CSSProperties => ({
    border:       `1px solid ${accent ? accent + "50" : t.border.subtle}`,
    borderRadius: t.radius.md,
    overflow:     "hidden",
  }),

  secHead: (accent?: string): React.CSSProperties => ({
    padding:       "7px 14px",
    background:    t.bg.s1,
    borderBottom:  `1px solid ${accent ? accent + "40" : t.border.subtle}`,
    fontSize:      t.size.sm,
    fontWeight:    700,
    color:         accent ?? t.text.disabled,
    textTransform: "uppercase",
    letterSpacing: 0.8,
  }),

  secBody: {
    padding:       12,
    display:       "flex"   as const,
    flexDirection: "column" as const,
    gap:           10,
  } satisfies React.CSSProperties,

  row2: {
    display:             "grid" as const,
    gridTemplateColumns: "1fr 1fr",
    gap:                 10,
  } satisfies React.CSSProperties,

  field: {
    display:       "flex"   as const,
    flexDirection: "column" as const,
    gap:           4,
  } satisfies React.CSSProperties,

  label: {
    fontSize:      t.size.sm,
    color:         t.text.muted,
    fontWeight:    600 as const,
    letterSpacing: 0.3,
  } satisfies React.CSSProperties,

  req: { color: t.red.base, marginLeft: 2 } satisfies React.CSSProperties,

  input: {
    background:   t.bg.s2,
    border:       `1px solid ${t.border.base}`,
    borderRadius: t.radius.md,
    color:        t.text.primary,
    padding:      "7px 10px",
    fontSize:     t.size.md,
    width:        "100%",
  } satisfies React.CSSProperties,

  inputErr: {
    background:   t.bg.s2,
    border:       `1px solid ${t.red.border}`,
    borderRadius: t.radius.md,
    color:        t.text.primary,
    padding:      "7px 10px",
    fontSize:     t.size.md,
    width:        "100%",
  } satisfies React.CSSProperties,

  select: {
    background:   t.bg.s2,
    border:       `1px solid ${t.border.base}`,
    borderRadius: t.radius.md,
    color:        t.text.primary,
    padding:      "7px 10px",
    fontSize:     t.size.md,
    width:        "100%",
    cursor:       "pointer",
  } satisfies React.CSSProperties,

  selectDis: {
    background:   t.bg.app,
    border:       `1px solid ${t.border.subtle}`,
    borderRadius: t.radius.md,
    color:        t.text.faint,
    padding:      "7px 10px",
    fontSize:     t.size.md,
    width:        "100%",
    cursor:       "not-allowed",
  } satisfies React.CSSProperties,

  hint: { fontSize: t.size.xs, color: t.text.disabled } satisfies React.CSSProperties,
  err:  { fontSize: t.size.xs, color: t.red.fg } satisfies React.CSSProperties,

  textarea: {
    background:   t.bg.s2,
    border:       `1px solid ${t.border.base}`,
    borderRadius: t.radius.md,
    color:        t.text.primary,
    padding:      "7px 10px",
    fontSize:     t.size.md,
    width:        "100%",
    resize:       "vertical" as const,
    minHeight:    52,
    fontFamily:   "inherit",
  } satisfies React.CSSProperties,
} as const;
