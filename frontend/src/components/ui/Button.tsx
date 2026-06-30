import React from "react";
import { t } from "../../theme";

type Variant = "primary" | "secondary" | "ghost" | "danger" | "success";
type Size    = "sm" | "md";

interface Props extends Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, "children"> {
  variant?: Variant;
  size?:    Size;
  children: React.ReactNode;
}

const STYLES: Record<Variant, { bg: string; color: string; border: string }> = {
  primary:   { bg: t.blue.dim, color: t.text.inverse,   border: t.blue.dim },
  secondary: { bg: t.bg.s2,    color: t.text.secondary, border: t.border.base },
  ghost:     { bg: "transparent", color: t.text.muted, border: t.border.subtle },
  danger:    { bg: t.red.bg,   color: t.red.fg,         border: t.red.border },
  success:   { bg: t.green.bg, color: t.green.fg,       border: t.green.border },
};

const SIZES: Record<Size, React.CSSProperties> = {
  sm: { padding: `3px 8px`,  fontSize: t.size.sm,   fontWeight: 600 },
  md: { padding: `5px 12px`, fontSize: t.size.base, fontWeight: 700 },
};

export function Button({
  variant = "secondary", size = "md", disabled, style, children, ...rest
}: Props) {
  const s = STYLES[variant];
  return (
    <button
      disabled={disabled}
      style={{
        background:   s.bg,
        color:        s.color,
        border:       `1px solid ${s.border}`,
        borderRadius: t.radius.md,
        cursor:       disabled ? "not-allowed" : "pointer",
        opacity:      disabled ? 0.5 : 1,
        whiteSpace:   "nowrap",
        ...SIZES[size],
        ...style,
      }}
      {...rest}
    >
      {children}
    </button>
  );
}
