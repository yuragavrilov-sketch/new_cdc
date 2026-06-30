import React from "react";
import { t } from "../theme";

interface Props {
  checked: boolean;
  indeterminate?: boolean;
  disabled?: boolean;
  onChange: () => void;
  ariaLabel: string;
  title?: string;
}

export function VisibleCheckbox({
  checked,
  indeterminate = false,
  disabled = false,
  onChange,
  ariaLabel,
  title,
}: Props) {
  const ref = React.useRef<HTMLInputElement | null>(null);

  React.useEffect(() => {
    if (ref.current) ref.current.indeterminate = indeterminate;
  }, [indeterminate]);

  const active = checked || indeterminate;

  return (
    <label
      title={title}
      onClick={e => e.stopPropagation()}
      style={{
        position: "relative",
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        width: 18,
        height: 18,
        minWidth: 18,
        borderRadius: 4,
        border: `1px solid ${active ? t.tone.info : t.border.strong}`,
        background: disabled
          ? t.bg.s2
          : active
            ? t.tone.info
            : t.bg.s1,
        color: t.text.inverse,
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
        boxShadow: active ? `0 0 0 2px ${t.tone.infoSoft}` : "none",
        userSelect: "none",
      }}
    >
      <input
        ref={ref}
        type="checkbox"
        checked={checked}
        aria-label={ariaLabel}
        title={title}
        disabled={disabled}
        onChange={e => {
          e.stopPropagation();
          if (!disabled) onChange();
        }}
        style={{
          position: "absolute",
          inset: 0,
          width: "100%",
          height: "100%",
          margin: 0,
          opacity: 0,
          cursor: disabled ? "not-allowed" : "pointer",
        }}
      />
      <span
        aria-hidden="true"
        style={{
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          width: "100%",
          height: "100%",
          fontSize: indeterminate ? 14 : 13,
          lineHeight: 1,
          fontWeight: 800,
          color: t.text.inverse,
          transform: indeterminate ? "translateY(-1px)" : "none",
        }}
      >
        {indeterminate ? "-" : checked ? "✓" : ""}
      </span>
    </label>
  );
}
