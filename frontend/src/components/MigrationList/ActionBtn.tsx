import React from "react";
import { t } from "../../theme";

interface Props {
  icon:     string;
  title:    string;
  color:    string;
  bg:       string;
  disabled?: boolean;
  onClick:  (e: React.MouseEvent) => void;
}

export function ActionBtn({ icon, title, color, bg, disabled, onClick }: Props) {
  return (
    <button
      title={title}
      disabled={disabled}
      onClick={onClick}
      style={{
        background:   bg,
        border:       `1px solid ${color}44`,
        borderRadius: t.radius.sm,
        color,
        fontSize:     t.size.sm,
        fontWeight:   700,
        padding:      "2px 7px",
        cursor:       disabled ? "not-allowed" : "pointer",
        opacity:      disabled ? 0.4 : 1,
        lineHeight:   1.4,
      }}
    >
      {icon}
    </button>
  );
}
