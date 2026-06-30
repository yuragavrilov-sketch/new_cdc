import { phaseColor } from "../types/migration";
import { t } from "../theme";

interface Props {
  phase: string;
  size?: "sm" | "md";
}

export function PhaseBadge({ phase, size = "md" }: Props) {
  const c = phaseColor(phase);
  return (
    <span style={{
      background:    c.bg,
      color:         c.text,
      border:        `1px solid ${c.border}`,
      borderRadius:  t.radius.sm,
      fontSize:      size === "sm" ? t.size.xs : t.size.sm,
      fontWeight:    700,
      padding:       size === "sm" ? "2px 7px" : "3px 10px",
      letterSpacing: 0.3,
      display:       "inline-block",
      whiteSpace:    "nowrap",
    }}>
      {phase}
    </span>
  );
}
