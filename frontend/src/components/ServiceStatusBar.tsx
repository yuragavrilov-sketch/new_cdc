import type { ServiceStatuses, ServiceAvailability } from "../hooks/useSSE";
import { t } from "../theme";
import { fmtTs } from "../utils/format";

interface Props {
  statuses: ServiceStatuses;
}

const LABELS: Record<string, string> = {
  oracle_source: "Oracle Source",
  oracle_target: "Oracle Target",
  kafka:         "Kafka",
  kafka_connect: "Connect",
};

const COLORS: Record<ServiceAvailability, { bg: string; border: string; text: string; dot: string; dim: string }> = {
  up:      { bg: t.green.bg, border: t.green.border, text: t.green.fg,        dot: t.green.base,    dim: `color-mix(in oklab, ${t.green.fg} 53%, transparent)` },
  down:    { bg: t.red.bg,   border: t.red.border,   text: t.red.fg,          dot: t.red.base,      dim: `${t.red.fg}88` },
  unknown: { bg: t.bg.s2,    border: t.border.base,  text: t.text.secondary,  dot: t.text.disabled, dim: `${t.text.disabled}88` },
};

export function ServiceStatusBar({ statuses }: Props) {
  const services = ["oracle_source", "oracle_target", "kafka", "kafka_connect"] as const;

  return (
    <div style={{ display: "flex", gap: t.space[2], marginBottom: t.space[4], flexWrap: "wrap" }}>
      {services.map((svc) => {
        const { status, message, checked_at } = statuses[svc];
        const c = COLORS[status];
        return (
          <div
            key={svc}
            title={message}
            style={{
              display:       "flex",
              flexDirection: "column",
              gap:           2,
              background:    c.bg,
              color:         c.text,
              border:        `1px solid ${c.border}`,
              borderRadius:  t.radius.md,
              padding:       "6px 12px",
              fontSize:      t.size.base,
              fontWeight:    600,
              cursor:        "default",
              userSelect:    "none",
              minWidth:      120,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
              <span style={{
                width: 7, height: 7, borderRadius: "50%", background: c.dot,
                display: "inline-block", flexShrink: 0,
                animation: status === "up" ? "pulse 2s infinite" : undefined,
              }} />
              <span>{LABELS[svc]}</span>
              <span style={{ fontWeight: 400, opacity: 0.8, marginLeft: "auto" }}>
                {status === "up" ? "OK" : status === "down" ? "DOWN" : "—"}
              </span>
            </div>
            <div style={{ fontSize: t.size.xs, color: c.dim, paddingLeft: 14, fontWeight: 400 }}>
              {checked_at ? fmtTs(checked_at, { timeOnly: true, withSeconds: true }) : "не проверялось"}
            </div>
          </div>
        );
      })}
    </div>
  );
}
