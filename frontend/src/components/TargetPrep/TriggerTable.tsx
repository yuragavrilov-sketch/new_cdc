import { useMemo } from "react";
import { t } from "../../theme";
import type { Trigger } from "./types";
import {
  StatusPill, ActionBtn, EmptyRow, TH, TD, TR_BORDER,
} from "./helpers";

interface Props {
  src:      Trigger[];
  tgt:      Trigger[];
  busy:     Record<string, boolean>;
  actErr:   Record<string, string>;
  onAction: (action: string, name: string) => void;
}

export function TriggerTable({ src, tgt, busy, actErr, onAction }: Props) {
  const rows = useMemo(() => {
    const tgtMap = new Map(tgt.map(t => [t.name, t]));
    const srcMap = new Map(src.map(t => [t.name, t]));
    const out: { srcT: Trigger | null; tgtT: Trigger | null }[] = [];
    for (const st of src) out.push({ srcT: st, tgtT: tgtMap.get(st.name) ?? null });
    for (const tt of tgt) if (!srcMap.has(tt.name)) out.push({ srcT: null, tgtT: tt });
    return out;
  }, [src, tgt]);

  if (rows.length === 0) return <EmptyRow text="Нет триггеров" />;
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${t.border.subtle}` }}>
            {["Триггер", "Тип", "Событие", "На источнике", "На таргете (статус)", "Действие"].map(h => (
              <th key={h} style={TH}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(({ srcT, tgtT }) => {
            const trg      = tgtT ?? srcT!;
            const srcEvent = srcT ? `${srcT.trigger_type} / ${srcT.event}` : null;
            const tgtEvent = tgtT ? `${tgtT.trigger_type} / ${tgtT.event}` : null;
            const eventDiff = srcEvent && tgtEvent && srcEvent !== tgtEvent;
            return (
              <tr key={trg.name} style={TR_BORDER}>
                <td style={TD}>
                  <code style={{ color: t.text.primary, fontSize: t.size.sm }}>{trg.name}</code>
                </td>
                <td style={{ ...TD, color: t.text.muted, fontSize: t.size.sm }}>{trg.trigger_type}</td>
                <td style={{
                  ...TD,
                  color: eventDiff ? t.amber.base : t.text.secondary,
                  fontSize: t.size.sm,
                }}>
                  {tgtEvent ?? srcEvent}
                  {eventDiff && (
                    <div style={{ fontSize: t.size.xs, color: t.amber.base }}>src: {srcEvent}</div>
                  )}
                </td>
                <td style={TD}>
                  {srcT
                    ? <StatusPill status={srcT.status} ok="ENABLED" warn="DISABLED" />
                    : <span style={{ color: t.text.disabled, fontSize: t.size.sm }}>—</span>}
                </td>
                <td style={TD}>
                  {tgtT
                    ? <StatusPill status={tgtT.status} ok="ENABLED" warn="DISABLED" />
                    : <span style={{ color: t.text.disabled, fontSize: t.size.sm }}>отсутствует</span>}
                </td>
                <td style={TD}>
                  {tgtT && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                      {tgtT.status === "ENABLED"
                        ? <ActionBtn label="Отключить" onClick={() => onAction("disable_trigger", tgtT.name)} busy={busy[tgtT.name]} variant="danger" />
                        : <ActionBtn label="Включить"  onClick={() => onAction("enable_trigger",  tgtT.name)} busy={busy[tgtT.name]} variant="success" />
                      }
                      {actErr[tgtT.name] && (
                        <div style={{ fontSize: t.size.xs, color: t.red.base, maxWidth: 240 }}>
                          {actErr[tgtT.name]}
                        </div>
                      )}
                    </div>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
