import { useMemo } from "react";
import { t } from "../../theme";
import type { Constraint } from "./types";
import {
  Dot, StatusPill, ActionBtn, EmptyRow, TH, TD, TR_BORDER,
} from "./helpers";

const CTYPE_COLOR: Record<string, string> = {
  "PRIMARY KEY": t.blue.base,
  "UNIQUE":      t.purple.base,
  "FOREIGN KEY": t.amber.base,
  "CHECK":       t.text.muted,
};

interface Props {
  src:      Constraint[];
  tgt:      Constraint[];
  busy:     Record<string, boolean>;
  actErr:   Record<string, string>;
  onAction: (action: string, name: string) => void;
}

export function ConstraintTable({ src, tgt, busy, actErr, onAction }: Props) {
  const rows = useMemo(() => {
    const out: { srcC: Constraint | null; tgtC: Constraint | null; nameMatch: boolean }[] = [];
    const tgtUsed = new Set<string>(); const srcUsed = new Set<string>();
    for (const sc of src) {
      const tc = tgt.find(t => t.name === sc.name);
      if (tc) { out.push({ srcC: sc, tgtC: tc, nameMatch: true }); tgtUsed.add(tc.name); srcUsed.add(sc.name); }
    }
    for (const sc of src.filter(c => !srcUsed.has(c.name))) {
      const colKey = sc.columns.join(",");
      const tc = tgt.find(t => !tgtUsed.has(t.name) && t.type_code === sc.type_code && t.columns.join(",") === colKey);
      if (tc) { out.push({ srcC: sc, tgtC: tc, nameMatch: false }); tgtUsed.add(tc.name); srcUsed.add(sc.name); }
      else out.push({ srcC: sc, tgtC: null, nameMatch: false });
    }
    for (const tc of tgt.filter(c => !tgtUsed.has(c.name))) out.push({ srcC: null, tgtC: tc, nameMatch: false });
    return out;
  }, [src, tgt]);

  if (rows.length === 0) return <EmptyRow text="Нет ограничений" />;
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${t.border.subtle}` }}>
            {["Имя (таргет)", "Имя (источник)", "Тип", "Колонки", "Есть на источнике", "Статус на таргете", "Действие"].map(h => (
              <th key={h} style={TH}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(({ srcC, tgtC, nameMatch }) => {
            const c = tgtC ?? srcC!;
            const isPK = c.type_code === "P";
            const typeC = CTYPE_COLOR[c.type] ?? t.text.muted;
            const namesDiffer = !nameMatch && srcC && tgtC && srcC.name !== tgtC.name;
            return (
              <tr key={c.name} style={TR_BORDER}>
                <td style={TD}>
                  <code style={{ color: t.text.primary, fontSize: t.size.sm }}>{tgtC?.name ?? "—"}</code>
                </td>
                <td style={TD}>
                  {srcC
                    ? <code style={{ color: namesDiffer ? t.amber.base : t.text.muted, fontSize: t.size.sm }}>{srcC.name}</code>
                    : <span style={{ color: t.text.disabled, fontSize: t.size.sm }}>—</span>}
                </td>
                <td style={TD}>
                  <span style={{
                    background: typeC + "22", color: typeC,
                    borderRadius: t.radius.sm, padding: "1px 6px",
                    fontSize: t.size.xs, fontWeight: 600,
                  }}>{c.type}</span>
                </td>
                <td style={{ ...TD, color: t.text.secondary, fontSize: t.size.sm, fontFamily: t.font.mono }}>
                  {c.columns.join(", ") || "—"}
                </td>
                <td style={{ ...TD, textAlign: "center" }}>
                  {srcC
                    ? <Dot color={t.green.base} />
                    : <span style={{ color: t.text.disabled, fontSize: t.size.sm }}>—</span>}
                </td>
                <td style={TD}>
                  {tgtC
                    ? <StatusPill status={tgtC.status} ok="ENABLED" warn="DISABLED" />
                    : <span style={{ color: t.text.disabled, fontSize: t.size.sm }}>отсутствует</span>}
                </td>
                <td style={TD}>
                  {tgtC && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                      {isPK
                        ? <span style={{ fontSize: t.size.sm, color: t.text.disabled }}>PRIMARY KEY — нельзя отключить</span>
                        : tgtC.status === "ENABLED"
                          ? <ActionBtn label="Отключить" onClick={() => onAction("disable_constraint", tgtC.name)} busy={busy[tgtC.name]} variant="danger" />
                          : <ActionBtn label="Включить"  onClick={() => onAction("enable_constraint",  tgtC.name)} busy={busy[tgtC.name]} variant="success" />
                      }
                      {actErr[tgtC.name] && (
                        <div style={{ fontSize: t.size.xs, color: t.red.base, maxWidth: 240 }}>
                          {actErr[tgtC.name]}
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
