import { useMemo } from "react";
import { t } from "../../theme";
import type { OraIndex } from "./types";
import {
  Dot, StatusPill, ActionBtn, EmptyRow, TH, TD, TR_BORDER,
} from "./helpers";

interface Props {
  src:      OraIndex[];
  tgt:      OraIndex[];
  busy:     Record<string, boolean>;
  actErr:   Record<string, string>;
  onAction: (action: string, name: string) => void;
}

export function IndexTable({ src, tgt, busy, actErr, onAction }: Props) {
  const srcNames = useMemo(() => new Set(src.map(i => i.name)), [src]);
  const tgtMap   = useMemo(() => new Map(tgt.map(i => [i.name, i])), [tgt]);
  const rows = useMemo(() => {
    const out: { srcIdx: OraIndex | null; tgtIdx: OraIndex | null }[] = [];
    for (const si of src) out.push({ srcIdx: si, tgtIdx: tgtMap.get(si.name) ?? null });
    for (const ti of tgt) if (!srcNames.has(ti.name)) out.push({ srcIdx: null, tgtIdx: ti });
    return out;
  }, [src, tgt, srcNames, tgtMap]);

  if (rows.length === 0) return <EmptyRow text="Нет индексов" />;
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ borderBottom: `1px solid ${t.border.subtle}` }}>
            {["Индекс", "Тип", "Колонки", "Есть на источнике", "Статус на таргете", "Действие"].map(h => (
              <th key={h} style={TH}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(({ srcIdx, tgtIdx }) => {
            const idx = tgtIdx ?? srcIdx!;
            return (
              <tr key={idx.name} style={TR_BORDER}>
                <td style={TD}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <code style={{ color: t.text.primary, fontSize: t.size.sm }}>{idx.name}</code>
                    {idx.unique && (
                      <span style={{
                        fontSize: t.size.xs, color: t.blue.base,
                        background: t.blue.bg, padding: "1px 5px", borderRadius: 3,
                      }}>UNIQUE</span>
                    )}
                  </div>
                </td>
                <td style={{ ...TD, color: t.text.muted, fontSize: t.size.sm }}>{idx.index_type}</td>
                <td style={{ ...TD, color: t.text.secondary, fontSize: t.size.sm, fontFamily: t.font.mono }}>
                  {idx.columns.join(", ")}
                </td>
                <td style={{ ...TD, textAlign: "center" }}>
                  {srcIdx
                    ? <Dot color={t.green.base} />
                    : <span style={{ color: t.text.disabled, fontSize: t.size.sm }}>—</span>}
                </td>
                <td style={TD}>
                  {tgtIdx
                    ? <StatusPill status={tgtIdx.status} ok="VALID" warn="UNUSABLE" />
                    : <span style={{ color: t.text.disabled, fontSize: t.size.sm }}>отсутствует</span>}
                </td>
                <td style={TD}>
                  {tgtIdx && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                      {tgtIdx.status === "VALID"
                        ? <ActionBtn label="Отключить"  onClick={() => onAction("disable_index", tgtIdx.name)} busy={busy[tgtIdx.name]} variant="danger" />
                        : <ActionBtn label="Перестроить" onClick={() => onAction("enable_index",  tgtIdx.name)} busy={busy[tgtIdx.name]} variant="success" />
                      }
                      {actErr[tgtIdx.name] && (
                        <div style={{ fontSize: t.size.xs, color: t.red.base, maxWidth: 240 }}>
                          {actErr[tgtIdx.name]}
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
