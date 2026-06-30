import type { MigrationSummary } from "../../types/migration";
import { phaseColor } from "../../types/migration";
import { t } from "../../theme";
import type { GroupTable, TopicCount } from "./types";

interface Props {
  tables:           GroupTable[];
  topicCounts:      Map<string, TopicCount>;
  tableMigrationMap: Map<string, MigrationSummary>;
  onRemove:         (table: GroupTable) => void;
}

const KEY_BG: Record<string, string> = {
  PRIMARY_KEY:  t.green.bg,
  UNIQUE_KEY:   t.purple.bg,
  USER_DEFINED: t.bg.s3,
};
const KEY_FG: Record<string, string> = {
  PRIMARY_KEY:  t.green.fg,
  UNIQUE_KEY:   t.purple.fg,
  USER_DEFINED: t.blue.fg,
};

export function GroupTablesTable({
  tables, topicCounts, tableMigrationMap, onRemove,
}: Props) {
  if (tables.length === 0) {
    return <div style={{ color: t.text.disabled, fontSize: t.size.sm }}>Нет таблиц в CDC-пачке</div>;
  }

  return (
    <table style={{ width: "100%", fontSize: t.size.sm, borderCollapse: "collapse" }}>
      <thead>
        <tr style={{ color: t.text.disabled, textAlign: "left" }}>
          <th style={{ padding: "4px 8px" }}>Таблица</th>
          <th style={{ padding: "4px 8px" }}>Target</th>
          <th style={{ padding: "4px 8px" }}>Ключ</th>
          <th style={{ padding: "4px 8px" }}>Топик</th>
          <th style={{ padding: "4px 8px", textAlign: "right" }}>Сообщений</th>
          <th style={{ padding: "4px 8px", textAlign: "center" }}>Миграция</th>
          <th style={{ padding: "4px 8px", textAlign: "right" }}></th>
        </tr>
      </thead>
      <tbody>
        {tables.map(tbl => {
          const tc        = topicCounts.get(tbl.topic_name);
          const tableKey  = `${tbl.source_schema}.${tbl.source_table}`;
          const mig       = tableMigrationMap.get(tableKey);
          const isTerminal = mig && (mig.phase === "CANCELLED" || mig.phase === "FAILED" || mig.phase === "COMPLETED");
          const hasActive = mig && !isTerminal;
          return (
            <tr key={tbl.id} style={{ borderTop: `1px solid ${t.border.subtle}` }}>
              <td style={{ padding: "4px 8px", color: t.text.primary, fontFamily: t.font.mono }}>
                {tbl.source_schema}.{tbl.source_table}
              </td>
              <td style={{ padding: "4px 8px", color: t.text.muted, fontFamily: t.font.mono }}>
                {tbl.target_schema}.{tbl.target_table}
              </td>
              <td style={{ padding: "4px 8px" }}>
                <span style={{
                  fontSize: 9, padding: "1px 5px", borderRadius: 3,
                  background: KEY_BG[tbl.effective_key_type] ?? t.bg.s2,
                  color:      KEY_FG[tbl.effective_key_type] ?? t.text.muted,
                }}>
                  {tbl.effective_key_type}
                </span>
              </td>
              <td style={{
                padding: "4px 8px", color: t.text.disabled,
                fontSize: t.size.xs, fontFamily: t.font.mono,
              }}>
                {tbl.topic_name}
              </td>
              <td style={{ padding: "4px 8px", textAlign: "right" }}>
                {tc === undefined ? (
                  <span style={{ color: t.text.faint }}>—</span>
                ) : !tc.exists ? (
                  <span style={{ color: t.red.fg, fontSize: 9 }}>no topic</span>
                ) : (
                  <span style={{
                    color: tc.count > 0 ? t.green.fg : t.text.muted,
                    fontWeight: tc.count > 0 ? 700 : 400,
                    fontFamily: t.font.mono,
                  }}>
                    {tc.count.toLocaleString()}
                  </span>
                )}
              </td>
              <td style={{ padding: "4px 8px", textAlign: "center" }}>
                {hasActive ? (() => {
                  const pc = phaseColor(mig!.phase);
                  return (
                    <span style={{
                      fontSize: 9, padding: "1px 6px", borderRadius: 3,
                      background: pc.bg, color: pc.text,
                      border: `1px solid ${pc.border}`,
                      fontWeight: 600,
                    }}>
                      {mig!.phase}
                    </span>
                  );
                })() : (
                  <span
                    style={{
                      display: "inline-block",
                      background: t.bg.s2, border: `1px solid ${t.border.subtle}`,
                      borderRadius: t.radius.sm, color: t.green.fg,
                      padding: "1px 8px", fontSize: t.size.xs,
                      fontWeight: 600,
                    }}
                  >
                    в пачке
                  </span>
                )}
              </td>
              <td style={{ padding: "4px 8px", textAlign: "right" }}>
                <button
                  onClick={() => onRemove(tbl)}
                  disabled={!!hasActive}
                  title={hasActive ? "Нельзя убрать таблицу с активной строкой очереди" : "Убрать из CDC-пачки"}
                  style={{
                    background: t.red.bg,
                    border: `1px solid ${t.red.dim}`,
                    borderRadius: t.radius.sm,
                    color: t.red.fg,
                    padding: "1px 7px",
                    fontSize: t.size.xs,
                    cursor: hasActive ? "default" : "pointer",
                    opacity: hasActive ? 0.4 : 1,
                    fontWeight: 600,
                  }}
                >
                  Убрать
                </button>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
