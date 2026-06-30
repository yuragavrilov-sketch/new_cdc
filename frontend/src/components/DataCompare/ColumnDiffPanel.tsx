import { useEffect, useState } from "react";
import { t } from "../../theme";
import type { ColDiff } from "./types";

interface Props { taskId: string }

export function ColumnDiffPanel({ taskId }: Props) {
  const [cols,    setCols]    = useState<ColDiff[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState<string | null>(null);
  const [showAll, setShowAll] = useState(false);

  useEffect(() => {
    fetch(`/api/data-compare/column-diff/${taskId}`)
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d.error || "error")))
      .then(d => setCols(d.columns))
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, [taskId]);

  if (loading) return <div style={{ padding: "8px 10px", fontSize: t.size.sm, color: t.text.secondary }}>Загрузка поколоночного сравнения...</div>;
  if (error)   return <div style={{ padding: "8px 10px", fontSize: t.size.sm, color: t.red.fg }}>{error}</div>;
  if (!cols || cols.length === 0) return <div style={{ padding: "8px 10px", fontSize: t.size.sm, color: t.text.disabled }}>Нет колонок для сравнения</div>;

  const mismatched = cols.filter(c => !c.match);
  const displayed  = showAll ? cols : mismatched;

  return (
    <div style={{ padding: "8px 10px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
        <span style={{ fontSize: t.size.sm, color: t.text.secondary }}>
          Различия в {mismatched.length} из {cols.length} колонок
        </span>
        <label style={{
          fontSize: t.size.sm, color: t.text.muted, cursor: "pointer",
          display: "flex", alignItems: "center", gap: 4,
        }}>
          <input
            type="checkbox" checked={showAll}
            onChange={e => setShowAll(e.target.checked)}
            style={{ accentColor: t.blue.base }}
          />
          Показать все
        </label>
      </div>
      <table style={{ borderCollapse: "collapse", fontSize: t.size.sm, width: "100%" }}>
        <thead>
          <tr>
            {["Колонка", "Тип", "Source hash", "Target hash", ""].map(h => (
              <th key={h} style={{
                padding: "4px 8px", textAlign: "left",
                color: t.text.muted, fontWeight: 500,
                borderBottom: `1px solid ${t.border.subtle}`,
              }}>
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayed.map(c => (
            <tr key={c.column} style={{ background: c.match ? "transparent" : t.bg.s2 }}>
              <td style={{
                padding: "3px 8px",
                color: c.match ? t.text.secondary : t.red.fg,
                fontWeight: c.match ? 400 : 600,
              }}>{c.column}</td>
              <td style={{ padding: "3px 8px", color: t.text.muted }}>{c.data_type}</td>
              <td style={{ padding: "3px 8px", color: t.text.secondary, fontVariantNumeric: "tabular-nums" }}>
                {c.source_hash ?? "NULL"}
              </td>
              <td style={{ padding: "3px 8px", color: t.text.secondary, fontVariantNumeric: "tabular-nums" }}>
                {c.target_hash ?? "NULL"}
              </td>
              <td style={{ padding: "3px 8px" }}>
                {c.match
                  ? <span style={{ color: t.green.base }}>OK</span>
                  : <span style={{ color: t.red.base, fontWeight: 600 }}>DIFF</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
