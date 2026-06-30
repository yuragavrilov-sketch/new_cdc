import React, { useCallback, useEffect, useState } from "react";
import { t } from "../../theme";
import {
  api,
  DECISION_COLORS, DECISION_LABELS, STATUS_LABELS,
} from "./types";
import type { ChecklistList, Decision, Status, TableItem } from "./types";
import { SearchableSelect } from "./SearchableSelect";

// ── Local styles ──────────────────────────────────────────────────────────────

const thStyle: React.CSSProperties = {
  padding: "8px 10px", fontSize: t.size.sm, fontWeight: 600, color: t.text.disabled,
  textTransform: "uppercase", letterSpacing: 0.5, textAlign: "left",
};
const tdStyle: React.CSSProperties = { padding: "8px 10px" };

const selectStyle: React.CSSProperties = {
  background: t.bg.s2, border: `1px solid ${t.border.base}`, borderRadius: t.radius.md,
  color: t.text.primary, padding: "5px 10px", fontSize: t.size.base, minWidth: 180,
};

const inputStyle: React.CSSProperties = {
  background: t.bg.app, border: `1px solid ${t.border.base}`, borderRadius: t.radius.md,
  color: t.text.primary, padding: "5px 10px", fontSize: t.size.base, width: 200,
};

const cellSelectStyle: React.CSSProperties = {
  border: "1px solid", borderRadius: t.radius.sm, padding: "3px 8px",
  fontSize: t.size.base, cursor: "pointer", width: "100%",
};

function btnStyle(bg: string): React.CSSProperties {
  return {
    background: bg, border: `1px solid ${t.border.base}`,
    borderRadius: t.radius.md, color: t.text.primary,
    padding: "5px 12px", fontSize: t.size.base,
    cursor: "pointer", fontWeight: 500,
  };
}

// ── Component ────────────────────────────────────────────────────────────────

export function Checklist() {
  const [lists,       setLists]       = useState<ChecklistList[]>([]);
  const [activeId,    setActiveId]    = useState<number | null>(null);
  const [newListName, setNewListName] = useState("");
  const [loading,     setLoading]     = useState(true);

  // Source DB data
  const [schemas,         setSchemas]         = useState<string[]>([]);
  const [schemasLoading,  setSchemasLoading]  = useState(false);
  const [srcTables,       setSrcTables]       = useState<string[]>([]);
  const [tablesLoading,   setTablesLoading]   = useState(false);
  const [newSchema,       setNewSchema]       = useState("");
  const [newTable,        setNewTable]        = useState("");
  const [loadAllBusy,     setLoadAllBusy]     = useState(false);
  const [loadAllError,    setLoadAllError]    = useState("");

  const active = lists.find(l => l.list_id === activeId) ?? null;

  const fetchLists = useCallback(async () => {
    try {
      const data = await api<ChecklistList[]>("/api/checklists");
      setLists(data);
      if (data.length > 0 && !data.find(l => l.list_id === activeId)) {
        setActiveId(data[0].list_id);
      }
    } catch { /* ignore */ }
    finally { setLoading(false); }
  }, [activeId]);

  useEffect(() => { fetchLists(); }, []); // eslint-disable-line

  useEffect(() => {
    setSchemasLoading(true);
    fetch("/api/db/source/schemas")
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setSchemas)
      .catch(() => {})
      .finally(() => setSchemasLoading(false));
  }, []);

  useEffect(() => {
    setSrcTables([]);
    setNewTable("");
    if (!newSchema) return;
    setTablesLoading(true);
    fetch(`/api/db/source/tables?schema=${encodeURIComponent(newSchema)}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setSrcTables)
      .catch(() => {})
      .finally(() => setTablesLoading(false));
  }, [newSchema]);

  // ── List CRUD ───────────────────────────────────────────────────────────────

  const addList = useCallback(async () => {
    const name = newListName.trim();
    if (!name) return;
    try {
      const res = await api<ChecklistList>("/api/checklists", {
        method: "POST", body: JSON.stringify({ name }),
      });
      setLists(prev => [...prev, { ...res, tables: res.tables || [] }]);
      setActiveId(res.list_id);
      setNewListName("");
    } catch { /* ignore */ }
  }, [newListName]);

  const deleteList = useCallback(async () => {
    if (!active || !confirm(`Удалить «${active.name}»?`)) return;
    await api(`/api/checklists/${active.list_id}`, { method: "DELETE" });
    setLists(prev => prev.filter(l => l.list_id !== active.list_id));
    setActiveId(lists.find(l => l.list_id !== active.list_id)?.list_id ?? null);
  }, [active, lists]);

  // ── Item CRUD ───────────────────────────────────────────────────────────────

  const addRow = useCallback(async () => {
    if (!active || !newSchema || !newTable) return;
    try {
      const res = await api<{ added: TableItem[] }>(`/api/checklists/${active.list_id}/items`, {
        method: "POST",
        body: JSON.stringify({ items: [{ schema: newSchema, table: newTable }] }),
      });
      if (res.added.length > 0) {
        setLists(prev => prev.map(l =>
          l.list_id === active.list_id
            ? { ...l, tables: [...l.tables, ...res.added] }
            : l
        ));
      }
      setNewTable("");
    } catch { /* ignore */ }
  }, [active, newSchema, newTable]);

  const loadAllFromSchema = useCallback(async () => {
    if (!active || !newSchema) { setLoadAllError("Сначала выберите схему"); return; }
    setLoadAllBusy(true);
    setLoadAllError("");
    try {
      const res = await fetch(`/api/db/source/tables?schema=${encodeURIComponent(newSchema)}`);
      if (!res.ok) throw new Error("Не удалось загрузить таблицы");
      const tables: string[] = await res.json();
      const addRes = await api<{ added: TableItem[] }>(`/api/checklists/${active.list_id}/items`, {
        method: "POST",
        body: JSON.stringify({ items: tables.map(tbl => ({ schema: newSchema, table: tbl })) }),
      });
      if (addRes.added.length > 0) {
        setLists(prev => prev.map(l =>
          l.list_id === active.list_id
            ? { ...l, tables: [...l.tables, ...addRes.added] }
            : l
        ));
      }
    } catch (e: unknown) {
      setLoadAllError(e instanceof Error ? e.message : "Ошибка загрузки");
    } finally {
      setLoadAllBusy(false);
    }
  }, [active, newSchema]);

  const updateItem = useCallback(async (
    item: TableItem,
    patch: Partial<Pick<TableItem, "decision" | "status">>,
  ) => {
    if (!active) return;
    await api(`/api/checklists/${active.list_id}/items/${item.item_id}`, {
      method: "PATCH", body: JSON.stringify(patch),
    });
    setLists(prev => prev.map(l =>
      l.list_id === active.list_id
        ? { ...l, tables: l.tables.map(tbl => tbl.item_id === item.item_id ? { ...tbl, ...patch } : tbl) }
        : l
    ));
  }, [active]);

  const deleteItem = useCallback(async (item: TableItem) => {
    if (!active) return;
    await api(`/api/checklists/${active.list_id}/items/${item.item_id}`, { method: "DELETE" });
    setLists(prev => prev.map(l =>
      l.list_id === active.list_id
        ? { ...l, tables: l.tables.filter(tbl => tbl.item_id !== item.item_id) }
        : l
    ));
  }, [active]);

  const tables = active?.tables || [];
  const toMigrate = tables.filter(tbl => tbl.decision !== "skip");
  const doneCount = toMigrate.filter(tbl => tbl.status === "done").length;

  if (loading) {
    return <div style={{ color: t.text.disabled, padding: 24, textAlign: "center" }}>Загрузка…</div>;
  }

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto" }}>
      {/* List selector */}
      <div style={{
        display: "flex", alignItems: "center", gap: 8, marginBottom: 16, flexWrap: "wrap",
      }}>
        <select
          value={activeId ?? ""}
          onChange={e => setActiveId(Number(e.target.value) || null)}
          style={selectStyle}
        >
          {lists.length === 0 && <option value="">-- создайте список --</option>}
          {lists.map(l => <option key={l.list_id} value={l.list_id}>{l.name}</option>)}
        </select>
        <input
          placeholder="Новый список..."
          value={newListName}
          onChange={e => setNewListName(e.target.value)}
          onKeyDown={e => e.key === "Enter" && addList()}
          style={inputStyle}
        />
        <button onClick={addList} style={btnStyle(t.blue.dim)}>Создать</button>
        {active && <button onClick={deleteList} style={btnStyle(t.red.border)}>Удалить</button>}
      </div>

      {!active && (
        <div style={{
          textAlign: "center", padding: 48,
          color: t.text.disabled, fontSize: t.size.lg,
        }}>
          Создайте список таблиц для миграции.
        </div>
      )}

      {active && (
        <>
          {/* Stats + load all */}
          <div style={{
            display: "flex", gap: 16, marginBottom: 12,
            fontSize: t.size.base, color: t.text.muted,
            alignItems: "center", flexWrap: "wrap",
          }}>
            <span>Всего: <b style={{ color: t.text.primary }}>{tables.length}</b></span>
            {toMigrate.length > 0 && (
              <>
                <span>К переносу: <b style={{ color: t.green.fg }}>{toMigrate.length}</b></span>
                <span>Перенесено:{" "}
                  <b style={{
                    color: doneCount === toMigrate.length && doneCount > 0 ? t.green.base : t.blue.fg,
                  }}>
                    {doneCount}/{toMigrate.length}
                  </b>
                </span>
              </>
            )}
            <div style={{ marginLeft: "auto" }}>
              <button
                onClick={loadAllFromSchema}
                disabled={loadAllBusy}
                style={{ ...btnStyle(t.bg.s2), opacity: loadAllBusy ? 0.5 : 1 }}
              >
                {loadAllBusy ? "Загрузка…" : "Загрузить все таблицы схемы"}
              </button>
            </div>
            {loadAllError && (
              <span style={{ color: t.red.fg, fontSize: t.size.sm }}>{loadAllError}</span>
            )}
          </div>

          {/* Table */}
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: t.size.md }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${t.border.subtle}` }}>
                <th style={thStyle}>Схема</th>
                <th style={thStyle}>Таблица</th>
                <th style={{ ...thStyle, width: 160 }}>Решение</th>
                <th style={{ ...thStyle, width: 160 }}>Статус</th>
                <th style={{ ...thStyle, width: 40 }}></th>
              </tr>
            </thead>
            <tbody>
              {tables.map(row => {
                const dc = DECISION_COLORS[row.decision];
                return (
                  <tr key={row.item_id} style={{ borderBottom: `1px solid ${t.border.subtle}` }}>
                    <td style={tdStyle}>{row.schema}</td>
                    <td style={{ ...tdStyle, fontWeight: 600 }}>{row.table}</td>
                    <td style={tdStyle}>
                      <select
                        value={row.decision}
                        onChange={e => updateItem(row, { decision: e.target.value as Decision })}
                        style={{
                          ...cellSelectStyle,
                          background: dc.bg, color: dc.text,
                          borderColor: dc.text + "44",
                        }}
                      >
                        {(Object.keys(DECISION_LABELS) as Decision[]).map(d => (
                          <option key={d} value={d}>{DECISION_LABELS[d]}</option>
                        ))}
                      </select>
                    </td>
                    <td style={tdStyle}>
                      {row.decision !== "skip" ? (
                        <select
                          value={row.status}
                          onChange={e => updateItem(row, { status: e.target.value as Status })}
                          style={{
                            ...cellSelectStyle,
                            background: row.status === "done" ? t.green.bg : t.bg.s2,
                            color:      row.status === "done" ? t.green.fg : t.text.secondary,
                            borderColor: row.status === "done" ? `${t.green.dim}44` : `${t.border.base}44`,
                          }}
                        >
                          {(Object.keys(STATUS_LABELS) as Status[]).map(s => (
                            <option key={s} value={s}>{STATUS_LABELS[s]}</option>
                          ))}
                        </select>
                      ) : (
                        <span style={{ color: t.text.faint, fontSize: t.size.base }}>—</span>
                      )}
                    </td>
                    <td style={{ ...tdStyle, textAlign: "center" }}>
                      <button
                        onClick={() => deleteItem(row)}
                        style={{
                          background: "none", border: "none",
                          color: t.text.disabled, cursor: "pointer", fontSize: 14,
                        }}
                        title="Удалить"
                      >
                        ×
                      </button>
                    </td>
                  </tr>
                );
              })}

              {/* Add row */}
              <tr>
                <td style={tdStyle}>
                  <SearchableSelect
                    items={schemas}
                    value={newSchema}
                    onChange={setNewSchema}
                    placeholder="Схема…"
                    loading={schemasLoading}
                  />
                </td>
                <td style={tdStyle}>
                  <SearchableSelect
                    items={srcTables}
                    value={newTable}
                    onChange={setNewTable}
                    placeholder="Таблица…"
                    loading={tablesLoading}
                  />
                </td>
                <td style={tdStyle} colSpan={2}>
                  <button
                    onClick={addRow}
                    disabled={!newSchema || !newTable}
                    style={{
                      ...btnStyle(t.bg.s2),
                      opacity: (!newSchema || !newTable) ? 0.4 : 1,
                    }}
                  >
                    + Добавить
                  </button>
                </td>
                <td />
              </tr>
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
