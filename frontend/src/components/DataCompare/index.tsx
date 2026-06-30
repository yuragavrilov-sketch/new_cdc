import { useCallback, useEffect, useState } from "react";
import { t } from "../../theme";
import { SearchSelect } from "../TargetPrep/SearchSelect";
import type { CompareTask, ColInfo } from "./types";
import { TaskRow } from "./TaskRow";

export function DataCompare() {
  // Selectors
  const [srcSchema,     setSrcSchema]     = useState("");
  const [srcTable,      setSrcTable]      = useState("");
  const [tgtSchema,     setTgtSchema]     = useState("");
  const [tgtTable,      setTgtTable]      = useState("");
  const [mirrorTarget,  setMirrorTarget]  = useState(true);

  // Mode
  const [mode,        setMode]        = useState<"full" | "last_n">("full");
  const [lastN,       setLastN]       = useState(1000);
  const [orderColumn, setOrderColumn] = useState("");

  // Options
  const [srcSchemas, setSrcSchemas] = useState<string[]>([]);
  const [srcTables,  setSrcTables]  = useState<string[]>([]);
  const [tgtSchemas, setTgtSchemas] = useState<string[]>([]);
  const [tgtTables,  setTgtTables]  = useState<string[]>([]);
  const [columns,    setColumns]    = useState<ColInfo[]>([]);

  // Run state
  const [submitting, setSubmitting] = useState(false);
  const [error,      setError]      = useState<string | null>(null);

  // History
  const [tasks,        setTasks]        = useState<CompareTask[]>([]);
  const [loadingTasks, setLoadingTasks] = useState(false);

  useEffect(() => {
    fetch("/api/db/source/schemas").then(r => r.ok ? r.json() : []).then(setSrcSchemas).catch(() => {});
    fetch("/api/db/target/schemas").then(r => r.ok ? r.json() : []).then(setTgtSchemas).catch(() => {});
    loadTasks();
    // eslint-disable-next-line
  }, []);

  useEffect(() => {
    if (!srcSchema) { setSrcTables([]); return; }
    fetch(`/api/db/source/tables?schema=${srcSchema}`)
      .then(r => r.ok ? r.json() : [])
      .then(setSrcTables).catch(() => {});
  }, [srcSchema]);

  useEffect(() => {
    const schema = mirrorTarget ? srcSchema : tgtSchema;
    if (!schema) { setTgtTables([]); return; }
    fetch(`/api/db/target/tables?schema=${schema}`)
      .then(r => r.ok ? r.json() : [])
      .then(setTgtTables).catch(() => {});
  }, [tgtSchema, mirrorTarget, srcSchema]);

  useEffect(() => {
    if (mirrorTarget) {
      setTgtSchema(srcSchema);
      setTgtTable(srcTable);
    }
  }, [srcSchema, srcTable, mirrorTarget]);

  useEffect(() => {
    if (!srcSchema || !srcTable) { setColumns([]); return; }
    fetch(`/api/db/source/table-info?schema=${srcSchema}&table=${srcTable}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d?.columns) setColumns(d.columns); })
      .catch(() => {});
  }, [srcSchema, srcTable]);

  const loadTasks = useCallback(() => {
    setLoadingTasks(true);
    fetch("/api/data-compare/tasks")
      .then(r => r.ok ? r.json() : [])
      .then(setTasks)
      .catch(() => {})
      .finally(() => setLoadingTasks(false));
  }, []);

  useEffect(() => {
    const hasRunning = tasks.some(task =>
      task.status === "PENDING" || task.status === "RUNNING" || task.status === "CHUNKING"
    );
    if (!hasRunning) return;
    const id = setInterval(loadTasks, 3000);
    return () => clearInterval(id);
  }, [tasks, loadTasks]);

  const handleRun = async () => {
    setError(null);
    setSubmitting(true);
    try {
      const body: Record<string, unknown> = {
        source_schema: srcSchema,
        source_table:  srcTable,
        target_schema: mirrorTarget ? srcSchema : tgtSchema,
        target_table:  mirrorTarget ? srcTable  : tgtTable,
        compare_mode:  mode,
      };
      if (mode === "last_n") {
        body.last_n       = lastN;
        body.order_column = orderColumn;
      }
      const r = await fetch("/api/data-compare/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await r.json();
      if (!r.ok) { setError(data.error || "Ошибка"); return; }
      loadTasks();
    } catch (e: any) {
      setError(e.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (id: string) => {
    await fetch(`/api/data-compare/tasks/${id}`, { method: "DELETE" });
    loadTasks();
  };

  const canRun = srcSchema && srcTable && (mirrorTarget || (tgtSchema && tgtTable))
    && (mode === "full" || (lastN > 0 && orderColumn));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Form */}
      <div style={{
        background: t.bg.app, border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.lg, padding: 16,
        display: "flex", flexDirection: "column", gap: 14,
      }}>
        {/* Source */}
        <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
          <span style={{ fontSize: t.size.base, color: t.text.muted, width: 55 }}>Source</span>
          <SearchSelect
            value={srcSchema}
            onChange={v => { setSrcSchema(v); setSrcTable(""); }}
            options={srcSchemas}
            placeholder="Схема"
          />
          <SearchSelect
            value={srcTable}
            onChange={setSrcTable}
            options={srcTables}
            placeholder="Таблица"
            disabled={!srcSchema}
          />
        </div>

        {/* Mirror toggle */}
        <label style={{
          display: "flex", alignItems: "center", gap: 8,
          fontSize: t.size.base, color: t.text.secondary,
          cursor: "pointer", userSelect: "none",
        }}>
          <input
            type="checkbox" checked={mirrorTarget}
            onChange={e => setMirrorTarget(e.target.checked)}
            style={{ accentColor: t.blue.base }}
          />
          Target = Source (та же схема/таблица)
        </label>

        {/* Target */}
        {!mirrorTarget && (
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            <span style={{ fontSize: t.size.base, color: t.text.muted, width: 55 }}>Target</span>
            <SearchSelect
              value={tgtSchema}
              onChange={v => { setTgtSchema(v); setTgtTable(""); }}
              options={tgtSchemas}
              placeholder="Схема"
            />
            <SearchSelect
              value={tgtTable}
              onChange={setTgtTable}
              options={tgtTables}
              placeholder="Таблица"
              disabled={!tgtSchema}
            />
          </div>
        )}

        {/* Mode */}
        <div style={{ display: "flex", alignItems: "center", gap: 14, flexWrap: "wrap" }}>
          <span style={{ fontSize: t.size.base, color: t.text.muted, width: 55 }}>Режим</span>
          <div style={{
            display: "flex", gap: 0, borderRadius: t.radius.sm,
            overflow: "hidden", border: `1px solid ${t.border.base}`,
          }}>
            <button
              onClick={() => setMode("full")}
              style={{
                padding: "5px 14px", fontSize: t.size.base, border: "none", cursor: "pointer",
                background: mode === "full" ? t.blue.dim : t.bg.s2,
                color:      mode === "full" ? t.text.primary : t.text.muted,
                fontWeight: mode === "full" ? 600 : 400,
              }}
            >
              Вся таблица
            </button>
            <button
              onClick={() => setMode("last_n")}
              style={{
                padding: "5px 14px", fontSize: t.size.base, border: "none", cursor: "pointer",
                borderLeft: `1px solid ${t.border.base}`,
                background: mode === "last_n" ? t.blue.dim : t.bg.s2,
                color:      mode === "last_n" ? t.text.primary : t.text.muted,
                fontWeight: mode === "last_n" ? 600 : 400,
              }}
            >
              Последние N записей
            </button>
          </div>

          {mode === "last_n" && (
            <>
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <span style={{ fontSize: t.size.base, color: t.text.muted }}>N =</span>
                <input
                  type="number" min={1} value={lastN}
                  onChange={e => setLastN(Math.max(1, Number(e.target.value)))}
                  style={{
                    width: 80, background: t.bg.s2, border: `1px solid ${t.border.base}`,
                    borderRadius: t.radius.sm, color: t.text.primary,
                    padding: "4px 8px", fontSize: t.size.base,
                  }}
                />
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <span style={{ fontSize: t.size.base, color: t.text.muted }}>ORDER BY</span>
                <SearchSelect
                  value={orderColumn}
                  onChange={setOrderColumn}
                  options={columns.map(c => c.name)}
                  placeholder="Колонка"
                  disabled={columns.length === 0}
                />
                <span style={{ fontSize: t.size.sm, color: t.text.disabled }}>DESC</span>
              </div>
            </>
          )}
        </div>

        {/* Run */}
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <button
            onClick={handleRun}
            disabled={!canRun || submitting}
            style={{
              background: canRun && !submitting ? t.blue.dim : t.bg.s2,
              border: `1px solid ${canRun && !submitting ? t.blue.base : t.border.base}`,
              borderRadius: t.radius.md, color: t.text.primary,
              padding: "6px 20px", fontSize: t.size.md, fontWeight: 600,
              cursor: canRun && !submitting ? "pointer" : "not-allowed",
              opacity: canRun && !submitting ? 1 : 0.5,
            }}
          >
            {submitting ? "Запуск..." : "Сравнить"}
          </button>
          {error && <span style={{ fontSize: t.size.base, color: t.red.base }}>{error}</span>}
        </div>
      </div>

      {/* Results */}
      <div style={{
        background: t.bg.app, border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.lg, overflow: "hidden",
      }}>
        <div style={{
          padding: "10px 14px", borderBottom: `1px solid ${t.border.subtle}`,
          display: "flex", alignItems: "center", gap: 10,
        }}>
          <span style={{ fontWeight: 600, fontSize: t.size.md, color: t.text.primary }}>
            Результаты сравнений
          </span>
          <span style={{ fontSize: t.size.sm, color: t.text.disabled }}>({tasks.length})</span>
          <button
            onClick={loadTasks}
            disabled={loadingTasks}
            style={{
              marginLeft: "auto", background: "none", border: `1px solid ${t.border.base}`,
              borderRadius: t.radius.sm, color: t.text.muted,
              padding: "3px 10px", fontSize: t.size.sm, cursor: "pointer",
            }}
          >
            {loadingTasks ? "..." : "Обновить"}
          </button>
        </div>

        {tasks.length === 0 ? (
          <div style={{
            padding: "24px 14px", textAlign: "center",
            color: t.text.disabled, fontSize: t.size.base,
          }}>
            Нет результатов. Выберите таблицу и нажмите «Сравнить».
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${t.border.subtle}` }}>
                  {["Таблица", "Режим", "Статус", "Source", "Target", "Count", "Hash", "Время", ""].map(h => (
                    <th key={h} style={{
                      padding: "7px 10px", textAlign: "left",
                      color: t.text.muted, fontWeight: 500,
                      fontSize: t.size.sm, whiteSpace: "nowrap",
                    }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tasks.map(task => (
                  <TaskRow key={task.task_id} task={task} onDelete={handleDelete} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
