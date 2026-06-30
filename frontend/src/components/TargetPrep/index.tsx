import React, { useCallback, useEffect, useMemo, useState } from "react";
import { t } from "../../theme";
import type { FilterMode, PairStatus } from "./types";
import { DEFAULT_STATUS } from "./types";
import { TH, DiffCell, ActionBtn } from "./helpers";
import { SearchSelect } from "./SearchSelect";
import { TablePairDetail } from "./TablePairDetail";

export function TargetPrep() {
  const [srcSchemas, setSrcSchemas] = useState<string[]>([]);
  const [tgtSchemas, setTgtSchemas] = useState<string[]>([]);
  const [srcSchema,  setSrcSchema]  = useState("");
  const [tgtSchema,  setTgtSchema]  = useState("");

  const [srcTables,     setSrcTables]     = useState<string[]>([]);
  const [tgtTablesSet,  setTgtTablesSet]  = useState<Set<string>>(new Set());
  const [tablesLoaded,  setTablesLoaded]  = useState(false);
  const [tablesLoading, setTablesLoading] = useState(false);
  const [loadError,     setLoadError]     = useState<string | null>(null);

  const [pairStatus,     setPairStatus]     = useState<Record<string, PairStatus>>({});
  const [expandedKey,    setExpandedKey]    = useState<string | null>(null);
  const [compareAllBusy, setCompareAllBusy] = useState(false);

  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<FilterMode>("all");

  // Load schemas once
  useEffect(() => {
    fetch("/api/db/source/schemas").then(r => r.json()).then(d => Array.isArray(d) && setSrcSchemas(d)).catch(() => {});
    fetch("/api/db/target/schemas").then(r => r.json()).then(d => Array.isArray(d) && setTgtSchemas(d)).catch(() => {});
  }, []);

  const loadTables = useCallback(async () => {
    if (!srcSchema || !tgtSchema) return;
    setTablesLoading(true); setLoadError(null);
    try {
      const [srcR, tgtR] = await Promise.all([
        fetch("/api/db/source/tables?" + new URLSearchParams({ schema: srcSchema })).then(r => r.json()),
        fetch("/api/db/target/tables?"  + new URLSearchParams({ schema: tgtSchema })).then(r => r.json()),
      ]);
      if (Array.isArray(srcR)) setSrcTables(srcR);
      if (Array.isArray(tgtR)) setTgtTablesSet(new Set((tgtR as string[]).map(s => s.toUpperCase())));
      setTablesLoaded(true);
      setPairStatus({});
      setExpandedKey(null);
    } catch (e: any) { setLoadError(e.message); }
    finally { setTablesLoading(false); }
  }, [srcSchema, tgtSchema]);

  // Auto-matched pairs: each source table → same-named target table if it exists
  const pairs = useMemo(() =>
    srcTables.map(tbl => ({ srcTable: tbl, tgtTable: tgtTablesSet.has(tbl.toUpperCase()) ? tbl : null })),
    [srcTables, tgtTablesSet]
  );

  const setPair = useCallback((key: string, patch: Partial<PairStatus>) => {
    setPairStatus(p => ({ ...p, [key]: { ...(p[key] ?? DEFAULT_STATUS), ...patch } }));
  }, []);

  const comparePair = useCallback(async (srcTable: string, tgtTable: string) => {
    setPair(srcTable, { comparing: true, error: null });
    try {
      const r = await fetch("/api/target-prep/compare-summary", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ src_schema: srcSchema, src_table: srcTable, tgt_schema: tgtSchema, tgt_table: tgtTable }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || "Ошибка сравнения");
      setPair(srcTable, { comparing: false, compared: true, diff: d });
    } catch (e: any) {
      setPair(srcTable, { comparing: false, error: e.message });
    }
  }, [srcSchema, tgtSchema, setPair]);

  const compareAll = useCallback(async () => {
    setCompareAllBusy(true);
    const withTarget = pairs.filter(p => p.tgtTable);
    const CONCURRENCY = 5;
    let i = 0;
    async function worker() {
      while (i < withTarget.length) {
        const { srcTable, tgtTable } = withTarget[i++];
        await comparePair(srcTable, tgtTable!);
      }
    }
    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, withTarget.length) }, worker));
    setCompareAllBusy(false);
  }, [pairs, comparePair]);

  const loadDdl = useCallback(async (srcTable: string, tgtTable: string) => {
    setPair(srcTable, { ddlLoading: true });
    try {
      const r = await fetch("/api/target-prep/ddl?" + new URLSearchParams({
        src_schema: srcSchema, src_table: srcTable, tgt_schema: tgtSchema, tgt_table: tgtTable,
      }));
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || "Ошибка");
      setPair(srcTable, { ddl: d, ddlLoading: false });
    } catch (e: any) {
      setPair(srcTable, { ddlLoading: false, error: e.message });
    }
  }, [srcSchema, tgtSchema, setPair]);

  const toggleExpand = useCallback((srcTable: string, tgtTable: string | null) => {
    if (expandedKey === srcTable) { setExpandedKey(null); return; }
    setExpandedKey(srcTable);
    if (tgtTable) {
      const s = pairStatus[srcTable];
      if (!s?.ddl && !s?.ddlLoading) loadDdl(srcTable, tgtTable);
    }
  }, [expandedKey, pairStatus, loadDdl]);

  const syncPair = useCallback(async (srcTable: string, tgtTable: string) => {
    setPair(srcTable, { syncing: true, syncError: null });
    try {
      const r = await fetch("/api/target-prep/ensure-table", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ src_schema: srcSchema, src_table: srcTable, tgt_schema: tgtSchema, tgt_table: tgtTable }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || "Ошибка синхронизации");
      if (d.created) {
        setTgtTablesSet(prev => new Set([...prev, tgtTable.toUpperCase()]));
      }
      setPair(srcTable, { syncing: false, ddl: null });
      await comparePair(srcTable, tgtTable);
      if (expandedKey === srcTable) loadDdl(srcTable, tgtTable);
    } catch (e: any) {
      setPair(srcTable, { syncing: false, syncError: e.message });
    }
  }, [srcSchema, tgtSchema, comparePair, expandedKey, loadDdl, setPair]);

  const handleRefresh = useCallback((srcTable: string, tgtTable: string) => {
    loadDdl(srcTable, tgtTable);
    comparePair(srcTable, tgtTable);
  }, [loadDdl, comparePair]);

  // Filtered pairs
  const filteredPairs = useMemo(() => {
    let res = pairs;
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      res = res.filter(p => p.srcTable.toLowerCase().includes(q) || (p.tgtTable ?? "").toLowerCase().includes(q));
    }
    switch (filter) {
      case "diff":      res = res.filter(p => { const s = pairStatus[p.srcTable]; return s?.compared && s.diff && !s.diff.ok; }); break;
      case "ok":        res = res.filter(p => { const s = pairStatus[p.srcTable]; return s?.compared && !!s.diff?.ok; }); break;
      case "error":     res = res.filter(p => !!pairStatus[p.srcTable]?.error); break;
      case "no_target": res = res.filter(p => !p.tgtTable); break;
    }
    return res;
  }, [pairs, search, filter, pairStatus]);

  // Stats
  const stats = useMemo(() => {
    const compared = Object.values(pairStatus).filter(s => s.compared).length;
    const withDiff = Object.values(pairStatus).filter(s => s.compared && s.diff && !s.diff.ok).length;
    const noTarget = pairs.filter(p => !p.tgtTable).length;
    const errors   = Object.values(pairStatus).filter(s => s.error).length;
    return { total: pairs.length, compared, withDiff, noTarget, errors };
  }, [pairs, pairStatus]);

  const canLoad = !!(srcSchema && tgtSchema);

  return (
    <div style={{ animation: "fadeIn 0.2s ease-out" }}>

      {/* Schema selector + load/compare buttons */}
      <div style={{
        background: t.bg.s2, border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.lg, padding: "14px 18px",
        marginBottom: 16, display: "flex", gap: 16,
        alignItems: "flex-end", flexWrap: "wrap",
      }}>
        <div>
          <div style={{
            fontSize: t.size.sm, color: t.text.muted, marginBottom: 5,
            textTransform: "uppercase", letterSpacing: 0.5,
          }}>
            Источник (схема)
          </div>
          <SearchSelect
            value={srcSchema}
            onChange={v => { setSrcSchema(v); if (!tgtSchema) setTgtSchema(v); }}
            options={srcSchemas}
            placeholder="Схема..."
          />
        </div>

        <span style={{ color: t.text.faint, fontSize: 20, paddingBottom: 4 }}>→</span>

        <div>
          <div style={{
            fontSize: t.size.sm, color: t.text.muted, marginBottom: 5,
            textTransform: "uppercase", letterSpacing: 0.5,
          }}>
            Таргет (схема)
          </div>
          <SearchSelect value={tgtSchema} onChange={setTgtSchema} options={tgtSchemas} placeholder="Схема..." />
        </div>

        <button
          onClick={loadTables}
          disabled={!canLoad || tablesLoading}
          style={{
            background: canLoad && !tablesLoading ? t.blue.dim : t.bg.s2,
            border: "none", borderRadius: t.radius.md, color: t.text.inverse,
            padding: "7px 20px", fontSize: t.size.md, fontWeight: 600,
            cursor: canLoad && !tablesLoading ? "pointer" : "not-allowed",
            opacity: canLoad && !tablesLoading ? 1 : 0.5,
          }}
        >
          {tablesLoading ? "Загрузка…" : "Загрузить таблицы"}
        </button>

        {tablesLoaded && (
          <button
            onClick={compareAll}
            disabled={compareAllBusy || pairs.filter(p => p.tgtTable).length === 0}
            style={{
              background: t.blue.bg, border: `1px solid ${t.blue.dim}`,
              borderRadius: t.radius.md, color: t.blue.fg,
              padding: "7px 20px", fontSize: t.size.md, fontWeight: 600,
              cursor: compareAllBusy ? "not-allowed" : "pointer",
              opacity: compareAllBusy ? 0.6 : 1,
            }}
          >
            {compareAllBusy ? "Сравниваю…" : "Сравнить все DDL"}
          </button>
        )}
        {tablesLoaded && (
          <a
            href={`/api/target-prep/report-all?src_schema=${encodeURIComponent(srcSchema)}&tgt_schema=${encodeURIComponent(tgtSchema)}`}
            target="_blank" rel="noreferrer"
            style={{
              background: t.bg.s3, border: `1px solid ${t.blue.dim}`,
              borderRadius: t.radius.md, color: t.blue.fg,
              padding: "7px 20px", fontSize: t.size.md, fontWeight: 600,
              textDecoration: "none", cursor: "pointer",
            }}
          >
            Обобщённый отчёт
          </a>
        )}
      </div>

      {/* Load error */}
      {loadError && (
        <div style={{
          background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
          color: t.red.fg, padding: "10px 14px", borderRadius: t.radius.md,
          marginBottom: 12, fontSize: t.size.md,
        }}>
          {loadError}
        </div>
      )}

      {/* Initial hint */}
      {!tablesLoaded && !tablesLoading && !loadError && (
        <div style={{ textAlign: "center", color: t.text.faint, padding: "60px 0", fontSize: t.size.md }}>
          Выберите схемы источника и таргета, затем нажмите «Загрузить таблицы»
        </div>
      )}

      {/* Search + filter bar */}
      {tablesLoaded && (
        <div style={{ display: "flex", gap: 10, alignItems: "center", marginBottom: 10, flexWrap: "wrap" }}>
          <div style={{ position: "relative", flex: "1 1 200px", maxWidth: 320 }}>
            <span style={{
              position: "absolute", left: 9, top: "50%", transform: "translateY(-50%)",
              color: t.text.disabled, fontSize: t.size.base, pointerEvents: "none",
            }}>🔍</span>
            <input
              value={search} onChange={e => setSearch(e.target.value)}
              placeholder="Поиск по имени таблицы…"
              style={{
                width: "100%", background: t.bg.s2, border: `1px solid ${t.border.base}`,
                borderRadius: t.radius.md, color: t.text.primary, fontSize: t.size.base,
                padding: "6px 10px 6px 28px", outline: "none", boxSizing: "border-box",
              }}
            />
            {search && (
              <span
                onClick={() => setSearch("")}
                style={{
                  position: "absolute", right: 8, top: "50%", transform: "translateY(-50%)",
                  color: t.text.disabled, cursor: "pointer", fontSize: t.size.base,
                }}
              >✕</span>
            )}
          </div>

          {(["all", "diff", "ok", "error", "no_target"] as FilterMode[]).map(f => {
            const labels: Record<FilterMode, string> = {
              all:       `Все (${stats.total})`,
              diff:      `Различия (${stats.withDiff})`,
              ok:        `OK (${stats.compared - stats.withDiff})`,
              error:     `Ошибки (${stats.errors})`,
              no_target: `Нет пары (${stats.noTarget})`,
            };
            const active = filter === f;
            const accent =
              f === "error" ? t.red.base :
              f === "diff"  ? t.amber.base :
              f === "ok"    ? t.green.base :
              f === "no_target" ? t.amber.dim : t.blue.base;
            return (
              <button
                key={f} onClick={() => setFilter(f)}
                style={{
                  background: active ? accent + "22" : "transparent",
                  border: `1px solid ${active ? accent + "88" : t.border.subtle}`,
                  borderRadius: 20, color: active ? accent : t.text.muted,
                  padding: "4px 12px", fontSize: t.size.sm, fontWeight: 600,
                  cursor: "pointer", whiteSpace: "nowrap",
                }}
              >
                {labels[f]}
              </button>
            );
          })}

          <span style={{ fontSize: t.size.sm, color: t.text.faint, marginLeft: "auto" }}>
            {stats.compared}/{stats.total} сравнено
          </span>
        </div>
      )}

      {/* Tables list */}
      {tablesLoaded && filteredPairs.length > 0 && (
        <div style={{ border: `1px solid ${t.border.subtle}`, borderRadius: t.radius.lg, overflow: "hidden" }}>
          {/* Header row */}
          <div style={{
            display: "grid",
            gridTemplateColumns: "32px 1fr 1fr 80px 72px 90px 64px 210px",
            background: t.bg.s2, borderBottom: `1px solid ${t.border.subtle}`,
            padding: "0 4px",
          }}>
            {["", "Источник", "Таргет", "Колонки", "Индексы", "Констрейнты", "Тригг.", "Действия"].map((h, i) => (
              <div key={i} style={{ ...TH, padding: "8px 8px" }}>{h}</div>
            ))}
          </div>

          {/* Data rows */}
          {filteredPairs.map(({ srcTable, tgtTable }) => {
            const st         = pairStatus[srcTable] ?? DEFAULT_STATUS;
            const isExpanded = expandedKey === srcTable;
            const hasError   = !!st.error;
            const rowBg      = hasError ? "rgba(239,68,68,0.04)" : isExpanded ? t.bg.s2 : "transparent";

            return (
              <React.Fragment key={srcTable}>
                {/* Summary row */}
                <div
                  onClick={() => toggleExpand(srcTable, tgtTable)}
                  style={{
                    display: "grid",
                    gridTemplateColumns: "32px 1fr 1fr 80px 72px 90px 64px 210px",
                    background: rowBg, borderBottom: `1px solid ${t.bg.s2}`,
                    cursor: "pointer", alignItems: "center", padding: "0 4px",
                  }}
                  onMouseEnter={e => { if (!isExpanded && !hasError) (e.currentTarget as HTMLDivElement).style.background = t.bg.s2; }}
                  onMouseLeave={e => { if (!isExpanded && !hasError) (e.currentTarget as HTMLDivElement).style.background = "transparent"; }}
                >
                  <div style={{ padding: "9px 6px", color: t.text.disabled, fontSize: t.size.xs, textAlign: "center" }}>
                    {isExpanded ? "▼" : "▶"}
                  </div>

                  <div style={{ padding: "9px 8px" }}>
                    <code style={{ color: t.text.primary, fontSize: t.size.base }}>{srcTable}</code>
                  </div>

                  <div style={{ padding: "9px 8px" }}>
                    {tgtTable
                      ? <code style={{ color: t.text.secondary, fontSize: t.size.base }}>{tgtTable}</code>
                      : <span style={{ color: t.text.disabled, fontSize: t.size.sm, fontStyle: "italic" }}>— не найдена</span>}
                  </div>

                  <div style={{ padding: "9px 8px" }}>
                    {tgtTable
                      ? <DiffCell
                          missing={(st.diff?.cols_missing ?? 0) + (st.diff?.cols_extra ?? 0)}
                          disabled={st.diff?.cols_type}
                          comparing={st.comparing} compared={st.compared}
                        />
                      : <span style={{ color: t.text.faint }}>—</span>}
                  </div>

                  <div style={{ padding: "9px 8px" }}>
                    {tgtTable
                      ? <DiffCell missing={st.diff?.idx_missing ?? 0} disabled={st.diff?.idx_disabled} comparing={st.comparing} compared={st.compared} />
                      : <span style={{ color: t.text.faint }}>—</span>}
                  </div>

                  <div style={{ padding: "9px 8px" }}>
                    {tgtTable
                      ? <DiffCell missing={st.diff?.con_missing ?? 0} disabled={st.diff?.con_disabled} comparing={st.comparing} compared={st.compared} />
                      : <span style={{ color: t.text.faint }}>—</span>}
                  </div>

                  <div style={{ padding: "9px 8px" }}>
                    {tgtTable
                      ? <DiffCell missing={st.diff?.trg_missing ?? 0} comparing={st.comparing} compared={st.compared} />
                      : <span style={{ color: t.text.faint }}>—</span>}
                  </div>

                  <div
                    style={{ padding: "6px 8px", display: "flex", gap: 5, alignItems: "center", flexWrap: "wrap" }}
                    onClick={e => e.stopPropagation()}
                  >
                    {tgtTable && (
                      <ActionBtn label="Сравнить" onClick={() => comparePair(srcTable, tgtTable)} busy={st.comparing} variant="success" />
                    )}
                    {tgtTable && st.compared && st.diff && !st.diff.ok && (
                      <ActionBtn label="Привести" onClick={() => syncPair(srcTable, tgtTable)} busy={st.syncing} variant="success" />
                    )}
                    {!tgtTable && (
                      <ActionBtn label="Создать" onClick={() => syncPair(srcTable, srcTable)} busy={st.syncing} variant="success" />
                    )}
                    {hasError && (
                      <span title={st.error ?? ""} style={{ color: t.red.base, fontSize: t.size.sm, cursor: "help" }}>
                        ✕ Ошибка
                      </span>
                    )}
                    {st.syncError && (
                      <span title={st.syncError} style={{ color: t.amber.dim, fontSize: t.size.sm, cursor: "help" }}>
                        ⚠ Sync
                      </span>
                    )}
                  </div>
                </div>

                {/* Expanded detail */}
                {isExpanded && (
                  <div style={{ borderBottom: `1px solid ${t.border.subtle}`, background: t.bg.s2 }}>
                    {!tgtTable && (
                      <div style={{
                        padding: "20px", color: t.text.disabled, fontSize: t.size.md,
                        display: "flex", alignItems: "center", gap: 12,
                      }}>
                        <span>
                          Таблица <code style={{ color: t.text.primary }}>{srcTable}</code>
                          {" "}не найдена на таргете в схеме{" "}
                          <code style={{ color: t.text.primary }}>{tgtSchema}</code>
                        </span>
                        <ActionBtn
                          label="Создать по образцу source"
                          onClick={() => syncPair(srcTable, srcTable)}
                          busy={st.syncing}
                          variant="success"
                        />
                      </div>
                    )}
                    {tgtTable && st.ddlLoading && (
                      <div style={{ padding: "20px", color: t.text.disabled, fontSize: t.size.md }}>
                        Загрузка DDL…
                      </div>
                    )}
                    {tgtTable && st.error && !st.ddl && (
                      <div style={{ padding: "16px 20px", color: t.red.fg, fontSize: t.size.base }}>
                        {st.error}
                      </div>
                    )}
                    {tgtTable && st.ddl && (
                      <TablePairDetail
                        srcSchema={srcSchema} srcTable={srcTable}
                        tgtSchema={tgtSchema} tgtTable={tgtTable}
                        ddl={st.ddl}
                        onRefresh={() => handleRefresh(srcTable, tgtTable)}
                      />
                    )}
                  </div>
                )}
              </React.Fragment>
            );
          })}
        </div>
      )}

      {/* Empty filtered result */}
      {tablesLoaded && filteredPairs.length === 0 && pairs.length > 0 && (
        <div style={{ textAlign: "center", color: t.text.disabled, padding: "40px 0", fontSize: t.size.md }}>
          Нет таблиц, соответствующих фильтру
        </div>
      )}

      {tablesLoaded && pairs.length === 0 && (
        <div style={{ textAlign: "center", color: t.text.disabled, padding: "40px 0", fontSize: t.size.md }}>
          В схеме <code style={{ color: t.text.primary }}>{srcSchema}</code> нет таблиц
        </div>
      )}
    </div>
  );
}
