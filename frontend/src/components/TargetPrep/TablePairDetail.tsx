import React, { useCallback, useMemo, useState } from "react";
import { t } from "../../theme";
import type { ColInfo, DDLData } from "./types";
import {
  fmtType, Dot, ActionBtn, BulkDangerBtn,
  Section, SyncObjResultBar, TH, TD, TR_BORDER,
} from "./helpers";
import { IndexTable }      from "./IndexTable";
import { ConstraintTable } from "./ConstraintTable";
import { TriggerTable }    from "./TriggerTable";

interface Props {
  srcSchema: string; srcTable: string;
  tgtSchema: string; tgtTable: string;
  ddl:       DDLData;
  onRefresh: () => void;
}

export function TablePairDetail({
  srcSchema, srcTable, tgtSchema, tgtTable, ddl, onRefresh,
}: Props) {
  const [busy,          setBusy]          = useState<Record<string, boolean>>({});
  const [actErr,        setActErr]        = useState<Record<string, string>>({});
  const [syncBusy,      setSyncBusy]      = useState(false);
  const [syncResult,    setSyncResult]    = useState<{
    added:    { column: string; type: string }[];
    warnings: { column: string; source_type: string; target_type: string }[];
  } | null>(null);
  const [syncObjBusy,   setSyncObjBusy]   = useState<string | null>(null);
  const [syncObjResult, setSyncObjResult] = useState<{
    type: string; added: string[]; skipped: string[];
    errors: { name: string; error: string }[];
  } | null>(null);
  const [detailError,   setDetailError]   = useState<string | null>(null);

  const doSyncColumns = useCallback(async () => {
    setSyncBusy(true); setSyncResult(null); setDetailError(null);
    try {
      const r = await fetch("/api/target-prep/sync-columns", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ src_schema: srcSchema, src_table: srcTable, tgt_schema: tgtSchema, tgt_table: tgtTable }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || "Ошибка синхронизации");
      setSyncResult(d);
      onRefresh();
    } catch (e: any) { setDetailError(e.message); }
    finally { setSyncBusy(false); }
  }, [srcSchema, srcTable, tgtSchema, tgtTable, onRefresh]);

  const doSyncObjects = useCallback(async (type: "constraints" | "indexes" | "triggers") => {
    setSyncObjBusy(type); setSyncObjResult(null); setDetailError(null);
    try {
      const r = await fetch("/api/target-prep/sync-objects", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ src_schema: srcSchema, src_table: srcTable, tgt_schema: tgtSchema, tgt_table: tgtTable, types: [type] }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || "Ошибка");
      setSyncObjResult({ type, ...d[type] });
      onRefresh();
    } catch (e: any) { setDetailError(e.message); }
    finally { setSyncObjBusy(null); }
  }, [srcSchema, srcTable, tgtSchema, tgtTable, onRefresh]);

  const doAction = useCallback(async (action: string, objectName: string) => {
    setBusy(p => ({ ...p, [objectName]: true }));
    setActErr(p => { const n = { ...p }; delete n[objectName]; return n; });
    try {
      const r = await fetch("/api/target-prep/action", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, tgt_schema: tgtSchema, tgt_table: tgtTable, object_name: objectName }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || "Ошибка");
      onRefresh();
    } catch (e: any) {
      setActErr(p => ({ ...p, [objectName]: e.message }));
      setBusy(p => { const n = { ...p }; delete n[objectName]; return n; });
    }
  }, [tgtSchema, tgtTable, onRefresh]);

  const colDiff = useMemo(() => {
    const tgtMap = new Map(ddl.target.columns.map(c => [c.name, c]));
    const srcMap = new Map(ddl.source.columns.map(c => [c.name, c]));
    const rows: { src: ColInfo | null; tgt: ColInfo | null; state: "ok" | "type" | "noTgt" | "extra" }[] = [];
    for (const sc of ddl.source.columns) {
      const tc = tgtMap.get(sc.name);
      rows.push({ src: sc, tgt: tc ?? null, state: !tc ? "noTgt" : fmtType(sc) === fmtType(tc) ? "ok" : "type" });
    }
    for (const tc of ddl.target.columns) {
      if (!srcMap.has(tc.name)) rows.push({ src: null, tgt: tc, state: "extra" });
    }
    return rows;
  }, [ddl]);

  const colIssues = colDiff.filter(r => r.state !== "ok").length;

  const missingIndexCount = useMemo(() => {
    const tgtNames = new Set(ddl.target.indexes.map(i => i.name));
    return ddl.source.indexes.filter(i => !tgtNames.has(i.name)).length;
  }, [ddl]);

  const missingConstraintCount = useMemo(() => {
    const tgtKeys = new Set(ddl.target.constraints.map(c => `${c.type_code}|${c.columns.join(",")}`));
    return ddl.source.constraints.filter(c => !tgtKeys.has(`${c.type_code}|${c.columns.join(",")}`)).length;
  }, [ddl]);

  const missingTriggerCount = useMemo(() => {
    const tgtNames = new Set(ddl.target.triggers.map(t => t.name));
    return ddl.source.triggers.filter(t => !tgtNames.has(t.name)).length;
  }, [ddl]);

  const reportUrl = `/api/target-prep/report?src_schema=${encodeURIComponent(srcSchema)}&src_table=${encodeURIComponent(srcTable)}&tgt_schema=${encodeURIComponent(tgtSchema)}&tgt_table=${encodeURIComponent(tgtTable)}`;

  return (
    <div style={{ padding: "14px 16px", display: "flex", flexDirection: "column", gap: 10 }}>
      {/* Report button */}
      <div style={{ display: "flex", justifyContent: "flex-end" }}>
        <a
          href={reportUrl} target="_blank" rel="noreferrer"
          style={{
            background: t.bg.s3, color: t.blue.fg,
            border: `1px solid ${t.blue.dim}`,
            borderRadius: t.radius.md, padding: "4px 12px",
            fontSize: t.size.sm, fontWeight: 600,
            textDecoration: "none", cursor: "pointer",
          }}
        >
          Отчёт (PDF)
        </a>
      </div>

      {detailError && (
        <div style={{
          background: `${t.red.border}22`, border: `1px solid ${t.red.border}`,
          color: t.red.fg, padding: "8px 12px",
          borderRadius: t.radius.md, fontSize: t.size.base,
        }}>
          {detailError}
        </div>
      )}

      {/* Columns */}
      <Section
        title="Колонки" count={ddl.source.columns.length}
        status={colIssues === 0 ? "ok" : "warn"}
        bulkAction={
          colDiff.some(r => r.state === "noTgt")
            ? <ActionBtn label="Добавить недостающие колонки" onClick={doSyncColumns} busy={syncBusy} variant="success" />
            : undefined
        }
      >
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${t.border.subtle}` }}>
                {["#", "Колонка", "Тип (источник)", "Тип (таргет)", "NULL src", "NULL tgt", "Default (tgt)"].map(h => (
                  <th key={h} style={TH}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {colDiff.map((row, i) => {
                const bg =
                  row.state === "noTgt" ? "rgba(239,68,68,0.06)" :
                  row.state === "type"  ? "rgba(234,179,8,0.06)" :
                  row.state === "extra" ? "rgba(249,115,22,0.06)" :
                                          "transparent";
                const dotC = row.state === "ok" ? t.green.base : row.state === "type" ? t.amber.base : t.red.base;
                return (
                  <tr key={i} style={{ ...TR_BORDER, background: bg }}>
                    <td style={{ ...TD, color: t.text.disabled }}>{row.src?.column_id ?? "—"}</td>
                    <td style={TD}>
                      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <Dot color={dotC} />
                        <code style={{ color: t.text.primary, fontSize: t.size.base }}>{row.src?.name ?? row.tgt?.name}</code>
                      </div>
                    </td>
                    <td style={{ ...TD, color: t.text.secondary, fontFamily: t.font.mono, fontSize: t.size.sm }}>
                      {row.src ? fmtType(row.src) : <span style={{ color: t.text.faint }}>—</span>}
                    </td>
                    <td style={{
                      ...TD, fontFamily: t.font.mono, fontSize: t.size.sm,
                      color: row.state === "type" ? t.amber.base : row.state === "noTgt" ? t.red.base : t.text.secondary,
                    }}>
                      {row.tgt ? fmtType(row.tgt) : <span style={{ color: t.red.base, fontFamily: t.font.sans }}>отсутствует</span>}
                    </td>
                    <td style={{ ...TD, color: t.text.muted, textAlign: "center" }}>
                      {row.src ? (row.src.nullable ? "Y" : "N") : "—"}
                    </td>
                    <td style={{ ...TD, color: t.text.muted, textAlign: "center" }}>
                      {row.tgt ? (row.tgt.nullable ? "Y" : "N") : "—"}
                    </td>
                    <td style={{ ...TD, color: t.text.muted, fontFamily: t.font.mono, fontSize: t.size.sm }}>
                      {row.tgt?.data_default ?? "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {colIssues > 0 && (
          <div style={{
            padding: "7px 14px", borderTop: `1px solid ${t.border.subtle}`,
            fontSize: t.size.sm, color: t.text.muted,
          }}>
            {colDiff.filter(r => r.state === "noTgt").length > 0 && (
              <span style={{ color: t.red.base, marginRight: 12 }}>
                ✕ {colDiff.filter(r => r.state === "noTgt").length} отсутствуют в таргете
              </span>
            )}
            {colDiff.filter(r => r.state === "type").length > 0 && (
              <span style={{ color: t.amber.base, marginRight: 12 }}>
                ⚠ {colDiff.filter(r => r.state === "type").length} несовпадение типов
              </span>
            )}
            {colDiff.filter(r => r.state === "extra").length > 0 && (
              <span style={{ color: t.amber.dim }}>
                + {colDiff.filter(r => r.state === "extra").length} лишних в таргете
              </span>
            )}
          </div>
        )}
        {syncResult && (
          <div style={{
            padding: "8px 14px", borderTop: `1px solid ${t.border.subtle}`, fontSize: t.size.sm,
          }}>
            {syncResult.added.length > 0 && (
              <div style={{ color: t.green.base, marginBottom: 4 }}>
                ✓ Добавлено: {syncResult.added.map(a => `${a.column} (${a.type})`).join(", ")}
              </div>
            )}
            {syncResult.warnings.length > 0 && (
              <div style={{ color: t.amber.base }}>
                ⚠ Несовпадение типов (не применено): {syncResult.warnings.map(w => `${w.column}: src=${w.source_type} / tgt=${w.target_type}`).join("; ")}
              </div>
            )}
            {syncResult.added.length === 0 && syncResult.warnings.length === 0 && (
              <span style={{ color: t.text.disabled }}>Нет изменений</span>
            )}
          </div>
        )}
      </Section>

      {/* Indexes */}
      <Section
        title="Индексы" count={ddl.target.indexes.length} status="info"
        bulkAction={
          <div style={{ display: "flex", gap: 6 }}>
            {missingIndexCount > 0 && (
              <ActionBtn
                label={`Создать недостающие (${missingIndexCount})`}
                onClick={() => doSyncObjects("indexes")}
                busy={syncObjBusy === "indexes"}
                variant="success"
              />
            )}
            {ddl.target.indexes.some(ix => ix.status === "VALID") && (
              <BulkDangerBtn
                label="Отключить все (UNUSABLE)"
                onClick={() => ddl.target.indexes.filter(ix => ix.status === "VALID")
                  .forEach(ix => doAction("disable_index", ix.name))}
              />
            )}
            {ddl.target.indexes.some(ix => ix.status === "UNUSABLE") && (
              <BulkDangerBtn
                label="Перестроить все"
                onClick={() => ddl.target.indexes.filter(ix => ix.status === "UNUSABLE")
                  .forEach(ix => doAction("enable_index", ix.name))}
              />
            )}
          </div>
        }
      >
        <IndexTable src={ddl.source.indexes} tgt={ddl.target.indexes} busy={busy} actErr={actErr} onAction={doAction} />
        <SyncObjResultBar result={syncObjResult} type="indexes" />
      </Section>

      {/* Constraints */}
      <Section
        title="Ограничения (Constraints)" count={ddl.target.constraints.length} status="info"
        bulkAction={
          <div style={{ display: "flex", gap: 6 }}>
            {missingConstraintCount > 0 && (
              <ActionBtn
                label={`Создать недостающие (${missingConstraintCount})`}
                onClick={() => doSyncObjects("constraints")}
                busy={syncObjBusy === "constraints"}
                variant="success"
              />
            )}
            {ddl.target.constraints.some(c => c.status === "ENABLED" && c.type_code !== "P") && (
              <BulkDangerBtn
                label="Отключить FK / UK / CHECK"
                onClick={() => ddl.target.constraints
                  .filter(c => c.status === "ENABLED" && c.type_code !== "P")
                  .forEach(c => doAction("disable_constraint", c.name))}
              />
            )}
          </div>
        }
      >
        <ConstraintTable src={ddl.source.constraints} tgt={ddl.target.constraints} busy={busy} actErr={actErr} onAction={doAction} />
        <SyncObjResultBar result={syncObjResult} type="constraints" />
      </Section>

      {/* Triggers */}
      <Section
        title="Триггеры" count={ddl.target.triggers.length} status="info"
        bulkAction={
          <div style={{ display: "flex", gap: 6 }}>
            {missingTriggerCount > 0 && (
              <ActionBtn
                label={`Создать недостающие (${missingTriggerCount})`}
                onClick={() => doSyncObjects("triggers")}
                busy={syncObjBusy === "triggers"}
                variant="success"
              />
            )}
            {ddl.target.triggers.some(t => t.status === "ENABLED") && (
              <BulkDangerBtn
                label="Отключить все"
                onClick={() => ddl.target.triggers
                  .filter(t => t.status === "ENABLED")
                  .forEach(t => doAction("disable_trigger", t.name))}
              />
            )}
          </div>
        }
      >
        <TriggerTable src={ddl.source.triggers} tgt={ddl.target.triggers} busy={busy} actErr={actErr} onAction={doAction} />
        <SyncObjResultBar result={syncObjResult} type="triggers" />
      </Section>
    </div>
  );
}
