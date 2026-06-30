import React, { useState, useEffect, useMemo } from "react";
import { S } from "./styles";
import { MatchBadge, MigrationBadge } from "./StatusBadges";
import { ObjectActions } from "./ObjectActions";
import { Pagination, usePagination } from "./Pagination";
import { t } from "../../theme";

export interface CatalogObject {
  object_name: string;
  oracle_status: string;
  last_ddl_time: string | null;
  metadata: Record<string, unknown>;
  match_status: string;
  diff: Record<string, unknown>;
  migration_status: string;
  _type?: string;  // real Oracle object_type, set during load
}

interface Props {
  objects: CatalogObject[];
  snapshotId: number | null;
  selected: Set<string>;
  onToggle: (name: string) => void;
  onToggleAll: (names: string[]) => void;
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
  srcSchema: string;
  tgtSchema: string;
  onOpenMigration?: () => void;
}

type StatusFilter = "ALL" | "OK" | "DIFF" | "MISSING";

function fmtType(c: Record<string, unknown>): string {
  const dt = (c.data_type as string) ?? "";
  const prec = c.data_precision as number | null;
  const scale = c.data_scale as number | null;
  const len = c.data_length as number | null;
  if (["NUMBER"].includes(dt)) {
    if (prec != null && scale != null) return `NUMBER(${prec},${scale})`;
    if (prec != null) return `NUMBER(${prec})`;
    return "NUMBER";
  }
  if (["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"].includes(dt)) {
    return len != null ? `${dt}(${len})` : dt;
  }
  return dt;
}

function ColsTable({ cols }: { cols: Record<string, unknown>[] }) {
  return (
    <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
        <tr>
          {["#", "Имя", "Тип", "Nullable", "Default"].map(h => (
            <th key={h} style={S.th}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {cols.map((c, i) => (
          <tr key={i} style={S.trBorder}>
            <td style={{ ...S.td, color: t.text.disabled }}>{(c.column_id as number) ?? i + 1}</td>
            <td style={{ ...S.td, color: t.text.primary }}>{(c.name ?? c.column_name) as string}</td>
            <td style={{ ...S.td, color: t.text.secondary }}>{fmtType(c)}</td>
            <td style={S.td}>
              {(c.nullable === true || c.nullable === "Y")
                ? <span style={S.badge(`${t.green.base}22`, t.green.base)}>YES</span>
                : <span style={S.badge(`${t.red.base}22`, t.red.base)}>NO</span>}
            </td>
            <td style={{ ...S.td, color: t.text.muted, fontFamily: "monospace" }}>
              {(c.data_default as string) ?? "—"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function IdxTable({ idxs }: { idxs: Record<string, unknown>[] }) {
  return (
    <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
        <tr>
          {["Имя", "Тип / Unique", "Колонки", "Статус"].map(h => (
            <th key={h} style={S.th}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {idxs.map((idx, i) => {
          // columns can be string[] or {column_name}[]
          const rawCols = (idx.columns ?? []) as unknown[];
          const colStr = rawCols.map((c: unknown) => typeof c === "string" ? c : (c as Record<string, unknown>).column_name as string).join(", ");
          return (
            <tr key={i} style={S.trBorder}>
              <td style={{ ...S.td, color: t.text.primary }}>{(idx.name ?? idx.index_name) as string}</td>
              <td style={S.td}>
                <span style={S.badge(`${t.blue.base}22`, t.blue.base)}>{idx.index_type as string}</span>
                {(idx.unique === true || idx.uniqueness === "UNIQUE") && (
                  <span style={{ ...S.badge(`${t.purple.base}22`, t.purple.base), marginLeft: 4 }}>UNIQUE</span>
                )}
              </td>
              <td style={{ ...S.td, color: t.text.secondary }}>{colStr}</td>
              <td style={S.td}>
                <span style={S.badge(
                  idx.status === "VALID" ? `${t.green.base}22` : `${t.red.base}22`,
                  idx.status === "VALID" ? t.green.base : t.red.base
                )}>{idx.status as string}</span>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function ConstrTable({ constrs }: { constrs: Record<string, unknown>[] }) {
  const typeColor: Record<string, [string, string]> = {
    P: [`${t.blue.base}22`, t.blue.base], "PRIMARY KEY": [`${t.blue.base}22`, t.blue.base],
    U: [`${t.purple.base}22`, t.purple.base], "UNIQUE": [`${t.purple.base}22`, t.purple.base],
    R: [`${t.amber.base}22`, t.amber.base], "FOREIGN KEY": [`${t.amber.base}22`, t.amber.base],
    C: [`${t.blue.base}22`, t.blue.base], "CHECK": [`${t.blue.base}22`, t.blue.base],
  };
  return (
    <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
        <tr>
          {["Имя", "Тип", "Колонки", "Статус"].map(h => (
            <th key={h} style={S.th}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {constrs.map((c, i) => {
          // columns can be string[] or {column_name}[]
          const rawCols = (c.columns ?? []) as unknown[];
          const colStr = rawCols.map((x: unknown) => typeof x === "string" ? x : (x as Record<string, unknown>).column_name as string).join(", ");
          const ct = (c.type_code ?? c.type ?? c.constraint_type ?? "") as string;
          const label = (c.type ?? ct) as string;
          const [bg, fg] = typeColor[ct] ?? typeColor[label] ?? [`${t.border.base}22`, t.text.disabled];
          return (
            <tr key={i} style={S.trBorder}>
              <td style={{ ...S.td, color: t.text.primary }}>{(c.name ?? c.constraint_name) as string}</td>
              <td style={S.td}><span style={S.badge(bg, fg)}>{label}</span></td>
              <td style={{ ...S.td, color: t.text.secondary }}>{colStr}</td>
              <td style={S.td}>
                <span style={S.badge(
                  c.status === "ENABLED" ? `${t.green.base}22` : `${t.red.base}22`,
                  c.status === "ENABLED" ? t.green.base : t.red.base
                )}>{c.status as string}</span>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function TrigTable({ trigs }: { trigs: Record<string, unknown>[] }) {
  return (
    <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
        <tr>
          {["Имя", "Тип", "Событие", "Статус"].map(h => (
            <th key={h} style={S.th}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {trigs.map((trg, i) => (
          <tr key={i} style={S.trBorder}>
            <td style={{ ...S.td, color: t.text.primary }}>{(trg.name ?? trg.trigger_name) as string}</td>
            <td style={{ ...S.td, color: t.text.secondary }}>{trg.trigger_type as string}</td>
            <td style={{ ...S.td, color: t.text.secondary }}>{(trg.event ?? trg.triggering_event) as string}</td>
            <td style={S.td}>
              <span style={S.badge(
                trg.status === "ENABLED" ? `${t.green.base}22` : `${t.red.base}22`,
                trg.status === "ENABLED" ? t.green.base : t.red.base
              )}>{trg.status as string}</span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function DiffSummary({ diff }: { diff: Record<string, unknown> }) {
  if (!diff || diff.ok === true) return null;

  const items: { label: string; names: string[] }[] = [];
  const colsMissing = (diff.cols_missing ?? []) as string[];
  const colsExtra = (diff.cols_extra ?? []) as string[];
  const colsType = (diff.cols_type ?? []) as string[];
  const idxMissing = (diff.idx_missing ?? []) as string[];
  const idxDisabled = (diff.idx_disabled ?? []) as string[];
  const conMissing = (diff.con_missing ?? []) as string[];
  const conDisabled = (diff.con_disabled ?? []) as string[];
  const trgMissing = (diff.trg_missing ?? []) as string[];

  if (colsMissing.length) items.push({ label: "Колонки отсутствуют на таргете", names: colsMissing });
  if (colsExtra.length) items.push({ label: "Лишние колонки на таргете", names: colsExtra });
  if (colsType.length) items.push({ label: "Различие типов колонок", names: colsType });
  if (idxMissing.length) items.push({ label: "Индексы отсутствуют", names: idxMissing });
  if (idxDisabled.length) items.push({ label: "Индексы невалидные", names: idxDisabled });
  if (conMissing.length) items.push({ label: "Ограничения отсутствуют", names: conMissing });
  if (conDisabled.length) items.push({ label: "Ограничения отключены", names: conDisabled });
  if (trgMissing.length) items.push({ label: "Триггеры отсутствуют", names: trgMissing });

  if (items.length === 0) return null;

  return (
    <div style={{
      background: t.bg.s2, border: `1px solid ${`color-mix(in oklab, ${t.amber.dim} 27%, transparent)`}`, borderRadius: 6,
      padding: "10px 14px", marginBottom: 6,
    }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: t.amber.base, marginBottom: 6 }}>РАЗЛИЧИЯ С ТАРГЕТОМ</div>
      {items.map((item, i) => (
        <div key={i} style={{ marginBottom: 4 }}>
          <span style={{ fontSize: 11, color: t.amber.base, fontWeight: 600 }}>{item.label}: </span>
          <span style={{ fontSize: 11, color: t.amber.fg, fontFamily: "monospace" }}>
            {item.names.join(", ")}
          </span>
        </div>
      ))}
    </div>
  );
}

function TargetCompare({ srcCols, tgtCols }: { srcCols: Record<string, unknown>[]; tgtCols: Record<string, unknown>[] }) {
  const tgtMap = new Map<string, Record<string, unknown>>();
  for (const c of tgtCols) tgtMap.set((c.name ?? c.column_name) as string, c);
  const srcMap = new Map<string, Record<string, unknown>>();
  for (const c of srcCols) srcMap.set((c.name ?? c.column_name) as string, c);

  const allNames = new Set([...srcMap.keys(), ...tgtMap.keys()]);
  const rows: { name: string; srcType: string; tgtType: string; status: string }[] = [];
  for (const name of allNames) {
    const s = srcMap.get(name);
    const tgt = tgtMap.get(name);
    if (s && !tgt) rows.push({ name, srcType: fmtType(s), tgtType: "—", status: "missing" });
    else if (!s && tgt) rows.push({ name, srcType: "—", tgtType: fmtType(tgt), status: "extra" });
    else if (s && tgt) {
      const st = fmtType(s), tt = fmtType(tgt);
      rows.push({ name, srcType: st, tgtType: tt, status: st === tt ? "ok" : "type" });
    }
  }
  const diffs = rows.filter(r => r.status !== "ok");
  if (diffs.length === 0) return null;

  const colors: Record<string, string> = { missing: t.red.base, extra: t.purple.base, type: t.amber.base, ok: t.green.base };
  const labels: Record<string, string> = { missing: "Нет на тгт", extra: "Лишняя", type: "Тип отлич.", ok: "OK" };

  return (
    <div style={{ border: `1px solid ${`color-mix(in oklab, ${t.amber.dim} 27%, transparent)`}`, borderRadius: 6, overflow: "hidden", marginBottom: 6 }}>
      <div style={{ padding: "5px 12px", fontSize: 11, fontWeight: 700, color: t.amber.base, background: t.bg.s2, borderBottom: `1px solid ${`color-mix(in oklab, ${t.amber.dim} 27%, transparent)`}` }}>
        СРАВНЕНИЕ КОЛОНОК (только различия: {diffs.length})
      </div>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            {["Колонка", "Source тип", "Target тип", "Статус"].map(h => <th key={h} style={S.th}>{h}</th>)}
          </tr>
        </thead>
        <tbody>
          {diffs.map(r => (
            <tr key={r.name} style={S.trBorder}>
              <td style={{ ...S.td, color: t.text.primary, fontFamily: "monospace" }}>{r.name}</td>
              <td style={{ ...S.td, color: t.text.secondary, fontFamily: "monospace" }}>{r.srcType}</td>
              <td style={{ ...S.td, color: colors[r.status], fontFamily: "monospace" }}>{r.tgtType}</td>
              <td style={S.td}><span style={S.badge(colors[r.status] + "22", colors[r.status])}>{labels[r.status]}</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TableDetail({
  obj, snapshotId, srcSchema, tgtSchema, onOpenMigration,
}: {
  obj: CatalogObject;
  snapshotId: number | null;
  srcSchema: string;
  tgtSchema: string;
  onOpenMigration?: () => void;
}) {
  const [tgtMeta, setTgtMeta] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    if (!snapshotId || obj.match_status !== "DIFF") return;
    fetch(`/api/catalog/objects/${obj.object_name}/detail?snapshot_id=${snapshotId}&type=TABLE`)
      .then(r => r.ok ? r.json() : null)
      .then(d => d && setTgtMeta(d.target ?? null))
      .catch(() => {});
  }, [obj.object_name, obj.match_status, snapshotId]);

  const meta = obj.metadata;
  const cols = (meta.columns as Record<string, unknown>[]) ?? [];
  const idxs = (meta.indexes as Record<string, unknown>[]) ?? [];
  const constrs = (meta.constraints as Record<string, unknown>[]) ?? [];
  const trigs = (meta.triggers as Record<string, unknown>[]) ?? [];

  const sectionStyle: React.CSSProperties = {
    background: t.bg.s2,
    border: `1px solid ${t.border.subtle}`,
    borderRadius: 6,
    marginTop: 4,
    overflow: "hidden",
  };
  const sectionHeader: React.CSSProperties = {
    padding: "5px 12px",
    fontSize: 11,
    fontWeight: 700,
    color: t.text.muted,
    letterSpacing: 0.5,
    background: t.bg.s1,
    borderBottom: `1px solid ${t.border.subtle}`,
  };

  const blocked = obj.migration_status === "PLANNED" || obj.migration_status === "IN_PROGRESS";
  const canMigrate = !blocked && !!srcSchema && !!tgtSchema;
  const migrateAction = canMigrate ? (
    <button
      onClick={onOpenMigration}
      disabled={!onOpenMigration}
      style={{
        background: "#2f7d52",
        border: "1px solid #1f5a3a",
        borderRadius: t.radius.sm, color: "#fff",
        padding: "3px 12px", fontSize: t.size.xs,
        cursor: onOpenMigration ? "pointer" : "not-allowed", fontWeight: 700,
        opacity: onOpenMigration ? 1 : 0.55,
        boxShadow: "0 1px 2px rgba(20,20,20,0.18)",
      }}
      title="Откроет экран Эта миграция: добавление таблиц выполняется через единые пачки"
    >
      Открыть пачки на экране "Эта миграция"
    </button>
  ) : blocked ? (
    <span style={{
      background: t.amber.base + "22", color: t.amber.base,
      padding: "2px 10px", borderRadius: t.radius.sm,
      fontSize: t.size.xs, fontWeight: 600,
    }}>
      Миграция: {obj.migration_status}
    </span>
  ) : null;

  return (
    <td colSpan={6} style={{ padding: "8px 16px 12px 32px", background: t.bg.s2 }}>
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "0 4px 8px",
      }}>
        <span style={{ fontSize: 12, color: t.text.disabled }}>
          Карточка таблицы
        </span>
        {migrateAction}
      </div>
      {obj.match_status === "DIFF" && <DiffSummary diff={obj.diff} />}
      {obj.match_status === "DIFF" && tgtMeta && (
        <TargetCompare
          srcCols={cols}
          tgtCols={(tgtMeta.columns as Record<string, unknown>[]) ?? []}
        />
      )}
      {cols.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>КОЛОНКИ ({cols.length})</div>
          <ColsTable cols={cols} />
        </div>
      )}
      {idxs.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>ИНДЕКСЫ ({idxs.length})</div>
          <IdxTable idxs={idxs} />
        </div>
      )}
      {constrs.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>ОГРАНИЧЕНИЯ ({constrs.length})</div>
          <ConstrTable constrs={constrs} />
        </div>
      )}
      {trigs.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>ТРИГГЕРЫ ({trigs.length})</div>
          <TrigTable trigs={trigs} />
        </div>
      )}
    </td>
  );
}

export function TablesTab({
  objects, snapshotId, selected, onToggle, onToggleAll,
  syncBusy, onCompare, onSync, srcSchema, tgtSchema, onOpenMigration,
}: Props) {
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("ALL");
  const [expandedObj, setExpandedObj] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);

  // Reset page on filter change
  useEffect(() => { setPage(0); }, [search, statusFilter]);

  const filtered = useMemo(() => {
    return objects.filter(o => {
      const matchSearch = o.object_name.toLowerCase().includes(search.toLowerCase());
      const matchStatus =
        statusFilter === "ALL" ? true :
        statusFilter === "OK" ? o.match_status === "MATCH" :
        statusFilter === "DIFF" ? o.match_status === "DIFF" :
        statusFilter === "MISSING" ? o.match_status === "MISSING" : true;
      return matchSearch && matchStatus;
    });
  }, [objects, search, statusFilter]);

  const paged = usePagination(filtered, pageSize, page);
  const filteredNames = filtered.map(o => o.object_name);
  const allSelected = filteredNames.length > 0 && filteredNames.every(n => selected.has(n));

  const filterBtns: StatusFilter[] = ["ALL", "OK", "DIFF", "MISSING"];
  const filterLabels: Record<StatusFilter, string> = {
    ALL: "Все", OK: "OK", DIFF: "Diff", MISSING: "Missing",
  };

  return (
    <div style={S.card}>
      <div style={S.cardHeader}>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Поиск таблиц..."
          style={{ ...S.input, width: 220 }}
        />
        <div style={{ display: "flex", gap: 4 }}>
          {filterBtns.map(f => (
            <button
              key={f}
              onClick={() => setStatusFilter(f)}
              style={statusFilter === f ? S.btnPrimary : S.btnSecondary}
            >
              {filterLabels[f]}
            </button>
          ))}
        </div>
        <span style={{ marginLeft: "auto", fontSize: 12, color: t.text.disabled }}>
          {filtered.length} / {objects.length}
        </span>
      </div>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ background: t.bg.s1 }}>
            <th style={{ ...S.th, width: 32 }}>
              <input
                type="checkbox"
                checked={allSelected}
                onChange={() => onToggleAll(filteredNames)}
              />
            </th>
            <th style={S.th}>Таблица</th>
            <th style={S.th}>Совпадение</th>
            <th style={S.th}>Миграция</th>
            <th style={S.th}>Действия</th>
          </tr>
        </thead>
        <tbody>
          {paged.map(obj => {
            const expanded = expandedObj === obj.object_name;
            return (
              <React.Fragment key={obj.object_name}>
                <tr style={S.trBorder}>
                  <td style={{ ...S.td, width: 32 }}>
                    <input
                      type="checkbox"
                      checked={selected.has(obj.object_name)}
                      onChange={() => onToggle(obj.object_name)}
                    />
                  </td>
                  <td style={S.td}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <button
                        onClick={() => setExpandedObj(expanded ? null : obj.object_name)}
                        style={{
                          background: "none", border: "none", color: t.text.disabled,
                          cursor: "pointer", fontSize: 12, padding: "0 2px",
                        }}
                      >
                        {expanded ? "▼" : "▶"}
                      </button>
                      <span style={{ color: t.text.primary, fontFamily: "monospace" }}>
                        {obj.object_name}
                      </span>
                    </div>
                  </td>
                  <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                  <td style={S.td}><MigrationBadge status={obj.migration_status} /></td>
                  <td style={S.td}>
                    <ObjectActions
                      objectType="TABLE"
                      objectName={obj.object_name}
                      matchStatus={obj.match_status}
                      syncBusy={syncBusy.has(obj.object_name)}
                      onCompare={onCompare}
                      onSync={onSync}
                      onShowDetail={() => setExpandedObj(expanded ? null : obj.object_name)}
                    />
                  </td>
                </tr>
                {expanded && (
                  <tr style={{ background: t.bg.s2 }}>
                    <TableDetail
                      obj={obj} snapshotId={snapshotId}
                      srcSchema={srcSchema} tgtSchema={tgtSchema}
                      onOpenMigration={onOpenMigration}
                    />
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
      <Pagination total={filtered.length} page={page} pageSize={pageSize} onPage={setPage} onPageSize={setPageSize} />
    </div>
  );
}
