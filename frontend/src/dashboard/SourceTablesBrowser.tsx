import React, { useEffect, useMemo, useState } from "react";
import { Icon } from "../components/ui";
import { SearchSelect } from "../components/TargetPrep/SearchSelect";
import { useApi } from "../hooks/useApi";
import { t } from "../theme";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";
import { PACK_DEFINITIONS, type TablePackAddMode } from "./packModel";
import { PackModeSegment } from "./PackModeSegment";
import { VisibleCheckbox } from "./VisibleCheckbox";

interface DbInfo {
  host:         string;
  port:         number | string;
  service_name: string;
  schema?:      string;
  configured:   boolean;
  version:      string;
  ok:           boolean;
  error:        string | null;
}

type DbInfoResp = { source: DbInfo; target: DbInfo };

interface Props {
  initialSourceSchema?: string;
  initialTargetSchema?: string;
  createEnabled?: boolean;
  onCreatePack: (
    sourceSchema: string,
    targetSchema: string,
    meta: { sourceHost: string; sourceVersion: string; targetHost: string; targetVersion: string },
    selectedTables: string[],
    packMode: TablePackAddMode,
  ) => Promise<void>;
  onAddSelected?: (
    sourceSchema: string,
    targetSchema: string,
    selectedTables: string[],
    packMode: TablePackAddMode,
  ) => Promise<void>;
}

export function SourceTablesBrowser({
  initialSourceSchema = "",
  initialTargetSchema = "",
  createEnabled = true,
  onCreatePack,
  onAddSelected,
}: Props) {
  const info = useApi<DbInfoResp>("/api/db/info");
  const srcSchemas = useApi<string[]>("/api/db/source/schemas");
  const tgtSchemas = useApi<string[]>("/api/db/target/schemas");

  const [sourceSchema, setSourceSchema] = useState("");
  const [targetSchema, setTargetSchema] = useState("");
  const [targetEdited, setTargetEdited] = useState(false);
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<"name" | "name_desc">("name");
  const [selected, setSelected] = useState<Set<string>>(() => new Set());
  const [packMode, setPackMode] = useState<TablePackAddMode>("historical");
  const [creating, setCreating] = useState(false);
  const [adding, setAdding] = useState(false);
  const [createErr, setCreateErr] = useState("");

  const tablesUrl = sourceSchema
    ? `/api/db/source/tables?schema=${encodeURIComponent(sourceSchema)}`
    : null;
  const tablesApi = useApi<string[]>(tablesUrl, { deps: [sourceSchema], enabled: !!sourceSchema });

  useEffect(() => {
    const configured = (info.data?.source?.schema || "").trim().toUpperCase();
    const initial = initialSourceSchema.trim().toUpperCase();
    const schemas = srcSchemas.data || [];
    if (!sourceSchema && initial && schemas.includes(initial)) {
      setSourceSchema(initial);
      return;
    }
    if (!sourceSchema && configured && schemas.includes(configured)) {
      setSourceSchema(configured);
    }
  }, [info.data?.source?.schema, initialSourceSchema, sourceSchema, srcSchemas.data]);

  useEffect(() => {
    if (!targetEdited && initialTargetSchema && targetSchema !== initialTargetSchema) {
      setTargetSchema(initialTargetSchema);
      return;
    }
    if (!targetEdited && sourceSchema && targetSchema !== sourceSchema) {
      setTargetSchema(sourceSchema);
    }
  }, [initialTargetSchema, sourceSchema, targetEdited, targetSchema]);

  useEffect(() => {
    setSearch("");
    setSelected(new Set());
  }, [sourceSchema]);

  const tables = tablesApi.data || [];
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    const arr = q ? tables.filter(x => x.toLowerCase().includes(q)) : tables;
    return [...arr].sort((a, b) => sort === "name_desc" ? b.localeCompare(a) : a.localeCompare(b));
  }, [tables, search, sort]);

  const source = info.data?.source;
  const target = info.data?.target;
  const selectedTables = useMemo(
    () => Array.from(selected).filter(table => tables.includes(table)).sort((a, b) => a.localeCompare(b)),
    [selected, tables],
  );
  const visibleSelectedCount = filtered.filter(table => selected.has(table)).length;
  const visibleAllSelected = filtered.length > 0 && visibleSelectedCount === filtered.length;
  const visibleSomeSelected = visibleSelectedCount > 0 && !visibleAllSelected;
  const selectedPackTitle = packMode === "cdc"
    ? PACK_DEFINITIONS.cdc.title
    : PACK_DEFINITIONS.bulk.title.toLowerCase();
  const canCreate = !!sourceSchema && !!targetSchema && !creating;
  const canAddSelected = !!onAddSelected && !!sourceSchema && !!targetSchema && selectedTables.length > 0 && !adding;

  useEffect(() => {
    setSelected(prev => {
      let changed = false;
      const next = new Set<string>();
      prev.forEach(table => {
        if (tables.includes(table)) next.add(table);
        else changed = true;
      });
      return changed ? next : prev;
    });
  }, [tables]);

  const toggleTable = (table: string) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(table)) next.delete(table);
      else next.add(table);
      return next;
    });
  };

  const toggleVisible = () => {
    setSelected(prev => {
      const next = new Set(prev);
      if (visibleAllSelected) {
        filtered.forEach(table => next.delete(table));
      } else {
        filtered.forEach(table => next.add(table));
      }
      return next;
    });
  };

  const handleCreate = async () => {
    if (!canCreate) return;
    setCreating(true);
    setCreateErr("");
    try {
      await onCreatePack(sourceSchema, targetSchema, {
        sourceHost:    source?.host || "",
        sourceVersion: source?.version || "",
        targetHost:    target?.host || "",
        targetVersion: target?.version || "",
      }, selectedTables, packMode);
    } catch (e) {
      setCreateErr(String(e instanceof Error ? e.message : e));
    } finally {
      setCreating(false);
    }
  };

  const handleAddSelected = async () => {
    if (!canAddSelected || !onAddSelected) return;
    setAdding(true);
    setCreateErr("");
    try {
      await onAddSelected(sourceSchema, targetSchema, selectedTables, packMode);
      setSelected(new Set());
    } catch (e) {
      setCreateErr(String(e instanceof Error ? e.message : e));
    } finally {
      setAdding(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <section style={panelStyle}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16, flexWrap: "wrap" }}>
          <div>
            <div style={eyebrowStyle}>Каталог источника</div>
            <h2 style={{ margin: "2px 0 6px", fontSize: 18, fontWeight: 650 }}>
              Таблицы источника
            </h2>
            <div style={{ color: t.text.muted, fontSize: 12 }}>
              {source?.configured
                ? `${source.host}:${source.port || 1521}/${source.service_name || "-"}`
                : "Источник не настроен"}
              {source?.version && <span> · Oracle {source.version}</span>}
            </div>
          </div>
          <button
            onClick={() => {
              srcSchemas.reload();
              tgtSchemas.reload();
              tablesApi.reload();
              info.reload();
            }}
            style={secondaryActionStyle(srcSchemas.loading || tablesApi.loading)}
          >
            <Icon name="rotate" size={14}/>
            Обновить
          </button>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 10, alignItems: "end", marginTop: 16 }}>
          <Field label="Source-схема" loading={srcSchemas.loading} error={srcSchemas.error}>
            <SearchSelect
              value={sourceSchema}
              onChange={setSourceSchema}
              options={srcSchemas.data || []}
              placeholder="Выберите схему"
              disabled={!source?.configured}
            />
          </Field>
          {createEnabled && (
            <>
              <Field label="Target-схема" loading={tgtSchemas.loading} error={tgtSchemas.error}>
                <SearchSelect
                  value={targetSchema}
                  onChange={v => { setTargetSchema(v); setTargetEdited(true); }}
                  options={tgtSchemas.data || []}
                  placeholder="По умолчанию как source"
                  disabled={!target?.configured}
                />
              </Field>
              <Field label="Пачка">
                <PackModeSegment value={packMode} onChange={setPackMode} />
              </Field>
              <button onClick={handleCreate} disabled={!canCreate} style={primaryActionStyle(!canCreate)}>
                <Icon name="plus" size={14}/>
                {selectedTables.length
                  ? `Создать и добавить в ${selectedPackTitle} (${selectedTables.length})`
                  : "Создать эту миграцию"}
              </button>
            </>
          )}
          {!createEnabled && onAddSelected && (
            <>
              <Field label="Пачка">
                <PackModeSegment value={packMode} onChange={setPackMode} />
              </Field>
              <button onClick={handleAddSelected} disabled={!canAddSelected} style={primaryActionStyle(!canAddSelected)}>
                <Icon name="plus" size={14}/>
                В {selectedPackTitle} ({selectedTables.length})
              </button>
            </>
          )}
        </div>
        {createErr && <div style={errorStyle}>{createErr}</div>}
      </section>

      <section style={panelStyle}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", flexWrap: "wrap", marginBottom: 10 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <Icon name="db" size={16}/>
            <span style={{ fontWeight: 650 }}>Таблицы</span>
            <span style={{ color: t.text.muted, fontFamily: t.font.mono }}>
              {sourceSchema ? `${filtered.length}/${tables.length}` : "0"}
            </span>
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center", minWidth: 320 }}>
            <div style={searchBoxStyle}>
              <Icon name="search" size={14}/>
              <input
                value={search}
                onChange={e => setSearch(e.target.value)}
                placeholder="Поиск таблицы"
                style={inputStyle}
              />
            </div>
            <select value={sort} onChange={e => setSort(e.target.value as "name" | "name_desc")} style={selectStyle}>
              <option value="name">A-Z</option>
              <option value="name_desc">Z-A</option>
            </select>
          </div>
        </div>

        <div style={{
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.lg,
          overflow: "hidden",
          background: t.bg.s1,
        }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr>
                <Th style={{ width: 84 }}>
                  <div style={selectionHeaderStyle}>
                    <SelectAllCheckbox
                      checked={visibleAllSelected}
                      indeterminate={visibleSomeSelected}
                      disabled={filtered.length === 0}
                      onChange={toggleVisible}
                    />
                    <span>Выбор</span>
                  </div>
                </Th>
                <Th>Схема</Th>
                <Th>Таблица</Th>
              </tr>
            </thead>
            <tbody>
              {!sourceSchema && (
                <EmptyRow text="Выберите source schema, чтобы загрузить таблицы"/>
              )}
              {sourceSchema && tablesApi.loading && (
                <EmptyRow text="Загрузка таблиц..."/>
              )}
              {sourceSchema && !tablesApi.loading && tablesApi.error && (
                <EmptyRow text={`Ошибка загрузки: ${tablesApi.error}`}/>
              )}
              {sourceSchema && !tablesApi.loading && !tablesApi.error && filtered.length === 0 && (
                <EmptyRow text={tables.length ? "Нет таблиц под текущий поиск" : "В схеме нет таблиц"}/>
              )}
              {sourceSchema && !tablesApi.loading && !tablesApi.error && filtered.map(table => (
                <tr key={table} style={{ borderTop: `1px solid ${t.border.subtle}` }}>
                  <Td style={{ width: 84 }}>
                    <SelectionCheckbox
                      checked={selected.has(table)}
                      disabled={false}
                      onToggle={() => toggleTable(table)}
                      ariaLabel={`Выбрать ${table}`}
                    />
                  </Td>
                  <Td mono muted>{sourceSchema}</Td>
                  <Td mono>{table}</Td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function Field({ label, loading, error, children }: {
  label: string;
  loading?: boolean;
  error?: string | null;
  children: React.ReactNode;
}) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6, minWidth: 0 }}>
      <label style={{ fontSize: 11, color: t.text.muted, fontWeight: 600, display: "flex", gap: 8 }}>
        <span>{label}</span>
        {loading && <span style={{ color: t.text.faint, fontWeight: 500 }}>загрузка...</span>}
        {!loading && error && <span style={{ color: t.tone.error, fontWeight: 500 }}>{error}</span>}
      </label>
      {children}
    </div>
  );
}

function Th({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return (
    <th style={{
      textAlign: "left",
      padding: "9px 12px",
      background: t.bg.s2,
      color: t.text.muted,
      fontSize: 10.5,
      textTransform: "uppercase",
      letterSpacing: "0.06em",
      borderBottom: `1px solid ${t.border.subtle}`,
      ...style,
    }}>
      {children}
    </th>
  );
}

function SelectAllCheckbox({
  checked,
  indeterminate,
  disabled,
  onChange,
}: {
  checked: boolean;
  indeterminate: boolean;
  disabled: boolean;
  onChange: () => void;
}) {
  return (
    <SelectionCheckbox
      checked={checked}
      indeterminate={indeterminate}
      disabled={disabled}
      onToggle={onChange}
      ariaLabel="Выбрать таблицы под текущим фильтром"
    />
  );
}

const selectionHeaderStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  gap: 6,
};

function Td({ children, mono, muted, style }: { children: React.ReactNode; mono?: boolean; muted?: boolean; style?: React.CSSProperties }) {
  return (
    <td style={{
      padding: "8px 12px",
      color: muted ? t.text.muted : t.text.primary,
      fontFamily: mono ? t.font.mono : undefined,
      whiteSpace: "nowrap",
      ...style,
    }}>
      {children}
    </td>
  );
}

function SelectionCheckbox({
  checked,
  indeterminate = false,
  disabled,
  onToggle,
  ariaLabel,
}: {
  checked: boolean;
  indeterminate?: boolean;
  disabled: boolean;
  onToggle: () => void;
  ariaLabel: string;
}) {
  return (
    <VisibleCheckbox
      checked={checked}
      indeterminate={indeterminate}
      disabled={disabled}
      onChange={onToggle}
      ariaLabel={ariaLabel}
    />
  );
}

function EmptyRow({ text }: { text: string }) {
  return (
    <tr>
      <td colSpan={3} style={{ padding: 32, textAlign: "center", color: t.text.muted }}>
        {text}
      </td>
    </tr>
  );
}

const panelStyle: React.CSSProperties = {
  background: t.bg.s1,
  border: `1px solid ${t.border.subtle}`,
  borderRadius: t.radius.lg,
  padding: 16,
  boxShadow: t.shadow.s1,
};

const eyebrowStyle: React.CSSProperties = {
  fontSize: 10.5,
  textTransform: "uppercase",
  letterSpacing: "0.07em",
  fontWeight: 700,
  color: t.text.muted,
};

const searchBoxStyle: React.CSSProperties = {
  height: 30,
  display: "flex",
  alignItems: "center",
  gap: 6,
  flex: 1,
  minWidth: 220,
  padding: "0 9px",
  border: `1px solid ${t.border.subtle}`,
  borderRadius: t.radius.sm,
  background: t.bg.s2,
  color: t.text.muted,
};

const inputStyle: React.CSSProperties = {
  flex: 1,
  minWidth: 0,
  border: "none",
  background: "transparent",
  color: t.text.primary,
  fontSize: 12,
};

const selectStyle: React.CSSProperties = {
  height: 30,
  border: `1px solid ${t.border.subtle}`,
  borderRadius: t.radius.sm,
  background: t.bg.s2,
  color: t.text.primary,
  padding: "0 8px",
  fontSize: 12,
};

const errorStyle: React.CSSProperties = {
  marginTop: 10,
  color: t.tone.error,
  background: t.tone.errorSoft,
  border: `1px solid color-mix(in oklab, ${t.tone.error} 35%, transparent)`,
  borderRadius: t.radius.sm,
  padding: "8px 10px",
  fontSize: 12,
};
