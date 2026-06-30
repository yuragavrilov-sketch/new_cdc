import React from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";
import { OBJECT_TYPES, type ObjectType, type SchemaObject } from "./types";

export type SortKey = "priority" | "size" | "progress" | "name" | "type";
export type StatusFilter = "all" | "running" | "issues" | "queued" | "done";
export type KeyFilter = "all" | "pk" | "uk" | "no_key";
export type SuppFilter = "all" | "supp" | "no_supp";

interface Props {
  objects:      SchemaObject[];
  filtered:     SchemaObject[];
  typeFilter:   ObjectType | "all";
  onTypeFilter: (v: ObjectType | "all") => void;
  statusFilter: StatusFilter;
  onStatusFilter: (v: StatusFilter) => void;
  keyFilter:    KeyFilter;
  onKeyFilter:  (v: KeyFilter) => void;
  suppFilter:   SuppFilter;
  onSuppFilter: (v: SuppFilter) => void;
  search:       string;
  onSearch:     (v: string) => void;
  sort:         SortKey;
  onSort:       (v: SortKey) => void;
  tablesOnly?:  boolean;
}

const SORT_LABELS: Record<SortKey, string> = {
  priority: "Приоритет",
  size:     "Размер",
  progress: "Прогресс",
  name:     "Имя",
  type:     "Тип",
};

export function ObjectFilters({
  objects, filtered, typeFilter, onTypeFilter, statusFilter, onStatusFilter,
  keyFilter, onKeyFilter, suppFilter, onSuppFilter,
  search, onSearch, sort, onSort,
  tablesOnly = false,
}: Props) {
  const typeCounts: Record<string, number> = {};
  objects.forEach(o => { typeCounts[o.type] = (typeCounts[o.type] || 0) + 1; });

  const statusCounts = { all: objects.length, running: 0, error: 0, warn: 0, queued: 0, done: 0 };
  objects.forEach(o => {
    if (o.status === "running") statusCounts.running++;
    else if (o.status === "error") statusCounts.error++;
    else if (o.status === "warn") statusCounts.warn++;
    else if (o.status === "queued") statusCounts.queued++;
    else if (o.status === "done") statusCounts.done++;
  });

  // PK/UK/NO KEY и SUPP/NO SUPP считаем только по TABLE-объектам.
  // Если для таблицы поле ещё не загружено (undefined) — она не попадает в
  // конкретный счётчик, но и не отсекается фильтром "all".
  const tables = objects.filter(o => o.type === "TABLE");
  const keyCounts = { all: tables.length, pk: 0, uk: 0, no_key: 0 };
  const suppCounts = { all: tables.length, supp: 0, no_supp: 0 };
  tables.forEach(o => {
    if (o.hasPk) keyCounts.pk++;
    else if (o.hasUk) keyCounts.uk++;
    else if (o.hasPk === false && o.hasUk === false) keyCounts.no_key++;

    if (o.hasSuppLog === true) suppCounts.supp++;
    else if (o.hasSuppLog === false) suppCounts.no_supp++;
  });

  const typePills = (Object.keys(OBJECT_TYPES) as ObjectType[]).filter(k => typeCounts[k] > 0);

  return (
    <>
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "center",
        gap: 16, marginBottom: 10, flexWrap: "wrap",
      }}>
        <div style={{ fontSize: 14, fontWeight: 600, letterSpacing: "-0.01em",
                      display: "flex", alignItems: "baseline", gap: 6 }}>
          {tablesOnly ? "Таблицы" : "Объекты схемы"}
          <span style={{ fontFamily: t.font.mono, fontSize: 12, color: t.text.muted, fontWeight: 500 }}>
            {filtered.length}
          </span>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <SearchInput value={search} onChange={onSearch}/>
          <Segmented
            value={statusFilter}
            onChange={onStatusFilter}
            options={[
              { v: "all",     l: "Все",      c: statusCounts.all },
              { v: "running", l: "Идут",     c: statusCounts.running },
              { v: "issues",  l: "Проблемы", c: statusCounts.error + statusCounts.warn },
              { v: "queued",  l: "Очередь",  c: statusCounts.queued },
              { v: "done",    l: "Готово",   c: statusCounts.done },
            ]}
          />
          <SortPicker value={sort} onChange={onSort} tablesOnly={tablesOnly}/>
        </div>
      </div>

      {!tablesOnly && (
        <div style={{
          display: "flex", flexWrap: "wrap", gap: 4,
          marginBottom: 10, padding: "4px 0",
        }}>
          <TypePill active={typeFilter === "all"} label="Все типы" count={objects.length} onClick={() => onTypeFilter("all")}/>
          {typePills.map(k => (
            <TypePill
              key={k}
              active={typeFilter === k}
              label={OBJECT_TYPES[k].label}
              count={typeCounts[k]}
              onClick={() => onTypeFilter(k)}
            />
          ))}
        </div>
      )}

      {/* Метки таблиц: PK/UK/NO KEY и SUPP/NO SUPP. Видны только когда есть
          табличные объекты — для DDL-объектов эти фильтры бессмысленны. */}
      {tables.length > 0 && (
        <div style={{
          display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center",
          marginBottom: 10,
        }}>
          <span style={{ fontSize: 11, color: t.text.muted }}>Ключи</span>
          <Segmented
            value={keyFilter}
            onChange={onKeyFilter}
            options={[
              { v: "all",    l: "Все",    c: keyCounts.all    },
              { v: "pk",     l: "PK",     c: keyCounts.pk     },
              { v: "uk",     l: "UK",     c: keyCounts.uk     },
              { v: "no_key", l: "NO KEY", c: keyCounts.no_key },
            ]}
          />
          <span style={{ fontSize: 11, color: t.text.muted, marginLeft: 4 }}>Supp&nbsp;log</span>
          <Segmented
            value={suppFilter}
            onChange={onSuppFilter}
            options={[
              { v: "all",     l: "Все",     c: suppCounts.all     },
              { v: "supp",    l: "SUPP",    c: suppCounts.supp    },
              { v: "no_supp", l: "NO SUPP", c: suppCounts.no_supp },
            ]}
          />
        </div>
      )}
    </>
  );
}

function SearchInput({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <div style={{
      display: "inline-flex", alignItems: "center", gap: 6,
      background: t.bg.s1,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.sm,
      padding: "4px 10px",
      minWidth: 220,
    }}>
      <span style={{ color: t.text.muted, display: "flex" }}>
        <Icon name="search" size={13}/>
      </span>
      <input
        type="text"
        placeholder="Поиск по имени…"
        value={value}
        onChange={e => onChange(e.target.value)}
        style={{
          border: 0, outline: 0, background: "transparent",
          flex: 1, fontSize: 12,
        }}
      />
    </div>
  );
}

function Segmented<V extends string>({ value, onChange, options }: {
  value: V;
  onChange: (v: V) => void;
  options: { v: V; l: string; c: number }[];
}) {
  return (
    <div style={{
      display: "inline-flex",
      background: t.bg.s1,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.sm,
      padding: 2, gap: 0,
    }}>
      {options.map(o => {
        const isOn = value === o.v;
        return (
          <button key={o.v} onClick={() => onChange(o.v)} style={{
            padding: "3px 9px", fontSize: "11.5px", borderRadius: 4,
            background: isOn ? t.bg.s3 : "transparent",
            color: isOn ? t.text.primary : t.text.muted,
            fontWeight: 500,
            display: "inline-flex", alignItems: "center", gap: 5,
            border: "none", cursor: "pointer",
          }}>
            {o.l} <span style={{
              fontFamily: t.font.mono, fontSize: 10,
              color: isOn ? t.text.secondary : t.text.muted,
            }}>{o.c}</span>
          </button>
        );
      })}
    </div>
  );
}

function SortPicker({ value, onChange, tablesOnly }: {
  value: SortKey;
  onChange: (v: SortKey) => void;
  tablesOnly: boolean;
}) {
  const keys = (Object.keys(SORT_LABELS) as SortKey[]).filter(k => !tablesOnly || k !== "type");
  const valueForSelect = tablesOnly && value === "type" ? "name" : value;
  return (
    <div style={{
      display: "inline-flex", alignItems: "center", gap: 6,
      background: t.bg.s1,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.sm,
      padding: "3px 8px",
    }}>
      <span style={{ fontSize: 11, color: t.text.muted }}>Сорт.</span>
      <select value={valueForSelect} onChange={e => onChange(e.target.value as SortKey)} style={{
        border: 0, outline: 0, background: "transparent",
        fontSize: 12, padding: "2px 4px", cursor: "pointer",
      }}>
        {keys.map(k => (
          <option key={k} value={k}>{SORT_LABELS[k]}</option>
        ))}
      </select>
    </div>
  );
}

function TypePill({ active, label, count, onClick }: {
  active:  boolean;
  label:   string;
  count:   number;
  onClick: () => void;
}) {
  return (
    <button onClick={onClick} style={{
      display: "inline-flex", alignItems: "center", gap: 5,
      padding: "3px 9px", borderRadius: t.radius.pill,
      background: active ? t.text.primary : t.bg.s1,
      color:      active ? t.text.inverse : t.text.secondary,
      border:     `1px solid ${active ? t.text.primary : t.border.subtle}`,
      fontSize: "11.5px", fontWeight: 500,
      cursor: "pointer",
      transition: "all 80ms",
    }}>
      <span>{label}</span>
      <span style={{
        fontFamily: t.font.mono, fontSize: "10.5px",
        padding: "0 4px",
        background: active ? "rgba(255,255,255,0.18)" : t.bg.s2,
        color: active ? "rgba(255,255,255,0.85)" : t.text.muted,
        borderRadius: t.radius.pill,
      }}>
        {count}
      </span>
    </button>
  );
}
