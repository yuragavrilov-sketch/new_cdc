import React, { useState, useEffect, useMemo } from "react";
import { S } from "./styles";
import { MatchBadge } from "./StatusBadges";
import { ObjectActions } from "./ObjectActions";
import { CatalogObject } from "./TablesTab";
import { Pagination, usePagination } from "./Pagination";
import { t } from "../../theme";

interface Props {
  objects: CatalogObject[];
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
}

function detectOtherType(meta: Record<string, unknown>): "SEQUENCE" | "SYNONYM" | "TYPE" {
  if ("increment_by" in meta) return "SEQUENCE";
  if ("table_name" in meta) return "SYNONYM";
  return "TYPE";
}

function SequenceDetail({ meta }: { meta: Record<string, unknown> }) {
  const fields: { label: string; key: string }[] = [
    { label: "Min Value", key: "min_value" },
    { label: "Max Value", key: "max_value" },
    { label: "Increment By", key: "increment_by" },
    { label: "Cache Size", key: "cache_size" },
    { label: "Last Number", key: "last_number" },
  ];
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "repeat(5, 1fr)",
      gap: 10,
      padding: "8px 0",
    }}>
      {fields.map(f => (
        <div key={f.key} style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          <span style={S.label}>{f.label}</span>
          <span style={{ fontSize: 13, color: t.text.primary, fontFamily: "monospace" }}>
            {meta[f.key] != null ? String(meta[f.key]) : "—"}
          </span>
        </div>
      ))}
    </div>
  );
}

function SynonymDetail({ meta }: { meta: Record<string, unknown> }) {
  const owner = meta.table_owner as string | null;
  const table = meta.table_name as string | null;
  const dblink = meta.db_link as string | null;
  const ref = [
    owner ? `${owner}.` : "",
    table ?? "",
    dblink ? `@${dblink}` : "",
  ].join("");
  return (
    <div style={{ padding: "8px 0" }}>
      <span style={S.label}>Ссылается на</span>
      <div style={{ marginTop: 4, fontSize: 13, color: t.text.primary, fontFamily: "monospace" }}>
        {ref || "—"}
      </div>
    </div>
  );
}

function TypeDetail({ meta }: { meta: Record<string, unknown> }) {
  const source = (meta.source as string) ?? (meta.spec_source as string) ?? "";
  return (
    <div style={{
      background: t.bg.s2,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: 6,
      marginTop: 4,
      overflow: "hidden",
    }}>
      <div style={{
        padding: "5px 12px",
        fontSize: 11,
        fontWeight: 700,
        color: t.text.muted,
        letterSpacing: 0.5,
        background: t.bg.s1,
        borderBottom: `1px solid ${t.border.subtle}`,
      }}>
        ИСХОДНЫЙ КОД
      </div>
      <pre style={{
        margin: 0, padding: "10px 12px",
        fontSize: 11, color: t.text.secondary,
        fontFamily: "monospace", whiteSpace: "pre-wrap",
        overflowX: "auto", maxHeight: 300,
      }}>
        {source}
      </pre>
    </div>
  );
}

function OtherDetail({ obj }: { obj: CatalogObject }) {
  const meta = obj.metadata;
  const kind = detectOtherType(meta);
  return (
    <td colSpan={4} style={{ padding: "8px 16px 12px 32px", background: t.bg.s2 }}>
      {kind === "SEQUENCE" && <SequenceDetail meta={meta} />}
      {kind === "SYNONYM" && <SynonymDetail meta={meta} />}
      {kind === "TYPE" && <TypeDetail meta={meta} />}
    </td>
  );
}

export function OtherTab({ objects, syncBusy, onCompare, onSync }: Props) {
  const [search, setSearch] = useState("");
  const [expandedObj, setExpandedObj] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);

  useEffect(() => { setPage(0); }, [search]);

  const filtered = useMemo(() => {
    return objects.filter(o =>
      o.object_name.toLowerCase().includes(search.toLowerCase())
    );
  }, [objects, search]);

  const paged = usePagination(filtered, pageSize, page);

  const typeBadge = (meta: Record<string, unknown>) => {
    const kind = detectOtherType(meta);
    switch (kind) {
      case "SEQUENCE":
        return <span style={S.badge(`${t.amber.dim}222`, t.amber.dim)}>SEQUENCE</span>;
      case "SYNONYM":
        return <span style={S.badge(`${t.blue.dim}22`, t.blue.dim)}>SYNONYM</span>;
      case "TYPE":
        return <span style={S.badge(`${t.purple.base}22`, t.purple.base)}>TYPE</span>;
    }
  };

  const objTypeStr = (meta: Record<string, unknown>) => detectOtherType(meta);

  return (
    <div style={S.card}>
      <div style={S.cardHeader}>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Поиск объектов..."
          style={{ ...S.input, width: 220 }}
        />
        <span style={{ marginLeft: "auto", fontSize: 12, color: t.text.disabled }}>
          {filtered.length} / {objects.length}
        </span>
      </div>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ background: t.bg.s1 }}>
            <th style={S.th}>Имя</th>
            <th style={S.th}>Тип</th>
            <th style={S.th}>Совпадение</th>
            <th style={S.th}>Действия</th>
          </tr>
        </thead>
        <tbody>
          {paged.map(obj => {
            const expanded = expandedObj === obj.object_name;
            return (
              <React.Fragment key={obj.object_name}>
                <tr style={S.trBorder}>
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
                  <td style={S.td}>{typeBadge(obj.metadata)}</td>
                  <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                  <td style={S.td}>
                    <ObjectActions
                      objectType={objTypeStr(obj.metadata)}
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
                    <OtherDetail obj={obj} />
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
