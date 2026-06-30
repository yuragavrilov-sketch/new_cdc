import React, { useState, useEffect, useMemo } from "react";
import { S } from "./styles";
import { MatchBadge } from "./StatusBadges";
import { ObjectActions } from "./ObjectActions";
import { Pagination, usePagination } from "./Pagination";
import { CatalogObject } from "./TablesTab";
import { t } from "../../theme";

interface Props {
  objects: CatalogObject[];
  snapshotId: number | null;
  syncBusy: Set<string>;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
}

function isMView(meta: Record<string, unknown>): boolean {
  return "refresh_type" in meta;
}

function ViewDiffSummary({ diff, srcSql, tgtSql }: { diff: Record<string, unknown>; srcSql?: string; tgtSql?: string }) {
  if (!diff || diff.ok === true) return null;
  const items: string[] = [];
  if (diff.sql_match === false) items.push("SQL-текст отличается");
  if (diff.status_match === false) items.push("Статус отличается (VALID/INVALID)");
  if (diff.refresh_match === false) items.push("Тип refresh отличается");
  if (items.length === 0) return null;

  const diffStyle: React.CSSProperties = {
    margin: 0, padding: "8px 10px", fontSize: 10, color: t.text.secondary,
    fontFamily: "monospace", whiteSpace: "pre-wrap", overflowX: "auto",
    maxHeight: 250, background: t.bg.s2, borderTop: `1px solid ${t.border.subtle}`,
  };

  return (
    <div style={{
      background: t.bg.s2, border: `1px solid ${`color-mix(in oklab, ${t.amber.dim} 27%, transparent)`}`, borderRadius: 6,
      padding: "10px 14px", marginBottom: 6,
    }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: t.amber.base, marginBottom: 4 }}>РАЗЛИЧИЯ С ТАРГЕТОМ</div>
      {items.map((item, i) => (
        <div key={i} style={{ fontSize: 11, color: t.amber.fg }}>{item}</div>
      ))}
      {diff.sql_match === false && srcSql && tgtSql && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginTop: 8 }}>
          <div style={{ border: `1px solid ${t.border.subtle}`, borderRadius: 4, overflow: "hidden" }}>
            <div style={{ padding: "4px 10px", fontSize: 10, fontWeight: 700, color: t.blue.base, background: t.bg.s1 }}>SOURCE</div>
            <pre style={diffStyle}>{srcSql}</pre>
          </div>
          <div style={{ border: `1px solid ${t.border.subtle}`, borderRadius: 4, overflow: "hidden" }}>
            <div style={{ padding: "4px 10px", fontSize: 10, fontWeight: 700, color: t.amber.base, background: t.bg.s1 }}>TARGET</div>
            <pre style={diffStyle}>{tgtSql}</pre>
          </div>
        </div>
      )}
    </div>
  );
}

function ViewDetail({ obj, snapshotId }: { obj: CatalogObject; snapshotId: number | null }) {
  const [tgtMeta, setTgtMeta] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    if (!snapshotId || obj.match_status !== "DIFF") return;
    const objType = isMView(obj.metadata) ? "MVIEW" : "VIEW";
    fetch(`/api/catalog/objects/${obj.object_name}/detail?snapshot_id=${snapshotId}&type=${objType}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => d && setTgtMeta(d.target ?? null))
      .catch(() => {});
  }, [obj.object_name, obj.match_status, snapshotId]);

  const meta = obj.metadata;
  const cols = (meta.columns as Record<string, unknown>[]) ?? [];
  const sql = (meta.sql_text as string) ?? "";
  const mview = isMView(meta);
  const tgtSql = tgtMeta ? (tgtMeta.sql_text as string) ?? "" : "";

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

  return (
    <td colSpan={5} style={{ padding: "8px 16px 12px 32px", background: t.bg.s2 }}>
      {obj.match_status === "DIFF" && <ViewDiffSummary diff={obj.diff} srcSql={sql} tgtSql={tgtSql} />}
      {mview && (
        <div style={{ fontSize: 11, color: t.text.muted, marginBottom: 6 }}>
          Refresh: <span style={{ color: t.text.secondary }}>{(meta.refresh_type as string) ?? "—"}</span>
          {meta.last_refresh ? <span style={{ marginLeft: 12 }}>Last: <span style={{ color: t.text.secondary }}>{String(meta.last_refresh)}</span></span> : null}
        </div>
      )}
      {cols.length > 0 && (
        <div style={sectionStyle}>
          <div style={sectionHeader}>КОЛОНКИ ({cols.length})</div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                {["#", "Имя", "Тип", "Nullable"].map(h => (
                  <th key={h} style={S.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {cols.map((c, i) => (
                <tr key={i} style={S.trBorder}>
                  <td style={{ ...S.td, color: t.text.disabled }}>{i + 1}</td>
                  <td style={{ ...S.td, color: t.text.primary }}>{(c.name ?? c.column_name) as string}</td>
                  <td style={{ ...S.td, color: t.text.secondary }}>
                    {c.data_type as string}{c.data_length ? `(${c.data_length})` : ""}
                  </td>
                  <td style={S.td}>
                    {(c.nullable === true || c.nullable === "Y")
                      ? <span style={{ color: t.green.base, fontSize: 11 }}>Y</span>
                      : <span style={{ color: t.red.base, fontSize: 11 }}>N</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {sql && (
        <div style={{ ...sectionStyle, marginTop: 8 }}>
          <div style={sectionHeader}>SQL</div>
          <pre style={{
            margin: 0, padding: "10px 12px",
            fontSize: 11, color: t.text.secondary,
            fontFamily: "monospace", whiteSpace: "pre-wrap",
            overflowX: "auto", maxHeight: 300,
          }}>
            {sql}
          </pre>
        </div>
      )}
    </td>
  );
}

export function ViewsTab({ objects, snapshotId, syncBusy, onCompare, onSync }: Props) {
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

  return (
    <div style={S.card}>
      <div style={S.cardHeader}>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Поиск views..."
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
            <th style={S.th}>Oracle статус</th>
            <th style={S.th}>Совпадение</th>
            <th style={S.th}>Действия</th>
          </tr>
        </thead>
        <tbody>
          {paged.map(obj => {
            const expanded = expandedObj === obj.object_name;
            const mview = isMView(obj.metadata);
            const objType = mview ? "MVIEW" : "VIEW";
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
                  <td style={S.td}>
                    {mview
                      ? <span style={S.badge(`${t.blue.base}22`, t.blue.base)}>MVIEW</span>
                      : <span style={S.badge(`${t.blue.base}22`, t.blue.base)}>VIEW</span>
                    }
                  </td>
                  <td style={{ ...S.td, color: t.text.secondary }}>{obj.oracle_status}</td>
                  <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                  <td style={S.td}>
                    <ObjectActions
                      objectType={objType}
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
                    <ViewDetail obj={obj} snapshotId={snapshotId} />
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
