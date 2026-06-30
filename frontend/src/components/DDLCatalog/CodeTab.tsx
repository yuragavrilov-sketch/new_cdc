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

type CodeTypeFilter = "ALL" | "FUNCTION" | "PROCEDURE" | "PACKAGE";

function detectCodeType(obj: { _type?: string; metadata: Record<string, unknown> }): string {
  if (obj._type) return obj._type;
  const meta = obj.metadata;
  if ("spec_source" in meta) return "PACKAGE";
  if ("source" in meta && "body_source" in meta) return "TYPE";
  return "FUNCTION";
}

function CodeDiffSummary({ diff, codeType, srcMeta, tgtMeta }: {
  diff: Record<string, unknown>; codeType: string;
  srcMeta: Record<string, unknown>; tgtMeta: Record<string, unknown> | null;
}) {
  if (!diff || diff.ok === true) return null;
  const items: string[] = [];
  if (codeType === "PACKAGE") {
    if (diff.spec_match === false) items.push("Спецификация отличается");
    if (diff.body_match === false) items.push("Тело пакета отличается");
  } else if (codeType === "TYPE") {
    if (diff.source_match === false) items.push("Спецификация типа отличается");
    if (diff.body_match === false) items.push("Тело типа отличается");
  } else {
    if (diff.code_match === false) items.push("Исходный код отличается");
  }
  if (items.length === 0) return null;

  // Get source and target code for side-by-side
  const srcCode = (srcMeta.source_code ?? srcMeta.spec_source ?? srcMeta.source ?? "") as string;
  const tgtCode = tgtMeta
    ? (tgtMeta.source_code ?? tgtMeta.spec_source ?? tgtMeta.source ?? "") as string
    : "";

  const preStyle: React.CSSProperties = {
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
      {tgtMeta && srcCode && tgtCode && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginTop: 8 }}>
          <div style={{ border: `1px solid ${t.border.subtle}`, borderRadius: 4, overflow: "hidden" }}>
            <div style={{ padding: "4px 10px", fontSize: 10, fontWeight: 700, color: t.blue.base, background: t.bg.s1 }}>SOURCE</div>
            <pre style={preStyle}>{srcCode}</pre>
          </div>
          <div style={{ border: `1px solid ${t.border.subtle}`, borderRadius: 4, overflow: "hidden" }}>
            <div style={{ padding: "4px 10px", fontSize: 10, fontWeight: 700, color: t.amber.base, background: t.bg.s1 }}>TARGET</div>
            <pre style={preStyle}>{tgtCode}</pre>
          </div>
        </div>
      )}
    </div>
  );
}

function CodeDetail({ obj, snapshotId }: { obj: CatalogObject; snapshotId: number | null }) {
  const [bodyOpen, setBodyOpen] = useState(false);
  const [tgtMeta, setTgtMeta] = useState<Record<string, unknown> | null>(null);
  const meta = obj.metadata;
  const codeType = detectCodeType(obj);

  useEffect(() => {
    if (!snapshotId || obj.match_status !== "DIFF") return;
    fetch(`/api/catalog/objects/${obj.object_name}/detail?snapshot_id=${snapshotId}&type=${codeType}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => d && setTgtMeta(d.target ?? null))
      .catch(() => {});
  }, [obj.object_name, obj.match_status, snapshotId, codeType]);

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
  const preStyle: React.CSSProperties = {
    margin: 0, padding: "10px 12px",
    fontSize: 11, color: t.text.secondary,
    fontFamily: "monospace", whiteSpace: "pre-wrap",
    overflowX: "auto", maxHeight: 400,
  };

  const argCount = meta.argument_count as number | undefined;

  return (
    <td colSpan={5} style={{ padding: "8px 16px 12px 32px", background: t.bg.s2 }}>
      {obj.match_status === "DIFF" && <CodeDiffSummary diff={obj.diff} codeType={codeType} srcMeta={meta} tgtMeta={tgtMeta} />}
      {(codeType === "FUNCTION" || codeType === "PROCEDURE") && argCount != null && (
        <div style={{ marginBottom: 6, fontSize: 12, color: t.text.muted }}>
          Аргументов: <span style={{ color: t.text.secondary, fontWeight: 600 }}>{argCount}</span>
        </div>
      )}

      {codeType === "PACKAGE" ? (
        <>
          <div style={sectionStyle}>
            <div style={sectionHeader}>СПЕЦИФИКАЦИЯ</div>
            <pre style={preStyle}>{(meta.spec_source as string) ?? ""}</pre>
          </div>
          <div style={{ ...sectionStyle, marginTop: 8 }}>
            <div
              style={{ ...sectionHeader, cursor: "pointer", display: "flex", alignItems: "center", gap: 6 }}
              onClick={() => setBodyOpen(v => !v)}
            >
              <span>{bodyOpen ? "▼" : "▶"}</span>
              <span>ТЕЛО ПАКЕТА</span>
            </div>
            {bodyOpen && (
              <pre style={preStyle}>{(meta.body_source as string) ?? ""}</pre>
            )}
          </div>
        </>
      ) : codeType === "TYPE" ? (
        <>
          {meta.source && (
            <div style={sectionStyle}>
              <div style={sectionHeader}>СПЕЦИФИКАЦИЯ</div>
              <pre style={preStyle}>{meta.source as string}</pre>
            </div>
          )}
          {meta.body_source && (
            <div style={{ ...sectionStyle, marginTop: 8 }}>
              <div
                style={{ ...sectionHeader, cursor: "pointer", display: "flex", alignItems: "center", gap: 6 }}
                onClick={() => setBodyOpen(v => !v)}
              >
                <span>{bodyOpen ? "▼" : "▶"}</span>
                <span>ТЕЛО</span>
              </div>
              {bodyOpen && (
                <pre style={preStyle}>{meta.body_source as string}</pre>
              )}
            </div>
          )}
        </>
      ) : (
        <div style={sectionStyle}>
          <div style={sectionHeader}>ИСХОДНЫЙ КОД</div>
          <pre style={preStyle}>{(meta.source_code as string) ?? (meta.source as string) ?? ""}</pre>
        </div>
      )}
    </td>
  );
}

export function CodeTab({ objects, snapshotId, syncBusy, onCompare, onSync }: Props) {
  const [search, setSearch] = useState("");
  const [typeFilter, setTypeFilter] = useState<CodeTypeFilter>("ALL");
  const [expandedObj, setExpandedObj] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);

  useEffect(() => { setPage(0); }, [search, typeFilter]);

  const filtered = useMemo(() => {
    return objects.filter(o => {
      const matchSearch = o.object_name.toLowerCase().includes(search.toLowerCase());
      if (!matchSearch) return false;
      if (typeFilter === "ALL") return true;
      const ct = detectCodeType(o);
      return ct === typeFilter;
    });
  }, [objects, search, typeFilter]);

  const paged = usePagination(filtered, pageSize, page);

  const typeFilterBtns: CodeTypeFilter[] = ["ALL", "FUNCTION", "PROCEDURE", "PACKAGE"];
  const typeFilterLabels: Record<CodeTypeFilter, string> = {
    ALL: "Все", FUNCTION: "Function", PROCEDURE: "Procedure", PACKAGE: "Package",
  };

  return (
    <div style={S.card}>
      <div style={S.cardHeader}>
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Поиск объектов..."
          style={{ ...S.input, width: 220 }}
        />
        <div style={{ display: "flex", gap: 4 }}>
          {typeFilterBtns.map(f => (
            <button
              key={f}
              onClick={() => setTypeFilter(f)}
              style={typeFilter === f ? S.btnPrimary : S.btnSecondary}
            >
              {typeFilterLabels[f]}
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
            const codeType = detectCodeType(obj);
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
                    <span style={S.badge(`${t.purple.base}22`, t.purple.base)}>{codeType}</span>
                  </td>
                  <td style={{ ...S.td, color: t.text.secondary }}>{obj.oracle_status}</td>
                  <td style={S.td}><MatchBadge status={obj.match_status} /></td>
                  <td style={S.td}>
                    <ObjectActions
                      objectType={codeType}
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
                    <CodeDetail obj={obj} snapshotId={snapshotId} />
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
