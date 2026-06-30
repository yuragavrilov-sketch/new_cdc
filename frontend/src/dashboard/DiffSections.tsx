import React from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";
import type { DdlDetailResp } from "./api";

/** Renders type-appropriate diff sections inside ObjectDrawer.
 *  Branches: TABLE / VIEW / MVIEW / PACKAGE / PROCEDURE / FUNCTION /
 *  TYPE / SEQUENCE / SYNONYM / TRIGGER / fallback (raw JSON).
 */
export function DiffSections({ detail }: { detail: DdlDetailResp }) {
  const type = detail.object_type;
  const diff = detail.diff || {};
  const src = (detail.source?.metadata || {}) as Record<string, unknown>;
  const tgt = (detail.target?.metadata || {}) as Record<string, unknown>;
  const ms  = detail.match_status || "UNKNOWN";

  if (!detail.found) {
    return (
      <Section title="Нет данных в snapshot">
        <Empty text="Объект не найден в последнем DDL snapshot. Загрузите snapshot, чтобы увидеть детали."/>
      </Section>
    );
  }

  return (
    <>
      <StatusBanner status={ms}/>
      {(() => {
        switch (type) {
          case "TABLE":             return <TableDiff diff={diff} src={src} tgt={tgt}/>;
          case "VIEW":
          case "MATERIALIZED VIEW": return <SqlTextDiff diff={diff} src={src} tgt={tgt}/>;
          case "PACKAGE":           return <PackageDiff diff={diff} src={src} tgt={tgt}/>;
          case "PROCEDURE":
          case "FUNCTION":          return <CodeDiff diff={diff} src={src} tgt={tgt}/>;
          case "TRIGGER":           return <TriggerDiff diff={diff} src={src} tgt={tgt}/>;
          case "INDEX":             return <IndexDiff diff={diff} src={src} tgt={tgt}/>;
          case "TYPE":              return <TypeDiff diff={diff} src={src} tgt={tgt}/>;
          case "SEQUENCE":          return <FieldDiff diff={diff} src={src} tgt={tgt}/>;
          case "SYNONYM":           return <FieldDiff diff={diff} src={src} tgt={tgt}/>;
          case "DATABASE LINK":     return <FieldDiff diff={diff} src={src} tgt={tgt}/>;
          case "JOB":               return <JobDiff   diff={diff} src={src} tgt={tgt}/>;
          default:                  return <RawDiff diff={diff}/>;
        }
      })()}
    </>
  );
}

/** Top banner that summarises the match state. Shows for every object so
 *  the user immediately sees "✓ совпадает" / "⚠ отличается" / etc. */
function StatusBanner({ status }: { status: string }) {
  const cfg = {
    MATCH:   { tone: "ok"    as const, label: "DDL совпадает",
               hint: "Source и target идентичны — миграция объекта не требуется." },
    DIFF:    { tone: "warn"  as const, label: "DDL отличается",
               hint: "Поля и/или тело различаются — см. блоки ниже." },
    MISSING: { tone: "info"  as const, label: "Нет в target",
               hint: "Есть в source, не создан в target. Кнопка «Создать на target» поставит CREATE в очередь." },
    EXTRA:   { tone: "warn"  as const, label: "Только в target",
               hint: "Объект есть в target, но отсутствует в source — возможно лишний." },
    UNKNOWN: { tone: "info"  as const, label: "Статус неизвестен",
               hint: "Сравнение не выполнялось — загрузите snapshot." },
  }[status] || { tone: "info" as const, label: status, hint: "" };

  const bg =
    cfg.tone === "ok"   ? t.tone.okSoft   :
    cfg.tone === "warn" ? t.tone.warnSoft :
                          t.tone.infoSoft;
  const fg =
    cfg.tone === "ok"   ? t.tone.ok       :
    cfg.tone === "warn" ? t.tone.warn     :
                          t.tone.info;

  return (
    <div style={{
      background: bg,
      border:     `1px solid color-mix(in oklab, ${fg} 30%, transparent)`,
      borderRadius: t.radius.lg,
      padding: "10px 14px",
      display: "flex", gap: 10, alignItems: "center",
    }}>
      <span style={{ color: fg, display: "grid", placeItems: "center" }}>
        <Icon name={cfg.tone === "ok" ? "check" : "warn"} size={14}/>
      </span>
      <div>
        <div style={{ fontSize: "12.5px", fontWeight: 600, color: fg }}>{cfg.label}</div>
        {cfg.hint && (
          <div style={{ fontSize: 11, color: t.text.muted, marginTop: 2 }}>{cfg.hint}</div>
        )}
      </div>
    </div>
  );
}

function TriggerDiff({ diff, src, tgt }: {
  diff: Record<string, unknown>;
  src:  Record<string, unknown>;
  tgt:  Record<string, unknown>;
}) {
  const whenMatch = diff.when_match !== false;
  const bodyMatch = diff.body_match !== false;
  return (
    <>
      <FieldDiff diff={diff} src={src} tgt={tgt}/>
      <Section title="Сводка">
        <KeyValueList items={[
          { k: "When clause", v: whenMatch ? "совпадает" : "отличается", tone: whenMatch ? "ok" : "warn" },
          { k: "Body",        v: bodyMatch ? "совпадает" : "отличается", tone: bodyMatch ? "ok" : "warn" },
        ]}/>
      </Section>
      <Section title="Body — Source" tone={bodyMatch ? "ok" : "warn"}>
        <CodeBlock code={(src.trigger_body as string) || "—"}/>
      </Section>
      <Section title="Body — Target" tone={bodyMatch ? "ok" : "warn"}>
        <CodeBlock code={(tgt.trigger_body as string) || "—"}/>
      </Section>
    </>
  );
}

function IndexDiff({ diff, src, tgt }: {
  diff: Record<string, unknown>;
  src:  Record<string, unknown>;
  tgt:  Record<string, unknown>;
}) {
  const srcCols = (diff.src_cols as Array<[string, boolean]> | undefined)
    ?? ((src.columns as Array<{ name: string; descending?: boolean }> | undefined)
        ?? []).map(c => [c.name, !!c.descending] as [string, boolean]);
  const tgtCols = (diff.tgt_cols as Array<[string, boolean]> | undefined)
    ?? ((tgt.columns as Array<{ name: string; descending?: boolean }> | undefined)
        ?? []).map(c => [c.name, !!c.descending] as [string, boolean]);
  const colsMatch = diff.cols_match !== false;
  return (
    <>
      <FieldDiff diff={diff} src={src} tgt={tgt}/>
      <Section title="Колонки индекса" tone={colsMatch ? "ok" : "warn"}>
        <table style={tableStyle}>
          <thead><tr><Th>Source</Th><Th>Target</Th></tr></thead>
          <tbody>
            <Tr>
              <TdMono>{srcCols.map(c => c[1] ? `${c[0]} DESC` : c[0]).join(", ") || "—"}</TdMono>
              <TdMono>{tgtCols.map(c => c[1] ? `${c[0]} DESC` : c[0]).join(", ") || "—"}</TdMono>
            </Tr>
          </tbody>
        </table>
      </Section>
    </>
  );
}

function JobDiff({ diff, src, tgt }: {
  diff: Record<string, unknown>;
  src:  Record<string, unknown>;
  tgt:  Record<string, unknown>;
}) {
  const actionMatch = diff.action_match !== false;
  return (
    <>
      <FieldDiff diff={diff} src={src} tgt={tgt}/>
      <Section title="Job action — Source" tone={actionMatch ? "ok" : "warn"}>
        <CodeBlock code={(src.job_action as string) || "—"}/>
      </Section>
      <Section title="Job action — Target" tone={actionMatch ? "ok" : "warn"}>
        <CodeBlock code={(tgt.job_action as string) || "—"}/>
      </Section>
    </>
  );
}

// ── TABLE diff: missing/extra/wrong-type columns + indexes/constraints/triggers
function TableDiff({ diff, src, tgt }: {
  diff: Record<string, unknown>;
  src:  Record<string, unknown>;
  tgt:  Record<string, unknown>;
}) {
  const colsMissing = (diff.cols_missing as string[] | undefined) || [];
  const colsExtra   = (diff.cols_extra   as string[] | undefined) || [];
  const colsType    = (diff.cols_type    as string[] | undefined) || [];
  const idxMissing  = (diff.idx_missing  as string[] | undefined) || [];
  const idxDisabled = (diff.idx_disabled as string[] | undefined) || [];
  const conMissing  = (diff.con_missing  as string[] | undefined) || [];
  const conDisabled = (diff.con_disabled as string[] | undefined) || [];
  const trgMissing  = (diff.trg_missing  as string[] | undefined) || [];

  const srcCols = (src.columns as Array<{ name: string; data_type: string }> | undefined) || [];
  const tgtCols = (tgt.columns as Array<{ name: string; data_type: string }> | undefined) || [];
  const srcColMap = new Map(srcCols.map(c => [c.name, c]));
  const tgtColMap = new Map(tgtCols.map(c => [c.name, c]));

  // Union of column names, preserving source order first, then target-extras
  const colsOrdered: string[] = [];
  for (const c of srcCols) colsOrdered.push(c.name);
  for (const c of tgtCols) if (!srcColMap.has(c.name)) colsOrdered.push(c.name);
  const colsMissingSet = new Set(colsMissing);
  const colsExtraSet   = new Set(colsExtra);
  const colsTypeSet    = new Set(colsType);
  const anyColIssue = colsMissing.length + colsExtra.length + colsType.length > 0;

  return (
    <>
      {colsOrdered.length > 0 && (
        <Section title={`Колонки · ${colsOrdered.length}`} tone={anyColIssue ? "warn" : "ok"}>
          <table style={tableStyle}>
            <thead>
              <tr>
                <Th>Имя</Th><Th>Source</Th><Th>Target</Th><Th>Δ</Th>
              </tr>
            </thead>
            <tbody>
              {colsOrdered.map(name => {
                const s = srcColMap.get(name);
                const tg = tgtColMap.get(name);
                let tone: "ok" | "warn" | "error" | "info" = "ok";
                let label = "✓";
                if (colsMissingSet.has(name)) { tone = "error"; label = "нет в target"; }
                else if (colsExtraSet.has(name)) { tone = "info"; label = "только в target"; }
                else if (colsTypeSet.has(name)) { tone = "warn"; label = "тип ≠"; }
                return (
                  <Tr key={name}>
                    <TdMono>{name}</TdMono>
                    <TdMono dim={!s}>{s?.data_type || "—"}</TdMono>
                    <TdMono dim={!tg}>{tg?.data_type || "—"}</TdMono>
                    <TdTag tone={tone}>{label}</TdTag>
                  </Tr>
                );
              })}
            </tbody>
          </table>
        </Section>
      )}
      {(idxMissing.length > 0 || idxDisabled.length > 0) && (
        <Section title="Индексы">
          <NameList items={[
            ...idxMissing.map(n => ({ name: n, tone: "error" as const, label: "отсутствует" })),
            ...idxDisabled.map(n => ({ name: n, tone: "warn" as const, label: "DISABLED" })),
          ]}/>
        </Section>
      )}
      {(conMissing.length > 0 || conDisabled.length > 0) && (
        <Section title="Constraints">
          <NameList items={[
            ...conMissing.map(n => ({ name: n, tone: "error" as const, label: "отсутствует" })),
            ...conDisabled.map(n => ({ name: n, tone: "warn" as const, label: "DISABLED" })),
          ]}/>
        </Section>
      )}
      {trgMissing.length > 0 && (
        <Section title="Триггеры">
          <NameList items={trgMissing.map(n => ({ name: n, tone: "error" as const, label: "отсутствует" }))}/>
        </Section>
      )}
    </>
  );
}

// ── VIEW / MVIEW: show SQL text side-by-side
function SqlTextDiff({ diff, src, tgt }: {
  diff: Record<string, unknown>;
  src:  Record<string, unknown>;
  tgt:  Record<string, unknown>;
}) {
  const srcSql = (src.sql_text as string) || "";
  const tgtSql = (tgt.sql_text as string) || "";
  const sqlMatch = diff.sql_match !== false;
  const statusMatch = diff.status_match !== false;
  const refreshMatch = diff.refresh_match !== false;
  const flags = [
    { k: "SQL текст",        v: sqlMatch     ? "совпадает" : "отличается", tone: sqlMatch     ? "ok" as const : "warn" as const },
    { k: "Статус",           v: statusMatch  ? "совпадает" : "отличается", tone: statusMatch  ? "ok" as const : "warn" as const },
    ...(diff.refresh_match !== undefined
      ? [{ k: "Refresh", v: refreshMatch ? "совпадает" : "отличается", tone: refreshMatch ? "ok" as const : "warn" as const }]
      : []),
  ];

  return (
    <>
      <Section title="Сводка">
        <KeyValueList items={flags}/>
      </Section>
      <Section title="SQL — Source" tone={sqlMatch ? "ok" : "warn"}>
        <CodeBlock code={srcSql || "—"}/>
      </Section>
      <Section title="SQL — Target" tone={sqlMatch ? "ok" : "warn"}>
        <CodeBlock code={tgtSql || "—"}/>
      </Section>
    </>
  );
}

// ── PACKAGE: spec + body comparison
function PackageDiff({ diff, src, tgt }: {
  diff: Record<string, unknown>;
  src:  Record<string, unknown>;
  tgt:  Record<string, unknown>;
}) {
  const specMatch = diff.spec_match !== false;
  const bodyMatch = diff.body_match !== false;
  return (
    <>
      <Section title="Сводка">
        <KeyValueList items={[
          { k: "Спецификация", v: specMatch ? "совпадает" : "отличается", tone: specMatch ? "ok" : "warn" },
          { k: "Тело",         v: bodyMatch ? "совпадает" : "отличается", tone: bodyMatch ? "ok" : "warn" },
        ]}/>
      </Section>
      <Section title="Spec — Source" tone={specMatch ? "ok" : "warn"}>
        <CodeBlock code={(src.spec_source as string) || "—"}/>
      </Section>
      <Section title="Spec — Target" tone={specMatch ? "ok" : "warn"}>
        <CodeBlock code={(tgt.spec_source as string) || "—"}/>
      </Section>
      <Section title="Body — Source" tone={bodyMatch ? "ok" : "warn"}>
        <CodeBlock code={(src.body_source as string) || "—"}/>
      </Section>
      <Section title="Body — Target" tone={bodyMatch ? "ok" : "warn"}>
        <CodeBlock code={(tgt.body_source as string) || "—"}/>
      </Section>
    </>
  );
}

// ── PROCEDURE / FUNCTION / TRIGGER: single source body
function CodeDiff({ diff, src, tgt }: {
  diff: Record<string, unknown>;
  src:  Record<string, unknown>;
  tgt:  Record<string, unknown>;
}) {
  const srcCode = (src.source_code as string) || "";
  const tgtCode = (tgt.source_code as string) || "";
  const codeMatch = diff.code_match !== false;
  return (
    <>
      <Section title="Source" tone={codeMatch ? "ok" : "warn"}>
        <CodeBlock code={srcCode || "—"}/>
      </Section>
      <Section title="Target" tone={codeMatch ? "ok" : "warn"}>
        <CodeBlock code={tgtCode || "—"}/>
      </Section>
    </>
  );
}

function TypeDiff({ diff, src, tgt }: {
  diff: Record<string, unknown>;
  src:  Record<string, unknown>;
  tgt:  Record<string, unknown>;
}) {
  const sourceMatch = diff.source_match !== false;
  const bodyMatch   = diff.body_match   !== false;
  return (
    <>
      <Section title="Сводка">
        <KeyValueList items={[
          { k: "Объявление", v: sourceMatch ? "совпадает" : "отличается", tone: sourceMatch ? "ok" : "warn" },
          { k: "Тело",       v: bodyMatch   ? "совпадает" : "отличается", tone: bodyMatch   ? "ok" : "warn" },
        ]}/>
      </Section>
      <Section title="Объявление — Source" tone={sourceMatch ? "ok" : "warn"}>
        <CodeBlock code={(src.source as string) || "—"}/>
      </Section>
      <Section title="Объявление — Target" tone={sourceMatch ? "ok" : "warn"}>
        <CodeBlock code={(tgt.source as string) || "—"}/>
      </Section>
      <Section title="Body — Source" tone={bodyMatch ? "ok" : "warn"}>
        <CodeBlock code={(src.body_source as string) || "—"}/>
      </Section>
      <Section title="Body — Target" tone={bodyMatch ? "ok" : "warn"}>
        <CodeBlock code={(tgt.body_source as string) || "—"}/>
      </Section>
    </>
  );
}

// ── SEQUENCE / SYNONYM / INDEX header / TRIGGER header / DBLINK / JOB header
// Shows every field from src ∪ tgt (skipping nested arrays/objects), with
// mismatched rows highlighted by an "≠" badge.
function FieldDiff({ src, tgt }: {
  diff: Record<string, unknown>;
  src:  Record<string, unknown>;
  tgt:  Record<string, unknown>;
}) {
  const isScalar = (v: unknown) =>
    v === null || ["string", "number", "boolean"].includes(typeof v);
  const keys = Array.from(new Set([...Object.keys(src), ...Object.keys(tgt)]))
    .filter(k => isScalar(src[k]) && isScalar(tgt[k]) || (k in src ? isScalar(src[k]) : isScalar(tgt[k])))
    .sort();
  if (keys.length === 0) return null;
  return (
    <Section title="Поля">
      <table style={tableStyle}>
        <thead><tr><Th>Поле</Th><Th>Source</Th><Th>Target</Th><Th>Δ</Th></tr></thead>
        <tbody>
          {keys.map(k => {
            const s  = src[k];
            const tg = tgt[k];
            const has = (v: unknown) => v !== undefined && v !== null && v !== "";
            const both = has(s) && has(tg);
            const diff = both ? String(s) !== String(tg) : (has(s) !== has(tg));
            return (
              <Tr key={k}>
                <TdMono>{k}</TdMono>
                <TdMono dim={!has(s)}>{has(s) ? String(s) : "—"}</TdMono>
                <TdMono dim={!has(tg)}>{has(tg) ? String(tg) : "—"}</TdMono>
                <TdTag tone={diff ? "warn" : "ok"}>{diff ? "≠" : "✓"}</TdTag>
              </Tr>
            );
          })}
        </tbody>
      </table>
    </Section>
  );
}

function RawDiff({ diff }: { diff: Record<string, unknown> }) {
  return (
    <Section title="Diff">
      <CodeBlock code={JSON.stringify(diff, null, 2)}/>
    </Section>
  );
}

// ── primitives ──────────────────────────────────────────────────────────

function Section({ title, tone, children }: {
  title:   string;
  tone?:   "ok" | "warn" | "error" | "info";
  children: React.ReactNode;
}) {
  const borderColor =
    tone === "ok"    ? t.tone.ok :
    tone === "warn"  ? t.tone.warn :
    tone === "error" ? t.tone.error :
    tone === "info"  ? t.tone.info :
                       t.border.subtle;
  return (
    <div style={{
      background: t.bg.s1,
      border: `1px solid ${tone ? `color-mix(in oklab, ${borderColor} 24%, transparent)` : t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding: 14,
    }}>
      <div style={{ fontSize: "12.5px", fontWeight: 600, marginBottom: 10 }}>{title}</div>
      {children}
    </div>
  );
}

function Empty({ text }: { text: string }) {
  return <div style={{ fontSize: 12, color: t.text.muted }}>{text}</div>;
}

function CodeBlock({ code }: { code: string }) {
  return (
    <pre style={{
      margin: 0, fontFamily: t.font.mono,
      fontSize: "11.5px", lineHeight: 1.55,
      background: t.bg.s2,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.sm,
      padding: "12px 14px",
      overflowX: "auto",
      whiteSpace: "pre",
      maxHeight: 320,
      overflowY: "auto",
    }}>{code}</pre>
  );
}

const tableStyle: React.CSSProperties = { width: "100%", borderCollapse: "collapse", fontSize: 12 };

function Th({ children }: { children: React.ReactNode }) {
  return <th style={{
    textAlign: "left",
    fontSize: 10, textTransform: "uppercase", letterSpacing: "0.06em",
    color: t.text.muted, padding: "6px 10px",
    borderBottom: `1px solid ${t.border.subtle}`, fontWeight: 600,
  }}>{children}</th>;
}

function Tr({ children }: { children: React.ReactNode }) {
  return <tr>{children}</tr>;
}

function TdMono({ children, dim }: { children: React.ReactNode; dim?: boolean }) {
  return <td style={{
    padding: "6px 10px",
    borderBottom: `1px solid ${t.border.subtle}`,
    fontFamily: t.font.mono, fontSize: 12,
    color: dim ? t.text.muted : t.text.primary,
  }}>{children}</td>;
}

function TdTag({ children, tone }: {
  children: React.ReactNode;
  tone:     "ok" | "warn" | "error" | "info";
}) {
  const bg =
    tone === "ok"    ? t.tone.okSoft :
    tone === "warn"  ? t.tone.warnSoft :
    tone === "error" ? t.tone.errorSoft :
                       t.tone.infoSoft;
  const fg =
    tone === "ok"    ? t.tone.ok :
    tone === "warn"  ? t.tone.warn :
    tone === "error" ? t.tone.error :
                       t.tone.info;
  return (
    <td style={{
      padding: "6px 10px",
      borderBottom: `1px solid ${t.border.subtle}`,
    }}>
      <span style={{
        fontSize: 10, fontWeight: 500,
        padding: "1px 6px", borderRadius: 3,
        fontFamily: t.font.mono,
        background: bg, color: fg,
      }}>{children}</span>
    </td>
  );
}

function NameList({ items }: { items: { name: string; tone: "ok" | "warn" | "error" | "info"; label: string }[] }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      {items.map(it => (
        <div key={`${it.label}-${it.name}`} style={{
          display: "flex", alignItems: "center", gap: 8,
          fontSize: 12,
        }}>
          <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{it.name}</span>
          <span style={{
            fontSize: 10, padding: "1px 6px", borderRadius: 3,
            fontFamily: t.font.mono,
            background: it.tone === "warn" ? t.tone.warnSoft : t.tone.errorSoft,
            color:      it.tone === "warn" ? t.tone.warn     : t.tone.error,
          }}>{it.label}</span>
        </div>
      ))}
    </div>
  );
}

function KeyValueList({ items }: { items: { k: string; v: string; tone: "ok" | "warn" | "error" }[] }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      {items.map(item => {
        const fg = item.tone === "ok" ? t.tone.ok : item.tone === "warn" ? t.tone.warn : t.tone.error;
        return (
          <div key={item.k} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12 }}>
            <span style={{ color: t.text.muted, minWidth: 110 }}>{item.k}</span>
            <span style={{ color: fg, fontWeight: 500, display: "inline-flex", alignItems: "center", gap: 4 }}>
              <Icon name={item.tone === "ok" ? "check" : "warn"} size={12}/>
              {item.v}
            </span>
          </div>
        );
      })}
    </div>
  );
}
