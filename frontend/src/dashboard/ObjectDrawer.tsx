import React, { useState } from "react";
import { t } from "../theme";
import {
  Icon, ObjStatusBadge, ProgressBar,
} from "../components/ui";
import { useApi } from "../hooks/useApi";
import { fmtCompactNum, fmtMb } from "../utils/format";
import { STATUS_MAP, OBJECT_TYPES, type SchemaObject, type MigrationEvent } from "./types";
import {
  addDdlPackItems,
  loadCatalogSnapshot,
  syncTargetColumns,
  type DdlApplyAction,
  type ObjectDetailResp,
  type DdlDetailResp,
  type MigrationDetailResp,
  type SyncTargetColumnsResp,
} from "./api";
import { DiffSections } from "./DiffSections";
import { primaryActionStyle } from "./buttonStyles";
import { PACK_DEFINITIONS, type TablePackAddMode } from "./packModel";

interface Props {
  schemaMigrationId: string;
  object:            SchemaObject;
  events:            MigrationEvent[];
  srcSchema?:        string;
  tgtSchema?:        string;
  onClose:           () => void;
  onAction:          (o: SchemaObject, action: "pause" | "retry" | "rollback") => void;
  onApplied?:        () => void;
  onAddToPack?:      (object: SchemaObject, mode: TablePackAddMode) => void;
}

interface OracleError {
  type:           string;
  sequence:       number;
  line:           number;
  position:       number;
  text:           string;
  attribute:      string;     // "ERROR" or "WARNING"
  message_number: number;
}

/** Types that can take CREATE OR REPLACE — sync_diff is offered for these. */
const REPLACEABLE_TYPES = new Set([
  "VIEW", "PROCEDURE", "FUNCTION", "PACKAGE", "PACKAGE BODY",
  "TRIGGER", "TYPE", "TYPE BODY", "SYNONYM",
]);

interface ApplyOpt {
  action:      DdlApplyAction;
  label:       string;
  type:        string;     // Oracle canonical or frontend alias — backend accepts both
  name:        string;
  destructive: boolean;    // adds data-loss warning to confirm dialog
}

/** Decide which apply action makes sense, prefer the loaded detail (most
 *  accurate). Falls back to SchemaObject.note when detail isn't loaded yet. */
function detectApplyAction(
  o: SchemaObject,
  detail: ObjectDetailResp | null,
): ApplyOpt | null {
  // 1) DDL-kind detail — has direct match_status + statuses
  if (detail && detail.kind === "ddl" && detail.found) {
    const ms = detail.match_status || "UNKNOWN";
    const otype = detail.object_type;
    const oname = detail.object_name;
    const srcInvalid = (detail.source?.oracle_status || "").toUpperCase() === "INVALID";
    const tgtInvalid = (detail.target?.oracle_status || "").toUpperCase() === "INVALID";
    const isTable = otype === "TABLE" || otype === "MATERIALIZED VIEW";
    if (ms === "MISSING")
      return { action: "create_missing", label: "Создать на target", type: otype, name: oname, destructive: false };
    if (ms === "DIFF" && REPLACEABLE_TYPES.has(otype))
      return { action: "sync_diff", label: "Засинкать DDL", type: otype, name: oname, destructive: false };
    if (ms === "DIFF")
      return { action: "recreate", label: "Пересоздать", type: otype, name: oname, destructive: isTable };
    if (tgtInvalid && !srcInvalid)
      return { action: "recreate", label: "Пересоздать", type: otype, name: oname, destructive: isTable };
    return null;
  }

  // 2) Migration-kind — when detail.ddl_diff has a verdict, offer matching action.
  //    DIFF on TABLE is allowed but flagged destructive (DROP+CREATE loses data).
  if (detail && detail.kind === "migration" && detail.found) {
    const d = detail.ddl_diff;
    if (d && d.found) {
      const dt = d.object_type;
      const dn = d.object_name;
      const isTable = dt === "TABLE" || dt === "MATERIALIZED VIEW";
      if (d.match_status === "MISSING")
        return { action: "create_missing", label: "Создать на target", type: dt, name: dn, destructive: false };
      if (d.match_status === "DIFF" && REPLACEABLE_TYPES.has(dt))
        return { action: "sync_diff", label: "Засинкать DDL", type: dt, name: dn, destructive: false };
      if (d.match_status === "DIFF")
        return { action: "recreate", label: "Пересоздать на target", type: dt, name: dn, destructive: isTable };
    }
    return null;
  }

  // 3) Fallback before detail loads — use SchemaObject note (DDL-only)
  if (!o.id.startsWith("ddl-")) return null;
  const note = (o.note || "").toLowerCase();
  const isTable = o.type === "TABLE" || o.type === "MVIEW";
  if (note.startsWith("нет в target"))
    return { action: "create_missing", label: "Создать на target", type: o.type, name: o.name, destructive: false };
  if (note.startsWith("ddl отличается"))
    return REPLACEABLE_TYPES.has(o.type)
      ? { action: "sync_diff", label: "Засинкать DDL", type: o.type, name: o.name, destructive: false }
      : { action: "recreate",  label: "Пересоздать",   type: o.type, name: o.name, destructive: isTable };
  if (note.includes("invalid в target"))
    return { action: "recreate", label: "Пересоздать", type: o.type, name: o.name, destructive: isTable };
  return null;
}

function tableColumnDiffDetail(detail: ObjectDetailResp | null): DdlDetailResp | null {
  const d = detail?.kind === "ddl"
    ? detail
    : detail?.kind === "migration"
      ? detail.ddl_diff
      : null;
  if (!d || !d.found || d.object_type !== "TABLE") return null;
  const diff = d.diff || {};
  const count = (v: unknown) => Array.isArray(v) ? v.length : 0;
  const columnIssues =
    count(diff.cols_missing) +
    count(diff.cols_extra) +
    count(diff.cols_type);
  return columnIssues > 0 ? d : null;
}

function syncColumnsFeedback(result: SyncTargetColumnsResp): string {
  const parts = [
    `+${result.added?.length || 0}`,
    `-${result.dropped?.length || 0}`,
  ];
  const warnings = result.warnings?.length || 0;
  const dropErrors = result.drop_errors?.length || 0;
  if (warnings) parts.push(`типов: ${warnings}`);
  if (dropErrors) parts.push(`ошибок DROP: ${dropErrors}`);
  return `DDL колонки: ${parts.join(", ")}`;
}

function messageFromError(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function ObjectDrawer({
  schemaMigrationId, object: o, events,
  srcSchema, tgtSchema,
  onClose, onAction, onApplied, onAddToPack,
}: Props) {
  const canAddToPack = !!onAddToPack && o.type === "TABLE" && !!srcSchema && !!tgtSchema;
  const handleAddToPack = (mode: TablePackAddMode) => {
    if (!onAddToPack || !srcSchema || !tgtSchema) return;
    onAddToPack(o, mode);
    onClose();
  };
  const detail = useApi<ObjectDetailResp>(
    `/api/schema-migrations/${schemaMigrationId}/objects/${encodeURIComponent(o.id)}/detail`,
  );
  const [applyBusy,     setApplyBusy]     = useState(false);
  const [applyFeedback, setApplyFeedback] = useState<string | null>(null);
  const [columnSyncBusy, setColumnSyncBusy] = useState(false);
  const [columnSyncFeedback, setColumnSyncFeedback] = useState<string | null>(null);
  const applyOpt = detectApplyAction(o, detail.data);
  const columnDiffDetail = tableColumnDiffDetail(detail.data);
  const canSyncColumns = !!columnDiffDetail && !!srcSchema && !!tgtSchema;

  // Derive Oracle "all_errors" query params for compilable PL/SQL objects.
  // Only fired when the object is INVALID on that side.
  let oracleType: string | null = null;
  let oracleName: string | null = null;
  let srcInvalid = false;
  let tgtInvalid = false;
  if (detail.data?.kind === "ddl" && detail.data.found) {
    oracleType = detail.data.object_type;
    oracleName = detail.data.object_name;
    srcInvalid = (detail.data.source?.oracle_status || "").toUpperCase() === "INVALID";
    tgtInvalid = (detail.data.target?.oracle_status || "").toUpperCase() === "INVALID";
  } else if (detail.data?.kind === "migration" && detail.data.found && detail.data.ddl_diff?.found) {
    const d = detail.data.ddl_diff;
    oracleType = d.object_type;
    oracleName = d.object_name;
    srcInvalid = (d.source?.oracle_status || "").toUpperCase() === "INVALID";
    tgtInvalid = (d.target?.oracle_status || "").toUpperCase() === "INVALID";
  }
  const buildErrUrl = (side: "source" | "target", schema?: string) =>
    schema && oracleType && oracleName
      ? `/api/db/${side}/oracle-errors?schema=${encodeURIComponent(schema)}`
        + `&type=${encodeURIComponent(oracleType)}&name=${encodeURIComponent(oracleName)}`
      : null;
  const srcErrors = useApi<OracleError[]>(srcInvalid ? buildErrUrl("source", srcSchema) : null);
  const tgtErrors = useApi<OracleError[]>(tgtInvalid ? buildErrUrl("target", tgtSchema) : null);

  const runApply = async () => {
    if (!applyOpt) return;
    const baseMsg = `${applyOpt.label}: ${applyOpt.type} ${applyOpt.name} будет добавлен в DDL-пачку.`;
    const warning = applyOpt.destructive
      ? "\n\nПри запуске DDL-пачки объект будет DROP-нут на target до пересоздания.\nДанные в существующей таблице будут потеряны.\n\nПродолжить?"
      : "\n\nПродолжить?";
    if (!window.confirm(baseMsg + warning)) return;
    setApplyBusy(true);
    setApplyFeedback(null);
    try {
      const r = await addDdlPackItems(schemaMigrationId, applyOpt.action,
        [{ type: applyOpt.type, name: applyOpt.name }]);
      setApplyFeedback(r.skipped.length
        ? `пропущено: ${r.skipped[0].reason}`
        : `DDL-пачка: +${r.queued}`);
      onApplied?.();
    } catch (e) {
      setApplyFeedback(`ошибка: ${(e as Error).message}`);
    } finally {
      setApplyBusy(false);
    }
  };

  const runColumnSync = async () => {
    if (!columnDiffDetail || !srcSchema || !tgtSchema) return;
    const diff = columnDiffDetail.diff || {};
    const missing = Array.isArray(diff.cols_missing) ? diff.cols_missing.length : 0;
    const extra = Array.isArray(diff.cols_extra) ? diff.cols_extra.length : 0;
    const type = Array.isArray(diff.cols_type) ? diff.cols_type.length : 0;
    const ok = window.confirm(
      `Синхронизировать DDL колонок для ${columnDiffDetail.object_name}?\n\n`
      + `Будет добавлено: ${missing}, удалено с target: ${extra}, несовпадений типов: ${type}.\n`
      + "Типы колонок автоматически не меняются.",
    );
    if (!ok) return;
    setColumnSyncBusy(true);
    setColumnSyncFeedback(null);
    try {
      const result = await syncTargetColumns({
        src_schema: srcSchema,
        src_table:  columnDiffDetail.object_name,
        tgt_schema: tgtSchema,
        tgt_table:  columnDiffDetail.object_name,
      });
      let feedback = syncColumnsFeedback(result);
      try {
        await loadCatalogSnapshot(srcSchema, tgtSchema);
      } catch (e) {
        feedback += `; snapshot: ${messageFromError(e)}`;
      }
      setColumnSyncFeedback(feedback);
      detail.reload();
      onApplied?.();
    } catch (e) {
      setColumnSyncFeedback(`ошибка: ${messageFromError(e)}`);
    } finally {
      setColumnSyncBusy(false);
    }
  };
  const status = o.status;
  const tone =
    status === "error" ? "error" :
    status === "warn"  ? "warn"  :
    status === "done"  ? "ok"    :
                         "info";
  const objEvents = events.filter(e => e.obj === o.name);
  const hasIssues = o.err > 0 || o.warn > 0;

  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed", inset: 0,
        background: "rgba(20,20,20,0.18)",
        backdropFilter: "blur(2px)",
        zIndex: 50,
        animation: "fadeIn 140ms ease-out",
      }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          position: "absolute", top: 0, right: 0,
          height: "100vh",
          width: "min(860px, 92vw)",
          background: t.bg.s1,
          borderLeft: `1px solid ${t.border.subtle}`,
          boxShadow: t.shadow.s3,
          display: "flex", flexDirection: "column",
          animation: "slideIn 200ms cubic-bezier(.22,.61,.36,1)",
        }}
      >
        {/* Head */}
        <div style={{
          display: "flex", justifyContent: "space-between", alignItems: "flex-start",
          gap: 16, padding: "20px 26px 16px",
          borderBottom: `1px solid ${t.border.subtle}`,
        }}>
          <div>
            <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 8, flexWrap: "wrap" }}>
              <span style={{
                fontFamily: t.font.mono,
                fontSize: "9.5px", fontWeight: 600,
                letterSpacing: "0.05em", textTransform: "uppercase",
                padding: "2px 6px", borderRadius: 3,
                background: t.bg.s3, color: t.text.secondary,
              }}>
                {OBJECT_TYPES[o.type].label}
              </span>
              <ObjStatusBadge tone={STATUS_MAP[status].tone} label={STATUS_MAP[status].label}/>
              {o.err > 0 && (
                <span style={{
                  display: "inline-flex", alignItems: "center", gap: 3,
                  fontSize: "10.5px", padding: "1px 5px", borderRadius: 3,
                  fontFamily: t.font.mono,
                  background: t.tone.errorSoft, color: t.tone.error,
                }}>
                  <Icon name="error" size={11}/> {o.err}
                </span>
              )}
              {o.warn > 0 && (
                <span style={{
                  display: "inline-flex", alignItems: "center", gap: 3,
                  fontSize: "10.5px", padding: "1px 5px", borderRadius: 3,
                  fontFamily: t.font.mono,
                  background: t.tone.warnSoft, color: t.tone.warn,
                }}>
                  <Icon name="warn" size={11}/> {o.warn}
                </span>
              )}
              {o.srcStatus && (
                <OracleStatusChip side="src" status={o.srcStatus}/>
              )}
              {o.tgtStatus !== undefined && (
                <OracleStatusChip side="tgt" status={o.tgtStatus}/>
              )}
            </div>
            <h2 style={{
              margin: 0, fontSize: 20, fontWeight: 600,
              letterSpacing: "-0.02em",
              fontFamily: t.font.mono,
            }}>
              {o.name}
            </h2>
            {(srcSchema || tgtSchema) && (
              <div style={{
                display: "flex", gap: 6, alignItems: "center",
                fontSize: 12, marginTop: 6,
                fontFamily: t.font.mono, color: t.text.muted,
              }}>
                <span>{srcSchema || "—"}</span>
                <Icon name="arrow" size={14}/>
                <span>{tgtSchema || "—"}</span>
              </div>
            )}
          </div>
          <div style={{ display: "flex", gap: 6, alignItems: "center", flexShrink: 0 }}>
            {canAddToPack && (
              <>
                <button
                  onClick={() => handleAddToPack("historical")}
                  title={`Добавить таблицу в ${PACK_DEFINITIONS.bulk.title.toLowerCase()}`}
                  style={packActionStyle("historical")}
                >
                  В обычную
                </button>
                <button
                  onClick={() => handleAddToPack("cdc")}
                  title={`Добавить таблицу в ${PACK_DEFINITIONS.cdc.title}`}
                  style={packActionStyle("cdc")}
                >
                  В CDC
                </button>
              </>
            )}
            {canSyncColumns && (
              <button
                onClick={runColumnSync}
                disabled={columnSyncBusy}
                title="Добавить отсутствующие и удалить лишние колонки на target; типы не меняются автоматически"
                style={primaryActionStyle(columnSyncBusy)}
              >
                <Icon name="rotate" size={13}/>
                {columnSyncBusy ? "синхр..." : "Синхр. DDL"}
              </button>
            )}
            {applyOpt && (
              <button
                onClick={runApply}
                disabled={applyBusy}
                title={applyOpt.destructive
                  ? `${applyOpt.label}: добавить в DDL-пачку, DROP+CREATE при запуске`
                  : `${applyOpt.label}: добавить в DDL-пачку`}
                style={primaryActionStyle(applyBusy, applyOpt.destructive)}
              >
                <Icon name={applyOpt.destructive ? "warn" : "rotate"} size={13}/>
                {applyBusy ? "добавляю..." : "В DDL-пачку"}
              </button>
            )}
            {(columnSyncFeedback || applyFeedback) && (
              <span style={{
                fontSize: 11, color: t.text.muted, fontFamily: t.font.mono,
                maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}>
                {columnSyncFeedback || applyFeedback}
              </span>
            )}
            {status === "running" && (
              <ActionBtn icon="pause" label="Пауза" onClick={() => onAction(o, "pause")}/>
            )}
            <ActionBtn icon="close" onClick={onClose}/>
          </div>
        </div>

        {/* Body */}
        <div style={{
          flex: 1, overflowY: "auto",
          padding: "16px 26px 28px",
          display: "flex", flexDirection: "column", gap: 16,
        }}>
          {/* Stats strip */}
          <div style={{
            display: "grid", gridTemplateColumns: "repeat(5, minmax(0,1fr))", gap: 10,
          }}>
            <DStat label="Прогресс" value={`${o.progress.toFixed(1)}%`}>
              <ProgressBar value={o.progress} tone={tone} height={4} style={{ marginTop: 6 }}/>
            </DStat>
            {o.rows != null ? (
              <DStat
                label="Rows"
                value={fmtCompactNum(o.rowsDone)}
                sub={`из ${fmtCompactNum(o.rows)}`}
              />
            ) : (
              <DStat label="Совместимость" value={`${o.compat}%`} sub="по PL/SQL"/>
            )}
            <DStat label="Размер" value={o.sizeMb < 0.1 ? "—" : fmtMb(o.sizeMb)}/>
            <DStat
              label="Скорость"
              value={o.rowsPerSec > 0 ? `${fmtCompactNum(o.rowsPerSec)}/s` : o.mbPerSec > 0 ? `${o.mbPerSec} MB/s` : "—"}
            />
            <DStat label="ETA / Длит." value={o.eta} sub={o.dur}/>
          </div>

          {/* Issue callout */}
          {hasIssues && o.note && (
            <div style={{
              display: "grid",
              gridTemplateColumns: "auto 1fr auto",
              gap: 12, alignItems: "center",
              padding: "12px 14px",
              borderRadius: t.radius.lg,
              border: `1px solid color-mix(in oklab, ${o.err > 0 ? t.tone.error : t.tone.warn} 30%, transparent)`,
              background: o.err > 0 ? t.tone.errorSoft : t.tone.warnSoft,
              color: o.err > 0 ? t.tone.error : t.tone.warn,
            }}>
              <Icon name={o.err > 0 ? "error" : "warn"} size={14}/>
              <div>
                <div style={{
                  fontSize: 11, fontWeight: 700,
                  textTransform: "uppercase", letterSpacing: "0.06em",
                  marginBottom: 2,
                }}>
                  {o.err > 0 ? "Ошибка" : "Предупреждение"}
                </div>
                <div style={{
                  fontSize: "12.5px",
                  color: t.text.primary,
                  fontFamily: t.font.mono,
                  lineHeight: 1.4,
                }}>
                  {o.note}
                </div>
              </div>
              <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                {canSyncColumns && (
                  <button
                    onClick={runColumnSync}
                    disabled={columnSyncBusy}
                    style={primaryActionStyle(columnSyncBusy)}
                  >
                    <Icon name="rotate" size={13}/>
                    {columnSyncBusy ? "синхр..." : "Синхр. DDL"}
                  </button>
                )}
                {applyOpt && (
                  <button
                    onClick={runApply}
                    disabled={applyBusy}
                    style={primaryActionStyle(applyBusy, applyOpt.destructive)}
                  >
                    <Icon name={applyOpt.destructive ? "warn" : "rotate"} size={13}/>
                    {applyBusy ? "очередь…" : applyOpt.label}
                  </button>
                )}
                {(columnSyncFeedback || applyFeedback) && (
                  <span style={{ fontSize: 11, color: t.text.muted, fontFamily: t.font.mono }}>
                    {columnSyncFeedback || applyFeedback}
                  </span>
                )}
              </div>
            </div>
          )}

          {/* Compilation errors (Oracle all_errors) — only fetched for INVALID objects */}
          {(srcInvalid || tgtInvalid) && (
            <OracleErrorsBlock
              srcInvalid={srcInvalid}
              tgtInvalid={tgtInvalid}
              src={srcErrors}
              tgt={tgtErrors}
            />
          )}

          {/* Diff sections — populated from /objects/:id/detail */}
          {detail.loading && (
            <div style={{ fontSize: 12, color: t.text.muted }}>загружаем детали…</div>
          )}
          {detail.error && (
            <div style={{
              padding: "10px 12px", fontSize: 12,
              background: t.tone.errorSoft, color: t.tone.error,
              borderRadius: t.radius.sm,
            }}>
              Не удалось загрузить детали: {detail.error}
            </div>
          )}
          {detail.data && detail.data.kind === "ddl" && (
            <DiffSections detail={detail.data as DdlDetailResp}/>
          )}
          {detail.data && detail.data.kind === "migration" && (
            <MigrationDetailBlock data={detail.data as MigrationDetailResp}/>
          )}

          {/* Event log */}
          <div style={{
            background: t.bg.s1,
            border: `1px solid ${t.border.subtle}`,
            borderRadius: t.radius.lg,
            padding: 14,
          }}>
            <div style={{
              display: "flex", justifyContent: "space-between", alignItems: "baseline",
              marginBottom: 10, gap: 8,
            }}>
              <span style={{ fontSize: "12.5px", fontWeight: 600 }}>События</span>
              <span style={{ fontSize: 11, color: t.text.muted }}>{objEvents.length}</span>
            </div>
            <div style={{
              display: "flex", flexDirection: "column", gap: 1,
              background: t.bg.s2,
              borderRadius: t.radius.sm,
              padding: 6, maxHeight: 280, overflowY: "auto",
              fontFamily: t.font.mono,
            }}>
              {objEvents.length === 0 && (
                <div style={{ padding: 16, textAlign: "center", fontSize: 11, color: t.text.muted }}>
                  Нет событий по этому объекту
                </div>
              )}
              {objEvents.map((e, i) => (
                <div key={i} style={{
                  display: "grid",
                  gridTemplateColumns: "56px 44px 1fr",
                  gap: 8, padding: "4px 6px",
                  borderRadius: 3, fontSize: 11,
                  alignItems: "baseline",
                }}>
                  <span style={{ fontSize: 10, color: t.text.muted }}>{e.t}</span>
                  <span style={{
                    fontSize: "9.5px", fontWeight: 600, letterSpacing: "0.04em",
                    textTransform: "uppercase",
                    color: e.level === "error" ? t.tone.error : e.level === "warn" ? t.tone.warn : t.tone.info,
                  }}>{e.level}</span>
                  <span style={{ color: t.text.secondary, wordBreak: "break-word" }}>{e.msg}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function OracleStatusChip({ side, status }: { side: "src" | "tgt"; status: string }) {
  const isInvalid = status.toUpperCase() === "INVALID";
  const isMissing = !status;          // tgt without VALID/INVALID — no row in ddl_objects.target
  const bg =
    isMissing ? t.tone.warnSoft :
    isInvalid ? t.tone.warnSoft :
                t.bg.s3;
  const fg =
    isMissing ? t.tone.warn :
    isInvalid ? t.tone.warn :
                t.text.muted;
  const label = isMissing ? "нет" : status;
  return (
    <span style={{
      display: "inline-flex", alignItems: "center", gap: 4,
      fontSize: "10px", padding: "1px 6px", borderRadius: 3,
      fontFamily: t.font.mono,
      background: bg, color: fg,
    }}>
      <span style={{ fontWeight: 500, letterSpacing: "0.04em" }}>
        {side === "src" ? "SRC" : "TGT"}:
      </span>
      <span>{label}</span>
    </span>
  );
}

function MigrationDetailBlock({ data }: { data: MigrationDetailResp }) {
  if (!data.found) {
    return (
      <div style={{
        background: t.bg.s1,
        border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.lg,
        padding: 14, fontSize: 12, color: t.text.muted,
      }}>
        Миграция не найдена в БД.
      </div>
    );
  }
  const m = (data.migration || {}) as Record<string, unknown>;
  const errText      = m.error_text   as string | null | undefined;
  const failedPhase  = m.failed_phase as string | null | undefined;
  const errorCode    = m.error_code   as string | null | undefined;
  const phase        = m.phase        as string | undefined;
  const totalChunks  = m.total_chunks as number | null | undefined;
  const chunksDone   = m.chunks_done  as number | null | undefined;
  const chunksFailed = m.chunks_failed as number | null | undefined;
  const ddlDiff      = data.ddl_diff;

  return (
    <>
      {errText && (
        <div style={{
          background: t.bg.s1,
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.lg,
          padding: 14,
        }}>
          <div style={{ fontSize: "12.5px", fontWeight: 600, marginBottom: 8 }}>
            Текущая ошибка
          </div>
          <div style={{ display: "flex", gap: 12, fontSize: 11, color: t.text.muted, marginBottom: 6 }}>
            {errorCode && <span>Код: <span style={{ fontFamily: t.font.mono, color: t.tone.error }}>{errorCode}</span></span>}
            {failedPhase && <span>Фаза: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{failedPhase}</span></span>}
            {phase && <span>Текущая: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{phase}</span></span>}
          </div>
          <pre style={{
            margin: 0, fontFamily: t.font.mono, fontSize: "11.5px",
            lineHeight: 1.5, padding: "10px 12px",
            background: t.tone.errorSoft, color: t.tone.error,
            border: `1px solid color-mix(in oklab, ${t.tone.error} 26%, transparent)`,
            borderRadius: t.radius.sm,
            whiteSpace: "pre-wrap", wordBreak: "break-word",
          }}>{errText}</pre>
        </div>
      )}

      {((totalChunks || 0) > 0 || (chunksFailed || 0) > 0) && (
        <div style={{
          background: t.bg.s1,
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.lg,
          padding: 14,
        }}>
          <div style={{ fontSize: "12.5px", fontWeight: 600, marginBottom: 8 }}>
            Чанки
          </div>
          <div style={{ display: "flex", gap: 16, fontSize: 12 }}>
            <span style={{ color: t.text.muted }}>
              Всего: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{totalChunks ?? 0}</span>
            </span>
            <span style={{ color: t.text.muted }}>
              Готово: <span style={{ fontFamily: t.font.mono, color: t.tone.ok }}>{chunksDone ?? 0}</span>
            </span>
            {(chunksFailed || 0) > 0 && (
              <span style={{ color: t.text.muted }}>
                Упало: <span style={{ fontFamily: t.font.mono, color: t.tone.error }}>{chunksFailed}</span>
              </span>
            )}
          </div>
        </div>
      )}

      {ddlDiff && ddlDiff.found && ddlDiff.match_status !== "MATCH" && (
        <DiffSections detail={ddlDiff}/>
      )}
    </>
  );
}

function DStat({ label, value, sub, children }: {
  label: string;
  value: React.ReactNode;
  sub?:  string;
  children?: React.ReactNode;
}) {
  return (
    <div style={{
      background: t.bg.s2,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding: "10px 12px",
      display: "flex", flexDirection: "column", gap: 3,
    }}>
      <span style={{
        fontSize: 10, color: t.text.muted,
        textTransform: "uppercase", letterSpacing: "0.06em",
        fontWeight: 600,
      }}>{label}</span>
      <span style={{ fontSize: 16, fontWeight: 600, letterSpacing: "-0.015em", fontFamily: t.font.mono }}>
        {value}
      </span>
      {sub && <span style={{ fontSize: "10.5px", color: t.text.muted, fontFamily: t.font.mono }}>{sub}</span>}
      {children}
    </div>
  );
}

function ActionBtn({ icon, label, onClick, primary }: {
  icon:     "pause" | "rotate" | "close" | "check";
  label?:   string;
  onClick:  () => void;
  primary?: boolean;
}) {
  const isIconOnly = !label;
  return (
    <button onClick={onClick} style={{
      display: "inline-flex", alignItems: "center", gap: 6,
      padding: isIconOnly ? 6 : "5px 10px",
      borderRadius: t.radius.sm,
      fontSize: "12px",
      cursor: "pointer",
      background: primary ? t.text.primary : (isIconOnly ? "transparent" : t.bg.s1),
      color:      primary ? t.text.inverse : t.text.secondary,
      border:     `1px solid ${primary ? t.text.primary : (isIconOnly ? "transparent" : t.border.subtle)}`,
    }}>
      <Icon name={icon} size={14}/>
      {label && <span>{label}</span>}
    </button>
  );
}

function packActionStyle(mode: TablePackAddMode): React.CSSProperties {
  const isCdc = mode === "cdc";
  return {
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
    background: isCdc ? t.blue.bg : t.amber.bg,
    border: `1px solid ${isCdc ? t.blue.dim : t.amber.dim}`,
    borderRadius: t.radius.sm,
    color: isCdc ? t.blue.fg : t.amber.fg,
    padding: "5px 10px",
    fontSize: 12,
    cursor: "pointer",
    fontWeight: 700,
  };
}

function OracleErrorsBlock({
  srcInvalid, tgtInvalid, src, tgt,
}: {
  srcInvalid: boolean;
  tgtInvalid: boolean;
  src: { data: OracleError[] | null; loading: boolean; error: string | null };
  tgt: { data: OracleError[] | null; loading: boolean; error: string | null };
}) {
  return (
    <div style={{
      background: t.bg.s1,
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding: 14,
    }}>
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "baseline",
        marginBottom: 10, gap: 8,
      }}>
        <span style={{ fontSize: "12.5px", fontWeight: 600 }}>
          Compilation errors (Oracle <code style={{ fontFamily: t.font.mono, fontSize: "11px" }}>all_errors</code>)
        </span>
      </div>
      {srcInvalid && (
        <ErrorsList side="SOURCE" tone={t.tone.error} api={src}/>
      )}
      {tgtInvalid && (
        <ErrorsList side="TARGET" tone={t.tone.warn} api={tgt}/>
      )}
    </div>
  );
}

function ErrorsList({
  side, tone, api,
}: {
  side: string;
  tone: string;
  api:  { data: OracleError[] | null; loading: boolean; error: string | null };
}) {
  return (
    <div style={{ marginTop: 8 }}>
      <div style={{
        display: "flex", gap: 8, alignItems: "baseline", marginBottom: 4,
      }}>
        <span style={{
          fontFamily: t.font.mono, fontSize: "10px",
          fontWeight: 600, letterSpacing: "0.06em",
          padding: "1px 6px", borderRadius: 3,
          background: tone, color: "#fff",
        }}>
          {side}
        </span>
        {api.data && (
          <span style={{ fontSize: 11, color: t.text.muted, fontFamily: t.font.mono }}>
            {api.data.length} запис(ей)
          </span>
        )}
      </div>
      {api.loading && (
        <div style={{ fontSize: 11, color: t.text.muted }}>загружаем ошибки…</div>
      )}
      {api.error && (
        <div style={{
          fontSize: 11, padding: "8px 10px",
          background: t.tone.errorSoft, color: t.tone.error,
          borderRadius: t.radius.sm,
        }}>
          Не удалось получить all_errors: {api.error}
        </div>
      )}
      {api.data && api.data.length === 0 && (
        <div style={{ fontSize: 11, color: t.text.muted, fontStyle: "italic" }}>
          В all_errors пусто — INVALID может быть из-за зависимости от другого объекта.
        </div>
      )}
      {api.data && api.data.length > 0 && (
        <div style={{
          fontFamily: t.font.mono, fontSize: "11.5px",
          background: t.bg.s2, borderRadius: t.radius.sm,
          padding: 8, maxHeight: 240, overflowY: "auto",
          display: "flex", flexDirection: "column", gap: 6,
        }}>
          {api.data.map((e, i) => (
            <div key={i} style={{
              borderLeft: `3px solid ${e.attribute === "ERROR" ? t.tone.error : t.tone.warn}`,
              paddingLeft: 8,
              display: "grid",
              gridTemplateColumns: "auto auto 1fr",
              columnGap: 10, rowGap: 2,
              alignItems: "baseline",
            }}>
              <span style={{
                fontSize: "9.5px", fontWeight: 700, letterSpacing: "0.04em",
                color: e.attribute === "ERROR" ? t.tone.error : t.tone.warn,
                textTransform: "uppercase",
              }}>
                {e.attribute}
              </span>
              <span style={{ color: t.text.muted, fontSize: 10 }}>
                line {e.line}{e.position ? `:${e.position}` : ""}
                {e.message_number ? ` · PLS-${String(e.message_number).padStart(5, "0")}` : ""}
              </span>
              <span style={{
                color: t.text.primary,
                gridColumn: "1 / -1",
                whiteSpace: "pre-wrap", wordBreak: "break-word",
              }}>
                {e.text}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
