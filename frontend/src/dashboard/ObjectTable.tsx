import React from "react";
import { t } from "../theme";
import { Icon, ObjStatusBadge, ProgressBar } from "../components/ui";
import { fmtCompactNum, fmtMb } from "../utils/format";
import { VisibleCheckbox } from "./VisibleCheckbox";
import {
  STATUS_MAP, OBJECT_TYPES,
  type SchemaObject, type ObjectStatus, type ObjectType,
} from "./types";
import { columnDiffTitle, hasColumnDiff } from "./tableDdl";

interface Props {
  objects:   SchemaObject[];
  layout?:   "flat" | "grouped";
  onOpen:    (o: SchemaObject) => void;
  onAction:  (o: SchemaObject, action: "pause" | "retry" | "more") => void;
  /** Client-side pagination (omit to render everything). */
  page?:         number;       // 1-based
  pageSize?:     number;
  onPageChange?: (page: number) => void;
  onPageSizeChange?: (size: number) => void;
  /** Bulk-selection. selectableIds — текущая страница, среди которых row можно выбрать
   *  (например TABLE+queued). selectedIds — Set всех выбранных id (включая страницы вне видимости). */
  selectableIds?: Set<string>;
  selectedIds?:   Set<string>;
  onToggleSelect?: (id: string) => void;
  onSelectAllPage?: (ids: string[], allSelected: boolean) => void;
}

const TYPE_COLORS: Record<ObjectType, { bg: string; fg: string }> = {
  TABLE:     { bg: `color-mix(in oklab, ${t.tone.info} 14%, transparent)`,                   fg: t.tone.info },
  INDEX:     { bg: "color-mix(in oklab, oklch(0.6 0.13 200) 14%, transparent)", fg: "oklch(0.5 0.13 200)" },
  MVIEW:     { bg: "color-mix(in oklab, oklch(0.6 0.13 320) 14%, transparent)", fg: "oklch(0.5 0.13 320)" },
  SEQUENCE:  { bg: t.bg.s3, fg: t.text.secondary },
  VIEW:      { bg: "color-mix(in oklab, oklch(0.65 0.11 280) 14%, transparent)", fg: "oklch(0.5 0.13 280)" },
  PACKAGE:   { bg: "color-mix(in oklab, oklch(0.65 0.11 140) 16%, transparent)", fg: "oklch(0.45 0.11 140)" },
  PROCEDURE: { bg: "color-mix(in oklab, oklch(0.65 0.11 140) 12%, transparent)", fg: "oklch(0.45 0.11 140)" },
  FUNCTION:  { bg: "color-mix(in oklab, oklch(0.65 0.11 140) 12%, transparent)", fg: "oklch(0.45 0.11 140)" },
  TRIGGER:   { bg: `color-mix(in oklab, ${t.tone.warn} 12%, transparent)`,                   fg: t.tone.warn },
  TYPE:      { bg: t.bg.s3, fg: t.text.secondary },
  SYNONYM:   { bg: t.bg.s3, fg: t.text.muted     },
  GRANT:     { bg: t.bg.s3, fg: t.text.muted     },
  DBLINK:    { bg: t.bg.s3, fg: t.text.muted     },
  JOB:       { bg: "color-mix(in oklab, oklch(0.65 0.11 240) 14%, transparent)", fg: "oklch(0.5 0.13 240)" },
};

export function ObjectTable({
  objects, layout = "flat", onOpen, onAction,
  page, pageSize, onPageChange, onPageSizeChange,
  selectableIds, selectedIds, onToggleSelect, onSelectAllPage,
}: Props) {
  const total = objects.length;
  const pagingOn  = !!(page && pageSize && onPageChange);
  const startIdx  = pagingOn ? (page! - 1) * pageSize! : 0;
  const endIdx    = pagingOn ? Math.min(startIdx + pageSize!, total) : total;
  const pageItems = pagingOn ? objects.slice(startIdx, endIdx) : objects;
  const totalPages = pagingOn ? Math.max(1, Math.ceil(total / pageSize!)) : 1;

  // Bulk-select state for the visible page
  const selectionOn = !!(selectableIds && selectedIds && onToggleSelect);
  const pageSelectableIds = selectionOn
    ? pageItems.filter(o => selectableIds!.has(o.id)).map(o => o.id)
    : [];
  const pageSelectedCount = selectionOn
    ? pageSelectableIds.filter(id => selectedIds!.has(id)).length
    : 0;
  const pageAllSelected  = selectionOn && pageSelectableIds.length > 0
    && pageSelectedCount === pageSelectableIds.length;
  const pageSomeSelected = selectionOn && pageSelectedCount > 0 && !pageAllSelected;

  // Group by category when layout=grouped — applied AFTER pagination slice
  const grouped: [string, SchemaObject[]][] = (() => {
    if (layout !== "grouped") return [];
    const map = new Map<string, SchemaObject[]>();
    pageItems.forEach(o => {
      const g = OBJECT_TYPES[o.type].group;
      if (!map.has(g)) map.set(g, []);
      map.get(g)!.push(o);
    });
    return Array.from(map.entries());
  })();

  return (
    <div style={{
      background:   t.bg.s1,
      border:       `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      overflow:     "hidden",
      boxShadow:    t.shadow.s1,
    }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
        <thead>
          <tr>
            {selectionOn && (
              <Th style={{ width: 84 }}>
                <div style={selectionHeaderStyle}>
                  <SelectAllCheckbox
                    checked={pageAllSelected}
                    indeterminate={pageSomeSelected}
                    disabled={pageSelectableIds.length === 0}
                    onChange={() => onSelectAllPage?.(pageSelectableIds, pageAllSelected)}
                  />
                  <span>Выбор</span>
                </div>
              </Th>
            )}
            <Th>Тип</Th>
            <Th>Имя</Th>
            <Th>Статус</Th>
            <Th>Прогресс</Th>
            <Th right>Rows</Th>
            <Th right>Размер</Th>
            <Th right>Скорость</Th>
            <Th right>Compat</Th>
            <Th>Issues</Th>
            <Th right>ETA</Th>
            <Th>{""}</Th>
          </tr>
        </thead>
        <tbody>
          {(() => {
            const colSpan = selectionOn ? 12 : 11;
            const rowOf = (o: SchemaObject) => (
              <ObjectRow
                key={o.id}
                o={o}
                onOpen={onOpen}
                onAction={onAction}
                selectable={selectionOn && selectableIds!.has(o.id)}
                selected={selectionOn && selectedIds!.has(o.id)}
                onToggle={selectionOn ? onToggleSelect : undefined}
              />
            );
            if (layout === "grouped" && grouped.length > 0) {
              return grouped.flatMap(([group, rows]) => [
                <tr key={`g-${group}`} style={{ background: t.bg.s2 }}>
                  <td colSpan={colSpan} style={{ padding: "6px 12px" }}>
                    <span style={{
                      fontSize: "10.5px", fontWeight: 700,
                      textTransform: "uppercase", letterSpacing: "0.08em",
                      color: t.text.secondary, marginRight: 8,
                    }}>{group}</span>
                    <span style={{ fontFamily: t.font.mono, fontSize: "10.5px", color: t.text.muted }}>
                      {rows.length}
                    </span>
                  </td>
                </tr>,
                ...rows.map(rowOf),
              ]);
            }
            return pageItems.map(rowOf);
          })()}
          {total === 0 && (
            <tr><td colSpan={selectionOn ? 12 : 11} style={{
              textAlign: "center", padding: 40, color: t.text.muted,
            }}>Нет объектов под текущие фильтры</td></tr>
          )}
        </tbody>
      </table>
      {pagingOn && total > 0 && (
        <PaginationBar
          page={page!}
          pageSize={pageSize!}
          total={total}
          totalPages={totalPages}
          startIdx={startIdx}
          endIdx={endIdx}
          onPageChange={onPageChange!}
          onPageSizeChange={onPageSizeChange}
        />
      )}
    </div>
  );
}

function PaginationBar({
  page, pageSize, total, totalPages, startIdx, endIdx,
  onPageChange, onPageSizeChange,
}: {
  page:            number;
  pageSize:        number;
  total:           number;
  totalPages:      number;
  startIdx:        number;
  endIdx:          number;
  onPageChange:    (p: number) => void;
  onPageSizeChange?: (n: number) => void;
}) {
  const canPrev = page > 1;
  const canNext = page < totalPages;
  return (
    <div style={{
      display: "flex", alignItems: "center", justifyContent: "space-between",
      gap: 12, padding: "8px 14px",
      background: t.bg.s2,
      borderTop: `1px solid ${t.border.subtle}`,
      fontSize: 12,
    }}>
      <span style={{ color: t.text.muted, fontFamily: t.font.mono, fontSize: "11.5px" }}>
        {startIdx + 1}–{endIdx} из {total}
      </span>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        {onPageSizeChange && (
          <>
            <span style={{ color: t.text.muted, fontSize: 11 }}>по</span>
            <select
              value={pageSize}
              onChange={e => onPageSizeChange(Number(e.target.value))}
              style={{
                padding: "3px 6px", fontSize: 12,
                background: t.bg.s1, color: t.text.primary,
                border: `1px solid ${t.border.subtle}`,
                borderRadius: t.radius.sm,
                fontFamily: t.font.mono,
              }}
            >
              <option value={10}>10</option>
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
              <option value={200}>200</option>
            </select>
            <span style={{ width: 10 }}/>
          </>
        )}
        <PageBtn label="‹" disabled={!canPrev} onClick={() => onPageChange(page - 1)}/>
        <span style={{
          fontFamily: t.font.mono, fontSize: "11.5px",
          padding: "3px 8px",
          color: t.text.primary,
        }}>
          {page} / {totalPages}
        </span>
        <PageBtn label="›" disabled={!canNext} onClick={() => onPageChange(page + 1)}/>
      </div>
    </div>
  );
}

function PageBtn({ label, disabled, onClick }: {
  label:    string;
  disabled: boolean;
  onClick:  () => void;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        padding: "3px 9px",
        background: t.bg.s1,
        border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.sm,
        color: disabled ? t.text.muted : t.text.primary,
        cursor: disabled ? "default" : "pointer",
        fontFamily: t.font.mono,
        fontSize: 13,
        opacity: disabled ? 0.5 : 1,
      }}
    >
      {label}
    </button>
  );
}

function Th({ children, right, style }: { children: React.ReactNode; right?: boolean; style?: React.CSSProperties }) {
  return (
    <th style={{
      textAlign: right ? "right" : "left",
      fontSize: 10, textTransform: "uppercase", letterSpacing: "0.05em",
      color: t.text.muted, fontWeight: 600,
      padding: "8px 12px",
      borderBottom: `1px solid ${t.border.subtle}`,
      background: t.bg.s2,
      position: "sticky", top: 0,
      ...style,
    }}>
      {children}
    </th>
  );
}

const ObjectRow = React.memo(function ObjectRow({
  o, onOpen, onAction,
  selectable, selected, onToggle,
}: {
  o: SchemaObject;
  onOpen:   (o: SchemaObject) => void;
  onAction: (o: SchemaObject, a: "pause" | "retry" | "more") => void;
  selectable?: boolean;
  selected?:   boolean;
  onToggle?:   (id: string) => void;
}) {
  const isData = o.type === "TABLE" || o.type === "INDEX" || o.type === "MVIEW";
  const status: ObjectStatus = o.status;
  const tone =
    status === "error" ? "error" :
    status === "warn"  ? "warn"  :
    status === "done"  ? "ok"    :
                         "info";

  const rowBg =
    status === "error"   ? `color-mix(in oklab, ${t.tone.errorSoft} 50%, transparent)` :
    status === "running" ? `color-mix(in oklab, ${t.tone.infoSoft}  32%, transparent)` :
                           "transparent";

  const typeColor = TYPE_COLORS[o.type];

  return (
    <tr
      onClick={() => onOpen(o)}
      style={{
        cursor: "pointer",
        background: rowBg,
        transition: "background 80ms",
      }}
      onMouseEnter={e => {
        if (status !== "error" && status !== "running") {
          (e.currentTarget as HTMLTableRowElement).style.background = t.bg.s2;
        }
      }}
      onMouseLeave={e => {
        (e.currentTarget as HTMLTableRowElement).style.background = rowBg;
      }}
    >
      {onToggle !== undefined && (
        <Td>
          <div onClick={e => e.stopPropagation()} style={selectionCellStyle}>
            <SelectionCheckbox
              checked={!!selected}
              disabled={!selectable}
              onToggle={() => onToggle(o.id)}
              ariaLabel={selectable ? `Выбрать ${o.name}` : "Недоступно для выбора"}
              title={selectable ? "" : "Недоступно для выбранной пачки"}
            />
          </div>
        </Td>
      )}
      <Td>
        <span style={{
          display: "inline-block",
          fontFamily: t.font.mono,
          fontSize: "9.5px", fontWeight: 600,
          letterSpacing: "0.05em", textTransform: "uppercase",
          padding: "2px 6px", borderRadius: 3,
          background: typeColor.bg, color: typeColor.fg,
        }}>
          {OBJECT_TYPES[o.type].label}
        </span>
      </Td>
      <Td>
        <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap" }}>
          <span style={{ fontFamily: t.font.mono, fontSize: "12.5px", fontWeight: 500, letterSpacing: "-0.01em" }}>
            {o.name}
          </span>
          <MigrationChips o={o}/>
        </div>
        {o.note && (
          <div style={{
            fontSize: "10.5px", color: t.text.muted, marginTop: 2,
            maxWidth: 460, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          }}>
            {o.note}
          </div>
        )}
      </Td>
      <Td><ObjStatusBadge tone={STATUS_MAP[status].tone} label={STATUS_MAP[status].label}/></Td>
      <Td>
        {status === "queued" || status === "skipped" ? (
          <span style={{ color: t.text.muted, fontFamily: t.font.mono, fontSize: 11 }}>
            {o.eta === "blocked" ? "blocked" : "—"}
          </span>
        ) : (
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <ProgressBar value={o.progress} tone={tone}/>
            <span style={{
              fontFamily: t.font.mono, fontSize: "10.5px",
              minWidth: 32, textAlign: "right", color: t.text.muted,
            }}>
              {o.progress.toFixed(0)}%
            </span>
          </div>
        )}
      </Td>
      <Td right mono>
        {o.rows == null ? <span style={{ color: t.text.muted }}>—</span> : (
          <>
            <span>{fmtCompactNum(o.rowsDone)}</span>
            <span style={{ color: t.text.muted }}>/{fmtCompactNum(o.rows)}</span>
          </>
        )}
      </Td>
      <Td right mono>{o.sizeMb < 0.1 ? <span style={{ color: t.text.muted }}>—</span> : fmtMb(o.sizeMb)}</Td>
      <Td right mono>
        {o.rowsPerSec > 0
          ? <>{fmtCompactNum(o.rowsPerSec)}<span style={{ color: t.text.muted }}>/s</span></>
          : o.mbPerSec > 0
            ? <>{o.mbPerSec}<span style={{ color: t.text.muted }}> MB/s</span></>
            : <span style={{ color: t.text.muted }}>—</span>}
      </Td>
      <Td right mono>
        {!isData && o.compat < 100 ? (
          <span style={{
            fontWeight: 500, padding: "1px 5px", borderRadius: 3,
            background: o.compat < 80 ? t.tone.errorSoft : o.compat < 95 ? t.tone.warnSoft : "transparent",
            color:      o.compat < 80 ? t.tone.error     : o.compat < 95 ? t.tone.warn     : t.tone.ok,
          }}>{o.compat}%</span>
        ) : <span style={{ color: t.text.muted }}>{o.compat}%</span>}
      </Td>
      <Td>
        {o.err > 0 && (
          <span style={{
            display: "inline-flex", alignItems: "center", gap: 3,
            fontSize: "10.5px", padding: "1px 5px", borderRadius: 3,
            fontFamily: t.font.mono, fontWeight: 500,
            background: t.tone.errorSoft, color: t.tone.error,
            marginRight: 4,
          }}>
            <Icon name="error" size={11}/> {o.err}
          </span>
        )}
        {o.warn > 0 && (
          <span style={{
            display: "inline-flex", alignItems: "center", gap: 3,
            fontSize: "10.5px", padding: "1px 5px", borderRadius: 3,
            fontFamily: t.font.mono, fontWeight: 500,
            background: t.tone.warnSoft, color: t.tone.warn,
          }}>
            <Icon name="warn" size={11}/> {o.warn}
          </span>
        )}
        {o.err === 0 && o.warn === 0 && <span style={{ color: t.text.muted }}>—</span>}
      </Td>
      <Td right mono>{o.eta}</Td>
      <Td right>
        <div style={{ display: "flex", gap: 2, justifyContent: "flex-end" }}
             onClick={e => e.stopPropagation()}>
          {status === "running" && <RowBtn icon="pause"  onClick={() => onAction(o, "pause")}/>}
          {status === "error"   && <RowBtn icon="rotate" onClick={() => onAction(o, "retry")}/>}
          <RowBtn icon="more" onClick={() => onAction(o, "more")}/>
        </div>
      </Td>
    </tr>
  );
});

function Td({ children, right, mono }: {
  children?: React.ReactNode;
  right?:    boolean;
  mono?:     boolean;
}) {
  return (
    <td style={{
      padding: "7px 12px",
      borderBottom: `1px solid ${t.border.subtle}`,
      verticalAlign: "middle",
      height: 32,
      textAlign:  right ? "right" : "left",
      whiteSpace: right ? "nowrap" : undefined,
      fontFamily: mono ? t.font.mono : undefined,
    }}>
      {children}
    </td>
  );
}

function SelectAllCheckbox({
  checked, indeterminate, disabled, onChange,
}: {
  checked:        boolean;
  indeterminate:  boolean;
  disabled:       boolean;
  onChange:       () => void;
}) {
  return (
    <SelectionCheckbox
      checked={checked}
      indeterminate={indeterminate}
      disabled={disabled}
      onToggle={onChange}
      ariaLabel="Выбрать все на странице"
      title={disabled ? "Нет таблиц для выбора" : "Выбрать все таблицы на странице"}
    />
  );
}

const selectionHeaderStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  gap: 6,
};

const selectionCellStyle: React.CSSProperties = {
  display: "inline-flex",
  width: 60,
  justifyContent: "center",
};

function SelectionCheckbox({
  checked,
  indeterminate = false,
  disabled,
  onToggle,
  ariaLabel,
  title,
}: {
  checked: boolean;
  indeterminate?: boolean;
  disabled: boolean;
  onToggle: () => void;
  ariaLabel: string;
  title?: string;
}) {
  return (
    <VisibleCheckbox
      checked={checked}
      indeterminate={indeterminate}
      disabled={disabled}
      onChange={onToggle}
      ariaLabel={ariaLabel}
      title={title}
    />
  );
}

// ── Chips: strategy + key type для TABLE-миграций ───────────────────────────
// Чипы показываются только когда у объекта есть привязанная миграция
// (strategy непустой) — DDL-only TABLE без миграции их не получает.

const KEY_LABEL: Record<string, string> = {
  PRIMARY_KEY:  "PK",
  UNIQUE_KEY:   "UK",
  USER_DEFINED: "USER",
  ROWID:        "ROWID",
  NONE:         "NO KEY",
};

function MigrationChips({ o }: { o: SchemaObject }) {
  if (o.type !== "TABLE") return null;
  const columnChip = hasColumnDiff(o)
    ? <MiniChip label="COLS Δ" bg={t.tone.warnSoft} fg={t.tone.warn} title={columnDiffTitle(o)}/>
    : null;

  // Чип supplemental logging — рендерим всегда, когда знаем (true/false).
  // Для CDC миграции отсутствие supp-log = красный, для BULK/orphan — просто
  // info-метка, чтобы видеть готовность таблицы к CDC.
  const suppChip = (() => {
    if (o.hasSuppLog === undefined || o.hasSuppLog === null) return null;
    if (o.hasSuppLog) {
      return <MiniChip label="SUPP" bg={`color-mix(in oklab, ${t.tone.ok} 18%, transparent)`} fg={t.tone.ok}/>;
    }
    const isCdc = !!o.strategy && o.strategy.startsWith("CDC_");
    return isCdc
      ? <MiniChip label="NO SUPP" bg={t.tone.errorSoft} fg={t.tone.error}/>
      : <MiniChip label="NO SUPP" bg={t.bg.s3} fg={t.text.muted}/>;
  })();

  // Таблица без миграции — показываем только наличие ключа (если знаем).
  if (!o.strategy) {
    const keyChip =
      o.hasPk === undefined && o.hasUk === undefined ? null
      : o.hasPk ? <MiniChip label="PK"     bg={`color-mix(in oklab, ${t.tone.ok}   18%, transparent)`} fg={t.tone.ok}/>
      : o.hasUk ? <MiniChip label="UK"     bg={`color-mix(in oklab, ${t.tone.info} 14%, transparent)`} fg={t.tone.info}/>
                : <MiniChip label="NO KEY" bg={t.tone.errorSoft}                                       fg={t.tone.error}/>;
    if (!keyChip && !suppChip && !columnChip) return null;
    return <>{keyChip}{suppChip}{columnChip}</>;
  }

  const isCdc   = o.strategy.startsWith("CDC_");
  const isStage = o.strategy.endsWith("_STAGE");
  const keyTxt  = o.keyType ? (KEY_LABEL[o.keyType] || o.keyType) : null;
  const keyTone =
    o.keyType === "PRIMARY_KEY"  ? { bg: `color-mix(in oklab, ${t.tone.ok}    18%, transparent)`, fg: t.tone.ok    } :
    o.keyType === "UNIQUE_KEY"   ? { bg: `color-mix(in oklab, ${t.tone.info}  14%, transparent)`, fg: t.tone.info  } :
    o.keyType === "USER_DEFINED" ? { bg: `color-mix(in oklab, ${t.tone.warn}  14%, transparent)`, fg: t.tone.warn  } :
    o.keyType === "ROWID"        ? { bg: `color-mix(in oklab, ${t.tone.warn}  18%, transparent)`, fg: t.tone.warn  } :
    o.keyType === "NONE"         ? { bg: t.tone.errorSoft,                                        fg: t.tone.error } :
                                   { bg: t.bg.s3,                                                 fg: t.text.muted };
  return (
    <>
      <MiniChip
        label={isCdc ? "CDC" : "BULK"}
        bg={isCdc ? `color-mix(in oklab, ${t.tone.info} 18%, transparent)` : t.bg.s3}
        fg={isCdc ? t.tone.info : t.text.secondary}
      />
      <MiniChip
        label={isStage ? "STAGE" : "DIRECT"}
        bg={t.bg.s3}
        fg={t.text.muted}
      />
      {keyTxt && <MiniChip label={keyTxt} bg={keyTone.bg} fg={keyTone.fg}/>}
      {suppChip}
      {columnChip}
    </>
  );
}

function MiniChip({ label, bg, fg, title }: { label: string; bg: string; fg: string; title?: string }) {
  return (
    <span title={title} style={{
      display: "inline-block",
      fontFamily: t.font.mono,
      fontSize: "9.5px", fontWeight: 700,
      letterSpacing: "0.05em", textTransform: "uppercase",
      padding: "1px 5px", borderRadius: 3,
      background: bg, color: fg,
    }}>
      {label}
    </span>
  );
}

function RowBtn({ icon, onClick }: { icon: "pause" | "rotate" | "more"; onClick: () => void }) {
  return (
    <button onClick={onClick} style={{
      background: "transparent", border: "none",
      padding: 4, borderRadius: 4,
      color: t.text.muted, cursor: "pointer",
      display: "inline-flex",
    }}>
      <Icon name={icon} size={13}/>
    </button>
  );
}
