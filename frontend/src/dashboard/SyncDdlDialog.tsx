import React, { useMemo, useState } from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";
import { OBJECT_TYPES, type SchemaObject } from "./types";
import type { DdlApplyAction } from "./api";
import { primaryActionStyle, secondaryActionStyle } from "./buttonStyles";

/** A planned action plus its candidate objects. */
export interface SyncGroup {
  action:       DdlApplyAction;
  title:        string;
  description:  string;
  items:        SchemaObject[];
  destructive:  boolean;     // if true, default-uncheck data-bearing items
}

/** What the dialog returns on submit — objects already filtered by selection. */
export interface SyncSelection {
  action: DdlApplyAction;
  items:  SchemaObject[];
}

interface Props {
  groups:   SyncGroup[];
  onClose:  () => void;
  onSubmit: (selection: SyncSelection[]) => Promise<void>;
  title?: string;
  submitLabel?: string;
}

const DATA_BEARING_TYPES = new Set(["TABLE", "MVIEW"]);

export function SyncDdlDialog({
  groups,
  onClose,
  onSubmit,
  title = "Синхронизация DDL в target",
  submitLabel = "Применить",
}: Props) {
  // Initial selection: everything checked EXCEPT data-bearing objects in
  // destructive groups (we don't want to drop tables by default).
  const [checked, setChecked] = useState<Record<string, boolean>>(() => {
    const init: Record<string, boolean> = {};
    for (const g of groups) {
      for (const o of g.items) {
        const isDangerous = g.destructive && DATA_BEARING_TYPES.has(o.type);
        init[o.id] = !isDangerous;
      }
    }
    return init;
  });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const counts = useMemo(() => {
    const out: Array<{ group: SyncGroup; selectedItems: SchemaObject[] }> = [];
    for (const g of groups) {
      out.push({
        group:         g,
        selectedItems: g.items.filter(o => checked[o.id]),
      });
    }
    return out;
  }, [groups, checked]);

  const totalSelected = counts.reduce((a, c) => a + c.selectedItems.length, 0);
  const dangerousSelected = counts.reduce(
    (a, c) => a + (c.group.destructive
      ? c.selectedItems.filter(o => DATA_BEARING_TYPES.has(o.type)).length
      : 0),
    0,
  );

  const toggleOne = (id: string) =>
    setChecked(prev => ({ ...prev, [id]: !prev[id] }));

  const toggleGroup = (g: SyncGroup, nextValue: boolean) =>
    setChecked(prev => {
      const next = { ...prev };
      for (const o of g.items) next[o.id] = nextValue;
      return next;
    });

  const submit = async () => {
    if (totalSelected === 0) return;
    setBusy(true);
    setError(null);
    try {
      await onSubmit(
        counts
          .filter(c => c.selectedItems.length > 0)
          .map(c => ({ action: c.group.action, items: c.selectedItems })),
      );
      onClose();
    } catch (e) {
      setError((e as Error).message);
      setBusy(false);
    }
  };

  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed", inset: 0,
        background: "rgba(20,20,20,0.32)",
        backdropFilter: "blur(2px)",
        zIndex: 60,
        display: "flex", alignItems: "center", justifyContent: "center",
        padding: 24,
        animation: "fadeIn 140ms ease-out",
      }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          width: "min(720px, 100%)",
          maxHeight: "min(86vh, 800px)",
          background: t.bg.s1,
          borderRadius: t.radius.lg,
          border: `1px solid ${t.border.subtle}`,
          boxShadow: t.shadow.s3,
          display: "flex", flexDirection: "column",
          animation: "popIn 180ms cubic-bezier(.22,.61,.36,1)",
        }}
      >
        {/* Header */}
        <div style={{
          padding: "18px 22px 14px",
          borderBottom: `1px solid ${t.border.subtle}`,
          display: "flex", justifyContent: "space-between", alignItems: "flex-start",
          gap: 12,
        }}>
          <div>
            <h2 style={{
              margin: 0, fontSize: 16, fontWeight: 600,
              letterSpacing: "-0.01em",
            }}>
              {title}
            </h2>
            <div style={{
              fontSize: "11.5px", color: t.text.muted, marginTop: 4,
              fontFamily: t.font.mono,
            }}>
              Выбрано: {totalSelected} из {counts.reduce((a, c) => a + c.group.items.length, 0)}
            </div>
          </div>
          <button
            onClick={onClose}
            aria-label="Закрыть"
            style={{
              padding: 6, borderRadius: t.radius.sm,
              border: "none", background: "transparent",
              cursor: "pointer", color: t.text.muted,
            }}
          >
            <Icon name="close" size={16}/>
          </button>
        </div>

        {/* Body — scrollable list of groups */}
        <div style={{
          flex: 1, overflowY: "auto",
          padding: "12px 22px 16px",
          display: "flex", flexDirection: "column", gap: 14,
        }}>
          {counts.map(({ group, selectedItems }) => (
            <GroupSection
              key={group.action + "-" + group.title}
              group={group}
              checked={checked}
              selectedCount={selectedItems.length}
              onToggleOne={toggleOne}
              onToggleGroup={v => toggleGroup(group, v)}
            />
          ))}
        </div>

        {/* Footer — actions */}
        <div style={{
          padding: "12px 22px 16px",
          borderTop: `1px solid ${t.border.subtle}`,
          display: "flex", justifyContent: "space-between", alignItems: "center",
          gap: 12, flexWrap: "wrap",
        }}>
          <div style={{ fontSize: 11, color: t.text.muted, fontFamily: t.font.mono }}>
            {dangerousSelected > 0 && (
              <span style={{ color: t.tone.error, fontWeight: 600 }}>
                ⚠ С потерей данных: {dangerousSelected}
              </span>
            )}
            {error && (
              <span style={{ color: t.tone.error, marginLeft: 12 }}>{error}</span>
            )}
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <button
              onClick={onClose}
              disabled={busy}
              style={secondaryActionStyle(busy)}
            >
              Отмена
            </button>
            <button
              onClick={submit}
              disabled={busy || totalSelected === 0}
              style={primaryActionStyle(busy || totalSelected === 0, dangerousSelected > 0)}
            >
              {busy ? "в очередь..." : `${submitLabel} · ${totalSelected}`}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function GroupSection({
  group, checked, selectedCount, onToggleOne, onToggleGroup,
}: {
  group:         SyncGroup;
  checked:       Record<string, boolean>;
  selectedCount: number;
  onToggleOne:   (id: string) => void;
  onToggleGroup: (v: boolean) => void;
}) {
  // Hook must come BEFORE any conditional return to keep hook order stable
  const [expanded, setExpanded] = useState(true);
  if (group.items.length === 0) return null;
  const allSelected = selectedCount === group.items.length;
  const noneSelected = selectedCount === 0;
  // Indeterminate: some but not all selected — show as a half-state visually
  const partial = !allSelected && !noneSelected;
  const dangerousCount = group.destructive
    ? group.items.filter(o => DATA_BEARING_TYPES.has(o.type)).length
    : 0;

  return (
    <div style={{
      border: `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      background: t.bg.s2,
      overflow: "hidden",
    }}>
      <div style={{
        padding: "10px 12px",
        display: "flex", alignItems: "center", gap: 10,
        background: t.bg.s1,
        borderBottom: expanded ? `1px solid ${t.border.subtle}` : "none",
      }}>
        <CheckboxBox
          checked={allSelected}
          partial={partial}
          onClick={() => onToggleGroup(!allSelected)}
        />
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: "12.5px", fontWeight: 600 }}>
            {group.title}{" "}
            <span style={{ fontFamily: t.font.mono, fontSize: 11, color: t.text.muted, fontWeight: 400 }}>
              {selectedCount}/{group.items.length}
            </span>
          </div>
          <div style={{ fontSize: 11, color: t.text.muted, marginTop: 2 }}>
            {group.description}
            {dangerousCount > 0 && (
              <span style={{ color: t.tone.error, marginLeft: 6, fontWeight: 600 }}>
                · {dangerousCount} с DROP
              </span>
            )}
          </div>
        </div>
        <button
          onClick={() => setExpanded(e => !e)}
          style={{
            padding: 4, border: "none",
            background: "transparent", cursor: "pointer",
            color: t.text.muted, fontSize: 11,
          }}
        >
          {expanded ? "свернуть" : "развернуть"}
        </button>
      </div>

      {expanded && (
        <div style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))",
          gap: 1, background: t.border.subtle, padding: 1,
        }}>
          {group.items.map(o => {
            const isDangerous = group.destructive && DATA_BEARING_TYPES.has(o.type);
            const sel = checked[o.id];
            return (
              <label
                key={o.id}
                style={{
                  display: "flex", alignItems: "center", gap: 8,
                  padding: "6px 10px",
                  background: t.bg.s1, cursor: "pointer",
                  fontSize: "11.5px",
                }}
              >
                <CheckboxBox
                  checked={!!sel}
                  partial={false}
                  onClick={() => onToggleOne(o.id)}
                />
                <span style={{
                  fontFamily: t.font.mono,
                  fontSize: "9.5px", fontWeight: 600,
                  letterSpacing: "0.05em",
                  color: t.text.muted,
                  textTransform: "uppercase",
                  flexShrink: 0,
                }}>
                  {OBJECT_TYPES[o.type]?.label || o.type}
                </span>
                <span style={{
                  fontFamily: t.font.mono,
                  color: t.text.primary,
                  overflow: "hidden", textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}>
                  {o.name}
                </span>
                {isDangerous && (
                  <span title="DROP+CREATE — потеря данных" style={{
                    marginLeft: "auto",
                    fontSize: 10, color: t.tone.error, flexShrink: 0,
                  }}>
                    ⚠
                  </span>
                )}
              </label>
            );
          })}
        </div>
      )}
    </div>
  );
}

function CheckboxBox({ checked, partial, onClick }: {
  checked: boolean;
  partial: boolean;
  onClick: () => void;
}) {
  const isOn = checked || partial;
  return (
    <span
      role="checkbox"
      aria-checked={partial ? "mixed" : checked}
      tabIndex={0}
      onClick={e => { e.preventDefault(); e.stopPropagation(); onClick(); }}
      onKeyDown={e => {
        if (e.key === " " || e.key === "Enter") {
          e.preventDefault(); onClick();
        }
      }}
      style={{
        display: "inline-grid", placeItems: "center",
        width: 16, height: 16, flexShrink: 0,
        borderRadius: 4,
        background: isOn ? t.text.primary : t.bg.s1,
        border: `1.5px solid ${isOn ? t.text.primary : t.border.strong}`,
        cursor: "pointer",
        transition: "background 80ms ease",
      }}
    >
      {checked && !partial && (
        <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
          <path d="M2 5 L4 7 L8 3" stroke={t.text.inverse} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      )}
      {partial && (
        <span style={{ width: 7, height: 2, background: t.text.inverse, borderRadius: 1 }}/>
      )}
    </span>
  );
}
