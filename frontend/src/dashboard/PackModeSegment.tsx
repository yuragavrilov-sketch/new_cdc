import { t } from "../theme";
import { PACK_DEFINITIONS, type TablePackAddMode } from "./packModel";

interface Props {
  value: TablePackAddMode;
  onChange: (mode: TablePackAddMode) => void;
  locked?: boolean;
  disabled?: boolean;
}

const OPTIONS: Array<{ mode: TablePackAddMode; title: string; hint: string; tone: "bulk" | "cdc" }> = [
  {
    mode: "historical",
    title: PACK_DEFINITIONS.bulk.title,
    hint: "Исторические таблицы без CDC",
    tone: "bulk",
  },
  {
    mode: "cdc",
    title: PACK_DEFINITIONS.cdc.title,
    hint: "Таблицы для единой CDC-пачки",
    tone: "cdc",
  },
];

export function PackModeSegment({ value, onChange, locked = false, disabled = false }: Props) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
      {OPTIONS.map(option => {
        const active = value === option.mode;
        const blocked = disabled || (locked && !active);
        const inert = disabled || locked;
        const colors = option.tone === "cdc"
          ? { bg: t.blue.bg, border: t.blue.base, fg: t.blue.fg }
          : { bg: t.amber.bg, border: t.amber.base, fg: t.amber.fg };
        return (
          <button
            key={option.mode}
            type="button"
            disabled={blocked}
            onClick={() => {
              if (!blocked && !locked) onChange(option.mode);
            }}
            title={locked && !active ? "Пачка выбрана на предыдущем шаге" : option.hint}
            style={{
              minWidth: 0,
              minHeight: 48,
              display: "flex",
              flexDirection: "column",
              justifyContent: "center",
              gap: 2,
              padding: "7px 10px",
              borderRadius: t.radius.sm,
              border: `1px solid ${active ? colors.border : t.border.subtle}`,
              background: active ? colors.bg : t.bg.s2,
              color: active ? colors.fg : t.text.primary,
              cursor: inert ? "default" : "pointer",
              opacity: blocked && !active ? 0.45 : 1,
              textAlign: "left",
            }}
          >
            <span style={{
              fontSize: 12,
              fontWeight: 700,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}>
              {option.title}
            </span>
            <span style={{
              fontSize: 10.5,
              color: active ? colors.fg : t.text.muted,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}>
              {option.hint}
            </span>
          </button>
        );
      })}
    </div>
  );
}
