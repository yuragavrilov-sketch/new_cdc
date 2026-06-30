import { useState } from "react";
import { t } from "../../theme";
import { Strategy, hasCdc, usesStage, composeStrategy } from "../../types/migration";

interface Props {
  value: Strategy;
  onChange: (s: Strategy) => void;
  truncateTarget: boolean;
  onTruncateChange: (b: boolean) => void;
  /** Disable «С CDC» если коннектор CDC-пачки не RUNNING */
  cdcDisabledReason?: string;
}

export function StrategyPicker({ value, onChange, truncateTarget, onTruncateChange, cdcDisabledReason }: Props) {
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const cdc = hasCdc(value);
  const stage = usesStage(value);

  const setCdc = (c: boolean) => onChange(composeStrategy(c, stage));
  const setStage = (s: boolean) => onChange(composeStrategy(cdc, s));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ fontWeight: 500 }}>Стратегия миграции</div>

      <div style={{ display: "flex", gap: 8 }}>
        <button
          type="button"
          onClick={() => !cdcDisabledReason && setCdc(true)}
          disabled={!!cdcDisabledReason}
          title={cdcDisabledReason || ""}
          style={{
            flex: 1, padding: "8px 12px",
            border: `1px solid ${cdc ? t.purple.base : t.border.base}`,
            background: cdc ? t.purple.bg : t.bg.s2,
            color: cdc ? t.purple.fg : t.text.muted,
            cursor: cdcDisabledReason ? "not-allowed" : "pointer",
            opacity: cdcDisabledReason ? 0.5 : 1,
          }}
        >
          С CDC
        </button>
        <button
          type="button"
          onClick={() => setCdc(false)}
          style={{
            flex: 1, padding: "8px 12px",
            border: `1px solid ${!cdc ? t.green.base : t.border.base}`,
            background: !cdc ? t.green.bg : t.bg.s2,
            color: !cdc ? t.green.fg : t.text.muted,
            cursor: "pointer",
          }}
        >
          Без CDC
        </button>
      </div>
      <div style={{ fontSize: 12, color: t.text.muted }}>
        {cdc
          ? "Bulk-загрузка + apply из Kafka, миграция остаётся в STEADY_STATE."
          : "Один разовый перенос для исторических/неизменяемых таблиц, завершается после DATA_VERIFYING. SCN не фиксируется."}
      </div>

      <button
        type="button"
        onClick={() => setAdvancedOpen(o => !o)}
        style={{
          alignSelf: "flex-start",
          background: "transparent", border: "none", padding: 0,
          color: t.text.muted, cursor: "pointer", fontSize: 12,
        }}
      >
        {advancedOpen ? "▼" : "▶"} Дополнительно
      </button>

      {advancedOpen && (
        <div style={{ paddingLeft: 12, display: "flex", flexDirection: "column", gap: 8 }}>
          <div style={{ fontWeight: 500, fontSize: 13 }}>Способ загрузки</div>
          <div style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              onClick={() => setStage(true)}
              style={{
                flex: 1, padding: "6px 10px",
                border: `1px solid ${stage ? t.blue.base : t.border.base}`,
                background: stage ? t.bg.s3 : t.bg.s2,
                color: stage ? t.blue.fg : t.text.muted,
                cursor: "pointer", fontSize: 13,
              }}
            >
              STAGE
            </button>
            <button
              type="button"
              onClick={() => setStage(false)}
              style={{
                flex: 1, padding: "6px 10px",
                border: `1px solid ${!stage ? t.green.base : t.border.base}`,
                background: !stage ? t.green.bg : t.bg.s2,
                color: !stage ? t.green.fg : t.text.muted,
                cursor: "pointer", fontSize: 13,
              }}
            >
              DIRECT
            </button>
          </div>
          <div style={{ fontSize: 12, color: t.text.muted }}>
            {stage
              ? "Через промежуточную stage-таблицу (валидация + TRUNCATE + baseline). Надёжнее."
              : "Прямая загрузка в target. Target triggers отключаются, вторичные индексы пересчитываются после загрузки."}
          </div>
          <div style={{ paddingTop: 8, display: "flex", flexDirection: "column", gap: 4 }}>
            <label style={{ fontWeight: 500, fontSize: 13, cursor: usesStage(value) ? "not-allowed" : "pointer" }}>
              <input
                type="checkbox"
                checked={usesStage(value) ? true : truncateTarget}
                disabled={usesStage(value)}
                onChange={(e) => onTruncateChange(e.target.checked)}
              />
              {" "}Очистить target перед загрузкой (TRUNCATE TABLE)
            </label>
            <div style={{ fontSize: 12, color: t.text.muted }}>
              {usesStage(value)
                ? "Всегда ON для STAGE — таблица очищается перед публикацией baseline."
                : "Если выключено — данные дописываются поверх существующего (возможны PK-конфликты)."}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
