import React, { useState } from "react";
import { t } from "../theme";
import { DDLCatalog } from "../components/DDLCatalog/DDLCatalog";
import { TargetPrep } from "../components/TargetPrep";
import { DataCompare } from "../components/DataCompare";
import { Checklist } from "../components/Checklist";

type RulesTab = "catalog" | "target-prep" | "data-compare" | "checklist";

const TABS: { key: RulesTab; label: string }[] = [
  { key: "catalog",      label: "DDL Каталог"      },
  { key: "target-prep",  label: "Подготовка таргета" },
  { key: "data-compare", label: "Сравнение данных"  },
  { key: "checklist",    label: "Чек-лист"          },
];

/** Sub-tab container for the "Правила conversion" section. Groups conversion-related
 *  tools (DDL catalog, target prep, data verification, pre-cutover checklist). */
export function RulesTabs({ onOpenMigration }: { onOpenMigration?: () => void }) {
  const [active, setActive] = useState<RulesTab>("catalog");
  return (
    <div>
      <div style={{
        display: "flex", gap: 0,
        borderBottom: `1px solid ${t.border.subtle}`,
        marginBottom: 16,
      }}>
        {TABS.map(tab => (
          <button
            key={tab.key}
            onClick={() => setActive(tab.key)}
            style={{
              background: "none", border: "none",
              borderBottom: `2px solid ${active === tab.key ? t.tone.accent : "transparent"}`,
              color: active === tab.key ? t.text.primary : t.text.muted,
              padding: "8px 16px",
              fontSize: 13, fontWeight: active === tab.key ? 600 : 500,
              cursor: "pointer", marginBottom: -1,
            }}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {active === "catalog"      && <DDLCatalog onOpenMigration={onOpenMigration}/>}
      {active === "target-prep"  && <TargetPrep/>}
      {active === "data-compare" && <DataCompare/>}
      {active === "checklist"    && <Checklist/>}
    </div>
  );
}
