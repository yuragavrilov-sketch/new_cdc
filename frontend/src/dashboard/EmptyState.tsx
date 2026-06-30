import React from "react";
import { t } from "../theme";
import { Icon } from "../components/ui";

interface Props {
  onCreate: () => void;
}

export function DashboardEmptyState({ onCreate }: Props) {
  return (
    <div style={{
      background:   t.bg.s1,
      border:       `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.xl,
      padding:      "48px 32px",
      boxShadow:    t.shadow.s1,
      textAlign:    "center",
    }}>
      <div style={{
        width: 48, height: 48, borderRadius: 12,
        background: t.tone.accentSoft, color: t.tone.accent,
        display: "inline-grid", placeItems: "center", marginBottom: 16,
      }}>
        <Icon name="db" size={24}/>
      </div>
      <h2 style={{ margin: 0, fontSize: 18, fontWeight: 600, marginBottom: 8 }}>
        Нет активных миграций схем
      </h2>
      <p style={{
        margin: 0, marginBottom: 20,
        fontSize: 13, color: t.text.muted,
        maxWidth: 420,
        marginLeft: "auto", marginRight: "auto",
        lineHeight: 1.5,
      }}>
        Дашборд показывает прогресс одной операционной миграции схемы Oracle.
        Создайте миграцию через мастер, чтобы начать.
      </p>
      <button onClick={onCreate} style={{
        display: "inline-flex", alignItems: "center", gap: 6,
        padding: "8px 16px",
        background: t.text.primary, color: t.text.inverse,
        border: `1px solid ${t.text.primary}`,
        borderRadius: t.radius.sm,
        fontSize: 13, fontWeight: 500, cursor: "pointer",
      }}>
        <Icon name="plus" size={15}/>
        Новая миграция
      </button>
    </div>
  );
}
