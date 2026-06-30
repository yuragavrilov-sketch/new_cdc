import type { MigrationDetail } from "../../../types/migration";
import { SectionHeader } from "../../ui";
import { t } from "../../../theme";

export function ErrorsTab({ detail }: { detail: MigrationDetail }) {
  const hasMigError = !!(detail.error_code || detail.error_text);

  if (!hasMigError) {
    return (
      <div style={{
        display: "flex", flexDirection: "column", alignItems: "center",
        justifyContent: "center", padding: `${t.space[8]} 0`, gap: t.space[2],
      }}>
        <div style={{ fontSize: t.size.xxl }}>✓</div>
        <div style={{ color: t.green.base, fontSize: t.size.md, fontWeight: 600 }}>Ошибок нет</div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: t.space[3] }}>
      <div>
        <SectionHeader>Ошибка миграции</SectionHeader>
        <div style={{
          background: t.bg.s2, border: `1px solid ${t.red.border}`,
          borderRadius: t.radius.md, padding: t.space[4],
        }}>
          {detail.error_code && (
            <div style={{ marginBottom: t.space[2] }}>
              <div style={{ fontSize: t.size.xs, color: t.text.muted, textTransform: "uppercase", letterSpacing: 0.6, marginBottom: 2 }}>Код</div>
              <span style={{
                fontFamily: t.font.mono, fontSize: t.size.md, color: t.red.fg,
                fontWeight: 700, background: t.red.bg,
                border: `1px solid ${t.red.border}`, borderRadius: t.radius.sm, padding: `2px ${t.space[2]}`,
              }}>
                {detail.error_code}
              </span>
            </div>
          )}
          {detail.failed_phase && (
            <div style={{ marginBottom: t.space[2] }}>
              <div style={{ fontSize: t.size.xs, color: t.text.muted, textTransform: "uppercase", letterSpacing: 0.6, marginBottom: 2 }}>Фаза</div>
              <span style={{ fontSize: t.size.md, color: t.red.fg }}>{detail.failed_phase}</span>
            </div>
          )}
          {detail.error_text && (
            <div>
              <div style={{ fontSize: t.size.xs, color: t.text.muted, textTransform: "uppercase", letterSpacing: 0.6, marginBottom: t.space[1] }}>Подробности</div>
              <pre style={{
                margin: 0, fontFamily: t.font.mono, fontSize: t.size.base, color: t.red.fg,
                whiteSpace: "pre-wrap", wordBreak: "break-word",
                background: t.bg.s2, borderRadius: t.radius.sm, padding: `${t.space[2]} ${t.space[3]}`,
                maxHeight: 320, overflowY: "auto",
                border: `1px solid ${t.red.bg}`,
              }}>
                {detail.error_text}
              </pre>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
