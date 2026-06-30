import { useEffect, useState } from "react";
import { StatTile, SectionHeader } from "../../ui";
import { fmtTs, fmtNum, fmtDuration } from "../../../utils/format";
import { t } from "../../../theme";
import { TERMINAL_PHASES } from "../helpers";
import type { MigrationDetail } from "../../../types/migration";

interface PhasePeriod {
  phase: string;
  startedAt: Date;
  endedAt: Date | null;
  durationMs: number;
}

function useNow(intervalMs = 5000) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}

// Phase color map (reuse a small subset for the timeline bars). Kept as
// hardcoded hex values because these are specific phase-identity colors,
// not part of the general design-token palette.
function phaseBarColor(phase: string): string {
  if (phase === "BULK_LOADING")         return t.amber.dim;
  if (phase === "CDC_CATCHING_UP")      return t.amber.dim;
  if (phase === "STEADY_STATE")         return t.green.dim;
  if (phase === "BASELINE_PUBLISHING")  return t.purple.base;
  if (phase === "FAILED")               return t.red.base;
  if (phase === "CANCELLED")            return t.text.muted;
  if (phase === "COMPLETED")            return t.green.base;
  return t.blue.dim;
}

export function StatisticsTab({ detail }: { detail: MigrationDetail }) {
  const now = useNow(5000);
  const isTerminal = TERMINAL_PHASES.has(detail.phase);

  // Build phase periods from history (history is newest-first → reverse)
  const historyAsc = [...detail.history].reverse();
  const periods: PhasePeriod[] = historyAsc.map((h, i) => {
    const next = historyAsc[i + 1];
    const startedAt = new Date(h.created_at);
    const endedAt   = next ? new Date(next.created_at) : null;
    const refNow    = isTerminal && endedAt == null
      ? new Date(detail.state_changed_at).getTime()
      : now;
    const durationMs = endedAt
      ? endedAt.getTime() - startedAt.getTime()
      : refNow - startedAt.getTime();
    return { phase: h.to_phase, startedAt, endedAt, durationMs };
  });

  const totalMs = periods.reduce((sum, p) => sum + p.durationMs, 0);
  const maxMs   = Math.max(1, ...periods.map(p => p.durationMs));

  const bulkPeriod  = periods.find(p => p.phase === "BULK_LOADING");
  const bulkSec     = bulkPeriod ? bulkPeriod.durationMs / 1000 : null;
  const rowsLoaded  = detail.rows_loaded ?? 0;
  const rowsPerSec  = bulkSec && bulkSec > 0 && rowsLoaded > 0
    ? Math.round(rowsLoaded / bulkSec) : null;

  const chunkPct = detail.total_chunks && detail.total_chunks > 0
    ? Math.round(((detail.chunks_done ?? 0) / detail.total_chunks) * 100) : null;

  return (
    <div>
      {/* ── Total duration banner ── */}
      <div style={{
        background: t.bg.s1, border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.md, padding: `${t.space[3]} ${t.space[4]}`,
        marginBottom: t.space[4],
        display: "flex", alignItems: "center", justifyContent: "space-between",
        flexWrap: "wrap", gap: t.space[2],
      }}>
        <div>
          <div style={{
            fontSize: t.size.xs, color: t.text.disabled,
            textTransform: "uppercase", letterSpacing: 0.8,
          }}>
            Общая продолжительность
          </div>
          <div style={{
            fontSize: t.size.xxl, fontWeight: 800, color: t.text.primary,
            fontVariantNumeric: "tabular-nums", lineHeight: 1.2, marginTop: 2,
          }}>
            {fmtDuration(totalMs)}
          </div>
        </div>
        <div style={{
          textAlign: "right", fontSize: t.size.sm,
          color: t.text.disabled, lineHeight: 1.8,
        }}>
          <div>Начало: {fmtTs(detail.created_at, { withSeconds: true })}</div>
          <div>
            {isTerminal
              ? `Завершено: ${fmtTs(detail.state_changed_at, { withSeconds: true })}`
              : `Текущая фаза: ${fmtDuration(periods[periods.length - 1]?.durationMs ?? 0)}`}
          </div>
        </div>
      </div>

      {/* ── Key metrics grid ── */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))",
        gap: t.space[2], marginBottom: t.space[4],
      }}>
        <StatTile
          label="Строк загружено"
          value={fmtNum(rowsLoaded)}
          sub={rowsPerSec != null ? `~${fmtNum(rowsPerSec)} р/сек` : undefined}
          color={t.green.fg}
        />
        {detail.total_chunks != null && (
          <StatTile
            label="Чанки"
            value={`${detail.chunks_done ?? 0} / ${detail.total_chunks}`}
            sub={chunkPct != null ? `${chunkPct}%` : undefined}
            color={detail.chunks_failed ? t.red.fg : t.green.fg}
          />
        )}
        {detail.chunks_failed != null && detail.chunks_failed > 0 && (
          <StatTile
            label="Ошибки чанков"
            value={detail.chunks_failed}
            color={t.red.base}
          />
        )}
        {bulkPeriod && (
          <StatTile
            label="Время bulk load"
            value={fmtDuration(bulkPeriod.durationMs)}
            color={t.amber.fg}
          />
        )}
        {detail.start_scn && (
          <StatTile
            label="Start SCN"
            value={<span style={{ fontSize: t.size.md }}>{detail.start_scn}</span>}
            sub={detail.scn_fixed_at ? fmtTs(detail.scn_fixed_at) : undefined}
          />
        )}
        {detail.kafka_lag != null && (
          <StatTile
            label="Kafka lag"
            value={fmtNum(detail.kafka_lag)}
            color={
              detail.kafka_lag === 0
                ? t.green.base
                : detail.kafka_lag < 10000
                ? t.amber.fg
                : t.red.base
            }
          />
        )}
      </div>

      {/* ── Phase timeline ── */}
      <SectionHeader>Продолжительность фаз</SectionHeader>
      <div style={{
        background: t.bg.deep, border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.md, overflow: "hidden", marginBottom: t.space[4],
      }}>
        {/* header */}
        <div style={{
          display: "grid",
          gridTemplateColumns: "160px 1fr 110px 90px",
          gap: `2px ${t.space[3]}`,
          padding: `5px ${t.space[3]}`,
          borderBottom: `1px solid ${t.border.subtle}`,
          fontSize: t.size.xs, color: t.text.disabled, fontWeight: 700,
          textTransform: "uppercase", letterSpacing: 0.6,
        }}>
          <span>Фаза</span>
          <span>Относительно</span>
          <span style={{ textAlign: "right" }}>Начало</span>
          <span style={{ textAlign: "right" }}>Длительность</span>
        </div>

        {periods.map((p, i) => {
          const barPct = Math.max(1, Math.round((p.durationMs / maxMs) * 100));
          const isCurrent = !p.endedAt;
          const color = phaseBarColor(p.phase);
          return (
            <div key={i} style={{
              display: "grid",
              gridTemplateColumns: "160px 1fr 110px 90px",
              gap: `2px ${t.space[3]}`,
              padding: `5px ${t.space[3]}`,
              borderBottom: i < periods.length - 1 ? `1px solid ${t.bg.app}` : "none",
              alignItems: "center",
              background: isCurrent ? t.bg.s2 : "transparent",
            }}>
              {/* phase name */}
              <span style={{
                fontSize: t.size.sm,
                color: isCurrent ? t.text.primary : t.text.secondary,
                fontWeight: isCurrent ? 700 : 400,
                overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
              }}>
                {isCurrent && (
                  <span style={{
                    display: "inline-block", width: 6, height: 6, borderRadius: "50%",
                    background: t.green.base, marginRight: 5,
                    boxShadow: "0 0 4px var(--green)",
                  }} />
                )}
                {p.phase}
              </span>

              {/* bar */}
              <div style={{
                height: 6, background: t.bg.s2,
                borderRadius: t.radius.sm, overflow: "hidden",
              }}>
                <div style={{
                  width: `${barPct}%`, height: "100%",
                  background: color,
                  opacity: isCurrent ? 1 : 0.6,
                  borderRadius: t.radius.sm,
                  transition: "width 1s",
                }} />
              </div>

              {/* start time */}
              <span style={{
                fontSize: t.size.xs, color: t.text.disabled,
                textAlign: "right", whiteSpace: "nowrap",
              }}>
                {p.startedAt.toLocaleTimeString("ru-RU")}
              </span>

              {/* duration */}
              <span style={{
                fontSize: t.size.sm,
                color: isCurrent ? t.amber.fg : t.text.muted,
                textAlign: "right", whiteSpace: "nowrap",
                fontVariantNumeric: "tabular-nums",
              }}>
                {fmtDuration(p.durationMs)}
                {isCurrent && (
                  <span style={{ color: t.green.base, marginLeft: 3 }}>▶</span>
                )}
              </span>
            </div>
          );
        })}

        {periods.length === 0 && (
          <div style={{
            padding: `${t.space[3]} ${t.space[4]}`,
            color: t.text.faint, fontSize: t.size.base,
          }}>
            Нет данных о переходах
          </div>
        )}
      </div>
    </div>
  );
}
