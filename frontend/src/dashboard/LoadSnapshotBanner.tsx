import React, { useEffect, useMemo, useState } from "react";
import { t } from "../theme";
import { Icon, ProgressBar } from "../components/ui";
import { useApi } from "../hooks/useApi";
import { fmtTs } from "../utils/format";
import type { SSEEvent, DdlSnapshotProgressEvent, DdlSnapshotCompleteEvent } from "../hooks/useSSE";

interface LatestSnapshot {
  snapshot_id:    number;
  loaded_at:      string;
  object_counts:  Record<string, number>;
  match_summary:  Record<string, number>;
  total:          number;
}

interface Props {
  srcSchema:    string;
  tgtSchema:    string;
  sseEvents:    SSEEvent[];
  onLoaded:     () => void;
}

const PHASE_LABEL: Record<DdlSnapshotProgressEvent["phase"], string> = {
  listing:   "Сканируем схему",
  source:    "Источник",
  target:    "Таргет",
  comparing: "Сравниваем",
};

/** Banner above the object table:
 *  - if no snapshot yet → CTA «Загрузить snapshot»
 *  - if snapshot exists → «Загружен <date> · N объектов · M отличий» + «Обновить»
 *  - during load → progress bar with current object
 */
export function LoadSnapshotBanner({ srcSchema, tgtSchema, sseEvents, onLoaded }: Props) {
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState<string | null>(null);

  const url = srcSchema && tgtSchema
    ? `/api/catalog/snapshots/latest?src_schema=${srcSchema}&tgt_schema=${tgtSchema}`
    : null;
  const snapshot = useApi<LatestSnapshot | null>(url);

  // Filter SSE events: latest matching progress or complete event for THIS pair
  const progressEvent = useMemo<DdlSnapshotProgressEvent | null>(() => {
    if (!loading) return null;
    for (const e of sseEvents) {
      if (e.type === "ddl_snapshot.progress"
          && e.src_schema === srcSchema && e.tgt_schema === tgtSchema) {
        return e as DdlSnapshotProgressEvent;
      }
    }
    return null;
  }, [sseEvents, loading, srcSchema, tgtSchema]);

  // When complete event arrives, mark done + refresh snapshot info + notify parent
  useEffect(() => {
    if (!loading) return;
    for (const e of sseEvents) {
      if (e.type === "ddl_snapshot.complete"
          && e.src_schema === srcSchema && e.tgt_schema === tgtSchema) {
        setLoading(false);
        snapshot.reload();
        onLoaded();
        break;
      }
      if (e.type === "ddl_snapshot.progress" && (e as DdlSnapshotProgressEvent).src_schema === srcSchema) {
        break; // stop scanning past most recent
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sseEvents, loading, srcSchema, tgtSchema]);

  const startLoad = async () => {
    if (!srcSchema || !tgtSchema) {
      setError("src_schema / tgt_schema не заданы у миграции");
      return;
    }
    setLoading(true); setError(null);
    try {
      const r = await fetch("/api/catalog/load", {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ src_schema: srcSchema, tgt_schema: tgtSchema }),
      });
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        throw new Error(body.error || `HTTP ${r.status}`);
      }
      // HTTP returned — complete event may already have fired (or fire shortly)
      setLoading(false);
      snapshot.reload();
      onLoaded();
    } catch (e: unknown) {
      setError((e as Error).message);
      setLoading(false);
    }
  };

  // ── Rendering states ─────────────────────────────────────────────────────
  if (loading) {
    return (
      <Card>
        <IconBubble color="info"><Icon name="rotate" size={18}/></IconBubble>
        <div style={{ flex: 1, minWidth: 0 }}>
          <Title>Загружаем snapshot…</Title>
          <Sub>
            {progressEvent ? (
              <>
                {PHASE_LABEL[progressEvent.phase]} —{" "}
                <span style={{ fontFamily: t.font.mono }}>
                  {progressEvent.done}/{progressEvent.total || "?"}
                </span>
                {progressEvent.current && (
                  <span style={{ color: t.text.faint, marginLeft: 6 }}>
                    {progressEvent.current}
                  </span>
                )}
              </>
            ) : (
              <>Подключаемся к Oracle и перечисляем объекты…</>
            )}
          </Sub>
          {progressEvent && progressEvent.total > 0 && (
            <div style={{ marginTop: 8 }}>
              <ProgressBar
                value={(progressEvent.done / progressEvent.total) * 100}
                tone="info"
                height={4}
              />
            </div>
          )}
        </div>
      </Card>
    );
  }

  if (error) {
    return (
      <Card>
        <IconBubble color="error"><Icon name="error" size={18}/></IconBubble>
        <div style={{ flex: 1 }}>
          <Title>Ошибка загрузки snapshot</Title>
          <Sub><span style={{ color: t.tone.error }}>{error}</span></Sub>
        </div>
        <BannerBtn primary onClick={startLoad}>Повторить</BannerBtn>
      </Card>
    );
  }

  const hasSnapshot = snapshot.data && snapshot.data.total > 0;

  if (hasSnapshot) {
    const s = snapshot.data!;
    const diff = s.match_summary["DIFF"] || 0;
    const missing = s.match_summary["MISSING"] || 0;
    return (
      <Card>
        <IconBubble color="ok"><Icon name="check" size={18}/></IconBubble>
        <div style={{ flex: 1, minWidth: 0 }}>
          <Title>Snapshot актуален</Title>
          <Sub>
            Загружен <Mono>{fmtTs(s.loaded_at, { withSeconds: true })}</Mono>
            <span style={{ color: t.text.faint }}> · </span>
            <Mono>{s.total}</Mono> объектов
            {(diff > 0 || missing > 0) && (
              <>
                <span style={{ color: t.text.faint }}> · </span>
                {diff > 0    && <span style={{ color: t.tone.warn }}>{diff} с расхождениями</span>}
                {diff > 0 && missing > 0 && <span style={{ color: t.text.faint }}>, </span>}
                {missing > 0 && <span style={{ color: t.tone.info }}>{missing} нет в target</span>}
              </>
            )}
          </Sub>
        </div>
        <BannerBtn onClick={startLoad}>
          <Icon name="rotate" size={14}/>
          Обновить
        </BannerBtn>
      </Card>
    );
  }

  // No snapshot
  return (
    <Card>
      <IconBubble color="info"><Icon name="db" size={18}/></IconBubble>
      <div style={{ flex: 1 }}>
        <Title>Snapshot не загружен</Title>
        <Sub>
          Нет DDL snapshot для пары{" "}
          <Mono>{srcSchema || "?"}</Mono>{" → "}<Mono>{tgtSchema || "?"}</Mono>.
          Загрузите чтобы увидеть таблицы, views, PL/SQL.
        </Sub>
      </div>
      <BannerBtn primary onClick={startLoad} disabled={!srcSchema || !tgtSchema}>
        <Icon name="db" size={14}/>
        Загрузить snapshot
      </BannerBtn>
    </Card>
  );
}

// ── Sub-components ────────────────────────────────────────────────────────

function Card({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      background:   t.bg.s1,
      border:       `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.lg,
      padding:      "14px 18px",
      marginBottom: 12,
      display:      "flex",
      gap:          14,
      alignItems:   "center",
      boxShadow:    t.shadow.s1,
    }}>{children}</div>
  );
}

function IconBubble({ color, children }: { color: "info" | "ok" | "warn" | "error"; children: React.ReactNode }) {
  const bg =
    color === "ok"    ? t.tone.okSoft :
    color === "warn"  ? t.tone.warnSoft :
    color === "error" ? t.tone.errorSoft :
                        t.tone.accentSoft;
  const fg =
    color === "ok"    ? t.tone.ok :
    color === "warn"  ? t.tone.warn :
    color === "error" ? t.tone.error :
                        t.tone.accent;
  return (
    <div style={{
      width: 36, height: 36, borderRadius: 8,
      background: bg, color: fg,
      display: "grid", placeItems: "center", flexShrink: 0,
    }}>{children}</div>
  );
}

function Title({ children }: { children: React.ReactNode }) {
  return <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 2 }}>{children}</div>;
}

function Sub({ children }: { children: React.ReactNode }) {
  return <div style={{ fontSize: 12, color: t.text.muted, lineHeight: 1.4 }}>{children}</div>;
}

function Mono({ children }: { children: React.ReactNode }) {
  return <span style={{ fontFamily: t.font.mono, color: t.text.secondary }}>{children}</span>;
}

function BannerBtn({ children, onClick, primary, disabled }: {
  children:  React.ReactNode;
  onClick:   () => void;
  primary?:  boolean;
  disabled?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        display: "inline-flex", alignItems: "center", gap: 6,
        padding: "7px 14px",
        background: primary ? t.text.primary : t.bg.s1,
        color:      primary ? t.text.inverse : t.text.secondary,
        border:     `1px solid ${primary ? t.text.primary : t.border.subtle}`,
        borderRadius: t.radius.sm,
        fontSize: 12, fontWeight: 500,
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
        flexShrink: 0,
      }}
    >
      {children}
    </button>
  );
}
