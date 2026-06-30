import React, { useCallback, useEffect, useRef, useState } from "react";
import type { MigrationSummary } from "../../types/migration";
import type { SSEEvent } from "../../hooks/useSSE";
import { MigrationDetailPanel } from "../MigrationDetail";
import { EmptyState } from "../ui";

import { ACTIVE_PHASES } from "../MigrationDetail/helpers";
import { t } from "../../theme";
import type { FilterKey, SpeedSnapshot } from "./helpers";
import { FILTER_LABELS, DONE_PHASES } from "./helpers";
import { MigrationRow } from "./MigrationRow";

interface Props {
  refreshSignal?: number;
  sseEvents?:     SSEEvent[];
  onOpenMigration?: () => void;
}

export function MigrationList({ refreshSignal, sseEvents, onOpenMigration }: Props) {
  const [migrations, setMigrations] = useState<MigrationSummary[]>([]);
  const [loading,    setLoading]    = useState(true);
  const [error,      setError]      = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [actionBusy, setActionBusy] = useState<string | null>(null);
  const [speeds,     setSpeeds]     = useState<Record<string, { chunksSec: number; rowsSec: number }>>({});
  const [filter,     setFilter]     = useState<FilterKey>("all");
  const [search,     setSearch]     = useState("");
  const [bulkIds,    setBulkIds]    = useState<Set<string>>(() => new Set());
  const [bulkBusy,   setBulkBusy]   = useState(false);
  const snapRef = useRef<Record<string, SpeedSnapshot>>({});

  const load = useCallback(() => {
    fetch("/api/migrations")
      .then(r => r.ok ? r.json() : Promise.reject(`HTTP ${r.status}`))
      .then((data: MigrationSummary[]) => {
        const now = Date.now();
        const newSpeeds: Record<string, { chunksSec: number; rowsSec: number }> = {};
        data.forEach(m => {
          const prev = snapRef.current[m.migration_id];
          if (prev && (m.chunks_done > prev.chunks_done || m.rows_loaded > prev.rows_loaded)) {
            const dt = (now - prev.ts) / 1000;
            const chunksSec = (m.chunks_done - prev.chunks_done) / dt;
            const rowsSec   = (m.rows_loaded - prev.rows_loaded) / dt;
            newSpeeds[m.migration_id] = { chunksSec, rowsSec };
          } else if (prev) {
            newSpeeds[m.migration_id] = speeds[m.migration_id] ?? { chunksSec: 0, rowsSec: 0 };
          }
          snapRef.current[m.migration_id] = {
            chunks_done: m.chunks_done, rows_loaded: m.rows_loaded, ts: now,
          };
        });
        setSpeeds(newSpeeds);
        setMigrations(data);
        setLoading(false);
        setError(null);
      })
      .catch(e => { setError(String(e)); setLoading(false); });
  }, []); // eslint-disable-line

  useEffect(() => { load(); }, [load, refreshSignal]);

  // Refresh on migration_phase SSE events
  useEffect(() => {
    if (!sseEvents || sseEvents.length === 0) return;
    if (sseEvents[0].type === "migration_phase") load();
  }, [sseEvents]); // eslint-disable-line

  // Auto-refresh every 10s
  useEffect(() => {
    const id = setInterval(load, 10_000);
    return () => clearInterval(id);
  }, [load]);

  const visible = migrations.filter(m => {
    if (filter === "active") return ACTIVE_PHASES.has(m.phase);
    if (filter === "done")   return DONE_PHASES.has(m.phase);
    if (filter === "error")  return m.phase === "FAILED" || !!m.error_code;
    if (filter === "draft")  return m.phase === "DRAFT";
    return true;
  }).filter(m => {
    if (!search.trim()) return true;
    const q = search.toLowerCase();
    return (
      m.migration_name.toLowerCase().includes(q) ||
      `${m.source_schema}.${m.source_table}`.toLowerCase().includes(q) ||
      `${m.target_schema}.${m.target_table}`.toLowerCase().includes(q)
    );
  });

  const selected = migrations.find(m => m.migration_id === selectedId) ?? null;

  const handleAction = useCallback(async (id: string, action: "run" | "stop" | "delete") => {
    if (action === "delete") {
      if (!window.confirm("Удалить миграцию? Это действие необратимо.")) return;
    }
    setActionBusy(id);
    try {
      if (action === "run") {
        await fetch(`/api/migrations/${id}/action`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "run" }),
        });
      } else if (action === "stop") {
        await fetch(`/api/migrations/${id}/action`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ action: "cancel" }),
        });
      } else {
        await fetch(`/api/migrations/${id}`, { method: "DELETE" });
        if (selectedId === id) setSelectedId(null);
      }
      load();
    } finally {
      setActionBusy(null);
    }
  }, [load, selectedId]);

  const toggleBulk = useCallback((id: string) => {
    setBulkIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const clearBulk = useCallback(() => setBulkIds(new Set()), []);

  // Параллельный fetch с лимитом — чтобы при сотнях миграций браузер
  // не отстреливал «too many requests».
  async function _runBulk(ids: string[], call: (id: string) => Promise<Response>) {
    setBulkBusy(true);
    const failed: string[] = [];
    try {
      const CONCURRENCY = 6;
      const queue = [...ids];
      const workers = Array.from({ length: Math.min(CONCURRENCY, queue.length) }, async () => {
        while (queue.length > 0) {
          const id = queue.shift()!;
          try {
            const r = await call(id);
            if (!r.ok) failed.push(id);
          } catch {
            failed.push(id);
          }
        }
      });
      await Promise.all(workers);
    } finally {
      setBulkBusy(false);
      clearBulk();
      load();
    }
    if (failed.length > 0) {
      alert(`Не удалось выполнить для ${failed.length} из ${ids.length} миграций`);
    }
  }

  const bulkStop = useCallback(async () => {
    const ids = Array.from(bulkIds);
    if (ids.length === 0) return;
    if (!confirm(`Остановить ${ids.length} миграций?`)) return;
    await _runBulk(ids, id => fetch(`/api/migrations/${id}/action`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ action: "cancel" }),
    }));
  }, [bulkIds, clearBulk, load]);

  const bulkDelete = useCallback(async () => {
    const ids = Array.from(bulkIds);
    if (ids.length === 0) return;
    if (!confirm(`Удалить ${ids.length} миграций? Это действие необратимо.`)) return;
    await _runBulk(ids, id => fetch(`/api/migrations/${id}`, { method: "DELETE" }));
    if (selectedId && bulkIds.has(selectedId)) setSelectedId(null);
  }, [bulkIds, clearBulk, load, selectedId]);

  // Чистим выбор от удалённых/исчезнувших id
  useEffect(() => {
    setBulkIds(prev => {
      const valid = new Set(migrations.map(m => m.migration_id));
      let changed = false;
      const next = new Set<string>();
      prev.forEach(id => {
        if (valid.has(id)) next.add(id);
        else changed = true;
      });
      return changed ? next : prev;
    });
  }, [migrations]);

  return (
    <>
      <div style={{ display: "flex", gap: 12, height: "calc(100vh - 180px)", minHeight: 400 }}>
        {/* List panel */}
        <div style={{
          width: selected ? "38%" : "100%",
          minWidth: 0,
          display: "flex", flexDirection: "column",
          background: t.bg.s1, border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.lg, overflow: "hidden",
          transition: "width 0.2s",
        }}>
          {/* Toolbar */}
          <div style={{
            padding: "8px 14px", borderBottom: `1px solid ${t.border.subtle}`,
            display: "flex", flexDirection: "column", gap: 6,
          }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              {visible.length > 0 && (
                <input
                  type="checkbox"
                  checked={bulkIds.size > 0 && visible.every(m => bulkIds.has(m.migration_id))}
                  ref={el => { if (el) el.indeterminate = bulkIds.size > 0
                    && !visible.every(m => bulkIds.has(m.migration_id)); }}
                  onChange={e => {
                    if (e.target.checked) setBulkIds(new Set(visible.map(m => m.migration_id)));
                    else                  clearBulk();
                  }}
                  style={{ cursor: "pointer" }}
                  title="Выбрать все видимые"
                />
              )}
              <span style={{ fontWeight: 700, fontSize: t.size.md, color: t.text.secondary }}>
                Миграции
              </span>
              {migrations.length > 0 && (
                <span style={{
                  background: t.bg.s2, color: t.text.muted,
                  borderRadius: 10, fontSize: t.size.sm,
                  padding: "1px 7px", fontWeight: 600,
                }}>{migrations.length}</span>
              )}
              <button
                onClick={onOpenMigration}
                disabled={!onOpenMigration}
                style={{
                  marginLeft: "auto", background: t.blue.dim, border: "none",
                  borderRadius: t.radius.md, color: t.text.inverse,
                  fontSize: t.size.sm, padding: "4px 10px",
                  cursor: onOpenMigration ? "pointer" : "not-allowed", fontWeight: 700,
                  opacity: onOpenMigration ? 1 : 0.55,
                }}
                title="Новые таблицы добавляются через пачки на экране Эта миграция"
              >
                + Через пачки
              </button>
              <button
                onClick={load}
                style={{
                  background: "none", border: `1px solid ${t.border.subtle}`,
                  borderRadius: t.radius.md, color: t.text.disabled,
                  fontSize: t.size.sm, padding: "3px 8px", cursor: "pointer",
                }}
              >
                ↺
              </button>
            </div>
            {/* Filter pills + search */}
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              {FILTER_LABELS.map(({ key, label }) => {
                const count = key === "all" ? migrations.length
                  : key === "active" ? migrations.filter(m => ACTIVE_PHASES.has(m.phase)).length
                  : key === "done"   ? migrations.filter(m => DONE_PHASES.has(m.phase)).length
                  : key === "error"  ? migrations.filter(m => m.phase === "FAILED" || !!m.error_code).length
                  : migrations.filter(m => m.phase === "DRAFT").length;
                if (count === 0 && key !== "all") return null;
                const active = filter === key;
                return (
                  <button key={key} onClick={() => setFilter(key)} style={{
                    background: active ? t.bg.s3 : "none",
                    border: `1px solid ${active ? t.blue.base : t.border.subtle}`,
                    borderRadius: 12, color: active ? t.blue.fg : t.text.disabled,
                    fontSize: t.size.xs, padding: "2px 8px",
                    cursor: "pointer", fontWeight: 600, whiteSpace: "nowrap",
                  }}>
                    {label} {count > 0 && <span style={{ opacity: 0.7 }}>{count}</span>}
                  </button>
                );
              })}
              <input
                value={search}
                onChange={e => setSearch(e.target.value)}
                placeholder="Поиск…"
                style={{
                  marginLeft: "auto",
                  background: t.bg.s2, border: `1px solid ${t.border.base}`,
                  borderRadius: t.radius.md, color: t.text.primary,
                  padding: "3px 8px", fontSize: t.size.sm, width: 120, outline: "none",
                }}
              />
            </div>
          </div>

          {/* Bulk action bar — виден только когда что-то выбрано */}
          {bulkIds.size > 0 && (
            <div style={{
              padding: "6px 14px",
              background: t.bg.s2,
              borderBottom: `1px solid ${t.border.subtle}`,
              display: "flex", alignItems: "center", gap: 8,
              fontSize: t.size.sm,
            }}>
              <span style={{ color: t.text.muted }}>
                Выбрано: <b style={{ color: t.text.primary }}>{bulkIds.size}</b>
              </span>
              <button onClick={clearBulk} disabled={bulkBusy} style={{
                background: "none", border: `1px solid ${t.border.subtle}`,
                borderRadius: t.radius.sm, color: t.text.disabled,
                fontSize: t.size.xs, padding: "2px 8px", cursor: "pointer",
              }}>Сбросить</button>
              <span style={{ flex: 1 }}/>
              <button onClick={bulkStop} disabled={bulkBusy} style={{
                background: t.red.bg, border: `1px solid ${t.red.dim}`,
                borderRadius: t.radius.sm, color: t.red.fg,
                fontSize: t.size.xs, padding: "3px 10px", cursor: "pointer", fontWeight: 700,
              }}>⏹ Остановить</button>
              <button onClick={bulkDelete} disabled={bulkBusy} style={{
                background: t.bg.s3, border: `1px solid ${t.border.base}`,
                borderRadius: t.radius.sm, color: t.text.secondary,
                fontSize: t.size.xs, padding: "3px 10px", cursor: "pointer", fontWeight: 700,
              }}>✕ Удалить</button>
              {bulkBusy && (
                <span style={{ fontSize: t.size.xs, color: t.text.disabled }}>…</span>
              )}
            </div>
          )}

          {/* Content */}
          <div style={{ flex: 1, overflow: "auto" }}>
            {loading && (
              <div style={{ padding: 24, color: t.text.disabled, fontSize: t.size.md }}>
                Загрузка...
              </div>
            )}
            {error && (
              <div style={{ padding: 16, color: t.red.fg, fontSize: t.size.base }}>
                Ошибка загрузки: {error}
              </div>
            )}
            {!loading && !error && visible.length === 0 && (
              <EmptyState
                title="Нет миграций"
                description="Миграции будут отображаться здесь после добавления"
              />
            )}
            {!loading && !error && visible.map(m => (
              <MigrationRow
                key={m.migration_id}
                m={m}
                selected={m.migration_id === selectedId}
                compact={!!selected}
                busy={actionBusy === m.migration_id || bulkBusy}
                speed={speeds[m.migration_id]}
                onClick={() => setSelectedId(id => id === m.migration_id ? null : m.migration_id)}
                onAction={handleAction}
                checked={bulkIds.has(m.migration_id)}
                onToggleCheck={toggleBulk}
              />
            ))}
          </div>
        </div>

        {/* Detail panel */}
        {selected && (
          <div style={{ flex: 1, minWidth: 0, overflow: "hidden" }}>
            <MigrationDetailPanel
              migrationId={selected.migration_id}
              onClose={() => setSelectedId(null)}
              sseEvents={sseEvents}
            />
          </div>
        )}
      </div>
    </>
  );
}
