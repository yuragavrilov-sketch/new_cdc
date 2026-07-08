import React, { useEffect, useState } from "react";
import { Button } from "../ui";
import { t } from "../../theme";
import { ACTIVE_PHASES, DELETABLE_PHASES } from "./helpers";
import { migrationRowActions } from "../MigrationList/helpers";

// ── WorkerCountEditor ────────────────────────────────────────────────────────

export function WorkerCountEditor({
  migrationId, field, value, onSaved, minValue = 1,
}: {
  migrationId: string;
  field: "max_parallel_workers" | "baseline_parallel_degree" | "baseline_batch_size";
  value: number;
  onSaved: () => void;
  minValue?: number;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft]     = useState(value);
  const [saving, setSaving]   = useState(false);

  useEffect(() => { setDraft(value); }, [value]);

  async function save() {
    const v = Math.max(minValue, draft);
    if (v === value) { setEditing(false); return; }
    setSaving(true);
    try {
      const res = await fetch(`/api/migrations/${migrationId}/workers`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ [field]: v }),
      });
      if (res.ok) { onSaved(); setEditing(false); }
    } finally { setSaving(false); }
  }

  if (!editing) {
    return (
      <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
        <span style={{ color: t.text.primary }}>{value}</span>
        <button
          onClick={() => { setDraft(value); setEditing(true); }}
          style={{
            background: "none", border: `1px solid ${t.border.base}`, borderRadius: 4,
            color: t.text.secondary, fontSize: 10, padding: "1px 6px", cursor: "pointer",
          }}
        >
          изменить
        </button>
      </span>
    );
  }

  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
      <input
        type="number" min={minValue} value={draft}
        onChange={e => setDraft(parseInt(e.target.value) || minValue)}
        onKeyDown={e => { if (e.key === "Enter") save(); if (e.key === "Escape") setEditing(false); }}
        autoFocus
        style={{
          width: minValue >= 1000 ? 100 : 60,
          background: t.bg.app, border: `1px solid ${t.blue.base}`,
          borderRadius: 4, color: t.text.primary, fontSize: 12, padding: "2px 6px",
          textAlign: "center",
        }}
      />
      <button
        onClick={save} disabled={saving}
        style={{
          background: t.bg.s3, border: `1px solid ${t.blue.dim}`, borderRadius: 4,
          color: t.blue.fg, fontSize: 10, padding: "2px 8px", cursor: saving ? "not-allowed" : "pointer",
        }}
      >
        {saving ? "…" : "OK"}
      </button>
      <button
        onClick={() => setEditing(false)}
        style={{
          background: "none", border: `1px solid ${t.border.base}`, borderRadius: 4,
          color: t.text.muted, fontSize: 10, padding: "2px 6px", cursor: "pointer",
        }}
      >
        ✕
      </button>
    </span>
  );
}

// ── EnableIndexesButton ──────────────────────────────────────────────────────

export function EnableIndexesButton({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy,  setBusy]  = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  async function handleClick() {
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/enable-indexes`, { method: "POST" });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      } else {
        onDone();
      }
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", gap: 4 }}>
      <button
        onClick={handleClick}
        disabled={busy}
        style={{
          background: busy ? t.bg.s3 : t.blue.dim,
          color: busy ? t.text.muted : t.text.primary,
          border: `1px solid ${t.blue.dim}`,
          borderRadius: 5,
          padding: "5px 14px",
          fontSize: 12,
          fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer",
        }}
      >
        {busy ? "Запуск..." : "Включить индексы"}
      </button>
      {errMsg && (
        <span style={{ fontSize: 11, color: t.red.fg }}>{errMsg}</span>
      )}
    </div>
  );
}

// ── EnableTriggersButton ─────────────────────────────────────────────────────

export function EnableTriggersButton({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  type TriggerJob = {
    job_id: string;
    state: "PENDING" | "RUNNING" | "DONE" | "FAILED";
    enabled_count: number;
    error_text: string | null;
  };

  const [busy, setBusy] = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);
  const [jobs, setJobs] = useState<TriggerJob[]>([]);
  const latest = jobs[0];

  async function loadJobs() {
    const r = await fetch(`/api/migrations/${migrationId}/trigger-jobs`);
    if (r.ok) setJobs(await r.json());
  }

  useEffect(() => {
    let alive = true;
    async function tick() {
      const r = await fetch(`/api/migrations/${migrationId}/trigger-jobs`);
      if (alive && r.ok) setJobs(await r.json());
    }
    tick();
    const iv = setInterval(tick, 3000);
    return () => { alive = false; clearInterval(iv); };
  }, [migrationId]);

  async function createJob() {
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/trigger-jobs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ requested_by: "ui" }),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      }
      await loadJobs();
      onDone();
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function runJob(jobId: string) {
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/trigger-jobs/${jobId}/run`, { method: "POST" });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      }
      await loadJobs();
      onDone();
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  if (latest?.state === "DONE") {
    return (
      <span style={{ fontSize: 11, color: t.green.base, fontWeight: 600 }}>
        Триггеры включены: {latest.enabled_count}
      </span>
    );
  }

  const label =
    !latest || latest.state === "FAILED"
      ? "Создать job триггеров"
      : latest.state === "RUNNING"
        ? "Job триггеров выполняется..."
        : "Запустить job триггеров";

  const handleClick =
    !latest || latest.state === "FAILED"
      ? createJob
      : latest.state === "PENDING"
        ? () => runJob(latest.job_id)
        : undefined;

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", gap: 4 }}>
      <button
        onClick={handleClick}
        disabled={busy || latest?.state === "RUNNING"}
        style={{
          background: busy ? t.bg.s2 : t.green.base,
          color: busy ? t.text.muted : t.text.primary,
          border: `1px solid ${t.green.border}`,
          borderRadius: 5,
          padding: "5px 14px",
          fontSize: 12,
          fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer",
        }}
      >
        {busy ? "..." : label}
      </button>
      {latest?.state === "FAILED" && latest.error_text && (
        <span style={{ fontSize: 11, color: t.red.fg }}>{latest.error_text}</span>
      )}
      {errMsg && (
        <span style={{ fontSize: 11, color: t.red.fg }}>{errMsg}</span>
      )}
    </div>
  );
}

// ── RestartBaselineButton ────────────────────────────────────────────────────

export function RestartBaselineButton({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy, setBusy]     = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  async function handleClick() {
    if (!confirm("Перезапустить baseline? Целевая таблица будет очищена (TRUNCATE) и загрузка начнётся заново.")) return;
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/restart-baseline`, { method: "POST" });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      } else {
        onDone();
      }
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-start", gap: 4 }}>
      <button
        onClick={handleClick}
        disabled={busy}
        style={{
          background: busy ? t.amber.bg : t.amber.dim,
          color: busy ? t.text.muted : t.amber.bg,
          border: `1px solid ${t.amber.dim}`,
          borderRadius: 5,
          padding: "5px 14px",
          fontSize: 12,
          fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer",
        }}
      >
        {busy ? "Перезапуск..." : "Перезапустить baseline"}
      </button>
      {errMsg && (
        <span style={{ fontSize: 11, color: t.red.fg }}>{errMsg}</span>
      )}
    </div>
  );
}

// ── DataMismatchButtons ──────────────────────────────────────────────────────

export function DataMismatchButtons({ migrationId, onDone }: { migrationId: string; onDone: () => void }) {
  const [busy, setBusy] = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  async function doAction(action: string, confirmMsg?: string) {
    if (confirmMsg && !confirm(confirmMsg)) return;
    setBusy(true);
    setErrMsg(null);
    try {
      const r = await fetch(`/api/migrations/${migrationId}/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action }),
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        setErrMsg(d.error ?? `Ошибка ${r.status}`);
      } else {
        onDone();
      }
    } catch (e) {
      setErrMsg(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <>
      <button
        onClick={() => doAction("retry_verify")}
        disabled={busy}
        style={{
          background: t.bg.s3, border: `1px solid ${t.blue.dim}`, borderRadius: 5,
          color: t.blue.fg, padding: "4px 12px", fontSize: 11, fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer", opacity: busy ? 0.5 : 1,
        }}
      >
        Повторить сверку
      </button>
      <button
        onClick={() => doAction("force_complete", "Завершить миграцию без успешной сверки данных?")}
        disabled={busy}
        style={{
          background: t.red.bg, border: `1px solid ${t.amber.dim}`, borderRadius: 5,
          color: t.amber.fg, padding: "4px 12px", fontSize: 11, fontWeight: 600,
          cursor: busy ? "not-allowed" : "pointer", opacity: busy ? 0.5 : 1,
        }}
      >
        Завершить принудительно
      </button>
      {errMsg && (
        <span style={{ fontSize: 10, color: t.red.fg }}>{errMsg}</span>
      )}
    </>
  );
}

// ── DataVerifyCard ───────────────────────────────────────────────────────────

export function DataVerifyCard({ taskId, phase }: { taskId: string; phase: string }) {
  const [info, setInfo] = useState<any>(null);

  useEffect(() => {
    let alive = true;
    function load() {
      fetch(`/api/data-compare/${taskId}`)
        .then(r => r.ok ? r.json() : null)
        .then(d => { if (alive && d) setInfo(d); });
    }
    load();
    const iv = setInterval(load, 3000);
    return () => { alive = false; clearInterval(iv); };
  }, [taskId]);

  if (!info) return null;

  const progress = info.chunks_total > 0
    ? Math.round((info.chunks_done / info.chunks_total) * 100)
    : 0;

  return (
    <div style={{
      background: t.bg.s1, border: `1px solid ${t.border.subtle}`, borderRadius: 6,
      padding: 12, marginBottom: 14,
    }}>
      <div style={{ fontSize: 12, fontWeight: 600, color: t.text.primary, marginBottom: 8 }}>
        Сверка данных
        {phase === "DATA_VERIFYING" && (
          <span style={{ color: t.blue.fg, fontWeight: 400, marginLeft: 8 }}>
            {info.status === "RUNNING" ? `${progress}% (${info.chunks_done}/${info.chunks_total})` : info.status}
          </span>
        )}
      </div>
      {(info.source_count != null || info.target_count != null) && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, fontSize: 11 }}>
          <div>
            <div style={{ color: t.text.muted }}>Source count</div>
            <div style={{ color: t.text.primary, fontWeight: 600 }}>
              {info.source_count?.toLocaleString("ru-RU") ?? "—"}
            </div>
          </div>
          <div>
            <div style={{ color: t.text.muted }}>Target count</div>
            <div style={{ color: t.text.primary, fontWeight: 600 }}>
              {info.target_count?.toLocaleString("ru-RU") ?? "—"}
            </div>
          </div>
          <div>
            <div style={{ color: t.text.muted }}>Результат</div>
            <div style={{ fontWeight: 600 }}>
              {info.counts_match === null
                ? <span style={{ color: t.text.disabled }}>Ожидание</span>
                : info.counts_match && info.hash_match
                  ? <span style={{ color: t.green.fg }}>OK</span>
                  : <span style={{ color: t.red.fg }}>
                      {!info.counts_match ? "COUNT mismatch" : "HASH mismatch"}
                    </span>
              }
            </div>
          </div>
        </div>
      )}
      {info.error_text && (
        <div style={{ fontSize: 10, color: t.red.fg, marginTop: 6 }}>{info.error_text}</div>
      )}
    </div>
  );
}

// ── StopDeleteButtons ────────────────────────────────────────────────────────

export function StopDeleteButtons({ migrationId, phase, paused, onDone, onDeleted }: {
  migrationId: string; phase: string; paused?: boolean; onDone: () => void; onDeleted: () => void;
}) {
  const [busy, setBusy] = useState(false);

  const { canPause, canResume } = migrationRowActions(phase, !!paused);
  const canStop   = ACTIVE_PHASES.has(phase);
  const canDelete = DELETABLE_PHASES.has(phase);
  if (!canPause && !canResume && !canStop && !canDelete) return null;

  async function doPause() {
    if (!confirm("Поставить миграцию на паузу? Уже взятая операция завершится, новые браться не будут.")) return;
    setBusy(true);
    try {
      await fetch(`/api/migrations/${migrationId}/action`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "pause" }),
      });
      onDone();
    } finally { setBusy(false); }
  }

  async function doResume() {
    setBusy(true);
    try {
      await fetch(`/api/migrations/${migrationId}/action`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "resume" }),
      });
      onDone();
    } finally { setBusy(false); }
  }

  async function doStop() {
    if (!confirm("Остановить миграцию? Текущая операция завершится, после чего миграция перейдёт в CANCELLED.")) return;
    setBusy(true);
    try {
      await fetch(`/api/migrations/${migrationId}/action`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "cancel" }),
      });
      onDone();
    } finally { setBusy(false); }
  }

  async function doDelete() {
    if (!confirm("Удалить миграцию? Это действие необратимо.")) return;
    setBusy(true);
    try {
      const r = await fetch(`/api/migrations/${migrationId}`, { method: "DELETE" });
      if (r.ok) { onDeleted(); } else { onDone(); }
    } finally { setBusy(false); }
  }

  return (
    <div style={{ display: "flex", gap: 4 }}>
      {canPause && (
        <Button variant="secondary" size="sm" onClick={doPause} disabled={busy}>
          Пауза
        </Button>
      )}
      {canResume && (
        <Button variant="success" size="sm" onClick={doResume} disabled={busy}>
          Продолжить
        </Button>
      )}
      {canStop && (
        <Button variant="danger" size="sm" onClick={doStop} disabled={busy}>
          Остановить
        </Button>
      )}
      {canDelete && (
        <Button variant="danger" size="sm" onClick={doDelete} disabled={busy}>
          Удалить
        </Button>
      )}
    </div>
  );
}
