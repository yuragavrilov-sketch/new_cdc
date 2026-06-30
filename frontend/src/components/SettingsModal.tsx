import React, { useEffect, useState } from "react";
import { t } from "../theme";
import { useApi } from "../hooks/useApi";
import { fmtBytes } from "../utils/format";

// ─── Service metrics types ───────────────────────────────────────────────────

interface OracleMetric {
  ok:            boolean;
  error?:        string;
  host?:         string;
  service_name?: string;
  version?:      string;
  cpu_pct?:      number;
  redo_bps?:     number;
  network_bps?:  number;
  active_sessions?: number | null;
  instance?:     { name: string; host: string; status: string } | null;
  rtt_ms?:       number;
}

interface KafkaMetric {
  ok:         boolean;
  error?:     string;
  bootstrap?: string;
  brokers?:   number;
  topics?:    number;
  cluster_id?: string | null;
  rtt_ms?:    number;
}

interface KafkaConnectMetric {
  ok:         boolean;
  error?:     string;
  url?:       string;
  version?:   string;
  cluster_id?: string;
  connectors?: { total: number; running: number; failed: number; paused: number; unassigned: number };
  rtt_ms?:    number;
}

interface ServicesMetrics {
  oracle_source: OracleMetric;
  oracle_target: OracleMetric;
  kafka:         KafkaMetric;
  kafka_connect: KafkaConnectMetric;
}

// ─── Types ───────────────────────────────────────────────────────────────────

interface OracleConfig {
  host: string;
  port: string;
  service_name: string;
  schema: string;
  user: string;
  password: string;
  owner_user: string;
  owner_password: string;
}

interface KafkaConfig {
  bootstrap_servers: string;
}

interface ConnectConfig {
  url: string;
}

interface AllConfigs {
  oracle_source: OracleConfig;
  oracle_target: OracleConfig;
  kafka: KafkaConfig;
  kafka_connect: ConnectConfig;
}

const ORACLE_DEFAULT: OracleConfig = {
  host: "", port: "1521", service_name: "", schema: "", user: "", password: "",
  owner_user: "", owner_password: "",
};
const KAFKA_DEFAULT: KafkaConfig   = { bootstrap_servers: "" };
const CONNECT_DEFAULT: ConnectConfig = { url: "" };

type TabKey = "oracle_source" | "oracle_target" | "kafka" | "kafka_connect";

const TABS: { key: TabKey; label: string }[] = [
  { key: "oracle_source", label: "Oracle Source" },
  { key: "oracle_target", label: "Oracle Target" },
  { key: "kafka",         label: "Kafka"         },
  { key: "kafka_connect", label: "Connect"       },
];

const CONFIG_TOKEN_KEY = "coordinator.configApiToken";

function configHeaders(token: string, json = false): HeadersInit {
  const headers: Record<string, string> = {};
  if (json) headers["Content-Type"] = "application/json";
  if (token.trim()) headers["X-Config-Token"] = token.trim();
  return headers;
}

// ─── Field ───────────────────────────────────────────────────────────────────

function Field({
  label, value, onChange, type = "text", placeholder, span,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  type?: string;
  placeholder?: string;
  span?: number; // grid column span
}) {
  return (
    <div style={{ marginBottom: 12, gridColumn: span ? `span ${span}` : undefined }}>
      <label style={{
        display: "block", fontSize: 11, color: t.text.muted,
        marginBottom: 4, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.4,
      }}>
        {label}
      </label>
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        autoComplete="off"
        style={{
          width: "100%", background: t.bg.app, border: `1px solid ${t.border.base}`,
          borderRadius: 5, color: t.text.primary, padding: "6px 10px", fontSize: 13,
        }}
      />
    </div>
  );
}

// ─── Main modal ──────────────────────────────────────────────────────────────

interface Props {
  onClose: () => void;
}

export function SettingsModal({ onClose }: Props) {
  const [activeTab, setActiveTab] = useState<TabKey>("oracle_source");
  const [configs, setConfigs] = useState<AllConfigs>({
    oracle_source: { ...ORACLE_DEFAULT },
    oracle_target: { ...ORACLE_DEFAULT },
    kafka:         { ...KAFKA_DEFAULT },
    kafka_connect: { ...CONNECT_DEFAULT },
  });
  const [saving,      setSaving]      = useState(false);
  const [saved,       setSaved]       = useState(false);
  const [error,       setError]       = useState<string | null>(null);
  const [testing,     setTesting]     = useState(false);
  const [testResult,  setTestResult]  = useState<{ status: string; message: string } | null>(null);
  const [configToken, setConfigToken] = useState(() => localStorage.getItem(CONFIG_TOKEN_KEY) || "");

  // Live service-health metrics (polled every 10s)
  const metricsApi = useApi<ServicesMetrics>("/api/services/metrics", { intervalMs: 10000 });

  useEffect(() => {
    fetch("/api/config", { headers: configHeaders(configToken) })
      .then((r) => {
        if (!r.ok) {
          throw new Error(r.status === 401 ? "CONFIG_API_TOKEN required" : `HTTP ${r.status}`);
        }
        return r.json();
      })
      .then((data: Record<string, Record<string, string>>) => {
        setConfigs({
          oracle_source: { ...ORACLE_DEFAULT, ...(data.oracle_source ?? {}) },
          oracle_target: { ...ORACLE_DEFAULT, ...(data.oracle_target ?? {}) },
          kafka:         { ...KAFKA_DEFAULT,  ...(data.kafka         ?? {}) },
          kafka_connect: { ...CONNECT_DEFAULT,...(data.kafka_connect ?? {}) },
        });
        setError(null);
      })
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, [configToken]);

  async function testConnection() {
    setTesting(true);
    setTestResult(null);
    try {
      const r = await fetch(`/api/config/${activeTab}/test`, {
        method: "POST",
        headers: configHeaders(configToken, true),
        body: JSON.stringify(configs[activeTab]),
      });
      const d = await r.json();
      setTestResult(d);
    } catch (e: unknown) {
      setTestResult({ status: "down", message: e instanceof Error ? e.message : String(e) });
    } finally {
      setTesting(false);
    }
  }

  async function saveActive() {
    setSaving(true);
    setError(null);
    setTestResult(null);
    const body = configs[activeTab];
    try {
      const r = await fetch(`/api/config/${activeTab}`, {
        method: "POST",
        headers: configHeaders(configToken, true),
        body: JSON.stringify(body),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      setSaved(true);
      setTimeout(() => setSaved(false), 2500);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  function setOracle(key: "oracle_source" | "oracle_target", field: keyof OracleConfig, v: string) {
    setConfigs((p) => ({ ...p, [key]: { ...p[key], [field]: v } }));
  }

  function updateConfigToken(v: string) {
    setConfigToken(v);
    if (v.trim()) localStorage.setItem(CONFIG_TOKEN_KEY, v);
    else localStorage.removeItem(CONFIG_TOKEN_KEY);
  }

  const tabBarStyle: React.CSSProperties = {
    display: "flex", gap: 2, marginBottom: 20,
    borderBottom: `1px solid ${t.border.base}`, paddingBottom: 0,
  };

  function tabStyle(active: boolean): React.CSSProperties {
    return {
      padding: "7px 14px", fontSize: 12, fontWeight: 600,
      cursor: "pointer", border: "none", borderRadius: "5px 5px 0 0",
      background: active ? t.bg.app : "transparent",
      color: active ? t.text.primary : t.text.muted,
      borderBottom: active ? `2px solid ${t.blue.base}` : "2px solid transparent",
      marginBottom: -1,
    };
  }

  const gridTwo: React.CSSProperties = {
    display: "grid", gridTemplateColumns: "1fr 90px", gap: "0 12px",
  };

  return (
    <>
      {/* Overlay */}
      <div
        onClick={onClose}
        style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.65)", zIndex: 200 }}
      />
      {/* Panel */}
      <div style={{
        position: "fixed", top: "50%", left: "50%",
        transform: "translate(-50%, -50%)",
        background: t.bg.s2, border: `1px solid ${t.border.base}`,
        borderRadius: 10, padding: "24px 28px",
        width: 500, maxWidth: "92vw", maxHeight: "88vh",
        overflowY: "auto", zIndex: 201,
        fontFamily: "'Inter', 'Segoe UI', sans-serif",
      }}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 18 }}>
          <h2 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: t.text.primary }}>
            Connection Settings
          </h2>
          <button onClick={onClose} style={{
            background: "none", border: "none", color: t.text.muted,
            cursor: "pointer", fontSize: 18, lineHeight: 1, padding: 2,
          }}>✕</button>
        </div>

        {/* Tab bar */}
        <div style={tabBarStyle}>
          {TABS.map(({ key, label }) => (
            <button key={key} style={tabStyle(activeTab === key)} onClick={() => {
              setActiveTab(key);
              setTestResult(null);
            }}>
              {label}
            </button>
          ))}
        </div>

        {/* Error */}
        {error && (
          <div style={{
            background: t.red.bg, color: t.red.fg, padding: "8px 12px",
            borderRadius: 6, marginBottom: 14, fontSize: 12,
          }}>
            {error}
          </div>
        )}

        <Field
          label="Config Token"
          type="password"
          value={configToken}
          onChange={updateConfigToken}
          placeholder="optional"
        />

        {/* Live metrics for the current tab */}
        <ServiceMetricsPanel tab={activeTab} data={metricsApi.data}/>

        {/* Tab content */}
        {(activeTab === "oracle_source" || activeTab === "oracle_target") && (
          <div>
            <div style={gridTwo}>
              <Field label="Host" value={configs[activeTab].host}
                onChange={(v) => setOracle(activeTab, "host", v)} placeholder="db-host.example.com" />
              <Field label="Port" value={configs[activeTab].port}
                onChange={(v) => setOracle(activeTab, "port", v)} placeholder="1521" />
            </div>
            <div style={gridTwo}>
              <Field label="Service Name" value={configs[activeTab].service_name}
                onChange={(v) => setOracle(activeTab, "service_name", v)} placeholder="ORCL" />
              <Field label="Schema" value={configs[activeTab].schema}
                onChange={(v) => setOracle(activeTab, "schema", v)} placeholder="SCOTT" />
            </div>
            <div style={{ fontSize: 11, color: t.text.disabled, fontWeight: 600, marginBottom: 4, marginTop: 4, textTransform: "uppercase", letterSpacing: 0.4 }}>
              {activeTab === "oracle_source" ? "Debezium User (CDC)" : "User"}
            </div>
            <Field label="User" value={configs[activeTab].user}
              onChange={(v) => setOracle(activeTab, "user", v)} placeholder={activeTab === "oracle_source" ? "debezium" : "scott"} />
            <Field label="Password" type="password" value={configs[activeTab].password}
              onChange={(v) => setOracle(activeTab, "password", v)} />

            {activeTab === "oracle_source" && (
              <>
                <div style={{
                  fontSize: 11, color: t.text.disabled, fontWeight: 600, marginBottom: 4, marginTop: 8,
                  textTransform: "uppercase", letterSpacing: 0.4,
                  borderTop: `1px solid ${t.border.base}`, paddingTop: 12,
                }}>
                  Schema Owner (DDL / Catalog)
                </div>
                <Field label="Owner User" value={configs.oracle_source.owner_user}
                  onChange={(v) => setOracle("oracle_source", "owner_user", v)} placeholder="schema_owner" />
                <Field label="Owner Password" type="password" value={configs.oracle_source.owner_password}
                  onChange={(v) => setOracle("oracle_source", "owner_password", v)} />
              </>
            )}
          </div>
        )}

        {activeTab === "kafka" && (
          <div>
            <Field
              label="Bootstrap Servers"
              value={configs.kafka.bootstrap_servers}
              onChange={(v) => setConfigs((p) => ({ ...p, kafka: { bootstrap_servers: v } }))}
              placeholder="broker1:9092,broker2:9092"
            />
          </div>
        )}

        {activeTab === "kafka_connect" && (
          <div>
            <Field
              label="REST API URL"
              value={configs.kafka_connect.url}
              onChange={(v) => setConfigs((p) => ({ ...p, kafka_connect: { url: v } }))}
              placeholder="http://connect:8083"
            />
          </div>
        )}

        {/* Test result */}
        {testResult && (
          <div style={{
            background: testResult.status === "up" ? t.green.bg : t.red.bg,
            color:      testResult.status === "up" ? t.green.fg : t.red.fg,
            border:     `1px solid ${testResult.status === "up" ? t.green.dim : t.red.border}`,
            borderRadius: 6, padding: "8px 12px", marginTop: 8, fontSize: 12,
          }}>
            {testResult.status === "up" ? "✓ " : "✗ "}{testResult.message}
          </div>
        )}

        {/* Save / Test buttons */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 6 }}>
          <button
            onClick={saveActive}
            disabled={saving}
            style={{
              background: saved ? t.green.bg : t.blue.dim,
              color: saved ? t.green.fg : t.text.primary,
              border: "none", borderRadius: 6,
              padding: "8px 22px", fontSize: 13, cursor: saving ? "wait" : "pointer", fontWeight: 600,
            }}
          >
            {saving ? "Saving…" : saved ? "✓ Saved" : "Save"}
          </button>
          {(activeTab === "oracle_source" || activeTab === "oracle_target") && (
            <button
              onClick={testConnection}
              disabled={testing}
              style={{
                background: t.bg.s2, border: `1px solid ${t.border.base}`, borderRadius: 6,
                color: t.text.secondary, padding: "8px 16px", fontSize: 13,
                cursor: testing ? "wait" : "pointer", fontWeight: 600,
              }}
            >
              {testing ? "Проверка…" : "Тест подключения"}
            </button>
          )}
          <span style={{ fontSize: 11, color: t.text.disabled, marginLeft: 4 }}>
            {TABS.find((t) => t.key === activeTab)?.label}
          </span>
        </div>

        {/* DB hint */}
        <p style={{ margin: "18px 0 0", fontSize: 10, color: t.border.base, lineHeight: 1.6 }}>
          State DB: env <code style={{ color: t.text.disabled }}>STATE_DB_DSN</code>
        </p>
      </div>
    </>
  );
}

// ─── Service metrics panel ───────────────────────────────────────────────────

function ServiceMetricsPanel({
  tab, data,
}: {
  tab:   TabKey;
  data:  ServicesMetrics | null;
}) {
  if (!data) {
    return (
      <div style={metricsBoxStyle}>
        <span style={{ color: t.text.muted, fontSize: 11 }}>загружаем метрики…</span>
      </div>
    );
  }
  if (tab === "oracle_source" || tab === "oracle_target") {
    return <OracleMetricsPanel data={data[tab]}/>;
  }
  if (tab === "kafka") {
    return <KafkaMetricsPanel data={data.kafka}/>;
  }
  return <KafkaConnectMetricsPanel data={data.kafka_connect}/>;
}

function StatusDot({ ok }: { ok: boolean }) {
  const color = ok ? t.green.fg : t.red.fg;
  return (
    <span aria-hidden style={{
      width: 8, height: 8, borderRadius: "50%",
      background: color,
      boxShadow: `0 0 0 3px color-mix(in oklab, ${color} 22%, transparent)`,
      display: "inline-block", flexShrink: 0,
    }}/>
  );
}

function MetricItem({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 1 }}>
      <span style={{ fontSize: "9.5px", textTransform: "uppercase", letterSpacing: "0.06em", color: t.text.muted, fontWeight: 600 }}>
        {label}
      </span>
      <span style={{ fontSize: "12.5px", fontWeight: 500, fontFamily: mono ? t.font.mono : undefined }}>
        {value}
      </span>
    </div>
  );
}

function OracleMetricsPanel({ data }: { data: OracleMetric }) {
  return (
    <div style={metricsBoxStyle}>
      <div style={metricsHeadStyle}>
        <StatusDot ok={data.ok}/>
        <span style={{ fontWeight: 600, fontSize: "12.5px" }}>
          {data.ok ? "Подключение работает" : "Недоступен"}
        </span>
        {data.rtt_ms !== undefined && (
          <span style={{ marginLeft: "auto", fontSize: 11, color: t.text.muted, fontFamily: t.font.mono }}>
            {data.rtt_ms} ms
          </span>
        )}
      </div>
      {!data.ok && data.error && (
        <div style={{ fontSize: 11, color: t.red.fg, fontFamily: t.font.mono }}>{data.error}</div>
      )}
      {data.ok && (
        <div style={metricsGridStyle}>
          <MetricItem label="Host"     value={data.host || "—"} mono/>
          <MetricItem label="Service"  value={data.service_name || "—"} mono/>
          <MetricItem label="Version"  value={data.version || "—"} mono/>
          <MetricItem label="CPU"      value={`${data.cpu_pct ?? 0}%`}/>
          <MetricItem label="Redo/s"   value={fmtBytes(data.redo_bps || 0) + "/s"}/>
          <MetricItem label="Network"  value={fmtBytes(data.network_bps || 0) + "/s"}/>
          <MetricItem label="Sessions" value={data.active_sessions ?? "—"}/>
          {data.instance && (
            <MetricItem label="Instance" value={`${data.instance.name} · ${data.instance.status}`} mono/>
          )}
        </div>
      )}
    </div>
  );
}

function KafkaMetricsPanel({ data }: { data: KafkaMetric }) {
  return (
    <div style={metricsBoxStyle}>
      <div style={metricsHeadStyle}>
        <StatusDot ok={data.ok}/>
        <span style={{ fontWeight: 600, fontSize: "12.5px" }}>
          {data.ok ? "Кластер доступен" : "Недоступен"}
        </span>
        {data.rtt_ms !== undefined && (
          <span style={{ marginLeft: "auto", fontSize: 11, color: t.text.muted, fontFamily: t.font.mono }}>
            {data.rtt_ms} ms
          </span>
        )}
      </div>
      {!data.ok && data.error && (
        <div style={{ fontSize: 11, color: t.red.fg, fontFamily: t.font.mono }}>{data.error}</div>
      )}
      {data.ok && (
        <div style={metricsGridStyle}>
          <MetricItem label="Bootstrap"  value={data.bootstrap || "—"} mono/>
          <MetricItem label="Brokers"    value={data.brokers ?? 0}/>
          <MetricItem label="Topics"     value={data.topics  ?? 0}/>
          {data.cluster_id && <MetricItem label="Cluster ID" value={data.cluster_id} mono/>}
        </div>
      )}
    </div>
  );
}

function KafkaConnectMetricsPanel({ data }: { data: KafkaConnectMetric }) {
  return (
    <div style={metricsBoxStyle}>
      <div style={metricsHeadStyle}>
        <StatusDot ok={data.ok}/>
        <span style={{ fontWeight: 600, fontSize: "12.5px" }}>
          {data.ok ? "REST API отвечает" : "Недоступен"}
        </span>
        {data.rtt_ms !== undefined && (
          <span style={{ marginLeft: "auto", fontSize: 11, color: t.text.muted, fontFamily: t.font.mono }}>
            {data.rtt_ms} ms
          </span>
        )}
      </div>
      {!data.ok && data.error && (
        <div style={{ fontSize: 11, color: t.red.fg, fontFamily: t.font.mono }}>{data.error}</div>
      )}
      {data.ok && data.connectors && (
        <div style={metricsGridStyle}>
          <MetricItem label="URL"     value={data.url || "—"} mono/>
          <MetricItem label="Version" value={data.version || "—"} mono/>
          <MetricItem label="CDC runtimes" value={data.connectors.total}/>
          <MetricItem label="Работают" value={<span style={{ color: t.green.fg }}>{data.connectors.running}</span>}/>
          <MetricItem label="Ошибки"  value={<span style={{ color: data.connectors.failed > 0 ? t.red.fg : t.text.primary }}>{data.connectors.failed}</span>}/>
          <MetricItem label="Пауза"  value={data.connectors.paused}/>
        </div>
      )}
    </div>
  );
}

const metricsBoxStyle: React.CSSProperties = {
  background:   t.bg.s2,
  border:       `1px solid ${t.border.subtle}`,
  borderRadius: 6,
  padding:      "10px 12px",
  marginBottom: 14,
  display:      "flex",
  flexDirection: "column",
  gap:          8,
};
const metricsHeadStyle: React.CSSProperties = {
  display:    "flex",
  alignItems: "center",
  gap:        8,
};
const metricsGridStyle: React.CSSProperties = {
  display:             "grid",
  gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))",
  gap:                 "8px 16px",
};
