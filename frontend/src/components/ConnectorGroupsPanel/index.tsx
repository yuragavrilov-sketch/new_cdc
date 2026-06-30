import { useCallback, useEffect, useState } from "react";
import type { ConnectorGroup, MigrationSummary } from "../../types/migration";
import type { SSEEvent } from "../../hooks/useSSE";
import { t } from "../../theme";
import type { GroupTable, TopicCount, GroupHistoryEntry } from "./types";
import { STATUS_COLORS, actionBtn } from "./helpers";
import { GroupTablesTable } from "./GroupTablesTable";
import { GroupHistory } from "./GroupHistory";
import { DebeziumConfigModal } from "./DebeziumConfigModal";
import { cdcRuntimeStatusLabel } from "../../dashboard/displayLabels";

interface CdcContinuationResp {
  status?: string;
  error?: string;
  plan_starts?: Array<{ started?: unknown[] }>;
  plan_start_error?: string | null;
  cdc_queue_kicked?: boolean;
}

interface DebeziumSyncStatus {
  connector_name: string;
  exists: boolean;
  in_sync: boolean;
  desired_table_include_list: string;
  actual_table_include_list: string | null;
  desired_message_key_columns: string;
  actual_message_key_columns: string | null;
  missing_tables: string[];
  extra_tables: string[];
  key_columns_match: boolean;
}

export function ConnectorGroupsPanel({ sseEvents = [] }: { sseEvents?: SSEEvent[] }) {
  const [groups,        setGroups]        = useState<ConnectorGroup[]>([]);
  const [loading,       setLoading]       = useState(true);
  const [expanded,      setExpanded]      = useState<string | null>(null);
  const [detail,        setDetail]        = useState<(ConnectorGroup & { tables?: GroupTable[] }) | null>(null);
  const [configModal,   setConfigModal]   = useState<{ json: string; name: string } | null>(null);
  const [configLoading, setConfigLoading] = useState(false);
  const [topicCounts,   setTopicCounts]   = useState<Map<string, TopicCount>>(new Map());
  const [topicLoading,  setTopicLoading]  = useState(false);
  const [history,       setHistory]       = useState<GroupHistoryEntry[]>([]);
  const [actionMsg,     setActionMsg]     = useState<{ tone: "ok" | "bad"; text: string } | null>(null);
  const [syncStatus,    setSyncStatus]    = useState<DebeziumSyncStatus | null>(null);
  const [syncStatusErr, setSyncStatusErr] = useState("");
  const [syncStatusLoading, setSyncStatusLoading] = useState(false);

  const load = useCallback(() => {
    fetch("/api/connector-groups")
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setGroups)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, 10_000);
    return () => clearInterval(id);
  }, [load]);

  const loadDebeziumSyncStatus = useCallback((gid: string) => {
    setSyncStatus(null);
    setSyncStatusErr("");
    setSyncStatusLoading(true);
    fetch(`/api/connector-groups/${gid}/debezium-sync-status`)
      .then(async r => {
        const body = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(body?.error || `HTTP ${r.status}`);
        return body as DebeziumSyncStatus;
      })
      .then(setSyncStatus)
      .catch(e => setSyncStatusErr(String(e instanceof Error ? e.message : e)))
      .finally(() => setSyncStatusLoading(false));
  }, []);

  const toggleExpand = (gid: string) => {
    if (expanded === gid) {
      setExpanded(null); setDetail(null); setTopicCounts(new Map()); setHistory([]);
      setSyncStatus(null); setSyncStatusErr(""); setSyncStatusLoading(false);
    } else {
      setExpanded(gid);
      fetch(`/api/connector-groups/${gid}`)
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(d => {
          setDetail(d);
          loadTopicCounts(gid);
          loadHistory(gid);
          loadDebeziumSyncStatus(gid);
        })
        .catch(() => setDetail(null));
    }
  };

  const loadHistory = useCallback((gid: string) => {
    fetch(`/api/connector-groups/${gid}/history`)
      .then(r => r.ok ? r.json() : [])
      .then(setHistory)
      .catch(() => setHistory([]));
  }, []);

  const loadTopicCounts = useCallback((gid: string) => {
    setTopicLoading(true);
    fetch(`/api/connector-groups/${gid}/topic-counts`)
      .then(r => r.ok ? r.json() : [])
      .then((counts: TopicCount[]) => {
        const map = new Map<string, TopicCount>();
        for (const c of counts) map.set(c.topic_name, c);
        setTopicCounts(map);
      })
      .catch(() => {})
      .finally(() => setTopicLoading(false));
  }, []);

  useEffect(() => {
    const event = sseEvents[0];
    if (!event) return;
    if (
      event.type !== "connector_group_status"
      && event.type !== "schema_migration.plan_items_added"
    ) return;

    load();
    if (expanded) {
      fetch(`/api/connector-groups/${expanded}`)
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(setDetail)
        .catch(() => setDetail(null));
      loadTopicCounts(expanded);
      loadHistory(expanded);
      loadDebeziumSyncStatus(expanded);
    }
  }, [sseEvents, expanded, load, loadHistory, loadTopicCounts, loadDebeziumSyncStatus]);

  const reloadGroupDetail = (gid: string) => {
    load();
    if (expanded !== gid) return;
    fetch(`/api/connector-groups/${gid}`)
      .then(r => r.ok ? r.json() : Promise.reject())
      .then(setDetail)
      .catch(() => setDetail(null));
    loadTopicCounts(gid);
    loadHistory(gid);
    loadDebeziumSyncStatus(gid);
  };

  const continuationMessage = (body: CdcContinuationResp, prefix: string, runningVerb: string, waitingVerb: string) => {
    const startedCount = (body.plan_starts || []).reduce(
      (sum: number, item: { started?: unknown[] }) => sum + (item.started?.length || 0),
      0,
    );
    const status = String(body.status || "").toUpperCase();
    const rowText = status === "RUNNING" ? runningVerb : waitingVerb;
    if (body.plan_start_error) {
      return { tone: "bad" as const, text: `${prefix}, но очередь не продолжена: ${body.plan_start_error}` };
    }
    return {
      tone: "ok" as const,
      text: startedCount
        ? `${prefix}, ${rowText}: ${startedCount}`
        : body.cdc_queue_kicked
          ? `${prefix}, очередь CDC продолжена`
          : prefix,
    };
  };

  const startGroup = async (gid: string) => {
    setActionMsg(null);
    try {
      const res = await fetch(`/api/connector-groups/${gid}/start`, { method: "POST" });
      const body = await res.json().catch(() => ({})) as CdcContinuationResp;
      if (!res.ok) {
        setActionMsg({ tone: "bad", text: body?.error || `HTTP ${res.status}` });
        return;
      }
      const status = String(body.status || "").toUpperCase();
      const prefix = status === "RUNNING"
        ? "CDC-пачка работает"
        : status
          ? `Запуск CDC-пачки: ${cdcRuntimeStatusLabel(status)}`
          : "Запуск CDC-пачки запрошен";
      setActionMsg(continuationMessage(
        body,
        prefix,
        "запущено CDC строк",
        "CDC строк переведено в ожидание CDC-пачки",
      ));
      reloadGroupDetail(gid);
    } catch (e) {
      setActionMsg({ tone: "bad", text: `Сеть: ${String(e)}` });
    }
  };
  const syncGroup = async (gid: string) => {
    setActionMsg(null);
    try {
      const res = await fetch(`/api/connector-groups/${gid}/refresh-tables`, { method: "POST" });
      const body = await res.json().catch(() => ({})) as CdcContinuationResp;
      if (!res.ok) {
        setActionMsg({ tone: "bad", text: body?.error || `HTTP ${res.status}` });
        return;
      }
      const status = String(body.status || "").toUpperCase();
      const prefix = status && status !== "RUNNING"
        ? `CDC-пачка ${cdcRuntimeStatusLabel(status)}; Debezium синхронизируется после запуска`
        : "Debezium синхронизирован";
      setActionMsg(continuationMessage(
        body,
        prefix,
        "запущено CDC строк",
        "CDC строк переведено в ожидание CDC-пачки",
      ));
      reloadGroupDetail(gid);
    } catch (e) {
      setActionMsg({ tone: "bad", text: `Сеть: ${String(e)}` });
    }
  };
  const stopGroup   = (gid: string) => { setActionMsg(null); fetch(`/api/connector-groups/${gid}/stop`,   { method: "POST" }).then(load).catch(() => {}); };
  const createTopics = (gid: string) => fetch(`/api/connector-groups/${gid}/create-topics`, { method: "POST" }).then(() => loadTopicCounts(gid)).catch(() => {});

  const removeTable = async (gid: string, table: GroupTable) => {
    const label = `${table.source_schema}.${table.source_table}`;
    if (!confirm(`Убрать ${label} из CDC-пачки? Debezium table.include.list будет обновлён.`)) return;
    try {
      const r = await fetch(
        `/api/connector-groups/${gid}/tables/${encodeURIComponent(table.source_schema)}/${encodeURIComponent(table.source_table)}`,
        { method: "DELETE" },
      );
      if (!r.ok) {
        const body = await r.json().catch(() => ({}));
        alert(body?.error || `HTTP ${r.status}`);
        return;
      }
      const body = await r.json().catch(() => ({}));
      const d = await fetch(`/api/connector-groups/${gid}`)
        .then(resp => resp.ok ? resp.json() : Promise.reject());
      setDetail(d);
      loadTopicCounts(gid);
      load();
      if (body?.sync_error) alert(`Таблица удалена из пачки, но Debezium не синхронизирован: ${body.sync_error}`);
    } catch (e) {
      alert(`Сеть: ${String(e)}`);
    }
  };

  const deleteGroup = async (gid: string) => {
    if (!confirm("Удалить runtime CDC-пачки? Состав пачки останется в очереди, но привязанный Debezium runtime будет удалён.")) return;
    try {
      const r = await fetch(`/api/connector-groups/${gid}`, { method: "DELETE" });
      if (r.ok) {
        load();
        if (expanded === gid) { setExpanded(null); setDetail(null); }
        return;
      }
      const body = await r.json().catch(() => ({}));
      const msg  = body?.error || `HTTP ${r.status}`;
      // Backend отказался — обычно из-за активных миграций. Предлагаем force.
      if (r.status === 400 && /активных миграций/i.test(msg)) {
        if (!confirm(`${msg}.\n\nПеревести их в CANCELLED и удалить runtime CDC-пачки?`)) return;
        const r2 = await fetch(`/api/connector-groups/${gid}?force=true`, { method: "DELETE" });
        if (r2.ok) {
          load();
          if (expanded === gid) { setExpanded(null); setDetail(null); }
          return;
        }
        const body2 = await r2.json().catch(() => ({}));
        alert(body2?.error || `Не удалось удалить (HTTP ${r2.status})`);
        return;
      }
      alert(msg);
    } catch (e) {
      alert(`Сеть: ${String(e)}`);
    }
  };

  const showConfig = (gid: string, groupName: string) => {
    setConfigLoading(true);
    fetch(`/api/connector-groups/${gid}/debezium-config`)
      .then(r => r.json())
      .then(d => {
        if (d.error) { alert(d.error); return; }
        setConfigModal({ json: JSON.stringify(d, null, 2), name: groupName });
      })
      .catch(e => alert(String(e)))
      .finally(() => setConfigLoading(false));
  };

  // "SCHEMA.TABLE" → most-recent-active migration in this group
  const tableMigrationMap = new Map<string, MigrationSummary>();
  if (detail?.migrations) {
    for (const m of detail.migrations) {
      const key = `${m.source_schema}.${m.source_table}`;
      const existing = tableMigrationMap.get(key);
      if (!existing || (existing.phase === "CANCELLED" || existing.phase === "FAILED" || existing.phase === "COMPLETED")) {
        tableMigrationMap.set(key, m);
      }
    }
  }

  if (loading) return <div style={{ color: t.text.muted, padding: 16 }}>Загрузка...</div>;

  return (
    <div>
      <div style={{ marginBottom: 12 }}>
        <h2 style={{ margin: 0, fontSize: 15, fontWeight: 600, color: t.text.secondary }}>
          CDC-пачки
        </h2>
      </div>
      {actionMsg && (
        <div style={{
          marginBottom: 10,
          padding: "7px 10px",
          borderRadius: t.radius.sm,
          background: actionMsg.tone === "ok" ? t.green.bg : `${t.red.border}22`,
          border: `1px solid ${actionMsg.tone === "ok" ? t.green.dim : t.red.border}`,
          color: actionMsg.tone === "ok" ? t.green.fg : t.red.fg,
          fontSize: 12,
        }}>
          {actionMsg.text}
        </div>
      )}

      {groups.length === 0 && (
        <div style={{ color: t.text.disabled, padding: 24, textAlign: "center" }}>
          CDC-пачка ещё не создана. Добавьте таблицы в CDC-пачку на экране "Эта миграция".
        </div>
      )}

      {groups.map(g => {
        const sc         = STATUS_COLORS[g.status] || STATUS_COLORS.PENDING;
        const isExpanded = expanded === g.group_id;
        return (
          <div key={g.group_id} style={{
            background: t.bg.app, border: `1px solid ${t.border.subtle}`,
            borderRadius: t.radius.lg, marginBottom: 8, overflow: "hidden",
          }}>
            <div
              onClick={() => toggleExpand(g.group_id)}
              style={{
                display: "flex", alignItems: "center", gap: 12,
                padding: "10px 14px", cursor: "pointer",
              }}
            >
              <span style={{
                background: sc.bg, color: sc.text,
                padding: "2px 8px", borderRadius: t.radius.sm,
                fontSize: t.size.sm, fontWeight: 600,
              }}>
                {cdcRuntimeStatusLabel(g.status)}
              </span>
              <strong style={{ fontSize: t.size.md, color: t.text.primary }}>{g.group_name}</strong>
              <span style={{ fontSize: t.size.sm, color: t.text.disabled }}>
                Runtime: {cdcRuntimeStatusLabel(g.status)} | topic prefix: {g.topic_prefix}
              </span>
              <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                {(g.status === "PENDING" || g.status === "STOPPED" || g.status === "FAILED") && (
                  <button onClick={e => { e.stopPropagation(); startGroup(g.group_id); }}
                    style={actionBtn(t.green.bg, t.green.dim)}>
                    Запустить
                  </button>
                )}
                {g.status === "RUNNING" && (
                  <button onClick={e => { e.stopPropagation(); stopGroup(g.group_id); }}
                    style={actionBtn(t.red.bg, t.amber.dim)}>
                    Остановить
                  </button>
                )}
                {(g.status === "PENDING" || g.status === "STOPPED") && (
                  <button onClick={e => { e.stopPropagation(); deleteGroup(g.group_id); }}
                    style={actionBtn(t.red.bg, t.red.dim)}>
                    Удалить
                  </button>
                )}
              </div>
            </div>

            {isExpanded && detail && detail.group_id === g.group_id && (
              <div style={{ padding: "0 14px 12px", borderTop: `1px solid ${t.border.subtle}` }}>
                {g.error_text && (
                  <div style={{ color: t.red.fg, fontSize: t.size.sm, marginTop: 8 }}>{g.error_text}</div>
                )}
                <div style={{
                  fontSize: t.size.sm, color: t.text.disabled, marginTop: 8,
                  display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap",
                }}>
                  <span>Источник: {detail.source_connection_id} | consumer prefix: {detail.consumer_group_prefix || detail.topic_prefix}</span>
                  <span style={{ flex: 1 }} />
                  <button
                    onClick={() => syncGroup(g.group_id)}
                    style={{
                      background: t.bg.s2, border: `1px solid ${t.border.base}`,
                      borderRadius: t.radius.sm, color: t.text.secondary,
                      padding: "2px 10px", fontSize: t.size.xs, cursor: "pointer", fontWeight: 600,
                    }}
                  >Синхронизировать CDC-пачку</button>
                  <button
                    onClick={() => createTopics(g.group_id)}
                    style={{
                      background: t.bg.s2, border: `1px solid ${t.border.base}`,
                      borderRadius: t.radius.sm, color: t.text.secondary,
                      padding: "2px 10px", fontSize: t.size.xs, cursor: "pointer", fontWeight: 600,
                    }}
                  >Создать topics</button>
                  <button
                    onClick={() => loadTopicCounts(g.group_id)}
                    disabled={topicLoading}
                    style={{
                      background: t.bg.s2, border: `1px solid ${t.border.base}`,
                      borderRadius: t.radius.sm, color: t.text.secondary,
                      padding: "2px 10px", fontSize: t.size.xs, cursor: "pointer", fontWeight: 600,
                    }}
                  >{topicLoading ? "..." : "Обновить счётчики"}</button>
                  <button
                    onClick={() => showConfig(g.group_id, g.group_name)}
                    disabled={configLoading}
                    style={{
                      background: t.bg.s2, border: `1px solid ${t.border.base}`,
                      borderRadius: t.radius.sm, color: t.text.secondary,
                      padding: "2px 10px", fontSize: t.size.xs, cursor: "pointer", fontWeight: 600,
                    }}
                  >{configLoading ? "..." : "Конфиг Debezium"}</button>
                </div>
                {(detail.table_include_list || detail.message_key_columns) && (
                  <div style={{
                    marginTop: 8,
                    padding: "7px 9px",
                    border: `1px solid ${t.blue.dim}`,
                    borderRadius: t.radius.sm,
                    background: t.blue.bg,
                    color: t.blue.fg,
                    fontSize: t.size.sm,
                    lineHeight: 1.45,
                    overflowWrap: "anywhere",
                  }}>
                    <div>
                      Kafka Connect синхронизируется из полного состава этой CDC-пачки, не из последнего добавления.
                    </div>
                    {detail.table_include_list && (
                      <div style={{ marginTop: 4, fontFamily: t.font.mono, color: t.text.primary }}>
                        table.include.list: {detail.table_include_list}
                      </div>
                    )}
                    {detail.message_key_columns && (
                      <div style={{ marginTop: 3, fontFamily: t.font.mono, color: t.text.primary }}>
                        message.key.columns: {detail.message_key_columns}
                      </div>
                    )}
                  </div>
                )}
                {(syncStatusLoading || syncStatusErr || syncStatus) && (
                  <div style={{
                    marginTop: 8,
                    padding: "7px 9px",
                    border: `1px solid ${
                      syncStatusErr
                        ? t.red.border
                        : syncStatus?.in_sync
                          ? t.green.dim
                          : t.amber.dim
                    }`,
                    borderRadius: t.radius.sm,
                    background: syncStatusErr
                      ? `${t.red.border}22`
                      : syncStatus?.in_sync
                        ? t.green.bg
                        : t.amber.bg,
                    color: syncStatusErr
                      ? t.red.fg
                      : syncStatus?.in_sync
                        ? t.green.fg
                        : t.amber.fg,
                    fontSize: t.size.sm,
                    lineHeight: 1.45,
                    overflowWrap: "anywhere",
                  }}>
                    {syncStatusLoading && <div>Проверяю фактический config Kafka Connect...</div>}
                    {syncStatusErr && <div>Kafka Connect config не прочитан: {syncStatusErr}</div>}
                    {syncStatus && (
                      <>
                        <div>
                          Конфиг Debezium runtime: <strong>{syncStatus.exists ? (syncStatus.in_sync ? "совпадает" : "есть расхождение") : "runtime не создан"}</strong>
                          {" "}({syncStatus.connector_name})
                        </div>
                        {!syncStatus.exists && (
                          <div style={{ marginTop: 3 }}>
                            Debezium runtime будет создан из состава CDC-пачки при запуске.
                          </div>
                        )}
                        {syncStatus.missing_tables.length > 0 && (
                          <div style={{ marginTop: 3 }}>
                            Нет в Kafka Connect: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{syncStatus.missing_tables.join(", ")}</span>
                          </div>
                        )}
                        {syncStatus.extra_tables.length > 0 && (
                          <div style={{ marginTop: 3 }}>
                            Лишние в Kafka Connect: <span style={{ fontFamily: t.font.mono, color: t.text.primary }}>{syncStatus.extra_tables.join(", ")}</span>
                          </div>
                        )}
                        {!syncStatus.key_columns_match && (
                          <div style={{ marginTop: 3 }}>
                            Расходятся CDC key columns. Нажмите "Синхронизировать CDC-пачку" после проверки состава пачки.
                          </div>
                        )}
                        {syncStatus.actual_table_include_list && (
                          <div style={{ marginTop: 4, fontFamily: t.font.mono, color: t.text.primary }}>
                            actual table.include.list: {syncStatus.actual_table_include_list}
                          </div>
                        )}
                      </>
                    )}
                  </div>
                )}

                {/* Pack tables */}
                <h4 style={{ color: t.text.muted, fontSize: t.size.base, margin: "12px 0 6px" }}>
                  Состав CDC-пачки ({detail.tables?.length || 0})
                </h4>
                <GroupTablesTable
                  tables={detail.tables ?? []}
                  topicCounts={topicCounts}
                  tableMigrationMap={tableMigrationMap}
                  onRemove={(table) => removeTable(g.group_id, table)}
                />

                {/* Queue rows linked to this CDC pack runtime */}
                {detail.migrations && detail.migrations.length > 0 && (
                  <>
                    <h4 style={{ color: t.text.muted, fontSize: t.size.base, margin: "12px 0 6px" }}>
                      Строки очереди ({detail.migrations.length})
                    </h4>
                    <table style={{ width: "100%", fontSize: t.size.sm, borderCollapse: "collapse" }}>
                      <thead>
                        <tr style={{ color: t.text.disabled, textAlign: "left" }}>
                          <th style={{ padding: "4px 8px" }}>Таблица</th>
                          <th style={{ padding: "4px 8px" }}>Фаза</th>
                          <th style={{ padding: "4px 8px" }}>Режим</th>
                        </tr>
                      </thead>
                      <tbody>
                        {detail.migrations.map(m => (
                          <tr key={m.migration_id} style={{ borderTop: `1px solid ${t.border.subtle}` }}>
                            <td style={{ padding: "4px 8px", color: t.text.primary }}>
                              {m.source_schema}.{m.source_table}
                            </td>
                            <td style={{ padding: "4px 8px", color: t.text.secondary }}>{m.phase}</td>
                            <td style={{ padding: "4px 8px", color: t.text.muted }}>{m.strategy ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                )}

                {/* State history */}
                <GroupHistory history={history} />
              </div>
            )}
          </div>
        );
      })}

      {configModal && (
        <DebeziumConfigModal
          json={configModal.json}
          name={configModal.name}
          onClose={() => setConfigModal(null)}
        />
      )}
    </div>
  );
}
