import React from "react";
import { t } from "../theme";
import { Sparkline } from "../components/ui";
import type { LiveMetrics, MigrationEvent } from "../dashboard/types";
import { fmtBytes } from "../utils/format";

interface Props {
  schemaName: string;
  metrics:    LiveMetrics;
  events:     MigrationEvent[];
}

export function RightRail({ schemaName, metrics, events }: Props) {
  return (
    <aside style={{
      borderLeft:    `1px solid ${t.border.subtle}`,
      background:    t.bg.s2,
      padding:       "16px",
      display:       "flex",
      flexDirection: "column",
      gap:           14,
      position:      "sticky",
      top:           0,
      height:        "100vh",
      overflow:      "hidden",
    }}>
      {/* Live metrics */}
      <section style={{ display: "flex", flexDirection: "column", gap: 8, minHeight: 0 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span style={{
            fontSize: 11, fontWeight: 600,
            textTransform: "uppercase", letterSpacing: "0.06em",
            color: t.text.secondary,
          }}>
            Live · <span style={{ fontFamily: t.font.mono }}>{schemaName}</span>
          </span>
          <span aria-hidden style={{
            width: 7, height: 7, borderRadius: "50%",
            background: t.tone.ok,
            boxShadow: `0 0 0 3px color-mix(in oklab, ${t.tone.ok} 22%, transparent)`,
            animation: "pulse 1.6s infinite",
          }}/>
        </div>
        <div style={{
          display: "flex", flexDirection: "column", gap: 4,
          background: t.bg.s1,
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.lg,
          padding: 10,
        }}>
          <GroupHeading>Source</GroupHeading>
          <MiniStat label="CPU"     value={`${metrics.sourceCpu}%`}             tone="info" data={metrics.cpuSpark}/>
          <MiniStat label="Network" value={`${metrics.network} MB/s`}           tone="ok"   data={metrics.netSpark}/>
          <MiniStat label="Redo/s"  value={fmtBytes(metrics.redoPerSec) + "/s"} tone="warn" data={metrics.redoSpark}/>

          <GroupHeading>Target</GroupHeading>
          <MiniStat label="CPU"     value={`${metrics.targetCpu}%`}                   tone="info" data={metrics.targetCpuSpark}/>
          <MiniStat label="Network" value={`${metrics.targetNetwork} MB/s`}           tone="ok"   data={metrics.targetNetSpark}/>
          <MiniStat label="Redo/s"  value={fmtBytes(metrics.targetRedoPerSec) + "/s"} tone="warn" data={metrics.targetRedoSpark}/>

          <GroupHeading>CDC</GroupHeading>
          <MiniStat
            label="Lag"
            value={`${metrics.cdcLag.toLocaleString()} msg`}
            tone={metrics.cdcLag === 0 ? "ok" : metrics.cdcLag < 10_000 ? "warn" : "error"}
            data={metrics.lagSpark}
          />
        </div>
      </section>

      {/* Event feed */}
      <section style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0, gap: 8 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span style={{
            fontSize: 11, fontWeight: 600,
            textTransform: "uppercase", letterSpacing: "0.06em",
            color: t.text.secondary,
          }}>
            События
          </span>
          <span style={{ fontFamily: t.font.mono, fontSize: 11, color: t.text.muted }}>
            {events.length}
          </span>
        </div>
        <div style={{
          display: "flex", flexDirection: "column", gap: 1,
          overflowY: "auto",
          background: t.bg.s1,
          border: `1px solid ${t.border.subtle}`,
          borderRadius: t.radius.lg,
          padding: 4,
          flex: 1,
          minHeight: 0,
        }}>
          {events.map((e, i) => <FeedRow key={i} event={e}/>)}
        </div>
      </section>
    </aside>
  );
}

function GroupHeading({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      fontSize: "9.5px", fontWeight: 700, letterSpacing: "0.07em",
      textTransform: "uppercase",
      color: t.text.muted,
      marginTop: 6,
      paddingBottom: 2,
      borderBottom: `1px dashed ${t.border.subtle}`,
    }}>
      {children}
    </div>
  );
}

function MiniStat({ label, value, tone, data }: {
  label: string;
  value: string;
  tone:  "info" | "ok" | "warn" | "error";
  data:  number[];
}) {
  const color =
    tone === "info"  ? t.tone.info :
    tone === "ok"    ? t.tone.ok   :
    tone === "warn"  ? t.tone.warn :
                       t.tone.error;
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "70px 1fr auto",
      gap: 8, alignItems: "center",
      padding: "2px 0",
    }}>
      <span style={{ fontSize: 11, color: t.text.muted }}>{label}</span>
      <span style={{ fontFamily: t.font.mono, fontSize: "11.5px", fontWeight: 500 }}>{value}</span>
      <Sparkline data={data} width={64} height={16} color={color}/>
    </div>
  );
}

function FeedRow({ event }: { event: MigrationEvent }) {
  const dotColor =
    event.level === "error" ? t.tone.error :
    event.level === "warn"  ? t.tone.warn  :
                              t.tone.info;
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "52px 8px 1fr",
      gap: 6, alignItems: "flex-start",
      padding: "5px 6px",
      borderRadius: 4,
    }}>
      <span style={{ fontFamily: t.font.mono, fontSize: 10, color: t.text.muted, paddingTop: 2 }}>
        {event.t}
      </span>
      <span aria-hidden style={{
        width: 6, height: 6, borderRadius: "50%",
        background: dotColor,
        marginTop: 6,
      }}/>
      <div style={{ lineHeight: 1.3, minWidth: 0 }}>
        <div style={{ fontFamily: t.font.mono, fontSize: 10, color: t.text.muted, marginBottom: 1 }}>
          {event.obj}
        </div>
        <div style={{ fontSize: 11, color: t.text.secondary, wordBreak: "break-word" }}>
          {event.msg}
        </div>
      </div>
    </div>
  );
}
