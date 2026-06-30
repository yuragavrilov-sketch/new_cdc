import React from "react";
import { t } from "../theme";
import { Icon, ProgressBar, Endpoint } from "../components/ui";
import { fmtGb } from "../utils/format";
import { type SchemaInfo } from "./types";

interface Props {
  schema:   SchemaInfo;
  progress: number;    // 0–100
}

export function SchemaHeader({ schema, progress }: Props) {
  return (
    <div style={{
      background:   t.bg.s1,
      border:       `1px solid ${t.border.subtle}`,
      borderRadius: t.radius.xl,
      padding:      "16px 20px",
      marginBottom: 12,
      boxShadow:    t.shadow.s1,
    }}>
      <div style={{
        display: "flex", justifyContent: "space-between",
        alignItems: "flex-start", gap: 16, marginBottom: 14,
      }}>
        <div>
          <div style={{
            display: "flex", alignItems: "center", gap: 6,
            fontSize: 11, color: t.text.muted, marginBottom: 6,
          }}>
            <span>Миграции</span>
            <Icon name="chevron" size={11}/>
            <span style={{ fontFamily: t.font.mono }}>{schema.id}</span>
            <Icon name="chevron" size={11}/>
            <span style={{ color: t.text.secondary, fontWeight: 500 }}>{schema.name}</span>
          </div>
          <div style={{
            display: "flex", alignItems: "center", gap: 10,
            marginBottom: 8, flexWrap: "wrap",
          }}>
            <h1 style={{
              margin: 0, fontSize: 21, fontWeight: 600,
              letterSpacing: "-0.02em",
            }}>
              <span style={{ fontFamily: t.font.mono }}>{schema.name}</span>
              <span style={{ color: t.text.muted }}> / </span>
              миграция схемы
            </h1>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 12, fontSize: 12 }}>
            <Endpoint label="SOURCE" value={`${schema.source.host} · ${schema.source.version}`}/>
            <Icon name="arrow" size={14}/>
            <Endpoint label="TARGET" value={`${schema.target.host} · ${schema.target.version}`}/>
          </div>
        </div>
      </div>

      <div style={{
        display: "flex", flexDirection: "column", gap: 12,
        paddingTop: 12,
        borderTop: `1px dashed ${t.border.subtle}`,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <ProgressBar value={progress} tone="info" height={8}/>
          <span style={{
            fontFamily: t.font.mono, fontSize: 15, fontWeight: 600,
            minWidth: 60, textAlign: "right",
          }}>
            {progress.toFixed(1)}%
          </span>
        </div>
        <div style={{
          display: "flex", gap: 10, flexWrap: "wrap",
          fontSize: "11.5px", color: t.text.muted,
        }}>
          <span>Стартовала <Mono>{schema.startedAt}</Mono></span>
          <span>·</span>
          <span>Окно cutover <Mono>{schema.windowAt}</Mono></span>
          <span>·</span>
          <span>Размер <Mono>{fmtGb(schema.sizeGb)}</Mono></span>
          <span>·</span>
          <span>Владелец <Mono>{schema.owner}</Mono></span>
        </div>
      </div>
    </div>
  );
}

function Mono({ children }: { children: React.ReactNode }) {
  return <span style={{ fontFamily: t.font.mono, color: t.text.secondary }}>{children}</span>;
}
