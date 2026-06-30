import ReactDOM from "react-dom";
import { t } from "../../theme";

interface Props {
  json:    string;
  name:    string;
  onClose: () => void;
}

export function DebeziumConfigModal({ json, name, onClose }: Props) {
  function copy() {
    try {
      if (navigator.clipboard?.writeText) {
        navigator.clipboard.writeText(json);
      } else {
        const ta = document.createElement("textarea");
        ta.value = json;
        ta.style.position = "fixed";
        ta.style.opacity = "0";
        document.body.appendChild(ta);
        ta.select();
        document.execCommand("copy");
        document.body.removeChild(ta);
      }
    } catch {}
  }

  return ReactDOM.createPortal(
    <div
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,.72)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
      onClick={e => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div style={{
        background: t.bg.app, border: `1px solid ${t.border.subtle}`,
        borderRadius: t.radius.lg, width: "100%", maxWidth: 720, maxHeight: "80vh",
        display: "flex", flexDirection: "column",
        boxShadow: "0 24px 48px rgba(0,0,0,.55)",
      }}>
        <div style={{
          padding: "12px 20px", borderBottom: `1px solid ${t.border.subtle}`,
          display: "flex", alignItems: "center", gap: 10,
        }}>
          <span style={{ fontSize: t.size.lg, fontWeight: 700, color: t.text.primary }}>
            Конфиг Debezium runtime
          </span>
          <span style={{ fontSize: t.size.sm, color: t.text.disabled }}>{name}</span>
          <span style={{ flex: 1 }} />
          <button
            onClick={copy}
            style={{
              background: t.bg.s2, border: `1px solid ${t.border.base}`,
              borderRadius: t.radius.sm, color: t.text.secondary,
              padding: "3px 10px", fontSize: t.size.xs, cursor: "pointer",
            }}
          >
            Копировать
          </button>
          <button
            onClick={onClose}
            style={{
              background: "none", border: "none", color: t.text.disabled,
              cursor: "pointer", fontSize: 18, lineHeight: 1, padding: "0 2px",
            }}
          >✕</button>
        </div>
        <pre style={{
          padding: 20, margin: 0, overflowY: "auto",
          fontSize: t.size.sm, lineHeight: 1.5, color: t.text.primary,
          fontFamily: t.font.mono,
          whiteSpace: "pre-wrap", wordBreak: "break-all",
        }}>{json}</pre>
      </div>
    </div>,
    document.body,
  );
}
