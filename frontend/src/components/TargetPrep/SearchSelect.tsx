import React, { useEffect, useMemo, useRef, useState } from "react";
import { t } from "../../theme";

function highlightMatch(text: string, query: string) {
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx === -1) return <>{text}</>;
  return (
    <>
      {text.slice(0, idx)}
      <mark style={{ background: `${t.blue.dim}44`, color: t.blue.fg, padding: 0 }}>
        {text.slice(idx, idx + query.length)}
      </mark>
      {text.slice(idx + query.length)}
    </>
  );
}

interface Props {
  value:       string;
  onChange:    (v: string) => void;
  options:     string[];
  placeholder: string;
  disabled?:   boolean;
}

export function SearchSelect({
  value, onChange, options, placeholder, disabled,
}: Props) {
  const [query,  setQuery]  = useState("");
  const [open,   setOpen]   = useState(false);
  const wrapRef  = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setOpen(false); setQuery("");
      }
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  useEffect(() => { if (open) inputRef.current?.focus(); }, [open]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? options.filter(o => o.toLowerCase().includes(q)) : options;
  }, [options, query]);

  const handleOpen   = () => { if (!disabled) { setOpen(o => !o); setQuery(""); } };
  const handleSelect = (opt: string) => { onChange(opt); setOpen(false); setQuery(""); };
  const handleKey    = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") { setOpen(false); setQuery(""); }
    if (e.key === "Enter" && filtered.length === 1) handleSelect(filtered[0]);
  };

  return (
    <div ref={wrapRef} style={{ position: "relative", minWidth: 165 }}>
      <div
        onClick={handleOpen}
        style={{
          display: "flex", alignItems: "center", gap: 6,
          background: t.bg.s2,
          border: `1px solid ${open ? t.blue.base : t.border.base}`,
          borderRadius: t.radius.sm, padding: "0 8px", height: 30,
          cursor: disabled ? "not-allowed" : "pointer",
          opacity: disabled ? 0.5 : 1, userSelect: "none",
        }}
      >
        <span style={{
          fontSize: t.size.base, flex: 1,
          color: value ? t.text.primary : t.text.disabled,
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
        }}>
          {value || placeholder}
        </span>
        <span style={{ color: t.text.disabled, fontSize: 9, flexShrink: 0 }}>
          {open ? "▲" : "▼"}
        </span>
      </div>
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 3px)", left: 0, right: 0,
          background: t.bg.s2, border: `1px solid ${t.border.base}`,
          borderRadius: t.radius.sm, zIndex: 200,
          boxShadow: "0 6px 20px rgba(0,0,0,0.5)",
          display: "flex", flexDirection: "column",
        }}>
          <div style={{
            padding: "6px 8px", borderBottom: `1px solid ${t.bg.s2}`,
            display: "flex", alignItems: "center", gap: 6,
          }}>
            <span style={{ color: t.text.disabled, fontSize: t.size.sm }}>🔍</span>
            <input
              ref={inputRef} value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={handleKey} placeholder="Поиск..."
              style={{
                background: "none", border: "none", color: t.text.primary,
                fontSize: t.size.base, width: "100%", outline: "none",
              }}
            />
            {query && (
              <span
                onClick={() => setQuery("")}
                style={{ color: t.text.disabled, cursor: "pointer", fontSize: t.size.sm, flexShrink: 0 }}
              >✕</span>
            )}
          </div>
          <div style={{ maxHeight: 200, overflowY: "auto" }}>
            {value && (
              <div
                onMouseDown={() => handleSelect("")}
                style={{
                  padding: "5px 10px", fontSize: t.size.sm,
                  cursor: "pointer", color: t.text.disabled,
                  borderBottom: `1px solid ${t.bg.s2}`,
                }}
                onMouseEnter={e => (e.currentTarget.style.background = t.bg.s2)}
                onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
              >
                — Очистить —
              </div>
            )}
            {filtered.length === 0
              ? <div style={{ padding: "8px 10px", color: t.text.disabled, fontSize: t.size.base }}>
                  Нет совпадений
                </div>
              : filtered.map(o => (
                <div
                  key={o} onMouseDown={() => handleSelect(o)}
                  style={{
                    padding: "6px 10px", fontSize: t.size.base, cursor: "pointer",
                    background: o === value ? t.bg.s3 : "transparent",
                    color: o === value ? t.blue.fg : t.text.primary,
                  }}
                  onMouseEnter={e => (e.currentTarget.style.background = o === value ? t.bg.s3 : t.bg.s2)}
                  onMouseLeave={e => (e.currentTarget.style.background = o === value ? t.bg.s3 : "transparent")}
                >
                  {query ? highlightMatch(o, query) : o}
                </div>
              ))
            }
          </div>
        </div>
      )}
    </div>
  );
}
