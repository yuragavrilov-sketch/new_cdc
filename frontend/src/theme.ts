/**
 * Design tokens — single source of truth for colors, spacing, typography.
 *
 * Light theme palette, derived from the CDC·Migrator design handoff.
 * The `t.*` API is intentionally compatible with the legacy dark theme:
 * existing components reading `t.bg.s1`/`t.green.fg`/etc. resolve to
 * sensible light-theme equivalents (page surfaces, oklch tones).
 */

export const t = {
  bg: {
    app: "var(--bg)",
    s1:  "var(--surface)",    // primary surface (cards, panels)
    s2:  "var(--surface-2)",  // secondary (sidebar, right rail, sticky thead)
    s3:  "var(--surface-3)",  // tertiary (hover, segmented track)
    deep:"var(--surface-2)",  // alias — no deeper-than-surface tier in light
  },
  border: {
    subtle:  "var(--border)",
    base:    "var(--border)",
    strong:  "var(--border-strong)",
  },
  text: {
    primary:   "var(--fg)",
    secondary: "var(--fg-2)",
    muted:     "var(--fg-dim)",
    disabled:  "var(--fg-dim)",
    faint:     "var(--fg-faint)",
    inverse:   "var(--bg)",
  },
  blue: {
    base:  "var(--tone-info)",
    dim:   "var(--accent)",
    bg:    "var(--tone-info-soft)",
    fg:    "var(--tone-info)",
  },
  green: {
    base:  "var(--tone-ok)",
    dim:   "var(--tone-ok)",
    bg:    "var(--tone-ok-soft)",
    fg:    "var(--tone-ok)",
    border:"var(--tone-ok)",
  },
  amber: {
    base:  "var(--tone-warn)",
    dim:   "var(--tone-warn)",
    bg:    "var(--tone-warn-soft)",
    fg:    "var(--tone-warn)",
  },
  red: {
    base:  "var(--tone-error)",
    dim:   "var(--tone-error)",
    bg:    "var(--tone-error-soft)",
    fg:    "var(--tone-error)",
    border:"var(--tone-error)",
  },
  purple: {
    base:  "var(--tone-violet)",
    fg:    "var(--tone-violet)",
    bg:    "var(--tone-violet-soft)",
  },
  // Direct tone aliases (preferred for new code)
  tone: {
    info:  "var(--tone-info)",
    infoSoft:  "var(--tone-info-soft)",
    ok:    "var(--tone-ok)",
    okSoft:    "var(--tone-ok-soft)",
    warn:  "var(--tone-warn)",
    warnSoft:  "var(--tone-warn-soft)",
    error: "var(--tone-error)",
    errorSoft: "var(--tone-error-soft)",
    accent:    "var(--accent)",
    accentSoft:"var(--accent-soft)",
  },
  shadow: {
    s1: "var(--shadow-1)",
    s2: "var(--shadow-2)",
    s3: "var(--shadow-3)",
  },
  radius: {
    sm:   "6px",
    md:   "6px",
    lg:   "8px",
    xl:   "12px",
    pill: "999px",
  },
  space: {
    1: "4px",
    2: "8px",
    3: "12px",
    4: "16px",
    5: "20px",
    6: "24px",
    8: "32px",
  },
  font: {
    sans: "'Geist', 'Inter', system-ui, -apple-system, sans-serif",
    mono: "'Geist Mono', 'JetBrains Mono', ui-monospace, Menlo, monospace",
  },
  size: {
    xs:   "10px",
    sm:   "11px",
    base: "12px",
    md:   "13px",
    lg:   "14px",
    xl:   "18px",
    xxl:  "24px",
  },
} as const;

/**
 * Global stylesheet — injected once via main.tsx.
 * Light palette, oklch tones, Geist typography (from the CDC·Migrator handoff).
 */
// В контурах без интернета (или с фильтрацией fonts.gstatic.com) запрос
// к Google Fonts падал и DevTools шумел. font-family уже содержит фолбэк
// на 'Inter' / system-ui, поэтому удаление @import не ломает вид.
export const themeCss = `
  :root {
    /* surfaces — warm off-white */
    --bg:         #fbfaf7;
    --surface:    #ffffff;
    --surface-2:  #f5f3ee;
    --surface-3:  #ebe8e2;

    /* borders */
    --border:        #e7e4dd;
    --border-strong: #d5d1c8;

    /* foreground / text */
    --fg:       #1c1c1c;
    --fg-2:     #3a3a3a;
    --fg-dim:   #8a8780;
    --fg-faint: #b8b5ad;
    --fg-inverse: #ffffff;

    /* accent — interactive primary (focus rings, links).
       sRGB hex, чтобы не зависеть от поддержки oklch() — был случай,
       когда oklch не парсился в браузере и background падал в transparent. */
    --accent:      #2a75ba;
    --accent-soft: #d4ebff;
    --accent-fg:   #ffffff;

    /* status tones */
    --tone-info:        #3c7ebe;
    --tone-info-soft:   #e0f1ff;
    --tone-ok:          #4a925c;
    --tone-ok-soft:     #dcf7e1;
    --tone-warn:        #c0851f;
    --tone-warn-soft:   #ffefcd;
    --tone-error:       #ce5249;
    --tone-error-soft:  #ffe8e3;
    --tone-violet:      #7262b7;
    --tone-violet-soft: #eeecff;

    /* shadows */
    --shadow-1: 0 1px 0 rgba(20,20,20,0.04), 0 1px 2px rgba(20,20,20,0.03);
    --shadow-2: 0 1px 0 rgba(20,20,20,0.04), 0 8px 24px -8px rgba(20,20,20,0.12);
    --shadow-3: 0 8px 32px -8px rgba(20,20,20,0.18);

    color-scheme: light;
  }

  * { box-sizing: border-box; }
  html, body {
    margin: 0;
    padding: 0;
    background: var(--bg);
    color: var(--fg);
    font-family: 'Geist', 'Inter', system-ui, -apple-system, sans-serif;
    font-size: 13px;
    line-height: 1.45;
    -webkit-font-smoothing: antialiased;
    font-feature-settings: 'ss01', 'cv11';
  }
  input, button, textarea, select { outline: none; font: inherit; color: inherit; }

  @keyframes pulse  { 0%,100%{opacity:1} 50%{opacity:.4} }
  @keyframes fadeIn { from{opacity:0;transform:translateY(-4px)} to{opacity:1;transform:none} }
  @keyframes slideIn { from{transform:translateX(20px);opacity:0} to{transform:none;opacity:1} }
  @keyframes popIn  { from{transform:translateY(10px) scale(.98);opacity:0} to{transform:none;opacity:1} }

  ::-webkit-scrollbar       { width: 8px; height: 8px; }
  ::-webkit-scrollbar-track { background: var(--surface-2); }
  ::-webkit-scrollbar-thumb { background: var(--border-strong); border-radius: 4px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--fg-faint); }
`;
