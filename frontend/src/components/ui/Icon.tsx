import React from "react";

export type IconName =
  | "dashboard" | "db" | "flow" | "log" | "settings"
  | "plus" | "search" | "pause" | "play" | "rotate"
  | "more" | "close" | "arrow" | "check" | "warn"
  | "error" | "filter" | "bell" | "chevron" | "history"
  | "clusters" | "rules";

const PATHS: Record<IconName, React.ReactNode> = {
  dashboard:<><rect x="3" y="3" width="7" height="9" rx="1.5"/><rect x="14" y="3" width="7" height="5" rx="1.5"/><rect x="14" y="12" width="7" height="9" rx="1.5"/><rect x="3" y="16" width="7" height="5" rx="1.5"/></>,
  db:       <><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></>,
  flow:     <><circle cx="6" cy="6" r="2.5"/><circle cx="18" cy="6" r="2.5"/><circle cx="12" cy="18" r="2.5"/><path d="M7.7 7.6L11 16M16.3 7.6L13 16"/></>,
  log:      <path d="M4 5h16M4 10h16M4 15h10M4 20h7"/>,
  settings: <><circle cx="12" cy="12" r="3"/><path d="M19 12a7 7 0 0 0-.1-1.2l2-1.5-2-3.4-2.4.9a7 7 0 0 0-2-1.2L14 3h-4l-.5 2.6a7 7 0 0 0-2 1.2l-2.4-.9-2 3.4 2 1.5A7 7 0 0 0 5 12c0 .4 0 .8.1 1.2l-2 1.5 2 3.4 2.4-.9a7 7 0 0 0 2 1.2L10 21h4l.5-2.6a7 7 0 0 0 2-1.2l2.4.9 2-3.4-2-1.5c.1-.4.1-.8.1-1.2z"/></>,
  plus:     <path d="M12 5v14M5 12h14"/>,
  search:   <><circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/></>,
  pause:    <><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></>,
  play:     <path d="M7 5l12 7-12 7z"/>,
  rotate:   <><path d="M21 12a9 9 0 1 1-3-6.7L21 8"/><path d="M21 3v5h-5"/></>,
  more:     <><circle cx="5" cy="12" r="1.5"/><circle cx="12" cy="12" r="1.5"/><circle cx="19" cy="12" r="1.5"/></>,
  close:    <path d="M6 6l12 12M18 6L6 18"/>,
  arrow:    <path d="M5 12h14M13 6l6 6-6 6"/>,
  check:    <path d="M5 12l5 5L20 7"/>,
  warn:     <><path d="M12 3l10 18H2z"/><path d="M12 10v5M12 18v.5"/></>,
  error:    <><circle cx="12" cy="12" r="9"/><path d="M9 9l6 6M15 9l-6 6"/></>,
  filter:   <path d="M4 5h16l-6 8v5l-4 2v-7z"/>,
  bell:     <><path d="M6 16V11a6 6 0 0 1 12 0v5l1.5 2h-15z"/><path d="M10 20a2 2 0 0 0 4 0"/></>,
  chevron:  <path d="M9 6l6 6-6 6"/>,
  history:  <><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></>,
  clusters: <><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></>,
  rules:    <><path d="M4 6h16M4 12h12M4 18h8"/><circle cx="20" cy="12" r="2"/><circle cx="16" cy="18" r="2"/></>,
};

interface Props {
  name:  IconName;
  size?: number;
}

export function Icon({ name, size = 16 }: Props) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor"
         strokeWidth={1.6} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      {PATHS[name]}
    </svg>
  );
}
