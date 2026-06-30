/** Shared formatting helpers — replace per-file copies of fmtTs/fmtNum/etc. */

interface FmtTsOpts {
  withSeconds?: boolean;
  timeOnly?:    boolean;
  withDate?:    boolean;
}

export function fmtTs(iso: string | null | undefined, opts: FmtTsOpts = {}): string {
  if (!iso) return "—";
  const { withSeconds = false, timeOnly = false, withDate = true } = opts;
  try {
    const d = new Date(iso);
    if (timeOnly) {
      return d.toLocaleTimeString("ru-RU", {
        hour: "2-digit", minute: "2-digit",
        ...(withSeconds ? { second: "2-digit" } : {}),
      });
    }
    return d.toLocaleString("ru-RU", {
      ...(withDate ? { day: "2-digit", month: "2-digit", year: "numeric" } : {}),
      hour: "2-digit", minute: "2-digit",
      ...(withSeconds ? { second: "2-digit" } : {}),
    });
  } catch {
    return iso;
  }
}

export function fmtNum(n: number | null | undefined): string {
  if (n == null) return "—";
  return n.toLocaleString("ru-RU");
}

export function fmtDuration(ms: number): string {
  if (ms < 0) ms = 0;
  if (ms < 1000) return `${ms} мс`;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s} сек`;
  const m  = Math.floor(s / 60);
  const rs = s % 60;
  if (m < 60) return rs > 0 ? `${m} мин ${rs} сек` : `${m} мин`;
  const h  = Math.floor(m / 60);
  const rm = m % 60;
  return rm > 0 ? `${h} ч ${rm} мин` : `${h} ч`;
}

export function fmtSpeed(v: number): string {
  if (v >= 1000) return `${(v / 1000).toFixed(1)}k/s`;
  return `${v.toFixed(v < 10 ? 1 : 0)}/s`;
}

export function fmtBytes(n: number | null | undefined): string {
  if (n == null) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let u = 0;
  while (v >= 1024 && u < units.length - 1) { v /= 1024; u++; }
  return `${v.toFixed(v < 10 && u > 0 ? 1 : 0)} ${units[u]}`;
}

/** Compact integer: 1842 → "1.8k", 184_000_000 → "184M". For dashboard KPI/rows columns. */
export function fmtCompactNum(n: number | null | undefined): string {
  if (n == null) return "—";
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + "M";
  if (n >= 1_000)     return (n / 1_000).toFixed(1) + "k";
  return n.toFixed(0);
}

/** Format gigabytes — input GB, output `1.8 TB` or `412 GB`. */
export function fmtGb(gb: number | null | undefined): string {
  if (gb == null) return "—";
  if (gb >= 1024) return (gb / 1024).toFixed(1) + " TB";
  return gb + " GB";
}

/** Format megabytes — auto-scales to KB / MB / GB / TB. */
export function fmtMb(mb: number | null | undefined): string {
  if (mb == null) return "—";
  if (mb >= 1024 * 1024) return (mb / 1024 / 1024).toFixed(1) + " TB";
  if (mb >= 1024)        return (mb / 1024).toFixed(1) + " GB";
  if (mb >= 10)          return mb.toFixed(0) + " MB";
  if (mb >= 0.1)         return mb.toFixed(1) + " MB";
  return (mb * 1024).toFixed(0) + " KB";
}
