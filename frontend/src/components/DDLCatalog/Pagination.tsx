import React from "react";
import { S } from "./styles";
import { t } from "../../theme";

const PAGE_SIZES = [25, 50, 100];

interface Props {
  total: number;
  page: number;
  pageSize: number;
  onPage: (p: number) => void;
  onPageSize: (s: number) => void;
}

export function Pagination({ total, page, pageSize, onPage, onPageSize }: Props) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const from = total === 0 ? 0 : page * pageSize + 1;
  const to = Math.min((page + 1) * pageSize, total);

  const btnStyle = (disabled: boolean): React.CSSProperties => ({
    ...S.btnSecondary,
    fontSize: 11,
    padding: "3px 8px",
    opacity: disabled ? 0.3 : 1,
    cursor: disabled ? "not-allowed" : "pointer",
  });

  return (
    <div style={{
      display: "flex", alignItems: "center", justifyContent: "space-between",
      padding: "8px 16px", borderTop: `1px solid ${t.border.subtle}`, fontSize: 11, color: t.text.muted,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <span>{from}–{to} из {total}</span>
        <select
          value={pageSize}
          onChange={e => { onPageSize(Number(e.target.value)); onPage(0); }}
          style={{ ...S.select, width: "auto", padding: "2px 6px", fontSize: 11 }}
        >
          {PAGE_SIZES.map(s => <option key={s} value={s}>{s} / стр</option>)}
        </select>
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
        <button disabled={page === 0} onClick={() => onPage(0)} style={btnStyle(page === 0)}>«</button>
        <button disabled={page === 0} onClick={() => onPage(page - 1)} style={btnStyle(page === 0)}>‹</button>
        <span style={{ padding: "0 8px", color: t.text.secondary }}>
          {page + 1} / {totalPages}
        </span>
        <button disabled={page >= totalPages - 1} onClick={() => onPage(page + 1)} style={btnStyle(page >= totalPages - 1)}>›</button>
        <button disabled={page >= totalPages - 1} onClick={() => onPage(totalPages - 1)} style={btnStyle(page >= totalPages - 1)}>»</button>
      </div>
    </div>
  );
}

export function usePagination<T>(items: T[], pageSize: number, page: number): T[] {
  return items.slice(page * pageSize, (page + 1) * pageSize);
}
