import { t } from "../../theme";

export type Decision = "migrate" | "skip" | "archive";
export type Status   = "done" | "pending";

export interface TableItem {
  item_id:  number;
  schema:   string;
  table:    string;
  decision: Decision;
  status:   Status;
}

export interface ChecklistList {
  list_id: number;
  name:    string;
  tables:  TableItem[];
}

export const DECISION_LABELS: Record<Decision, string> = {
  migrate: "Переносить",
  skip:    "Нет",
  archive: "Архивные данные",
};

export const DECISION_COLORS: Record<Decision, { bg: string; text: string }> = {
  migrate: { bg: t.green.bg, text: t.green.fg },
  skip:    { bg: t.bg.s2,    text: t.text.muted },
  archive: { bg: t.amber.bg, text: t.amber.fg },
};

export const STATUS_LABELS: Record<Status, string> = {
  done:    "Перенесено",
  pending: "Не перенесено",
};

export async function api<T>(url: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(url, { headers: { "Content-Type": "application/json" }, ...opts });
  if (res.status === 204) return undefined as T;
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `HTTP ${res.status}`);
  }
  return res.json();
}
