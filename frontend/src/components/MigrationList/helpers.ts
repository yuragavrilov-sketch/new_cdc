export type FilterKey = "all" | "active" | "done" | "error" | "draft";

export const FILTER_LABELS: { key: FilterKey; label: string }[] = [
  { key: "all",    label: "Все"         },
  { key: "active", label: "Активные"    },
  { key: "done",   label: "Завершённые" },
  { key: "error",  label: "Ошибки"      },
  { key: "draft",  label: "Черновики"   },
];

export const DONE_PHASES = new Set(["COMPLETED", "STEADY_STATE"]);
export const BULK_PHASES = new Set(["CHUNKING", "BULK_LOADING", "BULK_LOADED"]);

export interface SpeedSnapshot {
  chunks_done: number;
  rows_loaded: number;
  ts:          number;
}
