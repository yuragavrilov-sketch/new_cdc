import { ACTIVE_PHASES, DELETABLE_PHASES } from "../MigrationDetail/helpers";
import { ORDERED_PHASES } from "../../types/migration";

export type FilterKey = "all" | "active" | "paused" | "done" | "error" | "draft";

export const FILTER_LABELS: { key: FilterKey; label: string }[] = [
  { key: "all",    label: "Все"         },
  { key: "active", label: "Активные"    },
  { key: "paused", label: "Пауза"       },
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

export type MigrationRowAction = "run" | "pause" | "resume" | "stop" | "delete";

export interface MigrationRowActionState {
  canRun:    boolean;
  canPause:  boolean;
  canResume: boolean;
  canStop:   boolean;
  canDelete: boolean;
}

export function migrationRowActions(phase: string, paused = false): MigrationRowActionState {
  const legacyPaused = phase === "PAUSED";
  const isPaused = paused || legacyPaused;
  return {
    canRun:    phase === "DRAFT",
    canPause:  ACTIVE_PHASES.has(phase) && !isPaused,
    canResume: isPaused,
    canStop:   ACTIVE_PHASES.has(phase),
    canDelete: DELETABLE_PHASES.has(phase),
  };
}

export interface PhaseOption {
  phase: string;
  count: number;
}

export function phaseFilterOptions(items: Array<{ phase: string }>): PhaseOption[] {
  const counts = new Map<string, number>();
  for (const item of items) {
    counts.set(item.phase, (counts.get(item.phase) ?? 0) + 1);
  }
  const order = new Map<string, number>(ORDERED_PHASES.map((phase, index) => [phase, index]));
  return Array.from(counts.entries())
    .map(([phase, count]) => ({ phase, count }))
    .sort((a, b) => {
      const ai = order.get(a.phase) ?? Number.MAX_SAFE_INTEGER;
      const bi = order.get(b.phase) ?? Number.MAX_SAFE_INTEGER;
      if (ai !== bi) return ai - bi;
      return a.phase.localeCompare(b.phase);
    });
}
