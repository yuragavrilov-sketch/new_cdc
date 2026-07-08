import { describe, expect, it } from "vitest";
import { migrationRowActions, phaseFilterOptions } from "./helpers";

describe("migrationRowActions", () => {
  it("allows pausing any active migration without replacing stop", () => {
    expect(migrationRowActions("CDC_CATCHING_UP")).toEqual({
      canRun: false,
      canPause: true,
      canResume: false,
      canStop: true,
      canDelete: false,
    });
  });

  it("allows resuming paused migrations while preserving their phase", () => {
    expect(migrationRowActions("BULK_LOADING", true)).toEqual({
      canRun: false,
      canPause: false,
      canResume: true,
      canStop: true,
      canDelete: false,
    });
  });

  it("keeps draft migrations runnable and deletable", () => {
    expect(migrationRowActions("DRAFT")).toEqual({
      canRun: true,
      canPause: false,
      canResume: false,
      canStop: false,
      canDelete: true,
    });
  });
});

describe("phaseFilterOptions", () => {
  it("returns present phases with counts", () => {
    expect(phaseFilterOptions([
      { phase: "BULK_LOADING" },
      { phase: "CDC_CATCHING_UP" },
      { phase: "BULK_LOADING" },
    ])).toEqual([
      { phase: "BULK_LOADING", count: 2 },
      { phase: "CDC_CATCHING_UP", count: 1 },
    ]);
  });
});
