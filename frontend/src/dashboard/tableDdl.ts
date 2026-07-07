import type { SchemaObject } from "./types";

export function hasColumnDiff(o: SchemaObject): boolean {
  return o.type === "TABLE" && (o.columnsDiff === true || (o.columnDiffCounts?.total || 0) > 0);
}

export function columnDiffTitle(o: SchemaObject): string {
  const counts = o.columnDiffCounts;
  if (!counts) return "Колонки source/target различаются";
  return [
    `нет в target: ${counts.missing || 0}`,
    `лишних в target: ${counts.extra || 0}`,
    `типов: ${counts.type || 0}`,
  ].join(", ");
}
