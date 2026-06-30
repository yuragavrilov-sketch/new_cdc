/** Schema migration domain types — used by Dashboard (mock data + future API). */

export type ObjectStatus =
  | "running" | "paused" | "error" | "warn"
  | "validating" | "done" | "queued" | "skipped";

export type ObjectType =
  | "TABLE" | "INDEX" | "MVIEW" | "SEQUENCE"
  | "VIEW" | "PACKAGE" | "PROCEDURE" | "FUNCTION"
  | "TRIGGER" | "TYPE" | "SYNONYM" | "GRANT"
  | "DBLINK" | "JOB";

export type ObjectGroupKey =
  | "tables"
  | "indexes"
  | "views"
  | "code"
  | "triggers"
  | "sequences"
  | "jobs"
  | "other";

export const OBJECT_GROUPS: Array<{ key: ObjectGroupKey; label: string; types: ObjectType[] }> = [
  { key: "tables",    label: "Таблицы",        types: ["TABLE"] },
  { key: "indexes",   label: "Индексы",        types: ["INDEX"] },
  { key: "views",     label: "Представления",  types: ["VIEW", "MVIEW"] },
  { key: "code",      label: "PL/SQL",         types: ["PACKAGE", "PROCEDURE", "FUNCTION", "TYPE"] },
  { key: "triggers",  label: "Триггеры",       types: ["TRIGGER"] },
  { key: "sequences", label: "Сиквенсы",       types: ["SEQUENCE"] },
  { key: "jobs",      label: "Задания / DB links", types: ["JOB", "DBLINK"] },
  { key: "other",     label: "Синонимы / права", types: ["SYNONYM", "GRANT"] },
];

export type SchemaStatus =
  | "running" | "cdc" | "paused" | "error"
  | "validating" | "done" | "queued";

export type SchemaStage =
  | "assess" | "schema" | "bulk" | "cdc" | "validate" | "cutover";

export type Priority = "P0" | "P1" | "P2";

export interface SchemaInfo {
  id:        string;
  name:      string;
  source:    { host: string; version: string; tns: string };
  target:    { host: string; version: string; tns: string };
  src_schema?: string;
  tgt_schema?: string;
  owner:     string;
  priority:  Priority;
  status:    SchemaStatus;
  stage:     SchemaStage;
  startedAt: string;
  windowAt:  string;
  schemaCompat: number;
  sizeGb:    number;
  totals:    { rowsPerSec: number; mbPerSec: number };
}

export interface SchemaObject {
  id:         string;
  type:       ObjectType;
  name:       string;
  rows:       number | null;
  rowsDone:   number | null;
  sizeMb:     number;
  status:     ObjectStatus;
  progress:   number;
  rowsPerSec: number;
  mbPerSec:   number;
  compat:     number;
  warn:       number;
  err:        number;
  eta:        string;
  dur:        string;
  note:       string;
  srcStatus?: string;        // Oracle VALID/INVALID on source side (DDL objects)
  tgtStatus?: string;        // Oracle VALID/INVALID on target side
  /** Только для TABLE-миграций */
  strategy?: string;         // "CDC_STAGE" | "CDC_DIRECT" | "BULK_STAGE" | "BULK_DIRECT" | ""
  keyType?:  string;         // "PRIMARY_KEY" | "UNIQUE_KEY" | "USER_DEFINED" | "ROWID" | "NONE" | ""
  hasPk?:    boolean;
  hasUk?:    boolean;
  /** supplemental_log_data_all (Oracle ALL COLUMNS supp-logging).
   *  undefined — снапшот старый и поле не сохранено; null/false — нет; true — есть. */
  hasSuppLog?: boolean | null;
}

export interface MigrationEvent {
  t:     string;
  obj:   string;
  level: "info" | "warn" | "error";
  msg:   string;
}

export interface LiveMetrics {
  sourceCpu: number;   // %
  network:   number;   // MB/s
  redoPerSec: number;  // bytes/s, formatted by callee
  cdcLag:    number;   // суммарный consumer-group lag по всем CDC-миграциям схемы, в сообщениях
  cpuSpark:  number[];
  netSpark:  number[];
  redoSpark: number[];
  lagSpark:  number[];
  // Target side (same metrics from target Oracle's V$SYSMETRIC)
  targetCpu:        number;
  targetNetwork:    number;
  targetRedoPerSec: number;
  targetCpuSpark:   number[];
  targetNetSpark:   number[];
  targetRedoSpark:  number[];
}

export const STATUS_MAP: Record<ObjectStatus, { label: string; tone: "info" | "ok" | "warn" | "error" | "muted" }> = {
  running:    { label: "Идёт",     tone: "info"  },
  paused:     { label: "Пауза",    tone: "muted" },
  error:      { label: "Ошибка",   tone: "error" },
  warn:       { label: "Warn",     tone: "warn"  },
  validating: { label: "Валидация",tone: "warn"  },
  done:       { label: "Готово",   tone: "ok"    },
  queued:     { label: "В очереди",tone: "muted" },
  skipped:    { label: "Пропущен", tone: "muted" },
};

export const STAGES: { key: SchemaStage; label: string }[] = [
  { key: "assess",   label: "Анализ"    },
  { key: "schema",   label: "Схема"     },
  { key: "bulk",     label: "Bulk Load" },
  { key: "cdc",      label: "CDC"       },
  { key: "validate", label: "Валидация" },
  { key: "cutover",  label: "Cutover"   },
];

export const OBJECT_TYPES: Record<ObjectType, { label: string; group: string; ord: number }> = {
  TABLE:     { label: "TABLE",     group: "Данные",  ord:  1 },
  INDEX:     { label: "INDEX",     group: "Данные",  ord:  2 },
  MVIEW:     { label: "MVIEW",     group: "Данные",  ord:  3 },
  SEQUENCE:  { label: "SEQUENCE",  group: "Данные",  ord:  4 },
  VIEW:      { label: "VIEW",      group: "Код",     ord:  5 },
  PACKAGE:   { label: "PACKAGE",   group: "Код",     ord:  6 },
  PROCEDURE: { label: "PROC",      group: "Код",     ord:  7 },
  FUNCTION:  { label: "FUNC",      group: "Код",     ord:  8 },
  TRIGGER:   { label: "TRIGGER",   group: "Код",     ord:  9 },
  TYPE:      { label: "TYPE",      group: "Код",     ord: 10 },
  SYNONYM:   { label: "SYNONYM",   group: "Доступ",  ord: 11 },
  GRANT:     { label: "GRANT",     group: "Доступ",  ord: 12 },
  DBLINK:    { label: "DBLINK",    group: "Доступ",  ord: 13 },
  JOB:       { label: "JOB",       group: "Код",     ord: 14 },
};
