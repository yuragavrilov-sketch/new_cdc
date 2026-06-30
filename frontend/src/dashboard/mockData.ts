/** Mock data for the Dashboard — ported from CDC·Migrator handoff (prototype/data.jsx).
 *  Will be replaced by /api/schema-migrations/:id in Phase 5.
 */
import type { SchemaInfo, SchemaObject, MigrationEvent } from "./types";

export const schemaInfo: SchemaInfo = {
  id: "MIG-2041",
  name: "BILLING",
  source: { host: "ora-prod-01", version: "12.2.0.1", tns: "PROD.DB" },
  target: { host: "ora-rac-04",  version: "19.21",    tns: "BILL19.DB" },
  owner: "a.volkov",
  priority: "P0",
  status: "running",
  stage:  "bulk",
  startedAt: "2026-05-13 09:14",
  windowAt:  "2026-05-20 02:00",
  schemaCompat: 96.8,
  sizeGb: 1842,
  totals: { rowsPerSec: 184320, mbPerSec: 412 },
};

export const initialObjects: SchemaObject[] = [
  { id: "1",  type: "TABLE", name: "INVOICES",        rows: 184_200_000, rowsDone: 168_400_000, sizeMb: 312_400, status: "running", progress: 91.4, rowsPerSec:  84_120, mbPerSec: 184, compat: 100, warn: 0, err: 0, eta: "00:08:42", dur: "02:14:18", note: "" },
  { id: "2",  type: "TABLE", name: "PAYMENTS",        rows:  98_400_000, rowsDone:  98_400_000, sizeMb: 184_200, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:48:11", note: "" },
  { id: "3",  type: "TABLE", name: "LEDGER",          rows: 612_400_000, rowsDone: 384_120_000, sizeMb: 940_800, status: "running", progress: 62.7, rowsPerSec: 142_080, mbPerSec: 248, compat: 100, warn: 2, err: 0, eta: "01:18:04", dur: "01:32:55", note: "4 partitions reorg" },
  { id: "4",  type: "TABLE", name: "CUSTOMER",        rows:   8_240_000, rowsDone:   8_240_000, sizeMb:  18_400, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:04:32", note: "" },
  { id: "5",  type: "TABLE", name: "CUSTOMER_AUDIT",  rows: 412_800_000, rowsDone:           0, sizeMb: 184_200, status: "queued",  progress:   0,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "queued",   dur: "—",        note: "wait: LEDGER" },
  { id: "6",  type: "TABLE", name: "PRODUCT_CATALOG", rows: 184_000,     rowsDone: 184_000,     sizeMb: 412,     status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:00:11", note: "" },
  { id: "7",  type: "TABLE", name: "ORDER_LINES",     rows: 142_800_000, rowsDone: 142_800_000, sizeMb: 218_400, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 1, err: 0, eta: "—",        dur: "00:54:20", note: "LOB rebuild" },
  { id: "8",  type: "TABLE", name: "ORDERS",          rows:  24_800_000, rowsDone:  24_800_000, sizeMb:  38_400, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:10:18", note: "" },
  { id: "9",  type: "TABLE", name: "INVOICE_ITEMS",   rows: 412_000_000, rowsDone: 412_000_000, sizeMb: 612_400, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "03:28:42", note: "" },
  { id: "10", type: "TABLE", name: "REFUNDS",         rows:   1_800_000, rowsDone:           0, sizeMb:   4_120, status: "error",   progress: 18.4, rowsPerSec:       0, mbPerSec:   0, compat:  88, warn: 0, err: 1, eta: "—",        dur: "00:01:22", note: "ORA-01400: cannot insert NULL into REASON_CODE" },
  { id: "11", type: "TABLE", name: "TAX_RATES",       rows:       1_840, rowsDone:       1_840, sizeMb:     0.6, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:00:01", note: "" },
  { id: "12", type: "TABLE", name: "INVOICE_HISTORY", rows: 218_400_000, rowsDone:           0, sizeMb: 318_400, status: "queued",  progress:   0,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "queued",   dur: "—",        note: "" },
  { id: "13", type: "TABLE", name: "CUSTOMER_NOTES",  rows:  42_800_000, rowsDone:  42_800_000, sizeMb:  88_400, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 4, err: 0, eta: "—",        dur: "00:14:42", note: "CLOB columns" },
  { id: "14", type: "TABLE", name: "CURRENCY_CONV",   rows:      18_400, rowsDone:      18_400, sizeMb:       6, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:00:02", note: "" },
  { id: "15", type: "TABLE", name: "BILLING_RULES",   rows:         412, rowsDone:         412, sizeMb:     0.2, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:00:01", note: "" },
  { id: "16", type: "TABLE", name: "GEO_REGION",      rows:       2_840, rowsDone:       2_840, sizeMb:     1.4, status: "done",    progress: 100,  rowsPerSec:       0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:00:01", note: "" },

  { id: "20", type: "INDEX", name: "IDX_LEDGER_ACC_DT",          rows: null, rowsDone: null, sizeMb:  84_120, status: "queued",  progress:   0, rowsPerSec: 0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "queued",  dur: "—",        note: "after LEDGER" },
  { id: "21", type: "INDEX", name: "IDX_INVOICES_CUST_DT",       rows: null, rowsDone: null, sizeMb:  18_400, status: "running", progress: 42.0, rowsPerSec: 0, mbPerSec: 124, compat: 100, warn: 0, err: 0, eta: "00:04:21", dur: "00:02:08", note: "parallel 16" },
  { id: "22", type: "INDEX", name: "IDX_PAYMENTS_INV",           rows: null, rowsDone: null, sizeMb:   8_400, status: "done",    progress: 100, rowsPerSec: 0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:01:42", note: "" },
  { id: "23", type: "INDEX", name: "PK_INVOICES",                rows: null, rowsDone: null, sizeMb:   4_840, status: "done",    progress: 100, rowsPerSec: 0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:00:48", note: "" },
  { id: "24", type: "INDEX", name: "UK_CUSTOMER_EMAIL",          rows: null, rowsDone: null, sizeMb:     412, status: "done",    progress: 100, rowsPerSec: 0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:00:08", note: "" },
  { id: "25", type: "INDEX", name: "IDX_ORDERS_STATUS",          rows: null, rowsDone: null, sizeMb:   1_280, status: "done",    progress: 100, rowsPerSec: 0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:00:21", note: "" },
  { id: "26", type: "INDEX", name: "IDX_LEDGER_ENTRY_ID",        rows: null, rowsDone: null, sizeMb:  42_180, status: "queued",  progress:   0, rowsPerSec: 0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "queued",   dur: "—",        note: "after LEDGER" },
  { id: "27", type: "INDEX", name: "IDX_REFUNDS_INV",            rows: null, rowsDone: null, sizeMb:     184, status: "queued",  progress:   0, rowsPerSec: 0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "blocked",  dur: "—",        note: "wait: REFUNDS (error)" },
  { id: "28", type: "INDEX", name: "IDX_INVOICE_ITEMS_PROD",     rows: null, rowsDone: null, sizeMb:  18_400, status: "done",    progress: 100, rowsPerSec: 0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:03:48", note: "" },
  { id: "29", type: "INDEX", name: "IDX_CUSTOMER_NOTES_CREATED", rows: null, rowsDone: null, sizeMb:   2_840, status: "done",    progress: 100, rowsPerSec: 0, mbPerSec:   0, compat: 100, warn: 0, err: 0, eta: "—",        dur: "00:00:42", note: "" },

  { id: "30", type: "MVIEW", name: "MV_DAILY_REVENUE",     rows:     8_400, rowsDone:  8_400, sizeMb:  28, status: "done",   progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—",      dur: "00:00:12", note: "refresh: COMPLETE" },
  { id: "31", type: "MVIEW", name: "MV_CUSTOMER_BALANCE",  rows: 2_400_000, rowsDone:      0, sizeMb: 412, status: "queued", progress:   0, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "queued", dur: "—",       note: "after CUSTOMER" },

  { id: "40", type: "SEQUENCE", name: "SEQ_INVOICE_ID",   rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "lastVal: 184,201,422" },
  { id: "41", type: "SEQUENCE", name: "SEQ_PAYMENT_ID",   rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "lastVal: 98,401,221" },
  { id: "42", type: "SEQUENCE", name: "SEQ_REFUND_ID",    rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "43", type: "SEQUENCE", name: "SEQ_LEDGER_ENTRY", rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },

  { id: "50", type: "VIEW", name: "V_OPEN_INVOICES",       rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "51", type: "VIEW", name: "V_CUSTOMER_BALANCE",    rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "52", type: "VIEW", name: "V_PAYMENT_AUDIT",       rows: null, rowsDone: null, sizeMb: 0, status: "warn", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat:  92, warn: 1, err: 0, eta: "—", dur: "<1s", note: "deprecated CONNECT BY syntax — auto-rewritten" },
  { id: "53", type: "VIEW", name: "V_LEDGER_SUMMARY",      rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "54", type: "VIEW", name: "V_DAILY_INVOICE_STATS", rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },

  { id: "60", type: "PACKAGE", name: "PKG_INVOICE_API",   rows: null, rowsDone: null, sizeMb: 0, status: "done",  progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "142 lines, 12 procedures" },
  { id: "61", type: "PACKAGE", name: "PKG_BILLING_RULES", rows: null, rowsDone: null, sizeMb: 0, status: "warn",  progress: 100, rowsPerSec: 0, mbPerSec: 0, compat:  86, warn: 2, err: 0, eta: "—", dur: "<1s", note: "uses DBMS_OBFUSCATION_TOOLKIT (deprecated) → rewritten to DBMS_CRYPTO" },
  { id: "62", type: "PACKAGE", name: "PKG_PAYMENTS",      rows: null, rowsDone: null, sizeMb: 0, status: "done",  progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "63", type: "PACKAGE", name: "PKG_REPORTS",       rows: null, rowsDone: null, sizeMb: 0, status: "error", progress:  60, rowsPerSec: 0, mbPerSec: 0, compat:  74, warn: 0, err: 2, eta: "—", dur: "<1s", note: "PLS-00201: identifier UTL_FILE_DIR must be declared (removed in 19c)" },
  { id: "64", type: "PACKAGE", name: "PKG_LEDGER_TX",     rows: null, rowsDone: null, sizeMb: 0, status: "done",  progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },

  { id: "70", type: "PROCEDURE", name: "PR_CLOSE_PERIOD",        rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "71", type: "PROCEDURE", name: "PR_RECONCILE",           rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "72", type: "FUNCTION",  name: "FN_TAX_FOR",             rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "73", type: "FUNCTION",  name: "FN_OUTSTANDING_BALANCE", rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },

  { id: "80", type: "TRIGGER", name: "TRG_INVOICES_AUDIT", rows: null, rowsDone: null, sizeMb: 0, status: "done",   progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—",       dur: "<1s", note: "" },
  { id: "81", type: "TRIGGER", name: "TRG_LEDGER_AI",      rows: null, rowsDone: null, sizeMb: 0, status: "done",   progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—",       dur: "<1s", note: "" },
  { id: "82", type: "TRIGGER", name: "TRG_PAYMENTS_CHK",   rows: null, rowsDone: null, sizeMb: 0, status: "warn",   progress: 100, rowsPerSec: 0, mbPerSec: 0, compat:  94, warn: 1, err: 0, eta: "—",       dur: "<1s", note: "autonomous TX flagged for review" },
  { id: "83", type: "TRIGGER", name: "TRG_REFUNDS_AI",     rows: null, rowsDone: null, sizeMb: 0, status: "queued", progress:   0, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "blocked", dur: "—",   note: "wait: REFUNDS" },

  { id: "90", type: "TYPE", name: "T_INVOICE_LINE",    rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "91", type: "TYPE", name: "T_PAYMENT_DETAIL",  rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },

  { id: "100", type: "SYNONYM", name: "SYN_INVOICES_EXT",          rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "101", type: "SYNONYM", name: "SYN_LEDGER_RO",             rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "" },
  { id: "110", type: "GRANT",   name: "BILLING_RO → REPORTS_USER", rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "SELECT on 12 objects" },
  { id: "111", type: "GRANT",   name: "BILLING_RW → ETL_SVC",      rows: null, rowsDone: null, sizeMb: 0, status: "done", progress: 100, rowsPerSec: 0, mbPerSec: 0, compat: 100, warn: 0, err: 0, eta: "—", dur: "<1s", note: "SELECT,INSERT,UPDATE on 8 objects" },
];

export const initialEvents: MigrationEvent[] = [
  { t: "11:42:18", obj: "INVOICES",             level: "info",  msg: "Batch rows 12.4M committed (84k rows/s)" },
  { t: "11:42:11", obj: "LEDGER",               level: "info",  msg: "Partition LEDGER_2026_Q1 loaded" },
  { t: "11:41:58", obj: "REFUNDS",              level: "error", msg: "ORA-01400: cannot insert NULL into REASON_CODE (row 142,841)" },
  { t: "11:41:42", obj: "PKG_BILLING_RULES",    level: "warn",  msg: "DBMS_OBFUSCATION_TOOLKIT auto-rewritten to DBMS_CRYPTO" },
  { t: "11:41:31", obj: "IDX_INVOICES_CUST_DT", level: "info",  msg: "Building parallel 16, 42%" },
  { t: "11:41:12", obj: "PKG_REPORTS",          level: "error", msg: "PLS-00201: UTL_FILE_DIR must be declared (removed in 19c)" },
  { t: "11:40:55", obj: "CUSTOMER_NOTES",       level: "warn",  msg: "CLOB column NOTE_BODY may exceed 4000 chars after charset conversion" },
  { t: "11:40:38", obj: "PAYMENTS",             level: "info",  msg: "Hash compare OK (sha256: e4b1…)" },
  { t: "11:40:21", obj: "INVOICE_ITEMS",        level: "info",  msg: "Bulk load complete, 412M rows in 03:28:42" },
  { t: "11:40:02", obj: "TRG_PAYMENTS_CHK",     level: "warn",  msg: "Autonomous transaction flagged for manual review" },
  { t: "11:39:48", obj: "INVOICES",             level: "info",  msg: "CDC apply lag 1.8s (within SLA 10s)" },
  { t: "11:39:31", obj: "V_PAYMENT_AUDIT",      level: "warn",  msg: "CONNECT BY syntax auto-rewritten" },
];

export const initialMetrics = {
  sourceCpu:  0,
  network:    0,
  redoPerSec: 0,
  cdcLag:     0,
  cpuSpark:  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
  netSpark:  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
  redoSpark: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
  lagSpark:  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
  targetCpu:        0,
  targetNetwork:    0,
  targetRedoPerSec: 0,
  targetCpuSpark:   [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
  targetNetSpark:   [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
  targetRedoSpark:  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
};
