export interface ColInfo {
  name:           string;
  data_type:      string;
  data_length:    number | null;
  data_precision: number | null;
  data_scale:     number | null;
  nullable:       boolean;
  data_default:   string | null;
  column_id:      number;
}

export interface Constraint {
  name:      string;
  type:      string;
  type_code: string;
  status:    string;
  columns:   string[];
}

export interface OraIndex {
  name:       string;
  unique:     boolean;
  index_type: string;
  status:     string;
  columns:    string[];
}

export interface Trigger {
  name:         string;
  trigger_type: string;
  event:        string;
  status:       string;
}

export interface TableDDL {
  schema:      string;
  table:       string;
  columns:     ColInfo[];
  constraints: Constraint[];
  indexes:     OraIndex[];
  triggers:    Trigger[];
}

export interface DDLData {
  source: TableDDL;
  target: TableDDL;
}

export interface DiffSummary {
  ok:            boolean;
  total:         number;
  cols_missing:  number;
  cols_extra:    number;
  cols_type:     number;
  idx_missing:   number;
  idx_disabled:  number;
  con_missing:   number;
  con_disabled:  number;
  trg_missing:   number;
}

export interface PairStatus {
  comparing:  boolean;
  compared:   boolean;
  error:      string | null;
  diff:       DiffSummary | null;
  ddl:        DDLData | null;
  ddlLoading: boolean;
  syncing:    boolean;
  syncError:  string | null;
}

export type FilterMode = "all" | "diff" | "ok" | "error" | "no_target";

export const DEFAULT_STATUS: PairStatus = {
  comparing: false, compared: false, error: null, diff: null,
  ddl: null, ddlLoading: false, syncing: false, syncError: null,
};
