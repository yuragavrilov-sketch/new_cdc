export interface CompareTask {
  task_id:        string;
  source_schema:  string;
  source_table:   string;
  target_schema:  string;
  target_table:   string;
  compare_mode:   "full" | "last_n";
  last_n:         number | null;
  order_column:   string | null;
  status:         "PENDING" | "RUNNING" | "DONE" | "FAILED" | "CHUNKING";
  source_count:   number | null;
  target_count:   number | null;
  source_hash:    string | null;
  target_hash:    string | null;
  counts_match:   boolean | null;
  hash_match:     boolean | null;
  chunks_total:   number;
  chunks_done:    number;
  error_text:     string | null;
  started_at:     string | null;
  completed_at:   string | null;
  created_at:     string;
}

export interface ColInfo {
  name:     string;
  type:     string;
  nullable: boolean;
}

export interface ColDiff {
  column:      string;
  data_type:   string;
  source_hash: string | null;
  target_hash: string | null;
  match:       boolean;
}
