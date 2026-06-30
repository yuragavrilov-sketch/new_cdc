export interface TopicCount {
  topic_name: string;
  count:      number;
  exists:     boolean;
}

export interface GroupTable {
  id:                 string;
  source_schema:      string;
  source_table:       string;
  target_schema:      string;
  target_table:       string;
  effective_key_type: string;
  topic_name:         string;
}

export interface GroupHistoryEntry {
  from_status: string | null;
  to_status:   string;
  message:     string | null;
  created_at:  string;
}
