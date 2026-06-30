import React from "react";
import type { GroupStatus } from "../../types/migration";
import { t } from "../../theme";

export const STATUS_COLORS: Record<GroupStatus, { bg: string; text: string }> = {
  PENDING:            { bg: t.bg.s2,    text: t.text.secondary },
  TOPICS_CREATING:    { bg: t.bg.s3,    text: t.blue.fg },
  CONNECTOR_STARTING: { bg: t.bg.s3,    text: t.blue.fg },
  RUNNING:            { bg: t.green.bg, text: t.green.fg },
  STOPPING:           { bg: t.red.bg,  text: t.amber.fg },
  STOPPED:            { bg: t.bg.s2,  text: t.text.muted },
  FAILED:             { bg: t.red.bg,   text: t.red.fg },
};

export function actionBtn(bg: string, border: string): React.CSSProperties {
  return {
    background:   bg,
    border:       `1px solid ${border}`,
    borderRadius: t.radius.sm,
    color:        border,
    padding:      "2px 10px",
    fontSize:     t.size.sm,
    cursor:       "pointer",
  };
}
