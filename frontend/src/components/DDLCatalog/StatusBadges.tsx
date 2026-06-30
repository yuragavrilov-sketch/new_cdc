import React from "react";
import { S } from "./styles";
import { t } from "../../theme";

export function MatchBadge({ status }: { status: string }) {
  switch (status) {
    case "MATCH":
      return <span style={S.badge(`${t.green.base}22`, t.green.base)}>Совпадает</span>;
    case "DIFF":
      return <span style={S.badge(`${t.amber.base}22`, t.amber.base)}>Отличается</span>;
    case "MISSING":
      return <span style={S.badge(`${t.red.base}22`, t.red.base)}>Нет на таргете</span>;
    case "EXTRA":
      return <span style={S.badge(`${t.purple.base}22`, t.purple.base)}>Лишний</span>;
    default:
      return <span style={S.badge(`${t.border.base}22`, t.text.disabled)}>Не проверено</span>;
  }
}

export function MigrationBadge({ status }: { status: string }) {
  switch (status) {
    case "PLANNED":
      return <span style={S.badge(`${t.blue.base}22`, t.blue.base)}>В пачке</span>;
    case "IN_PROGRESS":
      return <span style={S.badge(`${t.amber.base}22`, t.amber.base)}>В работе</span>;
    case "COMPLETED":
      return <span style={S.badge(`${t.green.base}22`, t.green.base)}>Завершена</span>;
    case "FAILED":
      return <span style={S.badge(`${t.red.base}22`, t.red.base)}>Ошибка</span>;
    default:
      return <span style={S.badge(`${t.border.base}22`, t.text.disabled)}>Нет</span>;
  }
}
