import React from "react";
import { S } from "./styles";

interface Props {
  objectType: string;
  objectName: string;
  matchStatus: string;
  syncBusy: boolean;
  onCompare: (type: string, name: string) => void;
  onSync: (type: string, name: string, action: string) => void;
  onShowDetail: (name: string) => void;
}

export function ObjectActions({
  objectType, objectName, matchStatus, syncBusy,
  onCompare, onSync, onShowDetail,
}: Props) {
  const btnSmall = { fontSize: 10, padding: "2px 8px" };
  const busy = syncBusy;

  return (
    <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
      <button
        onClick={() => onShowDetail(objectName)}
        style={{ ...S.btnSecondary, ...btnSmall }}
      >
        Детали
      </button>
      <button
        onClick={() => onCompare(objectType, objectName)}
        disabled={busy}
        style={{ ...S.btnSecondary, ...btnSmall, opacity: busy ? 0.5 : 1 }}
      >
        {busy ? "..." : "Сравнить"}
      </button>

      {matchStatus === "MISSING" && (
        <button
          onClick={() => onSync(objectType, objectName, "create")}
          disabled={busy}
          style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
        >
          {busy ? "..." : "Создать"}
        </button>
      )}

      {matchStatus === "DIFF" && objectType === "TABLE" && (
        <>
          <button
            onClick={() => onSync(objectType, objectName, "sync_cols")}
            disabled={busy}
            style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
          >
            Колонки
          </button>
          <button
            onClick={() => onSync(objectType, objectName, "sync_objects")}
            disabled={busy}
            style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
          >
            Объекты
          </button>
        </>
      )}

      {matchStatus === "DIFF" && objectType !== "TABLE" && objectType !== "SEQUENCE" && (
        <button
          onClick={() => onSync(objectType, objectName, "create")}
          disabled={busy}
          style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
        >
          {busy ? "..." : "Синхронизировать"}
        </button>
      )}

      {matchStatus === "DIFF" && objectType === "SEQUENCE" && (
        <button
          onClick={() => onSync(objectType, objectName, "sync")}
          disabled={busy}
          style={{ ...S.btnSuccess, ...btnSmall, opacity: busy ? 0.5 : 1 }}
        >
          Обновить
        </button>
      )}
    </div>
  );
}
