export function cdcRuntimeStatusLabel(status: string | null | undefined) {
  switch (String(status || "").toUpperCase()) {
    case "RUNNING": return "работает";
    case "FAILED": return "ошибка";
    case "STOPPED": return "остановлена";
    case "STOPPING": return "останавливается";
    case "STARTING": return "запускается";
    case "CONNECTOR_STARTING": return "запускается";
    case "TOPICS_CREATING": return "создаёт topics";
    case "NOT_FOUND": return "Debezium runtime не создан";
    case "MISSING": return "Debezium runtime не найден";
    case "PENDING": return "ожидает запуска";
    case "NEW": return "новая";
    case "": return "неизвестно";
    default: return String(status);
  }
}

export function packQueueStatusLabel(status: string | null | undefined) {
  switch (String(status || "").toUpperCase()) {
    case "READY": return "готова";
    case "RUNNING": return "в работе";
    case "DONE": return "готово";
    case "FAILED": return "ошибка";
    case "CANCELLED": return "отменена";
    case "PAUSED": return "пауза";
    case "DRAFT": return "черновик";
    case "PENDING": return "в очереди";
    case "": return "не создана";
    default: return String(status);
  }
}
