"""Migration strategy enum — one field replaces (migration_mode, migration_strategy).

CDC_*  → миграция продолжает реплицировать source через CDC после bulk-load.
BULK_* → один разовый перенос данных без CDC, завершается после DATA_VERIFYING.
*_STAGE  → грузим через промежуточную stage-таблицу (валидация + TRUNCATE + baseline).
*_DIRECT → грузим сразу в target (быстрее, без валидации/baseline).
"""

from enum import Enum


class Strategy(str, Enum):
    CDC_STAGE   = "CDC_STAGE"
    CDC_DIRECT  = "CDC_DIRECT"
    BULK_STAGE  = "BULK_STAGE"
    BULK_DIRECT = "BULK_DIRECT"

    @property
    def has_cdc(self) -> bool:
        return self.value.startswith("CDC_")

    @property
    def uses_stage(self) -> bool:
        return self.value.endswith("_STAGE")

    @property
    def forces_target_truncate(self) -> bool:
        return self is Strategy.CDC_DIRECT

    @classmethod
    def parse(cls, raw: str | None) -> "Strategy":
        """Strict parser: raises ValueError on unknown/empty value.

        Используется в API-валидации и при чтении из БД (где CHECK
        constraint гарантирует одно из 4 значений, но мы всё равно
        не хотим silent fallback).
        """
        if not raw:
            raise ValueError("strategy is required")
        return cls(raw.strip().upper())
