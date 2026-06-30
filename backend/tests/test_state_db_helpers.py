from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from db import state_db


class CursorStub:
    description = (("created_at",), ("amount",), ("name",))


def test_masked_dsn_hides_password():
    dsn = "postgresql://user:secret@localhost:5432/db"

    assert state_db._masked_dsn(dsn) == "postgresql://user:***@localhost:5432/db"


def test_row_to_dict_serializes_datetime_and_decimal():
    row = (
        datetime(2026, 6, 22, 12, 0, tzinfo=timezone.utc),
        Decimal("12.50"),
        "migration",
    )

    result = state_db.row_to_dict(CursorStub(), row)

    assert result == {
        "created_at": "2026-06-22T12:00:00Z",
        "amount": "12.50",
        "name": "migration",
    }
