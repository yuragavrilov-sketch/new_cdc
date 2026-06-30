from __future__ import annotations

import pytest

from services.strategy import Strategy


@pytest.mark.parametrize(
    ("raw", "expected", "has_cdc", "uses_stage"),
    [
        ("CDC_STAGE", Strategy.CDC_STAGE, True, True),
        ("cdc_direct", Strategy.CDC_DIRECT, True, False),
        (" BULK_STAGE ", Strategy.BULK_STAGE, False, True),
        ("bulk_direct", Strategy.BULK_DIRECT, False, False),
    ],
)
def test_strategy_parse_and_flags(raw, expected, has_cdc, uses_stage):
    strategy = Strategy.parse(raw)

    assert strategy is expected
    assert strategy.has_cdc is has_cdc
    assert strategy.uses_stage is uses_stage


@pytest.mark.parametrize("raw", [None, "", "unknown"])
def test_strategy_parse_rejects_invalid_values(raw):
    with pytest.raises(ValueError):
        Strategy.parse(raw)
