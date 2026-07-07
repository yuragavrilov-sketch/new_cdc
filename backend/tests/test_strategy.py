from __future__ import annotations

import pytest

from services.strategy import Strategy


@pytest.mark.parametrize(
    ("raw", "expected", "has_cdc", "uses_stage", "forces_target_truncate"),
    [
        ("CDC_STAGE", Strategy.CDC_STAGE, True, True, False),
        ("cdc_direct", Strategy.CDC_DIRECT, True, False, True),
        (" BULK_STAGE ", Strategy.BULK_STAGE, False, True, False),
        ("bulk_direct", Strategy.BULK_DIRECT, False, False, False),
    ],
)
def test_strategy_parse_and_flags(raw, expected, has_cdc, uses_stage, forces_target_truncate):
    strategy = Strategy.parse(raw)

    assert strategy is expected
    assert strategy.has_cdc is has_cdc
    assert strategy.uses_stage is uses_stage
    assert strategy.forces_target_truncate is forces_target_truncate


@pytest.mark.parametrize("raw", [None, "", "unknown"])
def test_strategy_parse_rejects_invalid_values(raw):
    with pytest.raises(ValueError):
        Strategy.parse(raw)
