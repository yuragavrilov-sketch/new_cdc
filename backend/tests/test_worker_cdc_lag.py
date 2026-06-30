from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace


WORKERS_DIR = Path(__file__).resolve().parents[2] / "workers"
if str(WORKERS_DIR) not in sys.path:
    sys.path.insert(0, str(WORKERS_DIR))

import worker  # noqa: E402


@dataclass(frozen=True)
class TP:
    topic: str
    partition: int


class FakeAdmin:
    offsets: dict = {}
    configs: dict = {}

    def __init__(self, **configs):
        type(self).configs = configs

    def list_consumer_group_offsets(self, _consumer_group):
        return self.offsets

    def close(self):
        pass


class FakeConsumer:
    def __init__(self, *, assignment=(), end=None, beginning=None, positions=None):
        self._assignment = set(assignment)
        self._end = end or {}
        self._beginning = beginning or {}
        self._positions = positions or {}

    def assignment(self):
        return self._assignment

    def end_offsets(self, partitions):
        return {tp: self._end[tp] for tp in partitions}

    def beginning_offsets(self, partitions):
        return {tp: self._beginning.get(tp, 0) for tp in partitions}

    def position(self, tp):
        if tp not in self._positions:
            raise RuntimeError("position not initialized")
        return self._positions[tp]


def test_cdc_lag_uses_assigned_partition_when_group_has_no_committed_offsets(monkeypatch):
    import kafka

    tp = TP("cdc.TCBPAY.ALLORDERS", 0)
    FakeAdmin.offsets = {}
    monkeypatch.setattr(kafka, "KafkaAdminClient", FakeAdmin)

    total, by_partition = worker._calc_lag(
        FakeConsumer(
            assignment={tp},
            end={tp: 7},
            beginning={tp: 0},
        ),
        "group-1",
        ["broker:9092"],
    )

    assert total == 7
    assert by_partition == {"cdc.TCBPAY.ALLORDERS-0": 7}
    assert FakeAdmin.configs == {
        "bootstrap_servers": ["broker:9092"],
        "request_timeout_ms": 5000,
        "connections_max_idle_ms": 8000,
    }


def test_cdc_lag_prefers_committed_offsets(monkeypatch):
    import kafka

    tp = TP("cdc.TCBPAY.ALLORDERS", 0)
    FakeAdmin.offsets = {tp: SimpleNamespace(offset=4)}
    monkeypatch.setattr(kafka, "KafkaAdminClient", FakeAdmin)

    total, by_partition = worker._calc_lag(
        FakeConsumer(end={tp: 10}, positions={tp: 9}),
        "group-1",
        ["broker:9092"],
    )

    assert total == 6
    assert by_partition == {"cdc.TCBPAY.ALLORDERS-0": 6}
