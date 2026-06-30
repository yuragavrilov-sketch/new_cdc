from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from services import checkers, kafka_lag


def test_check_kafka_uses_readonly_consumer(monkeypatch):
    captured = {}

    class FakeKafkaConsumer:
        def __init__(self, **configs):
            captured.update(configs)

        def topics(self):
            return {"topic-1"}

        def close(self):
            pass

    import kafka

    monkeypatch.setattr(kafka, "KafkaConsumer", FakeKafkaConsumer)

    status, message = checkers.check_kafka({"bootstrap_servers": "broker:9092"})

    assert (status, message) == ("up", "Connected")
    assert captured == {
        "bootstrap_servers": ["broker:9092"],
        "request_timeout_ms": 5000,
        "connections_max_idle_ms": 8000,
        "enable_auto_commit": False,
    }


@dataclass(frozen=True)
class TP:
    topic: str
    partition: int


def test_consumer_group_lag_uses_readonly_consumer(monkeypatch):
    admin_configs = {}
    consumer_configs = {}
    tp = TP("cdc.TCBPAY.ALLORDERS", 0)

    class FakeAdmin:
        def __init__(self, **configs):
            admin_configs.update(configs)

        def list_consumer_group_offsets(self, _consumer_group):
            return {tp: SimpleNamespace(offset=4)}

        def close(self):
            pass

    class FakeConsumer:
        def __init__(self, **configs):
            consumer_configs.update(configs)

        def end_offsets(self, partitions):
            return {partitions[0]: 9}

        def close(self):
            pass

    import kafka

    monkeypatch.setattr(kafka, "KafkaAdminClient", FakeAdmin)
    monkeypatch.setattr(kafka, "KafkaConsumer", FakeConsumer)

    result = kafka_lag.get_consumer_group_lag("broker:9092", "group-1")

    assert result == {"total_lag": 5, "by_partition": {"cdc.TCBPAY.ALLORDERS-0": 5}}
    assert admin_configs == {
        "bootstrap_servers": ["broker:9092"],
        "request_timeout_ms": 10_000,
    }
    assert consumer_configs == {
        "bootstrap_servers": ["broker:9092"],
        "request_timeout_ms": 10_000,
        "connections_max_idle_ms": 15_000,
        "enable_auto_commit": False,
    }
