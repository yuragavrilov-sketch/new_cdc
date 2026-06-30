from __future__ import annotations

from services import kafka_topics


def test_kafka_topic_admin_uses_supported_timeout_configs(monkeypatch):
    captured = {}

    class FakeKafkaAdminClient:
        def __init__(self, **configs):
            captured.update(configs)

    import kafka.admin

    monkeypatch.setattr(kafka.admin, "KafkaAdminClient", FakeKafkaAdminClient)

    assert isinstance(
        kafka_topics._new_admin_client(["broker:9092"]),
        FakeKafkaAdminClient,
    )
    assert captured == {
        "bootstrap_servers": ["broker:9092"],
        "request_timeout_ms": 5000,
        "connections_max_idle_ms": 8000,
    }
