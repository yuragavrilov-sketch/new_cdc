from __future__ import annotations

from services import services_metrics


def test_kafka_metrics_uses_kafka_python_supported_configs(monkeypatch):
    captured = {}

    from db import state_db
    import kafka.admin

    monkeypatch.setattr(
        state_db,
        "load_configs",
        lambda _db_available: {
            "kafka": {"bootstrap_servers": "broker-1:9092,broker-2:9092"}
        },
    )

    class FakeKafkaAdminClient:
        def __init__(self, **configs):
            captured.update(configs)

        def describe_cluster(self):
            return {
                "brokers": [{"node_id": 1}, {"node_id": 2}],
                "cluster_id": "cluster-a",
                "controller_id": 1,
            }

        def list_topics(self):
            return ["topic-a", "topic-b"]

        def close(self):
            pass

    monkeypatch.setattr(kafka.admin, "KafkaAdminClient", FakeKafkaAdminClient)

    result = services_metrics._kafka_metrics()

    assert result["ok"] is True
    assert captured == {
        "bootstrap_servers": ["broker-1:9092", "broker-2:9092"],
        "request_timeout_ms": 5000,
    }
