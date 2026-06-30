"""Kafka consumer-group lag measurement via KafkaAdminClient."""

from typing import Optional


def get_consumer_group_lag(
    bootstrap_servers: str,
    consumer_group: str,
    topic: Optional[str] = None,
) -> dict:
    """
    Return consumer-group lag for *consumer_group*.

    Result:
        {
            "total_lag": <int>,
            "by_partition": {"<topic>-<partition>": <lag>, ...},
        }

    *topic* is optional: if given, only partitions of that topic are included.
    """
    try:
        from kafka import KafkaAdminClient, KafkaConsumer
        from kafka.structs import TopicPartition
    except ImportError:
        raise ImportError("kafka-python не установлен (pip install kafka-python)")

    servers = [s.strip() for s in bootstrap_servers.split(",")]

    admin = KafkaAdminClient(
        bootstrap_servers=servers,
        request_timeout_ms=10_000,
    )
    consumer = KafkaConsumer(
        bootstrap_servers=servers,
        request_timeout_ms=10_000,
        connections_max_idle_ms=15_000,
        enable_auto_commit=False,
    )

    try:
        # Committed offsets per partition for the consumer group
        committed: dict[TopicPartition, int] = {}
        offsets_response = admin.list_consumer_group_offsets(consumer_group)
        for tp, offset_meta in offsets_response.items():
            if topic and tp.topic != topic:
                continue
            if offset_meta.offset >= 0:
                committed[tp] = offset_meta.offset

        if not committed:
            return {"total_lag": 0, "by_partition": {}}

        # End offsets (high watermarks)
        end_offsets = consumer.end_offsets(list(committed.keys()))

        total_lag = 0
        by_partition: dict[str, int] = {}
        for tp, committed_offset in committed.items():
            end = end_offsets.get(tp, committed_offset)
            lag = max(0, end - committed_offset)
            total_lag += lag
            by_partition[f"{tp.topic}-{tp.partition}"] = lag

        return {"total_lag": total_lag, "by_partition": by_partition}

    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            admin.close()
        except Exception:
            pass
