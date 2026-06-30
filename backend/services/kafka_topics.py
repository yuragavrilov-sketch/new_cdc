"""Kafka topic management via kafka-python AdminClient."""

import os


def _bootstrap_servers() -> list[str]:
    """Read bootstrap servers from environment or default."""
    raw = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return [s.strip() for s in raw.split(",")]


def _new_admin_client(bootstrap_servers: list[str]):
    from kafka.admin import KafkaAdminClient

    return KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        request_timeout_ms=5000,
        connections_max_idle_ms=8000,
    )


def create_topic(
    bootstrap_servers: list[str] | None = None,
    topic_name: str = "",
    num_partitions: int | None = None,
    replication_factor: int | None = None,
) -> None:
    """Pre-create a Kafka topic.  Idempotent — no error if already exists."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    if not topic_name:
        raise ValueError("topic_name is required")

    servers = bootstrap_servers or _bootstrap_servers()
    partitions = num_partitions or int(os.environ.get("DEBEZIUM_TOPIC_PARTITIONS", "1"))
    replication = replication_factor or int(os.environ.get("DEBEZIUM_TOPIC_REPLICATION_FACTOR", "1"))

    admin = _new_admin_client(servers)
    try:
        admin.create_topics([
            NewTopic(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=replication,
            )
        ])
        print(f"[kafka_topics] created topic {topic_name}")
    except TopicAlreadyExistsError:
        print(f"[kafka_topics] topic {topic_name} already exists")
    finally:
        admin.close()


def topic_exists(
    bootstrap_servers: list[str] | None = None,
    topic_name: str = "",
) -> bool:
    """Check if a topic already exists."""
    servers = bootstrap_servers or _bootstrap_servers()
    admin = _new_admin_client(servers)
    try:
        topics = admin.list_topics()
        return topic_name in topics
    finally:
        admin.close()


def delete_topic(
    bootstrap_servers: list[str] | None = None,
    topic_name: str = "",
) -> None:
    """Delete a Kafka topic.  No error if it does not exist."""
    from kafka.errors import UnknownTopicOrPartitionError

    servers = bootstrap_servers or _bootstrap_servers()
    admin = _new_admin_client(servers)
    try:
        admin.delete_topics([topic_name])
        print(f"[kafka_topics] deleted topic {topic_name}")
    except UnknownTopicOrPartitionError:
        pass
    finally:
        admin.close()
