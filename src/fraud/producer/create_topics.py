"""Create Kafka topics needed by the platform (idempotent)."""

from __future__ import annotations

from confluent_kafka.admin import AdminClient, NewTopic

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.producer.kafka_utils import base_conf

log = get_logger("create_topics")


def create_topics() -> None:
    admin = AdminClient(base_conf())
    topics = [
        NewTopic(settings.kafka_raw_topic, num_partitions=6, replication_factor=1),
        NewTopic(settings.kafka_dlq_topic, num_partitions=1, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for name, fut in futures.items():
        try:
            fut.result()
            log.info("topic_created", topic=name)
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                log.info("topic_exists", topic=name)
            else:
                log.error("topic_error", topic=name, error=str(exc))


if __name__ == "__main__":
    create_topics()
