"""Kafka helper: shared producer/consumer configuration + topic admin."""

from __future__ import annotations

from typing import Any

from config.settings import settings


def base_conf() -> dict[str, Any]:
    """Common Kafka client config, adding SASL only when configured (cloud)."""
    conf: dict[str, Any] = {"bootstrap.servers": settings.kafka_bootstrap_servers}
    if settings.kafka_security_protocol:
        conf["security.protocol"] = settings.kafka_security_protocol
        conf["sasl.mechanism"] = settings.kafka_sasl_mechanism
        conf["sasl.username"] = settings.kafka_sasl_username
        conf["sasl.password"] = settings.kafka_sasl_password
    return conf


def producer_conf() -> dict[str, Any]:
    conf = base_conf()
    conf.update({"acks": "all", "enable.idempotence": True, "linger.ms": 20})
    return conf


def consumer_conf(group: str | None = None) -> dict[str, Any]:
    conf = base_conf()
    conf.update(
        {
            "group.id": group or settings.kafka_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    return conf
