"""The hot-path stream job: Kafka -> features -> graph -> score -> Mongo + lake.

Design guarantees:
* At-least-once ingest with manual offset commits AFTER a successful write.
* Idempotent Mongo upserts keyed on txn_id -> safe to replay Kafka.
* Poison messages are routed to a dead-letter topic (never crash the consumer).
* The LLM is NEVER called here (hot path stays deterministic and fast).
"""

from __future__ import annotations

import json
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.producer.kafka_utils import consumer_conf, producer_conf
from fraud.schemas import (
    Alert,
    CaseStatus,
    Features,
    GraphInfo,
    ScoredTransaction,
    TransactionEvent,
    UserRollingState,
)
from fraud.serving import mongo
from fraud.stream.features import compute_features, empty_state
from fraud.stream.graph_features import enrich_with_graph
from fraud.stream.scorer import get_scorer

log = get_logger("stream")

_running = True
_latencies: list[float] = []


def _handle_sigterm(*_: Any) -> None:
    global _running
    _running = False


def _load_state(user_id: str) -> UserRollingState:
    doc = mongo.get_user_state(user_id)
    if doc is None:
        return empty_state(user_id)
    try:
        return UserRollingState(**doc)
    except Exception:  # noqa: BLE001
        return empty_state(user_id)


def _write_bronze(event: TransactionEvent) -> None:
    """Append raw event to the bronze lakehouse layer (Parquet-friendly JSONL)."""
    settings.ensure_dirs()
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = Path(settings.bronze_dir) / f"raw-{day}.jsonl"
    with path.open("a", encoding="utf-8") as fh:
        fh.write(event.model_dump_json() + "\n")


def process_event(event: TransactionEvent) -> ScoredTransaction:
    """Pure-ish per-event pipeline: features -> graph -> score -> persist."""
    state = _load_state(event.user_id)
    features, new_state = compute_features(event, state)
    features = enrich_with_graph(features, event.user_id)

    scorer = get_scorer()
    score, band = scorer.score(features)

    shap_top: list[tuple[str, float]] = []
    if band != band.LOW:
        try:
            from fraud.ml.explain import top_features

            shap_top = top_features(scorer.model, features.to_vector())
        except Exception as exc:  # noqa: BLE001
            log.warning("shap_failed", error=str(exc))

    scored = ScoredTransaction(
        _id=event.txn_id,
        user_id=event.user_id,
        amount=event.amount,
        currency=event.currency,
        merchant=event.merchant,
        risk_score=round(score, 4),
        risk_band=band,
        features=features.model_dump(),
        graph=GraphInfo(
            ring_id=features.ring_id,
            shared_device_flags=features.shared_device_flags,
            component_size=features.component_size,
        ),
        shap_top=shap_top,
        geo=event.geo,
        device_id=event.device_id,
        model_version=scorer.version,
    )

    mongo.upsert_scored(scored)
    mongo.upsert_user_state(new_state)
    if band != band.LOW:
        mongo.upsert_alert(
            Alert(
                _id=event.txn_id,
                txn_id=event.txn_id,
                user_id=event.user_id,
                risk_band=band,
                expected_loss=scored.expected_loss,
                status=CaseStatus.OPEN,
                priority=2 if band == band.HIGH else 1,
            )
        )
    _write_bronze(event)
    return scored


def _to_dlq(producer: Producer, raw: bytes, error: str) -> None:
    producer.produce(
        settings.kafka_dlq_topic,
        value=raw,
        headers=[("error", error.encode()[:200])],
    )
    producer.poll(0)


def run() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    mongo.init_indexes()
    consumer = Consumer(consumer_conf())
    dlq = Producer(producer_conf())
    consumer.subscribe([settings.kafka_raw_topic])
    log.info("stream_started", topic=settings.kafka_raw_topic, group=settings.kafka_consumer_group)

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            raw = msg.value()
            start = time.perf_counter()
            try:
                event = TransactionEvent(**json.loads(raw))
            except Exception as exc:  # noqa: BLE001 - poison message
                log.warning("poison_message_to_dlq", error=str(exc))
                _to_dlq(dlq, raw, str(exc))
                consumer.commit(msg)
                continue

            try:
                scored = process_event(event)
            except Exception as exc:  # noqa: BLE001
                log.error("processing_failed", txn=event.txn_id, error=str(exc))
                _to_dlq(dlq, raw, str(exc))
                consumer.commit(msg)  # avoid poison-loop; raw is preserved in DLQ
                continue

            # commit ONLY after a successful, idempotent write (at-least-once).
            consumer.commit(msg)
            latency = (time.perf_counter() - start) * 1000
            _record_latency(latency)
            if scored.risk_band != scored.risk_band.LOW:
                log.info(
                    "flagged",
                    txn=scored.id,
                    band=scored.risk_band.value,
                    score=scored.risk_score,
                    latency_ms=round(latency, 1),
                )
    finally:
        consumer.close()
        dlq.flush()
        log.info("stream_stopped")


def _record_latency(ms: float) -> None:
    _latencies.append(ms)
    if len(_latencies) > 5000:
        del _latencies[:1000]


if __name__ == "__main__":
    run()
