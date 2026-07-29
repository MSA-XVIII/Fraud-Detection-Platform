"""Synthetic transaction + attack generator that publishes to Kafka.

Emits a steady stream of realistic transactions with a controllable fraud ratio
(~2%), plus injectable attack patterns (card-testing bursts, geo-impossible
travel, velocity spikes). The dashboard "attack injector" drops a signal file
that this producer polls, so demos can trigger detections live.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from faker import Faker

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.producer.kafka_utils import producer_conf
from fraud.schemas import Geo, TransactionEvent

log = get_logger("generator")
fake = Faker()

ATTACK_SIGNAL = Path(settings.data_dir) / "attack_signal.json"

MERCHANTS = [
    ("Amazon", "ecommerce"),
    ("Flipkart", "ecommerce"),
    ("Uber", "transport"),
    ("Swiggy", "food"),
    ("BigBazaar", "grocery"),
    ("Apple Store", "electronics"),
    ("Steam", "gaming"),
    ("Netflix", "subscription"),
]

CITIES = [
    ("IN", 19.07, 72.87),  # Mumbai
    ("IN", 28.61, 77.20),  # Delhi
    ("IN", 12.97, 77.59),  # Bengaluru
    ("US", 40.71, -74.00),  # New York
    ("GB", 51.50, -0.12),  # London
    ("SG", 1.35, 103.81),  # Singapore
]


def _user_pool(n: int = 500) -> list[str]:
    return [f"user_{i:04d}" for i in range(n)]


USERS = _user_pool()
DEVICES = {u: f"dev_{random.randint(1000, 9999)}" for u in USERS}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normal_txn() -> TransactionEvent:
    user = random.choice(USERS)
    merchant, cat = random.choice(MERCHANTS)
    country, lat, lon = random.choice(CITIES[:3])  # normal = domestic
    return TransactionEvent(
        txn_id=str(uuid.uuid4()),
        user_id=user,
        amount=round(random.gauss(1500, 700), 2),
        currency="INR",
        merchant=merchant,
        merchant_category=cat,
        geo=Geo(lat=lat, lon=lon, country=country),
        device_id=DEVICES[user],
        ip=fake.ipv4_public(),
        timestamp=_now_iso(),
    )


def card_testing_burst(user: str | None = None) -> list[TransactionEvent]:
    """Many tiny transactions in seconds — classic card testing."""
    user = user or random.choice(USERS)
    merchant, cat = random.choice(MERCHANTS)
    country, lat, lon = CITIES[0]
    burst = []
    for _ in range(random.randint(5, 12)):
        burst.append(
            TransactionEvent(
                txn_id=str(uuid.uuid4()),
                user_id=user,
                amount=round(random.uniform(1, 25), 2),
                currency="INR",
                merchant=merchant,
                merchant_category=cat,
                geo=Geo(lat=lat, lon=lon, country=country),
                device_id=f"dev_{random.randint(1000, 9999)}",
                ip=fake.ipv4_public(),
                timestamp=_now_iso(),
            )
        )
    return burst


def geo_impossible_travel(user: str | None = None) -> list[TransactionEvent]:
    """Two txns from far-apart geos within seconds."""
    user = user or random.choice(USERS)
    merchant, cat = random.choice(MERCHANTS)
    a = CITIES[0]
    b = CITIES[3]
    out = []
    for country, lat, lon in (a, b):
        out.append(
            TransactionEvent(
                txn_id=str(uuid.uuid4()),
                user_id=user,
                amount=round(random.uniform(5000, 40000), 2),
                currency="INR",
                merchant=merchant,
                merchant_category=cat,
                geo=Geo(lat=lat, lon=lon, country=country),
                device_id=DEVICES[user],
                ip=fake.ipv4_public(),
                timestamp=_now_iso(),
            )
        )
    return out


def _delivery(err, msg) -> None:  # noqa: ANN001
    if err is not None:
        log.error("delivery_failed", error=str(err))


def _produce(producer: Producer, ev: TransactionEvent) -> None:
    producer.produce(
        settings.kafka_raw_topic,
        key=ev.user_id.encode(),
        value=ev.model_dump_json().encode(),
        on_delivery=_delivery,
    )


def _check_attack_signal(producer: Producer) -> None:
    if not ATTACK_SIGNAL.exists():
        return
    try:
        payload = json.loads(ATTACK_SIGNAL.read_text())
        kind = payload.get("attack", "card_testing")
    except Exception:  # noqa: BLE001
        kind = "card_testing"
    ATTACK_SIGNAL.unlink(missing_ok=True)
    if kind == "geo_impossible":
        events = geo_impossible_travel()
    else:
        events = card_testing_burst()
    for ev in events:
        _produce(producer, ev)
    producer.flush()
    log.info("attack_injected", kind=kind, count=len(events))


def inject_attack(kind: str = "card_testing") -> None:
    """Drop a signal file the running producer polls (used by the dashboard)."""
    settings.ensure_dirs()
    ATTACK_SIGNAL.write_text(json.dumps({"attack": kind, "ts": _now_iso()}))


def run(rate_per_sec: float = 20.0, fraud_ratio: float = 0.02) -> None:
    settings.ensure_dirs()
    producer = Producer(producer_conf())
    log.info("producer_started", topic=settings.kafka_raw_topic, rate=rate_per_sec)
    interval = 1.0 / rate_per_sec
    try:
        while True:
            _check_attack_signal(producer)
            if random.random() < fraud_ratio:
                events = random.choice([card_testing_burst, geo_impossible_travel])()
                for ev in events:
                    _produce(producer, ev)
            else:
                _produce(producer, normal_txn())
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        log.info("producer_stopping")
    finally:
        producer.flush()


if __name__ == "__main__":
    run()
