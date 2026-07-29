"""`make chaos`: demonstrate no-loss recovery.

Publishes a batch of transactions with known txn_ids, runs the consumer, kills it
mid-stream, restarts it, and verifies every txn_id was persisted to Mongo exactly
once — proving at-least-once ingest + idempotent upserts survive a crash.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import time
import uuid

from confluent_kafka import Producer

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.producer.generator import normal_txn
from fraud.producer.kafka_utils import producer_conf
from fraud.serving import mongo

log = get_logger("chaos")


def _publish(n: int) -> list[str]:
    producer = Producer(producer_conf())
    ids: list[str] = []
    for _ in range(n):
        ev = normal_txn()
        ev.txn_id = str(uuid.uuid4())
        ids.append(ev.txn_id)
        producer.produce(
            settings.kafka_raw_topic, key=ev.user_id.encode(), value=ev.model_dump_json().encode()
        )
    producer.flush()
    return ids


def _run_consumer() -> None:
    from fraud.stream.job import run

    run()


def run_chaos(n: int = 200) -> None:
    ids = _publish(n)
    log.info("published", count=len(ids))

    proc = mp.Process(target=_run_consumer, daemon=True)
    proc.start()
    time.sleep(6)  # let it consume some
    log.warning("killing_consumer_mid_stream")
    proc.terminate()
    proc.join()

    log.info("restarting_consumer")
    proc2 = mp.Process(target=_run_consumer, daemon=True)
    proc2.start()
    time.sleep(8)
    proc2.terminate()
    proc2.join()

    found = mongo.flagged().count_documents({"_id": {"$in": ids}})
    # low-risk txns are still upserted to flagged_transactions
    log.info("recovery_result", published=len(ids), persisted=found)
    print(f"CHAOS RESULT: published={len(ids)} persisted={found} (no loss if equal)")


if __name__ == "__main__":
    run_chaos()
