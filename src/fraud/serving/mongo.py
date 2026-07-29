"""MongoDB serving layer: idempotent upserts, indexes, vector search.

Mongo is the low-latency *serving* store (source of truth for training is the
lakehouse). All writes upsert on the natural key so Kafka can be safely replayed.
Vector search uses Atlas `$vectorSearch` when enabled, else a local brute-force
cosine fallback so everything runs with no cloud.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any

import numpy as np
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.schemas import Alert, ScoredTransaction, UserRollingState

log = get_logger("mongo")

_client: MongoClient | None = None


def get_db() -> Database:
    global _client
    if _client is None:
        _client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    return _client[settings.mongo_db]


def flagged() -> Collection:
    return get_db()["flagged_transactions"]


def user_state() -> Collection:
    return get_db()["user_rolling_state"]


def alerts() -> Collection:
    return get_db()["alerts"]


def case_embeddings() -> Collection:
    return get_db()["case_embeddings"]


def init_indexes() -> None:
    """Create indexes; idempotent."""
    flagged().create_index([("user_id", ASCENDING)])
    flagged().create_index([("risk_band", ASCENDING)])
    flagged().create_index([("created_at", DESCENDING)])
    flagged().create_index([("status", ASCENDING)])
    alerts().create_index([("status", ASCENDING), ("priority", DESCENDING)])
    log.info("mongo_indexes_ready", db=settings.mongo_db)


# --- idempotent writes ----------------------------------------------------


def upsert_scored(txn: ScoredTransaction) -> None:
    doc = txn.to_mongo()
    flagged().update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)


def upsert_alert(alert: Alert) -> None:
    doc = alert.model_dump(by_alias=True, mode="json")
    alerts().update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)


def upsert_user_state(state: UserRollingState) -> None:
    doc = state.model_dump(by_alias=True, mode="json")
    user_state().update_one({"_id": doc["_id"]}, {"$set": doc}, upsert=True)


def get_user_state(user_id: str) -> dict[str, Any] | None:
    return user_state().find_one({"_id": user_id})


def set_label(txn_id: str, label: bool) -> None:
    flagged().update_one(
        {"_id": txn_id},
        {"$set": {"label": label, "status": "closed", "labeled_at": _now_iso()}},
    )


def get_user_history(user_id: str, limit: int = 20) -> list[dict[str, Any]]:
    cur = flagged().find({"user_id": user_id}).sort("created_at", DESCENDING).limit(limit)
    return list(cur)


def top_alerts(limit: int = 50) -> list[dict[str, Any]]:
    cur = flagged().find({"risk_band": {"$in": ["high", "medium"]}})
    docs = list(cur)
    docs.sort(key=lambda d: d.get("risk_score", 0) * d.get("amount", 0), reverse=True)
    return docs[:limit]


# --- vector search --------------------------------------------------------


def upsert_case_embedding(case_id: str, vector: list[float], meta: dict[str, Any]) -> None:
    doc = {"_id": case_id, "vector": vector, **meta, "created_at": _now_iso()}
    case_embeddings().update_one({"_id": case_id}, {"$set": doc}, upsert=True)


def vector_search(query: list[float], k: int = 3) -> list[dict[str, Any]]:
    """Return top-k similar resolved cases.

    Uses Atlas `$vectorSearch` when MONGO_USE_ATLAS_VECTOR=true, else a local
    brute-force cosine similarity over stored vectors.
    """
    if settings.mongo_use_atlas_vector:
        return _atlas_vector_search(query, k)
    return _bruteforce_search(query, k)


def _atlas_vector_search(query: list[float], k: int) -> list[dict[str, Any]]:
    pipeline = [
        {
            "$vectorSearch": {
                "index": "case_vector_index",
                "path": "vector",
                "queryVector": query,
                "numCandidates": 100,
                "limit": k,
            }
        },
        {"$project": {"vector": 0, "score": {"$meta": "vectorSearchScore"}}},
    ]
    return list(case_embeddings().aggregate(pipeline))


def _bruteforce_search(query: list[float], k: int) -> list[dict[str, Any]]:
    q = np.asarray(query, dtype=float)
    qn = np.linalg.norm(q) + 1e-9
    scored: list[tuple[float, dict[str, Any]]] = []
    for doc in case_embeddings().find({}):
        v = np.asarray(doc.get("vector", []), dtype=float)
        if v.size != q.size:
            continue
        sim = float(np.dot(q, v) / (qn * (np.linalg.norm(v) + 1e-9)))
        d = {kk: vv for kk, vv in doc.items() if kk != "vector"}
        d["score"] = sim
        scored.append((sim, d))
    scored.sort(key=lambda t: t[0], reverse=True)
    return [d for _, d in scored[:k]]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true", help="create indexes")
    args = ap.parse_args()
    if args.init:
        init_indexes()
