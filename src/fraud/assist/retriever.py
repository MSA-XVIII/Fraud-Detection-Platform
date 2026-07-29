"""Embeddings + retrieval over resolved cases and fraud-policy snippets.

Embeds with sentence-transformers when available; otherwise a deterministic
hashing embedder keeps tests/offline demos working with no model download.
Vectors live in Mongo `case_embeddings`; search delegates to serving.mongo
(Atlas Vector Search or local brute-force cosine).
"""

from __future__ import annotations

import argparse
import hashlib
from functools import lru_cache
from typing import Any

import numpy as np

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.serving import mongo

log = get_logger("retriever")

# A tiny corpus of resolved cases + policy snippets for grounded RAG.
SEED_CASES: list[dict[str, Any]] = [
    {
        "_id": "CASE_017",
        "text": "Card-testing ring: many sub-$25 authorisations in under 60s from new "
        "devices. Confirmed fraud. Action: block + step-up auth.",
        "kind": "case",
        "label": "fraud",
    },
    {
        "_id": "CASE_004",
        "text": "Velocity spike: 12 transactions in 5 minutes across 3 merchants sharing "
        "one IP with a known ring. Confirmed fraud.",
        "kind": "case",
        "label": "fraud",
    },
    {
        "_id": "CASE_022",
        "text": "Geo-impossible travel: two high-value transactions 8,000km apart within "
        "90 seconds on the same card. Confirmed fraud.",
        "kind": "case",
        "label": "fraud",
    },
    {
        "_id": "CASE_051",
        "text": "Large purchase from a returning customer at a usual merchant and geo; "
        "amount within 2 std of baseline. Reviewed and cleared as legitimate.",
        "kind": "case",
        "label": "legit",
    },
    {
        "_id": "POLICY_HIGHRISK",
        "text": "Policy: transactions scoring >= 0.80 are auto-held. Analysts must verify "
        "device + geo novelty and check for shared-device rings before release.",
        "kind": "policy",
        "label": "policy",
    },
]


@lru_cache(maxsize=1)
def _model() -> Any | None:
    try:
        from sentence_transformers import SentenceTransformer

        m = SentenceTransformer(settings.embedding_model)
        log.info("embedder_loaded", model=settings.embedding_model)
        return m
    except Exception as exc:  # noqa: BLE001
        log.warning("embedder_fallback_hashing", error=str(exc))
        return None


def embed(text: str) -> list[float]:
    """Return an embedding vector for `text` (real model or hashing fallback)."""
    model = _model()
    if model is not None:
        return model.encode(text, normalize_embeddings=True).tolist()
    return _hash_embed(text, settings.embedding_dim)


def _hash_embed(text: str, dim: int) -> list[float]:
    vec = np.zeros(dim, dtype=float)
    for token in text.lower().split():
        h = int(hashlib.md5(token.encode()).hexdigest(), 16)  # noqa: S324 (non-crypto use)
        vec[h % dim] += 1.0
    norm = np.linalg.norm(vec) + 1e-9
    return (vec / norm).tolist()


def seed_cases() -> None:
    """Embed and upsert the seed corpus into Mongo `case_embeddings`."""
    for case in SEED_CASES:
        vector = embed(case["text"])
        mongo.upsert_case_embedding(
            case["_id"],
            vector,
            {"text": case["text"], "kind": case["kind"], "label": case["label"]},
        )
    log.info("cases_seeded", count=len(SEED_CASES))


def similar_cases(query_text: str, k: int = 3) -> list[dict[str, Any]]:
    """Retrieve top-k similar resolved cases / policy snippets."""
    return mongo.vector_search(embed(query_text), k=k)


def format_context(cases: list[dict[str, Any]]) -> str:
    """Render retrieved cases into a cited context block for prompts."""
    lines = []
    for c in cases:
        cid = c.get("_id", "?")
        score = c.get("score", 0.0)
        lines.append(f"[{cid}] (sim={score:.2f}) {c.get('text', '')}")
    return "\n".join(lines) if lines else "No similar cases found."


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", action="store_true")
    args = ap.parse_args()
    if args.seed:
        seed_cases()
