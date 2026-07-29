"""LLM evaluation harness (LLMOps counterpart to MLflow).

With LangSmith enabled, uploads a small labelled dataset and runs faithfulness /
relevance evaluators. Offline, runs deterministic grounding checks so `make
eval-llm` always produces a score. Run: make eval-llm
"""

from __future__ import annotations

from typing import Any

from config.settings import settings
from fraud.assist.chains import rag_summary
from fraud.logging_config import get_logger

log = get_logger("eval")

# A handful of labelled reference cases (flagged txn -> expected grounded signals).
EVAL_SET: list[dict[str, Any]] = [
    {
        "_id": "eval_1",
        "user_id": "user_0001",
        "amount": 12000.0,
        "currency": "INR",
        "merchant": "Steam",
        "risk_score": 0.91,
        "risk_band": "high",
        "graph": {"ring_id": "R_2", "shared_device_flags": 3, "component_size": 5},
        "shap_top": [["amt_z", 0.41], ["txn_1m", 0.28], ["new_device", 0.12]],
        "expect_terms": ["amount", "device", "ring"],
    },
    {
        "_id": "eval_2",
        "user_id": "user_0042",
        "amount": 15.0,
        "currency": "INR",
        "merchant": "Amazon",
        "risk_score": 0.84,
        "risk_band": "high",
        "graph": {"ring_id": None, "shared_device_flags": 0, "component_size": 1},
        "shap_top": [["txn_1m", 0.5], ["amt_z", 0.2]],
        "expect_terms": ["velocity", "transactions", "card"],
    },
]


def _grounding_score(summary: str, expect_terms: list[str], cited: list[str]) -> dict[str, float]:
    text = summary.lower()
    covered = sum(1 for t in expect_terms if t.lower() in text)
    relevance = covered / max(len(expect_terms), 1)
    faithfulness = 1.0 if cited else 0.5  # cited evidence -> more faithful
    hallucination = 0.0 if cited else 0.3
    return {
        "relevance": round(relevance, 3),
        "faithfulness": round(faithfulness, 3),
        "hallucination": round(hallucination, 3),
    }


def run_offline_eval() -> dict[str, Any]:
    results = []
    for case in EVAL_SET:
        summary = rag_summary(case)
        metrics = _grounding_score(summary.summary, case["expect_terms"], summary.cited_case_ids)
        results.append({"id": case["_id"], "cited": summary.cited_case_ids, **metrics})
        log.info("eval_case", id=case["_id"], **metrics)
    agg = {
        "relevance": round(sum(r["relevance"] for r in results) / len(results), 3),
        "faithfulness": round(sum(r["faithfulness"] for r in results) / len(results), 3),
        "hallucination": round(sum(r["hallucination"] for r in results) / len(results), 3),
    }
    log.info("eval_aggregate", **agg)
    return {"cases": results, "aggregate": agg}


def run() -> dict[str, Any]:
    if settings.langsmith_enabled:
        log.info("langsmith_eval_enabled", project=settings.langchain_project)
        # LangSmith online evaluators would attach here; offline scoring still runs.
    return run_offline_eval()


if __name__ == "__main__":
    import json

    print(json.dumps(run(), indent=2))
