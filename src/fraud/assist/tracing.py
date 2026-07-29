"""LangSmith tracing wrapper with a no-op fallback + analyst feedback capture.

When LANGCHAIN_API_KEY is set and LANGCHAIN_TRACING_V2=true, runs are traced to
LangSmith. Otherwise everything is a no-op so local runs never break. Analyst
thumbs up/down is persisted locally (and to LangSmith when enabled) and doubles
as the fine-tuning dataset source.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.settings import settings
from fraud.logging_config import get_logger

log = get_logger("tracing")

FEEDBACK_LOG = Path(settings.data_dir) / "feedback" / "analyst_feedback.jsonl"


def configure_tracing() -> None:
    """Export LangSmith env vars if configured; else disable tracing."""
    if settings.langsmith_enabled:
        os.environ["LANGCHAIN_TRACING_V2"] = "true"
        os.environ["LANGCHAIN_ENDPOINT"] = settings.langchain_endpoint
        os.environ["LANGCHAIN_API_KEY"] = settings.langchain_api_key
        os.environ["LANGCHAIN_PROJECT"] = settings.langchain_project
        log.info("langsmith_enabled", project=settings.langchain_project)
    else:
        os.environ["LANGCHAIN_TRACING_V2"] = "false"


def record_feedback(
    txn_id: str,
    summary: str,
    score: int,
    prompt: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> None:
    """Persist analyst 👍(1)/👎(0) feedback locally (+ LangSmith when enabled).

    Thumbs-up, grounded summaries become fine-tuning examples (see assist.finetune).
    """
    FEEDBACK_LOG.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "txn_id": txn_id,
        "summary": summary,
        "score": int(score),
        "prompt": prompt or {},
        "run_id": run_id,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    with FEEDBACK_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")

    if settings.langsmith_enabled and run_id:
        try:
            from langsmith import Client

            Client().create_feedback(run_id, key="analyst_thumb", score=float(score))
        except Exception as exc:  # noqa: BLE001
            log.warning("langsmith_feedback_failed", error=str(exc))
    log.info("feedback_recorded", txn=txn_id, score=score)


def load_feedback() -> list[dict[str, Any]]:
    if not FEEDBACK_LOG.exists():
        return []
    return [json.loads(line) for line in FEEDBACK_LOG.read_text().splitlines() if line.strip()]
