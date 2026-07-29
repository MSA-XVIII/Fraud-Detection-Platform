"""Loads the Production-aliased model from MLflow and scores transactions.

Supports hot-reload: the stream reloads the Production model on a schedule, so a
newly promoted model is picked up WITHOUT restarting the stream. If MLflow has no
model yet, a deterministic heuristic scorer keeps the pipeline working.
"""

from __future__ import annotations

import time
from typing import Any

import numpy as np

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.schemas import Features, RiskBand

log = get_logger("scorer")


def band_for(score: float) -> RiskBand:
    if score >= settings.risk_high_threshold:
        return RiskBand.HIGH
    if score >= settings.risk_medium_threshold:
        return RiskBand.MEDIUM
    return RiskBand.LOW


class HeuristicScorer:
    """Fallback used before any model is trained/registered."""

    kind = "heuristic"

    def score_one(self, fv: list[float]) -> float:
        f = dict(zip(Features.feature_names(), fv, strict=False))
        s = 0.0
        s += min(abs(f["amt_z"]) / 10.0, 0.4)
        s += min(f["txn_1m"] / 10.0, 0.25)
        s += 0.15 if f["new_geo"] else 0.0
        s += 0.1 if f["new_device"] else 0.0
        s += min(f["shared_device_flags"] / 10.0, 0.2)
        return float(min(s, 1.0))


class ModelScorer:
    """Wraps an MLflow-loaded model with periodic hot-reload."""

    def __init__(self) -> None:
        self._model: Any = HeuristicScorer()
        self._version: str = "heuristic"
        self._loaded_at: float = 0.0
        self.reload(force=True)

    @property
    def version(self) -> str:
        return self._version

    def reload(self, force: bool = False) -> None:
        if not force and (time.time() - self._loaded_at) < settings.model_reload_seconds:
            return
        self._loaded_at = time.time()
        try:
            import mlflow

            mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
            from mlflow import MlflowClient

            client = MlflowClient()
            mv = client.get_model_version_by_alias(
                settings.mlflow_model_name, settings.mlflow_model_alias
            )
            if mv.version == self._version:
                return
            import mlflow.sklearn

            model = mlflow.sklearn.load_model(
                f"models:/{settings.mlflow_model_name}@{settings.mlflow_model_alias}"
            )
            self._model = model
            self._version = str(mv.version)
            log.info("model_loaded", version=self._version, kind=getattr(model, "kind", "?"))
        except Exception as exc:  # noqa: BLE001
            if self._version == "heuristic":
                log.warning("model_unavailable_using_heuristic", error=str(exc))
            # keep whatever we already had

    def score(self, features: Features) -> tuple[float, RiskBand]:
        self.reload()
        fv = features.to_vector()
        if hasattr(self._model, "score_one"):
            score = self._model.score_one(fv)
        else:
            score = float(self._model.score(np.asarray(fv, dtype=float))[0])
        return score, band_for(score)

    @property
    def model(self) -> Any:
        return self._model


_singleton: ModelScorer | None = None


def get_scorer() -> ModelScorer:
    global _singleton
    if _singleton is None:
        _singleton = ModelScorer()
    return _singleton
