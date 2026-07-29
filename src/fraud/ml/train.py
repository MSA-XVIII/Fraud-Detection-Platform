"""Model training: IsolationForest bootstrap + optional XGBoost, logged to MLflow.

The stream loads whatever model is aliased `Production` in the MLflow registry,
so `make train` can promote a new model WITHOUT restarting the stream.
"""

from __future__ import annotations

import argparse
from typing import Any

import numpy as np

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.ml.synthetic import make_training_frame
from fraud.schemas import Features

log = get_logger("train")


class FraudScorer:
    """Uniform wrapper producing a fraud probability in [0, 1]."""

    def __init__(self, model: Any, kind: str, score_min: float = 0.0, score_max: float = 1.0):
        self.model = model
        self.kind = kind  # "iforest" | "xgboost"
        self.score_min = score_min
        self.score_max = score_max
        self.feature_names = Features.feature_names()

    def score(self, x: np.ndarray) -> np.ndarray:
        x = np.atleast_2d(x)
        if self.kind == "iforest":
            # score_samples: higher = more normal. Invert + normalise to [0,1].
            raw = -self.model.score_samples(x)
            rng = (self.score_max - self.score_min) or 1.0
            return np.clip((raw - self.score_min) / rng, 0.0, 1.0)
        # xgboost / sklearn classifier
        proba = self.model.predict_proba(x)[:, 1]
        return np.clip(proba, 0.0, 1.0)

    def score_one(self, feature_vector: list[float]) -> float:
        return float(self.score(np.asarray(feature_vector, dtype=float))[0])


def train_isolation_forest(x: np.ndarray) -> FraudScorer:
    from sklearn.ensemble import IsolationForest

    model = IsolationForest(n_estimators=200, contamination=0.03, random_state=42)
    model.fit(x)
    raw = -model.score_samples(x)
    return FraudScorer(model, "iforest", float(raw.min()), float(raw.max()))


def train_xgboost(x: np.ndarray, y: np.ndarray) -> FraudScorer:
    from xgboost import XGBClassifier

    pos = max(int(y.sum()), 1)
    neg = max(int(len(y) - y.sum()), 1)
    model = XGBClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.1,
        scale_pos_weight=neg / pos,
        eval_metric="aucpr",
        n_jobs=2,
    )
    model.fit(x, y)
    return FraudScorer(model, "xgboost")


def evaluate(scorer: FraudScorer, x: np.ndarray, y: np.ndarray) -> dict[str, float]:
    from sklearn.metrics import average_precision_score, precision_score, recall_score

    scores = scorer.score(x)
    preds = (scores >= settings.risk_high_threshold).astype(int)
    return {
        "pr_auc": float(average_precision_score(y, scores)) if y.sum() else 0.0,
        "precision": float(precision_score(y, preds, zero_division=0)),
        "recall": float(recall_score(y, preds, zero_division=0)),
        "flag_rate": float(preds.mean()),
    }


def train_and_register(model_kind: str = "auto") -> dict[str, Any]:
    """Train a model, log + register to MLflow, alias Production if it wins."""
    import mlflow

    df = make_training_frame(n=8000, fraud_ratio=0.03)
    feature_names = Features.feature_names()
    x = df[feature_names].to_numpy(dtype=float)
    y = df["label"].to_numpy(dtype=int)

    use_supervised = model_kind == "xgboost" or (model_kind == "auto" and y.sum() >= 50)
    scorer = train_xgboost(x, y) if use_supervised else train_isolation_forest(x)

    metrics = evaluate(scorer, x, y)
    log.info("trained", kind=scorer.kind, **metrics)

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow.set_experiment(settings.mlflow_experiment)
    with mlflow.start_run() as run:
        mlflow.log_param("kind", scorer.kind)
        mlflow.log_metrics(metrics)
        import mlflow.sklearn

        mlflow.sklearn.log_model(scorer, artifact_path="model")
        model_uri = f"runs:/{run.info.run_id}/model"
        result = _register_and_promote(mlflow, model_uri, metrics)
    return {"kind": scorer.kind, "metrics": metrics, **result}


def _register_and_promote(mlflow, model_uri: str, metrics: dict[str, float]) -> dict[str, Any]:  # noqa: ANN001
    """Register the model; promote to Production alias if it beats the incumbent."""
    from mlflow import MlflowClient

    client = MlflowClient()
    name = settings.mlflow_model_name
    try:
        client.create_registered_model(name)
    except Exception:  # noqa: BLE001
        pass
    mv = mlflow.register_model(model_uri, name)

    incumbent_pr = _incumbent_metric(client, name, "pr_auc")
    challenger_pr = metrics.get("pr_auc", 0.0)
    promote = challenger_pr >= incumbent_pr
    if promote:
        client.set_registered_model_alias(name, settings.mlflow_model_alias, mv.version)
        log.info("promoted", version=mv.version, alias=settings.mlflow_model_alias)
    else:
        log.info("held_challenger", version=mv.version, incumbent_pr=incumbent_pr)
    return {"version": mv.version, "promoted": promote}


def _incumbent_metric(client, name: str, key: str) -> float:  # noqa: ANN001
    try:
        mv = client.get_model_version_by_alias(name, settings.mlflow_model_alias)
        run = client.get_run(mv.run_id)
        return float(run.data.metrics.get(key, 0.0))
    except Exception:  # noqa: BLE001
        return -1.0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", default="auto", choices=["auto", "iforest", "xgboost"])
    args = ap.parse_args()
    out = train_and_register(args.kind)
    print(out)
