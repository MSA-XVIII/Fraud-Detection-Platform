"""SHAP explainability: top-N feature attributions for a scored transaction."""

from __future__ import annotations

from typing import Any

import numpy as np

from fraud.logging_config import get_logger
from fraud.schemas import Features

log = get_logger("explain")


def top_features(
    model: Any,
    feature_vector: list[float],
    feature_names: list[str] | None = None,
    top_n: int = 5,
) -> list[tuple[str, float]]:
    """Return the top-N (feature, signed_contribution) pairs via SHAP.

    Falls back to a simple magnitude ranking if SHAP cannot explain the model.
    """
    names = feature_names or Features.feature_names()
    x = np.asarray(feature_vector, dtype=float).reshape(1, -1)
    try:
        import shap

        explainer = _build_explainer(model, x)
        values = explainer(x)
        contribs = np.asarray(values.values).reshape(-1)
        pairs = sorted(
            zip(names, contribs.tolist(), strict=False),
            key=lambda t: abs(t[1]),
            reverse=True,
        )
        return [(n, round(float(v), 4)) for n, v in pairs[:top_n]]
    except Exception as exc:  # noqa: BLE001
        log.warning("shap_fallback", error=str(exc))
        pairs = sorted(
            zip(names, x.reshape(-1).tolist(), strict=False),
            key=lambda t: abs(t[1]),
            reverse=True,
        )
        return [(n, round(float(v), 4)) for n, v in pairs[:top_n]]


def _build_explainer(model: Any, background: np.ndarray):  # noqa: ANN201
    import shap

    inner = getattr(model, "model", model)
    # Tree models (XGBoost / IsolationForest) -> TreeExplainer where possible.
    try:
        return shap.TreeExplainer(inner)
    except Exception:  # noqa: BLE001
        predict = getattr(model, "score", None) or getattr(inner, "predict", None)
        return shap.Explainer(predict, background)
