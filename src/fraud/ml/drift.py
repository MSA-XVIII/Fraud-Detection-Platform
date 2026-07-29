"""Drift detection: PSI on features + score-distribution drift -> trigger retrain."""

from __future__ import annotations

import numpy as np

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.ml.synthetic import make_training_frame
from fraud.schemas import Features

log = get_logger("drift")

PSI_THRESHOLD = 0.2  # >0.2 = significant population shift


def psi(expected: np.ndarray, actual: np.ndarray, buckets: int = 10) -> float:
    """Population Stability Index between two 1-D distributions."""
    quantiles = np.linspace(0, 100, buckets + 1)
    edges = np.percentile(expected, quantiles)
    edges[0], edges[-1] = -np.inf, np.inf
    e_perc = np.histogram(expected, bins=edges)[0] / max(len(expected), 1)
    a_perc = np.histogram(actual, bins=edges)[0] / max(len(actual), 1)
    e_perc = np.clip(e_perc, 1e-6, None)
    a_perc = np.clip(a_perc, 1e-6, None)
    return float(np.sum((a_perc - e_perc) * np.log(a_perc / e_perc)))


def compute_drift(reference, current) -> dict[str, float]:  # noqa: ANN001
    """Per-feature PSI between a reference and current feature frame."""
    names = Features.feature_names()
    return {n: round(psi(reference[n].to_numpy(), current[n].to_numpy()), 4) for n in names}


def run_drift_check(trigger_retrain: bool = True) -> dict[str, object]:
    """Compare a reference frame to a drifted current frame; retrain if needed."""
    reference = make_training_frame(n=6000, fraud_ratio=0.03, seed=1)
    # simulate an adversarial shift (attackers adapt): heavier fraud + higher amounts
    current = make_training_frame(n=6000, fraud_ratio=0.08, seed=99)
    current["amt_z"] = current["amt_z"] + 1.5

    scores = compute_drift(reference, current)
    drifted = {k: v for k, v in scores.items() if v > PSI_THRESHOLD}
    log.info("drift_scores", **scores)

    result: dict[str, object] = {"psi": scores, "drifted_features": list(drifted)}
    if drifted and trigger_retrain:
        log.warning("drift_detected_triggering_retrain", features=list(drifted))
        from fraud.ml.train import train_and_register

        result["retrain"] = train_and_register("auto")
    else:
        log.info("no_significant_drift")
    return result


if __name__ == "__main__":
    print(run_drift_check())
