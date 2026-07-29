"""Synthetic labelled training data matching the production feature schema.

Generates a pandas frame with the same columns as ``Features.feature_names()``
plus a ``label`` column, so training, drift and tests share one contract.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from fraud.schemas import Features


def make_training_frame(n: int = 8000, fraud_ratio: float = 0.03, seed: int = 42) -> pd.DataFrame:
    """Return a labelled feature frame (fraud rows have anomalous features)."""
    rng = np.random.default_rng(seed)
    names = Features.feature_names()
    n_fraud = int(n * fraud_ratio)
    n_legit = n - n_fraud

    # --- legitimate behaviour ---
    legit = {
        "amt_z": rng.normal(0.0, 1.0, n_legit),
        "txn_1m": rng.poisson(1.0, n_legit).astype(float),
        "txn_5m": rng.poisson(2.0, n_legit).astype(float),
        "txn_1h": rng.poisson(5.0, n_legit).astype(float),
        "distinct_merchants_1h": rng.poisson(2.0, n_legit).astype(float),
        "distinct_geos_1h": np.clip(rng.poisson(1.0, n_legit), 1, None).astype(float),
        "seconds_since_last": rng.exponential(600, n_legit),
        "new_geo": rng.binomial(1, 0.05, n_legit).astype(float),
        "new_device": rng.binomial(1, 0.05, n_legit).astype(float),
        "shared_device_flags": rng.binomial(1, 0.02, n_legit).astype(float),
        "component_size": np.clip(rng.poisson(1.0, n_legit), 1, None).astype(float),
    }

    # --- fraudulent behaviour (velocity spikes, new geo/device, rings) ---
    fraud = {
        "amt_z": rng.normal(6.0, 2.5, n_fraud),
        "txn_1m": rng.poisson(6.0, n_fraud).astype(float) + 3,
        "txn_5m": rng.poisson(12.0, n_fraud).astype(float) + 5,
        "txn_1h": rng.poisson(25.0, n_fraud).astype(float) + 10,
        "distinct_merchants_1h": rng.poisson(6.0, n_fraud).astype(float),
        "distinct_geos_1h": rng.poisson(3.0, n_fraud).astype(float) + 1,
        "seconds_since_last": rng.exponential(5, n_fraud),
        "new_geo": rng.binomial(1, 0.7, n_fraud).astype(float),
        "new_device": rng.binomial(1, 0.6, n_fraud).astype(float),
        "shared_device_flags": rng.poisson(3.0, n_fraud).astype(float),
        "component_size": rng.poisson(8.0, n_fraud).astype(float) + 2,
    }

    df_legit = pd.DataFrame({k: legit[k] for k in names})
    df_legit["label"] = 0
    df_fraud = pd.DataFrame({k: fraud[k] for k in names})
    df_fraud["label"] = 1

    df = pd.concat([df_legit, df_fraud], ignore_index=True)
    return df.sample(frac=1.0, random_state=seed).reset_index(drop=True)
