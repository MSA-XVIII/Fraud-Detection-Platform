"""Tests for risk banding and the heuristic scorer."""

from fraud.schemas import Features, RiskBand
from fraud.stream.scorer import HeuristicScorer, band_for


def test_band_thresholds():
    assert band_for(0.95) == RiskBand.HIGH
    assert band_for(0.6) == RiskBand.MEDIUM
    assert band_for(0.1) == RiskBand.LOW


def test_heuristic_scorer_flags_anomalies_higher():
    scorer = HeuristicScorer()
    normal = Features(amt_z=0.2, txn_1m=1, new_geo=False, new_device=False)
    fraud = Features(
        amt_z=9.0, txn_1m=8, new_geo=True, new_device=True, shared_device_flags=4
    )
    assert scorer.score_one(fraud.to_vector()) > scorer.score_one(normal.to_vector())


def test_heuristic_score_in_unit_range():
    scorer = HeuristicScorer()
    f = Features(amt_z=100.0, txn_1m=100, shared_device_flags=100)
    s = scorer.score_one(f.to_vector())
    assert 0.0 <= s <= 1.0
