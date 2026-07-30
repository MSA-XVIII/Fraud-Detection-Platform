"""The demo scenarios must keep telling the story the dashboard narrates."""

from __future__ import annotations

import pytest

from fraud.dashboard.demo_cases import SCENARIOS, run_scenario, score_contributions
from fraud.schemas import Features


@pytest.fixture(scope="module")
def results() -> dict[str, list[dict]]:
    return {key: run_scenario(sc) for key, sc in SCENARIOS.items()}


def test_normal_shopper_stays_low(results):
    assert all(r["risk_band"] == "low" for r in results["normal"])


def test_card_testing_cashout_scores_high(results):
    rows = results["card_testing"]
    assert rows[-1]["risk_band"] == "high"
    # the burst escalates well above the victim's quiet baseline
    baseline = rows[1]["risk_score"]
    assert rows[3]["risk_score"] > baseline + 0.4  # first probe
    assert rows[-1]["risk_score"] >= 0.8


def test_geo_impossible_flags_with_impossible_speed(results):
    london = results["geo_impossible"][2]
    assert london["risk_band"] in ("medium", "high")
    assert london["travel_kmh"] > 1000  # far beyond any airliner


def test_account_takeover_drain_is_flagged(results):
    drain = results["account_takeover"][-1]
    assert drain["risk_band"] in ("medium", "high")
    # tight baseline -> amount-anomaly signal saturates its cap
    assert drain["contributions"]["amount anomaly (amt_z)"] == 0.4


def test_fraud_ring_visible_only_through_graph(results):
    for r in results["fraud_ring"]:
        assert r["risk_band"] in ("medium", "high")
        assert r["risk_score"] > r["risk_score_no_graph"]
        assert r["risk_score_no_graph"] < 0.5  # unremarkable without the graph


def test_contributions_sum_matches_scorer_capped_total(results):
    for rows in results.values():
        for r in rows:
            total = min(sum(r["contributions"].values()), 1.0)
            assert total == pytest.approx(r["risk_score"], abs=0.005)


def test_contribution_caps():
    f = Features(amt_z=100, txn_1m=50, new_geo=True, new_device=True, shared_device_flags=99)
    c = score_contributions(f)
    assert c["amount anomaly (amt_z)"] == 0.4
    assert c["velocity (txn_1m)"] == 0.25
    assert c["shared-device ring"] == 0.2
