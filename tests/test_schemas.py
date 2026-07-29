"""Tests for schema contracts."""

from fraud.schemas import (
    Features,
    Geo,
    RiskBand,
    ScoredTransaction,
    TransactionEvent,
)


def test_transaction_event_parses_iso_timestamp():
    ev = TransactionEvent(
        txn_id="t1",
        user_id="u1",
        amount=100.0,
        merchant="Amazon",
        merchant_category="ecommerce",
        geo=Geo(lat=1.0, lon=2.0, country="IN"),
        device_id="d1",
        ip="1.2.3.4",
        timestamp="2026-01-01T00:00:00Z",
    )
    assert ev.timestamp.year == 2026


def test_feature_vector_matches_names():
    f = Features()
    assert len(f.to_vector()) == len(Features.feature_names())


def test_scored_transaction_expected_loss_and_alias():
    txn = ScoredTransaction(
        _id="t1",
        user_id="u1",
        amount=1000.0,
        merchant="Amazon",
        risk_score=0.5,
        risk_band=RiskBand.MEDIUM,
    )
    assert txn.expected_loss == 500.0
    assert txn.to_mongo()["_id"] == "t1"


def test_negative_amount_rejected():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        TransactionEvent(
            txn_id="t1",
            user_id="u1",
            amount=-5.0,
            merchant="Amazon",
            merchant_category="ecommerce",
            geo=Geo(lat=1.0, lon=2.0, country="IN"),
            device_id="d1",
            ip="1.2.3.4",
        )
