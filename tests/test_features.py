"""Unit tests for pure feature engineering."""

from datetime import datetime, timedelta, timezone

from fraud.schemas import Geo, TransactionEvent
from fraud.stream.features import compute_features, empty_state, haversine_km


def _txn(user="user_1", amount=1000.0, device="dev_1", country="IN", lat=19.0, lon=72.0, ts=None):
    return TransactionEvent(
        txn_id="t1",
        user_id=user,
        amount=amount,
        merchant="Amazon",
        merchant_category="ecommerce",
        geo=Geo(lat=lat, lon=lon, country=country),
        device_id=device,
        ip="1.2.3.4",
        timestamp=(ts or datetime.now(timezone.utc)).isoformat(),
    )


def test_first_txn_is_new_geo_and_device():
    state = empty_state("user_1")
    feats, new_state = compute_features(_txn(), state)
    assert feats.new_geo is True
    assert feats.new_device is True
    assert feats.txn_1m == 1
    assert new_state.count == 1


def test_velocity_counts_within_window():
    state = empty_state("user_1")
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(4):
        feats, state = compute_features(
            _txn(ts=base + timedelta(seconds=i * 10)), state
        )
    # 4th txn: three prior within 60s + itself
    assert feats.txn_1m == 4


def test_amount_zscore_grows_with_outlier():
    state = empty_state("user_1")
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(10):
        _, state = compute_features(_txn(amount=1000.0, ts=base + timedelta(minutes=i)), state)
    feats, _ = compute_features(_txn(amount=50000.0, ts=base + timedelta(minutes=11)), state)
    assert feats.amt_z > 3.0


def test_state_is_not_mutated():
    state = empty_state("user_1")
    _, new_state = compute_features(_txn(), state)
    assert state.count == 0
    assert new_state.count == 1


def test_haversine_far_distance():
    # Mumbai to New York ~ 12,500 km
    d = haversine_km(19.07, 72.87, 40.71, -74.0)
    assert d > 10000
