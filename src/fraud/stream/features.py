"""Pure rolling-window feature engineering.

Deliberately free of Kafka/Spark so it can be unit-tested with plain dicts.
Given a user's rolling state and a new transaction, it computes the feature set
and returns the updated state (idempotent, side-effect free).
"""

from __future__ import annotations

from datetime import datetime

from fraud.schemas import Features, TransactionEvent, UserRollingState


def _within(seconds: float, window: float) -> bool:
    return 0 <= seconds <= window


def compute_features(
    event: TransactionEvent,
    state: UserRollingState,
) -> tuple[Features, UserRollingState]:
    """Compute features for `event` given prior `state`.

    Returns the computed :class:`Features` and an updated :class:`UserRollingState`.
    The input state is not mutated.
    """
    ts = event.timestamp
    recent_ts = list(state.recent_ts)
    recent_amounts = list(state.recent_amounts)

    # velocity over windows
    def count_within(window_s: float) -> int:
        return sum(1 for t in recent_ts if 0 <= (ts - t).total_seconds() <= window_s)

    txn_1m = count_within(60) + 1
    txn_5m = count_within(300) + 1
    txn_1h = count_within(3600) + 1

    # amount z-score vs user baseline (pre-update stats)
    mean = state.amount_mean
    std = state.amount_std
    if state.count < 2:
        amt_z = 0.0
    elif std > 1e-6:
        amt_z = (event.amount - mean) / std
    else:
        # Zero-variance baseline: a large jump must still register. Fall back to a
        # relative deviation with a sane floor so outliers are not masked.
        denom = max(abs(mean) * 0.25, 1.0)
        amt_z = (event.amount - mean) / denom

    # distinct merchants/geos in last hour is approximated with seen sets + recent
    distinct_geos_1h = len(set(state.seen_geos))
    distinct_merchants_1h = len({event.merchant})  # conservative; enriched in stream

    # time since last
    seconds_since_last = (
        (ts - state.last_ts).total_seconds() if state.last_ts is not None else 0.0
    )

    geo_key = f"{event.geo.country}:{round(event.geo.lat, 1)}:{round(event.geo.lon, 1)}"
    new_geo = geo_key not in set(state.seen_geos)
    new_device = event.device_id not in set(state.seen_devices)

    features = Features(
        amt_z=round(amt_z, 4),
        txn_1m=txn_1m,
        txn_5m=txn_5m,
        txn_1h=txn_1h,
        distinct_merchants_1h=distinct_merchants_1h,
        distinct_geos_1h=max(distinct_geos_1h, 1),
        seconds_since_last=round(seconds_since_last, 2),
        new_geo=new_geo,
        new_device=new_device,
    )

    # --- update state (Welford for running variance) ---
    count = state.count + 1
    delta = event.amount - mean
    new_mean = mean + delta / count
    delta2 = event.amount - new_mean
    m2 = state.amount_m2 + delta * delta2

    seen_geos = list(state.seen_geos)
    if geo_key not in seen_geos:
        seen_geos.append(geo_key)
    seen_devices = list(state.seen_devices)
    if event.device_id not in seen_devices:
        seen_devices.append(event.device_id)

    # keep bounded recent history (last 200) within 1h
    recent_ts.append(ts)
    recent_amounts.append(event.amount)
    kept = [
        (t, a)
        for t, a in zip(recent_ts, recent_amounts, strict=False)
        if (ts - t).total_seconds() <= 3600
    ][-200:]
    recent_ts = [t for t, _ in kept]
    recent_amounts = [a for _, a in kept]

    new_state = UserRollingState(
        _id=state.id,
        count=count,
        amount_mean=new_mean,
        amount_m2=m2,
        last_ts=ts,
        seen_geos=seen_geos[-100:],
        seen_devices=seen_devices[-100:],
        recent_amounts=recent_amounts,
        recent_ts=recent_ts,
    )
    return features, new_state


def empty_state(user_id: str) -> UserRollingState:
    """Return a fresh rolling state for a user."""
    return UserRollingState(_id=user_id)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km (used for impossible-travel checks)."""
    from math import asin, cos, radians, sin, sqrt

    r = 6371.0
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = sin(d_lat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lon / 2) ** 2
    return 2 * r * asin(sqrt(a))


def _now() -> datetime:
    from datetime import timezone

    return datetime.now(timezone.utc)
