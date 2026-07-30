"""Self-contained example cases for the demo dashboard.

Each scenario is a scripted sequence of transactions replayed through the REAL
pipeline code — :func:`fraud.stream.features.compute_features` and the
:class:`fraud.stream.scorer.HeuristicScorer` — entirely in-process, so the demo
needs no Kafka/Mongo/Neo4j. Graph features (which come from Neo4j in
production) are supplied per-event via ``graph`` overrides, mirroring what
``enrich_with_graph`` does on the stream.

Pure module: no Streamlit imports, unit-testable with plain asserts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from fraud.schemas import Features, Geo, TransactionEvent, UserRollingState
from fraud.stream.features import compute_features, empty_state, haversine_km
from fraud.stream.scorer import HeuristicScorer, band_for

BASE_TS = datetime(2026, 7, 30, 12, 0, 0, tzinfo=UTC)

CITIES: dict[str, tuple[float, float, str]] = {
    "Mumbai": (19.0760, 72.8777, "IN"),
    "Delhi": (28.6139, 77.2090, "IN"),
    "Bengaluru": (12.9716, 77.5946, "IN"),
    "London": (51.5072, -0.1276, "GB"),
    "Lagos": (6.5244, 3.3792, "NG"),
    "Sao Paulo": (-23.5505, -46.6333, "BR"),
    "New York": (40.7128, -74.0060, "US"),
    "Singapore": (1.3521, 103.8198, "SG"),
    "Moscow": (55.7558, 37.6173, "RU"),
    "Hanoi": (21.0285, 105.8542, "VN"),
    "Kyiv": (50.4501, 30.5234, "UA"),
    "Bogota": (4.7110, -74.0721, "CO"),
}


@dataclass
class DemoTxn:
    """One scripted transaction: offsets/graph info + the raw event fields."""

    minute: float  # minutes after BASE_TS
    user_id: str
    amount: float
    merchant: str
    category: str
    city: str
    device: str
    note: str = ""
    graph: dict[str, Any] = field(default_factory=dict)  # ring_id / shared_device_flags / component_size


@dataclass
class Scenario:
    key: str
    title: str
    icon: str
    summary: str
    expected: str  # what the audience should watch for
    txns: list[DemoTxn]


def _event(t: DemoTxn, seq: int) -> TransactionEvent:
    lat, lon, country = CITIES[t.city]
    return TransactionEvent(
        txn_id=f"demo-{t.user_id}-{seq:03d}",
        user_id=t.user_id,
        amount=t.amount,
        merchant=t.merchant,
        merchant_category=t.category,
        geo=Geo(lat=lat, lon=lon, country=country),
        device_id=t.device,
        ip=f"203.0.113.{seq + 1}",
        timestamp=BASE_TS + timedelta(minutes=t.minute),
    )


def run_scenario(scenario: Scenario) -> list[dict[str, Any]]:
    """Replay a scenario through features + scorer; one result row per txn."""
    scorer = HeuristicScorer()
    states: dict[str, UserRollingState] = {}
    rows: list[dict[str, Any]] = []
    prev_geo: dict[str, tuple[float, float, datetime]] = {}

    for seq, t in enumerate(scenario.txns):
        event = _event(t, seq)
        state = states.get(t.user_id) or empty_state(t.user_id)
        features, states[t.user_id] = compute_features(event, state)
        score_no_graph = scorer.score_one(features.to_vector())
        if t.graph:
            features = features.model_copy(update=t.graph)

        score = scorer.score_one(features.to_vector())
        band = band_for(score)

        travel_km = travel_kmh = 0.0
        if t.user_id in prev_geo:
            plat, plon, pts = prev_geo[t.user_id]
            travel_km = haversine_km(plat, plon, event.geo.lat, event.geo.lon)
            hours = max((event.timestamp - pts).total_seconds() / 3600, 1e-6)
            travel_kmh = travel_km / hours
        prev_geo[t.user_id] = (event.geo.lat, event.geo.lon, event.timestamp)

        rows.append(
            {
                "seq": seq + 1,
                "txn_id": event.txn_id,
                "time": event.timestamp,
                "user_id": t.user_id,
                "amount": t.amount,
                "merchant": t.merchant,
                "city": t.city,
                "device": t.device,
                "lat": event.geo.lat,
                "lon": event.geo.lon,
                "risk_score": round(score, 3),
                "risk_score_no_graph": round(score_no_graph, 3),
                "risk_band": band.value,
                "expected_loss": round(score * t.amount, 2),
                "note": t.note,
                "travel_km": round(travel_km, 1),
                "travel_kmh": round(travel_kmh, 1),
                "features": features.model_dump(),
                "contributions": score_contributions(features),
            }
        )
    return rows


def score_contributions(f: Features) -> dict[str, float]:
    """Per-signal breakdown mirroring HeuristicScorer.score_one (same caps)."""
    return {
        "amount anomaly (amt_z)": round(min(abs(f.amt_z) / 10.0, 0.4), 3),
        "velocity (txn_1m)": round(min(f.txn_1m / 10.0, 0.25), 3),
        "new geo": 0.15 if f.new_geo else 0.0,
        "new device": 0.10 if f.new_device else 0.0,
        "shared-device ring": round(min(f.shared_device_flags / 10.0, 0.2), 3),
    }


# --------------------------------------------------------------------------
# The example cases
# --------------------------------------------------------------------------

_NORMAL = Scenario(
    key="normal",
    title="Normal shopper",
    icon="🛒",
    summary=(
        "A regular customer in Mumbai making everyday purchases over the day — "
        "same device, familiar city, amounts near their baseline."
    ),
    expected="Every transaction stays LOW risk. This is the healthy baseline the model learns per user.",
    txns=[
        DemoTxn(0, "user_normal", 850, "BigBasket", "grocery", "Mumbai", "dev-A1", "morning groceries"),
        DemoTxn(95, "user_normal", 1200, "Swiggy", "food", "Mumbai", "dev-A1", "lunch order"),
        DemoTxn(180, "user_normal", 640, "BookMyShow", "entertainment", "Mumbai", "dev-A1", "movie tickets"),
        DemoTxn(260, "user_normal", 990, "Reliance Digital", "electronics", "Mumbai", "dev-A1", "phone case"),
        DemoTxn(410, "user_normal", 1100, "Zomato", "food", "Mumbai", "dev-A1", "dinner order"),
        DemoTxn(500, "user_normal", 760, "BigBasket", "grocery", "Mumbai", "dev-A1", "evening top-up"),
    ],
)

_CARD_TESTING = Scenario(
    key="card_testing",
    title="Card-testing burst",
    icon="💳",
    summary=(
        "A stolen card is validated with a burst of tiny purchases at many merchants "
        "within one minute — the classic pattern before a big cash-out attempt."
    ),
    expected=(
        "Baseline purchases score LOW. The probes ride a proxy pool (new geo each time, new device) while "
        "the per-minute velocity counter climbs — then the cash-out lands on top of the burst window and "
        "amount-anomaly + velocity + new-geo stack to a HIGH score before the big loss clears."
    ),
    txns=[
        DemoTxn(0, "user_burst", 2100, "Amazon", "retail", "Delhi", "dev-B7", "victim's normal purchase"),
        DemoTxn(60, "user_burst", 1850, "Flipkart", "retail", "Delhi", "dev-B7", "victim's normal purchase"),
        DemoTxn(150, "user_burst", 2300, "BigBasket", "grocery", "Delhi", "dev-B7", "victim's normal purchase"),
        DemoTxn(240.0, "user_burst", 10, "GameTopUp", "digital", "Lagos", "dev-X9", "probe #1 — new device, proxy geo"),
        DemoTxn(240.1, "user_burst", 10, "GiftCardHub", "digital", "Sao Paulo", "dev-X9", "probe #2"),
        DemoTxn(240.2, "user_burst", 12, "StreamPass", "digital", "London", "dev-X9", "probe #3"),
        DemoTxn(240.3, "user_burst", 11, "CoinRecharge", "digital", "New York", "dev-X9", "probe #4"),
        DemoTxn(240.4, "user_burst", 10, "AppCredits", "digital", "Singapore", "dev-X9", "probe #5"),
        DemoTxn(240.5, "user_burst", 13, "DonatePlus", "digital", "Hanoi", "dev-X9", "probe #6"),
        DemoTxn(240.6, "user_burst", 12, "VoucherKing", "digital", "Kyiv", "dev-X9", "probe #7"),
        DemoTxn(240.7, "user_burst", 9, "TopUpNow", "digital", "Bogota", "dev-X9", "probe #8"),
        DemoTxn(240.8, "user_burst", 11, "GiftCardHub", "digital", "Lagos", "dev-X9", "probe #9"),
        DemoTxn(240.95, "user_burst", 14500, "LuxWatch Store", "luxury", "Moscow", "dev-X9", "cash-out attempt"),
    ],
)

_GEO_IMPOSSIBLE = Scenario(
    key="geo_impossible",
    title="Impossible travel",
    icon="✈️",
    summary=(
        "A card active in Mumbai suddenly transacts from London 30 minutes later — "
        "an implied speed of ~14,000 km/h — from a device never seen before."
    ),
    expected=(
        "Mumbai activity is LOW (~0.1). The first London charge trips new-geo + new-device + "
        "amount-anomaly at once and jumps to 0.75 — flagged for review — with an implied travel "
        "speed the case inspector shows is physically impossible."
    ),
    txns=[
        DemoTxn(0, "user_travel", 950, "Cafe Coffee Day", "food", "Mumbai", "dev-C3", "coffee in Mumbai"),
        DemoTxn(45, "user_travel", 1300, "DMart", "grocery", "Mumbai", "dev-C3", "groceries in Mumbai"),
        DemoTxn(75, "user_travel", 38000, "Harrods", "luxury", "London", "dev-Z1", "London, 30 min after Mumbai"),
        DemoTxn(75.5, "user_travel", 26500, "Selfridges", "luxury", "London", "dev-Z1", "second London charge"),
        DemoTxn(76, "user_travel", 41000, "Apple Regent St", "electronics", "London", "dev-Z1", "third London charge"),
    ],
)

_ACCOUNT_TAKEOVER = Scenario(
    key="account_takeover",
    title="Account takeover",
    icon="🔓",
    summary=(
        "After weeks of small, steady spending, credentials are phished and the attacker "
        "drains the account: a huge transfer from a brand-new device."
    ),
    expected=(
        "The steady history builds a tight per-user baseline (Welford mean/std), so the 60× amount "
        "spike maxes the z-score signal; the never-seen device and foreign login geo stack on top, "
        "flagging the drain at 0.75 — roughly 5× the user's baseline score."
    ),
    txns=[
        DemoTxn(0, "user_ato", 500, "Metro Card", "transport", "Bengaluru", "dev-D2", "daily commute"),
        DemoTxn(300, "user_ato", 450, "Cafe Third Wave", "food", "Bengaluru", "dev-D2", "coffee"),
        DemoTxn(600, "user_ato", 520, "Metro Card", "transport", "Bengaluru", "dev-D2", "daily commute"),
        DemoTxn(900, "user_ato", 480, "Udupi Kitchen", "food", "Bengaluru", "dev-D2", "lunch"),
        DemoTxn(1200, "user_ato", 510, "Metro Card", "transport", "Bengaluru", "dev-D2", "daily commute"),
        DemoTxn(1440, "user_ato", 30000, "QuickWire Transfer", "transfer", "Moscow", "dev-EVIL", "drain attempt — new device, foreign geo"),
    ],
)

_FRAUD_RING = Scenario(
    key="fraud_ring",
    title="Fraud ring (shared devices)",
    icon="🕸️",
    summary=(
        "Five 'different' customers cash out gift cards — but Neo4j finds they all share "
        "two devices, forming one connected component. Graph features expose the ring."
    ),
    expected=(
        "Individually each transaction looks unremarkable; the graph signals "
        "(shared_device_flags, component_size from Neo4j community detection) lift every ring member "
        "into MEDIUM+ so analysts see the network, not five isolated cases."
    ),
    txns=[
        DemoTxn(0, "mule_01", 4800, "GiftCardHub", "digital", "Lagos", "dev-RING-1", "ring member 1",
                {"ring_id": "ring-77", "shared_device_flags": 4, "component_size": 5}),
        DemoTxn(6, "mule_02", 5100, "GiftCardHub", "digital", "Lagos", "dev-RING-1", "same device as mule_01",
                {"ring_id": "ring-77", "shared_device_flags": 5, "component_size": 5}),
        DemoTxn(11, "mule_03", 4950, "VoucherKing", "digital", "Lagos", "dev-RING-2", "ring member 3",
                {"ring_id": "ring-77", "shared_device_flags": 5, "component_size": 5}),
        DemoTxn(17, "mule_04", 5200, "VoucherKing", "digital", "Lagos", "dev-RING-2", "same device as mule_03",
                {"ring_id": "ring-77", "shared_device_flags": 6, "component_size": 5}),
        DemoTxn(21, "mule_05", 5050, "GiftCardHub", "digital", "Lagos", "dev-RING-1", "back to device 1",
                {"ring_id": "ring-77", "shared_device_flags": 7, "component_size": 5}),
    ],
)

SCENARIOS: dict[str, Scenario] = {
    s.key: s for s in [_NORMAL, _CARD_TESTING, _GEO_IMPOSSIBLE, _ACCOUNT_TAKEOVER, _FRAUD_RING]
}
