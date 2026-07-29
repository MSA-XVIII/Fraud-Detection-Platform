"""Pydantic data contracts shared across the platform.

These models are the single source of truth for every event and document that
flows through Kafka, the stream processor, MongoDB, and the LLM analyst layer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RiskBand(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class CaseStatus(str, Enum):
    OPEN = "open"
    IN_REVIEW = "in_review"
    CLOSED = "closed"


class Geo(BaseModel):
    lat: float
    lon: float
    country: str


class TransactionEvent(BaseModel):
    """Raw event published to the Kafka `raw-transactions` topic."""

    txn_id: str
    user_id: str
    amount: float = Field(ge=0)
    currency: str = "INR"
    merchant: str
    merchant_category: str
    geo: Geo
    device_id: str
    ip: str
    timestamp: datetime = Field(default_factory=_utcnow)

    @field_validator("timestamp", mode="before")
    @classmethod
    def _parse_ts(cls, v: Any) -> Any:
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v


class Features(BaseModel):
    """Rolling-window + graph features computed by the stream processor."""

    amt_z: float = 0.0
    txn_1m: int = 0
    txn_5m: int = 0
    txn_1h: int = 0
    distinct_merchants_1h: int = 0
    distinct_geos_1h: int = 0
    seconds_since_last: float = 0.0
    new_geo: bool = False
    new_device: bool = False
    # graph features
    ring_id: str | None = None
    shared_device_flags: int = 0
    component_size: int = 0

    def to_vector(self) -> list[float]:
        """Ordered numeric feature vector for the ML model."""
        return [
            self.amt_z,
            float(self.txn_1m),
            float(self.txn_5m),
            float(self.txn_1h),
            float(self.distinct_merchants_1h),
            float(self.distinct_geos_1h),
            self.seconds_since_last,
            float(self.new_geo),
            float(self.new_device),
            float(self.shared_device_flags),
            float(self.component_size),
        ]

    @staticmethod
    def feature_names() -> list[str]:
        return [
            "amt_z",
            "txn_1m",
            "txn_5m",
            "txn_1h",
            "distinct_merchants_1h",
            "distinct_geos_1h",
            "seconds_since_last",
            "new_geo",
            "new_device",
            "shared_device_flags",
            "component_size",
        ]


class GraphInfo(BaseModel):
    ring_id: str | None = None
    shared_device_flags: int = 0
    component_size: int = 0


class ScoredTransaction(BaseModel):
    """Document persisted to Mongo `flagged_transactions` (`_id == txn_id`)."""

    id: str = Field(alias="_id")
    user_id: str
    amount: float
    currency: str = "INR"
    merchant: str
    risk_score: float
    risk_band: RiskBand
    features: dict[str, Any] = Field(default_factory=dict)
    graph: GraphInfo = Field(default_factory=GraphInfo)
    shap_top: list[tuple[str, float]] = Field(default_factory=list)
    geo: Geo | None = None
    device_id: str | None = None
    status: CaseStatus = CaseStatus.OPEN
    label: bool | None = None
    model_version: str | None = None
    created_at: datetime = Field(default_factory=_utcnow)

    model_config = {"populate_by_name": True}

    @property
    def expected_loss(self) -> float:
        return self.risk_score * self.amount

    def to_mongo(self) -> dict[str, Any]:
        doc = self.model_dump(by_alias=True, mode="json")
        return doc


class Alert(BaseModel):
    """Alert lifecycle document for the review queue."""

    id: str = Field(alias="_id")
    txn_id: str
    user_id: str
    risk_band: RiskBand
    expected_loss: float
    status: CaseStatus = CaseStatus.OPEN
    priority: int = 0
    created_at: datetime = Field(default_factory=_utcnow)

    model_config = {"populate_by_name": True}


class CaseSummary(BaseModel):
    """Output of the LangChain case-summary / RAG chain."""

    txn_id: str
    summary: str
    cited_case_ids: list[str] = Field(default_factory=list)
    grounded: bool = True
    model: str = "fake"
    created_at: datetime = Field(default_factory=_utcnow)


class UserRollingState(BaseModel):
    """Latest per-user aggregates (`_id == user_id`), idempotently upserted."""

    id: str = Field(alias="_id")
    count: int = 0
    amount_mean: float = 0.0
    amount_m2: float = 0.0  # Welford's aggregate for variance
    last_ts: datetime | None = None
    seen_geos: list[str] = Field(default_factory=list)
    seen_devices: list[str] = Field(default_factory=list)
    recent_amounts: list[float] = Field(default_factory=list)
    recent_ts: list[datetime] = Field(default_factory=list)

    model_config = {"populate_by_name": True}

    @property
    def amount_std(self) -> float:
        if self.count < 2:
            return 0.0
        return (self.amount_m2 / (self.count - 1)) ** 0.5
