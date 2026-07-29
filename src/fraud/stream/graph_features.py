"""Graph feature lookups for the stream (thin wrapper over graph.loader)."""

from __future__ import annotations

from fraud.graph.loader import query_graph
from fraud.schemas import Features, GraphInfo


def enrich_with_graph(features: Features, user_id: str) -> Features:
    """Attach ring / graph features to a Features object (best-effort)."""
    info: GraphInfo = query_graph(user_id)
    features.ring_id = info.ring_id
    features.shared_device_flags = info.shared_device_flags
    features.component_size = info.component_size
    return features
