"""Neo4j Aura graph layer: ETL, community detection, and ring lookups.

Graph model:  (:User)-[:USED]->(:Device)
              (:User)-[:PAID]->(:Merchant)
              (:User)-[:FROM_IP]->(:IP)

Community detection assigns a ``ring_id`` per connected component and computes
``component_size`` and ``shared_device_flags``. Uses Neo4j GDS when the Aura tier
supports it (NEO4J_USE_GDS=true), otherwise a networkx fallback over the exported
graph. All queries are safe no-ops when Aura is not configured, so the rest of the
platform keeps running locally.
"""

from __future__ import annotations

import argparse
import random
from contextlib import contextmanager
from typing import Any

from config.settings import settings
from fraud.logging_config import get_logger
from fraud.schemas import GraphInfo

log = get_logger("graph")

_driver: Any = None


def _get_driver() -> Any:
    """Return a cached Neo4j driver, or None if Aura is not configured."""
    global _driver
    if not settings.neo4j_configured:
        return None
    if _driver is None:
        from neo4j import GraphDatabase

        _driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_username, settings.neo4j_password),
        )
        _driver.verify_connectivity()
        log.info("neo4j_connected", uri=settings.neo4j_uri)
    return _driver


@contextmanager
def _session():  # noqa: ANN202
    driver = _get_driver()
    if driver is None:
        raise RuntimeError("Neo4j Aura not configured (set NEO4J_URI / NEO4J_PASSWORD)")
    with driver.session(database=settings.neo4j_database) as session:
        yield session


def ensure_constraints() -> None:
    if not settings.neo4j_configured:
        log.warning("neo4j_not_configured_skipping_constraints")
        return
    stmts = [
        "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
        "CREATE CONSTRAINT device_id IF NOT EXISTS FOR (d:Device) REQUIRE d.id IS UNIQUE",
        "CREATE CONSTRAINT merchant_id IF NOT EXISTS FOR (m:Merchant) REQUIRE m.id IS UNIQUE",
        "CREATE CONSTRAINT ip_id IF NOT EXISTS FOR (i:IP) REQUIRE i.id IS UNIQUE",
    ]
    with _session() as s:
        for stmt in stmts:
            s.run(stmt)
    log.info("neo4j_constraints_ready")


def upsert_transaction_edges(
    user_id: str, device_id: str, merchant: str, ip: str, flagged: bool = False
) -> None:
    """Idempotently MERGE the entity graph for one transaction."""
    if not settings.neo4j_configured:
        return
    query = """
    MERGE (u:User {id: $user_id})
      ON CREATE SET u.flagged = $flagged
      ON MATCH SET u.flagged = u.flagged OR $flagged
    MERGE (d:Device {id: $device_id})
    MERGE (m:Merchant {id: $merchant})
    MERGE (i:IP {id: $ip})
    MERGE (u)-[:USED]->(d)
    MERGE (u)-[:PAID]->(m)
    MERGE (u)-[:FROM_IP]->(i)
    """
    with _session() as s:
        s.run(
            query,
            user_id=user_id,
            device_id=device_id,
            merchant=merchant,
            ip=ip,
            flagged=flagged,
        )


def compute_communities() -> dict[str, Any]:
    """Assign ring_id / component_size via GDS or a networkx fallback."""
    if not settings.neo4j_configured:
        return {"status": "skipped", "reason": "neo4j_not_configured"}
    if settings.neo4j_use_gds:
        return _compute_communities_gds()
    return _compute_communities_networkx()


def _compute_communities_gds() -> dict[str, Any]:
    project = """
    CALL gds.graph.project.cypher(
      'fraud-graph',
      'MATCH (n) WHERE n:User OR n:Device OR n:Merchant OR n:IP RETURN id(n) AS id',
      'MATCH (a)--(b) RETURN id(a) AS source, id(b) AS target'
    ) YIELD graphName
    """
    write = """
    CALL gds.wcc.write('fraud-graph', {writeProperty: 'ring_component'})
    YIELD componentCount RETURN componentCount
    """
    drop = "CALL gds.graph.drop('fraud-graph', false) YIELD graphName RETURN graphName"
    with _session() as s:
        s.run("CALL gds.graph.drop('fraud-graph', false) YIELD graphName RETURN graphName").consume()
        s.run(project).consume()
        rec = s.run(write).single()
        s.run(drop).consume()
        # stamp ring_id string + component_size onto User nodes
        s.run(
            """
            MATCH (u:User)
            WITH u.ring_component AS comp, collect(u) AS users
            UNWIND users AS u
            SET u.ring_id = 'R_' + toString(comp), u.component_size = size(users)
            """
        ).consume()
    count = rec["componentCount"] if rec else 0
    log.info("gds_communities", components=count)
    return {"status": "ok", "engine": "gds", "components": count}


def _compute_communities_networkx() -> dict[str, Any]:
    import networkx as nx

    g = nx.Graph()
    with _session() as s:
        rows = s.run(
            "MATCH (a)--(b) WHERE a.id IS NOT NULL AND b.id IS NOT NULL "
            "RETURN labels(a)[0]+':'+a.id AS a, labels(b)[0]+':'+b.id AS b"
        )
        for r in rows:
            g.add_edge(r["a"], r["b"])

    updates: list[dict[str, Any]] = []
    for idx, comp in enumerate(nx.connected_components(g)):
        ring_id = f"R_{idx}"
        size = len(comp)
        for node in comp:
            if node.startswith("User:"):
                updates.append({"uid": node.split(":", 1)[1], "ring": ring_id, "size": size})

    with _session() as s:
        s.run(
            """
            UNWIND $updates AS row
            MATCH (u:User {id: row.uid})
            SET u.ring_id = row.ring, u.component_size = row.size
            """,
            updates=updates,
        ).consume()
    log.info("networkx_communities", components=g.number_of_nodes() and len(updates))
    return {"status": "ok", "engine": "networkx", "users_updated": len(updates)}


def query_graph(user_id: str) -> GraphInfo:
    """Return ring / shared-entity info for a user (features + agent tool)."""
    if not settings.neo4j_configured:
        return GraphInfo()
    query = """
    MATCH (u:User {id: $user_id})
    OPTIONAL MATCH (u)-[:USED]->(d:Device)<-[:USED]-(other:User)
    WHERE other.id <> u.id AND other.flagged = true
    RETURN u.ring_id AS ring_id,
           coalesce(u.component_size, 0) AS component_size,
           count(DISTINCT other) AS shared_device_flags
    """
    try:
        with _session() as s:
            rec = s.run(query, user_id=user_id).single()
        if rec is None:
            return GraphInfo()
        return GraphInfo(
            ring_id=rec["ring_id"],
            component_size=int(rec["component_size"] or 0),
            shared_device_flags=int(rec["shared_device_flags"] or 0),
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("query_graph_failed", error=str(exc))
        return GraphInfo()


def seed_demo_graph() -> None:
    """Create a couple of obvious fraud rings sharing devices/IPs for the demo."""
    if not settings.neo4j_configured:
        log.warning("neo4j_not_configured_cannot_seed")
        return
    ensure_constraints()
    # Ring A: 5 users share one device + IP
    shared_device = "dev_RINGA"
    shared_ip = "10.0.0.99"
    for i in range(5):
        upsert_transaction_edges(
            f"ring_a_user_{i}", shared_device, random.choice(["Amazon", "Steam"]),
            shared_ip, flagged=True,
        )
    # Ring B: 4 users share one IP
    shared_ip_b = "10.0.0.77"
    for i in range(4):
        upsert_transaction_edges(
            f"ring_b_user_{i}", f"dev_B{i}", "Netflix", shared_ip_b, flagged=True
        )
    compute_communities()
    log.info("demo_graph_seeded")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", action="store_true", help="seed demo fraud rings")
    ap.add_argument("--load", action="store_true", help="recompute communities")
    args = ap.parse_args()
    if args.seed:
        seed_demo_graph()
    elif args.load:
        ensure_constraints()
        print(compute_communities())
    else:
        ap.print_help()
