"""Optional tenant-isolated graph memory projection."""

import asyncio
import argparse
import hashlib
import logging
import json
import threading
from typing import Any

from fastapi import Request

from .config import settings

log = logging.getLogger("mnemo.graph")


class GraphStore:
    available = False

    async def upsert_episode(self, tenant_id: str, episode: dict[str, Any]) -> None:
        raise NotImplementedError

    async def upsert_fact(self, tenant_id: str, fact: dict[str, Any], supersedes_id: str | None = None) -> None:
        raise NotImplementedError

    async def upsert_procedure(self, tenant_id: str, procedure: dict[str, Any]) -> None:
        raise NotImplementedError

    async def upsert_handoff(self, tenant_id: str, handoff: dict[str, Any]) -> None:
        raise NotImplementedError

    async def delete_handoff(self, tenant_id: str, handoff_id: str) -> None:
        raise NotImplementedError

    async def neighbors(self, tenant_id: str, user_id: str | None, seed_ids: list[str] | None = None, depth: int = 3) -> dict[str, Any]:
        return {"available": self.available, "nodes": [], "edges": []}

    async def provenance(self, tenant_id: str, fact_id: str) -> list[dict[str, Any]]:
        return []

    async def clear(self, tenant_id: str) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        return None


class NoopGraphStore(GraphStore):
    """Graph-disabled backend used by default and in offline tests."""

    async def upsert_episode(self, tenant_id: str, episode: dict[str, Any]) -> None:
        return None

    async def upsert_fact(self, tenant_id: str, fact: dict[str, Any], supersedes_id: str | None = None) -> None:
        return None

    async def upsert_procedure(self, tenant_id: str, procedure: dict[str, Any]) -> None:
        return None

    async def upsert_handoff(self, tenant_id: str, handoff: dict[str, Any]) -> None:
        return None

    async def delete_handoff(self, tenant_id: str, handoff_id: str) -> None:
        return None

    async def clear(self, tenant_id: str) -> None:
        return None


class FalkorGraphStore(GraphStore):
    available = True

    def __init__(self, host: str | None = None, port: int | None = None):
        try:
            from falkordb import FalkorDB
        except ImportError as exc:
            raise RuntimeError("Install Mnemo with `uv sync --extra graph` to use FalkorDB") from exc
        self._client_type = FalkorDB
        self._host = host or settings.falkordb_host
        self._port = port or settings.falkordb_port
        self._client = None
        self._client_lock = threading.Lock()

    @staticmethod
    def graph_name(tenant_id: str) -> str:
        digest = hashlib.sha256(tenant_id.encode("utf-8")).hexdigest()
        return f"mnemo_{digest}"

    def _query(self, tenant_id: str, query: str, params: dict[str, Any] | None = None):
        with self._client_lock:
            if self._client is None:
                self._client = self._client_type(host=self._host, port=self._port)
            client = self._client
        graph = client.select_graph(self.graph_name(tenant_id))
        return graph.query(query, params or {})

    async def _run(self, tenant_id: str, query: str, params: dict[str, Any] | None = None):
        return await asyncio.to_thread(self._query, tenant_id, query, params)

    async def upsert_episode(self, tenant_id: str, episode: dict[str, Any]) -> None:
        await self._run(tenant_id, """
            MERGE (u:User {id: $user_id})
            MERGE (a:Agent {id: $agent_id})
            MERGE (e:Episode {id: $id})
            SET e.user_id=$user_id, e.agent_id=$agent_id, e.session_id=$session_id,
                e.ts=$ts, e.event_type=$event_type, e.content=$content,
                e.outcome=$outcome, e.importance=$importance, e.archived=$archived
            MERGE (u)-[:HAS_EPISODE]->(e)
            MERGE (a)-[:PERFORMED]->(e)
        """, {key: episode.get(key) for key in ("id", "user_id", "agent_id", "session_id", "ts", "event_type", "content", "outcome", "importance", "archived")})

    async def upsert_fact(self, tenant_id: str, fact: dict[str, Any], supersedes_id: str | None = None) -> None:
        await self._run(tenant_id, """
            MERGE (u:User {id: $user_id})
            MERGE (f:Fact {id: $id})
            SET f.user_id=$user_id, f.subject=$subject, f.predicate=$predicate,
                f.object=$object, f.confidence=$confidence, f.source_episode_id=$source_episode_id,
                f.valid_from=$valid_from, f.valid_to=$valid_to, f.superseded_by=$superseded_by
            MERGE (u)-[:HAS_FACT]->(f)
        """, {key: fact.get(key) for key in ("id", "user_id", "subject", "predicate", "object", "confidence", "source_episode_id", "valid_from", "valid_to", "superseded_by")})
        if fact.get("source_episode_id"):
            await self._run(tenant_id, """
                MATCH (f:Fact {id: $fact_id}), (e:Episode {id: $episode_id})
                MERGE (f)-[:DERIVED_FROM]->(e)
            """, {"fact_id": fact["id"], "episode_id": fact["source_episode_id"]})
        if supersedes_id:
            await self._run(tenant_id, """
                MATCH (f:Fact {id: $fact_id}), (old:Fact {id: $old_id})
                MERGE (f)-[:SUPERSEDES]->(old)
            """, {"fact_id": fact["id"], "old_id": supersedes_id})

    async def upsert_procedure(self, tenant_id: str, procedure: dict[str, Any]) -> None:
        await self._run(tenant_id, """
            MERGE (p:Procedure {id: $id})
            SET p.name=$name, p.trigger_text=$trigger_text, p.steps_json=$steps_json,
                p.version=$version, p.is_active=$is_active
        """, {key: procedure.get(key) for key in ("id", "name", "trigger_text", "steps_json", "version", "is_active")})
        for agent_id in procedure.get("used_by_agent_ids", []):
            await self._run(tenant_id, """
                MERGE (a:Agent {id: $agent_id})
                MATCH (p:Procedure {id: $procedure_id})
                MERGE (a)-[:USED_PROCEDURE]->(p)
            """, {"agent_id": agent_id, "procedure_id": procedure["id"]})

    async def upsert_handoff(self, tenant_id: str, handoff: dict[str, Any]) -> None:
        await self._run(tenant_id, """
            MERGE (source_agent:Agent {id: $from_agent})
            MERGE (target_agent:Agent {id: $to_agent})
            MERGE (h:Handoff {id: $id})
            SET h.user_id=$user_id, h.session_id=$session_id, h.summary=$summary,
                h.status=$status, h.created_at=$created_at
            MERGE (source_agent)-[:HANDED_OFF_TO]->(h)
            MERGE (h)-[:HANDED_OFF_TO]->(target_agent)
        """, {key: handoff.get(key) for key in ("id", "from_agent", "to_agent", "user_id", "session_id", "summary", "status", "created_at")})

    async def delete_handoff(self, tenant_id: str, handoff_id: str) -> None:
        await self._run(tenant_id, "MATCH (h:Handoff {id: $id}) DETACH DELETE h", {"id": handoff_id})

    async def neighbors(self, tenant_id: str, user_id: str | None, seed_ids: list[str] | None = None, depth: int = 3) -> dict[str, Any]:
        depth = min(max(int(depth), 1), 3)
        if user_id is None:
            nodes_result = await self._run(tenant_id, """
                MATCH (n)
                RETURN DISTINCT labels(n) AS labels, properties(n) AS properties
                LIMIT 500
            """)
            edges_result = await self._run(tenant_id, """
                MATCH (source)-[r]->(target)
                RETURN DISTINCT type(r) AS type, source.id AS source, target.id AS target
                LIMIT 500
            """)
        else:
            params = {"user_id": user_id, "seed_ids": seed_ids or []}
            path = f"(u:User {{id: $user_id}})-[*1..{depth}]-(n)"
            where = " WHERE size($seed_ids)=0 OR any(node IN nodes(p) WHERE node.id IN $seed_ids)"
            nodes_result = await self._run(tenant_id, f"""
                MATCH p={path}{where}
                UNWIND nodes(p) AS n
                RETURN DISTINCT labels(n) AS labels, properties(n) AS properties
                LIMIT 500
            """, params)
            edges_result = await self._run(tenant_id, f"""
                MATCH p={path}{where}
                UNWIND relationships(p) AS r
                RETURN DISTINCT type(r) AS type, startNode(r).id AS source, endNode(r).id AS target
                LIMIT 500
            """, params)
        return {
            "available": True,
            "nodes": [{"labels": row[0], **row[1]} for row in nodes_result.result_set],
            "edges": [{"type": row[0], "source": row[1], "target": row[2]} for row in edges_result.result_set],
        }

    async def provenance(self, tenant_id: str, fact_id: str) -> list[dict[str, Any]]:
        result = await self._run(tenant_id, """
            MATCH (f:Fact {id: $id})-[:DERIVED_FROM]->(e:Episode)<-[:PERFORMED]-(a:Agent)
            RETURN properties(f) AS fact, properties(e) AS episode, properties(a) AS agent
        """, {"id": fact_id})
        return [{"fact": row[0], "episode": row[1], "agent": row[2]} for row in result.result_set]

    async def clear(self, tenant_id: str) -> None:
        await self._run(tenant_id, "MATCH (n) DETACH DELETE n")

    async def close(self) -> None:
        close = getattr(self._client, "close", None) if self._client else None
        if close:
            await asyncio.to_thread(close)


def create_graph_store() -> GraphStore:
    backend = settings.graph_backend.lower()
    if backend == "none":
        return NoopGraphStore()
    if backend == "falkordb":
        return FalkorGraphStore()
    raise ValueError(f"Unsupported GRAPH_BACKEND: {settings.graph_backend}")


async def get_graph(request: Request) -> GraphStore:
    return request.app.state.graph


async def write_through(graph: GraphStore, operation: str, tenant_id: str, *args: Any) -> None:
    if not graph.available:
        return
    try:
        await getattr(graph, operation)(tenant_id, *args)
    except Exception:
        log.exception("Graph write failed; SQLite remains authoritative", extra={"tenant_id": tenant_id, "operation": operation})


async def rebuild_from_sqlite(graph: GraphStore, db, tenant_id: str) -> dict[str, int]:
    if not graph.available:
        raise RuntimeError("Set GRAPH_BACKEND=falkordb before rebuilding a tenant graph")
    if not await db.fetchone("SELECT id FROM tenants WHERE id=?", (tenant_id,)):
        raise ValueError("Tenant not found")
    episodes = [dict(row) for row in await db.fetchall("SELECT * FROM episodes WHERE tenant_id=?", (tenant_id,))]
    facts = [dict(row) for row in await db.fetchall("SELECT * FROM facts WHERE tenant_id=?", (tenant_id,))]
    procedures = [dict(row) for row in await db.fetchall("SELECT * FROM procedures WHERE tenant_id=?", (tenant_id,))]
    usage = await db.fetchall("SELECT procedure_id,agent_id FROM procedure_usage WHERE tenant_id=?", (tenant_id,))
    handoffs = [dict(row) for row in await db.fetchall("SELECT * FROM handoffs WHERE tenant_id=?", (tenant_id,))]

    await graph.clear(tenant_id)
    for episode in episodes:
        await graph.upsert_episode(tenant_id, episode)
    predecessor_by_new = {fact["superseded_by"]: fact["id"] for fact in facts if fact["superseded_by"]}
    for fact in facts:
        await graph.upsert_fact(tenant_id, fact)
    for fact in facts:
        predecessor = predecessor_by_new.get(fact["id"])
        if predecessor:
            await graph.upsert_fact(tenant_id, fact, predecessor)
    used_by = {}
    for row in usage:
        used_by.setdefault(row["procedure_id"], []).append(row["agent_id"])
    for procedure in procedures:
        procedure["used_by_agent_ids"] = used_by.get(procedure["id"], [])
        await graph.upsert_procedure(tenant_id, procedure)
    for handoff in handoffs:
        await graph.upsert_handoff(tenant_id, handoff)
    return {"episodes": len(episodes), "facts": len(facts), "procedures": len(procedures), "handoffs": len(handoffs)}


async def _rebuild_command(tenant_id: str) -> dict[str, int]:
    from .db import Database

    db = Database(settings.database_path)
    graph = create_graph_store()
    try:
        await db.open()
        return await rebuild_from_sqlite(graph, db, tenant_id)
    finally:
        await db.close()
        await graph.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild a tenant's FalkorDB graph from SQLite")
    subparsers = parser.add_subparsers(dest="command", required=True)
    rebuild = subparsers.add_parser("rebuild")
    rebuild.add_argument("--tenant-id", required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(_rebuild_command(args.tenant_id)), indent=2))


if __name__ == "__main__":
    main()