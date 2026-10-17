"""Graph projection behavior without requiring a FalkorDB service."""

from collections import defaultdict
from copy import deepcopy

import pytest

from mnemo.config import settings
from mnemo.graph import FalkorGraphStore, GraphStore, rebuild_from_sqlite
from tests.conftest import create_tenant


class MemoryGraphStore(GraphStore):
    available = True

    def __init__(self):
        self.nodes = defaultdict(dict)
        self.edges = defaultdict(set)

    def _node(self, tenant_id, label, props):
        self.nodes[tenant_id][props["id"]] = {"labels": [label], **props}

    def _edge(self, tenant_id, source, kind, target):
        self.edges[tenant_id].add((source, kind, target))

    async def upsert_episode(self, tenant_id, episode):
        self._node(tenant_id, "User", {"id": episode["user_id"]})
        self._node(tenant_id, "Agent", {"id": episode["agent_id"]})
        self._node(tenant_id, "Episode", episode)
        self._edge(tenant_id, episode["user_id"], "HAS_EPISODE", episode["id"])
        self._edge(tenant_id, episode["agent_id"], "PERFORMED", episode["id"])

    async def upsert_fact(self, tenant_id, fact, supersedes_id=None):
        self._node(tenant_id, "User", {"id": fact["user_id"]})
        self._node(tenant_id, "Fact", fact)
        self._edge(tenant_id, fact["user_id"], "HAS_FACT", fact["id"])
        if fact.get("source_episode_id") in self.nodes[tenant_id]:
            self._edge(tenant_id, fact["id"], "DERIVED_FROM", fact["source_episode_id"])
        if supersedes_id in self.nodes[tenant_id]:
            self._edge(tenant_id, fact["id"], "SUPERSEDES", supersedes_id)

    async def upsert_procedure(self, tenant_id, procedure):
        self._node(tenant_id, "Procedure", {key: procedure[key] for key in ("id", "name", "trigger_text", "steps_json", "version", "is_active")})
        for agent_id in procedure.get("used_by_agent_ids", []):
            self._node(tenant_id, "Agent", {"id": agent_id})
            self._edge(tenant_id, agent_id, "USED_PROCEDURE", procedure["id"])

    async def upsert_handoff(self, tenant_id, handoff):
        self._node(tenant_id, "Agent", {"id": handoff["from_agent"]})
        self._node(tenant_id, "Agent", {"id": handoff["to_agent"]})
        self._node(tenant_id, "Handoff", handoff)
        self._edge(tenant_id, handoff["from_agent"], "HANDED_OFF_TO", handoff["id"])
        self._edge(tenant_id, handoff["id"], "HANDED_OFF_TO", handoff["to_agent"])

    async def delete_handoff(self, tenant_id, handoff_id):
        self.nodes[tenant_id].pop(handoff_id, None)
        self.edges[tenant_id] = {edge for edge in self.edges[tenant_id] if handoff_id not in (edge[0], edge[2])}

    async def neighbors(self, tenant_id, user_id=None, seed_ids=None, depth=3):
        if user_id is None:
            return {
                "available": True,
                "nodes": [deepcopy(node) for node in self.nodes[tenant_id].values()],
                "edges": [{"source": source, "type": kind, "target": target} for source, kind, target in self.edges[tenant_id]],
            }
        seed_ids = set(seed_ids or [])
        adjacency = defaultdict(list)
        for edge in self.edges[tenant_id]:
            source, _, target = edge
            adjacency[source].append((target, edge))
            adjacency[target].append((source, edge))
        paths = [([user_id], [], user_id in seed_ids)]
        found_nodes, found_edges = set(), set()
        for _ in range(min(depth, 3)):
            next_paths = []
            for path, path_edges, seeded in paths:
                for neighbor, edge in adjacency[path[-1]]:
                    if neighbor in path:
                        continue
                    next_seeded = seeded or neighbor in seed_ids
                    new_path, new_edges = [*path, neighbor], [*path_edges, edge]
                    if not seed_ids or next_seeded:
                        found_nodes.update(new_path)
                        found_edges.update(new_edges)
                    next_paths.append((new_path, new_edges, next_seeded))
            paths = next_paths
        return {
            "available": True,
            "nodes": [deepcopy(self.nodes[tenant_id][node_id]) for node_id in found_nodes if node_id in self.nodes[tenant_id]],
            "edges": [{"source": source, "type": kind, "target": target} for source, kind, target in found_edges],
        }

    async def provenance(self, tenant_id, fact_id):
        by_source = {source: target for source, kind, target in self.edges[tenant_id] if kind == "DERIVED_FROM"}
        by_episode = {target: source for source, kind, target in self.edges[tenant_id] if kind == "PERFORMED"}
        episode_id = by_source.get(fact_id)
        agent_id = by_episode.get(episode_id)
        if not episode_id or not agent_id:
            return []
        return [{"fact": deepcopy(self.nodes[tenant_id][fact_id]), "episode": deepcopy(self.nodes[tenant_id][episode_id]), "agent": deepcopy(self.nodes[tenant_id][agent_id])}]

    async def clear(self, tenant_id):
        self.nodes[tenant_id].clear()
        self.edges[tenant_id].clear()


def test_tenant_graph_names_are_distinct_and_do_not_expose_ids():
    first = "03b428b9-54a4-4cea-bbfb-d89e5a5bd6e4"
    second = "0a9e122d-8570-4795-b891-9a9793096828"
    first_name = FalkorGraphStore.graph_name(first)
    second_name = FalkorGraphStore.graph_name(second)
    assert first_name != second_name
    assert first not in first_name and second not in second_name


@pytest.mark.asyncio
async def test_graph_tenant_isolation_supersession_and_rebuild(app, client):
    tenant_a = await create_tenant(client, "Graph A")
    tenant_b = await create_tenant(client, "Graph B")
    headers_a = {"X-API-Key": tenant_a["api_key"]}
    headers_b = {"X-API-Key": tenant_b["api_key"]}
    graph = app.state.graph = MemoryGraphStore()

    episode = (await client.post("/v1/episodes", headers=headers_a, json={"user_id": "alice", "content": "Plan discussion"})).json()
    old = (await client.post("/v1/facts", headers=headers_a, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Pro"})).json()
    new = (await client.post("/v1/facts", headers=headers_a, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Enterprise"})).json()
    procedure = (await client.post("/v1/procedures", headers=headers_a, json={"name": "Plan review", "trigger_text": "plan change", "steps": ["confirm"]})).json()
    await client.post(f"/v1/procedures/{procedure['id']}/outcome", headers=headers_a, json={"success": True})
    await client.post("/v1/handoffs", headers=headers_a, json={"to_agent": "billing", "user_id": "alice", "session_id": "s1", "summary": "Review plan"})

    assert (new["id"], "SUPERSEDES", old["id"]) in graph.edges[tenant_a["tenant_id"]]
    assert ("triage", "USED_PROCEDURE", procedure["id"]) in graph.edges[tenant_a["tenant_id"]]
    assert graph.nodes[tenant_a["tenant_id"]][episode["id"]]["labels"] == ["Episode"]
    whole_tenant = (await client.get("/v1/graph/neighbors", headers=headers_a)).json()
    assert {node["id"] for node in whole_tenant["nodes"]} >= {episode["id"], old["id"], new["id"], procedure["id"]}
    assert (await client.get("/v1/graph/neighbors?user_id=alice", headers=headers_b)).json()["nodes"] == []

    before = (deepcopy(graph.nodes[tenant_a["tenant_id"]]), deepcopy(graph.edges[tenant_a["tenant_id"]]))
    rebuilt = await rebuild_from_sqlite(graph, app.state.db, tenant_a["tenant_id"])
    after = (graph.nodes[tenant_a["tenant_id"]], graph.edges[tenant_a["tenant_id"]])
    assert after == before
    assert rebuilt == {"episodes": 1, "facts": 2, "procedures": 1, "handoffs": 1}


@pytest.mark.asyncio
async def test_recall_adds_graph_related_fact_missed_by_vector_results(app, client, monkeypatch):
    tenant = await create_tenant(client, "Graph recall")
    headers = {"X-API-Key": tenant["api_key"]}
    graph = app.state.graph = MemoryGraphStore()
    episode = (await client.post("/v1/episodes", headers=headers, json={"user_id": "alice", "content": "A long support conversation"})).json()
    fact = (await client.post("/v1/facts", headers=headers, json={"user_id": "alice", "subject": "alice", "predicate": "travel_companion", "object": "Mochi the cat"})).json()
    await app.state.db.execute("UPDATE facts SET source_episode_id=? WHERE tenant_id=? AND id=?", (episode["id"], tenant["tenant_id"], fact["id"]))
    stored_fact = dict(await app.state.db.fetchone("SELECT * FROM facts WHERE tenant_id=? AND id=?", (tenant["tenant_id"], fact["id"])))
    await graph.upsert_fact(tenant["tenant_id"], stored_fact)

    async def no_vector_fact_matches(*args, **kwargs):
        return []

    async def matched_episode(*args, **kwargs):
        return [{**episode, "score": 1.0}]

    import mnemo.api.routes as routes
    monkeypatch.setattr(routes, "search_facts", no_vector_fact_matches)
    monkeypatch.setattr(routes, "search_episodes", matched_episode)

    result = (await client.post("/v1/recall", headers=headers, json={"user_id": "alice", "query": "support conversation", "budget_tokens": 2000})).json()
    assert any(item["id"] == fact["id"] and item["related"] for item in result["facts"])
    provenance = await client.get(f"/v1/graph/provenance/{fact['id']}", headers=headers)
    assert provenance.json()["chain"][0]["episode"]["id"] == episode["id"]
    assert provenance.json()["chain"][0]["agent"]["id"] == "triage"


@pytest.mark.asyncio
async def test_graph_failure_does_not_fail_sqlite_write(app, client):
    tenant = await create_tenant(client, "Graph failure")
    headers = {"X-API-Key": tenant["api_key"]}

    class BrokenGraph(MemoryGraphStore):
        async def upsert_fact(self, tenant_id, fact, supersedes_id=None):
            raise ConnectionError("graph unavailable")

    app.state.graph = BrokenGraph()
    response = await client.post("/v1/facts", headers=headers, json={"user_id": "alice", "subject": "alice", "predicate": "timezone", "object": "UTC"})
    assert response.status_code == 201
    assert await app.state.db.fetchone("SELECT id FROM facts WHERE tenant_id=? AND id=?", (tenant["tenant_id"], response.json()["id"]))


@pytest.mark.skipif(settings.graph_backend != "falkordb", reason="requires GRAPH_BACKEND=falkordb and a live FalkorDB service")
@pytest.mark.asyncio
async def test_live_falkordb_isolation_supersession_and_rebuild(app, client):
    tenant_a = await create_tenant(client, "Live Graph A")
    tenant_b = await create_tenant(client, "Live Graph B")
    headers_a = {"X-API-Key": tenant_a["api_key"]}
    headers_b = {"X-API-Key": tenant_b["api_key"]}
    episode = (await client.post("/v1/episodes", headers=headers_a, json={"user_id": "alice", "content": "Plan discussion"})).json()
    old = (await client.post("/v1/facts", headers=headers_a, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Pro"})).json()
    new = (await client.post("/v1/facts", headers=headers_a, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Enterprise"})).json()

    before = await app.state.graph.neighbors(tenant_a["tenant_id"], "alice")
    assert {node["id"] for node in before["nodes"]} >= {episode["id"], old["id"], new["id"]}
    assert any(edge["type"] == "SUPERSEDES" and edge["source"] == new["id"] and edge["target"] == old["id"] for edge in before["edges"])
    assert (await app.state.graph.neighbors(tenant_b["tenant_id"], "alice"))["nodes"] == []

    counts = await rebuild_from_sqlite(app.state.graph, app.state.db, tenant_a["tenant_id"])
    after = await app.state.graph.neighbors(tenant_a["tenant_id"], "alice")
    assert counts["episodes"] == 1 and counts["facts"] == 2
    assert {node["id"] for node in after["nodes"]} == {node["id"] for node in before["nodes"]}
    assert {(edge["type"], edge["source"], edge["target"]) for edge in after["edges"]} == {(edge["type"], edge["source"], edge["target"]) for edge in before["edges"]}
