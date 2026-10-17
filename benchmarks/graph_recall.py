"""Compare vector-only recall with FalkorDB multi-hop graph expansion."""

import asyncio
import json
import math
import os
import statistics
import tempfile
import time
from pathlib import Path

os.environ.setdefault("EMBEDDER", "fake")
os.environ.setdefault("LLM", "mock")

import httpx

from mnemo.config import settings
from mnemo.graph import NoopGraphStore
from mnemo.main import create_app

ROOT = Path(__file__).resolve().parent / "results"
TRIALS = 30


def percentile(values: list[float], fraction: float) -> float:
    return sorted(values)[min(math.ceil(len(values) * fraction) - 1, len(values) - 1)]


async def run() -> dict:
    if settings.graph_backend != "falkordb":
        raise RuntimeError("Run with GRAPH_BACKEND=falkordb and a live FalkorDB service")
    ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mnemo-graph-recall-") as temp:
        object.__setattr__(settings, "data_dir", Path(temp) / "tenant-indexes")
        app = create_app(Path(temp) / "graph-recall.sqlite3")
        async with app.router.lifespan_context(app):
            graph = app.state.graph
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://bench") as client:
                tenant = (await client.post("/admin/tenants", headers={"X-Admin-Key": settings.admin_key}, json={"name": "Graph recall benchmark"})).json()
                headers = {"X-API-Key": tenant["api_key"]}
                try:
                    await graph.neighbors(tenant["tenant_id"], "benchmark-probe", depth=1)
                except Exception as exc:
                    unavailable = {
                        "status": "unavailable",
                        "backend": "falkordb",
                        "reason": "FalkorDB is not reachable; start docker compose before benchmarking",
                        "trials_requested": TRIALS,
                    }
                    (ROOT / "graph_recall.json").write_text(json.dumps(unavailable, indent=2) + "\n")
                    raise RuntimeError("FalkorDB is not reachable; start docker compose before benchmarking") from exc
                episode = (await client.post("/v1/episodes", headers=headers, json={
                    "user_id": "alice",
                    "session_id": "multi-hop",
                    "content": "Alice asks for hotel options and discusses an emergency plan.",
                })).json()
                target = (await client.post("/v1/facts", headers=headers, json={
                    "user_id": "alice",
                    "subject": "alice",
                    "predicate": "emergency_kit",
                    "object": "Carries an epinephrine injector in the travel bag",
                })).json()
                await app.state.db.execute(
                    "UPDATE facts SET source_episode_id=? WHERE tenant_id=? AND id=?",
                    (episode["id"], tenant["tenant_id"], target["id"]),
                )
                target_row = dict(await app.state.db.fetchone(
                    "SELECT * FROM facts WHERE tenant_id=? AND id=?",
                    (tenant["tenant_id"], target["id"]),
                ))
                await graph.upsert_fact(tenant["tenant_id"], target_row)
                for index in range(40):
                    await client.post("/v1/facts", headers=headers, json={
                        "user_id": "alice",
                        "subject": "alice",
                        "predicate": f"hotel_allergy_note_{index}",
                        "object": f"Hotel allergy travel note number {index}",
                    })

                query = "hotel allergy travel options"
                graph_hits = []
                graph_latencies = []
                vector_hits = []
                vector_latencies = []
                app.state.graph = NoopGraphStore()
                for _ in range(TRIALS):
                    started = time.perf_counter()
                    response = await client.post("/v1/recall", headers=headers, json={"user_id": "alice", "query": query, "budget_tokens": 5000})
                    response.raise_for_status()
                    vector_latencies.append((time.perf_counter() - started) * 1000)
                    vector_hits.append(any(item["id"] == target["id"] for item in response.json()["facts"]))

                app.state.graph = graph
                for _ in range(TRIALS):
                    started = time.perf_counter()
                    response = await client.post("/v1/recall", headers=headers, json={"user_id": "alice", "query": query, "budget_tokens": 5000})
                    response.raise_for_status()
                    graph_latencies.append((time.perf_counter() - started) * 1000)
                    graph_hits.append(any(item["id"] == target["id"] for item in response.json()["facts"]))

                report = {
                    "backend": "falkordb",
                    "embedder": settings.embedder,
                    "multi_hop_question": query,
                    "graph_hops": 3,
                    "distractor_facts": 40,
                    "trials": TRIALS,
                    "target_fact_returned": {
                        "vector_only": sum(vector_hits),
                        "with_graph": sum(graph_hits),
                        "of": TRIALS,
                    },
                    "recall_rate": {
                        "vector_only": sum(vector_hits) / TRIALS,
                        "with_graph": sum(graph_hits) / TRIALS,
                    },
                    "latency_ms_median": {
                        "vector_only": round(statistics.median(vector_latencies), 3),
                        "with_graph": round(statistics.median(graph_latencies), 3),
                    },
                    "latency_ms_p95": {
                        "vector_only": round(percentile(vector_latencies, 0.95), 3),
                        "with_graph": round(percentile(graph_latencies, 0.95), 3),
                    },
                    "target_fact_id": target["id"],
                    "related_episode_id": episode["id"],
                }
                (ROOT / "graph_recall.json").write_text(json.dumps(report, indent=2) + "\n")
                return report


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run()), indent=2))
