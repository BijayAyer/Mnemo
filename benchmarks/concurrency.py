"""Mixed read/write contention benchmark; defaults to 100 agents for 60 seconds."""

import argparse
import asyncio
import json
import os
import random
import statistics
import tempfile
import time
from pathlib import Path

os.environ.setdefault("EMBEDDER", "fake")
os.environ.setdefault("LLM", "mock")

import httpx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from mnemo.auth import hash_key
from mnemo.config import settings
from mnemo.main import create_app

ROOT = Path(__file__).resolve().parent / "results"


async def run(seconds: int, agent_count: int) -> dict:
    ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mnemo-concurrency-") as temp:
        object.__setattr__(settings, "data_dir", Path(temp) / "tenant-indexes")
        app = create_app(Path(temp) / "bench.sqlite3")
        transport = httpx.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            admin = httpx.AsyncClient(transport=transport, base_url="http://bench")
            tenant_clients = []
            agents = []
            for tenant_number in range(5):
                tenant = (await admin.post("/admin/tenants", headers={"X-Admin-Key": settings.admin_key}, json={"name": f"Tenant {tenant_number}", "agent_id": "setup"})).json()
                await admin.put("/v1/blackboard/counter", headers={"X-API-Key": tenant["api_key"]}, json={"value": 0, "expected_version": 0})
                marker = f"tenant-marker-{tenant_number}"
                await admin.post("/v1/episodes", headers={"X-API-Key": tenant["api_key"]}, json={"user_id": f"marker-{tenant_number}", "content": marker})
                tenant_clients.append((tenant["tenant_id"], marker))
                for agent_number in range(agent_count // 5):
                    key = f"bench-{tenant_number}-{agent_number}-{random.randrange(1 << 30)}"
                    await app.state.db.insert("INSERT INTO api_keys(key_hash,tenant_id,agent_id,role,created_at) VALUES(?,?,?,?,?)", (hash_key(key), tenant["tenant_id"], f"agent-{agent_number}", "agent", "now"))
                    agents.append((tenant_number, httpx.AsyncClient(transport=transport, base_url="http://bench", headers={"X-API-Key": key}, timeout=30)))

            latencies: list[float] = []
            failures = 0
            cas_conflicts = 0
            increments = [0] * 5
            end_time = time.perf_counter() + seconds

            async def request(client, method, path, **kwargs):
                nonlocal failures
                start = time.perf_counter()
                response = await client.request(method, path, **kwargs)
                latencies.append(time.perf_counter() - start)
                if response.status_code >= 400 and response.status_code != 409:
                    failures += 1
                return response

            async def worker(index, tenant_number, client):
                nonlocal cas_conflicts
                sequence = 0
                while time.perf_counter() < end_time:
                    choice = random.random()
                    if choice < .7:
                        await request(client, "GET", "/v1/episodes?limit=10")
                    elif choice < .85:
                        await request(client, "POST", "/v1/episodes", json={"user_id": "bench", "session_id": str(index), "content": f"agent {index} item {sequence}"})
                    else:
                        for attempt in range(100):
                            current = await request(client, "GET", "/v1/blackboard/counter")
                            if current.status_code != 200:
                                break
                            state = current.json()
                            updated = await request(client, "PUT", "/v1/blackboard/counter", json={"value": state["value"] + 1, "expected_version": state["version"]})
                            if updated.status_code == 200:
                                increments[tenant_number] += 1
                                break
                            if updated.status_code == 409:
                                cas_conflicts += 1
                                continue
                            break
                    sequence += 1

            started = time.perf_counter()
            await asyncio.gather(*(worker(index, tenant, client) for index, (tenant, client) in enumerate(agents)))
            elapsed = time.perf_counter() - started
            final_values, isolation_ok = [], True
            for tenant_number, (tenant_id, marker) in enumerate(tenant_clients):
                state = await admin.get("/v1/blackboard/counter", headers={"X-API-Key": agents[tenant_number * (agent_count // 5)][1].headers["X-API-Key"]})
                final_values.append(state.json()["value"])
                own_tenant, own_marker = tenant_clients[tenant_number]
                response = await agents[tenant_number * (agent_count // 5)][1].get("/v1/episodes?user_id=bench&limit=200")
                records = response.json()
                isolation_ok &= all(row["tenant_id"] == own_tenant for row in records)
                own_records = (await agents[tenant_number * (agent_count // 5)][1].get(f"/v1/episodes?user_id=marker-{tenant_number}" )).json()
                isolation_ok &= any(row["content"] == own_marker and row["tenant_id"] == own_tenant for row in own_records)
                for other_number, (_, _) in enumerate(tenant_clients):
                    if other_number != tenant_number:
                        foreign = await agents[tenant_number * (agent_count // 5)][1].get(f"/v1/episodes?user_id=marker-{other_number}")
                        isolation_ok &= foreign.json() == []
            for _, client in agents:
                await client.aclose()
            await admin.aclose()

            ordered = sorted(latencies)
            def pct(value):
                return ordered[min(int((len(ordered) - 1) * value), len(ordered) - 1)] * 1000 if ordered else 0
            requests = len(latencies)
            report = {
                "duration_seconds": round(elapsed, 2), "agents": len(agents), "tenants": 5,
                "requests": requests, "throughput_rps": round(requests / elapsed, 2),
                "p50_ms": round(pct(.50), 2), "p95_ms": round(pct(.95), 2), "p99_ms": round(pct(.99), 2),
                "error_rate": round(failures / requests, 5) if requests else 0,
                "cas_conflicts_retried": cas_conflicts,
                "cas_retry_success_rate": 1.0 if all(final_values[i] == increments[i] for i in range(5)) else 0.0,
                "successful_increments": sum(increments), "final_counters": final_values,
                "zero_lost_updates": all(final_values[i] == increments[i] for i in range(5)),
                "zero_cross_tenant_leakage": isolation_ok,
            }
            (ROOT / "concurrency.json").write_text(json.dumps(report, indent=2) + "\n")
            figure, axis = plt.subplots(figsize=(8, 4.5))
            axis.bar(["p50", "p95", "p99"], [report["p50_ms"], report["p95_ms"], report["p99_ms"]], color=["#75c69a", "#e7bd68", "#dd7868"])
            axis.set_ylabel("Latency (ms)")
            axis.set_title(f"Mnemo concurrency latency · {len(agents)} agents")
            figure.tight_layout()
            figure.savefig(ROOT / "concurrency.png", dpi=140)
            plt.close(figure)
            return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--agents", type=int, default=100)
    args = parser.parse_args()
    report = asyncio.run(run(args.seconds, args.agents))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()