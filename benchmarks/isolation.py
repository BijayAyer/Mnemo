"""Adversarial cross-tenant API checks."""

import asyncio
import json
import os
import tempfile
from pathlib import Path

os.environ.setdefault("EMBEDDER", "fake")
os.environ.setdefault("LLM", "mock")

import httpx

from mnemo.config import settings
from mnemo.main import create_app

ROOT = Path(__file__).resolve().parent / "results"


async def run() -> list[dict]:
    ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mnemo-isolation-") as temp:
        object.__setattr__(settings, "data_dir", Path(temp) / "tenant-indexes")
        app = create_app(Path(temp) / "isolation.sqlite3")
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://bench") as client:
                admin = {"X-Admin-Key": settings.admin_key}
                acme = (await client.post("/admin/tenants", headers=admin, json={"name": "Acme"})).json()
                globex = (await client.post("/admin/tenants", headers=admin, json={"name": "Globex"})).json()
                a, b = {"X-API-Key": acme["api_key"]}, {"X-API-Key": globex["api_key"]}
                episode = (await client.post("/v1/episodes", headers=a, json={"user_id": "alice", "content": "private"})).json()
                fact = (await client.post("/v1/facts", headers=a, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "private"})).json()
                await client.put("/v1/blackboard/state", headers=a, json={"value": "private", "expected_version": 0})
                handoff = (await client.post("/v1/handoffs", headers=a, json={"to_agent": "triage", "user_id": "alice", "session_id": "s", "summary": "private"})).json()
                checks = []

                async def check(name, response, expected):
                    passed = response.status_code == expected
                    checks.append({"attack": name, "status": response.status_code, "expected": expected, "passed": passed})
                    print(f"{'PASS' if passed else 'FAIL'} {name}: {response.status_code} (expected {expected})")

                await check("missing credential", await client.get("/v1/facts"), 401)
                await check("read foreign episode", await client.get(f"/v1/episodes/{episode['id']}", headers=b), 404)
                await check("update foreign episode", await client.patch(f"/v1/episodes/{episode['id']}", headers=b, json={"content": "overwrite"}), 404)
                await check("delete foreign episode", await client.delete(f"/v1/episodes/{episode['id']}", headers=b), 404)
                await check("read foreign fact", await client.get(f"/v1/facts/{fact['id']}/history", headers=b), 404)
                await check("update foreign fact", await client.put(f"/v1/facts/{fact['id']}", headers=b, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "overwrite"}), 404)
                await check("delete foreign fact", await client.delete(f"/v1/facts/{fact['id']}", headers=b), 404)
                await check("read foreign blackboard", await client.get("/v1/blackboard/state", headers=b), 404)
                update = await client.put("/v1/blackboard/state", headers=b, json={"value": "overwrite", "expected_version": 1})
                await check("write using foreign version", update, 409)
                assert update.json()["detail"]["value"] is None
                await check("delete foreign blackboard", await client.delete("/v1/blackboard/state", headers=b), 404)
                await check("read foreign handoff", await client.get(f"/v1/handoffs/{handoff['id']}", headers=b), 404)
                await check("accept foreign handoff", await client.post(f"/v1/handoffs/{handoff['id']}/accept", headers=b), 404)
                await check("delete foreign handoff", await client.delete(f"/v1/handoffs/{handoff['id']}", headers=b), 404)
                search = await client.post("/v1/facts/search", headers=b, json={"query": "private", "user_id": "alice"})
                await check("search ranking isolation", search, 200)
                assert search.json() == []
                (ROOT / "isolation.json").write_text(json.dumps(checks, indent=2) + "\n")
                return checks


if __name__ == "__main__":
    results = asyncio.run(run())
    if not all(result["passed"] for result in results):
        raise SystemExit(1)