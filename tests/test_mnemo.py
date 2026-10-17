"""Core behavior and security integration tests."""

import asyncio
import time

import httpx
import pytest

from agents.base import MemoryClient
from mnemo.auth import hash_key
from mnemo.config import settings
from mnemo.vectorstore import TenantIndex
from tests.conftest import create_tenant


async def headers_for(client, name, agent="triage"):
    tenant = await create_tenant(client, name, agent)
    return tenant, {"X-API-Key": tenant["api_key"]}


@pytest.mark.asyncio
async def test_auth_requires_valid_tenant_key(client):
    assert (await client.get("/v1/facts")).status_code == 401
    assert (await client.get("/v1/facts", headers={"X-API-Key": "wrong"})).status_code == 403


@pytest.mark.asyncio
async def test_foreign_tenant_item_access_leaks_nothing(client):
    acme, a = await headers_for(client, "Acme")
    _, b = await headers_for(client, "Globex")
    episode = (await client.post("/v1/episodes", headers=a, json={"user_id": "alice", "content": "Acme secret payment"})).json()
    fact = (await client.post("/v1/facts", headers=a, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Secret Pro"})).json()
    await client.put("/v1/blackboard/ticket", headers=a, json={"value": "secret", "expected_version": 0})
    handoff = (await client.post("/v1/handoffs", headers=a, json={"to_agent": "triage", "user_id": "alice", "session_id": "s1", "summary": "secret handoff"})).json()

    assert (await client.get(f"/v1/episodes/{episode['id']}", headers=b)).status_code == 404
    assert (await client.patch(f"/v1/episodes/{episode['id']}", headers=b, json={"content": "stolen"})).status_code == 404
    assert (await client.delete(f"/v1/episodes/{episode['id']}", headers=b)).status_code == 404
    assert (await client.get(f"/v1/facts/{fact['id']}/history", headers=b)).status_code == 404
    assert (await client.put(f"/v1/facts/{fact['id']}", headers=b, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "stolen"})).status_code == 404
    assert (await client.delete(f"/v1/facts/{fact['id']}", headers=b)).status_code == 404
    assert (await client.get("/v1/blackboard/ticket", headers=b)).status_code == 404
    conflict = await client.put("/v1/blackboard/ticket", headers=b, json={"value": "stolen", "expected_version": 1})
    assert conflict.status_code == 409
    assert conflict.json()["detail"] == {"version": 0, "value": None}
    assert (await client.delete("/v1/blackboard/ticket", headers=b)).status_code == 404
    assert (await client.get(f"/v1/handoffs/{handoff['id']}", headers=b)).status_code == 404
    assert (await client.post(f"/v1/handoffs/{handoff['id']}/accept", headers=b)).status_code == 404
    assert (await client.delete(f"/v1/handoffs/{handoff['id']}", headers=b)).status_code == 404
    assert (await client.post("/v1/episodes/search", headers=b, json={"query": "Acme secret"})).json() == []
    assert (await client.post("/v1/facts/search", headers=b, json={"query": "Secret Pro"})).json() == []
    assert (await client.get("/v1/episodes", headers=b)).json() == []
    assert (await client.get("/v1/facts", headers=b)).json() == []
    assert acme["tenant_id"] != (await client.get("/v1/blackboard", headers=b)).headers.get("x-tenant-id")


@pytest.mark.asyncio
async def test_fact_supersession_and_point_in_time(client):
    _, headers = await headers_for(client, "Facts")
    old = (await client.post("/v1/facts", headers=headers, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Pro"})).json()
    new = (await client.post("/v1/facts", headers=headers, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Enterprise"})).json()
    current = await client.get("/v1/facts?user_id=alice", headers=headers)
    historical = await client.get("/v1/facts", headers=headers, params={"user_id": "alice", "as_of": old["valid_from"]})
    history = await client.get(f"/v1/facts/{new['id']}/history", headers=headers)
    assert [item["object"] for item in current.json()] == ["Enterprise"]
    assert [item["object"] for item in historical.json()] == ["Pro"]
    assert [item["object"] for item in history.json()] == ["Pro", "Enterprise"]
    assert history.json()[0]["superseded_by"] == new["id"]


@pytest.mark.asyncio
async def test_blackboard_cas_and_fifty_concurrent_updates(app, client):
    tenant, _ = await headers_for(client, "CAS")
    shared = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"X-API-Key": tenant["api_key"]})
    memory = MemoryClient("http://test", tenant["api_key"], shared)
    first = await shared.put("/v1/blackboard/count", json={"value": 0, "expected_version": 0})
    assert first.status_code == 200
    stale = await shared.put("/v1/blackboard/count", json={"value": 90, "expected_version": 0})
    assert stale.status_code == 409 and stale.json()["detail"]["version"] == 1
    await asyncio.gather(*(memory.update("count", lambda value: value + 1) for _ in range(50)))
    final = await memory.request("GET", "/v1/blackboard/count")
    assert final["value"] == 50
    assert final["version"] == 51
    await shared.aclose()


@pytest.mark.asyncio
async def test_handoff_lifecycle_and_lease_expiry(app, client):
    tenant, triage = await headers_for(client, "Coordination")
    billing_key = "billing-key"
    await app.state.db.insert("INSERT INTO api_keys(key_hash,tenant_id,agent_id,role,created_at) VALUES(?,?,?,?,?)", (hash_key(billing_key), tenant["tenant_id"], "billing", "agent", "now"))
    billing = {"X-API-Key": billing_key}
    handoff = await client.post("/v1/handoffs", headers=triage, json={"to_agent": "billing", "user_id": "alice", "session_id": "s1", "summary": "refund"})
    handoff_id = handoff.json()["id"]
    assert (await client.post(f"/v1/handoffs/{handoff_id}/accept", headers=billing)).status_code == 200
    assert (await client.post(f"/v1/handoffs/{handoff_id}/complete", headers=billing)).json()["status"] == "completed"
    lease = {"resource": "ticket-1", "ttl_seconds": 60}
    assert (await client.post("/v1/leases/acquire", headers=triage, json=lease)).status_code == 200
    assert (await client.post("/v1/leases/acquire", headers=billing, json=lease)).status_code == 409
    await app.state.db.execute("UPDATE leases SET expires_at=? WHERE tenant_id=? AND resource=?", (time.time() - 1, tenant["tenant_id"], "ticket-1"))
    assert (await client.post("/v1/leases/acquire", headers=billing, json=lease)).status_code == 200
    assert (await client.post("/v1/leases/release", headers=billing, json=lease)).status_code == 200


@pytest.mark.asyncio
async def test_history_survives_database_restart(client, app):
    _, headers = await headers_for(client, "Persistence")
    for index in range(5):
        response = await client.post("/v1/messages", headers=headers, json={"user_id": "alice", "session_id": "s1", "role": "user", "content": f"message {index}"})
        assert response.status_code == 201
    before = await client.get("/v1/history/alice/s1", headers=headers)
    assert before.json()["summary"]
    await app.state.db.close()
    await app.state.db.open()
    after = await client.get("/v1/history/alice/s1", headers=headers)
    assert after.json() == before.json()


@pytest.mark.asyncio
async def test_consolidation_extracts_and_reports(client):
    _, headers = await headers_for(client, "Sleep")
    await client.post("/v1/episodes", headers=headers, json={"user_id": "alice", "content": "My plan is Pro and I prefer email."})
    report = await client.post("/v1/consolidate", headers=headers)
    assert report.status_code == 200
    assert report.json()["facts_added"] >= 1
    facts = await client.get("/v1/facts?user_id=alice", headers=headers)
    assert any(item["predicate"] == "plan" and item["object"].lower() == "pro" for item in facts.json())
    reports = await client.get("/v1/consolidate/reports", headers=headers)
    assert len(reports.json()) == 1


@pytest.mark.asyncio
async def test_missing_vector_index_rebuilds_from_sqlite(client):
    tenant, headers = await headers_for(client, "Vector rebuild")
    fact = await client.post("/v1/facts", headers=headers, json={"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Pro"})
    index = TenantIndex(tenant["tenant_id"])
    index.path.unlink()
    index.faiss_path.unlink(missing_ok=True)
    index.ids_path.unlink(missing_ok=True)
    result = await client.post("/v1/facts/search", headers=headers, json={"user_id": "alice", "query": "plan Pro"})
    assert result.status_code == 200
    assert result.json()[0]["id"] == fact.json()["id"]
    assert index.path.exists()


@pytest.mark.asyncio
async def test_handoff_builds_session_summary_and_memory_refs(client):
    _, headers = await headers_for(client, "Handoff packet")
    await client.post("/v1/messages", headers=headers, json={"user_id": "alice", "session_id": "packet", "role": "user", "content": "Please refund the duplicate payment."})
    episode = await client.post("/v1/episodes", headers=headers, json={"user_id": "alice", "session_id": "packet", "content": "Duplicate payment reported."})
    handoff = await client.post("/v1/handoffs", headers=headers, json={"to_agent": "billing", "user_id": "alice", "session_id": "packet"})
    assert handoff.status_code == 201
    assert "duplicate payment" in handoff.json()["summary"].lower()
    assert episode.json()["id"] in handoff.json()["memory_refs"]


@pytest.mark.asyncio
async def test_procedure_versioning_and_failure_flag(client):
    _, headers = await headers_for(client, "Procedures")
    old = await client.post("/v1/procedures", headers=headers, json={"name": "Refund", "trigger_text": "duplicate charge", "steps": ["verify"]})
    updated = await client.put(f"/v1/procedures/{old.json()['id']}", headers=headers, json={"name": "Refund v2", "trigger_text": "duplicate charge", "steps": ["verify", "refund"]})
    assert updated.json()["version"] == 2
    assert (await client.get(f"/v1/procedures/{old.json()['id']}", headers=headers)).json()["is_active"] == 0
    for _ in range(3):
        await client.post(f"/v1/procedures/{updated.json()['id']}/outcome", headers=headers, json={"success": False})
    report = await client.post("/v1/consolidate", headers=headers)
    assert updated.json()["id"] in report.json()["procedures_flagged"]