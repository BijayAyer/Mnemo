"""Run the end-to-end multi-agent memory scenario locally."""

import asyncio
import os
from urllib.parse import quote

os.environ.setdefault("EMBEDDER", "fake")
os.environ.setdefault("LLM", "mock")

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from agents.base import MemoryClient
from agents.billing import BillingAgent
from agents.tech import TechAgent
from agents.triage import TriageAgent
from mnemo.config import settings
from mnemo.main import create_app

console = Console()


async def main() -> None:
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://mnemo") as admin:
        async with app.router.lifespan_context(app):
            headers = {"X-Admin-Key": settings.admin_key}
            acme = (await admin.post("/admin/tenants", headers=headers, json={"name": "Acme", "agent_id": "triage"})).json()
            globex = (await admin.post("/admin/tenants", headers=headers, json={"name": "Globex", "agent_id": "triage"})).json()
            def client(key: str):
                return MemoryClient("http://mnemo", key, httpx.AsyncClient(transport=transport, base_url="http://mnemo", headers={"X-API-Key": key}))
            triage_client = client(acme["api_key"])
            billing_api_key = (await admin.post(f"/admin/tenants/{acme['tenant_id']}/keys", headers=headers, json={"agent_id": "billing"})).json()["api_key"]
            tech_api_key = (await admin.post(f"/admin/tenants/{acme['tenant_id']}/keys", headers=headers, json={"agent_id": "tech"})).json()["api_key"]
            triage = TriageAgent(triage_client)
            billing_client = client(billing_api_key)
            billing = BillingAgent(billing_client)
            tech = TechAgent(client(tech_api_key))
            console.print(Panel.fit("[bold green]Mnemo agent memory demo[/bold green]", border_style="green"))
            console.print("[bold cyan]1. Session 1: remember Alice's context[/bold cyan]")
            await triage_client.request("POST", "/v1/messages", {"user_id": "alice", "session_id": "support-1", "role": "user", "content": "My plan is Pro, I prefer email, and I was double charged."})
            plan = await triage_client.request("POST", "/v1/facts", {"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Pro"})
            preference = await triage_client.request("POST", "/v1/facts", {"user_id": "alice", "subject": "alice", "predicate": "contact preference", "object": "email"})
            episode = await triage_client.request("POST", "/v1/episodes", {"user_id": "alice", "session_id": "support-1", "content": "Alice is on Pro, prefers email, and reports a double charge.", "importance": 0.9})
            handoff = await triage.handoff("billing", "alice", "support-1", "Investigate Alice's double charge; notify via email.")
            console.print(f"Stored plan {plan['object']}, preference {preference['object']}, episode {episode['id'][:8]}; handed to Billing.")
            console.print("[bold cyan]2. Billing accepts and resolves[/bold cyan]")
            accepted = await billing.accept_next()
            procedure = await billing.procedure()
            await billing_client.request("POST", f"/v1/procedures/{procedure['id']}/outcome", {"success": True})
            await billing_client.request("POST", f"/v1/handoffs/{accepted['id']}/complete")
            console.print(f"Accepted handoff {handoff['id'][:8]}; recorded procedure {procedure['name']}.")
            console.print("[bold cyan]3. Restart service, then recall session 2[/bold cyan]")
        async with app.router.lifespan_context(app):
            recalled = await triage.recall("alice", "How should I be contacted about my plan?", "support-2")
            # Use active facts explicitly: recall's lexical ranking should surface both durable facts.
            active_facts = await triage_client.request("GET", "/v1/facts?user_id=alice")
            console.print("Recalled after restart: " + ", ".join(f"{item['predicate']}={item['object']}" for item in active_facts))
            await triage_client.request("POST", "/v1/episodes", {"user_id": "alice", "session_id": "support-2", "content": "Alice returned with a new account question."})
            console.print("Recall bundle keys: " + ", ".join(recalled.keys()))
            console.print("[bold cyan]4. Supersede plan while retaining point-in-time history[/bold cyan]")
            await triage_client.request("POST", "/v1/facts", {"user_id": "alice", "subject": "alice", "predicate": "plan", "object": "Enterprise"})
            historical = await triage_client.request("GET", f"/v1/facts?user_id=alice&as_of={quote(plan['valid_from'], safe='')}")
            console.print("Current plan: Enterprise; at the earlier timestamp: " + ", ".join(f["object"] for f in historical if f["predicate"] == "plan"))
            console.print("[bold cyan]5. Concurrent blackboard writers and retry[/bold cyan]")
            await triage_client.request("PUT", "/v1/blackboard/ticket-count", {"value": 0, "expected_version": 0})
            first = httpx.AsyncClient(transport=transport, base_url="http://mnemo", headers={"X-API-Key": acme["api_key"]})
            second = httpx.AsyncClient(transport=transport, base_url="http://mnemo", headers={"X-API-Key": billing_api_key})
            outcomes = await asyncio.gather(first.put("/v1/blackboard/ticket-count", json={"value": 1, "expected_version": 1}), second.put("/v1/blackboard/ticket-count", json={"value": 1, "expected_version": 1}))
            console.print(f"CAS outcomes: {outcomes[0].status_code}, {outcomes[1].status_code}; SDK retry increments the shared value.")
            await billing_client.update("ticket-count", lambda value: (value or 0) + 1)
            final_state = await billing_client.request("GET", "/v1/blackboard/ticket-count")
            console.print(f"Ticket counter now {final_state['value']} at version {final_state['version']}.")
            await first.aclose()
            await second.aclose()
            console.print("[bold cyan]6. Verify tenant isolation[/bold cyan]")
            globex_client = client(globex["api_key"])
            denied = await globex_client.client.get(f"/v1/facts/{plan['id']}/history")
            console.print(f"Globex access to Acme fact history: [green]{denied.status_code}[/green] (expected 404).")
            console.print("[bold cyan]7. Consolidate recent memory[/bold cyan]")
            report = await triage_client.request("POST", "/v1/consolidate")
            table = Table(title="Consolidation report")
            for name, value in report.items():
                table.add_row(name.replace("_", " "), str(value))
            console.print(table)
            await globex_client.close()
            await triage_client.close()
            await billing_client.close()
            await tech.memory.close()


if __name__ == "__main__":
    asyncio.run(main())