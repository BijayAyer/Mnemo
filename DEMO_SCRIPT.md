# Three-Minute Mnemo Demo

## 0:00-0:20 — Pitch

“Agents usually forget what a user told another agent, and teams lose work between handoffs. Mnemo is a local memory and coordination service: it keeps episodic events, versioned facts, reusable procedures, and shared work state tenant-isolated. Today I’ll show a preference surviving a service restart, a fact changing without erasing history, and two agents updating shared state without losing a write.”

## 0:20-0:40 — Start the service

In terminal one, from the repository root:

```sh
uv sync --extra dev
cp .env.example .env
uv run uvicorn mnemo.main:app --reload
```

The example environment selects fake embeddings and MockLLM so this demo needs no paid API or running model.

## 0:40-1:25 — Run the agents

In terminal two:

```sh
uv run python -m agents.demo
```

Call out the transcript in order:

- Triage records Alice’s Pro plan and email preference, then routes the double-charge issue to Billing.
- Billing accepts the packet, resolves the issue, and records a reusable procedure.
- The app lifespan closes and reopens; Triage recalls the two preferences in session two.
- The plan changes to Enterprise, while the point-in-time query still returns Pro.
- Two writers race on the same blackboard version; one gets 409, then the SDK retry increments the state cleanly.
- Globex receives 404 for Acme’s fact, then Acme consolidation reports its changes.

## 1:25-2:10 — Inspect the dashboard

Open `http://127.0.0.1:8000/dashboard`. Create a dashboard-only tenant in a third terminal and copy the returned `api_key` into the page:

```sh
curl -s -X POST http://127.0.0.1:8000/admin/tenants \
  -H "X-Admin-Key: $(sed -n 's/^MNEMO_ADMIN_KEY=//p' .env)" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Dashboard demo","agent_id":"triage"}'
```

Switch through Memory graph, Timeline, Facts, Blackboard, Handoffs, Consolidation, and Metrics. To demonstrate live blackboard updates, keep Blackboard visible and run:

```sh
curl -s -X PUT http://127.0.0.1:8000/v1/blackboard/demo \
  -H 'X-API-Key: PASTE_THE_RETURNED_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"value":{"owner":"triage"},"expected_version":0}'
```

Point out that the browser sends the API key in the authorization header, not the URL.

## 2:10-2:40 — Show the integration surface

Mention that `.vscode/mcp.json` launches the official MCP Python SDK server after the HTTP service is running, and prompts for a tenant API key. Its tools cover remembering episodes/facts, recall, search, blackboard CAS, handoff creation, and inbox checks.

## 2:40-3:00 — Close with measured results

Use the generated report: 100 agents across five tenants completed the 60-second run at about 1.4k requests/second, with zero errors, zero lost counter updates, and zero cross-tenant leakage. Recall@1 and recall@5 were both 1.0 on the four deterministic session-one questions. State that the recall benchmark uses fake embeddings and is a functional check, not a production model-quality claim.
