"""MCP stdio tools backed by the local Mnemo HTTP API."""

import os
import httpx
from mcp.server import MCPServer

server = MCPServer("Mnemo", instructions="Tenant-scoped persistent memory and agent coordination.")
BASE_URL = os.getenv("MNEMO_URL", "http://127.0.0.1:8000")
API_KEY = os.getenv("MNEMO_API_KEY", "")


async def call(method: str, path: str, data: dict | None = None):
    async with httpx.AsyncClient(base_url=BASE_URL, headers={"X-API-Key": API_KEY}, timeout=30) as client:
        response = await client.request(method, path, json=data)
        response.raise_for_status()
        return response.json()


@server.tool()
async def remember_episode(user_id: str, content: str, session_id: str = "default") -> dict:
    """Persist an episode for a user."""
    return await call("POST", "/v1/episodes", {"user_id": user_id, "content": content, "session_id": session_id})


@server.tool()
async def remember_fact(user_id: str, subject: str, predicate: str, object: str) -> dict:
    """Store or supersede a semantic fact."""
    return await call("POST", "/v1/facts", {"user_id": user_id, "subject": subject, "predicate": predicate, "object": object})


@server.tool()
async def recall(user_id: str, query: str, session_id: str | None = None, budget_tokens: int = 1200) -> dict:
    """Retrieve relevant user memory and pending handoffs."""
    return await call("POST", "/v1/recall", {"user_id": user_id, "query": query, "session_id": session_id, "budget_tokens": budget_tokens})


@server.tool()
async def search_memory(query: str, user_id: str | None = None) -> dict:
    """Search episodic and semantic memory."""
    return {"episodes": await call("POST", "/v1/episodes/search", {"query": query, "user_id": user_id}), "facts": await call("POST", "/v1/facts/search", {"query": query, "user_id": user_id})}


@server.tool()
async def blackboard_get(key: str) -> dict:
    """Read a shared tenant blackboard entry."""
    return await call("GET", f"/v1/blackboard/{key}")


@server.tool()
async def blackboard_set(key: str, value: str, expected_version: int) -> dict:
    """Compare-and-swap a shared tenant blackboard entry."""
    return await call("PUT", f"/v1/blackboard/{key}", {"value": value, "expected_version": expected_version})


@server.tool()
async def create_handoff(to_agent: str, user_id: str, session_id: str, summary: str) -> dict:
    """Create a handoff packet for another agent."""
    return await call("POST", "/v1/handoffs", {"to_agent": to_agent, "user_id": user_id, "session_id": session_id, "summary": summary})


@server.tool()
async def check_inbox() -> dict:
    """List pending handoffs addressed to this API key's agent."""
    return {"handoffs": await call("GET", "/v1/handoffs/inbox")}


if __name__ == "__main__":
    server.run(transport="stdio")