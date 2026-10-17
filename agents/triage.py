"""Triage agent behavior."""

from .base import MemoryClient


class TriageAgent:
    """Recall user context and route work to specialist agents."""

    def __init__(self, memory: MemoryClient):
        self.memory = memory

    async def recall(self, user_id: str, query: str, session_id: str):
        return await self.memory.request("POST", "/v1/recall", {"user_id": user_id, "query": query, "session_id": session_id})

    async def handoff(self, to_agent: str, user_id: str, session_id: str, summary: str):
        return await self.memory.request("POST", "/v1/handoffs", {"to_agent": to_agent, "user_id": user_id, "session_id": session_id, "summary": summary, "open_tasks": ["Resolve the reported issue"]})