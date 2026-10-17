"""Billing agent behavior."""

from .base import MemoryClient


class BillingAgent:
    """Accept billing handoffs and record reusable procedures."""

    def __init__(self, memory: MemoryClient):
        self.memory = memory

    async def accept_next(self):
        inbox = await self.memory.request("GET", "/v1/handoffs/inbox")
        if not inbox:
            return None
        return await self.memory.request("POST", f"/v1/handoffs/{inbox[0]['id']}/accept")

    async def procedure(self):
        return await self.memory.request("POST", "/v1/procedures", {"name": "Double charge resolution", "trigger_text": "double charge duplicate payment", "steps": ["Verify invoice", "Issue refund", "Notify by email"]})