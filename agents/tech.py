"""Technical support agent behavior."""

from .base import MemoryClient


class TechAgent:
    """Retrieve technical context and resolve technical handoffs."""

    def __init__(self, memory: MemoryClient):
        self.memory = memory

    async def find_procedure(self, situation: str):
        return await self.memory.request("POST", "/v1/procedures/match", {"situation": situation})