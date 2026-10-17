"""Async SDK for Mnemo's tenant-scoped HTTP API."""

import asyncio
import random
import httpx


class MemoryClient:
    """Small client bound to one tenant key and agent identity."""

    def __init__(self, base_url: str, api_key: str, client: httpx.AsyncClient | None = None):
        self.client = client or httpx.AsyncClient(base_url=base_url, headers={"X-API-Key": api_key}, timeout=30)
        self.owns_client = client is None

    async def request(self, method: str, path: str, data: dict | None = None):
        response = await self.client.request(method, path, json=data)
        response.raise_for_status()
        return response.json() if response.content else None

    async def update(self, key: str, fn, retries: int = 64):
        """Apply a blackboard transform with optimistic retries and jitter."""
        for attempt in range(retries):
            try:
                current = await self.request("GET", f"/v1/blackboard/{key}")
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 404:
                    raise
                current = {"value": 0, "version": 0}
            value = fn(current["value"])
            try:
                return await self.request("PUT", f"/v1/blackboard/{key}", {"value": value, "expected_version": current["version"]})
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 409 or attempt == retries - 1:
                    raise
                await asyncio.sleep(random.uniform(0.001, 0.01) * (attempt + 1))
        raise RuntimeError("Blackboard update retries exhausted")

    async def close(self):
        if self.owns_client:
            await self.client.aclose()