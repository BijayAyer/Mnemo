"""Mock and OpenAI-compatible local LLM client."""

import json
import re
import httpx
from .config import settings


class MockLLM:
    """Rule-based model for offline demos and tests."""

    async def complete(self, prompt: str) -> str:
        text = prompt.lower()
        facts = []
        for predicate, pattern in (("plan", r"(?:plan is|plan=)\s*([\w-]+)"), ("preference", r"prefers?\s+([\w-]+)")):
            match = re.search(pattern, text)
            if match:
                facts.append({"subject": "user", "predicate": predicate, "object": match.group(1)})
        if "extract" in text:
            return json.dumps(facts)
        return "I reviewed the remembered context and will handle the request accordingly."


class OpenAICompatibleLLM:
    """Call a local OpenAI-compatible chat completions endpoint."""

    async def complete(self, prompt: str) -> str:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(f"{settings.llm_base_url}/chat/completions", json={"model": settings.llm_model, "messages": [{"role": "user", "content": prompt}]})
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]


def get_llm():
    return MockLLM() if settings.llm == "mock" else OpenAICompatibleLLM()