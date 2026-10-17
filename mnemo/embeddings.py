"""Pluggable local and deterministic embedding providers."""

import hashlib
import math
import re
from functools import lru_cache
from .config import settings


def embed(text: str) -> list[float]:
    """Embed text with the configured provider."""
    if settings.embedder == "fake":
        vector = [0.0] * 384
        for token in re.findall(r"[a-z0-9]+", text.lower()):
            digest = hashlib.sha256(token.encode()).digest()
            index = int.from_bytes(digest[:4], "big") % len(vector)
            vector[index] += 1.0 if digest[4] & 1 else -1.0
        norm = math.sqrt(sum(value * value for value in vector)) or 1
        return [value / norm for value in vector]
    if settings.embedder == "lm-studio":
        import httpx
        response = httpx.post(f"{settings.llm_base_url}/embeddings", json={"model": settings.llm_model, "input": text}, timeout=30)
        response.raise_for_status()
        return response.json()["data"][0]["embedding"]
    return _sentence_model().encode(text, normalize_embeddings=True).tolist()


@lru_cache(maxsize=1)
def _sentence_model():
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError("Install local embeddings with `uv sync --extra local-ml` or set EMBEDDER=fake") from exc
    return SentenceTransformer("all-MiniLM-L6-v2")