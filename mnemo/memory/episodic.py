"""Episodic memory ranking rules."""

from datetime import datetime, timezone


def relevance_score(similarity: float, timestamp: str, importance: float, at: datetime | None = None) -> float:
    """Combine semantic similarity, 30-day recency decay, and importance."""
    current = at or datetime.now(timezone.utc)
    age_days = max((current - datetime.fromisoformat(timestamp)).total_seconds() / 86400, 0)
    recency = 0.5 ** (age_days / 30)
    return 0.6 * similarity + 0.25 * recency + 0.15 * importance