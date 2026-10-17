"""Handoff packet assembly helpers."""


def conversation_summary(latest, messages) -> str:
    """Prefer a rolling summary, otherwise use the session transcript."""
    if latest:
        return latest["text"]
    return "\n".join(f"{message['role']}: {message['content']}" for message in reversed(messages))