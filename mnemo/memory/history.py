"""Conversation summary cursor and prompt helpers."""


def after_cursor(messages, cursor: str | None) -> list:
    """Return chronologically ordered messages after the stored message ID."""
    if cursor is None:
        return list(messages)
    found = False
    remaining = []
    for message in messages:
        if message["id"] == cursor:
            found = True
            continue
        if found:
            remaining.append(message)
    return remaining


def summary_prompt(messages) -> str:
    """Build a concise model prompt from unsummarized messages."""
    transcript = "\n".join(f"{message['role']}: {message['content']}" for message in messages)
    return "Summarize this conversation concisely: " + transcript