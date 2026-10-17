"""Episode pattern grouping for the consolidation pass."""


def successful_groups(episodes) -> dict[tuple[str, str], list]:
    """Group successful or resolved episodes by event type and outcome."""
    groups: dict[tuple[str, str], list] = {}
    for episode in episodes:
        outcome = (episode["outcome"] or "").lower()
        if outcome in {"success", "successful", "resolved"}:
            groups.setdefault((episode["event_type"], outcome), []).append(episode)
    return groups