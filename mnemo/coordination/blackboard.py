"""Compare-and-swap response helpers."""

import json


def conflict_state(current) -> dict:
    """Return the current version/value without exposing another tenant's state."""
    return {"version": current["version"] if current else 0, "value": json.loads(current["value_json"]) if current else None}