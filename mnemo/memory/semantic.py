"""Semantic fact normalization and temporal helpers."""

import re


def normalized_value(value: str) -> str:
    """Normalize punctuation and case for duplicate-fact detection."""
    return re.sub(r"[^a-z0-9 ]", "", value.lower()).strip()