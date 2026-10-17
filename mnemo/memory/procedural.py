"""Procedural memory quality rules."""


def needs_review(success_count: int, failure_count: int) -> bool:
    """Flag a procedure after at least three failures exceed successes."""
    return failure_count >= 3 and failure_count > success_count