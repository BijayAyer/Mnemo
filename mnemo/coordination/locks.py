"""Lease ownership and expiration rules."""


def lease_is_active(lease, at: float) -> bool:
    """Return whether a lease row is still active at the supplied time."""
    return bool(lease and lease["expires_at"] > at)