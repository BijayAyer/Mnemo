"""API key hashing and tenant context resolution."""

import hashlib
from fastapi import Depends, Header, HTTPException, Request
from .config import settings
from .db import Database
from .models import TenantContext


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


async def get_db(request: Request) -> Database:
    return request.app.state.db


async def tenant_context(x_api_key: str | None = Header(default=None), db: Database = Depends(get_db)) -> TenantContext:
    if not x_api_key:
        raise HTTPException(401, "X-API-Key required")
    row = await db.fetchone("SELECT tenant_id,agent_id,role FROM api_keys WHERE key_hash=?", (hash_key(x_api_key),))
    if not row:
        raise HTTPException(403, "Invalid API key")
    return TenantContext(**dict(row))


async def require_admin(x_admin_key: str | None = Header(default=None)) -> None:
    if x_admin_key != settings.admin_key:
        raise HTTPException(403, "Admin key required")