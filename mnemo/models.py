"""Validated API payloads and tenant identity."""

from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class TenantContext(BaseModel):
    tenant_id: str
    agent_id: str
    role: str = "agent"


class EpisodeIn(BaseModel):
    user_id: str
    session_id: str = "default"
    event_type: str = "interaction"
    content: str = Field(min_length=1, max_length=20000)
    outcome: str | None = None
    importance: float = Field(default=0.5, ge=0, le=1)


class FactIn(BaseModel):
    user_id: str
    subject: str = Field(min_length=1, max_length=500)
    predicate: str = Field(min_length=1, max_length=200)
    object: str = Field(min_length=1, max_length=2000)
    confidence: float = Field(default=1, ge=0, le=1)


class ProcedureIn(BaseModel):
    name: str
    trigger_text: str
    steps: list[str]


class MessageIn(BaseModel):
    user_id: str
    session_id: str
    role: str
    content: str


class RecallIn(BaseModel):
    user_id: str
    query: str
    session_id: str | None = None
    budget_tokens: int = Field(default=1200, ge=50, le=20000)


class BlackboardIn(BaseModel):
    value: Any
    expected_version: int = Field(ge=0)


class HandoffIn(BaseModel):
    to_agent: str
    user_id: str
    session_id: str
    summary: str = ""
    memory_refs: list[str] = Field(default_factory=list)
    open_tasks: list[str] = Field(default_factory=list)


class LeaseIn(BaseModel):
    resource: str
    ttl_seconds: int = Field(default=60, ge=1, le=86400)


class FactOut(FactIn):
    id: str
    valid_from: datetime | str
    valid_to: datetime | str | None = None
    superseded_by: str | None = None