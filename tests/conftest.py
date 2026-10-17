"""Shared offline test fixtures."""

import os
os.environ["EMBEDDER"] = "fake"
os.environ["LLM"] = "mock"
os.environ.setdefault("GRAPH_BACKEND", "none")
os.environ["SUMMARY_THRESHOLD"] = "5"

import httpx
import pytest
from mnemo.config import settings
from mnemo.main import create_app


@pytest.fixture
async def app(tmp_path):
    previous_data_dir = settings.data_dir
    object.__setattr__(settings, "data_dir", tmp_path / "tenant-indexes")
    application = create_app(tmp_path / "test.sqlite3")
    try:
        async with application.router.lifespan_context(application):
            yield application
    finally:
        object.__setattr__(settings, "data_dir", previous_data_dir)


@pytest.fixture
async def client(app):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as session:
        yield session


@pytest.fixture
def admin_headers():
    return {"X-Admin-Key": settings.admin_key}


async def create_tenant(client, name, agent="triage"):
    response = await client.post("/admin/tenants", headers={"X-Admin-Key": settings.admin_key}, json={"name": name, "agent_id": agent})
    assert response.status_code == 200, response.text
    return response.json()