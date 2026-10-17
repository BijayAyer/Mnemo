"""FastAPI app factory."""

from contextlib import asynccontextmanager
import logging
import time
import uuid
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from .config import settings
from .db import Database
from .api.routes import metrics, router
from .graph import create_graph_store


def create_app(database_path=None) -> FastAPI:
    db = Database(database_path or settings.database_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await db.open()
        app.state.db = db
        app.state.graph = create_graph_store()
        async def consolidate_periodically():
            from .api.routes import consolidate
            from .models import TenantContext
            while True:
                await asyncio.sleep(settings.consolidation_interval)
                for tenant in await db.fetchall("SELECT id FROM tenants"):
                    try:
                        await consolidate(TenantContext(tenant_id=tenant["id"], agent_id="scheduler", role="admin"), db, app.state.graph)
                    except Exception:
                        logging.getLogger("mnemo.scheduler").exception("Tenant consolidation failed", extra={"tenant_id": tenant["id"]})
        task = asyncio.create_task(consolidate_periodically()) if settings.consolidation_interval > 0 else None
        try:
            yield
        finally:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await db.close()
            await app.state.graph.close()

    app = FastAPI(title="Mnemo", version="0.1.0", lifespan=lifespan)
    app.include_router(router)

    @app.middleware("http")
    async def attach_db(request: Request, call_next):
        request.state.db = db
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        started = time.perf_counter()
        response = await call_next(request)
        metrics.append((time.perf_counter() - started, response.status_code))
        del metrics[:-1000]
        response.headers["X-Request-ID"] = request_id
        return response

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "mnemo"}

    @app.get("/dashboard", include_in_schema=False)
    async def dashboard():
        return FileResponse("dashboard/index.html")

    return app


app = create_app()