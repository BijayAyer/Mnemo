"""Tenant-scoped memory and coordination endpoints."""

import asyncio
import json
import logging
import re
import secrets
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from ..auth import get_db, hash_key, require_admin, tenant_context
from ..coordination.blackboard import conflict_state
from ..coordination.handoff import conversation_summary
from ..coordination.locks import lease_is_active
from ..config import settings
from ..db import Database
from ..embeddings import embed
from ..graph import GraphStore, get_graph, write_through
from ..llm import get_llm
from ..models import BlackboardIn, EpisodeIn, FactIn, HandoffIn, LeaseIn, MessageIn, ProcedureIn, RecallIn, TenantContext
from ..memory.consolidation import successful_groups
from ..memory.episodic import relevance_score
from ..memory.history import after_cursor, summary_prompt
from ..memory.procedural import needs_review
from ..memory.semantic import normalized_value
from ..vectorstore import TenantIndex

log = logging.getLogger("mnemo.api")
router = APIRouter()
events: dict[str, set[asyncio.Queue]] = {}
metrics: list[tuple[float, int]] = []


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ident() -> str:
    return str(uuid.uuid4())


def rowdict(row) -> dict[str, Any] | None:
    return dict(row) if row else None


def put_vector(tenant_id: str, item_id: str, text: str) -> None:
    try:
        TenantIndex(tenant_id).add(item_id, embed(text))
    except RuntimeError:
        raise


async def tenant_index(db: Database, tenant_id: str) -> TenantIndex:
    index = TenantIndex(tenant_id)
    if index.vectors:
        return index
    sources = (
        ("SELECT id,content AS text FROM episodes WHERE tenant_id=?", tenant_id),
        ("SELECT id,subject||' '||predicate||' '||object AS text FROM facts WHERE tenant_id=?", tenant_id),
        ("SELECT id,trigger_text AS text FROM procedures WHERE tenant_id=?", tenant_id),
    )
    for sql, value in sources:
        for row in await db.fetchall(sql, (value,)):
            index.add(row["id"], embed(row["text"]))
    return index


async def upsert_fact(db: Database, ctx: TenantContext, data: FactIn, graph: GraphStore, source_episode_id: str | None = None) -> dict:
    stamp, new_id = now(), ident()
    async with db.lock:
        await db.conn.execute("BEGIN IMMEDIATE")
        try:
            current = await (await db.conn.execute(
                "SELECT id,object FROM facts WHERE tenant_id=? AND user_id=? AND subject=? AND predicate=? AND valid_to IS NULL ORDER BY valid_from DESC LIMIT 1",
                (ctx.tenant_id, data.user_id, data.subject, data.predicate),
            )).fetchone()
            if current and current["object"] == data.object:
                await db.conn.execute("UPDATE facts SET confidence=? WHERE tenant_id=? AND id=?", (data.confidence, ctx.tenant_id, current["id"]))
                await db.conn.commit()
                result = dict(await db.fetchone("SELECT * FROM facts WHERE tenant_id=? AND id=?", (ctx.tenant_id, current["id"])))
                await write_through(graph, "upsert_fact", ctx.tenant_id, result)
                return result
            if current:
                await db.conn.execute("UPDATE facts SET valid_to=?,superseded_by=? WHERE tenant_id=? AND id=?", (stamp, new_id, ctx.tenant_id, current["id"]))
            await db.conn.execute(
                "INSERT INTO facts(id,tenant_id,user_id,subject,predicate,object,confidence,source_episode_id,valid_from) VALUES(?,?,?,?,?,?,?,?,?)",
                (new_id, ctx.tenant_id, data.user_id, data.subject, data.predicate, data.object, data.confidence, source_episode_id, stamp),
            )
            await db.conn.commit()
        except BaseException:
            await db.conn.rollback()
            raise
    put_vector(ctx.tenant_id, new_id, f"{data.subject} {data.predicate} {data.object}")
    result = rowdict(await db.fetchone("SELECT * FROM facts WHERE tenant_id=? AND id=?", (ctx.tenant_id, new_id)))
    if current:
        old_fact = dict(await db.fetchone("SELECT * FROM facts WHERE tenant_id=? AND id=?", (ctx.tenant_id, current["id"])))
        await write_through(graph, "upsert_fact", ctx.tenant_id, old_fact)
    await write_through(graph, "upsert_fact", ctx.tenant_id, result, current["id"] if current else None)
    return result


@router.post("/admin/tenants", dependencies=[Depends(require_admin)])
async def create_tenant(body: dict, db: Database = Depends(get_db)):
    tenant_id, key = ident(), secrets.token_urlsafe(32)
    await db.insert("INSERT INTO tenants(id,name,created_at) VALUES(?,?,?)", (tenant_id, body.get("name", "Tenant"), now()))
    await db.insert("INSERT INTO api_keys(key_hash,tenant_id,agent_id,role,created_at) VALUES(?,?,?,?,?)", (hash_key(key), tenant_id, body.get("agent_id", "agent"), "admin", now()))
    return {"tenant_id": tenant_id, "api_key": key}


@router.post("/admin/tenants/{tenant_id}/keys", dependencies=[Depends(require_admin)])
async def issue_tenant_key(tenant_id: str, body: dict, db: Database = Depends(get_db)):
    if not await db.fetchone("SELECT id FROM tenants WHERE id=?", (tenant_id,)):
        raise HTTPException(404, "Tenant not found")
    key = secrets.token_urlsafe(32)
    agent_id = str(body.get("agent_id", "agent"))
    role = str(body.get("role", "agent"))
    await db.insert("INSERT INTO api_keys(key_hash,tenant_id,agent_id,role,created_at) VALUES(?,?,?,?,?)", (hash_key(key), tenant_id, agent_id, role, now()))
    return {"api_key": key, "tenant_id": tenant_id, "agent_id": agent_id, "role": role}


@router.post("/v1/episodes", status_code=201)
async def create_episode(body: EpisodeIn, request: Request, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    item_id, stamp = ident(), now()
    await db.insert("INSERT INTO episodes(id,tenant_id,user_id,agent_id,session_id,ts,event_type,content,outcome,importance,embedding_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (item_id, ctx.tenant_id, body.user_id, ctx.agent_id, body.session_id, stamp, body.event_type, body.content, body.outcome, body.importance, item_id))
    put_vector(ctx.tenant_id, item_id, body.content)
    await write_through(graph, "upsert_episode", ctx.tenant_id, dict(await db.fetchone("SELECT * FROM episodes WHERE tenant_id=? AND id=?", (ctx.tenant_id, item_id))))
    await audit(db, ctx, "episode.create", item_id)
    return {"id": item_id, "tenant_id": ctx.tenant_id, "agent_id": ctx.agent_id, "ts": stamp, **body.model_dump()}


@router.get("/v1/episodes")
async def list_episodes(user_id: str | None = None, agent_id: str | None = None, session_id: str | None = None, since: str | None = None, limit: int = 50, offset: int = 0, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    clauses, params = ["tenant_id=?", "archived=0"], [ctx.tenant_id]
    for field, value in (("user_id", user_id), ("agent_id", agent_id), ("session_id", session_id)):
        if value:
            clauses.append(f"{field}=?")
            params.append(value)
    if since:
        clauses.append("ts>=?")
        params.append(since)
    rows = await db.fetchall(f"SELECT * FROM episodes WHERE {' AND '.join(clauses)} ORDER BY ts DESC LIMIT ? OFFSET ?", (*params, min(max(limit, 1), 200), max(offset, 0)))
    return [dict(row) for row in rows]


@router.get("/v1/episodes/{episode_id}")
async def get_episode(episode_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    row = await db.fetchone("SELECT * FROM episodes WHERE tenant_id=? AND id=?", (ctx.tenant_id, episode_id))
    if not row:
        raise HTTPException(404, "Episode not found")
    return dict(row)


@router.patch("/v1/episodes/{episode_id}")
async def update_episode(episode_id: str, body: dict, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    changed = await db.execute("UPDATE episodes SET content=COALESCE(?,content),importance=COALESCE(?,importance),outcome=COALESCE(?,outcome) WHERE tenant_id=? AND id=?", (body.get("content"), body.get("importance"), body.get("outcome"), ctx.tenant_id, episode_id))
    if not changed:
        raise HTTPException(404, "Episode not found")
    result = rowdict(await db.fetchone("SELECT * FROM episodes WHERE tenant_id=? AND id=?", (ctx.tenant_id, episode_id)))
    await write_through(graph, "upsert_episode", ctx.tenant_id, result)
    return result


@router.delete("/v1/episodes/{episode_id}")
async def delete_episode(episode_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    changed = await db.execute("UPDATE episodes SET archived=1 WHERE tenant_id=? AND id=?", (ctx.tenant_id, episode_id))
    if not changed:
        raise HTTPException(404, "Episode not found")
    await write_through(graph, "upsert_episode", ctx.tenant_id, dict(await db.fetchone("SELECT * FROM episodes WHERE tenant_id=? AND id=?", (ctx.tenant_id, episode_id))))
    return {"archived": True}

@router.post("/v1/episodes/search")
async def search_episodes(body: dict, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    query = str(body.get("query", ""))
    rows = await db.fetchall("SELECT * FROM episodes WHERE tenant_id=? AND archived=0 AND (? IS NULL OR user_id=?)", (ctx.tenant_id, body.get("user_id"), body.get("user_id")))
    index = await tenant_index(db, ctx.tenant_id)
    vector_scores = dict(index.search(embed(query), limit=max(len(index.vectors), 1)))
    scored = []
    for row in rows:
        item = dict(row)
        similarity = vector_scores.get(item["id"], 0.0)
        item["score"] = relevance_score(similarity, item["ts"], item["importance"])
        scored.append(item)
    return sorted(scored, key=lambda item: item["score"], reverse=True)[:min(max(int(body.get("limit", 10)), 1), 100)]


@router.post("/v1/facts", status_code=201)
async def create_fact(body: FactIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    return await upsert_fact(db, ctx, body, graph)


@router.get("/v1/facts")
async def list_facts(user_id: str | None = None, as_of: str | None = None, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    stamp = as_of or now()
    sql = "SELECT * FROM facts WHERE tenant_id=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)"
    params: tuple = (ctx.tenant_id, stamp, stamp)
    if user_id:
        sql += " AND user_id=?"
        params += (user_id,)
    return [dict(row) for row in await db.fetchall(sql + " ORDER BY valid_from DESC", params)]


@router.post("/v1/facts/search")
async def search_facts(body: dict, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    rows = await db.fetchall("SELECT * FROM facts WHERE tenant_id=? AND valid_to IS NULL AND (? IS NULL OR user_id=?)", (ctx.tenant_id, body.get("user_id"), body.get("user_id")))
    index = await tenant_index(db, ctx.tenant_id)
    scores = dict(index.search(embed(str(body.get("query", ""))), limit=max(len(index.vectors), 1)))
    ranked = sorted((dict(row) for row in rows), key=lambda row: scores.get(row["id"], 0.0), reverse=True)
    return ranked[:min(max(int(body.get("limit", 10)), 1), 100)]


@router.get("/v1/graph/neighbors")
async def graph_neighbors(user_id: str | None = None, depth: int = Query(default=3, ge=1, le=3), ctx: TenantContext = Depends(tenant_context), graph: GraphStore = Depends(get_graph)):
    try:
        return await graph.neighbors(ctx.tenant_id, user_id, depth=depth)
    except Exception:
        log.exception("Graph neighbor query failed", extra={"tenant_id": ctx.tenant_id})
        return {"available": False, "nodes": [], "edges": []}


@router.get("/v1/graph/provenance/{fact_id}")
async def graph_provenance(fact_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    fact = await db.fetchone("SELECT id FROM facts WHERE tenant_id=? AND id=?", (ctx.tenant_id, fact_id))
    if not fact:
        raise HTTPException(404, "Fact not found")
    try:
        chain = await graph.provenance(ctx.tenant_id, fact_id)
    except Exception:
        log.exception("Graph provenance query failed", extra={"tenant_id": ctx.tenant_id, "fact_id": fact_id})
        return {"available": False, "fact_id": fact_id, "chain": []}
    return {"available": graph.available, "fact_id": fact_id, "chain": chain}


@router.get("/v1/facts/{fact_id}/history")
async def fact_history(fact_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    row = await db.fetchone("SELECT user_id,subject,predicate FROM facts WHERE tenant_id=? AND id=?", (ctx.tenant_id, fact_id))
    if not row:
        raise HTTPException(404, "Fact not found")
    rows = await db.fetchall("SELECT * FROM facts WHERE tenant_id=? AND user_id=? AND subject=? AND predicate=? ORDER BY valid_from", (ctx.tenant_id, row["user_id"], row["subject"], row["predicate"]))
    return [dict(item) for item in rows]


@router.put("/v1/facts/{fact_id}")
async def update_fact(fact_id: str, body: FactIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    row = await db.fetchone("SELECT id FROM facts WHERE tenant_id=? AND id=?", (ctx.tenant_id, fact_id))
    if not row:
        raise HTTPException(404, "Fact not found")
    return await upsert_fact(db, ctx, body, graph)


@router.delete("/v1/facts/{fact_id}")
async def close_fact(fact_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    changed = await db.execute("UPDATE facts SET valid_to=? WHERE tenant_id=? AND id=? AND valid_to IS NULL", (now(), ctx.tenant_id, fact_id))
    if not changed:
        raise HTTPException(404, "Fact not found")
    await write_through(graph, "upsert_fact", ctx.tenant_id, dict(await db.fetchone("SELECT * FROM facts WHERE tenant_id=? AND id=?", (ctx.tenant_id, fact_id))))
    return {"closed": True}


@router.delete("/v1/blackboard/{key}")
async def blackboard_delete(key: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    changed = await db.execute("DELETE FROM blackboard WHERE tenant_id=? AND key=?", (ctx.tenant_id, key))
    if not changed:
        raise HTTPException(404, "Key not found")
    return {"deleted": True}
@router.post("/v1/facts/extract")
async def extract_facts(body: dict, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    text = str(body.get("text", ""))
    user_id = str(body.get("user_id", "unknown"))
    response = await get_llm().complete(f"Extract user facts as JSON array of subject,predicate,object from this text: {text}")
    try:
        parsed = json.loads(response[response.find("["):response.rfind("]") + 1])
    except (ValueError, TypeError):
        response = await get_llm().complete(f"Return only valid JSON array of facts. Text: {text}")
        parsed = json.loads(response[response.find("["):response.rfind("]") + 1])
    result = []
    for fact in parsed:
        if all(key in fact for key in ("subject", "predicate", "object")):
            result.append(await upsert_fact(db, ctx, FactIn(user_id=user_id, **fact), graph))
    return result


@router.delete("/v1/handoffs/{handoff_id}")
async def delete_handoff(handoff_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    changed = await db.execute("DELETE FROM handoffs WHERE tenant_id=? AND id=? AND from_agent=? AND status='pending'", (ctx.tenant_id, handoff_id, ctx.agent_id))
    if not changed:
        raise HTTPException(404, "Handoff not found")
    await write_through(graph, "delete_handoff", ctx.tenant_id, handoff_id)
    return {"deleted": True}


@router.delete("/v1/procedures/{procedure_id}")
async def delete_procedure(procedure_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    changed = await db.execute("UPDATE procedures SET is_active=0 WHERE tenant_id=? AND id=? AND is_active=1", (ctx.tenant_id, procedure_id))
    if not changed:
        raise HTTPException(404, "Procedure not found")
    await write_through(graph, "upsert_procedure", ctx.tenant_id, dict(await db.fetchone("SELECT * FROM procedures WHERE tenant_id=? AND id=?", (ctx.tenant_id, procedure_id))))
    return {"archived": True}
@router.post("/v1/procedures", status_code=201)
async def create_procedure(body: ProcedureIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    item_id = ident()
    await db.insert("INSERT INTO procedures(id,tenant_id,name,trigger_text,steps_json,version,updated_at) VALUES(?,?,?,?,?,1,?)", (item_id, ctx.tenant_id, body.name, body.trigger_text, json.dumps(body.steps), now()))
    put_vector(ctx.tenant_id, item_id, body.trigger_text)
    await write_through(graph, "upsert_procedure", ctx.tenant_id, dict(await db.fetchone("SELECT * FROM procedures WHERE tenant_id=? AND id=?", (ctx.tenant_id, item_id))))
    return {"id": item_id, **body.model_dump(), "version": 1, "is_active": True}


@router.get("/v1/procedures")
async def list_procedures(ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    return [dict(row) for row in await db.fetchall("SELECT * FROM procedures WHERE tenant_id=? ORDER BY updated_at DESC", (ctx.tenant_id,))]


@router.get("/v1/procedures/{procedure_id}")
async def get_procedure(procedure_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    row = await db.fetchone("SELECT * FROM procedures WHERE tenant_id=? AND id=?", (ctx.tenant_id, procedure_id))
    if not row:
        raise HTTPException(404, "Procedure not found")
    return dict(row)


@router.put("/v1/procedures/{procedure_id}")
async def update_procedure(procedure_id: str, body: ProcedureIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    old = await db.fetchone("SELECT * FROM procedures WHERE tenant_id=? AND id=? AND is_active=1", (ctx.tenant_id, procedure_id))
    if not old:
        raise HTTPException(404, "Procedure not found")
    new_id = ident()
    await db.transaction([
        ("UPDATE procedures SET is_active=0 WHERE tenant_id=? AND id=?", (ctx.tenant_id, procedure_id)),
        ("INSERT INTO procedures(id,tenant_id,name,trigger_text,steps_json,version,success_count,failure_count,is_active,updated_at) VALUES(?,?,?,?,?,?,?,?,1,?)", (new_id, ctx.tenant_id, body.name, body.trigger_text, json.dumps(body.steps), old["version"] + 1, old["success_count"], old["failure_count"], now())),
    ])
    put_vector(ctx.tenant_id, new_id, body.trigger_text)
    await write_through(graph, "upsert_procedure", ctx.tenant_id, dict(await db.fetchone("SELECT * FROM procedures WHERE tenant_id=? AND id=?", (ctx.tenant_id, procedure_id))))
    await write_through(graph, "upsert_procedure", ctx.tenant_id, dict(await db.fetchone("SELECT * FROM procedures WHERE tenant_id=? AND id=?", (ctx.tenant_id, new_id))))
    return {"id": new_id, **body.model_dump(), "version": old["version"] + 1}


@router.post("/v1/procedures/match")
async def match_procedures(body: dict, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    rows = await db.fetchall("SELECT * FROM procedures WHERE tenant_id=? AND is_active=1", (ctx.tenant_id,))
    index = await tenant_index(db, ctx.tenant_id)
    scores = dict(index.search(embed(str(body.get("situation", ""))), limit=max(len(index.vectors), 1)))
    ranked = sorted((dict(row) for row in rows), key=lambda row: scores.get(row["id"], 0.0), reverse=True)
    return ranked[:min(max(int(body.get("limit", 5)), 1), 50)]


@router.post("/v1/procedures/{procedure_id}/outcome")
async def procedure_outcome(procedure_id: str, body: dict, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    field = "success_count" if body.get("success") else "failure_count"
    changed = await db.execute(f"UPDATE procedures SET {field}={field}+1 WHERE tenant_id=? AND id=? AND is_active=1", (ctx.tenant_id, procedure_id))
    if not changed:
        raise HTTPException(404, "Procedure not found")
    await db.insert("INSERT OR IGNORE INTO procedure_usage(tenant_id,procedure_id,agent_id,first_used_at) VALUES(?,?,?,?)", (ctx.tenant_id, procedure_id, ctx.agent_id, now()))
    result = rowdict(await db.fetchone("SELECT * FROM procedures WHERE tenant_id=? AND id=?", (ctx.tenant_id, procedure_id)))
    usage = await db.fetchall("SELECT agent_id FROM procedure_usage WHERE tenant_id=? AND procedure_id=?", (ctx.tenant_id, procedure_id))
    await write_through(graph, "upsert_procedure", ctx.tenant_id, {**result, "used_by_agent_ids": [row["agent_id"] for row in usage]})
    return result


@router.post("/v1/messages", status_code=201)
async def append_message(body: MessageIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    conversation = await db.fetchone("SELECT id FROM conversations WHERE tenant_id=? AND user_id=? AND session_id=?", (ctx.tenant_id, body.user_id, body.session_id))
    conversation_id = conversation["id"] if conversation else ident()
    if not conversation:
        await db.insert("INSERT INTO conversations(id,tenant_id,user_id,session_id,created_at) VALUES(?,?,?,?,?)", (conversation_id, ctx.tenant_id, body.user_id, body.session_id, now()))
    message_id = ident()
    await db.insert("INSERT INTO messages(id,conversation_id,tenant_id,role,agent_id,content,ts) VALUES(?,?,?,?,?,?,?)", (message_id, conversation_id, ctx.tenant_id, body.role, ctx.agent_id, body.content, now()))
    rows = await db.fetchall("SELECT id,role,content FROM messages WHERE tenant_id=? AND conversation_id=? ORDER BY ts", (ctx.tenant_id, conversation_id))
    previous = await db.fetchone("SELECT upto_message_id FROM summaries WHERE tenant_id=? AND conversation_id=? ORDER BY created_at DESC LIMIT 1", (ctx.tenant_id, conversation_id))
    unsummarized = after_cursor(rows, previous["upto_message_id"] if previous else None)
    if len(unsummarized) >= settings.summary_threshold:
        summary = await get_llm().complete(summary_prompt(unsummarized))
        await db.insert("INSERT INTO summaries(id,conversation_id,tenant_id,upto_message_id,text,created_at) VALUES(?,?,?,?,?,?)", (ident(), conversation_id, ctx.tenant_id, unsummarized[-1]["id"], summary, now()))
    return {"id": message_id, "conversation_id": conversation_id}


@router.get("/v1/history/{user_id}/{session_id}")
async def get_history(user_id: str, session_id: str, limit: int | None = None, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    conv = await db.fetchone("SELECT id FROM conversations WHERE tenant_id=? AND user_id=? AND session_id=?", (ctx.tenant_id, user_id, session_id))
    if not conv:
        return {"summary": None, "messages": []}
    summary = await db.fetchone("SELECT text FROM summaries WHERE tenant_id=? AND conversation_id=? ORDER BY created_at DESC LIMIT 1", (ctx.tenant_id, conv["id"]))
    suffix = " LIMIT ?" if limit else ""
    params = (ctx.tenant_id, conv["id"], min(max(limit, 1), 100)) if limit else (ctx.tenant_id, conv["id"])
    messages = await db.fetchall("SELECT * FROM messages WHERE tenant_id=? AND conversation_id=? ORDER BY ts DESC" + suffix, params)
    return {"summary": summary["text"] if summary else None, "messages": [dict(row) for row in reversed(messages)]}


@router.post("/v1/recall")
async def recall(body: RecallIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    facts = await search_facts({"query": body.query, "user_id": body.user_id, "limit": 10}, ctx, db)
    episodes = await search_episodes({"query": body.query, "user_id": body.user_id, "limit": 10}, ctx, db)
    if graph.available:
        seed_ids = [item["id"] for item in (*facts, *episodes)]
        if seed_ids:
            try:
                related = await graph.neighbors(ctx.tenant_id, body.user_id, seed_ids=seed_ids, depth=3)
                fact_ids = list(dict.fromkeys(node["id"] for node in related["nodes"] if "Fact" in node.get("labels", []) and node.get("id")))
                episode_ids = list(dict.fromkeys(node["id"] for node in related["nodes"] if "Episode" in node.get("labels", []) and node.get("id")))
                existing_facts = {item["id"] for item in facts}
                existing_episodes = {item["id"] for item in episodes}
                if fact_ids:
                    placeholders = ",".join("?" for _ in fact_ids)
                    rows = await db.fetchall(f"SELECT * FROM facts WHERE tenant_id=? AND user_id=? AND valid_to IS NULL AND id IN ({placeholders})", (ctx.tenant_id, body.user_id, *fact_ids))
                    facts.extend({**dict(row), "related": True} for row in rows if row["id"] not in existing_facts)
                if episode_ids:
                    placeholders = ",".join("?" for _ in episode_ids)
                    rows = await db.fetchall(f"SELECT * FROM episodes WHERE tenant_id=? AND user_id=? AND archived=0 AND id IN ({placeholders})", (ctx.tenant_id, body.user_id, *episode_ids))
                    episodes.extend({**dict(row), "related": True} for row in rows if row["id"] not in existing_episodes)
            except Exception:
                log.exception("Graph recall expansion failed; returning vector results", extra={"tenant_id": ctx.tenant_id})
    procedures = await match_procedures({"situation": body.query}, ctx, db)
    history = await get_history(body.user_id, body.session_id, 8, ctx, db) if body.session_id else {"summary": None, "messages": []}
    handoffs = await db.fetchall("SELECT * FROM handoffs WHERE tenant_id=? AND to_agent=? AND status='pending' AND user_id=?", (ctx.tenant_id, ctx.agent_id, body.user_id))
    sections = [("facts", facts), ("episodes", episodes), ("procedures", procedures), ("summary", history["summary"]), ("messages", history["messages"]), ("handoffs", [dict(row) for row in handoffs])]
    limit = body.budget_tokens * 4
    result = {}
    for key, value in sections:
        if len(json.dumps(result, default=str)) >= limit:
            break
        if isinstance(value, list):
            selected = []
            for item in value:
                candidate = {**result, key: [*selected, item]}
                if len(json.dumps(candidate, default=str)) > limit:
                    break
                selected.append(item)
            result[key] = selected
        elif value is None or len(json.dumps({**result, key: value}, default=str)) <= limit:
            result[key] = value
        else:
            remaining = limit - len(json.dumps(result, default=str))
            result[key] = value[:max(remaining, 0)] if isinstance(value, str) else None
    return result


@router.get("/v1/blackboard/stream")
async def blackboard_stream(ctx: TenantContext = Depends(tenant_context)):
    queue: asyncio.Queue = asyncio.Queue()
    events.setdefault(ctx.tenant_id, set()).add(queue)
    async def stream():
        try:
            while True:
                event = await queue.get()
                yield f"data: {json.dumps(event)}\n\n"
        finally:
            events.get(ctx.tenant_id, set()).discard(queue)
    return StreamingResponse(stream(), media_type="text/event-stream")


@router.get("/v1/blackboard/{key}")
async def blackboard_get(key: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    row = await db.fetchone("SELECT * FROM blackboard WHERE tenant_id=? AND key=?", (ctx.tenant_id, key))
    if not row:
        raise HTTPException(404, "Key not found")
    return {**dict(row), "value": json.loads(row["value_json"])}


@router.put("/v1/blackboard/{key}")
async def blackboard_put(key: str, body: BlackboardIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    async with db.lock:
        await db.conn.execute("BEGIN IMMEDIATE")
        current = await (await db.conn.execute("SELECT version,value_json FROM blackboard WHERE tenant_id=? AND key=?", (ctx.tenant_id, key))).fetchone()
        version = current["version"] if current else 0
        if version != body.expected_version:
            await db.conn.rollback()
            raise HTTPException(409, conflict_state(current))
        new_version, stamp = version + 1, now()
        await db.conn.execute("INSERT INTO blackboard(tenant_id,key,value_json,version,updated_by,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(tenant_id,key) DO UPDATE SET value_json=excluded.value_json,version=excluded.version,updated_by=excluded.updated_by,updated_at=excluded.updated_at", (ctx.tenant_id, key, json.dumps(body.value), new_version, ctx.agent_id, stamp))
        await db.conn.commit()
    payload = {"tenant_id": ctx.tenant_id, "key": key, "value": body.value, "version": new_version, "updated_by": ctx.agent_id, "updated_at": stamp}
    for queue in events.get(ctx.tenant_id, set()):
        queue.put_nowait(payload)
    return payload


@router.get("/v1/blackboard")
async def blackboard_list(prefix: str = "", ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    rows = await db.fetchall("SELECT * FROM blackboard WHERE tenant_id=? AND key LIKE ? ORDER BY key", (ctx.tenant_id, prefix + "%"))
    return [{**dict(row), "value": json.loads(row["value_json"])} for row in rows]


@router.post("/v1/handoffs", status_code=201)
async def create_handoff(body: HandoffIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    item_id, stamp = ident(), now()
    summary = body.summary
    if not summary:
        conversation = await db.fetchone("SELECT id FROM conversations WHERE tenant_id=? AND user_id=? AND session_id=?", (ctx.tenant_id, body.user_id, body.session_id))
        if conversation:
            latest = await db.fetchone("SELECT text FROM summaries WHERE tenant_id=? AND conversation_id=? ORDER BY created_at DESC LIMIT 1", (ctx.tenant_id, conversation["id"]))
            messages = await db.fetchall("SELECT role,content FROM messages WHERE tenant_id=? AND conversation_id=? ORDER BY ts DESC LIMIT 10", (ctx.tenant_id, conversation["id"]))
            summary = conversation_summary(latest, messages)
    memory_refs = body.memory_refs
    if not memory_refs:
        episodes = await db.fetchall("SELECT id FROM episodes WHERE tenant_id=? AND user_id=? AND archived=0 ORDER BY ts DESC LIMIT 5", (ctx.tenant_id, body.user_id))
        facts = await db.fetchall("SELECT id FROM facts WHERE tenant_id=? AND user_id=? AND valid_to IS NULL ORDER BY valid_from DESC LIMIT 5", (ctx.tenant_id, body.user_id))
        memory_refs = [row["id"] for row in (*episodes, *facts)]
    await db.insert("INSERT INTO handoffs(id,tenant_id,from_agent,to_agent,user_id,session_id,summary,memory_refs_json,open_tasks_json,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,'pending',?)", (item_id, ctx.tenant_id, ctx.agent_id, body.to_agent, body.user_id, body.session_id, summary, json.dumps(memory_refs), json.dumps(body.open_tasks), stamp))
    await write_through(graph, "upsert_handoff", ctx.tenant_id, dict(await db.fetchone("SELECT * FROM handoffs WHERE tenant_id=? AND id=?", (ctx.tenant_id, item_id))))
    return {"id": item_id, "status": "pending", **body.model_dump(exclude={"summary", "memory_refs"}), "summary": summary, "memory_refs": memory_refs, "from_agent": ctx.agent_id, "created_at": stamp}


@router.get("/v1/handoffs/inbox")
async def handoff_inbox(ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    return [dict(row) for row in await db.fetchall("SELECT * FROM handoffs WHERE tenant_id=? AND to_agent=? AND status='pending' ORDER BY created_at", (ctx.tenant_id, ctx.agent_id))]


@router.get("/v1/handoffs/{handoff_id}")
async def get_handoff(handoff_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    row = await db.fetchone("SELECT * FROM handoffs WHERE tenant_id=? AND id=?", (ctx.tenant_id, handoff_id))
    if not row:
        raise HTTPException(404, "Handoff not found")
    return dict(row)


@router.post("/v1/handoffs/{handoff_id}/accept")
async def accept_handoff(handoff_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    changed = await db.execute("UPDATE handoffs SET status='accepted',accepted_at=? WHERE tenant_id=? AND id=? AND to_agent=? AND status='pending'", (now(), ctx.tenant_id, handoff_id, ctx.agent_id))
    if not changed:
        raise HTTPException(404, "Handoff not found")
    result = rowdict(await db.fetchone("SELECT * FROM handoffs WHERE tenant_id=? AND id=?", (ctx.tenant_id, handoff_id)))
    await write_through(graph, "upsert_handoff", ctx.tenant_id, result)
    return result


@router.post("/v1/handoffs/{handoff_id}/complete")
async def complete_handoff(handoff_id: str, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    changed = await db.execute("UPDATE handoffs SET status='completed' WHERE tenant_id=? AND id=? AND to_agent=? AND status='accepted'", (ctx.tenant_id, handoff_id, ctx.agent_id))
    if not changed:
        raise HTTPException(404, "Handoff not found")
    result = rowdict(await db.fetchone("SELECT * FROM handoffs WHERE tenant_id=? AND id=?", (ctx.tenant_id, handoff_id)))
    await write_through(graph, "upsert_handoff", ctx.tenant_id, result)
    return result


@router.post("/v1/leases/acquire")
async def lease_acquire(body: LeaseIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    stamp, expires = time.time(), time.time() + body.ttl_seconds
    async with db.lock:
        await db.conn.execute("BEGIN IMMEDIATE")
        current = await (await db.conn.execute("SELECT holder,expires_at FROM leases WHERE tenant_id=? AND resource=?", (ctx.tenant_id, body.resource))).fetchone()
        if lease_is_active(current, stamp) and current["holder"] != ctx.agent_id:
            await db.conn.rollback()
            raise HTTPException(409, "Lease is held")
        await db.conn.execute("INSERT INTO leases(tenant_id,resource,holder,expires_at) VALUES(?,?,?,?) ON CONFLICT(tenant_id,resource) DO UPDATE SET holder=excluded.holder,expires_at=excluded.expires_at", (ctx.tenant_id, body.resource, ctx.agent_id, expires))
        await db.conn.commit()
    return {"resource": body.resource, "holder": ctx.agent_id, "expires_at": expires}


@router.post("/v1/leases/renew")
async def lease_renew(body: LeaseIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    expires = time.time() + body.ttl_seconds
    changed = await db.execute("UPDATE leases SET expires_at=? WHERE tenant_id=? AND resource=? AND holder=? AND expires_at>?", (expires, ctx.tenant_id, body.resource, ctx.agent_id, time.time()))
    if not changed:
        raise HTTPException(404, "Active lease not found")
    return {"resource": body.resource, "holder": ctx.agent_id, "expires_at": expires}


@router.post("/v1/leases/release")
async def lease_release(body: LeaseIn, ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    changed = await db.execute("DELETE FROM leases WHERE tenant_id=? AND resource=? AND holder=?", (ctx.tenant_id, body.resource, ctx.agent_id))
    if not changed:
        raise HTTPException(404, "Lease not found")
    return {"released": True}


@router.post("/v1/consolidate")
async def consolidate(ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db), graph: GraphStore = Depends(get_graph)):
    episodes = await db.fetchall("SELECT * FROM episodes WHERE tenant_id=? AND archived=0 ORDER BY ts DESC LIMIT 100", (ctx.tenant_id,))
    report = {"episodes_scanned": len(episodes), "facts_added": 0, "facts_merged": 0, "procedures_created": 0, "archived": 0, "procedures_flagged": []}
    for episode in episodes[:20]:
        prompt = f"Extract facts as JSON array. Text: {episode['content']}"
        try:
            raw = await get_llm().complete(prompt)
            facts = json.loads(raw[raw.find("["):raw.rfind("]") + 1])
            for fact in facts:
                if all(key in fact for key in ("subject", "predicate", "object")):
                    await upsert_fact(db, ctx, FactIn(user_id=episode["user_id"], **fact), graph, episode["id"])
                    report["facts_added"] += 1
        except (ValueError, TypeError):
            log.warning("Fact extraction failed during consolidation")
    facts = await db.fetchall("SELECT * FROM facts WHERE tenant_id=? AND valid_to IS NULL ORDER BY confidence DESC,valid_from DESC", (ctx.tenant_id,))
    canonical: dict[tuple[str, str, str], str] = {}
    for fact in facts:
        normalized = normalized_value(fact["object"])
        key = (fact["user_id"], fact["subject"].lower(), fact["predicate"].lower(), normalized)
        if key in canonical:
            await db.execute("UPDATE facts SET valid_to=?,superseded_by=? WHERE tenant_id=? AND id=? AND valid_to IS NULL", (now(), canonical[key], ctx.tenant_id, fact["id"]))
            duplicate = dict(await db.fetchone("SELECT * FROM facts WHERE tenant_id=? AND id=?", (ctx.tenant_id, fact["id"])))
            replacement = dict(await db.fetchone("SELECT * FROM facts WHERE tenant_id=? AND id=?", (ctx.tenant_id, canonical[key])))
            await write_through(graph, "upsert_fact", ctx.tenant_id, duplicate)
            await write_through(graph, "upsert_fact", ctx.tenant_id, replacement, duplicate["id"])
            report["facts_merged"] += 1
        else:
            canonical[key] = fact["id"]
    patterns = successful_groups(episodes)
    for (event_type, outcome), pattern_episodes in patterns.items():
        exists = await db.fetchone("SELECT id FROM procedures WHERE tenant_id=? AND trigger_text=? AND is_active=1", (ctx.tenant_id, event_type))
        if len(pattern_episodes) >= 3 and not exists:
            proc_id = ident()
            steps = [row["content"] for row in pattern_episodes[:3]]
            await db.insert("INSERT INTO procedures(id,tenant_id,name,trigger_text,steps_json,version,success_count,updated_at) VALUES(?,?,?,?,?,1,?,?)", (proc_id, ctx.tenant_id, f"Repeated {event_type} resolution", event_type, json.dumps(steps), len(pattern_episodes), now()))
            put_vector(ctx.tenant_id, proc_id, event_type)
            await write_through(graph, "upsert_procedure", ctx.tenant_id, dict(await db.fetchone("SELECT * FROM procedures WHERE tenant_id=? AND id=?", (ctx.tenant_id, proc_id))))
            report["procedures_created"] += 1
    procedures = await db.fetchall("SELECT id,success_count,failure_count FROM procedures WHERE tenant_id=? AND is_active=1", (ctx.tenant_id,))
    report["procedures_flagged"] = [row["id"] for row in procedures if needs_review(row["success_count"], row["failure_count"])]
    stale = await db.execute("UPDATE episodes SET archived=1,importance=importance*0.5 WHERE tenant_id=? AND archived=0 AND importance<0.1 AND ts<?", (ctx.tenant_id, datetime.fromtimestamp(time.time() - 90 * 86400, timezone.utc).isoformat()))
    report["archived"] = stale
    await db.insert("INSERT INTO consolidation_reports(id,tenant_id,report_json,created_at) VALUES(?,?,?,?)", (ident(), ctx.tenant_id, json.dumps(report), now()))
    return report


@router.get("/v1/consolidate/reports")
async def consolidation_reports(ctx: TenantContext = Depends(tenant_context), db: Database = Depends(get_db)):
    return [dict(row) for row in await db.fetchall("SELECT * FROM consolidation_reports WHERE tenant_id=? ORDER BY created_at DESC LIMIT 50", (ctx.tenant_id,))]


@router.get("/v1/metrics")
async def get_metrics():
    durations = sorted(value for value, _ in metrics[-1000:])
    def percentile(p):
        return durations[min(int((len(durations) - 1) * p), len(durations) - 1)] if durations else 0
    return {"requests": len(metrics), "p50_ms": percentile(.5) * 1000, "p95_ms": percentile(.95) * 1000, "p99_ms": percentile(.99) * 1000}


async def audit(db: Database, ctx: TenantContext, action: str, resource: str) -> None:
    await db.insert("INSERT INTO audit_log(id,tenant_id,agent_id,action,resource,ts) VALUES(?,?,?,?,?,?)", (ident(), ctx.tenant_id, ctx.agent_id, action, resource, now()))