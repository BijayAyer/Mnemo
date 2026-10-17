"""SQLite schema and serialized async access."""

import asyncio
from pathlib import Path
import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants(id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS api_keys(key_hash TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, role TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS api_keys_tenant ON api_keys(tenant_id);
CREATE TABLE IF NOT EXISTS episodes(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, user_id TEXT NOT NULL, agent_id TEXT NOT NULL, session_id TEXT NOT NULL, ts TEXT NOT NULL, event_type TEXT NOT NULL, content TEXT NOT NULL, outcome TEXT, importance REAL NOT NULL, embedding_id TEXT, archived INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS episodes_tenant_user ON episodes(tenant_id,user_id,ts);
CREATE TABLE IF NOT EXISTS facts(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, user_id TEXT NOT NULL, subject TEXT NOT NULL, predicate TEXT NOT NULL, object TEXT NOT NULL, confidence REAL NOT NULL, source_episode_id TEXT, valid_from TEXT NOT NULL, valid_to TEXT, superseded_by TEXT, embedding_id TEXT);
CREATE INDEX IF NOT EXISTS facts_tenant_user ON facts(tenant_id,user_id,valid_from);
CREATE TABLE IF NOT EXISTS procedures(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, name TEXT NOT NULL, trigger_text TEXT NOT NULL, steps_json TEXT NOT NULL, version INTEGER NOT NULL, success_count INTEGER NOT NULL DEFAULT 0, failure_count INTEGER NOT NULL DEFAULT 0, is_active INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL, embedding_id TEXT);
CREATE TABLE IF NOT EXISTS conversations(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, user_id TEXT NOT NULL, session_id TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(tenant_id,user_id,session_id));
CREATE TABLE IF NOT EXISTS messages(id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, tenant_id TEXT NOT NULL, role TEXT NOT NULL, agent_id TEXT, content TEXT NOT NULL, ts TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS messages_tenant_conversation ON messages(tenant_id,conversation_id,ts);
CREATE TABLE IF NOT EXISTS summaries(id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, tenant_id TEXT NOT NULL, upto_message_id TEXT NOT NULL, text TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS blackboard(tenant_id TEXT NOT NULL, key TEXT NOT NULL, value_json TEXT NOT NULL, version INTEGER NOT NULL, updated_by TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(tenant_id,key));
CREATE TABLE IF NOT EXISTS handoffs(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, from_agent TEXT NOT NULL, to_agent TEXT NOT NULL, user_id TEXT NOT NULL, session_id TEXT NOT NULL, summary TEXT NOT NULL, memory_refs_json TEXT NOT NULL, open_tasks_json TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, accepted_at TEXT);
CREATE INDEX IF NOT EXISTS handoffs_inbox ON handoffs(tenant_id,to_agent,status);
CREATE TABLE IF NOT EXISTS leases(tenant_id TEXT NOT NULL, resource TEXT NOT NULL, holder TEXT NOT NULL, expires_at REAL NOT NULL, PRIMARY KEY(tenant_id,resource));
CREATE TABLE IF NOT EXISTS audit_log(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, agent_id TEXT NOT NULL, action TEXT NOT NULL, resource TEXT NOT NULL, ts TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS consolidation_reports(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, report_json TEXT NOT NULL, created_at TEXT NOT NULL);
"""


class Database:
    """A single WAL connection guarded by a process-local write/read lock."""

    def __init__(self, path: Path):
        self.path = path
        self.conn: aiosqlite.Connection | None = None
        self.lock = asyncio.Lock()
        self.readers: list[aiosqlite.Connection] = []
        self.reader_locks: list[asyncio.Lock] = []
        self.reader_index = 0
        self.reader_pool_lock = asyncio.Lock()

    async def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = await aiosqlite.connect(self.path, timeout=5)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.execute("PRAGMA busy_timeout=5000")
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()
        self.readers = [await aiosqlite.connect(self.path, timeout=5) for _ in range(4)]
        for reader in self.readers:
            reader.row_factory = aiosqlite.Row
            await reader.execute("PRAGMA busy_timeout=5000")
        self.reader_locks = [asyncio.Lock() for _ in self.readers]

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()
            self.conn = None
        for reader in self.readers:
            await reader.close()
        self.readers.clear()
        self.reader_locks.clear()

    async def execute(self, sql: str, params: tuple = ()) -> int:
        async with self.lock:
            cursor = await self.conn.execute(sql, params)
            await self.conn.commit()
            return cursor.rowcount

    async def insert(self, sql: str, params: tuple = ()) -> None:
        async with self.lock:
            await self.conn.execute(sql, params)
            await self.conn.commit()

    async def fetchone(self, sql: str, params: tuple = ()):
        reader, lock = await self._reader()
        async with lock:
            cursor = await reader.execute(sql, params)
            return await cursor.fetchone()

    async def fetchall(self, sql: str, params: tuple = ()):
        reader, lock = await self._reader()
        async with lock:
            cursor = await reader.execute(sql, params)
            return await cursor.fetchall()

    async def _reader(self):
        async with self.reader_pool_lock:
            index = self.reader_index % len(self.readers)
            self.reader_index += 1
        return self.readers[index], self.reader_locks[index]

    async def transaction(self, statements: list[tuple[str, tuple]]) -> None:
        async with self.lock:
            await self.conn.execute("BEGIN IMMEDIATE")
            try:
                for sql, params in statements:
                    await self.conn.execute(sql, params)
                await self.conn.commit()
            except BaseException:
                await self.conn.rollback()
                raise