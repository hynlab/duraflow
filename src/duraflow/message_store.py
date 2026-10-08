"""Atomic message consumption, state and outgoing intent; no broker I/O in locks."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import closing
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from .contracts import Clock, Conflict, canonical, fingerprint
from .messaging import Message, Publication
from .storage import clone

Update = Callable[[dict[str, Any], float], list[Publication]]


class MessageStore(Protocol):
    async def apply(self, key: str, message: Message, update: Update) -> bool: ...
    async def read(self, key: str) -> dict[str, Any]: ...
    async def list_states(self, prefix: str, *, after: str = "", limit: int = 100) -> list[dict[str, Any]]: ...
    async def claim(self, owner: str, limit: int = 32) -> list[Publication]: ...
    async def delivered(self, message_id: str, owner: str) -> None: ...
    async def release(self, message_id: str, owner: str) -> None: ...
    async def ping(self) -> bool: ...
    async def close(self) -> None: ...


class MemoryMessageStore:
    def __init__(self, *, clock: Clock | None = None):
        self.clock = clock or Clock()
        self.documents: dict[str, dict[str, Any]] = {}
        self.inbox: dict[tuple[str, str], str] = {}
        self.outbox: dict[str, dict[str, Any]] = {}
        self.lock = asyncio.Lock()

    async def apply(self, key: str, message: Message, update: Update) -> bool:
        async with self.lock:
            digest = fingerprint(json.loads(message.to_bytes()))
            previous = self.inbox.get((key, message.id))
            if previous is not None:
                if previous != digest:
                    raise Conflict("Message ID reused with different content")
                return False
            state = clone(self.documents.get(key, {}))
            outgoing = update(state, self.clock.now())
            pending: dict[str, dict[str, Any]] = {}
            for item in outgoing:
                item.message.to_bytes()
                document = clone(item.document())
                previous_item = self.outbox.get(item.message.id)
                previous_document = pending.get(item.message.id)
                if previous_item is not None:
                    previous_document = previous_item["publication"]
                if previous_document is not None and canonical(previous_document) != canonical(document):
                    raise Conflict("Outgoing identity conflict")
                pending[item.message.id] = document
            self.documents[key] = clone(state)
            self.inbox[key, message.id] = digest
            for message_id, document in pending.items():
                self.outbox.setdefault(message_id, {"publication": document, "owner": None, "until": 0})
            return True

    async def read(self, key: str) -> dict[str, Any]:
        async with self.lock:
            return clone(self.documents.get(key, {}))

    async def list_states(self, prefix: str, *, after: str = "", limit: int = 100) -> list[dict[str, Any]]:
        async with self.lock:
            return [
                clone(self.documents[key]) for key in sorted(self.documents) if key.startswith(prefix) and key > after
            ][:limit]

    async def claim(self, owner: str, limit: int = 32) -> list[Publication]:
        async with self.lock:
            result = []
            for item in self.outbox.values():
                if item["until"] <= self.clock.now():
                    item["owner"], item["until"] = owner, self.clock.now() + 30
                    result.append(Publication.restore(clone(item["publication"])))
                    if len(result) == limit:
                        break
            return result

    async def delivered(self, message_id: str, owner: str) -> None:
        async with self.lock:
            if message_id in self.outbox and self.outbox[message_id]["owner"] == owner:
                del self.outbox[message_id]

    async def release(self, message_id: str, owner: str) -> None:
        async with self.lock:
            item = self.outbox.get(message_id)
            if item and item["owner"] == owner:
                item["owner"], item["until"] = None, self.clock.now() + 1

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        pass


class SQLiteMessageStore:
    """File-backed local runtime. Separate tables leave protocol-1 histories intact."""

    def __init__(self, path: str | Path, *, clock: Clock | None = None):
        if str(path) == ":memory:":
            raise ValueError("Use MemoryMessageStore for in-memory state")
        self.path, self.clock = str(path), clock or Clock()
        with closing(self._connect()) as conn:
            conn.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS message_states(key TEXT PRIMARY KEY, document TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS message_schema_version(version INTEGER PRIMARY KEY);
                INSERT OR IGNORE INTO message_schema_version VALUES(1);
                CREATE TABLE IF NOT EXISTS message_inbox(key TEXT, id TEXT, digest TEXT NOT NULL, PRIMARY KEY(key,id));
                CREATE TABLE IF NOT EXISTS message_outbox(id TEXT PRIMARY KEY, document TEXT NOT NULL,
                    owner TEXT, lease_until REAL NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS message_outbox_due ON message_outbox(lease_until);
            """)
            if conn.execute("SELECT version FROM message_schema_version").fetchall() != [(1,)]:
                raise Conflict("Unsupported message-store schema version")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    async def apply(self, key: str, message: Message, update: Update) -> bool:
        def execute() -> bool:
            conn = self._connect()
            try:
                with conn:
                    conn.execute("BEGIN IMMEDIATE")
                    digest = fingerprint(json.loads(message.to_bytes()))
                    row = conn.execute(
                        "SELECT digest FROM message_inbox WHERE key=? AND id=?", (key, message.id)
                    ).fetchone()
                    if row:
                        if row[0] != digest:
                            raise Conflict("Message ID reused with different content")
                        return False
                    row = conn.execute("SELECT document FROM message_states WHERE key=?", (key,)).fetchone()
                    state = json.loads(row[0]) if row else {}
                    outgoing = update(state, self.clock.now())
                    conn.execute("INSERT OR REPLACE INTO message_states VALUES(?,?)", (key, canonical(state)))
                    conn.execute("INSERT INTO message_inbox VALUES(?,?,?)", (key, message.id, digest))
                    for item in outgoing:
                        item.message.to_bytes()
                        inserted = conn.execute(
                            "INSERT INTO message_outbox(id,document) VALUES(?,?) "
                            "ON CONFLICT(id) DO UPDATE SET document=excluded.document "
                            "WHERE message_outbox.document=excluded.document RETURNING id",
                            (item.message.id, canonical(item.document())),
                        ).fetchone()
                        if inserted is None:
                            raise Conflict("Outgoing identity conflict")
                    return True
            finally:
                conn.close()

        return await asyncio.to_thread(execute)

    async def read(self, key: str) -> dict[str, Any]:
        def execute() -> dict[str, Any]:
            conn = self._connect()
            try:
                row = conn.execute("SELECT document FROM message_states WHERE key=?", (key,)).fetchone()
                return json.loads(row[0]) if row else {}
            finally:
                conn.close()

        return await asyncio.to_thread(execute)

    async def claim(self, owner: str, limit: int = 32) -> list[Publication]:
        def execute() -> list[Publication]:
            conn = self._connect()
            try:
                with conn:
                    conn.execute("BEGIN IMMEDIATE")
                    now = self.clock.now()
                    rows = conn.execute(
                        "SELECT id,document FROM message_outbox WHERE lease_until<=? ORDER BY rowid LIMIT ?",
                        (now, limit),
                    ).fetchall()
                    for message_id, _ in rows:
                        conn.execute(
                            "UPDATE message_outbox SET owner=?,lease_until=? WHERE id=?", (owner, now + 30, message_id)
                        )
                    return [Publication.restore(json.loads(row[1])) for row in rows]
            finally:
                conn.close()

        return await asyncio.to_thread(execute)

    async def list_states(self, prefix: str, *, after: str = "", limit: int = 100) -> list[dict[str, Any]]:
        def execute() -> list[dict[str, Any]]:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT document FROM message_states WHERE substr(key,1,?)=? AND key>? ORDER BY key LIMIT ?",
                    (len(prefix), prefix, after, limit),
                ).fetchall()
                return [json.loads(row[0]) for row in rows]
            finally:
                conn.close()

        return await asyncio.to_thread(execute)

    async def _finish(self, message_id: str, owner: str, *, delivered: bool) -> None:
        def execute() -> None:
            conn = self._connect()
            try:
                with conn:
                    if delivered:
                        conn.execute("DELETE FROM message_outbox WHERE id=? AND owner=?", (message_id, owner))
                    else:
                        conn.execute(
                            "UPDATE message_outbox SET owner=NULL,lease_until=? WHERE id=? AND owner=?",
                            (self.clock.now() + 1, message_id, owner),
                        )
            finally:
                conn.close()

        await asyncio.to_thread(execute)

    async def delivered(self, message_id: str, owner: str) -> None:
        await self._finish(message_id, owner, delivered=True)

    async def release(self, message_id: str, owner: str) -> None:
        await self._finish(message_id, owner, delivered=False)

    async def ping(self) -> bool:
        await self.read("health")
        return True

    async def close(self) -> None:
        pass
