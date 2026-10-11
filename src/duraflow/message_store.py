"""Atomic message consumption, state and outgoing intent; no broker I/O in locks."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any, Protocol

from .contracts import Clock, Conflict, MessageIntegrityError, canonical, fingerprint
from .messaging import Message, Publication
from .storage import clone
from .message_records import LocalUpdate, Records, Update, apply_update
from .message_telemetry import head_status, sample


class MessageStore(Protocol):
    async def apply(self, key: str, message: Message, update: Update) -> bool: ...
    async def read(self, key: str) -> dict[str, Any]: ...
    async def list_states(self, prefix: str, *, after: str = "", limit: int = 100) -> list[dict[str, Any]]: ...
    async def claim(self, owner: str, limit: int = 32) -> list[Publication]: ...
    async def delivered(self, message_id: str, owner: str) -> None: ...
    async def release(self, message_id: str, owner: str) -> None: ...
    async def ping(self) -> bool: ...
    async def close(self) -> None: ...
    async def collect_garbage(self, *, limit: int = 100, dry_run: bool = True) -> dict[str, int]: ...
    async def telemetry(self, namespace: str) -> dict[str, Any]: ...


class MemoryMessageStore:
    def __init__(self, *, clock: Clock | None = None):
        self.clock = clock or Clock()
        self.documents: dict[str, dict[str, Any]] = {}
        self.inbox: dict[tuple[str, str], str] = {}
        self.inbox_expiry: dict[tuple[str, str], float] = {}
        self.outbox: dict[str, dict[str, Any]] = {}
        self.records: dict[tuple[str, str], tuple[dict[str, Any], float | None]] = {}
        self.lock = asyncio.Lock()
        self.heads: dict[str, tuple[str | None, str | None]] = {}

    async def apply(self, key: str, message: Message, update: Update) -> bool:
        async with self.lock:
            message.admit(self.clock.now())
            if isinstance(update, LocalUpdate) and not update.create and key not in self.documents:
                return False
            digest = fingerprint(json.loads(message.to_bytes()))
            previous = self.inbox.get((key, message.id)) if not isinstance(update, LocalUpdate) else None
            if previous is not None:
                if previous != digest:
                    raise MessageIntegrityError("Message ID reused with different content")
                return False
            state = clone(self.documents.get(key, {}))
            records = Records(lambda name: self.records.get((key, name), (None, None))[0])
            outgoing = apply_update(update, state, self.clock.now(), records)
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
            self.heads[key] = head_status(state)
            for name, value in records.writes.items():
                self.records[key, name] = clone(value[0]), value[1]
            if not isinstance(update, LocalUpdate):
                self.inbox[key, message.id] = digest
                expires_at = message.expires_at()
                if expires_at is not None:
                    self.inbox_expiry[key, message.id] = expires_at
            for message_id, document in pending.items():
                self.outbox.setdefault(
                    message_id,
                    {"publication": document, "owner": None, "until": 0, "key": key, "created_at": self.clock.now()},
                )
            return True

    async def telemetry(self, namespace: str) -> dict[str, Any]:
        async with self.lock:
            counts: dict[str, int] = {}
            for ns, status in self.heads.values():
                if ns == namespace and status is not None:
                    counts[status] = counts.get(status, 0) + 1
            oldest = min((item["created_at"] for item in self.outbox.values()), default=None)
            return sample(self.clock.now(), counts, oldest)

    async def collect_garbage(self, *, limit: int = 100, dry_run: bool = True) -> dict[str, int]:
        if not 1 <= limit <= 10000:
            raise ValueError("Invalid GC batch size")
        async with self.lock:
            now = self.clock.now()
            pending = {item.get("key") for item in self.outbox.values()}
            records = [
                k
                for k, (_, expiry) in self.records.items()
                if expiry is not None and expiry <= now and k[0] not in pending
            ][:limit]
            inbox = [k for k, expiry in self.inbox_expiry.items() if expiry <= now and k[0] not in pending][:limit]
            remaining_children = {k[0] for k in self.inbox if dry_run or k not in inbox} | {
                k[0] for k in self.records if dry_run or k not in records
            }
            states = [
                k
                for k, state in self.documents.items()
                if state.get("_gc_after", float("inf")) <= now and k not in pending and k not in remaining_children
            ][:limit]
            if not dry_run:
                for key in records:
                    del self.records[key]
                for key in inbox:
                    self.inbox.pop(key, None)
                    del self.inbox_expiry[key]
                for state_key in states:
                    del self.documents[state_key]
                    self.heads.pop(state_key, None)
            return {"records": len(records), "inbox": len(inbox), "states": len(states)}

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
                INSERT INTO message_schema_version SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM message_schema_version);
                CREATE TABLE IF NOT EXISTS message_inbox(key TEXT, id TEXT, digest TEXT NOT NULL, PRIMARY KEY(key,id));
                CREATE TABLE IF NOT EXISTS message_outbox(id TEXT PRIMARY KEY, document TEXT NOT NULL,
                    owner TEXT, lease_until REAL NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS message_outbox_due ON message_outbox(lease_until);
                CREATE TABLE IF NOT EXISTS message_records(
                    key TEXT NOT NULL, name TEXT NOT NULL, document TEXT NOT NULL, expires_at REAL,
                    PRIMARY KEY(key,name));
                CREATE INDEX IF NOT EXISTS message_records_expiry ON message_records(expires_at);
            """)
            if conn.execute("SELECT version FROM message_schema_version").fetchall() != [(1,)]:
                if conn.execute("SELECT version FROM message_schema_version").fetchall() not in ([(2,)], [(3,)]):
                    raise Conflict("Unsupported message-store schema version")
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                if conn.execute("SELECT version FROM message_schema_version").fetchall() == [(1,)]:
                    conn.execute("ALTER TABLE message_states ADD COLUMN expires_at REAL")
                    conn.execute("ALTER TABLE message_inbox ADD COLUMN expires_at REAL")
                    conn.execute("ALTER TABLE message_outbox ADD COLUMN state_key TEXT")
                    conn.execute("UPDATE message_schema_version SET version=2")
                if conn.execute("SELECT version FROM message_schema_version").fetchall() == [(2,)]:
                    conn.execute("ALTER TABLE message_states ADD COLUMN namespace TEXT")
                    conn.execute("ALTER TABLE message_states ADD COLUMN status TEXT")
                    conn.execute("ALTER TABLE message_outbox ADD COLUMN created_at REAL")
                    conn.execute("UPDATE message_outbox SET created_at=?", (self.clock.now(),))
                    for key, document in conn.execute("SELECT key,document FROM message_states").fetchall():
                        conn.execute(
                            "UPDATE message_states SET namespace=?,status=? WHERE key=?",
                            (*head_status(json.loads(document)), key),
                        )
                    conn.execute("UPDATE message_schema_version SET version=3")
                conn.execute("CREATE INDEX IF NOT EXISTS message_states_status ON message_states(namespace,status)")
                conn.execute("CREATE INDEX IF NOT EXISTS message_outbox_age ON message_outbox(created_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS message_states_expiry ON message_states(expires_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS message_inbox_expiry ON message_inbox(expires_at)")
                conn.execute("CREATE INDEX IF NOT EXISTS message_outbox_state ON message_outbox(state_key)")

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
                    message.admit(self.clock.now())
                    digest = fingerprint(json.loads(message.to_bytes()))
                    row = conn.execute(
                        "SELECT digest FROM message_inbox WHERE key=? AND id=?", (key, message.id)
                    ).fetchone()
                    if row and not isinstance(update, LocalUpdate):
                        if row[0] != digest:
                            raise MessageIntegrityError("Message ID reused with different content")
                        return False
                    row = conn.execute("SELECT document FROM message_states WHERE key=?", (key,)).fetchone()
                    if row is None and isinstance(update, LocalUpdate) and not update.create:
                        return False
                    state = json.loads(row[0]) if row else {}

                    def load(name: str) -> dict[str, Any] | None:
                        found = conn.execute(
                            "SELECT document FROM message_records WHERE key=? AND name=?", (key, name)
                        ).fetchone()
                        return json.loads(found[0]) if found else None

                    records = Records(load)
                    outgoing = apply_update(update, state, self.clock.now(), records)
                    for name, (document, expires_at) in records.writes.items():
                        conn.execute(
                            "INSERT OR REPLACE INTO message_records VALUES(?,?,?,?)",
                            (key, name, canonical(document), expires_at),
                        )
                    conn.execute(
                        "INSERT OR REPLACE INTO message_states VALUES(?,?,?,?,?)",
                        (key, canonical(state), state.get("_gc_after"), *head_status(state)),
                    )
                    if not isinstance(update, LocalUpdate):
                        conn.execute(
                            "INSERT INTO message_inbox VALUES(?,?,?,?)", (key, message.id, digest, message.expires_at())
                        )
                    for item in outgoing:
                        item.message.to_bytes()
                        inserted = conn.execute(
                            "INSERT INTO message_outbox(id,document,state_key,created_at) VALUES(?,?,?,?) "
                            "ON CONFLICT(id) DO UPDATE SET document=excluded.document "
                            "WHERE message_outbox.document=excluded.document RETURNING id",
                            (item.message.id, canonical(item.document()), key, self.clock.now()),
                        ).fetchone()
                        if inserted is None:
                            raise Conflict("Outgoing identity conflict")
                    return True
            finally:
                conn.close()

        return await asyncio.to_thread(execute)

    async def collect_garbage(self, *, limit: int = 100, dry_run: bool = True) -> dict[str, int]:
        if not 1 <= limit <= 10000:
            raise ValueError("Invalid GC batch size")

        def execute() -> dict[str, int]:
            with closing(self._connect()) as conn, conn:
                conn.execute("BEGIN IMMEDIATE")
                result = {}
                for name in ("records", "inbox", "states"):
                    table = "message_" + name
                    children = (
                        (
                            " AND NOT EXISTS(SELECT 1 FROM message_inbox WHERE key=message_states.key)"
                            " AND NOT EXISTS(SELECT 1 FROM message_records WHERE key=message_states.key)"
                        )
                        if name == "states"
                        else ""
                    )
                    rows = conn.execute(
                        f"SELECT rowid FROM {table} WHERE expires_at<=? AND NOT EXISTS "
                        f"(SELECT 1 FROM message_outbox WHERE state_key={table}.key OR state_key IS NULL) "
                        f"{children} ORDER BY expires_at LIMIT ?",
                        (self.clock.now(), limit),
                    ).fetchall()
                    result[name] = len(rows)
                    if not dry_run:
                        conn.executemany(f"DELETE FROM {table} WHERE rowid=?", rows)
                return result

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

    async def telemetry(self, namespace: str) -> dict[str, Any]:
        def execute() -> dict[str, Any]:
            with closing(self._connect()) as conn:
                conn.execute("BEGIN")
                counts = dict(
                    conn.execute(
                        "SELECT status,COUNT(*) FROM message_states WHERE namespace=? GROUP BY status", (namespace,)
                    ).fetchall()
                )
                oldest = conn.execute("SELECT MIN(created_at) FROM message_outbox").fetchone()[0]
                return sample(self.clock.now(), counts, oldest)

        return await asyncio.to_thread(execute)

    async def close(self) -> None:
        pass
