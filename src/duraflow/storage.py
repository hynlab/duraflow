"""Transactional run aggregates with history, leases, inbox and outbox in one CAS.

The alpha schema deliberately uses a per-run JSON aggregate. No database
transaction spans user code or a broker call. See docs/architecture.md.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

from .contracts import Conflict, NotFound, canonical

State = dict[str, Any]


def clone(value: Any) -> Any:
    return json.loads(canonical(value))


class Store(Protocol):
    async def create(self, state: State, request_id: str, digest: str) -> State: ...
    async def load(self, namespace: str, run_id: str) -> State: ...
    async def save(self, state: State, revision: int) -> bool: ...
    async def scan(self, namespace: str, after: str = "", limit: int = 100) -> list[State]: ...
    async def head(self, namespace: str, workflow_id: str) -> str: ...
    async def rollover(self, old: State, revision: int, new: State) -> bool: ...
    async def close(self) -> None: ...


class MemoryStore:
    def __init__(self) -> None:
        self.runs: dict[tuple[str, str], State] = {}
        self.requests: dict[tuple[str, str], tuple[str, str]] = {}
        self.heads: dict[tuple[str, str], str] = {}
        self.lock = asyncio.Lock()

    async def create(self, state: State, request_id: str, digest: str) -> State:
        async with self.lock:
            ns = state["namespace"]
            if (ns, request_id) in self.requests:
                run_id, previous = self.requests[ns, request_id]
                if previous != digest:
                    raise Conflict("Idempotency key reused with a different request")
                return clone(self.runs[ns, run_id])
            if (ns, state["workflow_id"]) in self.heads:
                raise Conflict("Workflow identity already exists")
            self.runs[ns, state["run_id"]] = clone(state)
            self.requests[ns, request_id] = (state["run_id"], digest)
            self.heads[ns, state["workflow_id"]] = state["run_id"]
            return clone(state)

    async def load(self, namespace: str, run_id: str) -> State:
        async with self.lock:
            try:
                return clone(self.runs[namespace, run_id])
            except KeyError:
                raise NotFound(run_id) from None

    async def save(self, state: State, revision: int) -> bool:
        async with self.lock:
            key = state["namespace"], state["run_id"]
            previous = self.runs.get(key)
            if previous is None or previous["revision"] != revision:
                return False
            self.runs[key] = clone({**state, "revision": revision + 1})
            return True

    async def scan(self, namespace: str, after: str = "", limit: int = 100) -> list[State]:
        async with self.lock:
            keys = [(ns, run_id) for ns, run_id in sorted(self.runs) if ns == namespace and run_id > after]
            return [clone(self.runs[key]) for key in keys[:limit]]

    async def head(self, namespace: str, workflow_id: str) -> str:
        async with self.lock:
            try:
                return self.heads[namespace, workflow_id]
            except KeyError:
                raise NotFound(workflow_id) from None

    async def rollover(self, old: State, revision: int, new: State) -> bool:
        async with self.lock:
            key = old["namespace"], old["run_id"]
            if self.runs[key]["revision"] != revision:
                return False
            self.runs[key] = clone({**old, "revision": revision + 1})
            self.runs[new["namespace"], new["run_id"]] = clone(new)
            self.heads[new["namespace"], new["workflow_id"]] = new["run_id"]
            return True

    async def close(self) -> None:
        pass


class SQLiteStore:
    """Development and fresh-process recovery-test store, not a PostgreSQL substitute."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("Use MemoryStore or a file-backed SQLite database")
        with self._connect() as conn:
            conn.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS schema_version(version INTEGER PRIMARY KEY);
                INSERT OR IGNORE INTO schema_version VALUES(1);
                CREATE TABLE IF NOT EXISTS runs (
                  namespace TEXT, run_id TEXT, revision INTEGER NOT NULL, document TEXT NOT NULL,
                  PRIMARY KEY(namespace, run_id));
                CREATE TABLE IF NOT EXISTS requests (
                  namespace TEXT, request_id TEXT, run_id TEXT NOT NULL, digest TEXT NOT NULL,
                  PRIMARY KEY(namespace, request_id));
                CREATE TABLE IF NOT EXISTS heads (
                  namespace TEXT, workflow_id TEXT, run_id TEXT NOT NULL,
                  PRIMARY KEY(namespace, workflow_id));
            """)
            if conn.execute("SELECT version FROM schema_version").fetchall() != [(1,)]:
                raise Conflict("Unsupported SQLite schema version")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=10)
        try:
            conn.execute("PRAGMA synchronous=FULL")
            with conn:
                yield conn
        finally:
            conn.close()

    async def create(self, state: State, request_id: str, digest: str) -> State:
        def execute() -> State:
            ns, run_id = state["namespace"], state["run_id"]
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute("SELECT run_id,digest FROM requests WHERE namespace=? AND request_id=?",
                                   (ns, request_id)).fetchone()
                if row:
                    if row[1] != digest:
                        raise Conflict("Idempotency key reused with a different request")
                    return json.loads(conn.execute("SELECT document FROM runs WHERE namespace=? AND run_id=?",
                                                   (ns, row[0])).fetchone()[0])
                try:
                    conn.execute("INSERT INTO heads VALUES(?,?,?)", (ns, state["workflow_id"], run_id))
                    conn.execute("INSERT INTO runs VALUES(?,?,?,?)", (ns, run_id, 0, canonical(state)))
                    conn.execute("INSERT INTO requests VALUES(?,?,?,?)", (ns, request_id, run_id, digest))
                except sqlite3.IntegrityError as exc:
                    raise Conflict("Workflow identity already exists") from exc
            return clone(state)
        return await asyncio.to_thread(execute)

    async def load(self, namespace: str, run_id: str) -> State:
        def execute() -> State:
            with self._connect() as conn:
                row = conn.execute("SELECT document FROM runs WHERE namespace=? AND run_id=?",
                                   (namespace, run_id)).fetchone()
                if row is None:
                    raise NotFound(run_id)
                return json.loads(row[0])
        return await asyncio.to_thread(execute)

    async def save(self, state: State, revision: int) -> bool:
        def execute() -> bool:
            with self._connect() as conn:
                return conn.execute(
                    "UPDATE runs SET revision=?,document=? WHERE namespace=? AND run_id=? AND revision=?",
                    (revision + 1, canonical({**state, "revision": revision + 1}),
                     state["namespace"], state["run_id"], revision)).rowcount == 1
        return await asyncio.to_thread(execute)

    async def scan(self, namespace: str, after: str = "", limit: int = 100) -> list[State]:
        def execute() -> list[State]:
            with self._connect() as conn:
                return [json.loads(row[0]) for row in conn.execute(
                    "SELECT document FROM runs WHERE namespace=? AND run_id>? ORDER BY run_id LIMIT ?",
                    (namespace, after, limit))]
        return await asyncio.to_thread(execute)

    async def head(self, namespace: str, workflow_id: str) -> str:
        def execute() -> str:
            with self._connect() as conn:
                row = conn.execute("SELECT run_id FROM heads WHERE namespace=? AND workflow_id=?",
                                   (namespace, workflow_id)).fetchone()
                if row is None:
                    raise NotFound(workflow_id)
                return str(row[0])
        return await asyncio.to_thread(execute)

    async def rollover(self, old: State, revision: int, new: State) -> bool:
        def execute() -> bool:
            with self._connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                changed = conn.execute(
                    "UPDATE runs SET revision=?,document=? WHERE namespace=? AND run_id=? AND revision=?",
                    (revision + 1, canonical({**old, "revision": revision + 1}),
                     old["namespace"], old["run_id"], revision)).rowcount
                if changed != 1:
                    return False
                conn.execute("INSERT INTO runs VALUES(?,?,?,?)",
                             (new["namespace"], new["run_id"], 0, canonical(new)))
                conn.execute("UPDATE heads SET run_id=? WHERE namespace=? AND workflow_id=?",
                             (new["run_id"], new["namespace"], new["workflow_id"]))
                return True
        return await asyncio.to_thread(execute)

    async def close(self) -> None:
        pass
