"""Versioned PostgreSQL storage with atomic authoritative-time mutations.

The schema-v1 to v2 migration requires pausing old runtime writers. It preserves
run documents and adds rebuildable scheduling projections; it is not an online
mixed-runtime migration. Application workflow builds may coexist on runtime v2.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable

from .contracts import Conflict, NotFound, TERMINAL, _STORE_TIME, duration, fingerprint, name
from .projection import next_due
from .storage import State, clone

SCHEMA_VERSION = 2


class PostgresStore:
    def __init__(
        self,
        url: str,
        *,
        schema: str = "duraflow",
        pool_size: int = 5,
        operation_timeout: float = 15.0,
        lock_timeout: float = 2.0,
        statement_timeout: float = 5.0,
    ):
        name(schema)
        if not schema.replace("_", "").isalnum() or not schema.isascii() or len(schema) > 63:
            raise ValueError("Schema must be an ASCII SQL identifier of at most 63 characters")
        if not url.startswith("postgresql+psycopg://"):
            raise ValueError("Use postgresql+psycopg:// for the async psycopg adapter")
        if type(pool_size) is not int or pool_size < 1:
            raise ValueError("pool_size must be positive")
        for value in (operation_timeout, lock_timeout, statement_timeout):
            duration(value)
        if not lock_timeout < statement_timeout < operation_timeout:
            raise ValueError("Require lock_timeout < statement_timeout < operation_timeout")
        from sqlalchemy import Column, Float, Integer, MetaData, String, Table
        from sqlalchemy.dialects.postgresql import JSONB
        from sqlalchemy.ext.asyncio import create_async_engine

        self.engine = create_async_engine(
            url,
            pool_size=pool_size,
            max_overflow=0,
            pool_pre_ping=True,
            pool_timeout=operation_timeout,
            connect_args={
                "connect_timeout": max(1, math.ceil(statement_timeout)),
                "options": f"-c statement_timeout={int(statement_timeout * 1000)} "
                f"-c lock_timeout={int(lock_timeout * 1000)}",
            },
        )
        self.schema, self.operation_timeout = schema, operation_timeout
        self.metadata = MetaData(schema=schema)
        self.runs = Table(
            "runs",
            self.metadata,
            Column("namespace", String(128), primary_key=True),
            Column("run_id", String(64), primary_key=True),
            Column("revision", Integer, nullable=False),
            Column("document", JSONB, nullable=False),
            Column("next_due", Float, nullable=True),
            Column("status", String(16), nullable=False),
            Column("implementation", String(64), nullable=False),
        )
        self.requests = Table(
            "requests",
            self.metadata,
            Column("namespace", String(128), primary_key=True),
            Column("request_id", String(256), primary_key=True),
            Column("run_id", String(64), nullable=False),
            Column("digest", String(64), nullable=False),
        )
        self.heads = Table(
            "heads",
            self.metadata,
            Column("namespace", String(128), primary_key=True),
            Column("workflow_id", String(256), primary_key=True),
            Column("run_id", String(64), nullable=False),
        )
        self.versions = Table("schema_version", self.metadata, Column("version", Integer, primary_key=True))

    @asynccontextmanager
    async def _transaction(self, *, migration: bool = False) -> AsyncIterator[Any]:
        async with asyncio.timeout(120 if migration else self.operation_timeout):
            async with self.engine.begin() as conn:
                if migration:
                    from sqlalchemy import text

                    await conn.execute(text("SET LOCAL statement_timeout = '120s'"))
                yield conn

    @staticmethod
    async def _now(conn: Any) -> float:
        from sqlalchemy import text

        return float((await conn.execute(text("SELECT EXTRACT(EPOCH FROM clock_timestamp())"))).scalar_one())

    async def now(self) -> float:
        async with self._transaction() as conn:
            return await self._now(conn)

    def _values(self, state: State, revision: int, now: float) -> dict[str, Any]:
        document = clone({**state, "revision": revision})
        return {
            "revision": revision,
            "document": document,
            "next_due": next_due(document, now, reconcile_interval=document.get("reconcile_interval", 10.0)),
            "status": document["status"],
            "implementation": fingerprint(document["manifest"]),
        }

    @staticmethod
    def _stamp_start(state: State, now: float) -> State:
        document = clone(state)
        document["created_at"] = now
        for entry in document["history"]:
            if entry["kind"] == "started":
                entry["time"] = now
        for item in document["outbox"].values():
            item["created_at"] = now
        return document

    async def initialize(self) -> None:
        await self.migrate()

    async def migrate(self, *, target: int = SCHEMA_VERSION) -> None:
        from sqlalchemy import delete, insert, select, text, update

        if type(target) is not int or not 1 <= target <= SCHEMA_VERSION:
            raise ValueError("Unsupported migration target")
        q = f'"{self.schema}"'
        lock_key = int.from_bytes(hashlib.sha256(self.schema.encode()).digest()[:8], "big", signed=True)
        async with self._transaction(migration=True) as conn:
            await conn.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_key})
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {q}"))
            await conn.execute(text(f"CREATE TABLE IF NOT EXISTS {q}.schema_version (version INTEGER PRIMARY KEY)"))
            versions = (await conn.execute(select(self.versions.c.version))).scalars().all()
            if len(versions) > 1:
                raise Conflict("Invalid schema version ledger")
            current = versions[0] if versions else 0
            if current > target or current < 0:
                raise Conflict("Downgrades/unknown schema versions are not supported")
            if current == 0:
                existing = (await conn.execute(text("SELECT to_regclass(:table)"), {"table": f"{q}.runs"})).scalar()
                if existing is not None:
                    raise Conflict("Existing tables without a schema version require operator review")
                statements = (
                    f"CREATE TABLE {q}.runs (namespace VARCHAR(128) NOT NULL, run_id VARCHAR(64) NOT NULL, "
                    "revision INTEGER NOT NULL, document JSONB NOT NULL, PRIMARY KEY(namespace, run_id))",
                    f"CREATE TABLE {q}.requests (namespace VARCHAR(128) NOT NULL, request_id VARCHAR(256) NOT NULL, "
                    "run_id VARCHAR(64) NOT NULL, digest VARCHAR(64) NOT NULL, PRIMARY KEY(namespace, request_id))",
                    f"CREATE TABLE {q}.heads (namespace VARCHAR(128) NOT NULL, workflow_id VARCHAR(256) NOT NULL, "
                    "run_id VARCHAR(64) NOT NULL, PRIMARY KEY(namespace, workflow_id))",
                )
                for statement in statements:
                    await conn.execute(text(statement))
                current = 1
            if current < 2 <= target:
                await conn.execute(
                    text(
                        f"ALTER TABLE {q}.runs ADD COLUMN next_due DOUBLE PRECISION, "
                        "ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'PENDING', "
                        "ADD COLUMN implementation VARCHAR(64) NOT NULL DEFAULT ''"
                    )
                )
                now = await self._now(conn)
                rows = await conn.stream(select(self.runs.c.namespace, self.runs.c.run_id, self.runs.c.document))
                async for row in rows:
                    state = row.document
                    await conn.execute(
                        update(self.runs)
                        .where(self.runs.c.namespace == row.namespace, self.runs.c.run_id == row.run_id)
                        .values(
                            next_due=next_due(state, now),
                            status=state["status"],
                            implementation=fingerprint(state["manifest"]),
                        )
                    )
                await conn.execute(
                    text(
                        f"CREATE INDEX runs_due_idx ON {q}.runs (namespace, next_due, run_id) "
                        "WHERE next_due IS NOT NULL"
                    )
                )
                current = 2
            await conn.execute(delete(self.versions))
            await conn.execute(insert(self.versions).values(version=current))

    async def schema_version(self) -> int:
        from sqlalchemy import select

        async with self._transaction() as conn:
            versions = (await conn.execute(select(self.versions.c.version))).scalars().all()
            if len(versions) != 1:
                raise Conflict("Invalid schema version ledger")
            return int(versions[0])

    async def create(self, state: State, request_id: str, digest: str) -> State:
        from sqlalchemy import insert, select
        from sqlalchemy.exc import IntegrityError

        ns = state["namespace"]
        try:
            async with self._transaction() as conn:
                state = self._stamp_start(state, await self._now(conn))
                await conn.execute(
                    insert(self.requests).values(
                        namespace=ns, request_id=request_id, run_id=state["run_id"], digest=digest
                    )
                )
                await conn.execute(
                    insert(self.heads).values(namespace=ns, workflow_id=state["workflow_id"], run_id=state["run_id"])
                )
                await conn.execute(
                    insert(self.runs).values(
                        namespace=ns, run_id=state["run_id"], **self._values(state, 0, state["created_at"])
                    )
                )
            return clone(state)
        except IntegrityError as exc:
            async with self._transaction() as conn:
                row = (
                    (
                        await conn.execute(
                            select(self.requests).where(
                                self.requests.c.namespace == ns, self.requests.c.request_id == request_id
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
            if row is not None and row["digest"] == digest:
                return await self.load(ns, row["run_id"])
            raise Conflict("Idempotency key or workflow identity conflicts") from exc

    async def load(self, namespace: str, run_id: str) -> State:
        from sqlalchemy import select

        async with self._transaction() as conn:
            row = (
                await conn.execute(
                    select(self.runs.c.document).where(self.runs.c.namespace == namespace, self.runs.c.run_id == run_id)
                )
            ).first()
        if row is None:
            raise NotFound(run_id)
        return clone(row[0])

    async def save(self, state: State, revision: int) -> bool:
        from sqlalchemy import update

        async with self._transaction() as conn:
            now = await self._now(conn)
            result = await conn.execute(
                update(self.runs)
                .where(
                    self.runs.c.namespace == state["namespace"],
                    self.runs.c.run_id == state["run_id"],
                    self.runs.c.revision == revision,
                )
                .values(**self._values(state, revision + 1, now))
            )
            return result.rowcount == 1

    async def mutate_atomic(self, namespace: str, run_id: str, change: Callable[[State], Any]) -> Any:
        """Run an internal non-I/O mutation after the row lock, using server time."""
        from sqlalchemy import select, update

        async with self._transaction() as conn:
            row = (
                await conn.execute(
                    select(self.runs.c.document, self.runs.c.revision)
                    .where(self.runs.c.namespace == namespace, self.runs.c.run_id == run_id)
                    .with_for_update()
                )
            ).first()
            if row is None:
                raise NotFound(run_id)
            now = await self._now(conn)
            state = clone(row.document)
            before = fingerprint(state)
            token = _STORE_TIME.set(now)
            try:
                result = change(state)
            finally:
                _STORE_TIME.reset(token)
            if fingerprint(state) != before:
                await conn.execute(
                    update(self.runs)
                    .where(self.runs.c.namespace == namespace, self.runs.c.run_id == run_id)
                    .values(**self._values(state, row.revision + 1, now))
                )
            return result

    async def scan(self, namespace: str, after: str = "", limit: int = 100) -> list[State]:
        from sqlalchemy import select

        async with self._transaction() as conn:
            rows = await conn.execute(
                select(self.runs.c.document)
                .where(self.runs.c.namespace == namespace, self.runs.c.run_id > after)
                .order_by(self.runs.c.run_id)
                .limit(limit)
            )
            return [clone(row[0]) for row in rows]

    async def scan_due(
        self, namespace: str, after: str = "", limit: int = 100, *, manifests: tuple[dict[str, Any], ...] = ()
    ) -> list[State]:
        from sqlalchemy import or_, select

        async with self._transaction() as conn:
            now = await self._now(conn)
            rows = await conn.execute(
                select(self.runs.c.run_id)
                .where(
                    self.runs.c.namespace == namespace,
                    self.runs.c.run_id > after,
                    self.runs.c.next_due <= now,
                    or_(
                        self.runs.c.status.in_(TERMINAL | {"CANCELLING", "BLOCKED"}),
                        self.runs.c.implementation.in_([fingerprint(m) for m in manifests]),
                    ),
                )
                .order_by(self.runs.c.run_id)
                .limit(limit)
            )
            return [{"run_id": row[0]} for row in rows]

    async def refresh_projection(self, namespace: str, run_id: str) -> None:
        from sqlalchemy import select, update

        async with self._transaction() as conn:
            row = (
                await conn.execute(
                    select(self.runs.c.document, self.runs.c.revision).where(
                        self.runs.c.namespace == namespace, self.runs.c.run_id == run_id
                    )
                )
            ).first()
            if row is None:
                return
            due = next_due(
                row.document, await self._now(conn), reconcile_interval=row.document.get("reconcile_interval", 10.0)
            )
            await conn.execute(
                update(self.runs)
                .where(
                    self.runs.c.namespace == namespace,
                    self.runs.c.run_id == run_id,
                    self.runs.c.revision == row.revision,
                )
                .values(next_due=due)
            )

    async def head(self, namespace: str, workflow_id: str) -> str:
        from sqlalchemy import select

        async with self._transaction() as conn:
            value = (
                await conn.execute(
                    select(self.heads.c.run_id).where(
                        self.heads.c.namespace == namespace, self.heads.c.workflow_id == workflow_id
                    )
                )
            ).scalar_one_or_none()
            if value is None:
                raise NotFound(workflow_id)
            return str(value)

    async def rollover(self, old: State, revision: int, new: State) -> bool:
        from sqlalchemy import insert, update

        async with self._transaction() as conn:
            now = await self._now(conn)
            result = await conn.execute(
                update(self.runs)
                .where(
                    self.runs.c.namespace == old["namespace"],
                    self.runs.c.run_id == old["run_id"],
                    self.runs.c.revision == revision,
                )
                .values(**self._values(old, revision + 1, now))
            )
            if result.rowcount != 1:
                return False
            new = self._stamp_start(new, now)
            await conn.execute(
                insert(self.runs).values(namespace=new["namespace"], run_id=new["run_id"], **self._values(new, 0, now))
            )
            result = await conn.execute(
                update(self.heads)
                .where(
                    self.heads.c.namespace == new["namespace"],
                    self.heads.c.workflow_id == new["workflow_id"],
                    self.heads.c.run_id == old["run_id"],
                )
                .values(run_id=new["run_id"])
            )
            if result.rowcount != 1:
                raise Conflict("Logical workflow head changed during rollover")
        return True

    async def close(self) -> None:
        await self.engine.dispose()
