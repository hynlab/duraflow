"""Optional PostgreSQL adapter using SQLAlchemy 2 Core and psycopg 3 async I/O."""

from __future__ import annotations

from .contracts import Conflict, NotFound, name
from .storage import State, clone


class PostgresStore:
    def __init__(self, url: str, *, schema: str = "duraflow", pool_size: int = 5):
        name(schema)
        if not schema.replace("_", "").isalnum() or not schema.isascii():
            raise ValueError("Schema must be an ASCII SQL identifier")
        if not url.startswith("postgresql+psycopg://"):
            raise ValueError("Use postgresql+psycopg:// for the async psycopg adapter")
        if pool_size < 1:
            raise ValueError("pool_size must be positive")
        from sqlalchemy import Column, Integer, MetaData, String, Table
        from sqlalchemy.dialects.postgresql import JSONB
        from sqlalchemy.ext.asyncio import create_async_engine

        self.engine = create_async_engine(url, pool_size=pool_size, max_overflow=0, pool_pre_ping=True)
        self.schema = schema
        self.metadata = MetaData(schema=schema)
        self.runs = Table(
            "runs",
            self.metadata,
            Column("namespace", String(128), primary_key=True),
            Column("run_id", String(64), primary_key=True),
            Column("revision", Integer, nullable=False),
            Column("document", JSONB, nullable=False),
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

    async def initialize(self) -> None:
        from sqlalchemy import insert, select, text

        async with self.engine.begin() as conn:
            await conn.execute(text("SELECT pg_advisory_xact_lock(740613271)"))
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"'))
            await conn.run_sync(self.metadata.create_all)
            versions = (await conn.execute(select(self.versions.c.version))).scalars().all()
            if not versions:
                await conn.execute(insert(self.versions).values(version=1))
            elif versions != [1]:
                raise Conflict("Unsupported PostgreSQL schema version; explicit migration required")

    async def create(self, state: State, request_id: str, digest: str) -> State:
        from sqlalchemy import insert, select
        from sqlalchemy.exc import IntegrityError

        ns = state["namespace"]
        try:
            async with self.engine.begin() as conn:
                await conn.execute(
                    insert(self.requests).values(
                        namespace=ns, request_id=request_id, run_id=state["run_id"], digest=digest
                    )
                )
                await conn.execute(
                    insert(self.heads).values(namespace=ns, workflow_id=state["workflow_id"], run_id=state["run_id"])
                )
                await conn.execute(
                    insert(self.runs).values(namespace=ns, run_id=state["run_id"], revision=0, document=clone(state))
                )
            return clone(state)
        except IntegrityError as exc:
            async with self.engine.connect() as conn:
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

        async with self.engine.connect() as conn:
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

        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(self.runs)
                .where(
                    self.runs.c.namespace == state["namespace"],
                    self.runs.c.run_id == state["run_id"],
                    self.runs.c.revision == revision,
                )
                .values(revision=revision + 1, document=clone({**state, "revision": revision + 1}))
            )
            return result.rowcount == 1

    async def scan(self, namespace: str, after: str = "", limit: int = 100) -> list[State]:
        from sqlalchemy import select

        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(self.runs.c.document)
                .where(self.runs.c.namespace == namespace, self.runs.c.run_id > after)
                .order_by(self.runs.c.run_id)
                .limit(limit)
            )
            return [clone(row[0]) for row in rows]

    async def head(self, namespace: str, workflow_id: str) -> str:
        from sqlalchemy import select

        async with self.engine.connect() as conn:
            result = await conn.execute(
                select(self.heads.c.run_id).where(
                    self.heads.c.namespace == namespace, self.heads.c.workflow_id == workflow_id
                )
            )
            value = result.scalar_one_or_none()
            if value is None:
                raise NotFound(workflow_id)
            return str(value)

    async def rollover(self, old: State, revision: int, new: State) -> bool:
        from sqlalchemy import insert, update

        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(self.runs)
                .where(
                    self.runs.c.namespace == old["namespace"],
                    self.runs.c.run_id == old["run_id"],
                    self.runs.c.revision == revision,
                )
                .values(revision=revision + 1, document=clone({**old, "revision": revision + 1}))
            )
            if result.rowcount != 1:
                return False
            await conn.execute(
                insert(self.runs).values(
                    namespace=new["namespace"], run_id=new["run_id"], revision=0, document=clone(new)
                )
            )
            await conn.execute(
                update(self.heads)
                .where(self.heads.c.namespace == new["namespace"], self.heads.c.workflow_id == new["workflow_id"])
                .values(run_id=new["run_id"])
            )
        return True

    async def close(self) -> None:
        await self.engine.dispose()
