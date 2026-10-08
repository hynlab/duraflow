"""PostgreSQL message journal with per-instance locks and recoverable outbox."""

from __future__ import annotations

import json
from typing import Any

from .contracts import Conflict, fingerprint
from .message_store import Update
from .messaging import Message, Publication
from .postgres import PostgresStore


class PostgresMessageStore:
    def __init__(self, url: str, *, schema: str = "duraflow_messages", **options: Any):
        self.database = PostgresStore(url, schema=schema, **options)
        from sqlalchemy import Column, Float, Index, MetaData, String, Table
        from sqlalchemy.dialects.postgresql import JSON

        self.metadata = MetaData(schema=schema)
        self.states = Table(
            "states",
            self.metadata,
            Column("key", String(1024), primary_key=True),
            Column("document", JSON, nullable=False),
        )
        self.inbox = Table(
            "inbox",
            self.metadata,
            Column("key", String(1024), primary_key=True),
            Column("id", String(128), primary_key=True),
            Column("digest", String(64), nullable=False),
        )
        self.outbox = Table(
            "outbox",
            self.metadata,
            Column("id", String(128), primary_key=True),
            Column("document", JSON, nullable=False),
            Column("owner", String(64)),
            Column("lease_until", Float, nullable=False, default=0),
        )
        Index("outbox_due", self.outbox.c.lease_until)

    async def initialize(self) -> None:
        from sqlalchemy import text

        async with self.database._transaction(migration=True) as conn:
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{self.database.schema}"'))
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:schema))"), {"schema": self.database.schema}
            )
            await conn.execute(
                text(f'CREATE TABLE IF NOT EXISTS "{self.database.schema}".schema_version(version INTEGER PRIMARY KEY)')
            )
            versions = (
                (await conn.execute(text(f'SELECT version FROM "{self.database.schema}".schema_version')))
                .scalars()
                .all()
            )
            if versions and versions not in ([1], [2]):
                raise Conflict("Unsupported message-store schema version")
            if not versions:
                await conn.execute(text(f'INSERT INTO "{self.database.schema}".schema_version VALUES(2)'))
            await conn.run_sync(self.metadata.create_all)
            if versions == [1]:
                # JSONB normalizes exponent-form floats and negative zero, which
                # changes replay fingerprints and publication wire identities.
                for table in ("states", "outbox"):
                    await conn.execute(
                        text(
                            f'ALTER TABLE "{self.database.schema}".{table} '
                            "ALTER COLUMN document TYPE JSON USING document::json"
                        )
                    )
                await conn.execute(text(f'UPDATE "{self.database.schema}".schema_version SET version=2'))

    async def apply(self, key: str, message: Message, update: Update) -> bool:
        from sqlalchemy import insert, select, update as sql_update
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        async with self.database._transaction() as conn:
            await conn.execute(pg_insert(self.states).values(key=key, document={}).on_conflict_do_nothing())
            state = (
                await conn.execute(select(self.states.c.document).where(self.states.c.key == key).with_for_update())
            ).scalar_one()
            digest = fingerprint(json.loads(message.to_bytes()))
            previous = (
                await conn.execute(
                    select(self.inbox.c.digest).where(self.inbox.c.key == key, self.inbox.c.id == message.id)
                )
            ).scalar_one_or_none()
            if previous is not None:
                if previous != digest:
                    raise Conflict("Message ID reused with different content")
                return False
            outgoing = update(state, await self.database._now(conn))
            await conn.execute(sql_update(self.states).where(self.states.c.key == key).values(document=state))
            await conn.execute(insert(self.inbox).values(key=key, id=message.id, digest=digest))
            for item in outgoing:
                item.message.to_bytes()
                publication = pg_insert(self.outbox).values(id=item.message.id, document=item.document(), lease_until=0)
                inserted = await conn.execute(
                    publication.on_conflict_do_update(
                        index_elements=[self.outbox.c.id],
                        set_={"document": self.outbox.c.document},
                    ).returning(self.outbox.c.document)
                )
                # Compare wire fingerprints, not numeric JSON equality.
                # Lock the existing row without changing its document or lease.
                if fingerprint(inserted.scalar_one()) != fingerprint(item.document()):
                    raise Conflict("Outgoing identity conflict")
            return True

    async def read(self, key: str) -> dict[str, Any]:
        from sqlalchemy import select

        async with self.database._transaction() as conn:
            return (
                await conn.execute(select(self.states.c.document).where(self.states.c.key == key))
            ).scalar_one_or_none() or {}

    async def claim(self, owner: str, limit: int = 32) -> list[Publication]:
        from sqlalchemy import select, update

        async with self.database._transaction() as conn:
            now = await self.database._now(conn)
            rows = (
                await conn.execute(
                    select(self.outbox.c.id, self.outbox.c.document)
                    .where(self.outbox.c.lease_until <= now)
                    .order_by(self.outbox.c.id)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            if rows:
                await conn.execute(
                    update(self.outbox)
                    .where(self.outbox.c.id.in_([row.id for row in rows]))
                    .values(owner=owner, lease_until=now + 30)
                )
            return [Publication.restore(row.document) for row in rows]

    async def list_states(self, prefix: str, *, after: str = "", limit: int = 100) -> list[dict[str, Any]]:
        from sqlalchemy import select

        async with self.database._transaction() as conn:
            rows = await conn.execute(
                select(self.states.c.document)
                .where(self.states.c.key.startswith(prefix, autoescape=True), self.states.c.key > after)
                .order_by(self.states.c.key)
                .limit(limit)
            )
            return [row.document for row in rows]

    async def delivered(self, message_id: str, owner: str) -> None:
        from sqlalchemy import delete

        async with self.database._transaction() as conn:
            await conn.execute(delete(self.outbox).where(self.outbox.c.id == message_id, self.outbox.c.owner == owner))

    async def release(self, message_id: str, owner: str) -> None:
        from sqlalchemy import update

        async with self.database._transaction() as conn:
            await conn.execute(
                update(self.outbox)
                .where(self.outbox.c.id == message_id, self.outbox.c.owner == owner)
                .values(owner=None, lease_until=await self.database._now(conn) + 1)
            )

    async def ping(self) -> bool:
        await self.read("__health__")
        return True

    async def close(self) -> None:
        await self.database.close()
