"""PostgreSQL message journal with per-instance locks and recoverable outbox."""

from __future__ import annotations

import json
from typing import Any

from .contracts import Conflict, MessageIntegrityError, fingerprint
from .message_store import Update
from .message_records import LocalUpdate, Records, apply_update
from .messaging import Message, Publication, RetryLater
from .postgres import PostgresStore
from .message_telemetry import head_status, sample


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
            Column("expires_at", Float),
            Column("namespace", String(128)),
            Column("status", String(32)),
        )
        self.inbox = Table(
            "inbox",
            self.metadata,
            Column("key", String(1024), primary_key=True),
            Column("id", String(128), primary_key=True),
            Column("digest", String(64), nullable=False),
            Column("expires_at", Float),
        )
        self.outbox = Table(
            "outbox",
            self.metadata,
            Column("id", String(128), primary_key=True),
            Column("document", JSON, nullable=False),
            Column("owner", String(64)),
            Column("lease_until", Float, nullable=False, default=0),
            Column("state_key", String(1024)),
            Column("created_at", Float),
        )
        Index("outbox_due", self.outbox.c.lease_until)
        self.records = Table(
            "records",
            self.metadata,
            Column("key", String(1024), primary_key=True),
            Column("name", String(1024), primary_key=True),
            Column("document", JSON, nullable=False),
            Column("expires_at", Float),
        )
        Index("records_expiry", self.records.c.expires_at)

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
            if versions and versions not in ([1], [2], [3], [4]):
                raise Conflict("Unsupported message-store schema version")
            if not versions:
                await conn.execute(text(f'INSERT INTO "{self.database.schema}".schema_version VALUES(4)'))
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
            if versions in ([1], [2]):
                for table, column in (
                    ("states", "expires_at DOUBLE PRECISION"),
                    ("inbox", "expires_at DOUBLE PRECISION"),
                    ("outbox", "state_key VARCHAR(1024)"),
                ):
                    await conn.execute(
                        text(f'ALTER TABLE "{self.database.schema}".{table} ADD COLUMN IF NOT EXISTS {column}')
                    )
                await conn.execute(text(f'UPDATE "{self.database.schema}".schema_version SET version=3'))
            if versions in ([1], [2], [3]):
                schema = self.database.schema
                for table, column in (
                    ("states", "namespace VARCHAR(128)"),
                    ("states", "status VARCHAR(32)"),
                    ("outbox", "created_at DOUBLE PRECISION"),
                ):
                    await conn.execute(text(f'ALTER TABLE "{schema}".{table} ADD COLUMN IF NOT EXISTS {column}'))
                await conn.execute(
                    text(
                        f"UPDATE \"{schema}\".states SET namespace=document->'runs'->(document->>'current')->>'namespace',"
                        "status=document->'runs'->(document->>'current')->>'status'"
                    )
                )
                await conn.execute(
                    text(
                        f'UPDATE "{schema}".outbox SET created_at=EXTRACT(EPOCH FROM clock_timestamp()) '
                        "WHERE created_at IS NULL"
                    )
                )
                await conn.execute(text(f'UPDATE "{schema}".schema_version SET version=4'))
            await conn.execute(
                text(f'CREATE INDEX IF NOT EXISTS states_status ON "{self.database.schema}".states(namespace,status)')
            )
            await conn.execute(
                text(f'CREATE INDEX IF NOT EXISTS outbox_age ON "{self.database.schema}".outbox(created_at)')
            )
            for table, column in (("states", "expires_at"), ("inbox", "expires_at"), ("outbox", "state_key")):
                await conn.execute(
                    text(f'CREATE INDEX IF NOT EXISTS {table}_{column} ON "{self.database.schema}".{table}({column})')
                )

    async def apply(self, key: str, message: Message, update: Update) -> bool:
        from sqlalchemy import insert, select, update as sql_update
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        async with self.database._transaction() as conn:
            create = not isinstance(update, LocalUpdate) or update.create
            if create:
                await conn.execute(pg_insert(self.states).values(key=key, document={}).on_conflict_do_nothing())
            state = (
                await conn.execute(select(self.states.c.document).where(self.states.c.key == key).with_for_update())
            ).scalar_one_or_none()
            if state is None:
                if not create:
                    return False
                raise RetryLater()
            # Admission and dedup must observe time after acquiring the same
            # parent lock as GC, otherwise expiry can race deletion of the key.
            now = await self.database._now(conn)
            message.admit(now)
            digest = fingerprint(json.loads(message.to_bytes()))
            previous = (
                await conn.execute(
                    select(self.inbox.c.digest).where(self.inbox.c.key == key, self.inbox.c.id == message.id)
                )
            ).scalar_one_or_none()
            if previous is not None and not isinstance(update, LocalUpdate):
                if previous != digest:
                    raise MessageIntegrityError("Message ID reused with different content")
                return False

            def transition(sync_conn: Any) -> list[Publication]:
                def load(name: str) -> dict[str, Any] | None:
                    return sync_conn.execute(
                        select(self.records.c.document).where(self.records.c.key == key, self.records.c.name == name)
                    ).scalar_one_or_none()

                records = Records(load)
                outgoing = apply_update(update, state, now, records)
                for name, (document, expires_at) in records.writes.items():
                    statement = pg_insert(self.records).values(
                        key=key, name=name, document=document, expires_at=expires_at
                    )
                    sync_conn.execute(
                        statement.on_conflict_do_update(
                            index_elements=[self.records.c.key, self.records.c.name],
                            set_={"document": document, "expires_at": expires_at},
                        )
                    )
                return outgoing

            outgoing = await conn.run_sync(transition)
            await conn.execute(
                sql_update(self.states)
                .where(self.states.c.key == key)
                .values(
                    document=state,
                    expires_at=state.get("_gc_after"),
                    namespace=head_status(state)[0],
                    status=head_status(state)[1],
                )
            )
            if not isinstance(update, LocalUpdate):
                await conn.execute(
                    insert(self.inbox).values(key=key, id=message.id, digest=digest, expires_at=message.expires_at())
                )
            for item in outgoing:
                item.message.to_bytes()
                publication = pg_insert(self.outbox).values(
                    id=item.message.id, document=item.document(), lease_until=0, state_key=key, created_at=now
                )
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

    async def collect_garbage(self, *, limit: int = 100, dry_run: bool = True) -> dict[str, int]:
        from sqlalchemy import delete, or_, select, tuple_

        if not 1 <= limit <= 10000:
            raise ValueError("Invalid GC batch size")
        result = {}
        async with self.database._transaction() as conn:
            now = await self.database._now(conn)
            # Lock parent rows before inspecting their records, matching apply().
            # This prevents GC racing a transition that republishes retained work.
            for name, table in (("records", self.records), ("inbox", self.inbox), ("states", self.states)):
                pending = (
                    select(self.outbox.c.id)
                    .where(or_(self.outbox.c.state_key == table.c.key, self.outbox.c.state_key.is_(None)))
                    .exists()
                )
                eligibility = [table.c.expires_at <= now, ~pending]
                if name == "states":
                    eligibility.extend(
                        [
                            ~select(self.inbox.c.id).where(self.inbox.c.key == table.c.key).exists(),
                            ~select(self.records.c.name).where(self.records.c.key == table.c.key).exists(),
                        ]
                    )
                candidates = (
                    select(table.c.key).where(*eligibility).distinct().order_by(table.c.key).limit(limit).subquery()
                )
                parents = (
                    (
                        await conn.execute(
                            select(self.states.c.key)
                            .where(self.states.c.key.in_(select(candidates.c.key)))
                            .order_by(self.states.c.key)
                            .with_for_update(skip_locked=True)
                        )
                    )
                    .scalars()
                    .all()
                )
                columns = list(table.primary_key.columns)
                rows = (
                    await conn.execute(
                        select(*columns)
                        .where(table.c.key.in_(parents), *eligibility)
                        .order_by(table.c.expires_at)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
                result[name] = len(rows)
                if not dry_run and rows:
                    await conn.execute(delete(table).where(tuple_(*columns).in_([tuple(row) for row in rows])))
        return result

    async def read(self, key: str) -> dict[str, Any]:
        from sqlalchemy import select

        async with self.database._transaction() as conn:
            return (
                await conn.execute(select(self.states.c.document).where(self.states.c.key == key))
            ).scalar_one_or_none() or {}

    async def telemetry(self, namespace: str) -> dict[str, Any]:
        from sqlalchemy import select, func

        async with self.database._transaction() as conn:
            counts = dict(
                (
                    await conn.execute(
                        select(self.states.c.status, func.count())
                        .where(self.states.c.namespace == namespace)
                        .group_by(self.states.c.status)
                    )
                ).all()
            )
            oldest = (await conn.execute(select(func.min(self.outbox.c.created_at)))).scalar_one()
            return sample(await self.database._now(conn), counts, oldest)

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
