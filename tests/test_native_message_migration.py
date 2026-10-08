"""Transactional upgrade from lossy JSONB journals to lossless JSON documents."""

import os
from uuid import uuid4

import pytest

from duraflow import Conflict
from duraflow.message_postgres import PostgresMessageStore
from duraflow.messaging import Message, Publication

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not os.getenv("DURAFLOW_TEST_POSTGRES"), reason="Native PostgreSQL endpoint required"),
]


@pytest.mark.parametrize("fail_once", [False, True])
async def test_message_schema_upgrade_preserves_rows_and_rolls_back_on_failure(fail_once):
    from sqlalchemy import event, text

    schema = "message_migration_" + uuid4().hex[:12]
    store = PostgresMessageStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=schema)
    try:
        await store.initialize()
        outgoing = Publication("events", Message("result", "key", {"value": 7}))
        incoming = Message("seed", "key", {})
        await store.apply("key", incoming, lambda state, now: state.update(value=7) or [outgoing])
        await store.claim("owner")
        async with store.database._transaction() as conn:
            for table in ("states", "outbox"):
                await conn.execute(
                    text(f'ALTER TABLE "{schema}".{table} ALTER COLUMN document TYPE JSONB USING document::jsonb')
                )
            await conn.execute(text(f'UPDATE "{schema}".schema_version SET version=1'))
        if fail_once:

            def fail(conn, cursor, statement, parameters, context, executemany):
                if "ALTER TABLE" in statement and ".outbox " in statement:
                    raise RuntimeError("migration interrupted")

            event.listen(store.database.engine.sync_engine, "before_cursor_execute", fail)
            try:
                with pytest.raises(RuntimeError, match="migration interrupted"):
                    await store.initialize()
            finally:
                event.remove(store.database.engine.sync_engine, "before_cursor_execute", fail)
            async with store.database._transaction() as conn:
                assert (await conn.execute(text(f'SELECT version FROM "{schema}".schema_version'))).scalar_one() == 1
                assert (
                    await conn.execute(text(f'SELECT pg_typeof(document)::text FROM "{schema}".states'))
                ).scalar_one() == "jsonb"
        await store.initialize()
        await store.initialize()
        assert await store.read("key") == {"value": 7}
        assert not await store.apply("key", incoming, lambda state, now: [])
        assert await store.claim("other") == []
        await store.delivered(outgoing.message.id, "owner")
        async with store.database._transaction() as conn:
            assert (await conn.execute(text(f'SELECT version FROM "{schema}".schema_version'))).scalar_one() == 2
            assert (
                await conn.execute(text(f'SELECT pg_typeof(document)::text FROM "{schema}".states'))
            ).scalar_one() == "json"
            await conn.execute(text(f'UPDATE "{schema}".schema_version SET version=999'))
        with pytest.raises(Conflict):
            await store.initialize()
    finally:
        async with store.database._transaction() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await store.close()
