"""The same atomicity and ownership contract on all three message journals."""

import asyncio
import os
from uuid import uuid4

import pytest
import pytest_asyncio

from duraflow import Conflict, ManualClock, ProtocolError, SQLiteMessageStore
from duraflow.message_store import MemoryMessageStore
from duraflow.messaging import Message, Publication


@pytest_asyncio.fixture(params=["memory", "sqlite", pytest.param("postgres", marks=pytest.mark.integration)])
async def journals(request, tmp_path):
    clock = ManualClock()
    if request.param == "memory":
        first = second = MemoryMessageStore(clock=clock)
    elif request.param == "sqlite":
        first, second = [SQLiteMessageStore(tmp_path / "journal.db", clock=clock) for _ in range(2)]
    else:
        from duraflow.message_postgres import PostgresMessageStore

        url = os.getenv("DURAFLOW_TEST_POSTGRES")
        if not url:
            pytest.skip("Native PostgreSQL endpoint required")
        schema = "contract_" + uuid4().hex[:12]
        first, second = [PostgresMessageStore(url, schema=schema) for _ in range(2)]
        await first.initialize()
    try:
        yield first, second, clock
    finally:
        if request.param == "postgres":
            from sqlalchemy import text

            async with first.database._transaction() as conn:
                await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await first.close()
        if second is not first:
            await second.close()


def increment(state, now):
    state["count"] = state.get("count", 0) + 1
    return []


async def test_concurrent_duplicate_and_distinct_deliveries(journals):
    first, second, _ = journals
    message = Message("increment", "shared", {})
    accepted = await asyncio.gather(*[s.apply("shared", message, increment) for s in [first, second] * 12])
    assert sum(accepted) == 1
    await asyncio.gather(
        *[s.apply("shared", Message("increment", "shared", {}), increment) for s in [first, second] * 12]
    )
    assert await first.read("shared") == {"count": 25}
    with pytest.raises(Conflict):
        await second.apply("shared", Message("increment", "shared", {"changed": True}, id=message.id), increment)
    assert await second.read("shared") == {"count": 25}


@pytest.mark.parametrize("same_batch", [False, True])
@pytest.mark.parametrize("identical", [False, True])
async def test_outgoing_identity_collision_is_atomic(journals, same_batch, identical):
    first, second, _ = journals
    outgoing = Publication("events", Message("result", "shared", {"value": 1}))
    other = (
        outgoing
        if identical
        else Publication("events", Message("result", "shared", {"value": 2}, id=outgoing.message.id))
    )

    def original(state, now):
        state["count"] = 1
        return [outgoing]

    if not same_batch:
        await first.apply("shared", Message("original", "shared", {}), original)

    def collision(state, now):
        state["count"] = 2
        return [outgoing, other] if same_batch else [other]

    message = Message("collision", "shared", {})
    if identical:
        assert await second.apply("shared", message, collision)
        assert await first.read("shared") == {"count": 2}
    else:
        with pytest.raises(Conflict):
            await second.apply("shared", message, collision)
        assert await first.read("shared") == ({} if same_batch else {"count": 1})
        # The failed transaction must not consume the inbox identity either.
        assert await second.apply("shared", message, increment)
    claimed = await first.claim("relay")
    assert [p.document() for p in claimed] == ([] if same_batch and not identical else [outgoing.document()])


async def test_competing_relays_and_stale_completion(journals):
    first, second, _ = journals
    outgoing = [Publication("events", Message("result", str(i), {})) for i in range(40)]
    await first.apply("shared", Message("seed", "shared", {}), lambda state, now: outgoing)
    left, right = await asyncio.gather(first.claim("left", limit=20), second.claim("right", limit=20))
    assert len(left) == len(right) == 20
    assert {p.message.id for p in left}.isdisjoint(p.message.id for p in right)
    for publication in left:
        await second.delivered(publication.message.id, "right")
        await second.release(publication.message.id, "right")
    assert await first.claim("third") == []
    for owner, items in (("left", left), ("right", right)):
        for item in items:
            await first.delivered(item.message.id, owner)
    assert await second.claim("third") == []


@pytest.mark.parametrize("value", [1.0, True])
@pytest.mark.parametrize("same_batch", [False, True])
async def test_outbox_identity_uses_wire_types_and_retains_lease(journals, value, same_batch):
    first, second, _ = journals
    original = Publication("events", Message("result", "key", {"value": 1}, id="typed"))
    changed = Publication("events", Message("result", "key", {"value": value}, id="typed"))
    if not same_batch:
        await first.apply("key", Message("seed", "key", {}), lambda state, now: [original])
        assert len(await first.claim("old")) == 1
        await second.apply("key", Message("repeat", "key", {}), lambda state, now: [original])
        assert await second.claim("new") == [], "Identical publication must not release an existing lease"
    with pytest.raises(Conflict):
        await second.apply(
            "key", Message("changed", "key", {}), lambda state, now: [original, changed] if same_batch else [changed]
        )
    if not same_batch:
        await first.delivered("typed", "old")
    assert await second.claim("new") == []


async def test_rollback_after_partial_outbox_and_literal_pagination(journals):
    first, second, _ = journals
    message = Message("seed", "prefix_%/1", {})

    def broken(state, now):
        state["leaked"] = True
        return [
            Publication("events", Message("valid", "key", {})),
            Publication("events", Message("bad", "key", {"x": object()})),
        ]

    with pytest.raises(ProtocolError):
        await first.apply(message.key, message, broken)
    assert await second.read(message.key) == {}
    assert await second.claim("relay") == []
    assert await second.apply(message.key, message, increment)
    for key in ("prefix_%/2", "prefix_AB/3", "prefix_%/3"):
        await first.apply(key, Message("seed", key, {}), lambda state, now, key=key: state.update(key=key) or [])
    assert await second.list_states("prefix_%/", after="prefix_%/1", limit=1) == [{"key": "prefix_%/2"}]
    assert await second.list_states("prefix_%/", after="prefix_%/2", limit=5) == [{"key": "prefix_%/3"}]


async def test_sqlite_storage_full_rolls_back_and_can_retry(tmp_path, monkeypatch):
    import sqlite3

    store = SQLiteMessageStore(tmp_path / "quota.db")
    original = store._connect

    def limited():
        connection = original()
        pages = connection.execute("PRAGMA page_count").fetchone()[0]
        connection.execute(f"PRAGMA max_page_count={pages + 1}")
        return connection

    message = Message("large", "key", {})

    def write(state, now):
        state["payload"] = "x" * 200000
        return []

    monkeypatch.setattr(store, "_connect", limited)
    with pytest.raises(sqlite3.OperationalError, match="full"):
        await store.apply("key", message, write)
    assert await store.read("key") == {}
    monkeypatch.setattr(store, "_connect", original)
    assert await store.apply("key", message, increment)
    assert await store.read("key") == {"count": 1}


@pytest.mark.parametrize("value", [1e20, 1e-20, -0.0])
async def test_numeric_wire_representation_survives_journal_and_publication(journals, value):
    from duraflow.contracts import canonical

    first, second, _ = journals
    item = Publication("events", Message("value", "key", {"value": value}))

    def update(state, now):
        state["value"] = value
        return [item]

    await first.apply("key", Message("seed", "key", {}), update)
    assert canonical(await second.read("key")) == canonical({"value": value})
    await second.apply("key", Message("repeat", "key", {}), update)
    claimed = await first.claim("relay")
    assert len(claimed) == 1 and claimed[0].message.to_bytes() == item.message.to_bytes()
