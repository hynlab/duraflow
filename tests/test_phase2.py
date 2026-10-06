from __future__ import annotations

import asyncio
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest

from duraflow import Client, Engine, Registry, Worker
from duraflow.contracts import Clock, Conflict, clock_now
from duraflow.postgres import PostgresStore
from duraflow.projection import next_due
from duraflow.state import mutate, route
from duraflow.transport import MemoryTransport, PulsarTransport
from tests.test_engine import DOUBLE, double, sequence


def snapshot():
    return json.loads((Path(__file__).parent / "fixtures/alpha_sequence.json").read_text())


def test_due_projection_preserves_terminal_obligations_and_ignores_quiet_runs():
    state = snapshot()
    for item in state["outbox"].values():
        item["delivered"] = True
    assert next_due(state, 100) is None
    item = next(iter(state["outbox"].values()))
    item.update(delivered=False, lease_until=0)
    assert next_due(state, 100) == 0
    item["next_attempt_at"] = 150
    assert next_due(state, 100) == 150
    state["archived"] = True
    assert next_due(state, 100) is None


def test_sleep_and_signal_projection():
    state = snapshot()
    state["outbox"] = {}
    state["status"] = "WAITING"
    state["commands"] = [{"state": "pending", "members": ["timer"], "spec": {"kind": "sleep"}}]
    state["nodes"] = {"timer": {"state": "pending", "spec": {"kind": "sleep"}, "due_at": 10000}}
    assert next_due(state, 100) == 10000
    state["nodes"]["timer"] = {"state": "pending", "spec": {"kind": "signal", "name": "go"}, "due_at": None}
    assert next_due(state, 100) is None
    state["signals"] = [{"consumed": False, "name": "go"}]
    assert next_due(state, 100) == 0


async def test_native_cancel_keeps_capacity_until_real_call_finishes():
    transport = object.__new__(PulsarTransport)
    transport.native_pool = ThreadPoolExecutor(max_workers=1)
    transport.native_slots = asyncio.Semaphore(1)
    started, release = threading.Event(), threading.Event()

    def blocked():
        started.set()
        release.wait(5)

    first = asyncio.create_task(transport._native(blocked))
    try:
        for _ in range(100):
            if started.is_set():
                break
            await asyncio.sleep(0.005)
        assert started.is_set()
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        second = asyncio.create_task(transport._native(lambda: 42))
        await asyncio.sleep(0.02)
        assert not second.done()
        release.set()
        assert await asyncio.wait_for(second, 3) == 42
    finally:
        release.set()
        transport.native_pool.shutdown(wait=True)


native = pytest.mark.skipif(not os.getenv("DURAFLOW_TEST_POSTGRES"), reason="Native PostgreSQL not configured")


@pytest.fixture
async def pg():
    if not os.getenv("DURAFLOW_TEST_POSTGRES"):
        pytest.skip("Native PostgreSQL not configured")
    from sqlalchemy import text

    store = PostgresStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema="phase2_" + uuid4().hex[:12])
    try:
        yield store
    finally:
        async with store.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{store.schema}" CASCADE'))
        await store.close()


@pytest.mark.integration
@native
async def test_migration_preserves_json_and_rejects_downgrade(pg):
    from sqlalchemy import text

    await pg.migrate(target=1)
    state = snapshot()
    async with pg.engine.begin() as conn:
        await conn.execute(
            text(
                f'INSERT INTO "{pg.schema}".runs (namespace,run_id,revision,document) '
                "VALUES (:ns,:run,:rev,CAST(:doc AS jsonb))"
            ),
            {"ns": state["namespace"], "run": state["run_id"], "rev": state["revision"], "doc": json.dumps(state)},
        )
    await pg.migrate()
    assert await pg.schema_version() == 2
    assert await pg.load(state["namespace"], state["run_id"]) == state
    await pg.migrate()
    with pytest.raises(Conflict):
        await pg.migrate(target=1)


@pytest.mark.integration
@native
async def test_migration_rolls_back_on_projection_failure_then_retries(pg, monkeypatch):
    from sqlalchemy import text
    import duraflow.postgres as module

    await pg.migrate(target=1)
    state = snapshot()
    async with pg.engine.begin() as conn:
        await conn.execute(
            text(f'INSERT INTO "{pg.schema}".runs VALUES (:ns,:run,:rev,CAST(:doc AS jsonb))'),
            {"ns": state["namespace"], "run": state["run_id"], "rev": state["revision"], "doc": json.dumps(state)},
        )
    original = module.next_due

    def broken(*args, **kwargs):
        raise RuntimeError("injected migration boundary")

    monkeypatch.setattr(module, "next_due", broken)
    with pytest.raises(RuntimeError):
        await pg.migrate()
    assert await pg.schema_version() == 1
    async with pg.engine.connect() as conn:
        columns = (
            (
                await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema=:schema AND table_name='runs'"
                    ),
                    {"schema": pg.schema},
                )
            )
            .scalars()
            .all()
        )
    assert "next_due" not in columns
    monkeypatch.setattr(module, "next_due", original)
    await pg.migrate()
    assert await pg.load(state["namespace"], state["run_id"]) == state


class SkewClock(Clock):
    def now(self):
        return 1.0


@pytest.mark.integration
@native
async def test_server_clock_is_sampled_after_lock_and_fences_competing_claims(pg):
    from sqlalchemy import select

    await pg.initialize()
    registry = Registry(sequence, double)
    transport = MemoryTransport()
    client = Client(pg, registry, namespace="skew", clock=SkewClock())
    engine = Engine(pg, transport, registry, namespace="skew", clock=SkewClock())
    workers = [Worker(pg, transport, registry, namespace="skew", clock=SkewClock()) for _ in range(2)]
    for worker in workers:
        await worker.prepare()
    try:
        handle = await client.start(sequence, 2, request_id="clock")
        assert (await handle.describe())["created_at"] > 1_700_000_000
        await engine.tick()
        await engine.tick()
        delivery = await transport.receive(route("skew", DOUBLE.descriptor()), "workers")
        assert delivery is not None
        contexts = await asyncio.gather(*(worker._claim(delivery, DOUBLE) for worker in workers))
        assert sum(context is not None for context in contexts) == 1
        owner = next(context for context in contexts if context is not None)
        assert await owner.heartbeat()
        async with pg.engine.begin() as locked:
            await locked.execute(select(pg.runs.c.run_id).where(pg.runs.c.run_id == handle.run_id).with_for_update())
            pending = asyncio.create_task(mutate(pg, "skew", handle.run_id, lambda state: clock_now(SkewClock())))
            await asyncio.sleep(0.05)
            assert not pending.done()
            before_unlock = await pg.now()
        sampled = await asyncio.wait_for(pending, 5)
        assert sampled >= before_unlock
    finally:
        for worker in workers:
            await worker.close()
        await transport.close()


@pytest.mark.integration
@native
async def test_due_queries_exclude_quiet_history_and_engine_avoids_full_scan(pg):
    from sqlalchemy import insert, text

    await pg.initialize()
    registry = Registry(sequence, double)
    template = snapshot()
    template["namespace"] = "due"
    template["outbox"] = {}
    rows = []
    now = await pg.now()
    for index in range(500):
        state = deepcopy(template)
        state["run_id"] = f"quiet-{index:05}"
        rows.append({"namespace": "due", "run_id": state["run_id"], **pg._values(state, 0, now)})
    async with pg.engine.begin() as conn:
        await conn.execute(insert(pg.runs), rows)
    client = Client(pg, registry, namespace="due")
    active = await client.start(sequence, 2, request_id="active")
    due = await pg.scan_due("due", manifests=tuple(d.manifest for d in registry.workflows.values()))
    assert due == [{"run_id": active.run_id}]
    async with pg.engine.begin() as conn:
        await conn.execute(text("SET LOCAL enable_seqscan = off"))
        plan = (
            (
                await conn.execute(
                    text(f"EXPLAIN SELECT run_id FROM \"{pg.schema}\".runs WHERE namespace='due' AND next_due <= :now"),
                    {"now": now},
                )
            )
            .scalars()
            .all()
        )
    assert "runs_due_idx" in " ".join(plan)

    async def forbidden(*args, **kwargs):
        raise AssertionError("Runtime used a full JSON scan")

    pg.scan = forbidden
    engine = Engine(pg, MemoryTransport(), registry, namespace="due")
    assert await engine.tick() == 1
    await engine.close()
