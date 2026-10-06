from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from duraflow import Client, Conflict, Engine, MemoryTransport, Registry, SQLiteStore, Worker, WorkflowContext, workflow
from duraflow.cli import execute, parser
from duraflow.postgres import PostgresStore
from duraflow.testing import TestEnvironment
from duraflow.transport import PulsarTransport
from .test_engine import broadcast, a, b, c, bindings, sequence, double


def test_postgres_rejects_wrong_driver_and_unsafe_schema() -> None:
    with pytest.raises(ValueError):
        PostgresStore("sqlite:///file", schema="valid")
    with pytest.raises(ValueError):
        PostgresStore("postgresql+psycopg://localhost/db", schema="x;DROP TABLE runs")


async def test_sqlite_concurrent_start_and_atomic_rollover(tmp_path: Any) -> None:
    @workflow(name="roll", build_id="test")
    async def rolling(ctx: WorkflowContext, value: int) -> int:
        if value < 1:
            await ctx.continue_as_new(1)
        return value

    first, second = SQLiteStore(tmp_path / "shared.db"), SQLiteStore(tmp_path / "shared.db")
    reg = Registry(rolling)
    client1, client2 = Client(first, reg), Client(second, reg)
    h1, h2 = await asyncio.gather(
        client1.start(rolling, 0, request_id="same"), client2.start(rolling, 0, request_id="same")
    )
    assert h1.run_id == h2.run_id
    engine1, engine2 = Engine(first, MemoryTransport(), reg), Engine(second, MemoryTransport(), reg)
    for _ in range(10):
        await asyncio.gather(engine1.tick(), engine2.tick())
    assert await h1.result(timeout=1, follow_continued=True) == 1
    assert len(await client1.list()) == 2
    await first.close()
    await second.close()


async def test_pulsar_provisioning_closes_idle_consumer(monkeypatch: Any) -> None:
    subscribers = []

    class Consumer:
        def __init__(self, queue: int):
            self.queue, self.closed = queue, False

        def close(self) -> None:
            self.closed = True

    class NativeClient:
        def __init__(self, *args: Any, **kwargs: Any):
            pass

        def subscribe(self, *args: Any, **kwargs: Any) -> Consumer:
            consumer = Consumer(kwargs["receiver_queue_size"])
            subscribers.append(consumer)
            return consumer

        def close(self) -> None:
            pass

    monkeypatch.setitem(
        sys.modules,
        "pulsar",
        SimpleNamespace(
            Client=NativeClient,
            ConsumerType=SimpleNamespace(Shared="shared"),
            InitialPosition=SimpleNamespace(Earliest="earliest"),
        ),
    )
    transport = PulsarTransport("pulsar://test")
    await transport.ensure("topic", "handler")
    await transport.ensure("topic", "handler")
    assert len(subscribers) == 1 and subscribers[0].closed
    assert transport.consumers == {}
    await transport.close()


async def test_cli_read_and_explicit_control(tmp_path: Any) -> None:
    database = f"sqlite:///{tmp_path / 'cli.db'}"
    assert (await execute(parser().parse_args(["--database", database, "init"])))["initialized"]
    assert (await execute(parser().parse_args(["--database", database, "health"])))["store_readable"]
    assert await execute(parser().parse_args(["--database", database, "list"])) == []
    with pytest.raises(SystemExit):
        parser().parse_args(["terminate", "a-run", "--actor", "a", "--reason", "b", "--request-id", "c"])


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("DURAFLOW_TEST_POSTGRES"), reason="PostgreSQL service not configured")
async def test_real_postgres_start_cas_and_replay() -> None:
    ns = f"pg-{uuid4().hex[:12]}"
    store = PostgresStore(os.environ["DURAFLOW_TEST_POSTGRES"])
    await store.initialize()
    async with TestEnvironment(Registry(sequence, double), store=store, namespace=ns) as env:
        first, second = await asyncio.gather(
            env.client.start(sequence, 3, request_id="same"), env.client.start(sequence, 3, request_id="same")
        )
        assert first.run_id == second.run_id
        with pytest.raises(Conflict):
            await env.client.start(sequence, 4, request_id="same")
        assert await env.run(first) == 13
        original = await first.describe()
        assert await store.save(original, original["revision"])
        assert not await store.save(original, original["revision"])
        reopened = PostgresStore(os.environ["DURAFLOW_TEST_POSTGRES"])
        assert (await reopened.load(ns, first.run_id))["result"] == 13
        await reopened.close()


@pytest.mark.integration
@pytest.mark.skipif(
    not (os.getenv("DURAFLOW_TEST_POSTGRES") and os.getenv("DURAFLOW_TEST_PULSAR")),
    reason="Native PostgreSQL/Pulsar services not configured",
)
async def test_real_broadcast_with_independent_runtime_connections() -> None:
    ns = f"native-{uuid4().hex[:12]}"
    store = PostgresStore(os.environ["DURAFLOW_TEST_POSTGRES"])
    await store.initialize()
    engine_transport = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"])
    worker_transport = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"])
    reg = Registry(broadcast, a, b, c)
    client = Client(store, reg, namespace=ns)
    engine = Engine(store, engine_transport, reg, namespace=ns)
    worker = Worker(store, worker_transport, reg, namespace=ns, broadcasts=bindings())
    stop = asyncio.Event()
    running = [asyncio.create_task(engine.run(stop)), asyncio.create_task(worker.run(stop))]
    try:
        h = await client.start(broadcast, 2, request_id="native")
        assert isinstance(await h.result(timeout=90), str)
        state = await h.describe()
        assert len([n for n in state["nodes"].values() if n["spec"]["kind"] == "call"]) == 3
        assert worker.metrics["executed"] == 3
        assert not engine_transport.consumers, "An idle coordinator consumer would steal worker messages"
    finally:
        stop.set()
        try:
            await asyncio.wait_for(asyncio.gather(*running), timeout=15)
        except TimeoutError:
            for running_task in running:
                running_task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
        await worker.close()
        await worker_transport.close()
        await engine_transport.close()
        await store.close()
