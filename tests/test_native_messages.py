"""Message-driven qualification using independent real Pulsar clients and journals."""

import asyncio
import os
import json
import subprocess
import sys
from uuid import uuid4

import pytest
import pytest_asyncio

from duraflow import Client, Topics, WorkflowEngine, WorkflowWorker, TaskWorker, TagEngine, WorkflowFailed
from duraflow.executor import ProcessReplayExecutor
from duraflow.message_postgres import PostgresMessageStore
from duraflow.transport import PulsarTransport
from tests.message_app import APPROVAL, order, timed, registry

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.getenv("DURAFLOW_TEST_POSTGRES") and os.getenv("DURAFLOW_TEST_PULSAR")),
        reason="Native PostgreSQL/Pulsar endpoints required",
    ),
]


@pytest_asyncio.fixture
async def cluster():
    from sqlalchemy import text

    name = "msg_" + uuid4().hex[:12]
    topics = Topics(namespace=name)
    stores = [
        PostgresMessageStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=name + suffix)
        for suffix in ("", "_tasks", "_tags")
    ]
    transports = []
    runtimes = []
    running = []
    stop = asyncio.Event()
    client = None
    try:
        for store in stores:
            await store.initialize()
        transports = [PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"]) for _ in range(6)]
        runtimes = [
            WorkflowEngine(stores[0], transports[0], registry, topics=topics, concurrency=2),
            WorkflowEngine(stores[0], transports[1], registry, topics=topics, concurrency=2),
            WorkflowWorker(
                transports[2], registry, topics=topics, replay_executor=ProcessReplayExecutor("tests.message_app")
            ),
            TaskWorker(transports[3], registry, topics=topics, journal=stores[1]),
            TagEngine(stores[2], transports[4], topics=topics),
        ]
        for runtime in runtimes:
            await runtime.prepare()
        running = [asyncio.create_task(r.run(stop, poll_interval=0.01)) for r in runtimes]
        client = Client(transports[5], registry, topics=topics, timeout=30)
        yield client, runtimes, stores, topics
    finally:
        stop.set()
        if running:
            try:
                async with asyncio.timeout(5):
                    await asyncio.gather(*running)
            except TimeoutError:
                for task in running:
                    task.cancel()
                await asyncio.gather(*running, return_exceptions=True)
        if client:
            await client.close()
        for runtime in runtimes:
            await runtime.close()
        for transport in transports:
            await transport.close()
        for store in stores:
            try:
                async with store.database._transaction() as conn:
                    await conn.execute(text(f'DROP SCHEMA IF EXISTS "{store.database.schema}" CASCADE'))
            finally:
                await store.close()


async def wait_for(predicate, timeout=15):
    async with asyncio.timeout(timeout):
        while True:
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.05)


async def test_native_start_replay_task_signal_and_result_loop(cluster):
    client, runtimes, stores, topics = cluster
    handle = await client.start(order, 7, request_id="order")

    async def receiving():
        state = await handle.describe()
        return bool(state["channels"])

    await wait_for(receiving)
    await handle.signal(APPROVAL, True, signal_id="approved")
    assert await handle.result(timeout=20) == 28
    assert stores[0].database.schema != runtimes[3].journal.database.schema
    assert not hasattr(client, "store") and not hasattr(runtimes[2], "store")


async def test_native_tag_registration_ack_precedes_start_response(cluster):
    client, runtimes, stores, topics = cluster
    handles = [await client.start(order, i + 1, request_id=f"tagged-{i}", tags=("orders",)) for i in range(2)]

    async def ready():
        return all([bool((await h.describe())["channels"]) for h in handles])

    await wait_for(ready)
    await client.signal_tagged(order, "orders", APPROVAL, True, signal_id="approve-all")
    assert [await h.result(timeout=20) for h in handles] == [4, 8]


async def test_native_state_engine_restart_preserves_waiting_channel(cluster):
    client, runtimes, stores, topics = cluster
    handle = await client.start(order, 3, request_id="restart")

    async def waiting():
        state = await handle.describe()
        return any(n["spec"]["kind"] == "channel_next" and n["state"] == "pending" for n in state["nodes"].values())

    await wait_for(waiting)
    # Both engines stop admission. Events remain in Pulsar while state stays in DB.
    runtimes[0].accepting = runtimes[1].accepting = False
    signaling = asyncio.create_task(handle.signal(APPROVAL, True, signal_id="while-offline"))
    await asyncio.sleep(0.2)
    assert not signaling.done()
    runtimes[0].accepting = runtimes[1].accepting = True
    await signaling
    assert await handle.result(timeout=20) == 12


async def test_native_delayed_timer_drives_channel_timeout(cluster):
    client, runtimes, stores, topics = cluster
    handle = await client.start(timed, 0, request_id="timer")
    try:
        with pytest.raises(WorkflowFailed):
            await handle.result(timeout=10)
    except TimeoutError:
        pytest.fail(json.dumps({"state": await handle.describe(), "metrics": [r.metrics for r in runtimes]}))
    state = await handle.describe()
    assert state["error"]["code"] == "SIGNAL_TIMEOUT"
    assert state["finished_at"] >= state["nodes"]["1.0"]["due_at"]


async def test_native_cli_start_signal_and_result_without_database_credentials(cluster):
    client, runtimes, stores, topics = cluster

    async def cli(*args):
        result = await asyncio.to_thread(
            subprocess.run,
            [
                sys.executable,
                "-m",
                "duraflow",
                "--app",
                "tests.message_app",
                "--workflow",
                "message-order:v1",
                "--namespace",
                topics.namespace,
                "--broker",
                os.environ["DURAFLOW_TEST_PULSAR"],
                *args,
            ],
            env={
                **os.environ,
                "DURAFLOW_DATABASE_URL": "postgresql+psycopg://unreachable:no-credentials@invalid/workflows",
            },
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return json.loads(result.stdout)

    assert (await cli("start", "message-order:v1", "--input", "5", "--request-id", "cli-order"))[
        "workflow_id"
    ] == "cli-order"
    handle = client.get_handle(order, "cli-order")

    async def registered():
        return bool((await handle.describe())["channels"])

    await wait_for(registered)
    await cli("signal", "cli-order", "approval", "--input", "true", "--signal-id", "cli-approval")
    assert await cli("result", "cli-order", "--timeout", "20") == 20
