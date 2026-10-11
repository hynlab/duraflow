"""Real Pulsar/PostgreSQL: endless broadcast/join, rollover, client churn and GC.

Role loops share a process; use message_soak for independent process failures.
Requires DURAFLOW_TEST_POSTGRES and DURAFLOW_TEST_PULSAR. Every run owns fresh
schemas/topics. Samples are bounded; the external-effect ledger is disk-backed.
"""

import argparse
import asyncio
from collections import deque
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
from uuid import uuid4

from duraflow import (
    BroadcastBinding,
    ChannelRef,
    Client,
    HandlerRef,
    Registry,
    TaskRef,
    TopicRef,
    Topics,
    WorkflowContext,
    WorkflowEngine,
    WorkflowWorker,
    TaskWorker,
    task,
    workflow,
)
from duraflow.message_postgres import PostgresMessageStore
from duraflow.retention import DAY
from duraflow.transport import PulsarTransport


def workload(topics, ledger):
    refs = [TaskRef(f"lifecycle-{letter}", int, int) for letter in "abc"]
    handlers = tuple(HandlerRef(ref, ref.name) for ref in refs)
    event = TopicRef(topics.topic("business", "input"), int)
    output = TopicRef(topics.topic("business", "output"), int)
    channel = ChannelRef("next", int)

    def effect(key, value):
        with closing(sqlite3.connect(ledger, timeout=10)) as conn, conn:
            conn.execute(
                "INSERT INTO effects VALUES(?,?,1) ON CONFLICT(key) DO UPDATE SET calls=calls+1",
                (key, value),
            )
        return value

    implementations = []
    for ref in refs:

        async def execute(ctx, value):
            return await asyncio.to_thread(effect, ctx.idempotency_key, value)

        implementations.append(task(ref=ref)(execute))

    @workflow(name="lifecycle-loop", build_id="lifecycle-v1")
    async def continuous(ctx: WorkflowContext, generation: int) -> int:
        value = await ctx.channel(channel).receive(max_signals=1).next()
        if value < 0:
            return generation
        results = await ctx.broadcast(event, value, handlers=handlers)
        await ctx.publish(output, sum(results[handler] for handler in handlers))
        await ctx.continue_as_new(generation + 1)
        return generation

    return (
        Registry(continuous, *implementations),
        continuous,
        channel,
        tuple(BroadcastBinding(event, h) for h in handlers),
    )


async def run(seconds, directory):
    from sqlalchemy import func, select, text

    namespace = "lifecycle_" + uuid4().hex[:12]
    topics = Topics(namespace=namespace)
    ledger = directory / "effects.db"
    with closing(sqlite3.connect(ledger)) as conn, conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE effects(key TEXT PRIMARY KEY, value INTEGER, calls INTEGER)")
    registry, flow, channel, bindings = workload(topics, ledger)
    stores = [
        PostgresMessageStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=namespace + suffix)
        for suffix in ("", "_tasks")
    ]
    server = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"], max_routes=32)
    transport = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"], max_routes=4)
    main_client = Client(transport, topics=topics)

    def role(index):
        if index == 0:
            return WorkflowEngine(stores[0], server, registry, topics=topics, concurrency=1)
        if index == 1:
            return WorkflowWorker(server, registry, topics=topics, concurrency=1)
        return TaskWorker(
            server, registry, topics=topics, journal=stores[1], broadcasts=(bindings[index - 2],), concurrency=1
        )

    running = []
    samples = deque(maxlen=720)
    generations, restarts = 0, 0
    max_head_bytes = 0
    began = time.monotonic()
    try:
        for store in stores:
            await store.initialize()
        await transport.ensure(topics.topic("business", "output"), "soak-output")
        for index in range(5):
            runtime, stop = role(index), asyncio.Event()
            await runtime.prepare()
            running.append((runtime, stop, asyncio.create_task(runtime.run(stop, poll_interval=0.001))))
        await main_client.start(flow, 0, request_id="endless")
        handle = main_client.get_handle(flow, "endless")

        async def ready(generation, client_handle):
            async with asyncio.timeout(60):
                while True:
                    state = await client_handle.describe()
                    if state["status"] in {"FAILED", "BLOCKED", "CANCELLED", "TERMINATED"}:
                        raise AssertionError({key: state[key] for key in ("status", "error", "blocked_reason")})
                    if state["input"] == generation and any(
                        n["spec"]["kind"] == "channel_next" and n["state"] == "pending" for n in state["nodes"].values()
                    ):
                        return
                    await asyncio.sleep(0.02)

        await ready(0, handle)
        workload_started = time.monotonic()
        while time.monotonic() - workload_started < seconds:
            client = Client(transport, topics=topics)
            try:
                current = client.get_handle(flow, "endless")
                epoch = int(time.time() // DAY)
                await current.signal(channel, generations, signal_id=f"event-{generations}", signal_epoch=epoch)
                # An ambiguous sender retry must not generate a second broadcast.
                await current.signal(channel, generations, signal_id=f"event-{generations}", signal_epoch=epoch)
                generations += 1
                await ready(generations, current)
                async with asyncio.timeout(10):
                    published = None
                    while published is None:
                        published = await transport.receive(topics.topic("business", "output"), "soak-output")
                assert json.loads(published.data) == (generations - 1) * 3
                await transport.ack(published)
            finally:
                await client.close()
            if generations % 10 == 0:
                index = (generations // 10 - 1) % len(running)
                previous, stop, pending = running[index]
                stop.set()
                await pending
                await previous.close()
                runtime, stop = role(index), asyncio.Event()
                running[index] = runtime, stop, asyncio.create_task(runtime.run(stop, poll_interval=0.001))
                restarts += 1
            for store in stores:
                await store.collect_garbage(limit=1000, dry_run=False)
            head = await stores[0].read(topics.instance("lifecycle-loop", "endless"))
            assert len(head["runs"]) == 1
            head_bytes = len(json.dumps(head).encode())
            max_head_bytes = max(max_head_bytes, head_bytes)
            with closing(sqlite3.connect(ledger)) as conn:
                count, max_calls = conn.execute("SELECT count(*), max(calls) FROM effects").fetchone()
            assert count == generations * 3 and max_calls == 1
            assert len(server.producers) <= 32 and len(server.provisioned) <= 32
            if generations == 1 or generations % 10 == 0:
                sample = {
                    "generation": generations,
                    "elapsed": round(time.monotonic() - workload_started, 3),
                    "hot_bytes": head_bytes,
                    "producers": len(server.producers),
                    "routes": len(server.provisioned),
                }
                for label, store in zip(("workflow", "tasks"), stores, strict=True):
                    async with store.database._transaction() as conn:
                        for name in ("states", "records", "inbox", "outbox"):
                            sample[f"{label}_{name}"] = (
                                await conn.execute(select(func.count()).select_from(getattr(store, name)))
                            ).scalar_one()
                rss = await asyncio.to_thread(
                    subprocess.run,
                    ["ps", "-o", "rss=", "-p", str(os.getpid())],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                sample["rss_mib"] = round(int(rss.stdout.strip()) / 1024, 2)
                samples.append(sample)
                print(json.dumps(sample), flush=True)
        await handle.signal(channel, -1, signal_id="stop", signal_epoch=int(time.time() // DAY))
        assert await handle.result(timeout=60) == generations
        async with asyncio.timeout(60):
            while True:
                pending_count = 0
                for store in stores:
                    async with store.database._transaction() as conn:
                        pending_count += (
                            await conn.execute(select(func.count()).select_from(store.outbox))
                        ).scalar_one()
                if not pending_count:
                    break
                await asyncio.sleep(0.05)
        return {
            "status": "passed",
            "requested_seconds": seconds,
            "elapsed_seconds": round(time.monotonic() - began, 3),
            "generations": generations,
            "external_effects": generations * 3,
            "role_loop_restarts": restarts,
            "max_hot_bytes": max_head_bytes,
            "outbox_drained": True,
            "samples": list(samples),
        }
    finally:
        for _, stop, _ in running:
            stop.set()
        await asyncio.gather(*(pending for _, _, pending in running), return_exceptions=True)
        for runtime, _, _ in running:
            await runtime.close()
        await main_client.close()
        await transport.unsubscribe(topics.topic("business", "output"), "soak-output")
        await transport.close()
        await server.close()
        for store in stores:
            try:
                async with store.database._transaction() as conn:
                    await conn.execute(text(f'DROP SCHEMA IF EXISTS "{store.database.schema}" CASCADE'))
            finally:
                await store.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=3600)
    parser.add_argument("--output", type=Path, default=Path("lifecycle-soak-results.json"))
    args = parser.parse_args()
    if not 1 <= args.seconds <= 30 * DAY:
        parser.error("seconds must be in 1..2592000")
    with tempfile.TemporaryDirectory(prefix="duraflow-lifecycle-") as directory:
        result = asyncio.run(run(args.seconds, Path(directory)))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "samples"}, indent=2))


if __name__ == "__main__":
    main()
