from __future__ import annotations

import asyncio
from contextlib import closing
import itertools
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from duraflow import (
    Conflict,
    Deferred,
    LegacyEngine as Engine,
    Registry,
    RetryPolicy,
    TaskContext,
    TaskOptions,
    TaskRef,
    LegacyWorker as Worker,
    WorkflowContext,
    task,
    workflow,
)
from duraflow.contracts import ProtocolError, canonical
from duraflow.state import route, subscription
from duraflow.testing import LegacyTestEnvironment as TestEnvironment
from duraflow.transport import Delivery
from .test_engine import A, B, C, DOUBLE, HA, HB, HC, VALUES, a, b, c, double, sequence, parallel, broadcast, bindings


async def dispatch_first(env: TestEnvironment) -> Delivery:
    await env.engine.tick()
    await env.engine.tick()
    delivery = await env.transport.receive(route(env.namespace, DOUBLE.descriptor()), "workers")
    assert delivery is not None
    return delivery


async def test_live_duplicate_lease_and_stale_owner_fencing() -> None:
    async with TestEnvironment(Registry(sequence, double)) as env:
        h = await env.client.start(sequence, 1, request_id="lease")
        delivery = await dispatch_first(env)
        old = await env.worker._claim(delivery, DOUBLE)
        assert old is not None
        assert await env.worker._claim(delivery, DOUBLE) is None
        env.clock.advance(31)
        new = await env.worker._claim(delivery, DOUBLE)
        assert new is not None and new.lease_epoch == old.lease_epoch + 1
        assert new.attempt == old.attempt == 1
        assert new.idempotency_key == old.idempotency_key
        await env.worker._observe(old, 999, None)
        assert env.worker.metrics["stale_results"] == 1
        await env.worker._observe(new, 2, None)
        await env.engine.tick()
        assert (await h.describe())["commands"][0]["result"] == 2
        assert (await h.describe())["nodes"]["0.0"]["stale_observations"]


async def test_observation_survives_before_ack_crash() -> None:
    async with TestEnvironment(Registry(sequence, double)) as env:
        h = await env.client.start(sequence, 1, request_id="ack-crash")
        delivery = await dispatch_first(env)
        context = await env.worker._claim(delivery, DOUBLE)
        assert context is not None
        await env.worker._execute(context, DOUBLE)
        env.transport.redeliver_unacked()
        replacement = Worker(env.store, env.transport, Registry(double), namespace=env.namespace, clock=env.clock)
        await replacement.prepare()
        assert await replacement.step()
        assert replacement.metrics["executed"] == 0
        await replacement.close()
        assert await env.run(h) == 5


async def test_publish_then_mark_lost_republishes_same_identity() -> None:
    class CommitLoss(Exception):
        pass

    async with TestEnvironment(Registry(sequence, double)) as env:
        h = await env.client.start(sequence, 1, request_id="outbox-crash")
        await env.engine.advance(h.run_id)
        await env.engine.advance(h.run_id)
        original_publish, original_save = env.transport.publish, env.store.save
        armed = False

        async def publish(topic: str, data: bytes, properties: dict[str, str]) -> None:
            nonlocal armed
            await original_publish(topic, data, properties)
            if json.loads(properties["duraflow"])["kind"] == "task":
                armed = True

        async def save(state: dict[str, Any], revision: int) -> bool:
            nonlocal armed
            if armed:
                armed = False
                raise CommitLoss()
            return await original_save(state, revision)

        env.transport.publish, env.store.save = publish, save
        with pytest.raises(CommitLoss):
            await env.engine.flush(h.run_id)
        env.store.save, env.transport.publish = original_save, original_publish
        env.clock.advance(31)
        await env.engine.flush(h.run_id)
        messages = [m for m in env.transport.publications if m[0] == route(env.namespace, DOUBLE.descriptor())]
        assert len(messages) == 2 and messages[0] == messages[1]
        assert await env.run(h) == 5
        assert env.worker.metrics["executed"] == 2


async def test_broker_failure_preserves_outbox() -> None:
    async with TestEnvironment(Registry(sequence, double)) as env:
        h = await env.client.start(sequence, 2, request_id="broker")
        original = env.transport.publish

        async def broken(*args: Any, **kwargs: Any) -> None:
            raise ConnectionError("offline")

        env.transport.publish = broken
        with pytest.raises(ConnectionError):
            await env.engine.tick()
        assert any(not item["delivered"] for item in (await h.describe())["outbox"].values())
        env.transport.publish = original
        env.clock.advance(1)
        assert await env.run(h) == 9


@pytest.mark.parametrize("forgery", ["payload", "event", "subscription"])
async def test_conflicting_message_is_quarantined(forgery: str) -> None:
    async with TestEnvironment(Registry(sequence, double)) as env:
        h = await env.client.start(sequence, 2, request_id="poison")
        delivery = await dispatch_first(env)
        await env.worker.process(delivery, DOUBLE)
        meta = json.loads(delivery.properties["duraflow"])
        if forgery == "event":
            meta["event_id"] = "not-committed"
        forged = Delivery(
            delivery.topic,
            "wrong" if forgery == "subscription" else delivery.subscription,
            b"999" if forgery == "payload" else delivery.data,
            {"duraflow": canonical(meta)},
            999,
        )
        await env.worker.process(forged, DOUBLE)
        assert env.worker.metrics["quarantined"] == 1
        assert any(m[0].endswith("-dlq") for m in env.transport.publications)
        assert await env.run(h) == 9


@pytest.mark.parametrize("ordering", list(itertools.permutations(range(3))))
async def test_all_broadcast_completion_orders(ordering: tuple[int, ...]) -> None:
    async with TestEnvironment(Registry(broadcast, a, b, c), broadcasts=bindings()) as env:
        h = await env.client.start(broadcast, 1, request_id="permutation")
        await env.engine.tick()
        for index in ordering:
            ref, handler = ((A, HA), (B, HB), (C, HC))[index]
            delivery = await env.transport.receive("values", subscription(env.namespace, handler.subscription))
            assert delivery is not None
            await env.worker.process(delivery, ref)
            await env.worker.process(delivery, ref)
            await env.engine.tick()
        await env.drain()
        assert (await h.describe())["status"] == "COMPLETED"
        assert env.worker.metrics["executed"] == 3


async def test_only_failed_handler_retries_on_direct_route() -> None:
    calls = {"b": 0}

    @task(ref=B)
    def flaky(value: int) -> int:
        calls["b"] += 1
        if calls["b"] == 1:
            raise ValueError("temporary")
        return value + 2

    @workflow(name="broadcast-retry", build_id="test")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        results = await ctx.broadcast(
            VALUES, value, handlers=(HA, HB, HC), options=TaskOptions(retry=RetryPolicy(max_attempts=2))
        )
        return results[HA] + results[HB] + results[HC]

    async with TestEnvironment(Registry(flow, a, flaky, c), broadcasts=bindings()) as env:
        h = await env.client.start(flow, 1, request_id="broadcast-retry")
        await env.drain()
        env.clock.advance(1)
        assert await env.run(h) == 9
        assert calls["b"] == 2
        assert len([m for m in env.transport.publications if m[0] == "values"]) == 1
        assert len([m for m in env.transport.publications if m[0] == route(env.namespace, B.descriptor())]) == 1
        assert not [m for m in env.transport.publications if m[0] == route(env.namespace, A.descriptor())]


@pytest.mark.parametrize("which", ["attempt", "overall", "schedule"])
async def test_deadlines_and_late_completion(which: str) -> None:
    options = TaskOptions(**{f"{which}_timeout": 5})

    @workflow(name="deadline", build_id="test")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(DOUBLE, value, options=options)

    async with TestEnvironment(Registry(flow, double)) as env:
        h = await env.client.start(flow, 1, request_id="deadline")
        delivery = await dispatch_first(env)
        context = None if which == "schedule" else await env.worker._claim(delivery, DOUBLE)
        env.clock.advance(5)
        await env.engine.tick()
        assert (await h.describe())["status"] == "FAILED"
        assert (await h.describe())["error"]["code"] == f"{which.upper()}_TIMEOUT"
        if context is not None:
            await env.worker._observe(context, 200, None)
        assert (await h.describe())["status"] == "FAILED"


async def test_two_coordinators_commit_one_command() -> None:
    async with TestEnvironment(Registry(parallel, double)) as env:
        h = await env.client.start(parallel, 2, request_id="race-engines")
        second = Engine(env.store, env.transport, env.engine.registry, namespace=env.namespace, clock=env.clock)
        for _ in range(5):
            await asyncio.gather(env.engine.tick(), second.tick())
        assert len((await h.describe())["commands"]) == 1
        assert len((await h.describe())["nodes"]) == 2
        assert await env.run(h) == [4, 6]


DELEGATED = TaskRef("delegated", int, int)


async def test_delegation_token_validation_and_idempotency() -> None:
    tokens = []

    @task(ref=DELEGATED)
    async def external(ctx: TaskContext, value: int) -> Deferred:
        deferred = await ctx.defer(timeout=60)
        tokens.append(deferred.token)
        return deferred

    @workflow(name="external", build_id="test")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(DELEGATED, value)

    async with TestEnvironment(Registry(flow, external)) as env:
        h = await env.client.start(flow, 1, request_id="external")
        await env.drain()
        assert (await h.describe())["status"] == "WAITING"
        assert tokens[0] not in canonical(await h.describe())
        with pytest.raises(ProtocolError):
            await env.client.complete_external(tokens[0], "not-an-integer", ref=DELEGATED)
        assert await env.client.complete_external(tokens[0], 22, ref=DELEGATED)
        assert not await env.client.complete_external(tokens[0], 22, ref=DELEGATED)
        with pytest.raises(Conflict):
            await env.client.complete_external(tokens[0], 23, ref=DELEGATED)
        assert await env.run(h) == 22


async def test_external_completion_before_task_returns() -> None:
    async with TestEnvironment(Registry()) as env:

        @task(ref=DELEGATED)
        async def external(ctx: TaskContext, value: int) -> Deferred:
            deferred = await ctx.defer(timeout=60)
            await env.client.complete_external(deferred.token, 7, ref=DELEGATED)
            return deferred

        @workflow(name="early-external", build_id="test")
        async def flow(ctx: WorkflowContext, value: int) -> int:
            return await ctx.call(DELEGATED, value)

        env.engine.registry.register(flow)
        env.engine.registry.register(external)
        env.worker.prepared = False
        h = await env.client.start(flow, 1, request_id="early-external")
        assert await env.run(h) == 7


async def test_async_cooperative_cancellation() -> None:
    started = asyncio.Event()

    @task(ref=DOUBLE)
    async def long_task(ctx: TaskContext, value: int) -> int:
        started.set()
        await asyncio.sleep(30)
        return value * 2

    async with TestEnvironment(Registry(sequence, long_task)) as env:
        env.worker.lease_seconds = 0.06
        h = await env.client.start(sequence, 1, request_id="async-cancel")
        delivery = await dispatch_first(env)
        running = asyncio.create_task(env.worker.process(delivery, DOUBLE))
        await started.wait()
        await h.cancel(actor="test", reason="cancel", request_id="cancel")
        await asyncio.wait_for(running, timeout=1)
        await env.engine.tick()
        assert (await h.describe())["status"] == "CANCELLED"


async def test_sync_cancellation_waits_for_function_exit() -> None:
    import threading

    started, release = threading.Event(), threading.Event()

    @task(ref=DOUBLE)
    def long_task(ctx: TaskContext, value: int) -> int:
        started.set()
        release.wait(3)
        return value * 2

    async with TestEnvironment(Registry(sequence, long_task)) as env:
        h = await env.client.start(sequence, 1, request_id="sync-cancel")
        delivery = await dispatch_first(env)
        running = asyncio.create_task(env.worker.process(delivery, DOUBLE))
        await asyncio.to_thread(started.wait, 1)
        await h.cancel(actor="test", reason="cancel", request_id="cancel")
        await env.engine.tick()
        assert (await h.describe())["status"] == "CANCELLING"
        release.set()
        await running
        await env.engine.tick()
        assert (await h.describe())["status"] == "CANCELLED"


def test_kill_then_restore_two_of_three_handlers(tmp_path: Path) -> None:
    database, output = tmp_path / "recovery.db", tmp_path / "result.json"
    script = Path(__file__).with_name("recovery_process.py")
    environment = {**os.environ, "PYTHONPATH": f"{Path.cwd() / 'src'}:{Path.cwd()}"}
    first = subprocess.run(
        [sys.executable, str(script), "crash", str(database), str(output)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert first.returncode == 23, first.stderr
    restored = tmp_path / "restored.db"
    with closing(sqlite3.connect(database)) as source, closing(sqlite3.connect(restored)) as target:
        source.backup(target)
    second = subprocess.run(
        [sys.executable, str(script), "resume", str(restored), str(output)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert second.returncode == 0, second.stderr
    result = json.loads(output.read_text())
    assert result == {"status": "COMPLETED", "resumed_task_executions": 1, "final_publications": 1}


def test_fingerprint_is_independent_of_python_hash_seed() -> None:
    code = "from duraflow.contracts import fingerprint; print(fingerprint({'b':2,'a':1}))"
    values = []
    for seed in ("1", "193", "random"):
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": seed, "PYTHONPATH": str(Path.cwd() / "src")},
        )
        values.append(result.stdout)
    assert len(set(values)) == 1
