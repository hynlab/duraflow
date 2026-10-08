"""Behavioral regressions for replay, fencing, dispatch and ownership boundaries."""

from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from duraflow import (
    BroadcastBinding,
    Conflict,
    Deferred,
    Engine,
    HandlerRef,
    Registry,
    TaskCancelled,
    TaskFailure,
    TaskRef,
    TopicRef,
    Worker,
    WorkflowBlocked,
    WorkflowContext,
    WorkflowFailed,
    WorkflowRef,
    task,
    workflow,
)
from duraflow.contracts import (
    MAX_PAYLOAD_BYTES,
    NonDeterminism,
    ProtocolError,
    UnsupportedWorkflow,
    canonical,
    fingerprint,
)
from duraflow.replay import replay
from duraflow.state import mutate, outbox, route, subscription
from duraflow.testing import TestEnvironment
from duraflow.transport import PulsarTransport
from tests.test_engine import DOUBLE, double, sequence
from tests.test_recovery import dispatch_first


async def task_delivery(env, ref):
    for _ in range(4):
        await env.engine.tick()
        delivery = await env.transport.receive(route(env.namespace, ref.descriptor()), "workers")
        if delivery is not None:
            return delivery
    raise AssertionError("Registered task did not reach durable dispatch")


@pytest.mark.parametrize(
    "case", ["empty", "duplicate", "foreign", "nested", "continue", "handlers", "contract", "reused"]
)
async def test_invalid_orchestration_is_rejected_before_dispatch(case):
    @workflow(name="invalid-operations", build_id="regression")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        operation = ctx.call(DOUBLE, value)
        if case == "empty":
            await ctx.gather()
        elif case == "duplicate":
            await ctx.gather(operation, operation)
        elif case == "foreign":
            await ctx.gather(WorkflowContext().call(DOUBLE, value))
        elif case == "nested":
            await ctx.gather(ctx.gather(operation))
        elif case == "continue":
            await ctx.gather(ctx.continue_as_new(value))
        elif case == "handlers":
            handler = HandlerRef(DOUBLE, "same")
            await ctx.broadcast(TopicRef("invalid-topic", int), value, handlers=(handler, handler))
        elif case == "contract":
            await ctx.broadcast(
                TopicRef("invalid-topic", int), value, handlers=(HandlerRef(TaskRef("wrong", str, int), "wrong"),)
            )
        else:
            await operation
            await operation
        return 0

    async with TestEnvironment(Registry(flow, double)) as env:
        handle = await env.client.start(flow, 7, request_id=case)
        await env.drain()
        error = WorkflowFailed if case in {"empty", "duplicate", "handlers", "contract"} else WorkflowBlocked
        with pytest.raises(error):
            await handle.result(timeout=1)
        assert env.worker.metrics["executed"] == (1 if case == "reused" else 0)


async def test_replay_refuses_missing_suffix_and_enforces_operation_budget():
    @workflow(name="bounded-history", build_id="regression")
    async def original(ctx: WorkflowContext, value: int) -> int:
        for _ in range(value):
            await ctx.uuid()
        return value

    async with TestEnvironment(Registry(original)) as env:
        handle = await env.client.start(original, 3, request_id="history")
        assert await env.run(handle) == 3
        state = await handle.describe()
        definition = env.client.registry.resolve(original)
        with pytest.raises(UnsupportedWorkflow, match="budget"):
            replay(definition, state, max_steps=1)
        changed = {**state, "manifest": {**state["manifest"], "build_id": "different"}}
        with pytest.raises(NonDeterminism):
            replay(definition, changed)
        for mode in ("return", "task-failure", "exception"):

            @workflow(name="bounded-history", build_id="regression")
            async def replacement(ctx: WorkflowContext, value: int) -> int:
                if mode == "task-failure":
                    raise TaskFailure({"code": "TEST", "message": "expected"})
                if mode == "exception":
                    raise ValueError("expected")
                return value

            with pytest.raises(NonDeterminism):
                replay(Registry(replacement).resolve(replacement), state)


async def test_outbox_identity_collision_cannot_replace_a_committed_message():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="outbox")
        state = await handle.describe()
        event_id = outbox(state, "fixed", "publication", "topic", {"value": 1}, env.clock.now())
        original = canonical(state["outbox"][event_id])
        assert outbox(state, "fixed", "publication", "topic", {"value": 1}, env.clock.now()) == event_id
        with pytest.raises(Conflict):
            outbox(state, "fixed", "publication", "topic", {"value": 2}, env.clock.now())
        assert canonical(state["outbox"][event_id]) == original


@pytest.mark.parametrize("exhaust", [False, True])
async def test_cas_retry_reloads_state_without_duplicating_a_mutation(exhaust):
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="cas")
        save, calls = env.store.save, 0

        async def contested(state, revision):
            nonlocal calls
            calls += 1
            if exhaust or calls <= 2:
                return False
            return await save(state, revision)

        env.store.save = contested

        def add_tag(state):
            state["tags"].append("once")

        if exhaust:
            with pytest.raises(Conflict, match="retry budget"):
                await mutate(env.store, env.namespace, handle.run_id, add_tag)
            assert calls == 64 and (await handle.describe())["tags"] == []
        else:
            await mutate(env.store, env.namespace, handle.run_id, add_tag)
            assert calls == 3 and (await handle.describe())["tags"] == ["once"]


@pytest.mark.parametrize(
    "kwargs", [{"batch_size": 0}, {"batch_size": 1001}, {"max_commands": 0}, {"max_commands": 10001}]
)
async def test_invalid_engine_limits_fail_before_start(kwargs):
    async with TestEnvironment(Registry(sequence, double)) as env:
        with pytest.raises(ValueError):
            Engine(env.store, env.transport, env.client.registry, **kwargs)


@pytest.mark.parametrize("group", [False, True])
async def test_orchestration_quotas_prevent_excess_business_dispatch(group):
    @workflow(name="command-quota", build_id="regression")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        if group:
            await ctx.gather(*(ctx.call(DOUBLE, i) for i in range(1001)))
            return 0
        first = await ctx.call(DOUBLE, value)
        return await ctx.call(DOUBLE, first)

    async with TestEnvironment(Registry(flow, double)) as env:
        if not group:
            env.engine.max_commands = 1
        handle = await env.client.start(flow, 1, request_id="quota")
        await env.drain()
        with pytest.raises(WorkflowBlocked):
            await handle.result(timeout=1)
        assert env.worker.metrics["executed"] == (0 if group else 1)


async def test_failed_outbox_route_does_not_hold_other_ready_publications():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="routes")

        def enqueue(state):
            outbox(state, "bad", "publication", "unavailable", 1, env.clock.now())
            outbox(state, "good", "publication", "available", 2, env.clock.now())

        await mutate(env.store, env.namespace, handle.run_id, enqueue)
        publish = env.transport.publish

        async def one_bad_route(topic, *args):
            if topic == "unavailable":
                raise ConnectionError("expected")
            await publish(topic, *args)

        env.transport.publish = one_bad_route
        with pytest.raises(ConnectionError):
            await env.engine.flush(handle.run_id)
        assert any(message[0] == "available" for message in env.transport.publications)
        state = await handle.describe()
        failed = next(item for item in state["outbox"].values() if item["topic"] == "unavailable")
        assert not failed["delivered"] and failed["next_attempt_at"] > env.clock.now()
        env.transport.publish = publish
        await env.engine.flush(handle.run_id)
        assert not any(message[0] == "unavailable" for message in env.transport.publications)
        env.clock.advance(2)
        await env.engine.flush(handle.run_id)
        assert len([message for message in env.transport.publications if message[0] == "unavailable"]) == 1


async def test_cancel_suppresses_unpublished_business_work():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="cancel-before-publish")
        await env.engine.advance(handle.run_id)
        await env.engine.advance(handle.run_id)
        await handle.cancel(actor="test", reason="expected", request_id="cancel")
        await env.engine.flush(handle.run_id)
        state = await handle.describe()
        assert any(item.get("suppressed") for item in state["outbox"].values())
        assert not any(
            message[0] == route(env.namespace, DOUBLE.descriptor()) for message in env.transport.publications
        )


async def test_native_timeout_retains_capacity_until_physical_completion():
    transport = object.__new__(PulsarTransport)
    transport.native_pool = ThreadPoolExecutor(max_workers=1)
    transport.native_slots = asyncio.Semaphore(1)
    transport.operation_timeout, transport._closed = 0.03, False
    started, release = threading.Event(), threading.Event()

    def blocked():
        started.set()
        release.wait(5)

    try:
        with pytest.raises(TimeoutError):
            await transport._native(blocked)
        assert started.is_set()
        with pytest.raises(TimeoutError):
            await transport._native(lambda: 42)
        release.set()
        await asyncio.sleep(0.02)
        assert await transport._native(lambda: 42) == 42
        transport._closed = True
        with pytest.raises(RuntimeError, match="closed"):
            await transport._native(lambda: 43)
    finally:
        release.set()
        transport.native_pool.shutdown(wait=True)


@pytest.mark.parametrize(
    "case", ["version", "namespace", "kind", "identity", "attempt", "codec", "inbox", "payload-limit", "metadata-limit"]
)
async def test_invalid_delivery_is_quarantined_without_business_execution(case):
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 7, request_id=case)
        delivery = await dispatch_first(env)
        meta = json.loads(delivery.properties["duraflow"])
        if case == "version":
            meta["v"] = 999
        elif case == "namespace":
            meta["namespace"] = "different"
        elif case == "kind":
            meta["kind"] = "wake"
        elif case == "identity":
            meta["run_id"] = 123
        elif case == "attempt":
            meta["attempt"] = True
        elif case in {"codec", "inbox"}:

            def corrupt(state):
                if case == "codec":
                    state["codec_version"] = 999
                else:
                    state["inbox"][delivery.subscription + "/" + meta["event_id"]] = "conflict"

            await mutate(env.store, env.namespace, handle.run_id, corrupt)
        elif case == "payload-limit":
            delivery.data = b"x" * (MAX_PAYLOAD_BYTES + 1)
        delivery.properties["duraflow"] = canonical(meta) if case != "metadata-limit" else "x" * (MAX_PAYLOAD_BYTES + 1)
        await env.worker.process(delivery, DOUBLE)
        assert env.worker.metrics["executed"] == 0 and env.worker.metrics["quarantined"] == 1
        assert (await handle.describe())["nodes"]["0.0"]["attempts"][-1]["observation"] is None


@pytest.mark.parametrize("kwargs", [{"concurrency": 0}, {"concurrency": 257}])
async def test_worker_capacity_is_validated(kwargs):
    async with TestEnvironment(Registry(sequence, double)) as env:
        with pytest.raises(ValueError):
            Worker(env.store, env.transport, env.client.registry, **kwargs)


@pytest.mark.parametrize("duplicate", [False, True])
async def test_worker_refuses_incompatible_or_duplicate_broadcast_bindings(duplicate):
    async with TestEnvironment(Registry(double)) as env:
        binding = BroadcastBinding(
            TopicRef("binding", int), HandlerRef(DOUBLE if duplicate else TaskRef("missing", int, int), "handler")
        )
        worker = Worker(
            env.store, env.transport, env.client.registry, broadcasts=(binding, binding) if duplicate else (binding,)
        )
        try:
            with pytest.raises(Conflict if duplicate else ProtocolError):
                await worker.prepare()
        finally:
            await worker.close()


async def test_archived_delivery_is_acked_without_reexecution():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="archive-delivery")
        delivery = await dispatch_first(env)
        await env.worker.process(delivery, DOUBLE)
        assert await env.run(handle) == 5
        env.clock.advance(10)
        await handle.archive(actor="test", reason="expected", retention=1, safety_horizon=1)
        await env.worker.process(delivery, DOUBLE)
        assert env.worker.metrics["executed"] == 2


async def test_heartbeat_progress_and_expired_owner_cancellation():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="heartbeat")
        context = await env.worker._claim(await dispatch_first(env), DOUBLE)
        assert context is not None and await context.heartbeat(25)
        state = await handle.describe()
        assert state["nodes"]["0.0"]["attempts"][-1]["progress"] == 25
        with pytest.raises(ValueError):
            await context.heartbeat(101)
        context.check_cancelled()
        env.clock.advance(31)
        assert not await context.heartbeat()
        with pytest.raises(TaskCancelled):
            context.check_cancelled()
        with pytest.raises(Conflict):
            await context.defer(timeout=10)


async def test_delegation_is_idempotent_but_forbidden_after_cancellation():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="defer")
        context = await env.worker._claim(await dispatch_first(env), DOUBLE)
        assert context is not None
        deferred = await context.defer(timeout=10)
        assert await context.defer(timeout=10) is deferred
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="cancel-defer")
        context = await env.worker._claim(await dispatch_first(env), DOUBLE)
        assert context is not None
        await handle.cancel(actor="test", reason="expected", request_id="cancel")
        with pytest.raises(Conflict):
            await context.defer(timeout=10)


@pytest.mark.parametrize("bad", ["forged-marker", "missing-marker", "invalid-output", "task-cancelled"])
async def test_worker_records_contract_and_cancellation_errors(bad):
    ref = TaskRef("invalid-result", int, int)

    @task(ref=ref)
    async def implementation(ctx, value):
        if bad == "forged-marker":
            return Deferred("forged")
        if bad == "missing-marker":
            await ctx.defer(timeout=10)
            return value
        if bad == "task-cancelled":
            raise TaskCancelled("expected")
        return "wrong-type"

    @workflow(name="invalid-result-flow", build_id="regression")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value)

    async with TestEnvironment(Registry(flow, implementation)) as env:
        handle = await env.client.start(flow, 1, request_id=bad)
        await env.drain()
        with pytest.raises(WorkflowFailed):
            await handle.result(timeout=1)
        state = await handle.describe()
        assert state["error"]["code"] == ("CANCELLED" if bad == "task-cancelled" else "INVALID_RESULT")


async def test_late_or_repeated_observation_preserves_a_committed_result():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="observation")
        context = await env.worker._claim(await dispatch_first(env), DOUBLE)
        assert context is not None
        await env.worker._observe(context, 2, None)
        before = fingerprint((await handle.describe())["nodes"]["0.0"]["attempts"][-1]["observation"])
        await env.worker._observe(context, 2, None)
        assert fingerprint((await handle.describe())["nodes"]["0.0"]["attempts"][-1]["observation"]) == before
        await env.worker._observe(context, 999, None)
        assert env.worker.metrics["stale_results"] == 1
        await env.worker._observe(context, 999, None)
        assert len((await handle.describe())["nodes"]["0.0"]["stale_observations"]) == 1


@pytest.mark.parametrize("case", ["failed", "blocked", "rollover", "mismatched-contract"])
async def test_child_outcomes_and_pinned_contracts_survive_parent_replay(case):
    @workflow(name="regression-child", build_id="regression")
    async def child(ctx: WorkflowContext, value: int) -> int:
        if case == "failed":
            raise ValueError("expected")
        if case == "blocked":
            await ctx.gather(ctx.continue_as_new(value))
        if case == "rollover" and value > 0:
            await ctx.continue_as_new(value - 1)
        return 17

    @workflow(name="regression-parent", build_id="regression")
    async def parent(ctx: WorkflowContext, value: int) -> int:
        ref = WorkflowRef("regression-child", int, str if case == "mismatched-contract" else int)
        return await ctx.child(ref, value)

    async with TestEnvironment(Registry(parent, child)) as env:
        handle = await env.client.start(parent, 2, request_id=case)
        if case == "rollover":
            assert await env.run(handle) == 17
            rows = await env.client.list()
            assert len([row for row in rows if row["status"] == "CONTINUED"]) == 2
        else:
            await env.drain()
            with pytest.raises(WorkflowFailed if case == "failed" else WorkflowBlocked):
                await handle.result(timeout=1)


async def test_parent_termination_cancels_a_pending_child():
    @workflow(name="regression-open-child", build_id="regression")
    async def child(ctx: WorkflowContext, value: int) -> int:
        await ctx.sleep(100)
        return value

    @workflow(name="regression-open-parent", build_id="regression")
    async def parent(ctx: WorkflowContext, value: int) -> int:
        return await ctx.child(WorkflowRef("regression-open-child", int, int), value)

    async with TestEnvironment(Registry(parent, child)) as env:
        handle = await env.client.start(parent, 1, request_id="parent-close")
        await env.drain()
        await handle.terminate(actor="test", reason="expected", request_id="close")
        await env.drain()
        rows = await env.client.list()
        assert next(row for row in rows if row["manifest"]["name"] == "regression-open-child")["status"] == "CANCELLED"
        assert (await handle.describe())["status"] == "TERMINATED"


async def test_rollover_refuses_unresolved_race_losers():
    @workflow(name="regression-rollover", build_id="regression")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        await ctx.race(ctx.call(DOUBLE, value), ctx.sleep(100))
        await ctx.continue_as_new(value)
        return value

    async with TestEnvironment(Registry(flow, double)) as env:
        handle = await env.client.start(flow, 1, request_id="pending-rollover")
        await env.drain()
        with pytest.raises(WorkflowBlocked, match="outstanding"):
            await handle.result(timeout=1)
        assert len(await env.client.list()) == 1


async def test_termination_fences_pending_task_outcomes():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="termination")
        context = await env.worker._claim(await dispatch_first(env), DOUBLE)
        assert context is not None
        await handle.terminate(actor="test", reason="expected", request_id="terminate")
        await env.engine.advance(handle.run_id)
        state = await handle.describe()
        assert state["nodes"]["0.0"]["error"]["code"] == "TERMINATED"
        await env.worker._observe(context, 2, None)
        assert (await handle.describe())["status"] == "TERMINATED"


async def test_broadcast_delivery_cannot_execute_an_undeclared_participant():
    from tests.test_engine import broadcast, a, b, c, bindings, VALUES, A

    async with TestEnvironment(Registry(broadcast, a, b, c), broadcasts=bindings()) as env:
        handle = await env.client.start(broadcast, 1, request_id="participant")
        await env.engine.tick()
        delivery = await env.transport.receive(VALUES.name, subscription(env.namespace, "price"))
        assert delivery is not None
        meta = json.loads(delivery.properties["duraflow"])
        meta["handlers"] = {}
        delivery.properties["duraflow"] = canonical(meta)
        await env.worker.process(delivery, A)
        assert env.worker.metrics["quarantined"] == 1 and env.worker.metrics["executed"] == 0
        assert (await handle.describe())["status"] == "WAITING"


async def test_closed_worker_nacks_new_delivery():
    async with TestEnvironment(Registry(sequence, double)) as env:
        await env.client.start(sequence, 1, request_id="drain")
        delivery = await dispatch_first(env)
        env.worker.draining = True
        assert not await env.worker.step()
        await env.worker.process(delivery, DOUBLE)
        assert env.worker.metrics["executed"] == 0
        assert await env.transport.receive(delivery.topic, delivery.subscription) is not None


async def test_engine_pages_wrap_without_starving_runs():
    @workflow(name="regression-page", build_id="regression")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return value

    async with TestEnvironment(Registry(flow)) as env:
        env.engine.batch_size = 1
        handles = [await env.client.start(flow, i, request_id=str(i)) for i in range(2)]
        for _ in range(4):
            await env.engine.tick()
        assert [await handle.result(timeout=1) for handle in handles] == [0, 1]


async def test_engine_pauses_admission_while_dependency_is_unready():
    async with TestEnvironment(Registry(sequence, double)) as env:
        await env.client.start(sequence, 1, request_id="unready")
        env.engine.accepting = False
        stop = asyncio.Event()
        running = asyncio.create_task(env.engine.run(stop, poll_interval=0.01))
        await asyncio.sleep(0.03)
        stop.set()
        await running
        assert env.engine.metrics["activations"] == 0 and env.transport.publications == []


@pytest.mark.parametrize("failure", [False, True])
async def test_stale_outbox_owner_cannot_override_a_new_committed_delivery(failure):
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="publication-owner")
        original = env.transport.publish
        replacement = Engine(env.store, env.transport, env.client.registry, namespace=env.namespace, clock=env.clock)
        first = True

        async def delayed(topic, data, properties):
            nonlocal first
            if first:
                first = False
                env.clock.advance(31)
                await replacement.flush(handle.run_id, limit=1)
                if failure:
                    raise ConnectionError("old owner lost its response")
            await original(topic, data, properties)

        env.transport.publish = delayed
        try:
            if failure:
                with pytest.raises(ConnectionError):
                    await env.engine.flush(handle.run_id, limit=1)
            else:
                await env.engine.flush(handle.run_id, limit=1)
            state = await handle.describe()
            entry = next(iter(state["outbox"].values()))
            assert entry["delivered"] and entry["attempts"] == 2 and entry["next_attempt_at"] == 0
            identities = {json.loads(message[2]["duraflow"])["event_id"] for message in env.transport.publications}
            assert len(identities) == 1
        finally:
            await replacement.close()


@pytest.mark.parametrize("case", ["identity-conflict", "continuation-cycle"])
async def test_conflicting_or_corrupt_child_identity_blocks_without_business_failure(case):
    @workflow(name="regression-conflict-child", build_id="regression")
    async def child(ctx: WorkflowContext, value: int) -> int:
        return value

    @workflow(name="regression-conflict-parent", build_id="regression")
    async def parent(ctx: WorkflowContext, value: int) -> int:
        return await ctx.child(WorkflowRef("regression-conflict-child", int, int), value)

    async with TestEnvironment(Registry(parent, child)) as env:
        handle = await env.client.start(parent, 1, request_id="parent")
        await env.engine.advance(handle.run_id)
        node = (await handle.describe())["nodes"]["0.0"]
        child_id = node["child_run_id"]
        if case == "identity-conflict":
            await env.client.start(child, 99, request_id=f"child/{handle.run_id}/0.0")
        else:
            load = env.store.load

            async def corrupted(namespace, run_id):
                state = await load(namespace, run_id)
                if run_id == child_id:
                    state["status"], state["continued_run_id"] = "CONTINUED", child_id
                return state

            env.store.load = corrupted
        await env.engine.advance(handle.run_id)
        with pytest.raises(WorkflowBlocked):
            await handle.result(timeout=1)
        assert (await handle.describe())["error"] is None


async def test_stale_completion_cannot_mutate_a_history_restored_before_dispatch():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="restore-before-command")
        restored = await handle.describe()
        context = await env.worker._claim(await dispatch_first(env), DOUBLE)
        assert context is not None
        env.store.runs[env.namespace, handle.run_id] = restored
        await env.worker._observe(context, 999, None)
        assert fingerprint(await handle.describe()) == fingerprint(restored)
        assert env.worker.metrics["stale_results"] == 1


async def test_expired_or_archived_owner_cannot_record_late_completion():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="expired-result")
        context = await env.worker._claim(await dispatch_first(env), DOUBLE)
        assert context is not None
        env.clock.advance(31)
        await env.worker._observe(context, 999, None)
        assert (await handle.describe())["nodes"]["0.0"]["attempts"][-1]["observation"] is None
        assert await env.run(handle) == 5
        env.clock.advance(10)
        await handle.archive(actor="test", reason="expected", retention=1, safety_horizon=1)
        await env.worker._observe(context, 999, None)
        assert (await handle.describe())["archived"]


@pytest.mark.parametrize("lose_connection", [False, True])
async def test_async_heartbeat_renews_or_cancels_execution_on_connection_loss(lose_connection):
    ref = TaskRef("heartbeat-result", int, int)
    ready, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    @task(ref=ref)
    async def implementation(ctx, value):
        ready.set()
        try:
            await release.wait()
            return value
        finally:
            cancelled.set()

    @workflow(name="heartbeat-flow", build_id="regression")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value)

    async with TestEnvironment(Registry(flow, implementation)) as env:
        env.worker.lease_seconds = 0.03
        handle = await env.client.start(flow, 7, request_id="heartbeat-flow")
        delivery = await task_delivery(env, ref)
        context = await env.worker._claim(delivery, ref)
        assert context is not None
        renew, renewals = context.heartbeat, 0
        renewed_twice = asyncio.Event()

        async def heartbeat(progress=None):
            nonlocal renewals
            renewals += 1
            if lose_connection:
                raise ConnectionError("expected")
            result = await renew(progress)
            if renewals >= 2:
                renewed_twice.set()
            return result

        context.heartbeat = heartbeat
        running = asyncio.create_task(env.worker._execute(context, ref))
        await asyncio.wait_for(ready.wait(), 1)
        if lose_connection:
            await asyncio.wait_for(cancelled.wait(), 1)
        else:
            await asyncio.wait_for(renewed_twice.wait(), 1)
            release.set()
        await asyncio.wait_for(running, 1)
        state = await handle.describe()
        observation = state["nodes"]["0.0"]["attempts"][-1]["observation"]
        if lose_connection:
            assert observation["error"]["code"] == "CANCELLED"
        else:
            assert observation["result"] == 7


@pytest.mark.parametrize("is_async", [False, True])
async def test_worker_process_cancellation_leaves_uncommitted_work_for_redelivery(is_async):
    ref = TaskRef("cancelled-process", int, int)
    started, release = threading.Event(), threading.Event()
    cancelled = asyncio.Event()

    async def asynchronous(value):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    def synchronous(value):
        started.set()
        release.wait(5)
        return value

    implementation = task(ref=ref)(asynchronous if is_async else synchronous)

    @workflow(name="cancelled-process-flow", build_id="regression")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value)

    async with TestEnvironment(Registry(flow, implementation)) as env:
        handle = await env.client.start(flow, 7, request_id="process-cancellation")
        delivery = await task_delivery(env, ref)
        running = asyncio.create_task(env.worker.process(delivery, ref))
        try:
            for _ in range(100):
                if started.is_set():
                    break
                await asyncio.sleep(0.005)
            assert started.is_set()
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
            if is_async:
                assert cancelled.is_set()
            assert (await handle.describe())["nodes"]["0.0"]["attempts"][-1]["observation"] is None
            assert await env.transport.receive(delivery.topic, delivery.subscription) is not None
        finally:
            release.set()


@pytest.mark.parametrize("role", ["engine", "worker"])
async def test_runtime_iteration_errors_are_logged_and_do_not_kill_the_service(role, caplog):
    async with TestEnvironment(Registry(sequence, double)) as env:
        runtime = env.engine if role == "engine" else env.worker
        stop = asyncio.Event()

        async def unavailable(*args, **kwargs):
            stop.set()
            raise ConnectionError("expected")

        if role == "engine":
            env.engine.tick = unavailable
        else:
            env.transport.receive = unavailable
        await runtime.run(stop, poll_interval=0.001)
        assert any(record.msg == role + "_iteration_failed" for record in caplog.records)


async def test_task_returning_after_cooperative_cancel_records_cancellation():
    ref = TaskRef("cooperative-result", int, int)
    ready, release = asyncio.Event(), asyncio.Event()

    @task(ref=ref)
    async def implementation(ctx, value):
        ready.set()
        await release.wait()
        await ctx.heartbeat()
        return value

    @workflow(name="cooperative-result-flow", build_id="regression")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value)

    async with TestEnvironment(Registry(flow, implementation)) as env:
        handle = await env.client.start(flow, 7, request_id="cooperative-cancel")
        delivery = await task_delivery(env, ref)
        running = asyncio.create_task(env.worker.process(delivery, ref))
        await asyncio.wait_for(ready.wait(), 1)
        await handle.cancel(actor="test", reason="expected", request_id="cancel")
        release.set()
        await asyncio.wait_for(running, 1)
        observation = (await handle.describe())["nodes"]["0.0"]["attempts"][-1]["observation"]
        assert observation["error"]["code"] == "CANCELLED" and observation["result"] is None


@pytest.mark.parametrize("length", [99, 100, 101])
async def test_continuation_reconciliation_accepts_the_exact_limit(length):
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="continuation-boundary")
        template = await handle.describe()
        chain = {}
        for index in range(length + 1):
            run_id = "continued-" + str(index)
            chain[run_id] = {
                **template,
                "run_id": run_id,
                "status": "CONTINUED" if index < length else "COMPLETED",
                "continued_run_id": "continued-" + str(index + 1) if index < length else None,
            }
        load = env.store.load

        async def load_chain(namespace, run_id):
            return chain[run_id] if run_id in chain else await load(namespace, run_id)

        env.store.load = load_chain
        if length > 100:
            with pytest.raises(WorkflowBlocked, match="reconciliation bound"):
                await env.engine._follow_continued(chain["continued-0"])
        else:
            resolved = await env.engine._follow_continued(chain["continued-0"])
            assert resolved["run_id"] == "continued-" + str(length) and resolved["status"] == "COMPLETED"
