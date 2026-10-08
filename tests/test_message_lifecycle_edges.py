"""Protocol-2 behavioral edge cases formerly qualified only through the legacy engine."""

import asyncio

import pytest

from duraflow import (
    ChannelRef,
    Conflict,
    Registry,
    RetryPolicy,
    TaskFailure,
    TaskOptions,
    TaskRef,
    WorkflowContext,
    WorkflowFailed,
    WorkflowRef,
    task,
    workflow,
)
from duraflow.testing import TestEnvironment
from tests.test_engine import double, sequence


@pytest.mark.parametrize("deadline", ["schedule_timeout", "overall_timeout", "attempt_timeout"])
async def test_task_deadlines_fence_late_results(deadline):
    ref = TaskRef("deadline-task", int, int)
    release = asyncio.Event()
    started = asyncio.Event()

    @task(ref=ref)
    async def slow(value):
        started.set()
        await release.wait()
        return value

    @workflow(name="deadline-flow", build_id="edges-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value, options=TaskOptions(**{deadline: 1}))

    async with TestEnvironment(Registry(flow, slow)) as env:
        if deadline == "schedule_timeout":
            env.worker.accepting = False
        handle = await env.client.start(flow, 3, request_id="deadline")
        await env.drain()
        if deadline != "schedule_timeout":
            await asyncio.wait_for(started.wait(), 1)
        env.clock.advance(1)
        await env.drain()
        with pytest.raises(WorkflowFailed):
            await handle.result(timeout=1)
        state = await handle.describe()
        assert state["status"] == "FAILED" and "TIMEOUT" in state["error"]["code"]
        env.worker.accepting = True
        release.set()
        await env.drain()
        after = await handle.describe()
        assert (after["status"], after["result"], after["error"]) == (state["status"], state["result"], state["error"])


@pytest.mark.parametrize("exhausted", ["raise", "block"])
async def test_retry_exhaustion_and_stable_effect_key(exhausted):
    ref = TaskRef("always-failing", int, int)
    attempts = []

    @task(ref=ref)
    async def failing(ctx, value):
        attempts.append((ctx.attempt, ctx.idempotency_key))
        raise ValueError("private payload must not escape")

    @workflow(name="exhausted-flow", build_id="edges-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value, options=TaskOptions(retry=RetryPolicy(max_attempts=3, exhausted=exhausted)))

    async with TestEnvironment(Registry(flow, failing)) as env:
        handle = await env.client.start(flow, 0, request_id="retry")
        for _ in range(3):
            await env.drain()
            env.clock.advance(10)
        await env.drain()
        assert [a for a, _ in attempts] == [1, 2, 3]
        assert len({key for _, key in attempts}) == 1
        state = await handle.describe()
        assert state["status"] == ("FAILED" if exhausted == "raise" else "BLOCKED")
        assert "private payload" not in str(state)


@pytest.mark.parametrize("operation", ["cancel", "terminate"])
async def test_parent_control_cancels_pending_child(operation):
    @workflow(name="waiting-child", build_id="edges-v1")
    async def child(ctx: WorkflowContext, value: int) -> int:
        await ctx.sleep(100)
        return value

    @workflow(name="waiting-parent", build_id="edges-v1")
    async def parent(ctx: WorkflowContext, value: int) -> int:
        return await ctx.child(WorkflowRef("waiting-child", int, int), value)

    async with TestEnvironment(Registry(parent, child)) as env:
        handle = await env.client.start(parent, 1, request_id="parent")
        await env.drain()
        await getattr(handle, operation)(actor="test", reason="requested", request_id="control")
        await env.drain()
        parent_state = await handle.describe()
        assert parent_state["status"] in {"CANCELLED", "TERMINATED"}
        children = await env.store.list_states('["test","waiting-child",')
        assert len(children) == 1
        state = children[0]["runs"][children[0]["current"]]
        assert state["status"] in {"CANCELLED", "TERMINATED"}
        env.clock.advance(100)
        await env.drain()
        assert (await handle.describe())["status"] == parent_state["status"]


async def test_catch_task_error_then_compensate():
    ref = TaskRef("compensated-failure", int, int)

    @task(ref=ref)
    def failing(value):
        raise ValueError("upstream")

    @workflow(name="compensate", build_id="edges-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        try:
            return await ctx.call(ref, value)
        except TaskFailure:
            return await ctx.call(TaskRef("double", int, int), -value)

    async with TestEnvironment(Registry(flow, failing, double)) as env:
        handle = await env.client.start(flow, 7, request_id="compensate")
        assert await env.run(handle) == -14


async def test_result_waiter_cancellation_removes_remote_subscription():
    channel = ChannelRef("wait", bool)

    @workflow(name="waiter-cancel", build_id="edges-v1")
    async def flow(ctx: WorkflowContext, value: int) -> bool:
        return await ctx.channel(channel).receive(max_signals=1).next()

    async with TestEnvironment(Registry(flow)) as env:
        handle = await env.client.start(flow, 0, request_id="waiter")
        waiting = asyncio.create_task(handle.result(timeout=100))
        await env.drain()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        await env.drain()
        aggregate = await env.store.read(env.topics.instance("waiter-cancel", "waiter"))
        assert not aggregate["waiters"]
        await handle.signal(channel, True, signal_id="ready")
        assert await env.run(handle) is True


async def test_namespace_isolation_for_identical_request_ids():
    async with (
        TestEnvironment(Registry(sequence, double), namespace="one") as first,
        TestEnvironment(Registry(sequence, double), namespace="two") as second,
    ):
        handles = await asyncio.gather(
            first.client.start(sequence, 2, request_id="same"), second.client.start(sequence, 9, request_id="same")
        )
        assert await asyncio.gather(first.run(handles[0]), second.run(handles[1])) == [9, 37]
        assert first.topics.instance("sequence", "same") != second.topics.instance("sequence", "same")


@pytest.mark.parametrize("reason", ["expired", "cancelled", "forged"])
async def test_delegation_rejects_invalid_completion(reason):
    import base64
    import json

    ref = TaskRef("external", int, int)
    tokens = []

    @task(ref=ref)
    async def external(ctx, value):
        marker = await ctx.defer(timeout=2)
        tokens.append(marker.token)
        return marker

    @workflow(name="external-flow", build_id="edges-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value)

    async with TestEnvironment(Registry(flow, external)) as env:
        handle = await env.client.start(flow, 1, request_id="external")
        await env.drain()
        token = tokens[0]
        if reason == "expired":
            env.clock.advance(2)
        elif reason == "cancelled":
            await handle.cancel(actor="test", reason="stop", request_id="cancel")
        else:
            body = json.loads(base64.urlsafe_b64decode(token))
            body["secret"] = "forged"
            token = base64.urlsafe_b64encode(json.dumps(body).encode()).decode()
        await env.drain()
        with pytest.raises(Conflict):
            await env.client.complete_external(token, 42, ref=ref)
