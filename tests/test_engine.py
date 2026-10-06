from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from duraflow import (
    Archived, BroadcastBinding, Conflict, Engine, HandlerRef, MemoryStore,
    NonDeterminism, Registry, RetryPolicy, SignalRef, SQLiteStore, TaskFailure,
    TaskOptions, TaskRef, TopicRef, WorkflowBlocked, WorkflowContext, WorkflowFailed,
    WorkflowRef, task, workflow,
)
from duraflow.contracts import ProtocolError, canonical, decode, encode, fingerprint, parse_json
from duraflow.replay import replay
from duraflow.state import subscription
from duraflow.testing import TestEnvironment

DOUBLE = TaskRef("double", int, int)
APPROVAL = SignalRef("approval", bool)
VALUES = TopicRef("values", int)
OUTPUT = TopicRef("analyzed", int)


@task(ref=DOUBLE)
def double(value: int) -> int:
    return value * 2


@workflow(name="sequence", build_id="test-v1")
async def sequence(ctx: WorkflowContext, value: int) -> int:
    a = await ctx.call(DOUBLE, value)
    b = await ctx.call(DOUBLE, a)
    return b + 1


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
async def test_sequence_and_completed_replay(tmp_path: Any, backend: str) -> None:
    store = MemoryStore() if backend == "memory" else SQLiteStore(tmp_path / "engine.db")
    reg = Registry(sequence, double)
    async with TestEnvironment(reg, store=store) as env:
        h = await env.client.start(sequence, 3, request_id="one")
        assert await env.run(h) == 13
        before = len(env.transport.publications)
        assert replay(reg.resolve(sequence), await h.describe()).value == 13
        await env.engine.tick()
        assert len(env.transport.publications) == before
        assert env.worker.metrics["executed"] == 2


async def test_start_idempotency_and_conflict() -> None:
    async with TestEnvironment(Registry(sequence, double)) as env:
        a, b = await asyncio.gather(env.client.start(sequence, 3, request_id="same"),
                                    env.client.start(sequence, 3, request_id="same"))
        assert a.run_id == b.run_id
        with pytest.raises(Conflict):
            await env.client.start(sequence, 4, request_id="same")
        with pytest.raises(Conflict):
            await env.client.start(sequence, 3, request_id="other", workflow_id="same")


async def test_revision_cas_conflict() -> None:
    async with TestEnvironment(Registry(sequence)) as env:
        h = await env.client.start(sequence, 1, request_id="cas")
        a, b = await h.describe(), await h.describe()
        a["tags"], b["tags"] = ["a"], ["b"]
        assert await env.store.save(a, a["revision"])
        assert not await env.store.save(b, b["revision"])
        assert (await h.describe())["tags"] == ["a"]


@workflow(name="parallel", build_id="test-v1")
async def parallel(ctx: WorkflowContext, value: int) -> list[int]:
    return await ctx.gather(ctx.call(DOUBLE, value), ctx.call(DOUBLE, value + 1))


async def test_gather_distinct_invocations_and_order() -> None:
    async with TestEnvironment(Registry(parallel, double)) as env:
        h = await env.client.start(parallel, 4, request_id="parallel")
        assert await env.run(h) == [8, 10]
        assert len({n["task_id"] for n in (await h.describe())["nodes"].values()}) == 2


A, B, C = (TaskRef(label, int, int) for label in ("a", "b", "c"))
HA, HB, HC = HandlerRef(A, "price"), HandlerRef(B, "shipping"), HandlerRef(C, "demand")


@task(ref=A)
def a(value: int) -> int:
    return value + 1


@task(ref=B)
def b(value: int) -> int:
    return value + 2


@task(ref=C)
def c(value: int) -> int:
    return value + 3


@workflow(name="broadcast", build_id="test-v1")
async def broadcast(ctx: WorkflowContext, value: int) -> str:
    result = await ctx.broadcast(VALUES, value, handlers=(HA, HB, HC))
    return await ctx.publish(OUTPUT, result[HA] + result[HB] + result[HC])


def bindings() -> tuple[BroadcastBinding, ...]:
    return tuple(BroadcastBinding(VALUES, h) for h in (HA, HB, HC))


async def test_broadcast_duplicates_restart_and_next_publication() -> None:
    reg = Registry(broadcast, a, b, c)
    async with TestEnvironment(reg, broadcasts=bindings()) as env:
        h = await env.client.start(broadcast, 2, request_id="broadcast")
        await env.engine.tick()
        first = await env.transport.receive("values", subscription(env.namespace, "price"))
        second = await env.transport.receive("values", subscription(env.namespace, "shipping"))
        assert first is not None and second is not None
        for _ in range(3):
            await env.worker.process(first, A)
        await env.engine.tick()
        state = await h.describe()
        assert state["status"] == "WAITING"
        assert sum(n["state"] == "done" for n in state["nodes"].values()) == 1
        await env.worker.process(second, B)
        env.engine = Engine(env.store, env.transport, reg, namespace=env.namespace, clock=env.clock)
        await env.engine.tick()
        assert (await h.describe())["status"] == "WAITING"
        third = await env.transport.receive("values", subscription(env.namespace, "demand"))
        assert third is not None
        await env.worker.process(third, C)
        await env.drain()
        assert (await h.describe())["status"] == "COMPLETED"
        assert len([m for m in env.transport.publications if m[0] == "values"]) == 1
        assert [m[1] for m in env.transport.publications if m[0] == "analyzed"] == [b"12"]
        assert env.worker.metrics["executed"] == 3


@workflow(name="approval", build_id="test-v1")
async def approval(ctx: WorkflowContext, value: int) -> int:
    if await ctx.wait_signal(APPROVAL, timeout=10):
        await ctx.sleep(5)
        return value
    return 0


async def test_early_signal_deduplication_and_timer() -> None:
    async with TestEnvironment(Registry(approval)) as env:
        h = await env.client.start(approval, 7, request_id="approval")
        await h.signal(APPROVAL, True, signal_id="first")
        await h.signal(APPROVAL, True, signal_id="first")
        with pytest.raises(Conflict):
            await h.signal(APPROVAL, False, signal_id="first")
        await env.drain()
        env.clock.advance(4.9)
        await env.drain()
        assert (await h.describe())["status"] == "WAITING"
        env.clock.advance(0.1)
        assert await env.run(h) == 7
        assert len((await h.describe())["signals"]) == 1


async def test_signal_timeout() -> None:
    async with TestEnvironment(Registry(approval)) as env:
        h = await env.client.start(approval, 7, request_id="timeout")
        await env.drain()
        env.clock.advance(10)
        await env.drain()
        with pytest.raises(WorkflowFailed):
            await h.result()
        assert (await h.describe())["error"]["code"] == "SIGNAL_TIMEOUT"


FAIL = TaskRef("fail", int, int)


@task(ref=FAIL)
def fail(value: int) -> int:
    raise RuntimeError("SECRET_DO_NOT_LOG")


@workflow(name="retry", build_id="test-v1")
async def retrying(ctx: WorkflowContext, value: int) -> int:
    return await ctx.call(FAIL, value, options=TaskOptions(retry=RetryPolicy(max_attempts=2)))


async def test_retry_does_not_leak_exception_message() -> None:
    async with TestEnvironment(Registry(retrying, fail)) as env:
        h = await env.client.start(retrying, 1, request_id="retry")
        await env.drain()
        assert env.worker.metrics["executed"] == 1
        env.clock.advance(1)
        await env.drain()
        state = await h.describe()
        assert state["status"] == "FAILED"
        assert len(state["nodes"]["0.0"]["attempts"]) == 2
        assert "SECRET_DO_NOT_LOG" not in canonical(state)


@workflow(name="catch", build_id="test-v1")
async def catches(ctx: WorkflowContext, value: int) -> int:
    try:
        return await ctx.call(FAIL, value)
    except TaskFailure:
        return await ctx.call(DOUBLE, value)


async def test_error_replay_and_compensation() -> None:
    async with TestEnvironment(Registry(catches, fail, double)) as env:
        h = await env.client.start(catches, 5, request_id="catch")
        assert await env.run(h) == 10
        assert env.worker.metrics["executed"] == 2


@workflow(name="blocked", build_id="test-v1")
async def blocked(ctx: WorkflowContext, value: int) -> int:
    return await ctx.call(FAIL, value, options=TaskOptions(retry=RetryPolicy(exhausted="block")))


async def test_operator_retry_is_audited_and_terminal_not_reopened() -> None:
    async with TestEnvironment(Registry(blocked, fail)) as env:
        h = await env.client.start(blocked, 2, request_id="block")
        await env.drain()
        with pytest.raises(WorkflowBlocked):
            await h.result()
        with pytest.raises(Conflict):
            await h.resume_blocked(actor="test", reason="still blocked", request_id="no")
        for _ in range(2):
            await h.retry_blocked_task("0.0", actor="operator", reason="fixed", request_id="retry")
        await env.drain()
        assert len((await h.describe())["nodes"]["0.0"]["attempts"]) == 2
        await h.terminate(actor="operator", reason="stop", request_id="stop")
        with pytest.raises(Conflict):
            await h.resume_blocked(actor="operator", reason="no", request_id="invalid")


async def test_changed_command_and_missing_version_block() -> None:
    @workflow(name="sequence", build_id="test-v1")
    async def changed(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(DOUBLE, value + 1)
    async with TestEnvironment(Registry(sequence)) as env:
        h = await env.client.start(sequence, 2, request_id="change")
        await env.engine.tick()
        with pytest.raises(NonDeterminism):
            replay(Registry(changed).resolve(changed), await h.describe())
        env.engine.registry = Registry()
        await env.engine.tick()
        assert (await h.describe())["status"] == "BLOCKED"


async def test_early_return_against_history_blocks() -> None:
    @workflow(name="sequence", build_id="test-v1")
    async def changed(ctx: WorkflowContext, value: int) -> int:
        return 9
    async with TestEnvironment(Registry(sequence)) as env:
        h = await env.client.start(sequence, 2, request_id="return")
        await env.engine.tick()
        with pytest.raises(NonDeterminism):
            replay(Registry(changed).resolve(changed), await h.describe())


@pytest.mark.parametrize("pattern", ["asyncio", "finally", "swallowed-finally"])
async def test_unsupported_workflow_constructs(pattern: str) -> None:
    @workflow(name="bad", build_id="bad-v1")
    async def bad(ctx: WorkflowContext, value: int) -> int:
        if pattern == "asyncio":
            await asyncio.sleep(0)
        else:
            try:
                await ctx.sleep(1)
            finally:
                try:
                    await ctx.call(DOUBLE, value)
                except Exception:
                    if pattern != "swallowed-finally":
                        raise
        return value
    async with TestEnvironment(Registry(bad, double)) as env:
        h = await env.client.start(bad, 1, request_id="bad")
        await env.drain()
        assert (await h.describe())["status"] == "BLOCKED"
        assert env.worker.metrics["executed"] == 0


async def test_workflow_exception_is_failure() -> None:
    @workflow(name="bug", build_id="test")
    async def bug(ctx: WorkflowContext, value: int) -> int:
        return 1 // value
    async with TestEnvironment(Registry(bug)) as env:
        h = await env.client.start(bug, 0, request_id="bug")
        await env.drain()
        assert (await h.describe())["status"] == "FAILED"


async def test_now_uuid_recorded_once() -> None:
    @workflow(name="clock", build_id="test")
    async def clocked(ctx: WorkflowContext, value: int) -> str:
        when = await ctx.now()
        ident = await ctx.uuid()
        await ctx.sleep(1)
        return f"{when.isoformat()}/{ident}"
    reg = Registry(clocked)
    async with TestEnvironment(reg) as env:
        h = await env.client.start(clocked, 1, request_id="clock")
        await env.drain()
        env.clock.advance(1)
        value = await env.run(h)
        assert replay(reg.resolve(clocked), await h.describe()).value == value


async def test_race_records_winner_and_losers_continue() -> None:
    @workflow(name="race", build_id="test")
    async def racing(ctx: WorkflowContext, value: int) -> int:
        result = await ctx.race(ctx.sleep(10), ctx.call(DOUBLE, value))
        return result.value
    async with TestEnvironment(Registry(racing, double)) as env:
        h = await env.client.start(racing, 5, request_id="race")
        assert await env.run(h) == 10
        assert (await h.describe())["commands"][0]["result"]["index"] == 1
        env.clock.advance(10)
        await env.drain()
        assert await h.result() == 10
        assert (await h.describe())["nodes"]["0.0"]["state"] == "done"


async def test_child_unique() -> None:
    @workflow(name="parent", build_id="test")
    async def parent(ctx: WorkflowContext, value: int) -> int:
        return await ctx.child(WorkflowRef("sequence", int, int), value)
    async with TestEnvironment(Registry(parent, sequence, double)) as env:
        h = await env.client.start(parent, 2, request_id="parent")
        assert await env.run(h) == 9
        assert len(await env.client.list()) == 2


async def test_rollover_preserves_pending_signals_and_start_identity() -> None:
    @workflow(name="rollover", build_id="test")
    async def rolling(ctx: WorkflowContext, value: int) -> int:
        if value == 0:
            await ctx.continue_as_new(1)
        return value if await ctx.wait_signal(APPROVAL) else 0
    async with TestEnvironment(Registry(rolling)) as env:
        h = await env.client.start(rolling, 0, request_id="roll", workflow_id="logical")
        await h.signal(APPROVAL, True, signal_id="early")
        await env.drain()
        assert (await h.describe())["status"] == "CONTINUED"
        assert await h.result(timeout=1, follow_continued=True) == 1
        assert (await env.client.start(rolling, 0, request_id="roll", workflow_id="logical")).run_id == h.run_id
        assert (await env.client.current("logical")).run_id != h.run_id


async def test_cancel_and_result_timeout_are_distinct() -> None:
    async with TestEnvironment(Registry(approval)) as env:
        h = await env.client.start(approval, 1, request_id="cancel")
        await env.drain()
        with pytest.raises(TimeoutError):
            await h.result(timeout=0.001)
        assert (await h.describe())["status"] == "WAITING"
        await h.cancel(actor="test", reason="user request", request_id="cancel-request")
        await env.drain()
        assert (await h.describe())["status"] == "CANCELLED"


async def test_archive_keeps_request_tombstone() -> None:
    async with TestEnvironment(Registry(sequence, double)) as env:
        h = await env.client.start(sequence, 1, request_id="archive")
        assert await env.run(h) == 5
        with pytest.raises(ValueError):
            await h.archive(actor="test", reason="old", retention=5, safety_horizon=10)
        with pytest.raises(Conflict):
            await h.archive(actor="test", reason="old", retention=10, safety_horizon=10)
        env.clock.advance(11)
        await h.archive(actor="test", reason="old", retention=10, safety_horizon=10)
        with pytest.raises(Archived):
            await h.result()
        assert (await env.client.start(sequence, 1, request_id="archive")).run_id == h.run_id


async def test_tagged_controls_and_filtered_pagination() -> None:
    async with TestEnvironment(Registry(approval)) as env:
        for i in range(8):
            await env.client.start(approval, i, request_id=str(i), tags=("chosen",) if i % 2 else ("other",))
        selected = await env.client.list(tags=("chosen",), limit=3)
        assert len(selected) == 3
        page2 = await env.client.list(tags=("chosen",), after=selected[-1]["run_id"], limit=3)
        assert len(page2) == 1
        result = await env.client.control_tagged("cancel", tags=("chosen",), actor="test", reason="batch", request_id="batch")
        assert len(result["outcomes"]) == 4
        await env.drain()
        assert all(row["status"] == "CANCELLED" for row in await env.client.list(tags=("chosen",)))


@dataclass
class Input:
    number: int


def test_codec_limits_and_canonicalization() -> None:
    assert decode(encode(Input(1), Input), Input) == Input(1)
    for bad in (float("nan"), "x" * 262145, object(), {1, 2}):
        with pytest.raises(ProtocolError):
            encode(bad)
    with pytest.raises(ProtocolError):
        encode("1", int)
    with pytest.raises(ProtocolError):
        parse_json('{"a":1,"a":2}')
    assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})


@pytest.mark.parametrize("seconds", [0, -1, float("nan"), float("inf")])
def test_invalid_timeouts(seconds: float) -> None:
    with pytest.raises(ValueError):
        TaskOptions(attempt_timeout=seconds)
