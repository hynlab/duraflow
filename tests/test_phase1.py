from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from duraflow import LegacyEngine as Engine, Registry, TaskOptions, WorkflowContext, workflow
from duraflow.contracts import ProtocolError, WorkflowBlocked, decode, encode, schema_id
from duraflow.executor import ProcessReplayExecutor
from duraflow.replay import replay
from duraflow.state import route
from duraflow.testing import LegacyTestEnvironment as TestEnvironment
from tests.codec_payloads import Receipt, sample
from tests.hardening_app import endless, endless_cleanup, healthy, registry, sleeping
from tests.test_engine import DOUBLE, double, sequence


@pytest.mark.parametrize("bad", [endless, endless_cleanup])
async def test_isolated_replay_watchdog_does_not_block_healthy_work(bad):
    async with TestEnvironment(registry) as env:
        failed = await env.client.start(bad, 1, request_id="stuck")
        good = await env.client.start(healthy, 4, request_id="healthy")
        executor = ProcessReplayExecutor("tests.hardening_app", workers=2, timeout=0.4)
        env.engine.replay_executor = executor
        heartbeats = []

        async def monitor():
            for _ in range(20):
                heartbeats.append(1)
                await asyncio.sleep(0.025)

        try:
            async with asyncio.timeout(15):
                await asyncio.gather(env.engine.advance(failed.run_id), env.engine.advance(good.run_id), monitor())
            assert (await good.describe())["result"] == 5, await good.describe()
            state = await failed.describe()
            assert state["status"] == "BLOCKED"
            assert state["blocked_reason"] == "REPLAY_DEADLINE_EXCEEDED"
            assert state["commands"] == []
            assert len(heartbeats) == 20
        finally:
            await env.engine.close()
        assert not executor._processes


async def test_isolated_pool_reuses_worker_and_rejects_closed_executor():
    async with TestEnvironment(registry) as env:
        handle = await env.client.start(sleeping, 5, request_id="sleep")
        async with ProcessReplayExecutor("tests.hardening_app", workers=1, timeout=1) as executor:
            env.engine.replay_executor = executor
            await env.drain()
            pids = {p.pid for p in executor._processes}
            env.clock.advance(1)
            assert await env.run(handle) == 5
            assert {p.pid for p in executor._processes} == pids
        with pytest.raises(WorkflowBlocked):
            await executor.execute(registry.resolve(sleeping), await handle.describe())


@workflow(name="hardening_deadline", build_id="v1")
async def deadline(ctx: WorkflowContext, value: int) -> int:
    return await ctx.call(DOUBLE, value, options=TaskOptions(attempt_timeout=10))


async def claim(env, handle):
    await env.engine.tick()
    await env.engine.tick()
    delivery = await env.transport.receive(route(env.namespace, DOUBLE.descriptor()), "workers")
    assert delivery is not None
    context = await env.worker._claim(delivery, DOUBLE)
    assert context is not None
    return context


@pytest.mark.parametrize("accept_at,success", [(9, True), (10, False), (11, False)])
async def test_result_deadline_uses_recorded_acceptance_not_poll_time(accept_at, success):
    async with TestEnvironment(Registry(deadline, double)) as env:
        handle = await env.client.start(deadline, 3, request_id="deadline")
        context = await claim(env, handle)
        env.clock.advance(accept_at)
        await env.worker._observe(context, 6, None)
        env.clock.advance(50)
        await env.engine.tick()
        state = await handle.describe()
        assert (state["status"] == "COMPLETED") is success
        if not success:
            assert state["error"]["code"] == "ATTEMPT_TIMEOUT"


async def test_cancel_lost_owner_fences_late_result_and_records_uncertainty():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 3, request_id="lost")
        context = await claim(env, handle)
        await handle.cancel(actor="operator", reason="stop", request_id="cancel")
        await env.engine.tick()
        assert (await handle.describe())["status"] == "CANCELLING"
        env.clock.advance(env.worker.lease_seconds + 1)
        env.engine = Engine(
            env.store,
            env.transport,
            env.registry if hasattr(env, "registry") else Registry(sequence, double),
            namespace=env.namespace,
            clock=env.clock,
        )
        await env.engine.tick()
        state = await handle.describe()
        assert state["status"] == "CANCELLED"
        assert state["nodes"]["0.0"]["external_outcome"] == "unknown"
        await env.worker._observe(context, 6, None)
        assert (await handle.describe())["status"] == "CANCELLED"
        assert env.worker.metrics["stale_results"] == 1


async def test_new_engine_does_not_poison_old_build_and_old_engine_finishes():
    @workflow(name="sequence", build_id="different-build")
    async def other(ctx: WorkflowContext, value: int) -> int:
        return value + 100

    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 2, request_id="old-build")
        foreign = Engine(env.store, env.transport, Registry(other), namespace=env.namespace, clock=env.clock)
        before = await handle.describe()
        await foreign.advance(handle.run_id)
        assert await handle.describe() == before
        assert foreign.metrics["unsupported_activations"] == 1
        assert await env.run(handle) == 9


def test_frozen_alpha_history_replays_without_rewriting():
    state = json.loads((Path(__file__).parent / "fixtures/alpha_sequence.json").read_text())
    assert "lifecycle_version" not in state
    before = json.dumps(state, sort_keys=True)
    assert replay(Registry(sequence).resolve(sequence), state).value == 13
    assert json.dumps(state, sort_keys=True) == before
    state["codec_version"] = 999
    with pytest.raises(ProtocolError):
        replay(Registry(sequence).resolve(sequence), state)


def test_frozen_codec_payload_and_schema():
    fixture = json.loads((Path(__file__).parent / "fixtures/codec_v1.json").read_text())
    assert encode(sample(), Receipt) == fixture["payload"]
    assert decode(fixture["payload"], Receipt) == sample()
    assert schema_id(Receipt) == fixture["schema"]
