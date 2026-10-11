from __future__ import annotations

import asyncio
from dataclasses import asdict

import pytest

from duraflow.contracts import ActivationResultLimit, ManualClock, canonical
from duraflow.executor import ProcessReplayExecutor
from duraflow.message_store import SQLiteMessageStore
from duraflow.messaging import MAX_MESSAGE_BYTES, Message
from duraflow.replay import Activation
from duraflow.state import identity
from duraflow.testing import TestEnvironment
from duraflow.workflow_engine import new_state
from duraflow.workflow_replay import execute
from tests.activation_boundary_app import flow, registry
from duraflow import Registry, WorkflowContext, WorkflowRef, workflow


def sized_input(margin):
    definition = registry.resolve(flow)
    total = 64 * 200 * 1024
    state = new_state(
        {
            "namespace": "test",
            "workflow_id": "probe",
            "run_id": "probe",
            "manifest": definition.manifest,
            "input": total,
        },
        0,
    )
    activation = execute(definition, state)
    size = len(canonical({"ok": True, "kind": activation.kind, "value": activation.value}).encode()) + 1
    return total + MAX_MESSAGE_BYTES - margin - size


async def prepare(env, total):
    for role in env.runtimes:
        await role.prepare()
    handle = await env.client.dispatch(flow, total, request_id="probe")
    await env.engine.step()
    await env.engine.step()
    return handle


async def aggregate(env):
    return await env.store.read(env.topics.instance("activation-boundary", "probe"))


def responses(env):
    return [
        Message.from_bytes(raw)
        for _, raw, props in env.transport.publications
        if not props and Message.from_bytes(raw).kind == "activation_result"
    ]


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("process", [False, True])
@pytest.mark.parametrize("margin", [64, 4096])
async def test_real_boundary_blocks_or_commits_atomically(backend, process, margin, tmp_path):
    clock = ManualClock()
    store = SQLiteMessageStore(tmp_path / "boundary.db", clock=clock) if backend == "sqlite" else None
    env = TestEnvironment(registry, clock=clock, store=store)
    if process:
        env.workflows.replay_executor = ProcessReplayExecutor("tests.activation_boundary_app", workers=1)
    try:
        await prepare(env, sized_input(margin))
        for _ in range(4):
            await env.workflows.step()
            await env.engine.step()
            clock.advance(1)
        record = await aggregate(env)
        state = record["runs"][record["current"]]
        assert record["active"] is None
        assert len(responses(env)) == 1
        if margin == 64:
            assert state["status"] == "BLOCKED"
            assert state["blocked_reason"] == "ACTIVATION_RESULT_LIMIT"
            assert state["commands"] == []
            assert state["finished_at"] is None
        else:
            assert state["status"] == "WAITING"
            assert len(state["commands"]) == 1
        assert env.workflows.metrics["errors"] == env.engine.metrics["errors"] == 0
    finally:
        await env.__aexit__(None, None, None)


class FixedExecutor:
    def __init__(self, activation):
        self.activation = activation

    async def execute(self, definition, state):
        return self.activation

    async def close(self):
        pass


@pytest.mark.parametrize("excess", [0, 1])
async def test_exact_final_wire_boundary(excess, monkeypatch):
    import duraflow.messaging as messaging

    env = TestEnvironment(registry)
    try:
        await prepare(env, 64)
        delivery = await env.transport.receive(*env.workflows.routes[0][:2])
        request = Message.from_bytes(delivery.data)
        response = Message(
            "activation_result",
            request.key,
            {
                "run_id": request.body["snapshot"]["run_id"],
                "activation_id": request.body["activation_id"],
                "kind": "completed",
                "value": "x" * 4096,
                "_df_expires_at": request.body["_df_expires_at"],
            },
            id=identity(request.id, "result"),
        )
        monkeypatch.setattr(messaging, "MAX_MESSAGE_BYTES", len(canonical(asdict(response)).encode()) - excess)
        env.workflows.replay_executor = FixedExecutor(Activation("completed", "x" * 4096))
        await env.workflows.process(delivery)
        actual = responses(env)[0]
        assert actual.id == response.id
        assert actual.body["kind"] == ("blocked" if excess else "completed")
        await env.engine.step()
        record = await aggregate(env)
        assert record["active"] is None
    finally:
        await env.__aexit__(None, None, None)


async def test_publish_failure_redelivery_and_resume_preserve_identity(monkeypatch):
    import duraflow.messaging as messaging

    env = TestEnvironment(registry)
    try:
        handle = await prepare(env, 64)
        original = await aggregate(env)
        monkeypatch.setattr(messaging, "MAX_MESSAGE_BYTES", 8192)
        env.workflows.replay_executor = FixedExecutor(Activation("completed", "x" * 16384))
        publish = env.transport.publish
        attempted = []

        async def fail_after_publish(topic, data, properties, **kwargs):
            attempted.append(data)
            await publish(topic, data, properties, **kwargs)
            raise OSError("publication confirmation lost")

        monkeypatch.setattr(env.transport, "publish", fail_after_publish)
        with pytest.raises(OSError):
            await env.workflows.step()
        assert env.workflows.metrics["messages"] == 0
        assert (await aggregate(env))["active"] == original["active"]
        monkeypatch.setattr(env.transport, "publish", publish)
        env.clock.advance(1)
        await env.workflows.step()
        assert responses(env)[0].to_bytes() == responses(env)[1].to_bytes() == attempted[0]
        for _ in range(4):
            await env.engine.step()
        record = await aggregate(env)
        assert record["active"] is None
        state = record["runs"][handle.run_id]
        assert state["blocked_reason"] == "ACTIVATION_RESULT_LIMIT"
        assert state["commands"] == []
        env.workflows.replay_executor.activation = Activation("completed", 7)
        resume = asyncio.create_task(
            handle.resume_blocked(actor="test", reason="capacity restored", request_id="resume")
        )
        for _ in range(100):
            await env.engine.step()
            await env.workflows.step()
            await asyncio.sleep(0.005)
            if resume.done() and (await aggregate(env))["runs"][handle.run_id]["status"] == "COMPLETED":
                break
        await asyncio.wait_for(resume, 1)
        state = (await aggregate(env))["runs"][handle.run_id]
        assert state["status"] == "COMPLETED"
        result = asyncio.create_task(handle.result(timeout=2))
        for _ in range(100):
            await env.engine.step()
            await asyncio.sleep(0.005)
            if result.done():
                break
        assert await result == 7
    finally:
        await env.__aexit__(None, None, None)


async def test_process_limit_keeps_input_budget_and_reuses_healthy_child():
    definition = registry.resolve(flow)
    state = new_state(
        {
            "namespace": "test",
            "workflow_id": "probe",
            "run_id": "probe",
            "manifest": definition.manifest,
            "input": sized_input(64),
        },
        0,
    )
    async with ProcessReplayExecutor("tests.activation_boundary_app", workers=1) as executor:
        assert executor.max_bytes == MAX_MESSAGE_BYTES
        with pytest.raises(ActivationResultLimit, match="ACTIVATION_RESULT_LIMIT"):
            await executor.execute(definition, state)
        pids = {p.pid for p in executor._processes}
        state["input"] = 64
        assert (await executor.execute(definition, state)).kind == "schedule"
        assert pids == {p.pid for p in executor._processes}


async def test_child_manifest_expansion_is_checked_after_replay(monkeypatch):
    import duraflow.messaging as messaging

    @workflow(name="boundary-child", build_id="v1")
    async def child(ctx: WorkflowContext, value: int) -> int:
        return value

    @workflow(name="boundary-parent", build_id="v1")
    async def parent(ctx: WorkflowContext, value: int) -> int:
        return await ctx.child(WorkflowRef("boundary-child", int, int), value)

    definitions = Registry(child, parent)
    env = TestEnvironment(definitions)
    try:
        for role in env.runtimes:
            await role.prepare()
        await env.client.dispatch(parent, 1, request_id="probe")
        await env.engine.step()
        await env.engine.step()
        route = next(route for route in env.workflows.routes if route[0] == env.topics.replay("boundary-parent", "v1"))
        delivery = await env.transport.receive(*route[:2])
        request = Message.from_bytes(delivery.data)
        activation = execute(definitions.resolve(parent), request.body["snapshot"])
        bare = Message(
            "activation_result",
            request.key,
            {
                "run_id": request.body["snapshot"]["run_id"],
                "activation_id": request.body["activation_id"],
                "kind": activation.kind,
                "value": activation.value,
                "_df_expires_at": request.body["_df_expires_at"],
            },
            id=identity(request.id, "result"),
        )
        # Direct handle invocation isolates expansion from input admission.
        monkeypatch.setattr(messaging, "MAX_MESSAGE_BYTES", len(bare.to_bytes()))
        await env.workflows.handle(request, delivery)
        response = Message.from_bytes(env.transport.publications[-1][1])
        assert response.body["kind"] == "blocked"
        assert response.body["value"] == "ACTIVATION_RESULT_LIMIT"
    finally:
        await env.__aexit__(None, None, None)
