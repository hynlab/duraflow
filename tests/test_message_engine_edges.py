"""State-owner routing boundaries and all-or-nothing replay decisions."""

import pytest

from duraflow import ChannelRef, Registry, TaskRef, WorkflowBlocked, WorkflowContext, WorkflowRef, workflow
from duraflow.messaging import Message
from duraflow.testing import TestEnvironment
from tests.test_engine import double, sequence


@pytest.mark.parametrize(
    "case",
    [
        "foreign-namespace",
        "other-workflow",
        "control-on-command",
        "query-on-control",
        "parent-on-command",
        "malformed-start",
    ],
)
async def test_invalid_workflow_route_cannot_mutate_a_committed_instance(case):
    async with TestEnvironment(Registry(sequence, double)) as env:
        env.workflows.accepting = False
        handle = await env.client.start(sequence, 3, request_id="target")
        key = env.topics.instance("sequence", "target")
        before = await env.store.read(key)
        topic, kind, body = env.topics.commands("sequence"), "start", {}
        message_key = key
        if case == "foreign-namespace":
            message_key = '["foreign","sequence","target"]'
        elif case == "other-workflow":
            message_key = env.topics.instance("other", "target")
        elif case == "control-on-command":
            kind = "control"
        elif case == "query-on-control":
            topic, kind = env.topics.control("sequence"), "query"
        elif case == "parent-on-command":
            body = {"parent": {}}
        message = Message(kind, message_key, body)
        await env.transport.publish(topic, message.to_bytes(), {}, key=message_key)
        await env.drain()
        assert env.engine.metrics["errors"] == 1
        assert await env.store.read(key) == before
        assert (await handle.describe())["status"] == "PENDING"
        assert env.worker.metrics["executed"] == 0


async def test_partial_replay_decision_rolls_back_all_business_dispatch():
    async with TestEnvironment(Registry(sequence, double)) as env:
        env.workflows.accepting = False
        handle = await env.client.start(sequence, 3, request_id="invalid-decision")
        key = env.topics.instance("sequence", "invalid-decision")
        aggregate = await env.store.read(key)
        valid = WorkflowContext().call(TaskRef("double", int, int), 3).spec
        decision = Message(
            "activation_result",
            key,
            {
                "run_id": handle.run_id,
                "activation_id": aggregate["active"]["id"],
                "kind": "schedule",
                "value": [valid, {"kind": "unknown"}],
            },
        )
        await env.transport.publish(env.topics.workflow("sequence"), decision.to_bytes(), {}, key=key)
        await env.drain()
        with pytest.raises(WorkflowBlocked):
            await handle.result(timeout=1)
        state = await handle.describe()
        assert state["commands"] == [] and state["nodes"] == {}
        assert not any(Message.from_bytes(data).kind == "execute_task" for _, data, _ in env.transport.publications)


async def test_recorded_values_and_cross_workflow_signals_survive_replay():
    channel = ChannelRef("cross-workflow", list[str])

    @workflow(name="cross-target", build_id="edges-v1")
    async def target(ctx: WorkflowContext, value: int) -> list[str]:
        return await ctx.channel(channel).receive(max_signals=1).next()

    @workflow(name="cross-source", build_id="edges-v1")
    async def source(ctx: WorkflowContext, value: int) -> list[str]:
        values = [str(await ctx.now()), str(await ctx.uuid())]
        await ctx.send_signal(WorkflowRef("cross-target", int, list[str]), "target", channel, values, signal_id="sent")
        return values

    async with TestEnvironment(Registry(source, target)) as env:
        receiving = await env.client.start(target, 0, request_id="target")
        await env.drain()
        sending = await env.client.start(source, 0, request_id="source")
        values = await env.run(sending)
        assert await env.run(receiving) == values
        from duraflow.workflow_replay import execute

        replayed = execute(env.client.registry.resolve(source), await sending.describe())
        assert replayed.kind == "completed" and replayed.value == values
