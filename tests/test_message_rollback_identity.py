"""Broker traffic from the discarded timeline must not poison a new replay."""

from copy import deepcopy

import pytest

from duraflow import Registry, WorkflowContext, workflow
from duraflow.messaging import Message
from duraflow.testing import TestEnvironment


@workflow(name="rollback-identity", build_id="rollback-v1")
async def flow(ctx: WorkflowContext, value: int) -> int:
    await ctx.sleep(1)
    return value


@pytest.mark.parametrize("old_response_first", [False, True])
async def test_rollback_and_stale_replay_response_do_not_suppress_new_completion(old_response_first):
    async with TestEnvironment(Registry(flow)) as env:
        handle = await env.client.start(flow, 42, request_id="rollback")
        await env.drain()
        key = env.topics.instance("rollback-identity", "rollback")
        snapshot = deepcopy((env.store.documents, env.store.inbox, env.store.outbox))
        assert (await env.store.read(key))["active"] is None
        offset = len(env.transport.publications)
        env.clock.advance(1)
        await env.drain()
        completed = await env.store.read(key)
        assert completed["runs"][completed["current"]]["status"] == "COMPLETED"
        traffic = [
            (topic, Message.from_bytes(data)) for topic, data, props in env.transport.publications[offset:] if not props
        ]
        old_activation = next(message for _, message in traffic if message.kind == "activate")
        old_response = next(message for _, message in traffic if message.kind == "activation_result")
        original_timer = next(
            Message.from_bytes(data)
            for _, data, props in env.transport.publications
            if not props and Message.from_bytes(data).kind == "timer"
        )

        # Restore only the database. Broker ACKs and previously computed replay
        # decisions remain on the discarded timeline, as in physical PITR.
        env.workflows.accepting = False
        async with env.store.lock:
            env.store.documents, env.store.inbox, env.store.outbox = deepcopy(snapshot)
        if old_response_first:
            await env.transport.publish(env.topics.workflow("rollback-identity"), old_response.to_bytes(), {}, key=key)
            await env.drain()
            assert (await env.store.read(key))["active"] is None
        offset = len(env.transport.publications)
        await env.transport.publish(
            env.topics.topic("timer", "rollback-identity"), original_timer.to_bytes(), {}, key=key
        )
        await env.drain()
        restored = await env.store.read(key)
        activation = next(
            Message.from_bytes(data)
            for _, data, props in env.transport.publications[offset:]
            if not props and Message.from_bytes(data).kind == "activate"
        )
        assert activation.id != old_activation.id, "Restoring counters must not reuse a discarded publication identity"
        assert restored["active"]["id"] != old_activation.body["activation_id"]
        if not old_response_first:
            await env.transport.publish(env.topics.workflow("rollback-identity"), old_response.to_bytes(), {}, key=key)
            await env.drain()
            assert (await env.store.read(key))["active"] == restored["active"], (
                "Old completion must not finish a new activation"
            )
        env.workflows.accepting = True
        await env.drain()
        assert await handle.result(timeout=1) == 42
