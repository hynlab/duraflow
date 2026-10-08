"""Protocol-2 integration: real message handlers, no workflow scans or direct mutations."""

import asyncio

import pytest

from duraflow import (
    ChannelRef,
    Client,
    Archived,
    Conflict,
    Registry,
    RetryPolicy,
    SignalFilter,
    SQLiteMessageStore,
    ManualClock,
    TaskOptions,
    TaskFailure,
    WorkflowBlocked,
    TaskRef,
    WorkflowContext,
    WorkflowFailed,
    WorkflowRef,
    task,
    workflow,
)
from duraflow.message_store import MemoryMessageStore
from duraflow.messaging import Message, Publication
from duraflow.testing import TestEnvironment
from duraflow.workflow_engine import WorkflowEngine
from tests.test_engine import double, sequence, parallel, broadcast, a, b, c, bindings

APPROVAL = ChannelRef("approval", bool)
EVENTS = ChannelRef("events", dict[str, int])


@workflow(name="approval-flow", build_id="messages-v1")
async def approval_flow(ctx: WorkflowContext, value: int) -> bool:
    stream = ctx.channel(APPROVAL).receive(max_signals=1)
    await ctx.sleep(1)
    return await stream.next(timeout=10)


@workflow(name="filtered-flow", build_id="messages-v1")
async def filtered_flow(ctx: WorkflowContext, value: int) -> list[int]:
    stream = ctx.channel(EVENTS).receive(max_signals=2, filter=SignalFilter(equals={"account": value}))
    first = await stream.next()
    second = await stream.next()
    return [first["amount"], second["amount"]]


@pytest.mark.parametrize("definition,expected", [(sequence, 13), (parallel, [6, 8])])
async def test_workflow_and_task_execution_are_message_driven(definition, expected):
    async with TestEnvironment(Registry(definition, double)) as env:
        handle = await env.client.start(definition, 3, request_id="one")
        assert await env.run(handle) == expected
        kinds = [Message.from_bytes(data).kind for _, data, props in env.transport.publications if not props]
        assert {
            "start",
            "activate",
            "activation_result",
            "execute_task",
            "task_started",
            "task_result",
            "response",
        } <= set(kinds)
        assert not hasattr(env.engine, "tick") and not hasattr(env.store, "scan")
        assert env.client.transport is env.transport and not hasattr(env.client, "store")
        assert env.worker.journal is not env.store


async def test_start_retries_conflicts_and_client_reconnection():
    async with TestEnvironment(Registry(sequence, double)) as env:
        first, second = await asyncio.gather(
            env.client.start(sequence, 3, request_id="same"), env.client.start(sequence, 3, request_id="same")
        )
        assert first.run_id == second.run_id
        assert await env.run(first) == 13
        with pytest.raises(Conflict):
            await env.client.start(sequence, 4, request_id="same")
        from duraflow import Client

        reconnect = Client(env.transport, Registry(sequence, double), topics=env.topics)
        assert await reconnect.get_handle(sequence, "same").result(timeout=1) == 13


async def test_receive_registration_buffers_before_await_and_deduplicates():
    async with TestEnvironment(Registry(approval_flow)) as env:
        handle = await env.client.start(approval_flow, 1, request_id="approval")
        await env.drain()
        assert (await handle.signal(APPROVAL, True, signal_id="approved"))["receivers"] == 1
        await handle.signal(APPROVAL, True, signal_id="approved")
        assert (await handle.signal(APPROVAL, False, signal_id="too-many"))["receivers"] == 0
        with pytest.raises(Conflict):
            await handle.signal(APPROVAL, False, signal_id="approved")
        env.clock.advance(1)
        assert await env.run(handle) is True
        state = await handle.describe()
        assert len(next(iter(state["channels"].values()))["received"]) == 1


async def test_signals_before_registration_are_discarded():
    @workflow(name="late-registration", build_id="messages-v1")
    async def flow(ctx: WorkflowContext, value: int) -> bool:
        await ctx.sleep(1)
        return await ctx.channel(APPROVAL).receive(max_signals=1).next()

    async with TestEnvironment(Registry(flow)) as env:
        handle = await env.client.start(flow, 0, request_id="late")
        await env.drain()
        assert (await handle.signal(APPROVAL, True, signal_id="early"))["receivers"] == 0
        env.clock.advance(1)
        await env.drain()
        assert (await handle.describe())["status"] == "WAITING"
        await handle.signal(APPROVAL, False, signal_id="later")
        assert await env.run(handle) is False


async def test_filter_repeated_reception_and_tag_fanout():
    async with TestEnvironment(Registry(filtered_flow)) as env:
        env.tags.page_size = 1
        handles = [
            await env.client.start(filtered_flow, 42, request_id=f"flow-{i}", tags=("accounts",)) for i in range(3)
        ]
        await env.drain()
        await env.client.signal_tagged(
            filtered_flow, "accounts", EVENTS, {"account": 7, "amount": 1}, signal_id="filtered"
        )
        await env.client.signal_tagged(
            filtered_flow, "accounts", EVENTS, {"account": 42, "amount": 10}, signal_id="one"
        )
        await env.drain()
        for handle in handles:
            await handle.signal(EVENTS, {"account": 42, "amount": 20}, signal_id="two")
        assert [await env.run(h) for h in handles] == [[10, 20]] * 3


async def test_signal_timeout_is_a_delayed_message():
    async with TestEnvironment(Registry(approval_flow)) as env:
        handle = await env.client.start(approval_flow, 1, request_id="timeout")
        await env.drain()
        env.clock.advance(1)
        await env.drain()
        env.clock.advance(10)
        await env.drain()
        with pytest.raises(WorkflowFailed):
            await handle.result(timeout=1)
        assert (await handle.describe())["error"]["code"] == "SIGNAL_TIMEOUT"


async def test_signal_is_persisted_while_replay_is_running():
    async with TestEnvironment(Registry(approval_flow)) as env:
        env.workflows.accepting = False
        handle = await env.client.start(approval_flow, 0, request_id="during-replay")
        sending = asyncio.create_task(handle.signal(APPROVAL, True, signal_id="buffered"))
        await asyncio.sleep(0.03)
        aggregate = await env.store.read(env.topics.instance("approval-flow", "during-replay"))
        assert len(aggregate["buffered"]) == 1
        env.workflows.accepting = True
        assert (await sending)["receivers"] == 1
        env.clock.advance(1)
        assert await env.run(handle) is True


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
async def test_store_consumption_state_and_outbox_commit_or_roll_back(tmp_path, backend):
    store = MemoryMessageStore() if backend == "memory" else SQLiteMessageStore(tmp_path / "messages.db")
    message = Message("test", "key", {"value": 1})
    emitted = Message("next", "key", {})

    def broken(state, now):
        state["value"] = 42
        raise RuntimeError("before commit")

    with pytest.raises(RuntimeError):
        await store.apply("key", message, broken)
    assert await store.read("key") == {}

    def apply(state, now):
        state["value"] = state.get("value", 0) + 1
        return [Publication("topic", emitted)]

    assert await store.apply("key", message, apply)
    assert not await store.apply("key", message, apply)
    assert (await store.read("key"))["value"] == 1
    items = await store.claim("owner")
    assert len(items) == 1 and items[0].message == emitted
    with pytest.raises(Conflict):
        await store.apply("key", Message("test", "key", {"value": 2}, id=message.id), apply)
    await store.delivered(emitted.id, "wrong-owner")
    await store.delivered(emitted.id, "owner")
    assert await store.claim("owner") == []
    await store.close()


async def test_restart_state_engine_preserves_channel_mailbox(tmp_path):
    clock = ManualClock()
    store = SQLiteMessageStore(tmp_path / "state.db", clock=clock)
    async with TestEnvironment(Registry(approval_flow), store=store, clock=clock) as env:
        handle = await env.client.start(approval_flow, 0, request_id="restart")
        await env.drain()
        await handle.signal(APPROVAL, True, signal_id="before-restart")
        # Replace the actor, not a workflow snapshot; the broker and journal survive.
        env.engine.accepting = False
        replacement = WorkflowEngine(
            SQLiteMessageStore(tmp_path / "state.db", clock=clock),
            env.transport,
            Registry(approval_flow),
            topics=env.topics,
        )
        running = asyncio.create_task(replacement.run(env.stop, poll_interval=0.001))
        env.running.append(running)
        env.clock.advance(1)
        assert await handle.result(timeout=2) is True


async def test_duplicate_and_stale_activation_results_do_not_schedule_twice():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 3, request_id="duplicate")
        assert await env.run(handle) == 13
        results = [
            Message.from_bytes(data)
            for _, data, props in env.transport.publications
            if not props and Message.from_bytes(data).kind == "activation_result"
        ]
        message = results[0]
        await env.transport.publish(env.topics.workflow("sequence"), message.to_bytes(), {}, key=message.key)
        await env.transport.publish(
            env.topics.workflow("sequence"),
            Message(message.kind, message.key, message.body).to_bytes(),
            {},
            key=message.key,
        )
        await env.drain()
        assert env.worker.metrics["executed"] == 2
        assert await handle.result(timeout=1) == 13


async def test_broadcast_joins_events_and_reuses_successful_participants():
    async with TestEnvironment(Registry(broadcast, a, b, c), broadcasts=bindings()) as env:
        handle = await env.client.start(broadcast, 7, request_id="broadcast")
        assert await env.run(handle)
        state = await handle.describe()
        assert sum(n["state"] == "done" and n["spec"]["kind"] == "call" for n in state["nodes"].values()) == 3
        assert [data.decode() for topic, data, _ in env.transport.publications if topic == "analyzed"] == ["27"]


async def test_application_retry_is_a_delayed_task_message():
    ref = TaskRef("retry-message", int, int)
    calls = 0

    @task(ref=ref)
    def implementation(value: int) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("temporary")
        return value

    @workflow(name="retry-flow", build_id="messages-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value, options=TaskOptions(retry=RetryPolicy(max_attempts=2)))

    async with TestEnvironment(Registry(flow, implementation)) as env:
        handle = await env.client.start(flow, 7, request_id="retry")
        await env.drain()
        assert calls == 1
        env.clock.advance(1)
        assert await env.run(handle) == 7 and calls == 2


async def test_deferred_tasks_channel_and_child_rollover():
    @workflow(name="message-child", build_id="messages-v1")
    async def child(ctx: WorkflowContext, value: int) -> int:
        if value:
            await ctx.continue_as_new(0)
        return 17

    @workflow(name="message-parent", build_id="messages-v1")
    async def parent(ctx: WorkflowContext, value: int) -> int:
        result = ctx.dispatch(TaskRef("double", int, int), value)
        await ctx.timer(1)
        return await result + await ctx.child(WorkflowRef("message-child", int, int), 1)

    async with TestEnvironment(Registry(parent, child, double)) as env:
        handle = await env.client.start(parent, 3, request_id="parent")
        await env.drain()
        env.clock.advance(1)
        assert await env.run(handle) == 23


async def test_terminal_control_does_not_reopen_execution():
    async with TestEnvironment(Registry(approval_flow)) as env:
        handle = await env.client.start(approval_flow, 1, request_id="cancel")
        await env.drain()
        await handle.cancel(actor="operator", reason="requested", request_id="cancel-1")
        assert (await handle.describe())["status"] == "CANCELLED"
        with pytest.raises(WorkflowFailed):
            await handle.result(timeout=1)
        with pytest.raises(Conflict):
            await handle.signal(APPROVAL, True, signal_id="late")


async def test_contract_only_client_dispatch_and_receipt_before_engine_start():
    async with TestEnvironment(Registry(sequence, double)) as env:
        contract = WorkflowRef("sequence", int, int, build_id="test-v1")
        client = Client(env.transport, topics=env.topics)
        env.engine.accepting = False
        handle = await client.dispatch(contract, 3, request_id="contract-only")
        waiting = asyncio.create_task(handle.result(timeout=2))
        await asyncio.sleep(0.03)
        assert not waiting.done()
        env.engine.accepting = True
        assert await waiting == 13
        await client.close()
        assert (env.topics.reply(client.id), "client") not in env.transport.queues


async def test_stream_timeout_does_not_skip_the_next_signal():
    @workflow(name="timeout-then-receive", build_id="messages-v1")
    async def flow(ctx: WorkflowContext, value: int) -> bool:
        stream = ctx.channel(APPROVAL).receive()
        try:
            await stream.next(timeout=1)
        except TaskFailure:
            pass
        return await stream.next()

    async with TestEnvironment(Registry(flow)) as env:
        handle = await env.client.start(flow, 0, request_id="stream-timeout")
        await env.drain()
        env.clock.advance(1)
        await env.drain()
        await handle.signal(APPROVAL, True, signal_id="after-timeout")
        assert await env.run(handle) is True


async def test_type_filter_receives_only_the_selected_payload_schema():
    channel = ChannelRef("mixed", int | str)

    @workflow(name="typed-stream", build_id="messages-v1")
    async def flow(ctx: WorkflowContext, value: int) -> str:
        return await ctx.channel(channel).receive(payload_type=str, max_signals=1).next()

    async with TestEnvironment(Registry(flow)) as env:
        handle = await env.client.start(flow, 0, request_id="type-filter")
        await env.drain()
        assert (await handle.signal(channel, 7, signal_id="integer", payload_type=int))["receivers"] == 0
        assert (await handle.signal(channel, "accepted", signal_id="string", payload_type=str))["receivers"] == 1
        assert await env.run(handle) == "accepted"


async def test_signal_registration_is_committed_before_external_request_task():
    ref = TaskRef("request-approval", int, int)
    handle = None

    @task(ref=ref)
    async def send_request(value: int) -> int:
        await handle.signal(APPROVAL, True, signal_id="immediate-response")
        return value

    @workflow(name="immediate-approval", build_id="messages-v1")
    async def flow(ctx: WorkflowContext, value: int) -> bool:
        approvals = ctx.channel(APPROVAL).receive(max_signals=1)
        await ctx.call(ref, value)
        return await approvals.next()

    async with TestEnvironment(Registry(flow, send_request)) as env:
        env.worker.accepting = False
        handle = await env.client.start(flow, 0, request_id="instant")
        env.worker.accepting = True
        assert await env.run(handle) is True


async def test_cancel_can_fence_a_workflow_executor_that_is_offline():
    async with TestEnvironment(Registry(approval_flow)) as env:
        env.workflows.accepting = False
        handle = await env.client.start(approval_flow, 0, request_id="offline-replay")
        await handle.terminate(actor="operator", reason="stuck", request_id="stop")
        env.workflows.accepting = True
        await env.drain()
        assert (await handle.describe())["status"] == "TERMINATED"


async def test_result_wait_timeout_removes_its_remote_waiter():
    async with TestEnvironment(Registry(approval_flow)) as env:
        handle = await env.client.start(approval_flow, 0, request_id="wait-timeout")
        with pytest.raises(TimeoutError):
            await handle.result(timeout=0.02)
        await env.drain()
        aggregate = await env.store.read(env.topics.instance("approval-flow", "wait-timeout"))
        assert aggregate["waiters"] == []
        assert (await handle.describe())["status"] == "WAITING"


async def test_delegation_external_completion_and_conflicting_repeats():
    ref = TaskRef("delegated-message", int, int)
    tokens = []

    @task(ref=ref)
    async def delegate(ctx, value):
        marker = await ctx.defer(timeout=10)
        tokens.append(marker.token)
        return marker

    @workflow(name="delegate-flow", build_id="messages-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value)

    async with TestEnvironment(Registry(flow, delegate)) as env:
        handle = await env.client.start(flow, 0, request_id="delegate")
        await env.drain()
        assert await env.client.complete_external(tokens[0], 42, ref=ref)
        assert await env.run(handle) == 42
        assert await env.client.complete_external(tokens[0], 42, ref=ref)
        with pytest.raises(Conflict):
            await env.client.complete_external(tokens[0], 43, ref=ref)


async def test_blocked_task_is_retried_through_a_control_message():
    ref = TaskRef("operator-retry", int, int)
    broken = True

    @task(ref=ref)
    def implementation(value):
        if broken:
            raise ValueError("dependency")
        return value

    @workflow(name="operator-flow", build_id="messages-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value, options=TaskOptions(retry=RetryPolicy(exhausted="block")))

    async with TestEnvironment(Registry(flow, implementation)) as env:
        handle = await env.client.start(flow, 7, request_id="operator")
        await env.drain()
        with pytest.raises(WorkflowBlocked):
            await handle.result(timeout=1)
        broken = False
        await handle.retry_blocked_task("0.0", actor="operator", reason="fixed", request_id="retry")
        assert await env.run(handle) == 7


async def test_archiving_retains_identity_tombstones():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 3, request_id="archive")
        assert await env.run(handle) == 13
        with pytest.raises(Conflict):
            await handle.archive(actor="operator", reason="early", retention=10, safety_horizon=1)
        env.clock.advance(10)
        await handle.archive(actor="operator", reason="retention", retention=10, safety_horizon=1)
        assert (await handle.describe())["archived"]
        with pytest.raises(Archived):
            await handle.result(timeout=1)
        duplicate = await env.client.start(sequence, 3, request_id="archive")
        assert duplicate.run_id == handle.run_id


async def test_task_tags_cancel_an_inflight_service_without_workflow_db_access():
    from duraflow.contracts import canonical

    ref = TaskRef("tagged-service", int, int)
    started = asyncio.Event()

    @task(ref=ref)
    async def implementation(value):
        started.set()
        await asyncio.Event().wait()
        return value

    @workflow(name="task-tag-flow", build_id="messages-v1")
    async def flow(ctx: WorkflowContext, value: int) -> bool:
        try:
            await ctx.call(ref, value, tags=("campaign",))
        except TaskFailure:
            return False
        return True

    async with TestEnvironment(Registry(flow, implementation)) as env:
        env.worker.lease_seconds = 0.06
        handle = await env.client.start(flow, 7, request_id="task-tags")
        await asyncio.wait_for(started.wait(), 1)
        key = "task-tag/" + canonical([env.namespace, ref.name, ref.version, "campaign"])
        async with asyncio.timeout(1):
            while not (await env.journal.read(key)).get("members"):
                await asyncio.sleep(0.005)
        await env.client.cancel_tasks_tagged(ref, "campaign", request_id="stop-campaign")
        assert await handle.result(timeout=2) is False


async def test_early_timer_is_not_acknowledged_or_deduplicated_before_its_due_time():
    async with TestEnvironment(Registry(approval_flow)) as env:
        handle = await env.client.start(approval_flow, 0, request_id="early-timer")
        await env.drain()
        key = env.topics.instance("approval-flow", "early-timer")
        state = await handle.describe()
        timer_node = next(n for n in state["nodes"].values() if n["spec"]["kind"] == "sleep")
        early = Message(
            "timer", key, {"run_id": handle.run_id, "node_id": timer_node["id"], "not_before": timer_node["due_at"]}
        )
        await env.transport.publish(env.topics.topic("timer", "approval-flow"), early.to_bytes(), {}, key=key)
        await asyncio.sleep(0.02)
        assert (key, early.id) not in env.store.inbox
        assert (await handle.describe())["nodes"][timer_node["id"]]["state"] == "pending"
        env.clock.advance(1)
        await env.drain()
        assert (key, early.id) in env.store.inbox
        await handle.signal(APPROVAL, True, signal_id="approved")
        assert await env.run(handle) is True


async def test_public_command_topic_cannot_forge_a_workflow_execution_result():
    async with TestEnvironment(Registry(approval_flow)) as env:
        env.workflows.accepting = False
        handle = await env.client.start(approval_flow, 0, request_id="forged-result")
        key = env.topics.instance("approval-flow", "forged-result")
        aggregate = await env.store.read(key)
        forged = Message(
            "activation_result",
            key,
            {"run_id": handle.run_id, "activation_id": aggregate["active"]["id"], "kind": "completed", "value": True},
        )
        await env.transport.publish(env.topics.commands("approval-flow"), forged.to_bytes(), {}, key=key)
        await env.drain()
        assert (await handle.describe())["status"] == "PENDING"
        assert any(topic.endswith("-dlq") for topic, _, _ in env.transport.publications)


def test_maximum_contract_names_route_and_noninteger_protocol_versions_are_rejected():
    from duraflow import Topics, ProtocolError

    topics = Topics()
    assert topics.task("a" * 128, 1)
    assert topics.replay("a" * 128, "release-v1")
    with pytest.raises(ProtocolError):
        Message("test", "key", {}, version=2.0)


async def test_racing_completed_futures_uses_the_original_result_acceptance_order():
    slow_ref, fast_ref = TaskRef("slow-future", int, int), TaskRef("fast-future", int, int)
    release = asyncio.Event()

    @task(ref=slow_ref)
    async def slow(value: int) -> int:
        await release.wait()
        return value

    @task(ref=fast_ref)
    async def fast(value: int) -> int:
        return value

    @workflow(name="future-order", build_id="messages-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        first = ctx.dispatch(slow_ref, value)
        second = ctx.dispatch(fast_ref, value)
        await ctx.timer(1)
        return (await ctx.race(first, second)).index

    async with TestEnvironment(Registry(flow, slow, fast)) as env:
        handle = await env.client.start(flow, 0, request_id="futures")
        async with asyncio.timeout(1):
            while (await handle.describe())["nodes"].get("1.0", {}).get("state") != "done":
                await asyncio.sleep(0.005)
        release.set()
        await env.drain()
        env.clock.advance(1)
        assert await env.run(handle) == 1
