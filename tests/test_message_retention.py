"""Long-lived execution and expired-delivery fences, not merely TTL deletion."""

import pytest

from duraflow import ManualClock, ProtocolError, Registry, WorkflowContext, workflow
from duraflow.contracts import canonical
from duraflow.message_records import LocalUpdate, RecordUpdate
from duraflow.message_store import MemoryMessageStore, SQLiteMessageStore
from duraflow.messaging import Message, Publication, Topics
from duraflow.retention import DAY, DETAIL_RETENTION, MESSAGE_HORIZON
from duraflow.testing import TestEnvironment
from duraflow.workflow_engine import Transition, new_state
from tests.test_message_store_contract import journals as journals


async def test_auxiliary_records_and_outbox_rollback_together(journals):
    first, second, _ = journals
    outgoing = Message("event", "key", {})
    await first.apply("key", Message("seed", "key", {}), lambda state, now: [Publication("topic", outgoing)])

    def bad(state, now, records):
        state["changed"] = True
        records.put("signal/id", {"digest": "new"})
        return [Publication("different-topic", outgoing)]

    from duraflow import Conflict

    incoming = Message("change", "key", {})
    with pytest.raises(Conflict):
        await second.apply("key", incoming, RecordUpdate(bad))
    assert await first.read("key") == {}

    def verify(state, now, records):
        assert records.get("signal/id") is None
        records.put("signal/id", {"digest": "accepted"})
        return []

    assert await first.apply("key", incoming, RecordUpdate(verify))


async def test_gc_preserves_unsent_outbox_and_dry_run(journals):
    store, _, _ = journals

    def transition(state, now, records):
        records.put("expired", {"value": 1}, expires_at=0)
        records.put("live", {"value": 2})
        return [Publication("topic", Message("event", "key", {}))]

    await store.apply("key", Message("seed", "key", {}), RecordUpdate(transition))
    assert (await store.collect_garbage(dry_run=False))["records"] == 0
    for item in await store.claim("relay"):
        await store.delivered(item.message.id, "relay")
    assert (await store.collect_garbage())["records"] == 1
    assert (await store.collect_garbage())["records"] == 1
    assert (await store.collect_garbage(dry_run=False))["records"] == 1

    def verify(state, now, records):
        assert records.get("expired") is None
        assert records.get("live") == {"value": 2}
        return []

    await store.apply("key", Message("check", "key", {}), RecordUpdate(verify))
    with pytest.raises(ProtocolError, match="expired"):
        await store.apply("key", Message("old", "key", {"_df_expires_at": 0}), lambda state, now: [])


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
async def test_rollover_over_ten_thousand_signals_has_bounded_hot_state(backend, tmp_path):
    clock = ManualClock()
    store = (
        MemoryMessageStore(clock=clock)
        if backend == "memory"
        else SQLiteMessageStore(tmp_path / "runs.db", clock=clock)
    )
    topics = Topics()
    key = topics.instance("long-lived", "one")
    first_run = "initial"
    now = clock.now()
    epoch = int(now // DAY)
    seed = new_state(
        {"namespace": "default", "workflow_id": "one", "run_id": first_run, "manifest": {}, "input": 0}, now
    )

    def start(state, now):
        state.update(workflow="long-lived", current=first_run, runs={first_run: seed})
        return []

    await store.apply(key, Message("seed", key, {}), start)
    sizes = []
    for batch in range(11):

        def advance(aggregate, now, records):
            transition = Transition(aggregate, Message("batch", key, {}), now, topics, records=records)
            for number in range(1000):
                transition.signal(
                    {
                        "channel": "events",
                        "schema": "int",
                        "payload": number,
                        "signal_id": f"{batch}/{number}",
                        "signal_epoch": epoch,
                    }
                )
            node = {"id": "continue", "state": "pending", "spec": {"kind": "continue", "input": batch + 1}}
            transition.state["nodes"]["continue"] = node
            transition.rollover(node)
            transition.persist_runs()
            return transition.outgoing

        await store.apply(key, Message("batch", key, {}), RecordUpdate(advance))
        head = await store.read(key)
        assert len(head["runs"]) == 1
        assert head["runs"][head["current"]]["signal_keys"] == {}
        sizes.append(len(canonical(head)))
    assert max(sizes) - min(sizes) < 100

    def retry(aggregate, now, records):
        transition = Transition(aggregate, Message("retry", key, {}), now, topics, records=records)
        before = len(transition.state["history"])
        transition.signal(
            {"channel": "events", "schema": "int", "payload": 0, "signal_id": "0/0", "signal_epoch": epoch}
        )
        assert len(transition.state["history"]) == before
        assert transition.run(first_run)["status"] == "CONTINUED"
        transition.persist_runs()
        return []

    await store.apply(key, Message("retry", key, {}), RecordUpdate(retry))
    clock.advance(DETAIL_RETENTION + DAY)
    for _ in range(20):
        await store.collect_garbage(limit=1000, dry_run=False)

    def expired(aggregate, now, records):
        transition = Transition(aggregate, Message("retry", key, {}), now, topics, records=records)
        transition.signal(
            {
                "channel": "events",
                "schema": "int",
                "payload": 0,
                "signal_id": "0/0",
                "signal_epoch": epoch,
                "reply_to": "reply",
                "correlation_id": "old",
            }
        )
        assert transition.outgoing[0].message.body["error"] == "SignalExpired"
        assert transition.run(first_run) is None
        return []

    await store.apply(key, Message("check", key, {}), RecordUpdate(expired))
    await store.close()


async def test_task_gc_cannot_reexecute_old_work_and_long_timer_survives():
    from tests.test_engine import double, sequence

    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 1, request_id="gc")
        assert await env.run(handle) == 5
        await env.drain()
        executions = env.worker.metrics["executed"]
        original = next(
            Message.from_bytes(raw)
            for _, raw, props in env.transport.publications
            if not props and Message.from_bytes(raw).kind == "execute_task"
        )
        journal_key = env.worker.journal_key(original.body)
        assert (await env.journal.read(journal_key))["retired"]
        env.clock.advance(DETAIL_RETENTION + MESSAGE_HORIZON + 1)
        for _ in range(5):
            await env.journal.collect_garbage(limit=1000, dry_run=False)
        assert await env.journal.read(journal_key) == {}
        from duraflow.transport import Delivery

        delivery = Delivery(env.topics.task("double", 1), "tasks", original.to_bytes(), {}, None)
        # Use its registered route rather than relying on the task decorator name.
        delivery.topic = env.topics.task(original.body["ref"]["name"], original.body["ref"]["version"])
        with pytest.raises(ProtocolError, match="expired"):
            await env.worker.handle(original, delivery)
        assert env.worker.metrics["executed"] == executions

    @workflow(name="long-timer-retention", build_id="v1")
    async def timer(ctx: WorkflowContext, value: int) -> int:
        await ctx.sleep(30 * DAY)
        return value

    async with TestEnvironment(Registry(timer)) as env:
        handle = await env.client.start(timer, 42, request_id="timer")
        await env.drain()
        env.clock.advance(29 * DAY)
        await env.store.collect_garbage(dry_run=False)
        assert (await handle.describe())["status"] == "WAITING"
        env.clock.advance(DAY)
        assert await env.run(handle) == 42


async def test_inbox_expiration_and_local_heartbeats_do_not_accumulate():
    clock = ManualClock()
    store = MemoryMessageStore(clock=clock)
    incoming = Message("event", "key", {"_df_expires_at": clock.now() + 10})
    await store.apply("key", incoming, lambda state, now: [])
    for _ in range(100):
        await store.apply("key", Message("heartbeat", "key", {}), LocalUpdate(lambda state, now: []))
    assert len(store.inbox) == 1
    clock.advance(10)
    assert (await store.collect_garbage(dry_run=False))["inbox"] == 1
    with pytest.raises(ProtocolError, match="expired"):
        await store.apply("key", incoming, lambda state, now: [])


async def test_gc_does_not_starve_other_keys_behind_pending_outbox(journals):
    store, _, _ = journals

    def blocked(state, now, records):
        records.put("expired", {}, expires_at=0)
        return [Publication("topic", Message("event", "a-blocked", {}))]

    def free(state, now, records):
        records.put("expired", {}, expires_at=0)
        return []

    await store.apply("a-blocked", Message("seed", "a-blocked", {}), RecordUpdate(blocked))
    await store.apply("z-free", Message("seed", "z-free", {}), RecordUpdate(free))
    assert (await store.collect_garbage(limit=1, dry_run=False))["records"] == 1


async def test_legacy_retry_across_midnight_does_not_change_identity():
    from tests.test_message_runtime import APPROVAL, approval_flow

    async with TestEnvironment(Registry(approval_flow)) as env:
        handle = await env.client.start(approval_flow, 1, request_id="midnight")
        await env.drain()
        assert (await handle.signal(APPROVAL, True, signal_id="same"))["receivers"] == 1
        # Move only the SDK clock; the workflow's one-second timer stays pending.
        env.client.clock = ManualClock(env.clock.now() + DAY)
        assert await handle.signal(APPROVAL, True, signal_id="same") is None
        assert len(next(iter((await handle.describe())["channels"].values()))["received"]) == 1


async def test_legacy_signal_map_is_migrated_before_rollover():
    from duraflow.contracts import fingerprint

    topics = Topics()
    store = MemoryMessageStore()
    key = topics.instance("legacy", "one")
    state = new_state({"namespace": "default", "workflow_id": "one", "run_id": "old", "manifest": {}, "input": 0}, 1)
    state["signal_keys"] = {"signal": fingerprint(["events", "int", 1])}
    store.documents[key] = {"workflow": "legacy", "current": "old", "runs": {"old": state}}

    def transition(aggregate, now, records):
        t = Transition(aggregate, Message("test", key, {}), now, topics, records=records)
        node = {"id": "continue", "state": "pending", "spec": {"kind": "continue", "input": 1}}
        t.state["nodes"]["continue"] = node
        t.rollover(node)
        t.signal(
            {
                "channel": "events",
                "schema": "int",
                "payload_schema": "int",
                "payload": 1,
                "signal_id": "signal",
                "reply_to": "reply",
                "correlation_id": "retry",
            }
        )
        assert t.outgoing[-1].message.body["error"] is None
        assert t.outgoing[-1].message.body["value"] is None
        assert len(t.state["history"]) == 1
        t.persist_runs()
        return []

    await store.apply(key, Message("test", key, {}), RecordUpdate(transition))


async def test_retirement_after_retry_horizon_is_still_applied():
    from duraflow.broker import MemoryBroker
    from duraflow.task_worker import TaskWorker
    from duraflow.transport import Delivery
    from tests.test_engine import double

    clock = ManualClock()
    store = MemoryMessageStore(clock=clock)
    transport = MemoryBroker(clock=clock)
    worker = TaskWorker(transport, Registry(double), journal=store)
    request = {
        "dispatch_id": "dispatch",
        "admission_deadline": clock.now() + MESSAGE_HORIZON,
        "retain_for": DETAIL_RETENTION,
        "_df_expires_at": None,
    }
    key = worker.journal_key(request)
    await store.apply(key, Message("seed", key, {}), LocalUpdate(lambda state, now: []))
    clock.advance(MESSAGE_HORIZON + 1)
    ref = next(iter(worker.registry.tasks.values()))[0]
    message = Message("retire_task", "dispatch", request)
    await worker.handle(
        message, Delivery(worker.topics.task_control(ref.name, ref.version), "tasks", message.to_bytes(), {}, None)
    )
    assert (await store.read(key))["retired"]
    clock.advance(DETAIL_RETENTION + MESSAGE_HORIZON)
    assert (await store.collect_garbage(dry_run=False))["states"] == 1
    await worker.close()


async def test_in_progress_legacy_tag_page_keeps_snapshot_and_offset():
    from duraflow.broker import MemoryBroker
    from duraflow.contracts import fingerprint
    from duraflow.state import identity
    from duraflow.tag_engine import TagEngine
    from duraflow.transport import Delivery

    store = MemoryMessageStore()
    engine = TagEngine(store, MemoryBroker(), page_size=1)
    body = {
        "workflow": "workflow",
        "tag": "orders",
        "signal_id": "signal",
        "channel": "events",
        "schema": "int",
        "payload_schema": "int",
        "payload": 1,
    }
    key = "default/orders"
    store.documents["tag/" + key] = {
        "members": {"workflow": ["new-member"]},
        "jobs": {
            identity("workflow", "signal"): {
                "digest": fingerprint(body),
                "targets": ["first", "second"],
                "offset": 1,
                "complete": False,
            }
        },
    }
    message = Message("tag_signal", key, {**body, "offset": 1})
    await engine.handle(message, Delivery(engine.topics.tags(), "tags", message.to_bytes(), {}, None))
    outgoing = await store.claim("test")
    assert len(outgoing) == 1
    assert outgoing[0].message.key == engine.topics.instance("workflow", "second")


async def test_retirement_from_previous_timeline_does_not_suppress_restored_execution():
    from duraflow.transport import Delivery
    from tests.test_engine import double, sequence

    async with TestEnvironment(Registry(sequence, double)) as env:
        env.worker.accepting = False
        handle = await env.client.start(sequence, 1, request_id="restored")
        await env.drain()
        original = next(
            Message.from_bytes(raw)
            for _, raw, props in env.transport.publications
            if not props and Message.from_bytes(raw).kind == "execute_task"
        )
        request = original.body
        retirement = Message(
            "retire_task",
            original.key,
            {
                "dispatch_id": request["dispatch_id"],
                "admission_deadline": request["_df_expires_at"],
                "_df_expires_at": None,
            },
        )
        await env.worker.handle(
            retirement,
            Delivery(
                env.topics.task_control(request["ref"]["name"], request["ref"]["version"]),
                "tasks",
                retirement.to_bytes(),
                {},
                None,
            ),
        )
        env.worker.accepting = True
        assert await env.run(handle) == 5
        assert env.worker.metrics["executed"] == 2


async def test_heartbeat_protects_a_retired_but_still_running_task():
    from duraflow.broker import MemoryBroker
    from duraflow.task_worker import TaskContext, TaskWorker

    clock = ManualClock()
    store = MemoryMessageStore(clock=clock)
    worker = TaskWorker(MemoryBroker(clock=clock), Registry(), journal=store)
    request = {
        "dispatch_id": "dispatch",
        "task_id": "task",
        "attempt": 1,
        "run_id": "run",
        "node_id": "0",
        "namespace": "default",
    }
    key = worker.journal_key(request)
    store.documents[key] = {"generation": 1, "until": clock.now() + 2, "retired": True, "_gc_after": clock.now() + 1}
    context = TaskContext(worker, request, 1)
    assert await context.heartbeat()
    clock.advance(3)
    assert (await store.collect_garbage(dry_run=False))["states"] == 0
    assert await store.read(key)
    clock.advance(worker.lease_seconds)
    assert (await store.collect_garbage(dry_run=False))["states"] == 1
    assert not await context.heartbeat()
    assert await store.list_states(key) == []
    await worker.close()


async def test_missing_local_update_never_recreates_collected_state(journals):
    store, _, _ = journals

    def update(state, now):
        pytest.fail("Missing-state heartbeat must not run its callback")

    assert not await store.apply("collected", Message("heartbeat", "collected", {}), LocalUpdate(update, create=False))
    assert await store.list_states("collected") == []


async def test_task_tag_retry_preserves_job_identity_and_cancellation_deadline():
    from duraflow.broker import MemoryBroker
    from duraflow.task_tags import TaskTagEngine
    from duraflow.transport import Delivery

    clock = ManualClock()
    store = MemoryMessageStore(clock=clock)
    engine = TaskTagEngine(store, MemoryBroker(clock=clock))
    key = "task-tag-key"
    store.documents["task-tag/" + key] = {"members": {"dispatch": "task-control"}}
    original = Message("task_tag_cancel", key, {"request_id": "cancel", "_df_expires_at": clock.now() + 100})
    delivery = Delivery(engine.topics.task_tags(control=True), "tags", b"", {}, None)
    await engine.handle(original, delivery)
    clock.advance(1)
    await engine.handle(
        Message("task_tag_cancel", key, {"request_id": "cancel", "_df_expires_at": clock.now() + 100}), delivery
    )
    outgoing = await store.claim("relay")
    assert len(outgoing) == 1
    assert outgoing[0].message.expires_at() == original.expires_at()
    assert outgoing[0].message.kind == "cancel_task"


@pytest.mark.integration
async def test_postgres_admission_uses_time_after_parent_lock(monkeypatch):
    import asyncio
    import os
    from uuid import uuid4

    from sqlalchemy import select, text
    from duraflow.message_postgres import PostgresMessageStore

    url = os.getenv("DURAFLOW_TEST_POSTGRES")
    if not url:
        pytest.skip("Native PostgreSQL endpoint required")
    store = PostgresMessageStore(url, schema="admission_" + uuid4().hex[:12])
    await store.initialize()
    clock = ManualClock()

    async def now(conn):
        return clock.now()

    monkeypatch.setattr(store.database, "_now", now)
    task = None
    try:
        await store.apply("key", Message("seed", "key", {}), lambda state, now: [])
        async with store.database._transaction() as conn:
            await conn.execute(select(store.states.c.key).where(store.states.c.key == "key").with_for_update())
            task = asyncio.create_task(
                store.apply("key", Message("late", "key", {"_df_expires_at": clock.now() + 1}), lambda state, now: [])
            )
            await asyncio.sleep(0.05)
            assert not task.done()
            clock.advance(2)
        with pytest.raises(ProtocolError, match="expired"):
            await task
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        async with store.database._transaction() as conn:
            await conn.execute(text(f'DROP SCHEMA "{store.database.schema}" CASCADE'))
        await store.close()
