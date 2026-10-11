"""Positive regressions for the nine 2026-10-11 reliability review findings."""

import asyncio
import os
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

from duraflow import (
    BroadcastBinding,
    Client,
    HandlerRef,
    ManualClock,
    NotFound,
    ProtocolError,
    Registry,
    TaskOptions,
    TaskRef,
    TaskWorker,
    TopicRef,
    WorkflowContext,
    task,
    workflow,
)
from duraflow.broker import MemoryBroker
from duraflow.contracts import Conflict
from duraflow.message_runtime import Consumer
from duraflow.message_store import MemoryMessageStore, SQLiteMessageStore
from duraflow.messaging import Message, Publication, Topics
from duraflow.observability import HealthState, render_metrics
from duraflow.task_worker import TaskContext
from duraflow.testing import TestEnvironment
from tests.test_message_store_contract import journals as journals
from tests.test_message_worker_edges import REF, request


async def pump(*roles, limit=300):
    for _ in range(limit):
        worked = False
        for role in roles:
            worked = await role.step() or worked
        if not worked:
            return
    raise AssertionError("Runtime did not become quiescent")


async def close(env):
    for role in env.runtimes:
        await role.close()
    await env.client.close()
    await env.transport.close()
    await env.store.close()
    await env.journal.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("broadcast", [False, True])
@pytest.mark.parametrize("advance", [5, 60])
async def test_offline_task_deadlines_prevent_late_first_effect(backend, broadcast, advance, tmp_path):
    ref = TaskRef("review-deadline-task", int, int)
    topic = TopicRef("persistent://public/default/review-deadline", int)
    handler = HandlerRef(ref, "review-participant")
    effects = []

    @task(ref=ref)
    async def implementation(value):
        effects.append(value)
        return value

    @workflow(name="review-deadline", build_id="v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        options = TaskOptions(schedule_timeout=5, overall_timeout=10)
        if broadcast:
            return (await ctx.broadcast(topic, value, handlers=(handler,), options=options))[handler]
        return await ctx.call(ref, value, options=options)

    clock = ManualClock()
    store = (
        MemoryMessageStore(clock=clock)
        if backend == "memory"
        else SQLiteMessageStore(tmp_path / "flow.db", clock=clock)
    )
    journal = (
        MemoryMessageStore(clock=clock)
        if backend == "memory"
        else SQLiteMessageStore(tmp_path / "task.db", clock=clock)
    )
    env = TestEnvironment(
        Registry(flow, implementation),
        clock=clock,
        store=store,
        journal=journal,
        broadcasts=(BroadcastBinding(topic, handler),),
    )
    try:
        for role in env.runtimes:
            await role.prepare()
        handle = await env.client.dispatch(flow, 42, request_id="offline")
        await pump(env.engine, env.workflows)
        timers = [t for t, _, _ in env.transport.publications if t == env.topics.topic("timer", "review-deadline")]
        assert len(timers) == 2
        assert not broadcast or not any(
            Message.from_bytes(d).kind == "execute_task" for t, d, _ in env.transport.publications if t != topic.name
        )
        clock.advance(advance)
        await pump(env.engine, env.workflows)
        key = env.topics.instance("review-deadline", "offline")
        assert (await store.read(key))["runs"][handle.run_id]["status"] == "FAILED"
        # Exercise only the data lane first: execution expiry must work even if
        # cancellation delivery is delayed or reordered by the broker.
        await env.worker.consume(env.worker.execution_routes)
        await pump(env.worker, env.engine, env.workflows)
        assert effects == []
        assert env.worker.metrics["errors"] == 0
    finally:
        await close(env)


@pytest.mark.parametrize("concurrency", [1, 8])
@pytest.mark.parametrize("native", [False, pytest.param(True, marks=pytest.mark.integration)])
async def test_saturation_does_not_starve_control_or_progress(concurrency, native):
    contexts, started, release = [], asyncio.Event(), asyncio.Event()
    tokens = []

    @task(ref=REF)
    async def implementation(ctx, value):
        if value == -1:
            marker = await ctx.defer(timeout=60)
            tokens.append(marker.token)
            return marker
        contexts.append(ctx)
        await ctx.heartbeat(63)
        if len(contexts) == concurrency:
            started.set()
        await release.wait()
        return value

    broker, journal, topics = MemoryBroker(), MemoryMessageStore(), Topics()
    if native:
        if not (os.getenv("DURAFLOW_TEST_POSTGRES") and os.getenv("DURAFLOW_TEST_PULSAR")):
            pytest.skip("Native PostgreSQL/Pulsar endpoints required")
        from duraflow.message_postgres import PostgresMessageStore
        from duraflow.transport import PulsarTransport

        topics = Topics(namespace="saturation-" + uuid4().hex[:12])
        journal = PostgresMessageStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema="saturation_" + uuid4().hex[:12])
        await journal.initialize()
        broker = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"])
    worker = TaskWorker(
        broker,
        Registry(implementation),
        topics=topics,
        journal=journal,
        concurrency=concurrency,
        lease_seconds=3 if native else 0.3,
    )
    client = Client(broker, topics=topics, timeout=10)
    stop = asyncio.Event()
    running = None
    try:
        await worker.prepare()
        body = request(topics)
        deferred = Message(
            "execute_task", "deferred", {**body, "dispatch_id": "deferred", "task_id": "deferred", "input": -1}
        )
        await broker.publish(topics.task(REF.name, 1), deferred.to_bytes(), {}, key=deferred.key)
        running = asyncio.create_task(worker.run(stop, poll_interval=0.001))
        async with asyncio.timeout(10):
            while not tokens:
                await asyncio.sleep(0.01)
        for i in range(concurrency):
            dispatch = f"dispatch-{i}"
            message = Message("execute_task", dispatch, {**body, "dispatch_id": dispatch, "task_id": dispatch})
            await broker.publish(topics.task(REF.name, 1), message.to_bytes(), {}, key=dispatch)
        await asyncio.wait_for(started.wait(), 10)
        await asyncio.sleep(1.1 if native else 0.15)
        assert await client.complete_external(tokens[0], 99, ref=REF)
        first = contexts[0]
        key = worker.journal_key(first.request)
        assert (await journal.read(key))["progress"] == 63
        cancel = Message("cancel_task", first.request["dispatch_id"], {"dispatch_id": first.request["dispatch_id"]})
        await broker.publish(topics.task_control(REF.name, 1), cancel.to_bytes(), {}, key=cancel.key)
        async with asyncio.timeout(10):
            while "outcome" not in (state := await journal.read(key)):
                await asyncio.sleep(0.01)
        assert state["cancelled"] and state["outcome"]["error"]["code"] == "CANCELLED"
        assert first.cancel_requested
    finally:
        release.set()
        stop.set()
        if running:
            await asyncio.wait_for(running, 10)
        await worker.close()
        await client.close()
        await broker.close()
        if native:
            from sqlalchemy import text

            async with journal.database._transaction() as conn:
                await conn.execute(text(f'DROP SCHEMA "{journal.database.schema}" CASCADE'))
        await journal.close()


async def test_heartbeat_preserves_progress_across_journal_backends(journals):
    journal, _, _ = journals
    worker = TaskWorker(MemoryBroker(), Registry(), journal=journal)
    body = request(Topics())

    def seed(state, now):
        state.update(generation=1, until=now + 30)
        return []

    try:
        await journal.apply(worker.journal_key(body), Message("seed", "key", {}), seed)
        ctx = TaskContext(worker, body, 1)
        assert await ctx.heartbeat(63)
        assert await ctx.heartbeat()
        assert (await journal.read(worker.journal_key(body)))["progress"] == 63
    finally:
        await worker.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("run_id", ["does-not-exist", ""])
async def test_unknown_archive_target_cannot_change_current_run(backend, run_id, tmp_path):
    @workflow(name="review-archive", build_id="v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return value

    clock = ManualClock()
    store = (
        MemoryMessageStore(clock=clock)
        if backend == "memory"
        else SQLiteMessageStore(tmp_path / "archive.db", clock=clock)
    )
    async with TestEnvironment(Registry(flow), store=store, clock=clock) as env:
        handle = await env.client.start(flow, 42, request_id="archive")
        assert await env.run(handle) == 42
        clock.advance(200 * 86400)
        before = await handle.describe()
        with pytest.raises(NotFound):
            await env.client.request(
                "review-archive",
                "archive",
                "control",
                {
                    "action": "archive",
                    "run_id": run_id,
                    "actor": "test",
                    "reason": "review",
                    "request_id": "wrong-archive",
                    "retention": 100 * 86400,
                    "safety_horizon": 90 * 86400,
                },
            )
        assert await handle.describe() == before
        assert await handle.result(timeout=1) == 42


@pytest.mark.parametrize("error", [{"message": "provider failed"}, {"code": ""}, {"code": 3, "message": "bad"}, []])
async def test_invalid_external_error_preserves_completion_opportunity(error):
    tokens = []

    @task(ref=REF)
    async def implementation(ctx, value):
        marker = await ctx.defer(timeout=100)
        tokens.append(marker.token)
        return marker

    @workflow(name="review-external", build_id="v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(REF, value)

    async with TestEnvironment(Registry(flow, implementation)) as env:
        handle = await env.client.start(flow, 7, request_id="external")
        await env.drain()
        with pytest.raises(ProtocolError):
            await env.client.complete_external(tokens[0], ref=REF, error=error)
        # Bypass SDK validation as an old or foreign callback producer would.
        import base64
        import json

        body = json.loads(base64.urlsafe_b64decode(tokens[0]))
        with pytest.raises(ProtocolError):
            await env.client.request(
                "external",
                body["dispatch_id"],
                "complete_task",
                {**body, "result": None, "error": error},
                topic=env.topics.task_completion(REF.name, 1),
            )
        state = await env.journal.read(env.worker.journal_key(body))
        assert "outcome" not in state
        assert await env.client.complete_external(tokens[0], 99, ref=REF)
        assert await env.run(handle) == 99
        assert env.engine.metrics["errors"] == 0


async def test_permanent_inbox_conflict_is_quarantined_but_concurrency_conflict_retries(journals):
    store, _, _ = journals
    broker = MemoryBroker()

    class Handler(Consumer):
        async def handle(self, message, delivery):
            if message.body.get("transient"):
                raise Conflict("Optimistic transaction conflict")
            await store.apply("key", message, lambda state, now: [])

    consumer = Handler(broker, Topics())
    consumer.routes = [("input", "test", False)]
    await consumer.prepare()
    for body in ({}, {"different": True}):
        message = Message("input", "key", body, id="reused")
        await broker.publish("input", message.to_bytes(), {}, key="key")
        assert await consumer.step()
    assert consumer.metrics == {"messages": 1, "errors": 1}
    assert not broker.queues["input", "test"]
    message = Message("input", "key", {"transient": True})
    await broker.publish("input", message.to_bytes(), {}, key="key")
    with pytest.raises(Conflict):
        await consumer.step()
    assert len(broker.queues["input", "test"]) == 1
    assert consumer.metrics["errors"] == 1


async def test_protocol2_telemetry_projects_status_and_outbox_atomically(journals):
    store, _, _ = journals

    def seed(state, now):
        state.update(current="run", runs={"run": {"namespace": "metrics", "status": "WAITING"}})
        return [Publication("topic", Message("event", "key", {}))]

    await store.apply("key", Message("seed", "key", {}), seed)
    stats = await store.telemetry("metrics")
    assert stats["version"] == 2 and stats["statuses"] == {"WAITING": 1}
    assert stats["outbox_age"] >= 0 and stats["observed_at"] > 0
    assert (await store.telemetry("other"))["statuses"] == {}

    def fail(state, now):
        state["runs"]["run"]["status"] = "FAILED"
        raise Conflict("rollback")

    with pytest.raises(Conflict):
        await store.apply("key", Message("fail", "key", {}), fail)
    assert (await store.telemetry("metrics"))["statuses"] == {"WAITING": 1}
    health = HealthState(stats=stats, telemetry_available=True, telemetry_checked_at=time.monotonic())
    runtime = SimpleNamespace(metrics={})
    assert 'status="WAITING"} 1' in render_metrics(runtime, health, "workflow-engine")
    health.telemetry_checked_at -= 60
    metrics = render_metrics(runtime, health, "workflow-engine")
    assert 'status="WAITING"} NaN' in metrics
    assert 'duraflow_telemetry_available{role="workflow-engine"} 0' in metrics


@pytest.mark.parametrize("version", [1, 2])
async def test_sqlite_telemetry_upgrade_preserves_existing_history(version, tmp_path):
    import sqlite3
    from contextlib import closing
    from duraflow.contracts import canonical

    path = tmp_path / "upgrade.db"
    state = {"current": "run", "runs": {"run": {"namespace": "review", "status": "WAITING"}}}
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.executescript("""
            CREATE TABLE message_schema_version(version INTEGER PRIMARY KEY);
            CREATE TABLE message_states(key TEXT PRIMARY KEY, document TEXT NOT NULL);
            CREATE TABLE message_inbox(key TEXT, id TEXT, digest TEXT NOT NULL, PRIMARY KEY(key,id));
            CREATE TABLE message_outbox(id TEXT PRIMARY KEY, document TEXT NOT NULL, owner TEXT, lease_until REAL NOT NULL DEFAULT 0);
        """)
        conn.execute("INSERT INTO message_schema_version VALUES(?)", (version,))
        conn.execute("INSERT INTO message_states VALUES(?,?)", ("key", canonical(state)))
        if version == 2:
            conn.execute("ALTER TABLE message_states ADD COLUMN expires_at REAL")
            conn.execute("ALTER TABLE message_inbox ADD COLUMN expires_at REAL")
            conn.execute("ALTER TABLE message_outbox ADD COLUMN state_key TEXT")
    store = SQLiteMessageStore(path)
    assert await store.read("key") == state
    assert (await store.telemetry("review"))["statuses"] == {"WAITING": 1}
    await store.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("real_limit", [False, True])
async def test_valid_large_results_are_retained_in_recoverable_blocked_state(
    backend, real_limit, tmp_path, monkeypatch
):
    import duraflow.messaging as messaging

    if not real_limit:
        monkeypatch.setattr(messaging, "MAX_MESSAGE_BYTES", 96 * 1024)
    size = 200 * 1024 if real_limit else 16 * 1024
    ref = TaskRef("review-large-task", int, str)
    calls = []

    @task(ref=ref)
    async def implementation(value):
        calls.append(value)
        return "x" * size

    @workflow(name="review-large", build_id="v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        for i in range(value):
            await ctx.call(ref, i)
        return value

    clock = ManualClock()
    store = (
        MemoryMessageStore(clock=clock)
        if backend == "memory"
        else SQLiteMessageStore(tmp_path / "large.db", clock=clock)
    )
    env = TestEnvironment(Registry(flow, implementation), clock=clock, store=store)
    try:
        for role in env.runtimes:
            await role.prepare()
        handle = await env.client.dispatch(flow, 35, request_id="large")
        for _ in range(400):
            worked = False
            for role in (env.engine, env.workflows, env.worker):
                worked = await role.step() or worked
            # Release observation-only copies, never queued/inflight work.
            env.transport.records.clear()
            env.transport.publications.clear()
            if not worked:
                break
        aggregate = await store.read(env.topics.instance("review-large", "large"))
        state = aggregate["runs"][handle.run_id]
        assert state["status"] == "BLOCKED" and state["blocked_reason"] == "SNAPSHOT_LIMIT"
        assert aggregate["active"] is None and env.engine.metrics["errors"] == 0
        last = state["nodes"][state["commands"][-1]["members"][0]]
        assert last["state"] == "done" and last["result"] == "x" * size
        assert last["attempts"][-1]["observation"]["result"] == last["result"]
        # Oversize diagnostics must remain queryable without dropping details in storage.
        query = asyncio.create_task(handle.describe())
        for _ in range(100):
            await env.engine.step()
            await asyncio.sleep(0.005)
            if query.done():
                break
        described = await asyncio.wait_for(query, 1)
        assert described["blocked_reason"] == "SNAPSHOT_LIMIT"
        assert described["details_omitted"] == "SNAPSHOT_LIMIT"
        if not real_limit:
            # Simulate deploying a snapshot-capable runtime and use the public
            # audited resume path. Previously accepted effects must not repeat.
            monkeypatch.setattr(messaging, "MAX_MESSAGE_BYTES", 16 * 1024 * 1024)
            resume = asyncio.create_task(
                handle.resume_blocked(actor="test", reason="capacity restored", request_id="resume")
            )
            for _ in range(300):
                await pump(env.engine, env.workflows, env.worker)
                await asyncio.sleep(0.005)
                if resume.done():
                    break
            await asyncio.wait_for(resume, 1)
            await pump(env.engine, env.workflows, env.worker)
            assert calls == list(range(35))
            aggregate = await store.read(env.topics.instance("review-large", "large"))
            assert aggregate["runs"][handle.run_id]["status"] == "COMPLETED"
    finally:
        await close(env)


@pytest.mark.parametrize("stall", ["deadline", "lease", "missing", "cancelled"])
async def test_admission_rechecked_after_slow_publication(stall, monkeypatch):
    effects = []

    @task(ref=REF)
    async def implementation(value):
        effects.append(value)
        return value

    clock, topics = ManualClock(), Topics()
    broker, journal = MemoryBroker(clock=clock), MemoryMessageStore(clock=clock)
    worker = TaskWorker(broker, Registry(implementation), journal=journal)
    body = {**request(topics), "start_deadline": clock.now() + 5, "start_deadline_code": "SCHEDULE_TIMEOUT"}
    original = worker.relay.step
    calls = 0

    async def stall_once():
        nonlocal calls
        calls += 1
        if calls == 1:
            if stall == "missing":
                journal.documents.pop(worker.journal_key(body))
            elif stall == "cancelled":
                journal.documents[worker.journal_key(body)]["cancelled"] = True
            else:
                clock.advance(5 if stall == "deadline" else 31)
        return await original()

    monkeypatch.setattr(worker.relay, "step", stall_once)
    try:
        await worker.prepare()
        message = Message("execute_task", "dispatch", body)
        await broker.publish(topics.task(REF.name, 1), message.to_bytes(), {}, key="dispatch")
        await worker.consume(worker.execution_routes)
        assert effects == []
        if stall == "deadline":
            assert (await journal.read(worker.journal_key(body)))["outcome"]["error"]["code"] == "SCHEDULE_TIMEOUT"
        assert worker.metrics["errors"] == 0
    finally:
        await worker.close()
