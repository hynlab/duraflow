"""Independent reproductions of DF-AUDIT-01 through DF-AUDIT-04.

Assertions describe the required behavior, so unfixed defects fail normally.
"""

import json
from copy import deepcopy
from dataclasses import asdict
from decimal import Decimal, localcontext
from unittest.mock import AsyncMock

import pytest

from duraflow import Registry, RetryPolicy, TaskFailure, TaskOptions, TaskRef, WorkflowContext, task, workflow
from duraflow.broker import MemoryBroker
from duraflow.contracts import MAX_PAYLOAD_BYTES, ManualClock, ProtocolError, canonical
from duraflow.executor import ProcessReplayExecutor
from duraflow.failures import command_failure
from duraflow.message_store import MemoryMessageStore, SQLiteMessageStore
from duraflow.messaging import Message, Topics
from duraflow.state import attempt
from duraflow.testing import TestEnvironment
from duraflow.transitions import collect, settle_task
from duraflow.workflow_engine import WorkflowEngine, new_state
from duraflow.workflow_replay import execute


@pytest.mark.parametrize("count", [3, 300])
@pytest.mark.parametrize("process", [False, True])
async def test_audit_01_fanout_compensation(count, process):
    from tests.audit_app import flow, registry

    env = TestEnvironment(registry)
    if process:
        env.workflows.replay_executor = ProcessReplayExecutor("tests.audit_app", workers=1)
    async with env:
        handle = await env.client.start(flow, count, request_id="fanout")
        assert await env.run(handle, steps=4000, timeout=30) == -1
        assert env.worker.metrics["executed"] == count


def collected_failure(failures):
    nodes = {str(i): {"state": "error", "error": deepcopy(error)} for i, error in enumerate(failures)}
    record = {"id": "group", "state": "pending", "members": list(nodes), "spec": {"kind": "gather"}}
    state = {"nodes": nodes, "commands": [record], "sequence": 0, "history": []}
    collect(state, 0)
    assert [node["error"] for node in nodes.values()] == failures
    return record["error"]


@pytest.mark.parametrize("margin", [-1, 0, 1])
def test_audit_01_exact_group_payload_boundary(margin):
    failures = [
        {"code": "UPSTREAM", "message": "first", "details": ""},
        {"code": "UPSTREAM", "message": "second"},
    ]
    full = {"code": "GROUP_FAILURE", "message": "One or more group members failed", "causes": failures}
    failures[0]["details"] = "x" * (MAX_PAYLOAD_BYTES + margin - len(canonical(full).encode()))
    for failure in failures:
        TaskFailure(failure)
    result = collected_failure(failures)
    assert TaskFailure(result).code == "GROUP_FAILURE"
    assert result == collected_failure(failures)
    if margin <= 0:
        assert result == full
        assert len(canonical(result).encode()) == MAX_PAYLOAD_BYTES + margin
    else:
        assert result["causes"] == failures[:1]
        assert result["omitted_causes"] == 1
        assert len(canonical(result).encode()) <= MAX_PAYLOAD_BYTES


@pytest.mark.parametrize("metadata", ["large-unicode", "deep"])
def test_audit_01_metadata_cannot_prevent_catching_group_failure(metadata):
    details = "끝" * 30000
    if metadata == "deep":
        details = "leaf"
        for _ in range(62):
            details = {"nested": details}
    failure = TaskFailure({"code": "UPSTREAM", "message": "failure", "details": details}).error
    result = collected_failure([failure, failure])
    assert TaskFailure(result).code == "GROUP_FAILURE"
    assert result["omitted_causes"] > 0
    assert len(result["causes"]) + result["omitted_causes"] == 2


@pytest.mark.parametrize("driver", ["inline", "process", "legacy"])
async def test_audit_01_pre_upgrade_oversized_history_replays_without_mutation(driver):
    from duraflow.replay import replay
    from tests.audit_app import flow, registry

    definition = registry.resolve(flow)
    state = new_state(
        {"namespace": "audit", "workflow_id": "old", "run_id": "old", "manifest": definition.manifest, "input": 300},
        0,
    )
    runner = replay if driver == "legacy" else execute
    scheduled = runner(definition, state)
    error = {
        "code": "GROUP_FAILURE",
        "message": "One or more group members failed",
        "causes": [{"code": "UPSTREAM", "message": "x" * 1000} for _ in range(300)],
    }
    state["commands"] = [
        {
            "id": "group",
            "spec": scheduled.value if driver == "legacy" else scheduled.value[0],
            "members": [str(i) for i in range(300)],
            "state": "error",
            "error": error,
        }
    ]
    before = deepcopy(state)
    if driver == "process":
        async with ProcessReplayExecutor("tests.audit_app", workers=1) as executor:
            result = await executor.execute(definition, state)
    else:
        result = runner(definition, state)
    assert (result.kind, result.value) == ("completed", -1)
    assert state == before


@pytest.mark.parametrize("kind,members", [("call", ["0"]), ("gather", ["0"]), ("race", ["0", "1"])])
def test_audit_01_single_remote_errors_keep_strict_payload_validation(kind, members):
    error = {
        "code": "GROUP_FAILURE",
        "message": "remote",
        "causes": [{"code": "UPSTREAM", "message": "x" * MAX_PAYLOAD_BYTES}],
    }
    with pytest.raises(ProtocolError):
        command_failure({"spec": {"kind": kind}, "members": members, "error": error})


@pytest.mark.parametrize("bad_name", [None, [], {}, 123, "bad/name", "-bad"])
async def test_audit_02_malformed_key_is_acked_and_quarantined(bad_name):
    @workflow(name="audit-route", build_id="audit-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return value

    topics = Topics(namespace="audit")
    broker, store = MemoryBroker(), MemoryMessageStore()
    engine = WorkflowEngine(store, broker, Registry(flow), topics=topics)
    topic = topics.commands("audit-route")
    key = json.dumps(["audit", bad_name, "request"])
    try:
        await engine.prepare()
        await broker.publish(topic, Message("start", key, {}).to_bytes(), {}, key=key)
        delivery = await broker.receive(topic, "state")
        assert delivery is not None
        broker.ack = AsyncMock(wraps=broker.ack)
        broker.nack = AsyncMock(wraps=broker.nack)
        caught = None
        try:
            await engine.process(delivery)
        except (TypeError, ValueError) as exc:
            caught = exc
        dlq = [p for p in broker.publications if p[0] == topic + "-dlq"]
        assert (len(dlq), broker.ack.await_count, broker.nack.await_count, caught) == (1, 1, 0, None)
    finally:
        await engine.close()
        await broker.close()
        await store.close()


@pytest.mark.parametrize("workflow_id", [None, [], {}, 123, "", "x" * 257])
async def test_audit_02_invalid_instance_id_is_quarantined(workflow_id):
    topics = Topics(namespace="audit")
    broker, store = MemoryBroker(), MemoryMessageStore()
    engine = WorkflowEngine(store, broker, ["audit-route"], topics=topics)
    topic = topics.commands("audit-route")
    key = json.dumps(["audit", "audit-route", workflow_id])
    try:
        await engine.prepare()
        await broker.publish(topic, Message("query", key, {}).to_bytes(), {}, key=key)
        await engine.step()
        assert engine.metrics["errors"] == 1
        assert len([p for p in broker.publications if p[0] == topic + "-dlq"]) == 1
        assert await broker.receive(topic, "state") is None
        assert await store.read(key) == {}
    finally:
        await engine.close()
        await broker.close()
        await store.close()


async def test_audit_02_transient_store_failure_remains_retryable(monkeypatch):
    topics = Topics(namespace="audit")
    broker, store = MemoryBroker(), MemoryMessageStore()
    engine = WorkflowEngine(store, broker, ["audit-route"], topics=topics)
    topic = topics.commands("audit-route")
    key = topics.instance("audit-route", "valid/instance")
    try:
        await engine.prepare()
        await broker.publish(topic, Message("query", key, {}).to_bytes(), {}, key=key)
        delivery = await broker.receive(topic, "state")
        monkeypatch.setattr(store, "apply", AsyncMock(side_effect=OSError("database unavailable")))
        monkeypatch.setattr(broker, "ack", AsyncMock(wraps=broker.ack))
        monkeypatch.setattr(broker, "nack", AsyncMock(wraps=broker.nack))
        with pytest.raises(OSError, match="database unavailable"):
            await engine.process(delivery)
        assert broker.nack.await_count == 1
        assert broker.ack.await_count == 0
        assert not any(p[0] == topic + "-dlq" for p in broker.publications)
    finally:
        await engine.close()
        await broker.close()
        await store.close()


@pytest.mark.parametrize(
    "number,multiplier,expected",
    [(3, 2.0, 4.0), (3, 1e200, 60.0), (40, 1.01, 1.01**39)],
)
def test_audit_03_04_bounded_backoff(number, multiplier, expected):
    policy = RetryPolicy(max_attempts=100, delay=1, multiplier=multiplier, max_delay=60)
    current = attempt(number, 0)
    current["observation"] = {"error": {"code": "TASK_ERROR", "message": "failure"}, "result": None}
    node = {
        "id": "task",
        "created_at": 0,
        "attempts": [current],
        "spec": {"options": asdict(TaskOptions(retry=policy))},
    }
    state = {"status": "WAITING", "sequence": 0, "history": []}
    settle_task(state, node, 0)
    assert node["attempts"][-1]["not_before"] == pytest.approx(expected)


@pytest.mark.parametrize(
    "delay,multiplier,cap,number",
    [
        (1e-300, 1e200, 60.0, 2),
        (1e-300, 1e200, 60.0, 3),
        (1e-300, 1e200, 60.0, 4),
        (1.0, 1.01, 60.0, 999),
        (1.0, 1.0, 60.0, 999),
        (100.0, 2.0, 60.0, 1),
        (5e-324, 2.0, 1e-320, 20),
        (1e308, 1e308, 1.7e308, 2),
    ],
)
def test_audit_03_04_backoff_matches_high_precision_reference(delay, multiplier, cap, number):
    policy = RetryPolicy(max_attempts=1000, delay=delay, multiplier=multiplier, max_delay=cap)
    current = attempt(number, 0)
    current["observation"] = {"error": {"code": "TASK_ERROR", "message": "failure"}, "result": None}
    node = {
        "id": "task",
        "created_at": 0,
        "attempts": [current],
        "spec": {"options": asdict(TaskOptions(retry=policy))},
    }
    state = {"status": "WAITING", "sequence": 0, "history": []}
    settle_task(state, node, 0)
    with localcontext() as context:
        context.prec = 80
        expected = float(min(Decimal(cap), Decimal(delay) * Decimal(multiplier) ** (number - 1)))
    assert node["attempts"][-1]["not_before"] == pytest.approx(expected, rel=1e-12, abs=0)


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("multiplier", [2.0, 1e200])
async def test_audit_03_retry_reaches_fourth_attempt(tmp_path, backend, multiplier):
    ref = TaskRef("audit-retry-task", int, int)
    calls = []

    @task(ref=ref)
    async def failing(ctx, value):
        calls.append(ctx.attempt)
        if ctx.attempt < 4:
            raise TaskFailure({"code": "TASK_ERROR", "message": "retry"})
        return value

    policy = RetryPolicy(max_attempts=4, delay=1, multiplier=multiplier, max_delay=60)

    @workflow(name="audit-retry", build_id="audit-v1")
    async def flow(ctx: WorkflowContext, value: int) -> int:
        return await ctx.call(ref, value, options=TaskOptions(retry=policy))

    clock = ManualClock()
    store = SQLiteMessageStore(tmp_path / "retry.db", clock=clock) if backend == "sqlite" else None
    async with TestEnvironment(Registry(flow, failing), store=store, clock=clock) as env:
        handle = await env.client.start(flow, 42, request_id="retry")
        for _ in range(5):
            await env.drain(steps=1000)
            env.clock.advance(61)
        await env.drain(steps=1000)
        state = await handle.describe()
        assert (calls, state["status"], state["error"]) == ([1, 2, 3, 4], "COMPLETED", None)
        assert await handle.result(timeout=1) == 42
