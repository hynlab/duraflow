"""Malformed service envelopes and lease fencing through protocol-2 consumers."""

import asyncio

import pytest

from duraflow import ManualClock, Registry, TaskRef, TaskWorker, Topics, task
from duraflow.broker import MemoryBroker
from duraflow.message_store import MemoryMessageStore
from duraflow.messaging import Message

REF = TaskRef("edge-task", int, int)


def request(topics):
    return {
        "ref": REF.descriptor(),
        "input": 7,
        "namespace": topics.namespace,
        "task_id": "task",
        "dispatch_id": "dispatch",
        "node_id": "0.0",
        "run_id": "run",
        "workflow_key": topics.instance("flow", "instance"),
        "reply_to": topics.workflow("flow"),
        "attempt": 1,
    }


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-ref-name",
        "missing-ref-version",
        "bad-version",
        "boolean-version",
        "missing-input",
        "wrong-namespace",
        "wrong-input",
        "boolean-attempt",
        "unknown-task",
        "wrong-key",
        "completion-ref",
    ],
)
async def test_invalid_service_message_is_quarantined_without_execution(mutation):
    calls = []

    @task(ref=REF)
    def implementation(value):
        calls.append(value)
        return value

    broker, topics = MemoryBroker(), Topics()
    worker = TaskWorker(broker, Registry(implementation), topics=topics)
    body, kind = request(topics), "execute_task"
    topic = topics.task(REF.name, REF.version)
    key = "dispatch"
    if mutation == "missing-ref-name":
        del body["ref"]["name"]
    elif mutation == "missing-ref-version":
        del body["ref"]["version"]
    elif mutation == "bad-version":
        body["ref"]["version"] = []
    elif mutation == "boolean-version":
        body["ref"]["version"] = True
    elif mutation == "missing-input":
        del body["input"]
    elif mutation == "wrong-namespace":
        body["namespace"] = "foreign"
    elif mutation == "wrong-input":
        body["input"] = {"unexpected": True}
    elif mutation == "boolean-attempt":
        body["attempt"] = True
    elif mutation == "unknown-task":
        body["ref"]["name"] = "unknown"
    elif mutation == "wrong-key":
        key = "mismatched"
    else:
        kind = "complete_task"
        topic = topics.task_completion(REF.name, REF.version)
        body = {
            "dispatch_id": "dispatch",
            "ref": {},
            "generation": 1,
            "secret": "x",
            "correlation_id": "c",
            "reply_to": "reply",
            "result": 7,
        }
    message = Message(kind, "dispatch", body)
    try:
        await worker.prepare()
        await broker.publish(topic, message.to_bytes(), {}, key=key)
        assert await worker.step()
        assert calls == []
        assert worker.metrics["errors"] == 1
        assert not broker.inflight
        assert sum(t.endswith("-dlq") for t, _, _ in broker.publications) == 1
    finally:
        await worker.close()
        await broker.close()


@pytest.mark.parametrize("mode", ["success", "cancel", "heartbeat-loss", "stale-generation"])
async def test_live_task_duplicates_and_fenced_owner_results(mode):
    clock = ManualClock()
    broker, journal, topics = MemoryBroker(clock=clock), MemoryMessageStore(clock=clock), Topics()
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    @task(ref=REF)
    async def implementation(ctx, value):
        calls.append(value)
        started.set()
        await release.wait()
        return value

    worker = TaskWorker(broker, Registry(implementation), topics=topics, journal=journal, lease_seconds=0.3)
    body = request(topics)
    message = Message("execute_task", "dispatch", body)
    topic = topics.task(REF.name, REF.version)
    running = None
    try:
        await worker.prepare()
        await broker.publish(topic, message.to_bytes(), {}, key=message.key)
        delivery = await broker.receive(topic, "tasks")
        running = asyncio.create_task(worker.process(delivery))
        await asyncio.wait_for(started.wait(), 1)
        # Another physical delivery of the same task must not run concurrently.
        await broker.publish(topic, message.to_bytes(), {}, key=message.key)
        duplicate = await broker.receive(topic, "tasks")
        await worker.process(duplicate)
        assert calls == [7]
        key = worker.journal_key(body)
        if mode == "cancel":
            cancel = Message("cancel_task", "dispatch", {"dispatch_id": "dispatch"})
            route = topics.task_control(REF.name, REF.version)
            await broker.publish(route, cancel.to_bytes(), {}, key=cancel.key)
            await worker.process(await broker.receive(route, "tasks"))
        elif mode == "heartbeat-loss":
            clock.advance(1)
            await asyncio.sleep(0.15)
        elif mode == "stale-generation":

            def takeover(state, now):
                state["generation"] += 1
                return []

            await journal.apply(key, Message("takeover", "dispatch", {}), takeover)
        release.set()
        await asyncio.wait_for(running, 1)
        state = await journal.read(key)
        if mode in {"heartbeat-loss", "stale-generation"}:
            assert "outcome" not in state
        elif mode == "cancel":
            assert state["outcome"]["error"]["code"] == "CANCELLED"
        else:
            assert state["outcome"] == {"result": 7, "error": None}
            clock.advance(0.02)
            await worker.process(await broker.receive(topic, "tasks"))
            assert calls == [7]
            assert (await journal.read(key))["outcome"] == state["outcome"]
    finally:
        release.set()
        if running is not None and not running.done():
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
        await worker.close()
        await broker.close()
