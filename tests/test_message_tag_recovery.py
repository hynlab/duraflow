"""Task-tag fan-out resumes across replacement and does not resurrect closed tasks."""

from duraflow import Topics
from duraflow.broker import MemoryBroker
from duraflow.contracts import canonical
from duraflow.message_store import MemoryMessageStore
from duraflow.messaging import Message
from duraflow.task_tags import TaskTagEngine


async def test_paginated_task_tag_cancellation_restart_and_duplicate_controls():
    topics, broker, store = Topics(), MemoryBroker(), MemoryMessageStore()
    engine = TaskTagEngine(store, broker, topics=topics, page_size=1)
    key = canonical([topics.namespace, "service", 1, "group"])
    await engine.prepare()

    async def send(kind, body, *, control=False):
        message = Message(kind, key, body)
        await broker.publish(topics.task_tags(control=control), message.to_bytes(), {}, key=key)
        return message

    await send("task_tag_remove", {"dispatch_id": "closed"})
    for identity in ("closed", "a", "b", "c"):
        await send("task_tag_add", {"dispatch_id": identity, "topic": topics.task_control("service", 1)})
    for _ in range(5):
        assert await engine.step()
    assert set((await store.read("task-tag/" + key))["members"]) == {"a", "b", "c"}
    command = await send("task_tag_cancel", {"request_id": "stop"}, control=True)
    assert await engine.step()
    # Replace after the first page has committed, before its outbox is sent.
    await engine.close()
    engine = TaskTagEngine(store, broker, topics=topics, page_size=1)
    for _ in range(10):
        if not await engine.step():
            break
    else:
        raise AssertionError("Fan-out did not drain")
    cancellation = [
        Message.from_bytes(data) for _, data, _ in broker.publications if Message.from_bytes(data).kind == "cancel_task"
    ]
    assert sorted(message.body["dispatch_id"] for message in cancellation) == ["a", "b", "c"]
    assert (await store.read("task-tag/" + key))["jobs"]["stop"]["offset"] == 3
    await broker.publish(topics.task_tags(control=True), command.to_bytes(), {}, key=key)
    await send("task_tag_cancel", {"request_id": "stop", "offset": 0}, control=True)
    await send("task_tag_cancel", {"request_id": "stop", "changed": True}, control=True)
    for _ in range(6):
        if not await engine.step():
            break
    assert engine.metrics["errors"] == 1
    assert len([1 for topic, _, _ in broker.publications if topic == topics.task_control("service", 1)]) == 3
    await engine.close()
    await broker.close()
