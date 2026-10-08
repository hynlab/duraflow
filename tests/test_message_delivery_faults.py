"""Commit/ACK/publication ambiguity with real handlers and retained messages."""

import json

import pytest

from duraflow import ManualClock, ProtocolError, Topics
from duraflow.broker import MemoryBroker
from duraflow.message_runtime import Consumer, OutboxRelay
from duraflow.message_store import MemoryMessageStore
from duraflow.messaging import MAX_MESSAGE_BYTES, Message, Publication


class Counter(Consumer):
    def __init__(self, broker, store):
        super().__init__(broker, Topics())
        self.store = store
        self.routes = [("commands", "state", True)]

    async def handle(self, message, delivery):
        message.require(value=int)

        def update(state, now):
            state["total"] = state.get("total", 0) + message.body["value"]
            return []

        await self.store.apply(message.key, message, update)


@pytest.mark.parametrize("point", ["before_publish", "after_publish", "before_mark"])
async def test_ambiguous_publication_retries_stable_identity(monkeypatch, point):
    clock = ManualClock()
    broker, store, receiver = (
        MemoryBroker(clock=clock),
        MemoryMessageStore(clock=clock),
        MemoryMessageStore(clock=clock),
    )
    worker = Counter(broker, receiver)
    await worker.prepare()
    outgoing = Message("increment", "key", {"value": 7})
    await store.apply("source", Message("seed", "source", {}), lambda state, now: [Publication("commands", outgoing)])
    original_publish, original_mark = broker.publish, store.delivered
    failed = False

    async def publish(*args, **kwargs):
        nonlocal failed
        if point == "before_publish" and not failed:
            failed = True
            raise ConnectionError("send failed")
        await original_publish(*args, **kwargs)
        if point == "after_publish" and not failed:
            failed = True
            raise ConnectionError("send succeeded but response lost")

    async def delivered(*args):
        nonlocal failed
        if point == "before_mark" and not failed:
            failed = True
            raise ConnectionError("journal unavailable after send")
        await original_mark(*args)

    monkeypatch.setattr(broker, "publish", publish)
    monkeypatch.setattr(store, "delivered", delivered)
    await OutboxRelay(store, broker).step()
    assert failed and outgoing.id in store.outbox
    clock.advance(1)
    await OutboxRelay(store, broker).step()
    while await worker.step():
        pass
    assert await receiver.read("key") == {"total": 7}
    assert store.outbox == {}
    assert all(Message.from_bytes(data).id == outgoing.id for _, data, _ in broker.publications)
    assert len(broker.publications) == (1 if point == "before_publish" else 2)


async def test_committed_message_redelivery_after_ack_failure(monkeypatch):
    clock = ManualClock()
    broker, store = MemoryBroker(clock=clock), MemoryMessageStore(clock=clock)
    worker = Counter(broker, store)
    await worker.prepare()
    message = Message("increment", "key", {"value": 9})
    await broker.publish("commands", message.to_bytes(), {}, key=message.key)
    original = broker.ack

    async def fail(delivery):
        raise ConnectionError("ACK unavailable")

    monkeypatch.setattr(broker, "ack", fail)
    with pytest.raises(ConnectionError):
        await worker.step()
    assert await store.read("key") == {"total": 9}
    monkeypatch.setattr(broker, "ack", original)
    clock.advance(0.02)
    assert await worker.step()
    assert await store.read("key") == {"total": 9}
    assert not broker.inflight


@pytest.mark.parametrize("raw", [b"null", b"[]", b"1", b"{", b"\xff", b"{}", b'{"version":2}'])
def test_malformed_envelopes_raise_protocol_error(raw):
    with pytest.raises(ProtocolError):
        Message.from_bytes(raw)


@pytest.mark.parametrize(
    "field,value",
    [("version", True), ("version", 2.0), ("version", 1), ("kind", 2), ("body", []), ("id", ""), ("key", "")],
)
def test_envelope_field_validation(field, value):
    document = json.loads(Message("test", "key", {}).to_bytes())
    document[field] = value
    with pytest.raises(ProtocolError):
        Message.from_bytes(json.dumps(document).encode())


def test_message_size_limit_and_unicode_roundtrip():
    message = Message("test", "key", {"text": ""}, id="stable")
    overhead = len(message.to_bytes())
    exact = Message("test", "key", {"text": "a" * (MAX_MESSAGE_BYTES - overhead)}, id="stable")
    assert len(exact.to_bytes()) == MAX_MESSAGE_BYTES
    assert Message.from_bytes(exact.to_bytes()) == exact
    with pytest.raises(ProtocolError):
        Message("test", "key", {"text": exact.body["text"] + "a"}, id="stable").to_bytes()
    with pytest.raises(ProtocolError):
        Message.from_bytes(b" " * (MAX_MESSAGE_BYTES + 1))
    unicode_message = Message("test", "key", {"text": "한글 🦫 e\u0301"})
    assert Message.from_bytes(unicode_message.to_bytes()) == unicode_message


async def test_dlq_failure_retains_poison_message_then_healthy_work_proceeds(monkeypatch):
    clock = ManualClock()
    broker, store = MemoryBroker(clock=clock), MemoryMessageStore(clock=clock)
    worker = Counter(broker, store)
    await worker.prepare()
    await broker.publish("commands", b"invalid", {}, key="poison")
    original = broker.publish

    async def publish(topic, *args, **kwargs):
        if topic.endswith("-dlq"):
            raise ConnectionError("DLQ unavailable")
        await original(topic, *args, **kwargs)

    monkeypatch.setattr(broker, "publish", publish)
    with pytest.raises(ConnectionError):
        await worker.step()
    # A failed quarantine must be redelivered, not left permanently in flight.
    assert not broker.inflight
    monkeypatch.setattr(broker, "publish", original)
    clock.advance(0.02)
    assert await worker.step()
    valid = Message("increment", "healthy", {"value": 1})
    await broker.publish("commands", valid.to_bytes(), {}, key=valid.key)
    assert await worker.step()
    assert await store.read("healthy") == {"total": 1}
    assert worker.metrics["errors"] == 1


async def test_early_delivery_retains_identity_until_due():
    clock = ManualClock()
    broker = MemoryBroker(clock=clock)
    await broker.ensure("timers", "timers")
    message = Message("timer", "key", {})
    await broker.publish("timers", message.to_bytes(), {}, key="key", deliver_at=clock.now() + 10)
    assert await broker.receive("timers", "timers") is None
    clock.advance(10)
    delivery = await broker.receive("timers", "timers")
    assert Message.from_bytes(delivery.data) == message
    await broker.ack(delivery)


async def test_stale_outbox_owner_cannot_finish_reclaimed_work():
    clock = ManualClock()
    store = MemoryMessageStore(clock=clock)
    message = Message("result", "key", {})
    await store.apply("key", Message("seed", "key", {}), lambda state, now: [Publication("events", message)])
    assert len(await store.claim("old")) == 1
    clock.advance(30)
    assert len(await store.claim("new")) == 1
    await store.delivered(message.id, "old")
    await store.release(message.id, "old")
    assert store.outbox[message.id]["owner"] == "new"
    await store.delivered(message.id, "new")
    assert not store.outbox
