import asyncio
import sys
import threading
from types import SimpleNamespace

import pytest

from duraflow.client import Client
from duraflow.transport import MemoryTransport, PulsarTransport


def native_transport(monkeypatch, **options):
    producers = []

    class Producer:
        def __init__(self):
            self.closed = False
            self.entered = threading.Event()
            self.release = threading.Event()
            self.release.set()

        def send(self, *args, **kwargs):
            self.entered.set()
            assert self.release.wait(5)
            assert not self.closed

        def close(self):
            self.closed = True

    class Native:
        def __init__(self, *args, **kwargs):
            pass

        def create_producer(self, *args, **kwargs):
            producer = Producer()
            producers.append(producer)
            return producer

        def subscribe(self, *args, **kwargs):
            return SimpleNamespace(close=lambda: None, unsubscribe=lambda: None)

        def close(self):
            for producer in producers:
                producer.close()

    monkeypatch.setitem(
        sys.modules,
        "pulsar",
        SimpleNamespace(
            Client=Native,
            ConsumerType=SimpleNamespace(Shared=0, KeyShared=1),
            InitialPosition=SimpleNamespace(Earliest=0),
        ),
    )
    return PulsarTransport("pulsar://test", **options), producers


async def test_reply_route_churn_exceeds_original_limit(monkeypatch):
    transport, producers = native_transport(monkeypatch, max_routes=3)
    try:
        await transport.ensure("workflow", "state", ordered=True)
        for index in range(300):
            topic = f"reply-{index}"
            await transport.ensure(topic, "client")
            await transport.publish(topic, b"response", {})
        assert len(transport.provisioned) <= 3
        assert len(transport.producers) <= 3
        assert ("workflow", "state") in transport.ordered
        assert sum(not p.closed for p in producers) == 3
    finally:
        await transport.close()


async def test_cancelled_send_pins_producer_until_native_completion(monkeypatch):
    transport, producers = native_transport(monkeypatch, max_routes=1)
    try:
        await transport.publish("reply", b"initial", {})
        producer = producers[0]
        producer.entered.clear()
        producer.release.clear()
        pending = asyncio.create_task(transport.publish("reply", b"blocked", {}))
        assert await asyncio.to_thread(producer.entered.wait, 1)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        with pytest.raises(BufferError):
            await transport.publish("other", b"response", {})
        assert not producer.closed
        producer.release.set()
        await asyncio.gather(*transport.publishing)
        await transport.publish("other", b"response", {})
        assert producer.closed
    finally:
        for producer in producers:
            producer.release.set()
        await transport.close()


async def test_close_during_ensure_cleans_after_last_request():
    entered, release = asyncio.Event(), asyncio.Event()

    class Transport(MemoryTransport):
        async def ensure(self, topic, subscription, *, ordered=False):
            entered.set()
            await release.wait()
            await super().ensure(topic, subscription)

    transport = Transport()
    client = Client(transport)
    pending = asyncio.create_task(client.request("workflow", "id", "query", {}))
    await entered.wait()
    await client.close()
    release.set()
    with pytest.raises(RuntimeError, match="closed"):
        await pending
    assert not transport.queues
    assert not client.pending
    await client.close()


@pytest.mark.integration
async def test_native_reply_churn_and_ensure_only_unsubscribe():
    import os
    from uuid import uuid4

    from duraflow.message_runtime import OutboxRelay
    from duraflow.message_store import MemoryMessageStore
    from duraflow.messaging import Message, Publication

    url = os.getenv("DURAFLOW_TEST_PULSAR")
    if not url:
        pytest.skip("Native Pulsar endpoint required")
    server, client = PulsarTransport(url, max_routes=3), PulsarTransport(url, max_routes=3)
    store = MemoryMessageStore()
    relay = OutboxRelay(store, server)
    prefix = "persistent://public/default/reply-churn-" + uuid4().hex
    try:
        for index in range(12):
            topic = f"{prefix}-{index}"
            await client.ensure(topic, "client")
            response = Message("response", str(index), {"value": index})
            await store.apply(
                "relay",
                Message("request", "relay", {}),
                lambda state, now: [Publication(topic, response, subscription="client")],
            )
            assert await relay.step() == 1
            async with asyncio.timeout(10):
                delivery = None
                while delivery is None:
                    delivery = await client.receive(topic, "client")
            assert Message.from_bytes(delivery.data).body["value"] == index
            await client.ack(delivery)
            await client.unsubscribe(topic, "client")
        topic = prefix + "-ensure-only"
        await client.ensure(topic, "client")
        await client.unsubscribe(topic, "client")
        assert (topic, "client") not in client.provisioned
        assert len(server.producers) <= 3
        assert len(server.provisioned) <= 3
        assert not store.outbox
    finally:
        await client.close()
        await server.close()
