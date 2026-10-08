"""Late native subscribe must not leave an idle consumer stealing workflow activations."""

import asyncio
import sys
import threading
from types import SimpleNamespace

import pytest

from duraflow.transport import PulsarTransport


@pytest.mark.parametrize("interruption", ["timeout", "cancel"])
@pytest.mark.parametrize("operation", ["ensure", "receive"])
async def test_late_provisioned_consumer_is_closed_even_after_interrupted_await(monkeypatch, interruption, operation):
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()

    class NativeClient:
        def __init__(self, *args, **kwargs):
            pass

        def subscribe(self, *args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("Test failed to release native subscribe")
            message = SimpleNamespace(data=lambda: b"payload", properties=dict, partition_key=lambda: "key")
            return SimpleNamespace(close=closed.set, receive=lambda **kwargs: message)

        def close(self):
            pass

    monkeypatch.setitem(
        sys.modules,
        "pulsar",
        SimpleNamespace(
            Client=NativeClient,
            ConsumerType=SimpleNamespace(Shared="shared", KeyShared="ordered"),
            InitialPosition=SimpleNamespace(Earliest="earliest"),
        ),
    )
    transport = PulsarTransport("pulsar://test", operation_timeout=0.1 if interruption == "timeout" else 1)
    if operation == "receive":
        release.set()
        await transport.ensure("topic", "state", ordered=True)
        release.clear()
        entered.clear()
        closed.clear()
    pending = asyncio.create_task(
        transport.ensure("topic", "state", ordered=True)
        if operation == "ensure"
        else transport.receive("topic", "state")
    )
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        if interruption == "cancel":
            pending.cancel()
        with pytest.raises(TimeoutError if interruption == "timeout" else asyncio.CancelledError):
            await pending
        release.set()
        assert await asyncio.to_thread(closed.wait, 1), "Native consumer leaked after late subscribe completion"
        if operation == "ensure":
            assert ("topic", "state") not in transport.provisioned
            await transport.ensure("topic", "state", ordered=True)
            assert ("topic", "state") in transport.provisioned
        else:
            assert ("topic", "state") not in transport.consumers
            assert (await transport.receive("topic", "state")).data == b"payload"
    finally:
        release.set()
        await transport.close()
