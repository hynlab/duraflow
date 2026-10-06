"""Delivery adapters. Broker ACKs are never interpreted as business completion."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from collections import deque
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class Delivery:
    topic: str
    subscription: str
    data: bytes
    properties: dict[str, str]
    receipt: Any


class Transport(Protocol):
    async def ensure(self, topic: str, subscription: str) -> None: ...
    async def publish(self, topic: str, data: bytes, properties: dict[str, str]) -> None: ...
    async def receive(self, topic: str, subscription: str, timeout: float = 0.1) -> Delivery | None: ...
    async def ack(self, delivery: Delivery) -> None: ...
    async def nack(self, delivery: Delivery) -> None: ...
    async def close(self) -> None: ...


class MemoryTransport:
    """Testing only: exposes publication history and manual redelivery."""

    def __init__(self, *, capacity: int = 10_000):
        if capacity < 1:
            raise ValueError("Queue capacity must be positive")
        self.queues: dict[tuple[str, str], deque[Delivery]] = {}
        self.inflight: dict[tuple[str, str, int], Delivery] = {}
        self.publications: list[tuple[str, bytes, dict[str, str]]] = []
        self.counter, self.capacity = 0, capacity

    async def ensure(self, topic: str, subscription: str) -> None:
        self.queues.setdefault((topic, subscription), deque())

    async def publish(self, topic: str, data: bytes, properties: dict[str, str]) -> None:
        queues = [(key, queue) for key, queue in self.queues.items() if key[0] == topic]
        if any(len(queue) >= self.capacity for _, queue in queues):
            raise BufferError("Memory transport queue is full")
        self.counter += 1
        self.publications.append((topic, data, dict(properties)))
        for (_, sub), queue in queues:
            queue.append(Delivery(topic, sub, data, dict(properties), self.counter))

    async def receive(self, topic: str, subscription: str, timeout: float = 0.1) -> Delivery | None:
        queue = self.queues.get((topic, subscription))
        if not queue:
            return None
        delivery = queue.popleft()
        self.inflight[topic, subscription, delivery.receipt] = delivery
        return delivery

    async def ack(self, delivery: Delivery) -> None:
        self.inflight.pop((delivery.topic, delivery.subscription, delivery.receipt), None)

    async def nack(self, delivery: Delivery) -> None:
        key = delivery.topic, delivery.subscription, delivery.receipt
        if self.inflight.pop(key, None) is not None:
            self.queues[delivery.topic, delivery.subscription].append(delivery)

    def redeliver_unacked(self) -> None:
        for delivery in self.inflight.values():
            self.queues[delivery.topic, delivery.subscription].append(delivery)
        self.inflight.clear()

    async def close(self) -> None:
        self.redeliver_unacked()


class PulsarTransport:
    """Official client with off-loop native calls and bounded receiver queues.

    The initial adapter uses bytes-compatible topics. Existing schema-managed
    topics require an explicit schema/codec adapter; no silent schema replacement.
    """

    def __init__(
        self,
        url: str,
        *,
        receiver_queue_size: int = 64,
        max_routes: int = 256,
        authentication: Any = None,
        tls_trust_certs_file_path: str | None = None,
    ):
        import pulsar

        if receiver_queue_size < 1 or max_routes < 1:
            raise ValueError("Queue size and route limit must be positive")
        options: dict[str, Any] = {"operation_timeout_seconds": 10, "connection_timeout_ms": 5000}
        if authentication is not None:
            options["authentication"] = authentication
        if tls_trust_certs_file_path is not None:
            options["tls_trust_certs_file_path"] = tls_trust_certs_file_path
        self.client, self.pulsar = pulsar.Client(url, **options), pulsar
        self.queue_size, self.max_routes = receiver_queue_size, max_routes
        self.consumers: dict[tuple[str, str], Any] = {}
        self.producers: dict[str, Any] = {}
        self.provisioned: set[tuple[str, str]] = set()
        self.lock = asyncio.Lock()
        self.native_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="duraflow-pulsar")
        self.native_slots = asyncio.Semaphore(8)

    async def ensure(self, topic: str, subscription: str) -> None:
        async with self.lock:
            key = topic, subscription
            if key not in self.provisioned:
                if len(self.provisioned) >= self.max_routes:
                    raise ValueError("Declared subscription limit exceeded")
                consumer = await self._native(
                    self.client.subscribe,
                    topic,
                    subscription,
                    consumer_type=self.pulsar.ConsumerType.Shared,
                    initial_position=self.pulsar.InitialPosition.Earliest,
                    receiver_queue_size=1,
                )
                # Never retain an idle coordinator consumer: it would prefetch
                # and steal work. Closing returns any prefetched delivery.
                await self._native(consumer.close)
                self.provisioned.add(key)

    async def publish(self, topic: str, data: bytes, properties: dict[str, str]) -> None:
        async with self.lock:
            if topic not in self.producers:
                if len(self.producers) >= self.max_routes:
                    raise ValueError("Declared producer limit exceeded")
                self.producers[topic] = await self._native(
                    self.client.create_producer,
                    topic,
                    batching_enabled=False,
                    block_if_queue_full=True,
                    max_pending_messages=64,
                    send_timeout_millis=5000,
                )
        await self._native(self.producers[topic].send, data, properties=properties)

    async def receive(self, topic: str, subscription: str, timeout: float = 0.1) -> Delivery | None:
        await self.ensure(topic, subscription)
        async with self.lock:
            key = topic, subscription
            if key not in self.consumers:
                self.consumers[key] = await self._native(
                    self.client.subscribe,
                    topic,
                    subscription,
                    consumer_type=self.pulsar.ConsumerType.Shared,
                    initial_position=self.pulsar.InitialPosition.Earliest,
                    receiver_queue_size=self.queue_size,
                )
        try:
            message = await self._native(self.consumers[key].receive, timeout_millis=max(1, int(timeout * 1000)))
        except self.pulsar.Timeout:
            return None
        return Delivery(topic, subscription, message.data(), message.properties(), message)

    async def ack(self, delivery: Delivery) -> None:
        await self._native(self.consumers[delivery.topic, delivery.subscription].acknowledge, delivery.receipt)

    async def nack(self, delivery: Delivery) -> None:
        await self._native(self.consumers[delivery.topic, delivery.subscription].negative_acknowledge, delivery.receipt)

    async def close(self) -> None:
        try:
            await self._native(self.client.close)
        finally:
            self.native_pool.shutdown(wait=False, cancel_futures=True)

    async def _native(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        await self.native_slots.acquire()
        try:
            future = asyncio.get_running_loop().run_in_executor(self.native_pool, partial(fn, *args, **kwargs))
        except BaseException:
            self.native_slots.release()
            raise

        def released(done: Any) -> None:
            self.native_slots.release()
            if not done.cancelled():
                done.exception()

        future.add_done_callback(released)
        return await asyncio.shield(future)

    async def ping(self) -> bool:
        topic = next(iter(self.provisioned))[0] if self.provisioned else "persistent://public/default/df-health"
        return bool(await self._native(self.client.get_topic_partitions, topic))
