"""Delivery adapters. Broker ACKs are never interpreted as business completion."""

from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from collections import deque
from dataclasses import dataclass
from typing import Any, Protocol

from .contracts import duration


@dataclass
class Delivery:
    topic: str
    subscription: str
    data: bytes
    properties: dict[str, str]
    receipt: Any
    key: str | None = None


class Transport(Protocol):
    async def ensure(self, topic: str, subscription: str, *, ordered: bool = False) -> None: ...
    async def publish(
        self,
        topic: str,
        data: bytes,
        properties: dict[str, str],
        *,
        key: str | None = None,
        deliver_at: float | None = None,
    ) -> None: ...
    async def receive(self, topic: str, subscription: str, timeout: float = 0.1) -> Delivery | None: ...
    async def ack(self, delivery: Delivery) -> None: ...
    async def nack(self, delivery: Delivery) -> None: ...
    async def unsubscribe(self, topic: str, subscription: str) -> None: ...
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

    async def ensure(self, topic: str, subscription: str, *, ordered: bool = False) -> None:
        if ordered:
            raise ValueError("Use MemoryBroker for ordered message subscriptions")
        self.queues.setdefault((topic, subscription), deque())

    async def publish(
        self,
        topic: str,
        data: bytes,
        properties: dict[str, str],
        *,
        key: str | None = None,
        deliver_at: float | None = None,
    ) -> None:
        if key is not None or deliver_at is not None:
            raise ValueError("Use MemoryBroker for keyed or delayed messages")
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

    async def unsubscribe(self, topic: str, subscription: str) -> None:
        self.queues.pop((topic, subscription), None)
        for key in list(self.inflight):
            if key[:2] == (topic, subscription):
                del self.inflight[key]


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
        operation_timeout: float = 10.0,
    ):
        import pulsar

        if receiver_queue_size < 1 or max_routes < 1:
            raise ValueError("Queue size and route limit must be positive")
        duration(operation_timeout)
        options: dict[str, Any] = {
            "operation_timeout_seconds": 10,
            "connection_timeout_ms": 5000,
            "tls_allow_insecure_connection": False,
            "tls_validate_hostname": True,
        }
        native_logger = logging.getLogger("duraflow.pulsar.native")
        native_logger.setLevel(logging.CRITICAL + 1)
        native_logger.propagate = False
        options["logger"] = native_logger
        if authentication is not None:
            options["authentication"] = authentication
        if tls_trust_certs_file_path is not None:
            options["tls_trust_certs_file_path"] = tls_trust_certs_file_path
        self.client, self.pulsar = pulsar.Client(url, **options), pulsar
        self.queue_size, self.max_routes = receiver_queue_size, max_routes
        self.consumers: dict[tuple[str, str], Any] = {}
        self.producers: dict[str, Any] = {}
        self.provisioned: set[tuple[str, str]] = set()
        self.ordered: set[tuple[str, str]] = set()
        self.lock = asyncio.Lock()
        self.native_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="duraflow-pulsar")
        self.native_slots = asyncio.Semaphore(8)
        self.operation_timeout = operation_timeout
        self._closed = False

    async def ensure(self, topic: str, subscription: str, *, ordered: bool = False) -> None:
        async with self.lock:
            key = topic, subscription
            if key in self.provisioned and ordered != (key in self.ordered):
                raise ValueError("Cannot change an existing subscription's ordering mode")
            if key not in self.provisioned:
                if len(self.provisioned) >= self.max_routes:
                    raise ValueError("Declared subscription limit exceeded")

                def provision() -> None:
                    consumer = self.client.subscribe(
                        topic,
                        subscription,
                        consumer_type=self.pulsar.ConsumerType.KeyShared
                        if ordered
                        else self.pulsar.ConsumerType.Shared,
                        initial_position=self.pulsar.InitialPosition.Earliest,
                        receiver_queue_size=1,
                        negative_ack_redelivery_delay_ms=1000,
                    )
                    # Closing belongs to the same native operation. A timed-out
                    # or cancelled await must not strand a prefetching consumer.
                    consumer.close()

                await self._native(provision)
                self.provisioned.add(key)
                if ordered:
                    self.ordered.add(key)

    async def publish(
        self,
        topic: str,
        data: bytes,
        properties: dict[str, str],
        *,
        key: str | None = None,
        deliver_at: float | None = None,
    ) -> None:
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
        options: dict[str, Any] = {"properties": properties}
        if key is not None:
            options["partition_key"] = key
        if deliver_at is not None:
            import math

            if not math.isfinite(deliver_at) or deliver_at < 0:
                raise ValueError("Delivery time must be a finite UTC timestamp")
            options["deliver_at"] = math.ceil(deliver_at * 1000)
        await self._native(self.producers[topic].send, data, **options)

    async def receive(self, topic: str, subscription: str, timeout: float = 0.1) -> Delivery | None:
        await self.ensure(topic, subscription, ordered=(topic, subscription) in self.ordered)
        async with self.lock:
            key = topic, subscription
            if key not in self.consumers:
                self.consumers[key] = await self._subscribe(topic, subscription, ordered=key in self.ordered)
        try:
            message = await self._native(self.consumers[key].receive, timeout_millis=max(1, int(timeout * 1000)))
        except self.pulsar.Timeout:
            return None
        return Delivery(
            topic, subscription, message.data(), message.properties(), message, message.partition_key() or None
        )

    async def _subscribe(self, topic: str, subscription: str, *, ordered: bool) -> Any:
        # Native calls cannot be cancelled. Explicitly transfer the returned
        # consumer's ownership, or close it if the awaiting coroutine left.
        lock = threading.Lock()
        abandoned, completed = False, None

        def subscribe() -> Any:
            nonlocal completed
            consumer = self.client.subscribe(
                topic,
                subscription,
                consumer_type=self.pulsar.ConsumerType.KeyShared if ordered else self.pulsar.ConsumerType.Shared,
                initial_position=self.pulsar.InitialPosition.Earliest,
                receiver_queue_size=self.queue_size,
                negative_ack_redelivery_delay_ms=1000,
            )
            with lock:
                close = abandoned
                if not close:
                    completed = consumer
            if close:
                consumer.close()
            return consumer

        try:
            return await self._native(subscribe)
        except BaseException:
            with lock:
                abandoned = True
                consumer = completed
            if consumer is not None:
                await self._native(consumer.close)
            raise

    async def ack(self, delivery: Delivery) -> None:
        await self._native(self.consumers[delivery.topic, delivery.subscription].acknowledge, delivery.receipt)

    async def nack(self, delivery: Delivery) -> None:
        await self._native(self.consumers[delivery.topic, delivery.subscription].negative_acknowledge, delivery.receipt)

    async def unsubscribe(self, topic: str, subscription: str) -> None:
        key = topic, subscription
        async with self.lock:
            consumer = self.consumers.pop(key, None)
            if consumer is not None:
                await self._native(consumer.unsubscribe)
            self.provisioned.discard(key)
            self.ordered.discard(key)

    async def close(self) -> None:
        if self._closed:
            return
        try:
            await self._native(self.client.close)
        finally:
            self._closed = True
            self.native_pool.shutdown(wait=False, cancel_futures=True)

    async def _native(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        if self._closed:
            raise RuntimeError("Pulsar transport is closed")
        await asyncio.wait_for(self.native_slots.acquire(), timeout=self.operation_timeout)
        try:
            if self._closed:
                raise RuntimeError("Pulsar transport is closed")
            future = asyncio.get_running_loop().run_in_executor(self.native_pool, partial(fn, *args, **kwargs))
        except BaseException:
            self.native_slots.release()
            raise

        def released(done: Any) -> None:
            self.native_slots.release()
            if not done.cancelled():
                done.exception()

        future.add_done_callback(released)
        return await asyncio.wait_for(asyncio.shield(future), timeout=self.operation_timeout)

    async def ping(self) -> bool:
        topic = next(iter(self.provisioned))[0] if self.provisioned else "persistent://public/default/df-health"
        return bool(await self._native(self.client.get_topic_partitions, topic))
