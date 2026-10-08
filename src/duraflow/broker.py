"""Deterministic retained broker for message-runtime tests; no direct engine calls."""

from __future__ import annotations

from collections import deque

from .contracts import Clock
from .transport import Delivery, MemoryTransport


class MemoryBroker(MemoryTransport):
    def __init__(self, *, clock: Clock | None = None, capacity: int = 10000):
        super().__init__(capacity=capacity)
        self.clock = clock or Clock()
        self.records: list[tuple[str, bytes, dict[str, str], str | None, float, int]] = []
        self.ordered: set[tuple[str, str]] = set()
        self.due: dict[int, float] = {}

    async def ensure(self, topic: str, subscription: str, *, ordered: bool = False) -> None:
        route = topic, subscription
        if route in self.queues:
            if ordered != (route in self.ordered):
                raise ValueError("Cannot change subscription ordering")
            return
        self.queues[route] = deque()
        if ordered:
            self.ordered.add(route)
        for target, data, props, key, _, receipt in self.records:
            if target == topic:
                self.queues[route].append(Delivery(topic, subscription, data, dict(props), receipt, key))

    async def publish(
        self,
        topic: str,
        data: bytes,
        properties: dict[str, str],
        *,
        key: str | None = None,
        deliver_at: float | None = None,
    ) -> None:
        if len(data) > 16 * 1024 * 1024:
            raise ValueError("Message too large")
        if any(len(q) >= self.capacity for (target, _), q in self.queues.items() if target == topic):
            raise BufferError("Broker queue full")
        self.counter += 1
        due = self.clock.now() if deliver_at is None else deliver_at
        self.due[self.counter] = due
        self.records.append((topic, data, dict(properties), key, due, self.counter))
        self.publications.append((topic, data, dict(properties)))
        for (target, sub), queue in self.queues.items():
            if target == topic:
                queue.append(Delivery(topic, sub, data, dict(properties), self.counter, key))

    async def receive(self, topic: str, subscription: str, timeout: float = 0.1) -> Delivery | None:
        await self.ensure(topic, subscription, ordered=(topic, subscription) in self.ordered)
        queue = self.queues[topic, subscription]
        locked = {d.key for (target, sub, _), d in self.inflight.items() if target == topic and sub == subscription}
        for _ in range(len(queue)):
            delivery = queue.popleft()
            if self.due[delivery.receipt] > self.clock.now() or (
                (topic, subscription) in self.ordered and delivery.key in locked
            ):
                queue.append(delivery)
                continue
            self.inflight[topic, subscription, delivery.receipt] = delivery
            return delivery
        return None

    async def ping(self) -> bool:
        return True

    async def nack(self, delivery: Delivery) -> None:
        self.due[delivery.receipt] = max(self.due[delivery.receipt], self.clock.now() + 0.01)
        await super().nack(delivery)
