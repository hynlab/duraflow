"""Shared bounded consumption and recoverable publication infrastructure."""

from __future__ import annotations

import asyncio
import logging
import time
from uuid import uuid4

from .contracts import ProtocolError, canonical
from .message_store import MessageStore
from .messaging import Message, Topics, RetryLater
from .state import identity
from .transport import Delivery, Transport

log = logging.getLogger("duraflow.messages")


class OutboxRelay:
    def __init__(self, store: MessageStore, transport: Transport):
        self.store, self.transport = store, transport
        self.owner = str(uuid4())
        self.next_gc = time.monotonic() + 600
        self.gc_totals = {"records": 0, "inbox": 0, "states": 0}

    async def step(self) -> int:
        if time.monotonic() >= self.next_gc:
            self.next_gc = time.monotonic() + 600
            collected = await self.store.collect_garbage(limit=1000, dry_run=False)
            for key, value in collected.items():
                self.gc_totals[key] += value
            if any(value >= 1000 for value in collected.values()):
                self.next_gc = time.monotonic() + 1
        items = await self.store.claim(self.owner)
        for item in items:
            try:
                if item.subscription:
                    await self.transport.ensure(
                        item.topic, item.subscription, ordered=item.subscription in {"state", "replay", "tags"}
                    )
                message = item.message
                business = message.kind in {"publish", "broadcast"}
                if message.kind == "broadcast":
                    for handler in message.body["handlers"]:
                        await self.transport.ensure(item.topic, f"df2-{message.body['namespace']}-{handler}")
                data = canonical(message.body["payload"]).encode() if business else message.to_bytes()
                properties = {"duraflow-v2": message.to_bytes().decode()} if business else {}
                await self.transport.publish(item.topic, data, properties, key=message.key, deliver_at=item.deliver_at)
                if business and "confirmation" in message.body:
                    response = Message(
                        "published",
                        message.key,
                        {
                            **message.body["confirmation"],
                            **(
                                {"_df_expires_at": message.body["_df_expires_at"]}
                                if "_df_expires_at" in message.body
                                else {}
                            ),
                        },
                        id=identity(message.id, "published"),
                    )
                    await self.transport.publish(message.body["reply_to"], response.to_bytes(), {}, key=message.key)
                await self.store.delivered(message.id, self.owner)
            except Exception as exc:
                await self.store.release(item.message.id, self.owner)
                log.warning("publication_failed", extra={"error_type": type(exc).__name__})
        return len(items)


class Consumer:
    """ACK only after the handler has durably retained all subsequent work."""

    def __init__(self, transport: Transport, topics: Topics, *, concurrency: int = 1):
        if not 1 <= concurrency <= 256:
            raise ValueError("Invalid consumer concurrency")
        self.transport, self.topics, self.concurrency = transport, topics, concurrency
        self.routes: list[tuple[str, str, bool]] = []
        self.cursors: dict[tuple[tuple[str, str, bool], ...], int] = {}
        self.accepting, self.draining = True, False
        self.metrics = {"messages": 0, "errors": 0}

    async def prepare(self) -> None:
        for topic, subscription, ordered in self.routes:
            await self.transport.ensure(topic, subscription, ordered=ordered)

    async def process(self, delivery: Delivery) -> None:
        try:
            try:
                raw = delivery.properties.get("duraflow-v2")
                message = Message.from_bytes(raw.encode() if raw is not None else delivery.data)
                if delivery.key is not None and delivery.key != message.key:
                    raise ProtocolError("Broker key differs from message identity")
                await self.handle(message, delivery)
            except ProtocolError as exc:
                await self.transport.publish(
                    delivery.topic + "-dlq", delivery.data[:262144], {"reason": type(exc).__name__}, key=delivery.key
                )
                await self.transport.ack(delivery)
                self.metrics["errors"] += 1
            else:
                await self.transport.ack(delivery)
                self.metrics["messages"] += 1
        except RetryLater:
            await self.transport.nack(delivery)
        except BaseException:
            await self.transport.nack(delivery)
            raise

    async def handle(self, message: Message, delivery: Delivery) -> None:
        raise NotImplementedError

    async def step(self) -> bool:
        return await self.consume(self.routes)

    async def consume(self, routes: list[tuple[str, str, bool]]) -> bool:
        if self.draining or not self.accepting:
            return False
        await self.prepare()
        lane = tuple(routes)
        for _ in range(len(routes)):
            cursor = self.cursors.get(lane, 0)
            route = routes[cursor % len(routes)]
            self.cursors[lane] = cursor + 1
            delivery = await self.transport.receive(route[0], route[1])
            if delivery is not None:
                await self.process(delivery)
                return True
        return False

    async def run(self, stop: asyncio.Event, *, poll_interval: float = 0.05) -> None:
        async def loop(control: bool = False) -> None:
            while not stop.is_set():
                try:
                    worked = await (self.control_step() if control else self.execution_step())
                except Exception as exc:
                    log.error("consumer_iteration_failed", extra={"error_type": type(exc).__name__})
                    worked = False
                if not worked:
                    try:
                        await asyncio.wait_for(stop.wait(), poll_interval)
                    except TimeoutError:
                        pass
                else:
                    await asyncio.sleep(0)

        async with asyncio.TaskGroup() as tasks:
            for _ in range(self.concurrency):
                tasks.create_task(loop())
            if self.has_control_lane:
                tasks.create_task(loop(control=True))

    has_control_lane = False

    async def execution_step(self) -> bool:
        return await self.step()

    async def control_step(self) -> bool:
        return False

    async def close(self) -> None:
        pass
