"""Bounded task execution with fenced leases and durable automatic reporting."""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import secrets
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from .contracts import (Clock, Conflict, MAX_PAYLOAD_BYTES, NotFound, ProtocolError, Registry,
                        TaskCancelled, TaskFailure, TaskRef, HandlerRef, TopicRef,
                        decode, duration, encode, fingerprint, name, parse_json)
from .state import event, mutate, route, subscription, wake
from .storage import State, Store, clone
from .transport import Delivery, Transport

log = logging.getLogger("duraflow.worker")


@dataclass(frozen=True)
class Deferred:
    token: str


@dataclass(frozen=True)
class BroadcastBinding:
    topic: TopicRef[Any]
    handler: HandlerRef[Any, Any]


class TaskContext:
    def __init__(self, worker: Worker, run_id: str, node_id: str, number: int, epoch: int, task_id: str):
        self.worker, self.run_id, self.node_id = worker, run_id, node_id
        self.attempt, self.lease_epoch, self.task_id = number, epoch, task_id
        self.idempotency_key = f"{worker.namespace}/{task_id}"
        self.cancel_requested, self.stale = False, False
        self._deferred: Deferred | None = None

    def _current(self, state: State) -> State:
        node = state["nodes"].get(self.node_id)
        if node is None or node["state"] != "pending":
            raise Conflict("Invocation is no longer pending")
        current = node["attempts"][-1]
        if current["number"] != self.attempt or current["epoch"] != self.lease_epoch:
            raise Conflict("Execution lease has been superseded")
        return current

    async def heartbeat(self, progress: float | None = None) -> bool:
        if progress is not None and not 0 <= progress <= 100:
            raise ValueError("Progress must be within 0..100")

        def change(state: State) -> bool:
            current = self._current(state)
            if current["lease_until"] <= self.worker.clock.now() and current["deferred"] is None:
                raise Conflict("Execution lease expired")
            self.cancel_requested = bool(state["nodes"][self.node_id].get("cancel_requested"))
            current["lease_until"] = self.worker.clock.now() + self.worker.lease_seconds
            if progress is not None:
                current["progress"] = progress
            return True

        try:
            return bool(await mutate(self.worker.store, self.worker.namespace, self.run_id, change))
        except Conflict:
            self.cancel_requested, self.stale = True, True
            return False

    def check_cancelled(self) -> None:
        if self.cancel_requested:
            raise TaskCancelled("Cancellation requested")

    async def defer(self, *, timeout: float) -> Deferred:
        duration(timeout)
        if self._deferred is not None:
            return self._deferred
        token = f"{self.run_id}/{self.node_id}/{self.attempt}/{self.lease_epoch}/{secrets.token_urlsafe(32)}"
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        def change(state: State) -> None:
            current = self._current(state)
            if (current["lease_until"] <= self.worker.clock.now() or current["observation"] is not None
                    or state["nodes"][self.node_id].get("cancel_requested")):
                raise Conflict("Cannot defer an expired, resolved or cancelled invocation")
            current["deferred"] = {"token_hash": token_hash, "expires_at": self.worker.clock.now() + timeout}
            event(state, "task_delegated", self.worker.clock.now(), node_id=self.node_id)

        await mutate(self.worker.store, self.worker.namespace, self.run_id, change)
        self._deferred = Deferred(token)
        return self._deferred


class Worker:
    def __init__(self, store: Store, transport: Transport, registry: Registry, *,
                 namespace: str = "default", clock: Clock | None = None,
                 broadcasts: tuple[BroadcastBinding, ...] = (), concurrency: int = 8, lease_seconds: float = 30.0):
        duration(lease_seconds)
        if not 1 <= concurrency <= 256:
            raise ValueError("Worker concurrency must be within 1..256")
        self.store, self.transport, self.registry = store, transport, registry
        self.namespace, self.clock = name(namespace), clock or Clock()
        self.broadcasts, self.concurrency, self.lease_seconds = broadcasts, concurrency, lease_seconds
        self.owner = str(uuid4())
        self.pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="duraflow-task")
        self.bindings: list[tuple[str, str, TaskRef[Any, Any]]] = []
        self.semaphore = asyncio.Semaphore(concurrency)
        self.prepared, self.cursor = False, 0
        self.metrics = {"executed": 0, "duplicates": 0, "stale_results": 0, "quarantined": 0}

    async def prepare(self) -> None:
        if self.prepared:
            return
        bindings = [(route(self.namespace, ref.descriptor()), "workers", ref)
                    for ref, _ in self.registry.tasks.values()]
        for binding in self.broadcasts:
            entry = self.registry.tasks.get(binding.handler.task.key)
            if entry is None or entry[0].descriptor() != binding.handler.task.descriptor():
                raise ProtocolError("Broadcast binding has no compatible registered implementation")
            bindings.append((binding.topic.name, subscription(self.namespace, binding.handler.subscription), binding.handler.task))
        if len({(topic, sub) for topic, sub, _ in bindings}) != len(bindings):
            raise Conflict("Duplicate topic/subscription binding")
        for topic, sub, _ in bindings:
            await self.transport.ensure(topic, sub)
        self.bindings, self.prepared = bindings, True

    async def _claim(self, delivery: Delivery, ref: TaskRef[Any, Any]) -> TaskContext | None:
        if len(delivery.data) > MAX_PAYLOAD_BYTES or len(delivery.properties.get("duraflow", "")) > MAX_PAYLOAD_BYTES:
            raise ProtocolError("Oversized message")
        try:
            meta = parse_json(delivery.properties["duraflow"])
            payload = parse_json(delivery.data)
            if meta["v"] != 1 or meta["namespace"] != self.namespace:
                raise ProtocolError("Unsupported protocol or namespace")
            if meta["kind"] == "broadcast":
                node_id = meta["handlers"].get(delivery.subscription)
                if node_id is None:
                    raise ProtocolError("Subscription is not a declared participant")
            elif meta["kind"] == "task":
                node_id = meta["node_id"]
            else:
                raise ProtocolError("Unexpected delivery kind")
            run_id, number, event_id = meta["run_id"], meta["attempt"], meta["event_id"]
            if not all(isinstance(v, str) for v in (run_id, node_id, event_id)) or type(number) is not int:
                raise ProtocolError("Invalid task identity types")
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ProtocolError("Malformed task envelope") from exc
        digest, now = fingerprint([payload, meta]), self.clock.now()

        def change(state: State) -> Any:
            if state["archived"]:
                return None
            node = state["nodes"].get(node_id)
            if (node is None or node["spec"]["kind"] != "call" or node["spec"]["ref"] != ref.descriptor()
                    or fingerprint(payload) != fingerprint(node["spec"]["input"])):
                raise ProtocolError("Message identity/input does not match the committed invocation")
            committed = state["outbox"].get(event_id)
            if committed is None or fingerprint([committed["payload"], committed["metadata"]]) != digest:
                raise ProtocolError("Message does not match its committed dispatch")
            expected_topic = node.get("initial_topic") if meta["kind"] == "broadcast" else route(self.namespace, ref.descriptor())
            expected_sub = subscription(self.namespace, node["spec"]["handler"]) if meta["kind"] == "broadcast" else "workers"
            if delivery.topic != expected_topic or delivery.subscription != expected_sub:
                raise ProtocolError("Message delivered to the wrong route or participant")
            inbox_key = f"{delivery.subscription}/{event_id}"
            if inbox_key in state["inbox"] and state["inbox"][inbox_key] != digest:
                raise ProtocolError("Conflicting payload for an existing event identity")
            state["inbox"][inbox_key] = digest
            current = node["attempts"][-1]
            if (node["state"] != "pending" or node.get("cancel_requested")
                    or state["status"] in {"CANCELLING", "CANCELLED", "TERMINATED"}
                    or current["number"] != number or current["not_before"] > now
                    or current["observation"] is not None or current["deferred"] is not None
                    or current["lease_until"] > now):
                return None
            current["epoch"] += 1
            current["owner"], current["lease_until"] = self.owner, now + self.lease_seconds
            if current["started_at"] is None:
                current["started_at"] = now
            event(state, "task_claimed", now, node_id=node_id, attempt=number, epoch=current["epoch"])
            return current["epoch"], node["task_id"]

        claimed = await mutate(self.store, self.namespace, run_id, change)
        if claimed is None:
            self.metrics["duplicates"] += 1
            return None
        epoch, task_id = claimed
        return TaskContext(self, run_id, node_id, number, epoch, task_id)

    async def _observe(self, context: TaskContext, result: Any, error: dict[str, Any] | None) -> None:
        observation = {"result": clone(result), "error": clone(error)}

        def change(state: State) -> None:
            current = context._current(state)
            if current["lease_until"] <= self.clock.now() and current["deferred"] is None:
                raise Conflict("Cannot record an outcome after lease expiry")
            if current["observation"] is not None:
                if fingerprint(current["observation"]) != fingerprint(observation):
                    raise Conflict("Conflicting observation")
                return
            current["observation"] = observation
            event(state, "task_observed", self.clock.now(), node_id=context.node_id,
                  attempt=context.attempt, epoch=context.lease_epoch)
            wake(state, f"observation/{context.node_id}/{context.attempt}/{context.lease_epoch}", self.clock.now())

        try:
            await mutate(self.store, self.namespace, context.run_id, change)
        except Conflict:
            self.metrics["stale_results"] += 1

            def stale(state: State) -> None:
                if state["archived"]:
                    return
                node = state["nodes"].get(context.node_id)
                if node is None:
                    return
                key = f"{context.attempt}/{context.lease_epoch}"
                observations = node.setdefault("stale_observations", {})
                if len(observations) < 100 and key not in observations:
                    observations[key] = fingerprint(observation)
                    event(state, "stale_observation", self.clock.now(), node_id=context.node_id,
                          attempt=context.attempt, epoch=context.lease_epoch)

            await mutate(self.store, self.namespace, context.run_id, stale)

    async def _execute(self, context: TaskContext, ref: TaskRef[Any, Any]) -> None:
        _, fn = self.registry.tasks[ref.key]
        state = await self.store.load(self.namespace, context.run_id)
        value = decode(state["nodes"][context.node_id]["spec"]["input"], ref.input_type)
        args = (value,) if len(inspect.signature(fn).parameters) == 1 else (context, value)
        is_async = inspect.iscoroutinefunction(fn)
        loop = asyncio.get_running_loop()
        future = asyncio.ensure_future(fn(*args)) if is_async else loop.run_in_executor(self.pool, fn, *args)
        stopping = asyncio.Event()

        async def heartbeat() -> None:
            while not stopping.is_set():
                try:
                    await asyncio.wait_for(stopping.wait(), timeout=self.lease_seconds / 3)
                except TimeoutError:
                    try:
                        await context.heartbeat()
                    except Exception:
                        # Losing connectivity may lose ownership. Never continue
                        # advertising a valid lease after an unconfirmed renewal.
                        context.stale, context.cancel_requested = True, True
                    if context.cancel_requested and is_async:
                        future.cancel()
                        return

        watcher = asyncio.create_task(heartbeat())
        result, error, deferred_result = None, None, False
        try:
            result = await asyncio.shield(future)
            if isinstance(result, Deferred):
                if context._deferred != result:
                    raise ProtocolError("Return only the Deferred marker obtained from context.defer()")
                deferred_result = True
            elif context._deferred is not None:
                raise ProtocolError("A delegated task must return its Deferred marker")
            else:
                result = encode(result, ref.output_type)
            if context.cancel_requested and not deferred_result:
                result, error = None, {"code": "CANCELLED", "message": "Task acknowledged cancellation"}
        except asyncio.CancelledError:
            if context.cancel_requested:
                error = {"code": "CANCELLED", "message": "Task acknowledged cancellation"}
            else:
                if is_async:
                    future.cancel()
                    await asyncio.gather(future, return_exceptions=True)
                raise
        except TaskFailure as exc:
            error = exc.error
        except TaskCancelled:
            error = {"code": "CANCELLED", "message": "Task acknowledged cancellation"}
        except ProtocolError:
            error = {"code": "INVALID_RESULT", "message": "Task output violates its contract"}
        except Exception as exc:
            error = {"code": "TASK_ERROR", "message": type(exc).__name__}
        finally:
            stopping.set()
            await asyncio.gather(watcher, return_exceptions=True)
        if not deferred_result or error is not None:
            await self._observe(context, result if error is None else None, error)
        self.metrics["executed"] += 1

    async def process(self, delivery: Delivery, ref: TaskRef[Any, Any]) -> None:
        async with self.semaphore:
            try:
                context = await self._claim(delivery, ref)
                if context is not None:
                    await self._execute(context, ref)
                await self.transport.ack(delivery)
            except (ProtocolError, NotFound) as exc:
                await self.transport.publish(delivery.topic + "-dlq", delivery.data,
                                             {"reason": type(exc).__name__, "source_subscription": delivery.subscription})
                await self.transport.ack(delivery)
                self.metrics["quarantined"] += 1
            except BaseException:
                await self.transport.nack(delivery)
                raise

    async def step(self) -> bool:
        await self.prepare()
        for _ in range(len(self.bindings)):
            topic, sub, ref = self.bindings[self.cursor % len(self.bindings)]
            self.cursor += 1
            delivery = await self.transport.receive(topic, sub)
            if delivery is not None:
                await self.process(delivery, ref)
                return True
        return False

    async def run(self, stop: asyncio.Event, *, poll_interval: float = 0.05) -> None:
        await self.prepare()

        async def loop() -> None:
            while not stop.is_set():
                try:
                    if not await self.step():
                        await asyncio.sleep(poll_interval)
                except Exception as exc:
                    log.error("Worker iteration failed", extra={"error_type": type(exc).__name__})
                    await asyncio.sleep(poll_interval)

        async with asyncio.TaskGroup() as group:
            for _ in range(self.concurrency):
                group.create_task(loop())

    async def close(self) -> None:
        # Python cannot safely kill a running sync task. Applications requiring
        # hard timeouts must isolate those workers in supervised processes.
        self.pool.shutdown(wait=False, cancel_futures=True)
