"""Service executor with a separate execution journal, never workflow-state access."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import inspect
import secrets
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .contracts import (
    Conflict,
    ProtocolError,
    Registry,
    TaskCancelled,
    TaskFailure,
    canonical,
    decode,
    duration,
    encode,
    fingerprint,
)
from .message_runtime import Consumer, OutboxRelay
from .message_store import MemoryMessageStore, MessageStore
from .messaging import Message, Publication, Topics, RetryLater
from .contracts import BroadcastBinding, Deferred
from .state import identity
from .transport import Delivery, Transport


class TaskContext:
    def __init__(self, worker: TaskWorker, request: dict[str, Any], generation: int):
        self.worker, self.request, self.generation = worker, request, generation
        self.task_id, self.attempt = request["task_id"], request["attempt"]
        self.run_id, self.node_id = request["run_id"], request["node_id"]
        self.idempotency_key = f"{request['namespace']}/{self.task_id}"
        self.cancel_requested = False
        self.deferred: Deferred | None = None

    def check_cancelled(self) -> None:
        if self.cancel_requested:
            raise TaskCancelled("Cancellation requested or lease lost")

    async def heartbeat(self, progress: float | None = None) -> bool:
        if progress is not None and not 0 <= progress <= 100:
            raise ValueError("Progress must be in 0..100")
        valid = False

        def update(state: dict[str, Any], now: float) -> list[Publication]:
            nonlocal valid
            valid = (
                state.get("generation") == self.generation
                and state.get("until", 0) > now
                and not state.get("cancelled")
            )
            if valid:
                state["until"] = now + self.worker.lease_seconds
                state["progress"] = progress
            return []

        await self.worker.journal.apply(
            self.worker.journal_key(self.request), Message("heartbeat", self.request["dispatch_id"], {}), update
        )
        self.cancel_requested = not valid
        return valid

    async def defer(self, *, timeout: float) -> Deferred:
        duration(timeout)
        if self.deferred is not None:
            return self.deferred
        secret = secrets.token_urlsafe(32)
        token_data = {
            "dispatch_id": self.request["dispatch_id"],
            "ref": self.request["ref"],
            "generation": self.generation,
            "secret": secret,
        }
        token = base64.urlsafe_b64encode(canonical(token_data).encode()).decode()

        def update(state: dict[str, Any], now: float) -> list[Publication]:
            if state.get("generation") != self.generation or state.get("until", 0) <= now or state.get("cancelled"):
                raise Conflict("Task no longer accepts delegation")
            state["delegation"] = {"digest": hashlib.sha256(secret.encode()).hexdigest(), "expires_at": now + timeout}
            event = Message(
                "task_delegated",
                self.request["workflow_key"],
                {
                    "run_id": self.run_id,
                    "node_id": self.node_id,
                    "dispatch_id": self.request["dispatch_id"],
                    "expires_at": now + timeout,
                },
                id=identity(self.request["dispatch_id"], f"delegated/{self.generation}"),
            )
            return [Publication(self.request["reply_to"], event, subscription="state")]

        await self.worker.journal.apply(
            self.worker.journal_key(self.request), Message("delegate", self.request["dispatch_id"], {}), update
        )
        self.deferred = Deferred(token)
        return self.deferred


class TaskWorker(Consumer):
    def __init__(
        self,
        transport: Transport,
        registry: Registry,
        *,
        topics: Topics | None = None,
        journal: MessageStore | None = None,
        broadcasts: tuple[BroadcastBinding, ...] = (),
        concurrency: int = 8,
        lease_seconds: float = 30,
    ):
        super().__init__(transport, topics or Topics(), concurrency=concurrency)
        duration(lease_seconds)
        self.registry, self.journal = registry, journal or MemoryMessageStore()
        self.lease_seconds = lease_seconds
        self.pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="duraflow-task")
        self.relay = OutboxRelay(self.journal, transport)
        self.routes = [(self.topics.task(ref.name, ref.version), "tasks", False) for ref, _ in registry.tasks.values()]
        self.routes.extend(
            (self.topics.task_completion(ref.name, ref.version), "tasks", False) for ref, _ in registry.tasks.values()
        )
        self.routes.extend(
            (self.topics.task_control(ref.name, ref.version), "tasks", False) for ref, _ in registry.tasks.values()
        )
        self.bindings = {f"df2-{self.topics.namespace}-{b.handler.subscription}": b for b in broadcasts}
        if len(self.bindings) != len(broadcasts):
            raise Conflict("Duplicate broadcast bindings")
        for subscription, binding in self.bindings.items():
            registered = registry.tasks.get(binding.handler.task.key)
            if registered is None or registered[0].descriptor() != binding.handler.task.descriptor():
                raise ProtocolError("Missing broadcast implementation")
            self.routes.append((binding.topic.name, subscription, False))
        self.metrics["executed"] = 0

    def journal_key(self, request: dict[str, Any]) -> str:
        return canonical([self.topics.namespace, "task", request["dispatch_id"]])

    def result(self, state: dict[str, Any], request: dict[str, Any]) -> Publication:
        state["emitted"] = state.get("emitted", 0) + 1
        event = Message(
            "task_result",
            request["workflow_key"],
            {
                "run_id": request["run_id"],
                "node_id": request["node_id"],
                "dispatch_id": request["dispatch_id"],
                "started_at": state["started_at"],
                **state["outcome"],
            },
            id=identity(request["dispatch_id"], f"result/{state['emitted']}"),
        )
        return Publication(request["reply_to"], event, subscription="state")

    def tag_events(self, request: dict[str, Any], kind: str, source: str) -> list[Publication]:
        ref = request["ref"]
        result = []
        for tag in request.get("tags", []):
            key = canonical([self.topics.namespace, ref["name"], ref["version"], tag])
            message = Message(
                kind,
                key,
                {"dispatch_id": request["dispatch_id"], "topic": self.topics.task_control(ref["name"], ref["version"])},
                id=identity(source, tag),
            )
            result.append(Publication(self.topics.task_tags(), message, subscription="tags"))
        return result

    async def handle(self, message: Message, delivery: Delivery) -> None:
        request = message.body
        if message.kind == "broadcast":
            message.require(handlers=dict, namespace=str, reply_to=str, workflow_key=str, run_id=str)
            binding = self.bindings.get(delivery.subscription)
            if binding is None or delivery.topic != binding.topic.name:
                raise ProtocolError("Unexpected broadcast route")
            participant = request["handlers"].get(binding.handler.subscription)
            if participant is None or participant["ref"] != binding.handler.task.descriptor():
                raise ProtocolError("Undeclared broadcast participant")
            request = {**request, **participant, "input": request["payload"], "attempt": 1}
        elif message.kind == "cancel_task":
            message.require(dispatch_id=str)
            if delivery.topic not in {
                self.topics.task_control(ref.name, ref.version) for ref, _ in self.registry.tasks.values()
            }:
                raise ProtocolError("Task cancellation requires a control topic")

            def cancel(state: dict[str, Any], now: float) -> list[Publication]:
                state["cancelled"] = True
                return []

            await self.journal.apply(self.journal_key(request), message, cancel)
            return
        elif message.kind == "complete_task":
            message.require(dispatch_id=str, ref=dict, generation=int, secret=str, correlation_id=str, reply_to=str)
            ref = message.body["ref"]
            if delivery.topic != self.topics.task_completion(ref.get("name", "invalid"), ref.get("version", 0)):
                raise ProtocolError("External completion requires its completion topic")
            await self.complete(message)
            return
        elif message.kind != "execute_task":
            raise ProtocolError("Unexpected service message")
        else:
            message.require(
                ref=dict,
                namespace=str,
                task_id=str,
                dispatch_id=str,
                node_id=str,
                run_id=str,
                workflow_key=str,
                reply_to=str,
                attempt=int,
            )
        required = (
            "ref",
            "input",
            "namespace",
            "task_id",
            "dispatch_id",
            "node_id",
            "run_id",
            "workflow_key",
            "reply_to",
            "attempt",
        )
        if not all(key in request for key in required):
            raise ProtocolError("Incomplete service execution request")
        ref, function = self.registry.tasks.get(f"{request['ref']['name']}:v{request['ref']['version']}", (None, None))
        if (
            ref is None
            or function is None
            or ref.descriptor() != request["ref"]
            or request["namespace"] != self.topics.namespace
        ):
            raise ProtocolError("Service contract or namespace mismatch")
        if message.kind == "execute_task" and delivery.topic != self.topics.task(ref.name, ref.version):
            raise ProtocolError("Wrong service route")
        value = decode(request["input"], ref.input_type)
        generation, started_at, completed, busy = 0, 0.0, False, False
        digest = fingerprint(
            {
                key: request[key]
                for key in (
                    "dispatch_id",
                    "task_id",
                    "attempt",
                    "ref",
                    "input",
                    "run_id",
                    "node_id",
                    "workflow_key",
                    "reply_to",
                )
            }
        )

        def claim(state: dict[str, Any], now: float) -> list[Publication]:
            nonlocal generation, started_at, completed, busy
            if request.get("not_before", 0) > now:
                raise RetryLater()
            if "digest" in state and state["digest"] != digest:
                raise ProtocolError("Task identity reused with different input")
            if "outcome" in state:
                completed = True
                return [self.result(state, request)]
            if state.get("delegation"):
                completed = True
                return []
            if state.get("until", 0) > now:
                busy = True
                return []
            generation = state.get("generation", 0) + 1
            started_at = state.setdefault("started_at", now)
            state.update(digest=digest, generation=generation, until=now + self.lease_seconds, request=request)
            started = Message(
                "task_started",
                request["workflow_key"],
                {
                    "run_id": request["run_id"],
                    "node_id": request["node_id"],
                    "dispatch_id": request["dispatch_id"],
                    "started_at": started_at,
                },
                id=identity(request["dispatch_id"], f"started/{generation}"),
            )
            return [
                *self.tag_events(request, "task_tag_add", started.id),
                Publication(request["reply_to"], started, subscription="state"),
            ]

        # Each physical delivery may reclaim an expired owner. The original request
        # digest is fenced inside the journal; transport redelivery is not a retry.
        await self.journal.apply(self.journal_key(request), Message("claim_task", request["dispatch_id"], {}), claim)
        if busy:
            raise RetryLater()
        if completed:
            await self.relay.step()
            return
        await self.relay.step()
        context = TaskContext(self, request, generation)
        existing = await self.journal.read(self.journal_key(request))
        context.cancel_requested = bool(existing.get("cancelled"))
        args = (value,) if len(inspect.signature(function).parameters) == 1 else (context, value)
        is_async = inspect.iscoroutinefunction(function)
        future = None
        stopping = asyncio.Event()

        async def renew() -> None:
            while not stopping.is_set():
                try:
                    await asyncio.wait_for(stopping.wait(), self.lease_seconds / 3)
                except TimeoutError:
                    try:
                        await context.heartbeat()
                    except Exception:
                        context.cancel_requested = True
                    if context.cancel_requested and is_async and future is not None:
                        future.cancel()
                        return

        watcher = asyncio.create_task(renew())
        outcome: dict[str, Any]
        deferred = False
        try:
            context.check_cancelled()
            future = (
                asyncio.ensure_future(function(*args))
                if is_async
                else asyncio.get_running_loop().run_in_executor(self.pool, function, *args)
            )
            value = await asyncio.shield(future)
            if isinstance(value, Deferred):
                if value != context.deferred:
                    raise ProtocolError("Forged delegation marker")
                deferred = True
                outcome = {}
            elif context.deferred is not None:
                raise ProtocolError("A delegated task must return its marker")
            else:
                outcome = {"result": encode(value, ref.output_type), "error": None}
        except asyncio.CancelledError:
            if not context.cancel_requested:
                if is_async and future is not None:
                    future.cancel()
                    await asyncio.gather(future, return_exceptions=True)
                raise
            outcome = {"result": None, "error": {"code": "CANCELLED", "message": "Task acknowledged cancellation"}}
        except TaskCancelled:
            outcome = {"result": None, "error": {"code": "CANCELLED", "message": "Task acknowledged cancellation"}}
        except TaskFailure as exc:
            outcome = {"result": None, "error": exc.error}
        except Exception as exc:
            outcome = {
                "result": None,
                "error": {
                    "code": "INVALID_RESULT" if isinstance(exc, ProtocolError) else "TASK_ERROR",
                    "message": type(exc).__name__,
                },
            }
        finally:
            stopping.set()
            await watcher
        if not deferred:

            def finish(state: dict[str, Any], now: float) -> list[Publication]:
                if state["generation"] != generation or state["until"] <= now:
                    raise RetryLater()
                state["outcome"] = (
                    {
                        "result": None,
                        "error": {
                            "code": "CANCELLED",
                            "message": "Task returned after cancellation; external outcome may be unknown",
                        },
                    }
                    if state.get("cancelled")
                    else outcome
                )
                result = self.result(state, request)
                return [result, *self.tag_events(request, "task_tag_remove", result.message.id)]

            await self.journal.apply(
                self.journal_key(request), Message("finish_task", request["dispatch_id"], {}), finish
            )
        self.metrics["executed"] += 1
        await self.relay.step()

    async def complete(self, message: Message) -> None:
        body = message.body
        ref, _ = self.registry.tasks[f"{body['ref']['name']}:v{body['ref']['version']}"]
        outcome = {
            "result": encode(decode(body["result"], ref.output_type), ref.output_type)
            if body.get("error") is None
            else None,
            "error": body.get("error"),
        }

        def update(state: dict[str, Any], now: float) -> list[Publication]:
            delegated = state.get("delegation")
            response = Message(
                "response",
                body["correlation_id"],
                {"correlation_id": body["correlation_id"], "value": None, "error": "Conflict"},
                id=identity(message.id, "response"),
            )
            rejected = [Publication(body["reply_to"], response, subscription="client")]
            if (
                not delegated
                or state["generation"] != body["generation"]
                or delegated["expires_at"] <= now
                or state.get("cancelled")
                or not hmac.compare_digest(delegated["digest"], hashlib.sha256(body["secret"].encode()).hexdigest())
            ):
                return rejected
            if "outcome" in state and state["outcome"] != outcome:
                return rejected
            state["outcome"] = outcome
            result = self.result(state, state["request"])
            response = Message(
                "response",
                body["correlation_id"],
                {"correlation_id": body["correlation_id"], "value": True, "error": None},
                id=identity(message.id, "response"),
            )
            return [
                result,
                *self.tag_events(state["request"], "task_tag_remove", result.message.id),
                Publication(body["reply_to"], response, subscription="client"),
            ]

        await self.journal.apply(self.journal_key(body), message, update)
        await self.relay.step()

    async def step(self) -> bool:
        sent = await self.relay.step()
        return bool(await super().step() or sent)

    async def close(self) -> None:
        self.pool.shutdown(wait=False, cancel_futures=True)
